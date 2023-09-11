# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Classes for updaters. """

from abc import abstractmethod
from attrs import define, field
from datetime import datetime, timedelta
import logging
import time
from typing import Any, Dict, Mapping, MutableSequence, Sequence

from google import auth
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from googleapiclient import discovery
from googleapiclient.http import BatchHttpRequest, HttpRequest

from base import BaseInterfacer, BaseInterfacerBuilder, BaseInterfacerFactory, NamesInput, Primitives

NOT_FOUND_ERROR_CODE = 404

DEFAULT_MAX_TRIES = 3
DEFAULT_MAX_REQUESTS_PER_MINUTE = 60
BATCH_SIZE_LIMIT = 150

UPDATER_REGISTRATIONS_FILE = 'updaters/updaters.json'
UPDATERS_CLASS_PREFIX = 'updaters.'

RequestQueue = dict[str, HttpRequest]


@define(auto_attribs=True)
class BaseUpdater(BaseInterfacer):
  """Base class for updaters.

  Must be subclassed to be used.

  An 'updater' provides the means to update list of values based on their keys.
  E.g., Update a list of campaign names to new names (based on either their old
  names or their ids).

  Attributes:
    updates (Sequence[UpdatableEntity]): List of updates to be made.
  """
  # Required:
  updates: Sequence[NamesInput] = field(kw_only=True)

  # Private:
  _results: Mapping[str, str | Exception] = field(init=False, factory=dict)

  @abstractmethod
  def apply_updates(self):
    """ Updates underlying entities with matching keys in `updates`.

    Returns:
        Results of updates.
    """
    pass


@define(auto_attribs=True)
class GoogleAPIClientUpdater(BaseUpdater):
  """Base class for Google API Client-based updaters for advertising products.

  Subclasses must implement `get_entity_resource()` and
  `generate_update_request()`.

  An 'updater' provides the means to update list of values based on their keys.
  E.g., Update a list of campaign names to new names (based on either their old
  names or their ids).

  This class performs the first update try in batch mode (asynchronously) and
  subsequent tries synchronously.

  Attributes:
    entity_type (str): Type of entity being updated. (E.g., 'Campaign'.)
    customer_owner_id (str): Profile id for CM, account id for DV360, etc...
    entity_type (str): Type of entity being updated. (E.g., 'Campaign'.)
    updates (Sequence[UpdatableEntity]): List of updates to be made.
    max_batch_size (int): Maximum number of requests in a single batch.
    max_requests_per_minute (int) Maximum number of requests per minute.
    max_tries (int): Maximum number of requests  (retries if failed) per update.
    failed_updates (Mapping[str, Exception]): If any updates fail, the failed
      updates  will be passed in a mapping with the entity's id as the key and
      the Exception as the value.
  """
  # Required:
  entity_type: str = field(kw_only=True)
  customer_owner_id: str = field(kw_only=True)

  # Must be defined in subclass:
  _API_NAME: str = field(init=False)
  _API_VERSION: str = field(init=False)
  _API_SCOPES: Sequence[str] = field(init=False)

  # Optional:
  _service: discovery.Resource = field(default=None, kw_only=True)

  # Private constants:
  _MAX_TRIES: int = field(init=False, default=DEFAULT_MAX_TRIES)
  _MAX_BATCH_SIZE: int = field(init=False, default=BATCH_SIZE_LIMIT)
  _MAX_REQUESTS_PER_MINUTE: int = field(init=False, default=BATCH_SIZE_LIMIT)

  # Private:
  _batch_requester: BatchHttpRequest = field(init=False)
  _next_batch_execution_time: datetime = field(init=False,
                                               default=datetime(1, 1, 1))
  _num_tries: int = field(init=False, default=1)
  _in_progress: RequestQueue = field(init=False, factory=dict)
  _pending: RequestQueue = field(init=False, factory=dict)
  _pending_pops: MutableSequence[NamesInput] = field(init=False, factory=list)
  _results: Mapping[str, str | Exception] = field(init=False, factory=dict)

  def __attrs_post_init__(self):
    if self._MAX_BATCH_SIZE > BATCH_SIZE_LIMIT:
      self._MAX_BATCH_SIZE = BATCH_SIZE_LIMIT

    super().__attrs_post_init__()

  @abstractmethod
  def generate_update_request(self, entity_resource: discovery.Resource,
                              update: NamesInput):
    """Generates a request to update the entity.

    Args:
        entity_resource (discovery.Resource): Service entity resource.
        update (UpdatableEntity): Update info for entity.

    Returns:
        HttpRequest: HTTP request.
    """
    pass

  @abstractmethod
  def get_entity_resource(self) -> discovery.Resource:
    """Returns the applicable entity resource from the API.

    Based on the class object's `entity_type` property:
      E.g., service.campaigns(), service.creatives(), etc...

    Raises:
        ValueError: If an invalid value is used for `entity_type`.
    """
    pass

  def apply_updates(self) -> Sequence[NamesInput]:
    """ Updates underlying product's entities with matching keys in `updates`.

    Returns:
        Sequence[NamesInput]: Results of updates.
    """
    self._init_update()
    self._perform_updates()
    responses = self.merge_input_and_output(self.updates, self._results)
    return responses

  def _init_update(self):
    """Inits the Updater and returns the current queue."""
    if not self._service:
      self._service = self._build_service()
    self._batch_requester = self._service.new_batch_http_request()
    entity_resource = self.get_entity_resource()
    for update in self.updates:
      self._pending[update['key']] = self.generate_update_request(
          entity_resource, update)

  def _perform_updates(self):
    """Performs the update, retrying failed requests."""
    while self._num_tries <= self._MAX_TRIES:
      if len(self._in_progress) == 0:
        self._num_tries += 1
        self._batch_execute_updates()
      # need to do this in the main thread to inadvertently modifying `pending`
      # while we are iterating through its values.
      [self._pending.pop(k) for k in self._pending_pops]
      self._pending_pops.clear()

  def _batch_execute_updates(self):
    """Batches updates and executes batches as needed."""
    [self._handle_batching(key) for key in self._pending]
    #TODO(blevitan): Confirm `_batch_requester._requests` clears after batch execution.
    if self._batch_requester._requests:
      self._execute_batch()

  def _handle_batching(self, key: str):
    """Handles batching of runs for requests.

      Will add to batch and call `run_batch` if the batch is full.

    Args:
        key (str): Unique id to get update request from `_pending`.
    """
    request: HttpRequest = self._pending[key]
    self._in_progress[key] = request
    self._batch_requester.add(request,
                              request_id=key,
                              callback=self._update_callback)

    if len(self._batch_requester._requests) == self._MAX_BATCH_SIZE:
      self._execute_batch()

  def _execute_batch(self):
    """Runs batch (accounting for throttle-rate) if there is anything to run."""

    if (self._batch_requester._requests):
      sleep_time = (self._next_batch_execution_time -
                    datetime.now()).total_seconds() + 1
      time.sleep(max(0, sleep_time))
      self._batch_requester.execute()
      batch_size = len(self._batch_requester._requests)
      self._next_batch_execution_time = datetime.now() +\
        timedelta(60 * batch_size // self._MAX_REQUESTS_PER_MINUTE)
      self._batch_requester = self._service.new_batch_http_request()

  def _update_callback(self, request_id: str, _, error: Exception):
    """Callback function after patch command.

    Handles success (remove from queue) or failure (or add to retry queue if
    we haven't hit the max number of retries.)

    Args:
        request_id (str): Unique id of original request.
        _ (HttpResponse): [Not used] Response from server.
        error (Exception): Exception returned from server (if any).
    """
    if error:
      self._results[request_id] = self._handle_failed_update(request_id, error)
    else:
      self._results[request_id] = 'Updated.'
    # defer `_pending.pop()` to the main thread to avoid modifying `_pending`
    # while we are iterating through its values (will cause GIL failure).
    self._pending_pops.append(request_id)
    self._in_progress.pop(request_id)

  def _handle_failed_update(self, request_id: str, error: Exception) -> str:
    """Handles failed patch requests. Retries and return results message.

    Args:
        request_id (str): Unique id of original request.
        exception (Exception): Exception returned from server (if any).
    """
    err_msg = f'Update failed: [{error.status_code}] {error.reason}'
    if (self._num_tries == self._MAX_TRIES or
        error.status_code == NOT_FOUND_ERROR_CODE):
      logging.warning(f'Request {request_id} failed: {err_msg}')
      return err_msg
    else:
      logging.info(f'Request {request_id} failed: {err_msg} ...will retry.')
      # TODO(blevitan): Add in retry logic (exponential backoff?)
      try:
        self._pending(request_id).execute()
        return 'Updated.'
      except Exception as e:
        logging.warning(f'Request {request_id} failed: {err_msg}')
        return err_msg

  def _build_service(self) -> discovery.Resource:
    """"Builds the Google API service.

    Creates credentials via `_access_token`, if provided. Otherwise, uses the
    default account.

    Returns:
        discovery.Resource: Google API service.
    """
    if self._access_token:
      credentials = Credentials(self._access_token)
    else:
      credentials = auth.default(scopes=self._API_SCOPES)[0]

    return discovery.build(self._API_NAME,
                           self._API_VERSION,
                           credentials=credentials)


class UpdaterFactory(BaseInterfacerFactory):
  UPDATERS_CLASS_PREFIX = UPDATERS_CLASS_PREFIX
  UPDATER_REGISTRATIONS_FILE = UPDATER_REGISTRATIONS_FILE
