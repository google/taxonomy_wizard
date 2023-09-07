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
"""Base classes for interfaces (Validators and Updaters)."""

from abc import abstractmethod
from attrs import define, field
from bq_client import BqClient
from google.cloud import bigquery
from typing import Mapping, Sequence

Primitives = str | int | float | bool
NamesInput = dict[str, Primitives]

import json
import sys
from typing import Mapping

from google.cloud import bigquery

from base import Primitives


@define(auto_attribs=True)
class BaseInterfacer():
  """Base class for all Interfacers in the Validator web function.

  Attributes:
    cloud_project (str): Project running Taxonomy Wizard server-side.
    dataset (str): BigQuery Dataset containing taxonomy data.
    bq_client (bigquery.Client): BigQuery API client.
    key_field_name (str): Name of the entity's field to use as a key.
    response_field_name (str): Name of the field containing the response.
    base_row_columns (Sequence[str]): Columns from `base_row_data` to append
      to every row of output.
    _base_row_data (Mapping[str, Primitives]): Source of data to append to every
      row of output. (Must be a Mapping with at least all columns specified in
      `base_row_columns`.)
    _access_token: str: Access token to run as a specific user, else uses
      default service account.
  """
  # Required:
  cloud_project: str = field(kw_only=True)
  bigquery_dataset: str = field(kw_only=True)
  bq_client: bigquery.Client = field(kw_only=True, default=BqClient().get())
  key_field_name: str = field(default='key', kw_only=True)
  _base_row_data: Mapping[str, Primitives] = field(factory=dict, kw_only=True)
  response_field_name: str = field(default='response', kw_only=True)
  base_row_columns: Sequence[str] = field(factory=list, kw_only=True)

  # Optional
  _access_token: str = field(default='', kw_only=True)

  # Private:
  _base_row: Mapping[str, Primitives] = field(init=False)

  def __attrs_post_init__(self):
    self._base_row = {
        k: v
        for k, v in self._base_row_data.items()
        if k in self.base_row_columns
    }

  def merge_input_and_output(
      self, input: list[NamesInput],
      results: Mapping[str, str]) -> Sequence[NamesInput]:
    """ Merges `base_row` and validation messages with original input data rows.

    Args:
        input (list[NamesInput]): List of original input data rows.
        results ( Mapping[str, str]): Validated data.

    Returns:
        Sequence[NamesInput]: Merged data.
    """

    response: list[NamesInput] = []

    for row in input:
      data = {
          **self._base_row,
          **row,
          **{
              self.response_field_name: results[row[self.key_field_name]]
          }
      }

      response.append(data)

    return response


class BaseInterfacerFactory():
  UPDATERS_CLASS_PREFIX: str = ''
  UPDATER_REGISTRATIONS_FILE: str = ''

  _builders: Mapping[str, BaseInterfacer] = None

  def __init__(self):
    self._instance = None
    if self.UPDATER_REGISTRATIONS_FILE:
      self.register_interfacers()

  def register_interfacers(self) -> Mapping[str, str]:
    with open(self.UPDATER_REGISTRATIONS_FILE, 'r') as f:
      mappings: Mapping[str, str] = json.load(f)
      builders: Mapping(str, type) = dict()

    for k in mappings:
      full_path = f'{self.UPDATERS_CLASS_PREFIX}{mappings[k]}Builder'.split('.')
      module_name = '.'.join(full_path[0:-1])
      class_name = full_path[-1]
      __import__(module_name)
      module = sys.modules[module_name]
      builders[k.upper()] = getattr(module, class_name)

    self._builders = builders

  def register(cls, product: str, interfacer_class: type):
    cls._builders[product.upper()] = interfacer_class

  def get(cls, spec: Mapping[str, Primitives], project_id: str, dataset: str,
          bq_client: bigquery.Client, **kwargs) -> BaseInterfacer:
    """Returns an initialized Interfacer based on values in `spec`.

    Args:
      spec (Mapping[str, Primitives]): Contains keys with same name as
        parameters required to determine and initialize the Interfacer.
      project_id (str): Project running Taxonomy Wizard server-side.
      dataset (str): Dataset containing taxonomy data.
      bq_client (bigquery.Client): BigQuery API client.
      **kwargs: Other keyword arguments to pass to Builder.

    Raises:
        NotImplementedError: If `spec['product']` contains a Google product
          that does not have an Interfacer implemented.
        KeyError: If `spec['product']` contains an unrecognized value.
    """

    builder: BaseInterfacer = cls._builders[spec['product'].upper()]()
    if not builder:
      raise KeyError(f'Unsupported value for "product": "{spec["product"]}".')

    return builder(spec=spec,
                   project_id=project_id,
                   dataset=dataset,
                   bq_client=bq_client,
                   **kwargs)


class BaseInterfacerBuilder():

  def __init__(self):
    self._instance = None

  @abstractmethod
  def __call__(self, spec: Mapping[str,
                                   Primitives], project_id: str, dataset: str,
               bq_client: bigquery.Client, **kwargs) -> BaseInterfacer:
    """Returns an initialized Interfacer, based on values in spec.

    Must be overriden in child class.

    Args:
      spec (Mapping[str, Primitives]): Has keys of 'product', 'entity_type', and
        'customer_owner_id' to determine and initialize the Interfacer.
      updates (Sequence[UpdatableEntity]): List of updates to be made.
      project_id (str): Project running Taxonomy Wizard server-side.
      dataset (str): Dataset containing taxonomy data.
      bq_client (bigquery.Client): BigQuery API client.
      **kwargs: Placeholder to show that method signature may be overriden.
    """

    pass
