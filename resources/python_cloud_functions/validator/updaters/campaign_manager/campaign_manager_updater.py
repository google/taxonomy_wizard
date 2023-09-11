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
""" Campaign Manager updater. """

from attrs import define, field
from typing import Mapping, Sequence

from google.cloud import bigquery
from googleapiclient import discovery

from base import BaseInterfacerBuilder, Primitives
from updaters.updater import NamesInput, GoogleAPIClientUpdater, DEFAULT_MAX_TRIES

API_NAME = 'dfareporting'
API_VERSION = 'v4'
API_SCOPES = [
    'https://www.googleapis.com/auth/dfareporting',
    'https://www.googleapis.com/auth/dfatrafficking',
    'https://www.googleapis.com/auth/ddmconversions',
]

# 60 qpm, per https://developers.google.com/doubleclick-advertisers/quotas
CM_API_MAX_REQUESTS_PER_MINUTE = 60


@define(auto_attribs=True)
class CampaignManagerUpdater(GoogleAPIClientUpdater):
  """Updater for Campaign Manager."""
  _MAX_BATCH_SIZE: int = field(init=False,
                               default=CM_API_MAX_REQUESTS_PER_MINUTE)
  _MAX_REQUESTS_PER_MINUTE: int = field(init=False,
                                        default=CM_API_MAX_REQUESTS_PER_MINUTE)
  _API_NAME: str = field(init=False, default=API_NAME)
  _API_VERSION: str = field(init=False, default=API_VERSION)
  _API_SCOPES: Sequence[str] = field(init=False, default=API_SCOPES)

  def generate_update_request(self, entity_resource: discovery.Resource,
                              update: NamesInput):
    """Generates a request to update the entity.

    Args:
        entity_resource (discovery.Resource): Service entity resource.
        update (UpdatableEntity): Update info for entity.

    Returns:
        HttpRequest: HTTP request.
    """
    return entity_resource.patch(profileId=self.customer_owner_id,
                                 id=update['key'],
                                 fields='name',
                                 body={'name': update['new_value']})

  def get_entity_resource(self) -> discovery.Resource:
    """Returns the applicable entity resource from the API based on the object's
    `entity_type` attributes.

    Based on the class object's `entity_type` property:
      E.g., service.campaigns(), service.creatives(), etc...

    Raises:
        ValueError: If an invalid value is used for `entity_type`.
    """
    if self.entity_type.upper() == 'CAMPAIGN':
      return self._service.campaigns()
    if self.entity_type.upper() == 'PLACEMENT':
      return self._service.placements()
    if self.entity_type.upper() == 'REMARKETING LIST':
      return self._service.remarketingLists()
    else:
      raise ValueError(
          f'Unsupported value for "entity_type" (aka "Taxonomy level"): "{self.entity_type}".'
      )


class CampaignManagerUpdaterBuilder(BaseInterfacerBuilder):

  def __call__(self, spec: Mapping[str,
                                   Primitives], updates: Sequence[NamesInput],
               project_id: str, dataset: str, bq_client: bigquery.Client,
               access_token: str) -> CampaignManagerUpdater:
    """Returns an initialized Updater based on values in `spec`.

    Args:
      spec (Mapping[str, Primitives]): Has keys of 'product', 'entity_type', and
        'customer_owner_id' to determine and initialize the Updater.
      updates (Sequence[UpdatableEntity]): List of updates to be made.
      project_id (str): Project running Taxonomy Wizard server-side.
      dataset (str): Dataset containing taxonomy data.
      bq_client (bigquery.Client): BigQuery API client.
    """
    if not self._instance:
      self._instance = CampaignManagerUpdater(
          cloud_project=project_id,
          bigquery_dataset=dataset,
          bq_client=bq_client,
          entity_type=spec['entity_type'].upper(),
          customer_owner_id=spec['customer_owner_id'],
          updates=updates,
          access_token=access_token)
    return self._instance
