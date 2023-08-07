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
"""Factory to create for updaters. """

from typing import Any, Dict, Mapping, Sequence
from updaters.campaign_manager.campaign_manager_updater import CampaignManagerUpdater
from updaters.updater import BaseUpdater, GoogleAPIClientUpdater

from google.cloud import bigquery

from base import NamesInput, Primitives


class UpdaterFactory:

  @classmethod
  def get_updater(cls, spec: Mapping[str, Primitives],
                  updates: Sequence[NamesInput], project_id: str, dataset: str,
                  bq_client: bigquery.Client) -> BaseUpdater:
    """Returns an initialized Validator based on values in `spec`.


    Args:
      spec (Mapping[str, Primitives]): Has keys of 'product', 'entity_type', and
        'customer_owner_id' to determine and initialize the Validator.
      updates (Sequence[UpdatableEntity]): List of updates to be made.
      project_id (str): Project running Taxonomy Wizard server-side.
      dataset (str): Dataset containing taxonomy data.
      bq_client (bigquery.Client): BigQuery API client.

    Raises:
        NotImplementedError: If `spec['product']` contains a Google product
          that does not have a Validator implemented.
        KeyError: If `spec['product']` contains an unrecognized value.
    """
    product = spec['product']

    if product == 'Campaign Manager':
      validator: GoogleAPIClientUpdater = CampaignManagerUpdater(
          cloud_project=project_id,
          bigquery_dataset=dataset,
          bq_client=bq_client,
          entity_type=spec['entity_type'].upper(),
          customer_owner_id=spec['customer_owner_id'],
          updates=updates)

    elif product in ('DV360', 'Google Ads', 'SA360'):
      raise NotImplementedError(
          f'Support for "{product}" has not been implemented yet.')

    else:
      raise KeyError(f'Unsupported value for "product" field: "{product}".')
    return validator
