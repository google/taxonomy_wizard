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
""" Campaign Manager validator. """

from attrs import define
from typing import Any, Mapping, Sequence
from datetime import datetime

from google import auth
from googleapiclient import discovery, http
from google.cloud import bigquery

from base import BaseInterfacerBuilder, NamesInput, Primitives
from validators.validator import ProductValidator, ProductValidatorFilter

API_NAME = 'dfareporting'
API_VERSION = 'v4'
API_SCOPES = [
    'https://www.googleapis.com/auth/dfareporting',
    'https://www.googleapis.com/auth/dfatrafficking',
    'https://www.googleapis.com/auth/ddmconversions',
]
_BQ_DATE_FORMAT = '%Y-%m-%d'


@define(auto_attribs=True)
class CampaignManagerValidator(ProductValidator):

  def fetch_data_to_validate(self) -> Sequence[str]:
    """ Fetches the data to validate based on the object properties."""
    if self.entity_type == 'Campaign':
      return self._fetch_entity_values(
          root="campaigns",
          fields="id, name, startDate, endDate",
          request_params={
              'advertiserIds': self.filter.advertiser_ids,
              'archived': False
          },
          filter_function=self._filter_campaign_row)
    elif self.entity_type == 'Placement':
      return self._fetch_entity_values(
          root="placements",
          fields="id, name",
          request_params={
              'advertiserIds': self.filter.advertiser_ids,
              'campaignIds': self.filter.campaign_ids,
              'minStartDate': self.filter.min_start_date,
              'maxStartDate': self.filter.max_start_date,
              'minEndDate': self.filter.min_end_date,
              'maxEndDate': self.filter.max_end_date,
          },
          filter_function=self._filter_placement_row)
    else:
      raise ValueError(
          f'Unsupported value for "entity_type" (aka "Taxonomy level"): "{self.entity_type}".'
      )

  def _fetch_entity_values(self, root: str, fields: str, request_params,
                           filter_function) -> Sequence[NamesInput]:
    """Fetches filtered values from the specified entity.

    Args:
        root (str): Name of entity root.
        fields (str): Fields to return from the entity root.
        request_params (_type_): Parameters for the entity `list` request.
            `profileId`, `fields`, and `sortField` are passed by default based
            on object member values.
        filter_function (_type_): Function to filter the entity rows by.
            Must return an object with the following format:
                `{entity_id: str, entity_value: str}`

    Returns:
        Sequence[NamesInput]: An array of filtered values.
    """
    entity: discovery.Resource = self.fetch_entity_from_service(root)

    request: http.HttpRequest = entity.list(
        profileId=self.filter.customer_owner_id,
        fields=f'{root}({fields})',
        sortField='NAME',
        **request_params)

    return self.filter_paged_request(root, filter_function, entity, request)

  def filter_paged_request(self, root, filter_function, entity, request):
    output: Sequence[NamesInput] = []

    while True:
      response = request.execute()

      for row in response[root]:
        if filtered_row := filter_function(row):
          output.append(filtered_row)

      if root in response and 'nextPageToken' in response:
        request = entity.list_next(request, response)
      else:
        break

    return output

  def _filter_campaign_row(self, row):
    if (self.filter.min_start_date and self._from_bigquery_date(
        row['startDate']) < self.filter.min_start_date):
      return None
    if (self.filter.max_start_date and self._from_bigquery_date(
        row['startDate']) > self.filter.max_start_date):
      return None
    if self.filter.min_end_date and self._from_bigquery_date(
        row['endDate']) < self.filter.min_end_date:
      return None
    if self.filter.max_end_date and self._from_bigquery_date(
        row['endDate']) > self.filter.max_end_date:
      return None
    return {
        'entity_id': row['id'],
        'entity_value': row['name'],
    }

  def _from_bigquery_date(self, date_value: str) -> datetime.date:
    return datetime.strptime(date_value,
                             _BQ_DATE_FORMAT).date() if date_value else None

  def _filter_placement_row(self, row):
    return {
        'entity_id': row['id'],
        'entity_value': row['name'],
    }

  def fetch_entity_from_service(self, root_name: str) -> discovery.Resource:
    service: discovery.Resource = self._build_service()
    return getattr(service, root_name)()

  def _build_service(self) -> discovery.Resource:
    credentials, _ = auth.default(scopes=API_SCOPES)
    service = discovery.build(API_NAME, API_VERSION, credentials=credentials)
    return service


class CampaignManagerValidatorBuilder(BaseInterfacerBuilder):

  def __call__(self, spec: Mapping[str,
                                   Primitives], project_id: str, dataset: str,
               bq_client: bigquery.Client) -> CampaignManagerValidator:
    if not self._instance:
      filter = ProductValidatorFilter(
          customer_owner_id=spec['customer_owner_id'],
          advertiser_ids=spec['advertiser_ids'],
          campaign_ids=spec['campaign_ids'],
          min_start_date=spec['min_start_date'],
          max_start_date=spec['max_start_date'],
          min_end_date=spec['min_end_date'],
          max_end_date=spec['max_end_date'])
      self._instance = CampaignManagerValidator(cloud_project=project_id,
                                                bigquery_dataset=dataset,
                                                bq_client=bq_client,
                                                spec_name=spec['name'],
                                                product=spec['product'],
                                                entity_type=spec['entity_type'],
                                                base_row_data=spec,
                                                filter=filter)
    return self._instance
