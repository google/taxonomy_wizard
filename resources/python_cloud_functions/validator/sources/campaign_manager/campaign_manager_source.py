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
""" Campaign Manager validator source. """

from attrs import define
from sources.source import ValidatorProductSource
from googleapiclient import discovery, http
from google import auth

API_NAME = 'dfareporting'
API_VERSION = 'v4'
API_SCOPES = [
    'https://www.googleapis.com/auth/dfareporting',
    'https://www.googleapis.com/auth/dfatrafficking',
    'https://www.googleapis.com/auth/ddmconversions'
]


@define(auto_attribs=True)
class CampaignManagerValidatorSource(ValidatorProductSource):

  def fetch_data_to_validate(self):
    """ Fetches the data to validate based on the object properties."""
    entity_level, entity_name = self._choose_entity_level()

    request: http.HttpRequest = entity_level.list(
        profileId=self.filter.customer_owner_id,
        advertiserIds=self.filter.advertiser_ids,
        fields='id, name, start_date, end_date', <-----ERROR HERE
        archived=False,
        sortField='NAME')

    output: list[str] = []
    while True:
      response = request.execute()

      for row in response[entity_name]:
        if filtered_row := self._filter_row(row):
          output.append(filtered_row)

      if response[entity_name] and response['nextPageToken']:
        request = entity_level.list_next(request, response)
      else:
        break

    return output

  def _filter_row(self, row):
    if self.filter.min_start_date and row[
        'start_date'] < self.filter.min_start_date:
      return
    if self.filter.max_start_date and row[
        'start_date'] > self.filter.min_start_date:
      return
    if self.filter.min_end_date and row['end_date'] < self.filter.min_end_date:
      return
    if self.filter.max_end_date and row['end_date'] > self.filter.min_end_date:
      return
    data = {
        'entity_id': row['id'],
        'entity_name': row['name'],
    }

  def _choose_entity_level(self):
    service: discovery.Resource = self._build_service()
    if self.taxonomy_level == 'Campaign':
      entity_level = service.campaigns()
      entity_name = 'campaign'
    else:
      raise ValueError(
          f'Unsupported value for "taxonomy_level": "{self.taxonomy_level}".')
    return entity_level, entity_name

  def _build_service(self) -> discovery.Resource:
    credentials, _ = auth.default(scopes=API_SCOPES)
    # http = credentials.authorize(http=httplib2.Http())
    # service = discovery.build(API_NAME, API_VERSION, http=http)
    service = discovery.build(API_NAME, API_VERSION, credentials=credentials)
    return service
