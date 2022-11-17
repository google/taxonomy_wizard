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
""" Base class for validation sources. """

from abc import abstractmethod
from typing import Mapping, Sequence
from attrs import define, field
from google.cloud import bigquery

NamesInput = dict[str, str]
RequestJson = dict[str, str | list[NamesInput]]


@define(auto_attribs=True)
class BaseValidatorSource():
  """Base class for all validators. Must be subclassed to be used.

  A 'validator' provides the means to retrieve a list of values and validate
  them against a provided specification.

   `fetch_data_to_validate()`, needs to be implemented by a subclass. Other
   methods may be overridden as needed, but it's unlikely.
  """
  project: str = field()
  dataset: str = field()
  field_name: str = field()
  spec_columns_to_persist: list[str] = field()
  bq_client: bigquery.Client = field()

  def validate_from_spec(self, spec: Mapping[str, any]) -> Sequence[NamesInput]:
    data_to_validate = self.fetch_data_to_validate()
    unique_values = self._extract_unique_values(data_to_validate)

    query_template: str = self._fetch_validation_query_template(spec['name'])
    pp_query_template: str = self._post_process_query_template(query_template)

    validated_data = self.fetch_validation_results(pp_query_template,
                                                   unique_values)

    results: Sequence[NamesInput] = self._merge_input_and_output(
        data_to_validate, validated_data, spec)

    return results

  @abstractmethod
  def fetch_data_to_validate():
    pass

  def _extract_unique_values(self, data: list[dict[str,
                                                   str | int]]) -> list[str]:
    """Extracts a list of the unique entity names."""
    names: list[str] = [obj[self.field_name] for obj in data]
    unique_names = list(set(names))
    return unique_names

  def validate_values(self, spec_name: str,
                      input: list[any]) -> Mapping[str, str]:
    """Validates names."""
    query_template: str = self._fetch_validation_query_template(spec_name)
    query_template_v2: str = self._post_process_query_template(query_template)
    results = self.fetch_validation_results(query_template_v2, input)
    return results

  def _post_process_query_template(self, query_template: str):
    return query_template

  def _fetch_validation_query_template(self, spec_name: str) -> str:
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("spec_name", "STRING", spec_name)
    ])
    query = "SELECT ANY_VALUE(validation_query_template) AS validation_query_template\n"\
           f"FROM `{self.project}.{self.dataset}.specifications`\n"\
           f"WHERE name = @spec_name"
    job: bigquery.QueryJob = self.bq_client.query(query, job_config)
    validation_query_template: str = next(job.result())[0]
    return validation_query_template

  def fetch_validation_results(self, query, input) -> Mapping[str, str]:
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("entity_names", "STRING", input),
    ])

    rows = self.bq_client.query(query, job_config).result()
    return {row['name']: row['validation_message'] for row in rows}

  def _merge_input_and_output(
      self, input: list[NamesInput], results,
      spec: Mapping[str, any] = dict()) -> list[NamesInput]:
    """ Adds spec set info to validation results."""

    response: list[NamesInput] = []

    for row in input:
      data = self._merge_row(results, spec, row)
      response.append(data)

    return response

  def _merge_row(self, results, spec, row):
    data = row.copy()
    data['validation_message'] = results[row['value']]
    for col in self.spec_columns_to_persist:
      data[col] = spec[col]
    return data


@define(auto_attribs=True)
class ProductSourceFilter():
  customer_owner_id: str = field()
  advertiser_ids: list = field()
  campaign_ids: list = field()
  min_start_date: str = field()
  max_start_date: str = field()
  min_end_date: str = field()
  max_end_date: str = field()


from sources.source import BaseValidatorSource
from attrs import define, field


@define(auto_attribs=True)
class RawJsonValidatorSource(BaseValidatorSource):
  _values_to_validate: list[dict[str, any]]
  field_name: str = field(default='value')
  spec_columns_to_persist: list[str] = field(default='spec_name')

  def fetch_data_to_validate(self):
    return self._values_to_validate

  def _post_process_query_template(self, query_template: str):
    return query_template.replace('@entity_names', 'UNNEST(@entity_names)')


@define(auto_attribs=True)
class ValidatorProductSource(BaseValidatorSource):
  """Base class for validators that pull data from Google advertising products."""
  spec_name: str = field()
  product: str = field()
  taxonomy_level: str = field()
  filter: ProductSourceFilter = field()

  # Overriden from Base class
  field_name: str = field(default='name')
  spec_columns_to_persist: list[str] = field(
      default=['name', 'product', 'owner_id', 'entity_type'])

  def _post_process_query_template(self, query_template: str):
    return query_template.replace('@entity_names', 'UNNEST(@entity_names)')
