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
"""Classes for validation sources. """

from datetime import datetime
from abc import abstractmethod
from typing import Mapping, Sequence
from attrs import define, field
from google.cloud import bigquery

Primitives = str | int | float | bool
NamesInput = dict[str, Primitives]


@define(auto_attribs=True)
class BaseValidatorSource():
  """Base class for all validators. Must be subclassed to be used.

  A 'validator' provides the means to retrieve a list of values and validate
  them against a provided specification.

   `fetch_data_to_validate()`, needs to be implemented by a subclass. Other
   methods may be overridden as needed, but it's unlikely.
  """
  cloud_project: str = field()
  bigquery_dataset: str = field()
  bq_client: bigquery.Client = field()
  spec_name: str = field()
  field_name: str = field()
  base_row_columns: Sequence[str] = field()
  _base_row_data: Mapping[str, Primitives] = field()
  base_row: Mapping[str, Primitives] = field(init=False)

  def __attrs_post_init__(self):
    self.base_row = {
        k: v
        for k, v in self._base_row_data.items()
        if k in self.base_row_columns
    }

  def validate(self) -> Sequence[NamesInput]:
    """Validates data based on the object instance's fields.

    Returns:
        Sequence[NamesInput]: Data with validation messages.
    """
    data_to_validate = self.fetch_data_to_validate()
    unique_values = self._extract_unique_values(data_to_validate)

    query_template: str = self._fetch_validation_query_template()
    pp_query_template: str = self.post_process_query_template(query_template)

    validated_data = self.fetch_validation_results(pp_query_template,
                                                   unique_values)

    results: Sequence[NamesInput] = self._merge_input_and_output(
        data_to_validate, validated_data)

    return results

  @abstractmethod
  def fetch_data_to_validate() -> Sequence[NamesInput]:
    pass

  def _extract_unique_values(self,
                             data: list[Mapping[str, Primitives]]) -> list[str]:
    """Extracts a list of the unique entity names.

    Args:
        data (list[dict[str, Primitives]]): List of input data rows.

    Returns:
        list[str]: List of unique input strings (to be validated).
    """
    names: list[str] = [obj[self.field_name] for obj in data]
    unique_names = list(set(names))
    return unique_names

  def validate_values(self, input: Sequence[str]) -> Mapping[str, str]:
    """Validates names.

    Args:
        input (Sequence[str]): List of input strings to validate.

    Returns:
        Mapping[str, str]: Input strings (key) with validation messages (value).
    """
    query_template: str = self._fetch_validation_query_template()
    query_template_pp: str = self.post_process_query_template(query_template)
    results = self.fetch_validation_results(query_template_pp, input)
    return results

  def post_process_query_template(self, query_template: str) -> str:
    """Performs manipulation of the query template before running validation.

    This method can be overriden by subclasses.

    Args:
        query_template (str): Query template to manipulate.

    Returns:
        str: Manipulated query template.
    """
    return query_template

  def _fetch_validation_query_template(self) -> str:
    """Gets the validation query template for the object instance's `spec_name`.

    Returns:
        str: Validation query template.
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("spec_name", "STRING", self.spec_name)
    ])
    query = "SELECT ANY_VALUE(validation_query_template) AS validation_query_template\n"\
           f"FROM `{self.cloud_project}.{self.bigquery_dataset}.specifications`\n"\
           f"WHERE name = @spec_name"
    job: bigquery.QueryJob = self.bq_client.query(query, job_config)
    validation_query_template: str = next(job.result())[0]
    return validation_query_template

  def fetch_validation_results(self, query: str,
                               input: Sequence[str]) -> Mapping[str, str]:
    """Runs the query that will generate validation messagse for the input.

    Args:
        query (str): Query to run.
        input (Sequence[str]): List of values to validate.

    Returns:
        Mapping[str, str]: Input strings (key) with validation messages (value).
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("entity_names", "STRING", input),
    ])

    rows = self.bq_client.query(query, job_config).result()
    return {row['name']: row['validation_message'] for row in rows}

  def _merge_input_and_output(
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
          **self.base_row,
          **row,
          **{
              'validation_message': results[row[self.field_name]]
          }
      }

      response.append(data)

    return response


@define(auto_attribs=True)
class RawJsonValidatorSource(BaseValidatorSource):
  """Validator for when raw values are passed in via  JSON."""
  _values_to_validate: list[NamesInput]
  # Overriden from Base class
  field_name: str = field(default='value')
  base_row_columns: Sequence[str] = field(default='name')

  def fetch_data_to_validate(self):
    return self._values_to_validate

  def post_process_query_template(self, query_template: str):
    return query_template.replace('@entity_names', 'UNNEST(@entity_names)')


@define(auto_attribs=True)
class ProductSourceFilter():
  """Specifications filters for when retrieving from a product-based source."""
  customer_owner_id: str = field()
  advertiser_ids: Sequence[str | int] = field()
  campaign_ids: Sequence[str | int] = field()
  min_start_date: datetime = field()
  max_start_date: datetime = field()
  min_end_date: datetime = field()
  max_end_date: datetime = field()


@define(auto_attribs=True)
class ValidatorProductSource(BaseValidatorSource):
  """Base class for validators that pull data from Google advertising products."""
  product: str = field()
  entity_type: str = field()
  filter: ProductSourceFilter = field()

  # Overriden from Base class
  field_name: str = field(default='entity_value')
  base_row_columns: Sequence[str] = field(
      default=['name', 'product', 'customer_owner_id', 'entity_type'])

  def post_process_query_template(self, query_template: str):
    return query_template.replace('@entity_names', 'UNNEST(@entity_names)')
