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
"""Classes for validators. """

from abc import abstractmethod
from attrs import define, field
from datetime import datetime
from typing import Mapping, Sequence

from google.cloud import bigquery

from base import BaseInterfacer, BaseInterfacerFactory, NamesInput, Primitives
VALIDATOR_REGISTRATIONS_FILE = 'validators/validators.json'
VALIDATORS_CLASS_PREFIX = 'validators.'


@define(auto_attribs=True)
class BaseValidator(BaseInterfacer):
  """Base class for all validators. Must be subclassed to be used.

  A 'validator' provides the means to retrieve a list of values and validate
  them against a provided specification.

   `fetch_data_to_validate()`, needs to be implemented by a subclass. Other
   methods may be overridden as needed, but it's unlikely.

  Attributes:
    spec_name (str): Name of spec to validate against.
    field_name (str): Name of the entity's field to validate.
  """
  # Required:
  spec_name: str = field(kw_only=True)
  key_field_name: str = field(default='name', kw_only=True)

  # Overridden:
  response_field_name: str = field(default='validation_message')

  def validate(self) -> Sequence[NamesInput]:
    """Validates data based on the object instance's fields.

    Returns:
        Sequence[NamesInput]: Data with validation messages.
    """
    data_to_validate = self.fetch_data_to_validate()
    unique_values = self._extract_unique_values(data_to_validate)

    query_template: str = self._fetch_validation_query_template()
    if not query_template:
      raise ValueError(f'Could not retrieve spec with name "{self.spec_name}".')
    pp_query_template: str = self.post_process_query_template(query_template)

    validated_data = self.fetch_validation_results(pp_query_template,
                                                   unique_values)

    results: Sequence[NamesInput] = self.merge_input_and_output(
        data_to_validate, validated_data)

    return results

  @abstractmethod
  def fetch_data_to_validate() -> Sequence[NamesInput]:
    """Fetches data to validate.

    Returns:
        Sequence[NamesInput]: Data to validate.
    """
    pass

  def _extract_unique_values(self,
                             data: list[Mapping[str, Primitives]]) -> list[str]:
    """Extracts a list of the unique entity names.

    Args:
        data (list[dict[str, Primitives]]): List of input data rows.

    Returns:
        list[str]: List of unique input strings (to be validated).
    """
    names: list[str] = [obj[self.key_field_name] for obj in data]
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
    """Runs the query that will generate validation messages for the input.

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


@define(auto_attribs=True)
class RawJsonValidator(BaseValidator):
  """Validator for when raw values are passed in via JSON."""
  _values_to_validate: list[NamesInput] = field(kw_only=True)
  # Overriden from Base class
  key_field_name: str = field(default='value', kw_only=True)
  base_row_columns: Sequence[str] = field(default=list(['name']), kw_only=True)

  def fetch_data_to_validate(self):
    return self._values_to_validate

  def post_process_query_template(self, query_template: str):
    return query_template.replace('@entity_names', 'UNNEST(@entity_names)')


@define(auto_attribs=True)
class ProductValidatorFilter():
  """Filters for getting entities from advertising product-based validators.

  Note: Not all attributes are used by all entities.

  Attributes:
    customer_owner_id (str): Profile id for CM, account id for DV360, etc...
    advertiser_ids (Sequence[str | int]): List of advertiser ids.
    campaign_ids (Sequence[str | int]): List of campaign ids.
    floodlightActivityId (str): Floodlight activity id.
    min_start_date (datetime): Min start date.
    max_start_date (datetime): Max start date.
    min_end_date (datetime): Min end date.
    max_end_date (datetime): Max end date.
"""
  customer_owner_id: str = field(kw_only=True)
  advertiser_ids: Sequence[str | int] = field(kw_only=True)
  campaign_ids: Sequence[str | int] = field(kw_only=True)
  min_start_date: datetime = field(kw_only=True)
  max_start_date: datetime = field(kw_only=True)
  min_end_date: datetime = field(kw_only=True)
  max_end_date: datetime = field(kw_only=True)


@define(auto_attribs=True)
class ProductValidator(BaseValidator):
  """Base class for validators that pull data from advertising products."""
  product: str = field(kw_only=True)
  entity_type: str = field(kw_only=True)
  filter: ProductValidatorFilter = field(kw_only=True)

  # Overriden from Base class
  key_field_name: str = field(default='entity_value')
  base_row_columns: Sequence[str] = field(
      default=['name', 'product', 'customer_owner_id', 'entity_type'])

  def post_process_query_template(self, query_template: str):
    return query_template.replace('@entity_names', 'UNNEST(@entity_names)')


class ValidatorFactory(BaseInterfacerFactory):
  VALIDATORS_CLASS_PREFIX = VALIDATORS_CLASS_PREFIX
  VALIDATOR_REGISTRATIONS_FILE = VALIDATOR_REGISTRATIONS_FILE
