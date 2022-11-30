# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https: // www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
""" Validates names submitted according to specified Taxonomy."""

from collections.abc import Sequence
import flask
import os
from google.cloud import bigquery
from sources.source import RawJsonValidatorSource, ProductSourceFilter, ValidatorProductSource, NamesInput, NamesInput
from sources.campaign_manager.campaign_manager_source import CampaignManagerValidatorSource
import bq_client

_ListSpecsResponseJson = list[dict[str, str]]
_ValidateNamesResponseJson = dict[str, list[NamesInput]]

VALIDATION_RESULTS_TABLE: str = 'validation_results'

_bq_client: bq_client.BqClient = bq_client.BqClient(default_scopes=[
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/dfareporting',
    'https://www.googleapis.com/auth/dfatrafficking',
    'https://www.googleapis.com/auth/ddmconversions',
])


def handle_request(request: flask.Request):
  """Validation of request and creation of task.

  Args:
      request: Request object.

  Returns:
      Successful start response or error response.
  """
  try:
    action: str = request.args.get("action")
    project_id: str = request.args.get("taxonomy_cloud_project_id")
    dataset: str = request.args.get("taxonomy_bigquery_dataset")

    if action == 'list_specs':
      return list_specs(project_id, dataset)
    elif action == 'validate_everything':
      return validate_all_specs(project_id, dataset)
    elif action == 'validate_names':
      spec_name: str = request.args.get("spec_name")
      data: list[NamesInput] = request.get_json()
      return validate_entity_values(spec_name, data, project_id, dataset)
    else:
      raise ValueError(
          f'Invalid or missing value for parameter "action": {data["action"]}.')
  except Exception as e:
    print(f"Invocation failed with error: {str(e)}")
    return str(e), 400, None


def list_specs(project_id: str, dataset: str) -> _ListSpecsResponseJson:
  query: str = f"SELECT name FROM `{project_id}.{dataset}.specifications`\n"
  rows = _bq_client.get().query(query).result()
  return [{'name': row['name']} for row in rows]


def validate_all_specs(project_id: str, dataset: str):
  specs = get_specifications(project_id, dataset)

  validation_results = []
  for spec in specs:
    validator: ValidatorProductSource = choose_validator(
        spec, project_id, dataset)
    results: Sequence[NamesInput] = validator.validate()
    validation_results.extend(results)

  persist_results(validation_results, project_id, dataset)

  return "Successfully ran validation.", 200, None


def get_specifications(project_id: str, dataset: str):
  client: bigquery.Client = _bq_client.get()
  query = "SELECT\n"\
        "  name,\n"\
        "  product,\n"\
        "  customer_owner_id,\n"\
        "  entity_type,\n"\
        "  advertiser_ids,\n"\
        "  campaign_ids,\n"\
        "  min_start_date,\n"\
        "  max_start_date,\n"\
        "  min_end_date,\n"\
        "  max_end_date,\n"\
        "  validation_query_template,\n"\
       f"FROM `{project_id}.{dataset}.specifications`"
  specs = client.query(query).result()
  return specs


def choose_validator(spec, cloud_project_id, bigquery_dataset):
  product = spec['product']

  if product == 'Campaign Manager':
    filter = ProductSourceFilter(customer_owner_id=spec['customer_owner_id'],
                                 advertiser_ids=spec['advertiser_ids'],
                                 campaign_ids=spec['campaign_ids'],
                                 min_start_date=spec['min_start_date'],
                                 max_start_date=spec['max_start_date'],
                                 min_end_date=spec['min_end_date'],
                                 max_end_date=spec['max_end_date'])

    validator: ValidatorProductSource = CampaignManagerValidatorSource(
        cloud_project=cloud_project_id,
        bigquery_dataset=bigquery_dataset,
        bq_client=_bq_client.get(),
        spec_name=spec['name'],
        product=spec['product'],
        entity_type=spec['entity_type'],
        base_row_data=spec,
        filter=filter)

  elif product in ('DV360', 'Google Ads', 'SA360'):
    raise NotImplementedError(
        f'Support for "{product}" has not been implemented yet.')

  else:
    raise KeyError(f'Unsupported value for "product" field: "{product}".')
  return validator


def persist_results(data: list[dict[str, str]], project_id, dataset):
  client: bigquery.Client = _bq_client.get()

  job_config = bigquery.LoadJobConfig()
  job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
  job_config.write_disposition = 'WRITE_TRUNCATE'
  job_config.schema = [
      bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('product', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('customer_owner_id', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('entity_type', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('entity_id', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('entity_value', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('validation_message', 'STRING', mode='REQUIRED')
  ]

  table_ref: bigquery.TableReference = bigquery.TableReference(
      bigquery.DatasetReference(project_id, dataset), VALIDATION_RESULTS_TABLE)

  job: bigquery.LoadJob = client.load_table_from_json(data,
                                                      table_ref,
                                                      job_config=job_config)

  errors = job.result().errors

  if errors:
    print(f'Errors adding rows to table: {errors}')
  else:
    print(f'Added {len(data)} row{"s" if len(data)!=1 else ""} to table.')


def set_google_cloud_project_env_var(id):
  os.environ['GOOGLE_CLOUD_PROJECT'] = id


def validate_entity_values(spec_name: str, values: list[NamesInput],
                           project_id: str,
                           dataset: str) -> _ValidateNamesResponseJson:
  validator: RawJsonValidatorSource = RawJsonValidatorSource(
      cloud_project=project_id,
      bigquery_dataset=dataset,
      bq_client=_bq_client.get(),
      spec_name=spec_name,
      values_to_validate=values,
      base_row_data={})

  results = validator.validate()

  return {'results': results}


if __name__ == '__main__':
  pass
