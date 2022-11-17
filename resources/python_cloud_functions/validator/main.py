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

from dataclasses import field
import os
from google.cloud import bigquery
from google import auth
import flask
from sources import RawJsonValidatorSource
from sources.source import ProductSourceFilter, ValidatorProductSource, NamesInput, NamesInput, RequestJson
from sources.campaign_manager.campaign_manager_source import CampaignManagerValidatorSource

_ListSpecsResponseJson = list[dict[str, str]]
_ValidateNamesResponseJson = dict[str, list[NamesInput]]

VALIDATION_RESULTS_TABLE: str = 'validation_results'

_bq_client: bigquery.Client = None


def handle_request(request: flask.Request):
  """Validation of request and creation of task.

  Args:
      request: Request object.

  Returns:
      Successful start response or error response.
  """
  try:
    data: RequestJson = get_json_data(request)

    if data['action'] == 'list_specs':
      return list_specs(data)
    elif data['action'] == 'validate_everything':
      return validate_everything(data)
    elif data['action'] == 'validate_names':
      return validate_names(data)
    else:
      raise ValueError('Invalid value {data["action"]} passed for "action"')
  except Exception as e:
    return str(e), 400, None


def list_specs(data: RequestJson) -> _ListSpecsResponseJson:
  query: str = f"SELECT name FROM `{data['taxonomy_cloud_project_id']}.{data['taxonomy_bigquery_dataset']}.specifications`\n"
  rows = bq_client().query(query).result()
  return [{'name': row['name']} for row in rows]


def validate_everything(data: RequestJson):
  project_id: str = data['taxonomy_cloud_project_id']
  dataset: str = data['taxonomy_bigquery_dataset']

  spec_sets = get_spec_sets(project_id, dataset)

  validation_results = []
  for spec_set in spec_sets:
    validator: ValidatorProductSource = choose_validator(
        spec_set, project_id, dataset)
    names = validator.fetch_values_to_validate()
    just_names = validator.extract_names(names, 'name')
    validated_data = validator.validate_values(just_names)
    results: list[NamesInput] = validator.merge_input_and_output(
        names, validated_data, spec_set)

    validation_results.extend(results)

  persist_results(validation_results, project_id, dataset)


def get_spec_sets(project_id: str, dataset: str):
  client: bigquery.Client = bq_client()
  query = "SELECT\n"\
        "  name AS spec_set_name,\n"\
        "  product,\n"\
        "  customer_owner_id,\n"\
        "  taxonomy_level,\n"\
        "  advertiser_ids,\n"\
        "  campaign_ids,\n"\
        "  start_date,\n"\
        "  end_date,\n"\
        "  validation_query_template,\n"\
       f"FROM `{project_id}.{dataset}.specifications`"
  spec_sets = client.query(query).result()
  return spec_sets


def choose_validator(spec_set, cloud_project_id, bigquery_dataset):
  product = spec_set['product']

  if product == 'Campaign Manager':
    filter = ProductSourceFilter(
        customer_owner_id=spec_set['customer_owner_id'],
        advertiser_ids=spec_set['advertiser_ids'],
        campaign_ids=spec_set['campaign_ids'],
        start_date=spec_set['start_date'],
        end_date=spec_set['end_date'])

    validator: ValidatorProductSource = CampaignManagerValidatorSource(
        spec_set_name=spec_set['spec_set_name'],
        product=spec_set['product'],
        taxonomy_level=spec_set['taxonomy_level'],
        project_id=cloud_project_id,
        bigquery_dataset=bigquery_dataset,
        filter=filter)

  elif product in ('DV360', 'Google Ads', 'SA360'):
    raise NotImplementedError(
        f'Support for "{product}" has not been implemented yet.')

  else:
    raise KeyError(f'Unsupported value for "product" field: "{product}".')
  return validator


def persist_results(data: list[dict[str, str]], project_id, dataset):
  client: bigquery.Client = bq_client()

  job_config = bigquery.LoadJobConfig()
  job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
  job_config.write_disposition = 'WRITE_TRUNCATE'
  job_config.schema = [
      bigquery.SchemaField('spec_set_name', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('product', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('owner_id', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('entity_type', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('entity_id', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('entity_name', 'STRING', mode='REQUIRED'),
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
    print(f'Added rows to table.')


def set_google_cloud_project_env_var(id):
  os.environ['GOOGLE_CLOUD_PROJECT'] = id


def validate_names(data: RequestJson) -> _ValidateNamesResponseJson:
  validator: RawJsonValidatorSource = RawJsonValidatorSource(
      project_id=data['taxonomy_cloud_project_id'],
      bigquery_dataset=data['taxonomy_bigquery_dataset'])

  values = validator.fetch_values_to_validate(data)
  names = validator.extract_names(values)
  validated_data = validator.validate_values(data['spec_name'], names)
  results = validator.merge_input_and_output(values, validated_data)

  return {'results': results}


def get_json_data(request):
  data: dict[str, str | list[NamesInput]] = request.get_json()  # type: ignore
  return data


def bq_client() -> bigquery.Client:
  if not _bq_client:
    credentials, project = auth.default(scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/spreadsheets',
    ])
    _bq_client = bigquery.Client(credentials=credentials, project=project)

  return _bq_client


if __name__ == '__main__':
  pass
