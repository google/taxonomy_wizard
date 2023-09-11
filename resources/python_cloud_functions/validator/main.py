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
""" Validates/update names submitted according to specified Taxonomy."""

from collections.abc import Sequence
import flask
import logging
from typing import Mapping

from google.cloud import bigquery

from base import NamesInput
import bq_client
from updaters.updater import BaseUpdater, UpdaterFactory
from validators.validator import ProductValidator, RawJsonValidator, ValidatorFactory

_ListSpecsResponseJson = Sequence[dict[str, str]]
_ValidateNamesResponseJson = dict[str, Sequence[NamesInput]]

VALIDATION_RESULTS_TABLE: str = 'validation_results'

_bq_client: bq_client.BqClient = bq_client.BqClient()


def handle_request(request: flask.Request):
  """Validation of request and creation of task.

  Args:
      request: Request object.

  Returns:
      Successful start response or error response.
  """
  try:
    action: str = request.args.get('action')
    payload: Mapping[str:Sequence[NamesInput] | str] = request.get_json()
    project_id: str = payload['taxonomy_cloud_project_id']
    dataset: str = payload['taxonomy_bigquery_dataset']

    if action == 'list_specs':
      return list_specs(project_id, dataset)
    elif action == 'validate_everything':
      return validate_all_specs(project_id, dataset)
    elif action == 'validate_names':
      spec_name: str = payload['spec_name']
      data: Sequence[NamesInput] = payload['data']
      return validate_entity_values(spec_name, data, project_id, dataset)
    elif action == 'update_names':
      spec_name: str = payload['spec_name']
      data: Sequence[NamesInput] = payload['data']
      access_token: str = payload['access_token']
      return update_entity_values(spec_name, data, project_id, dataset,
                                  access_token)
    else:
      raise ValueError(
          f'Invalid (or missing) value for parameter "action": {action}.')
  except Exception as e:
    err = f'Invocation failed with error: {str(e)}'
    logging.error(err)
    return err, 400, None


def list_specs(project_id: str, dataset: str) -> _ListSpecsResponseJson:
  query: str = f'SELECT name FROM `{project_id}.{dataset}.specifications`\n'
  rows = _bq_client.get().query(query).result()
  return [{'name': row['name']} for row in rows]


def validate_all_specs(project_id: str, dataset: str):
  validator_fields ='  advertiser_ids,\n'\
                    '  campaign_ids,\n'\
                    '  min_start_date,\n'\
                    '  max_start_date,\n'\
                    '  min_end_date,\n'\
                    '  max_end_date,\n'\
                    '  validation_query_template,\n'

  specs = get_specifications(project_id, dataset, validator_fields)

  validation_results = []
  for spec in specs:
    validator: ProductValidator = ValidatorFactory().get(
        spec=spec,
        project_id=project_id,
        dataset=dataset,
        bq_client=_bq_client.get())
    results: Sequence[NamesInput] = validator.validate()
    validation_results.extend(results)

  persist_results(validation_results, project_id, dataset)

  return 'Successfully ran validation.', 200, None


def get_specifications(project_id: str,
                       dataset: str,
                       additional_fields: str = '',
                       where_clause: str = 'TRUE'):
  client: bigquery.Client = _bq_client.get()
  query = 'SELECT\n'\
           '  name,\n'\
           '  product,\n'\
           '  customer_owner_id,\n'\
           '  entity_type,\n'\
           f'  {additional_fields}\n'\
           f'FROM `{project_id}.{dataset}.specifications`\n'\
           f'WHERE {where_clause}'
  specs = client.query(query).result()
  return specs


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
    err = f'Errors adding rows to table: {errors}'
    logging.error(err)
    raise Exception(err)
  else:
    logging.info(
        f'Added {len(data)} row{"s" if len(data)!=1 else ""} to table.')


def validate_entity_values(spec_name: str, values: Sequence[NamesInput],
                           project_id: str,
                           dataset: str) -> _ValidateNamesResponseJson:
  validator: RawJsonValidator = RawJsonValidator(cloud_project=project_id,
                                                 bigquery_dataset=dataset,
                                                 bq_client=_bq_client.get(),
                                                 spec_name=spec_name,
                                                 values_to_validate=values,
                                                 base_row_data={})

  results = validator.validate()

  return {'results': results}


def update_entity_values(spec_name: str, updates: Sequence[NamesInput],
                         project_id: str, dataset: str,
                         access_token: str) -> _ValidateNamesResponseJson:

  escaped_spec_name = spec_name.replace("'", "\\'")
  where_clause = f"name = '{escaped_spec_name}'"
  spec = next(get_specifications(project_id, dataset,
                                 where_clause=where_clause))

  updater: BaseUpdater = UpdaterFactory().get(spec=spec,
                                              updates=updates,
                                              project_id=project_id,
                                              dataset=dataset,
                                              bq_client=_bq_client.get(),
                                              access_token=access_token)
  results = updater.apply_updates()

  return {'results': results}


if __name__ == '__main__':
  pass
