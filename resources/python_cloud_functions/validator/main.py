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
# Validates names submitted according to specified Taxonomy.


import json
import os
from typing import Any
from google.cloud import bigquery  # type: ignore
from google import auth
import flask


#TODO(blevitan): Refactor as singleton.
def get_bigquery_client() -> bigquery.Client:
  credentials, project = auth.default(
      scopes=[
          'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/bigquery',
          'https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/spreadsheets',
      ]
  )
  return bigquery.Client(credentials=credentials, project=project)


_NamesMatrixJson = dict[str, str]
_RequestJson = dict[str, str | list[_NamesMatrixJson]]
_ListSpecsResponseJson = list[dict[str, str]]
_ValidateNamesResponseJson = dict[str, list[_NamesMatrixJson]]


def handle_request(request: flask.Request):
  """Validation of request and creation of task.

  Args:
      request: Request object.

  Returns:
      Successful start response or error response.
  """
  try:
    data: _RequestJson = get_json_data(request)
    set_google_cloud_project_env_var(data['taxonomy_cloud_project_id'])

    if data['action'] == 'list_specs':
      return list_specs(data)
    elif data['action'] == 'validate_names':
      return validate_names(data)
    else:
      raise ValueError('Invalid value {data["action"]} passed for "action"')
  except Exception as e:
    return str(e), 400, None


def list_specs(data: _RequestJson) -> _ListSpecsResponseJson:
  query: str = f"SELECT name FROM `{data['taxonomy_cloud_project_id']}.{data['taxonomy_bigquery_dataset']}.specifications`\n"
  rows = get_bigquery_client().query(query).result()
  return [{'name': row['name']} for row in rows]


def validate_names(data: _RequestJson) -> _ValidateNamesResponseJson:
  names: list[str] = get_entity_names(data)
  names_subclause: str = get_names_subclause(names)
  validation_query: str = get_validation_query(data)
  immediate_statement: str = get_immediate_statement(validation_query,
                                                      names_subclause)
  validated_data = get_validation_data(immediate_statement)
  response = format_data_for_response(data['names'],
                                      validated_data)
                                      # type: ignore
  return {'results': response}


def set_google_cloud_project_env_var(id):
  os.environ['GOOGLE_CLOUD_PROJECT'] = id


def get_json_data(request):
  data: dict[str, str | list[_NamesMatrixJson]] = request.get_json()
  # type: ignore
  return data


def get_entity_names(data) -> list[str]:
  #TODO(blevitan): strip matrix info from JSON
  names_matrix: list[dict[str, str | int]] = data['names']
  names: list[str] = [obj['value'] for obj in names_matrix]  # type: ignore

  unique_names = list(set(names))
  return unique_names


def get_names_subclause(names):
  return f'UNNEST(ARRAY<STRING>{names})'


def get_validation_query(data) -> str:
  client: bigquery.Client = get_bigquery_client()
  query = "SELECT MAX(validation_query_template) AS validation_query_template\n"\
      + f"FROM `{data['taxonomy_cloud_project_id']}.{data['taxonomy_bigquery_dataset']}.specifications`\n"\
      + f"WHERE name = '{data['spec_name']}'"
  validation_query_template: str = next(client.query(query).result())[0]
  return validation_query_template


def get_immediate_statement(validation_query, entity_names: str):
  double_quote_escaped_entity_names = entity_names.replace('"', '\"')
  # TODO(blevitan): Get this working instead of python string replace
  # return f'EXECUTE IMMEDIATE """{validation_query}"""\n' +\
  #        f'USING "{double_quote_escaped_entity_names}" AS entity_names'
  return validation_query.replace('@entity_names', double_quote_escaped_entity_names)


def get_validation_data(query: str) -> dict[str, str]:
  rows = get_bigquery_client().query(query).result()
  return {row['name']: row['validation_message'] for row in rows}


def format_data_for_response(input: list[_NamesMatrixJson],
                             output: dict[str, str]) -> list[_NamesMatrixJson]:

  response: list[_NamesMatrixJson] = []
  for row in input:
    data = {
        'value': row['value'],
        'row': row['row'],
        'column': row['column'],
        'validation_message': output[row['value']]
    }
    response.append(data)

  return response


if __name__ == '__main__':
  pass
