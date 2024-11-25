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
# Generates set of Taxonomy Specs based on JSON received from template Sheet.

import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Mapping, OrderedDict, Sequence
import flask
from taxonomy import Dimension, Field, Specification, SpecificationSet
import bq_client

# TODO: Replace `Any` with more specific type info.
RequestJson = dict[str, Any]

_SUCCESS_MESSAGE = 'Successfully generated tables.'
_ISO_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

_bq_client: bq_client.BqClient = bq_client.BqClient(default_scopes=[
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/spreadsheets',
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
    payload: Mapping[str:Sequence[RequestJson] | str] = request.get_json()
    project_id: str = payload['taxonomy_cloud_project_id']
    dataset: str = payload['taxonomy_bigquery_dataset']

    spec_data: RequestJson = payload['data']

    # TODO: Figure out why both these need to be set.
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
    os.environ['GCP_PROJECT'] = project_id

    spec_set = create_objects(spec_data, project_id, dataset)
    response = push_to_database(action, spec_set)
  except Exception as e:
    return str(e), 400, None
  return response


def create_objects(data: RequestJson, project_id: str, dataset: str):
  """Generates Tables from JSON request data."""
  print("Creating objects from request...")
  print(data)

  fields_json, specs_json, dimensions_json = get_json_objects(data)

  fields = create_taxonomy_fields(fields_json, project_id, dataset)

  dims_for_specs: dict[str, OrderedDict[str, Dimension]] =\
      create_taxonomy_dimensions(dimensions_json, fields)

  return create_taxonomy_spec_set(specs_json, dims_for_specs, fields,
                                  project_id, dataset)


def get_json_objects(data):
  fields_json = []
  specs_json = []
  dimensions_json = []

  for data_array in data:
    if data_array['type'] == 'TaxonomyField':
      fields_json = data_array['data']
    elif data_array['type'] == 'TaxonomySpec':
      specs_json = data_array['data']
    elif data_array['type'] == 'TaxonomyDimension':
      dimensions_json = data_array['data']
    else:
      raise Exception(
          f"Error creating objects. Invalid request value '{data_array['type']}' for 'data[].type'."
      )
  return fields_json, specs_json, dimensions_json


def push_to_database(action: str, spec_set: SpecificationSet):
  """Pushes Specification Set to Database. Updating/deleting per action."""
  if action == 'overwrite':
    spec_set.create_in_bigquery()
    return {'response': _SUCCESS_MESSAGE}, 200, None
  else:
    return {'response': f"Invalid value '{action}' for 'action'."}, 400, None


def create_taxonomy_fields(fields_json, cloud_project_id, bigquery_dataset)\
        -> dict[str, Field]:
  fields: dict[str, Field] = {}

  for json in fields_json:
    field = Field(name=json['name'],
                  is_freeform_text=json['is_freeform_text'],
                  dictionary_url=json['dictionary_url'],
                  dictionary_sheet=json['dictionary_sheet'],
                  dictionary_range=json['dictionary_range'],
                  cloud_project_id=cloud_project_id,
                  bigquery_dataset=bigquery_dataset,
                  bq_client=_bq_client.get())
    fields[json['name']] = field

  return fields


def create_taxonomy_dimensions(dimensions_json: Any,
                               fields: dict[str, Field])\
        -> dict[str, OrderedDict[str, Dimension]]:
  dims_for_specs: defaultdict[str, OrderedDict[str, Dimension]] =\
      defaultdict(lambda: OrderedDict())
  regex_prefixes: dict[str, str] = {}

  dim_json_sorted = sorted(dimensions_json,
                           key=lambda d: int(d['prefix_index']))

  last_indexes = get_last_indexes(dim_json_sorted)

  for json in dim_json_sorted:
    dim, regex_prefix, regex_suffix = create_dimension(fields, last_indexes,
                                                       regex_prefixes, json)

    spec_name = json['taxonomy_spec_name']
    dims_for_specs[spec_name][dim.name] = dim
    regex_prefixes[spec_name] = f'{regex_prefix}(?:{regex_suffix}'

  return dims_for_specs


def create_dimension(fields, last_indexes, regex_prefixes, json):
  spec_name = json['taxonomy_spec_name']
  field: Field = fields[json['field_name']]
  escaped_end_delimiter: str = re.escape(json['end_delimiter'])

  if spec_name not in regex_prefixes:
    regex_prefixes[spec_name] = "CONCAT(r'^"
  regex_prefix = regex_prefixes[spec_name]

  if not json['end_delimiter']:
    if not int(json['prefix_index']) == last_indexes[spec_name]:
      regex_suffix = f"', D_{json['prefix_index']}.id, ')"
      requires_crossjoin_validation = True
    else:
      regex_suffix = '.*)$'
      requires_crossjoin_validation = False
  else:
    regex_suffix = f'[^{escaped_end_delimiter}]*?){escaped_end_delimiter}'
    requires_crossjoin_validation = False

  if int(json['prefix_index']) == last_indexes[spec_name]:
    extra_data_regex = f"{regex_prefix}(?:{regex_suffix}(.*)$')"
  else:
    extra_data_regex = ''

  dim = Dimension(name=field.normalized_name,
                  index=int(json['prefix_index']),
                  end_delimiter=json['end_delimiter'],
                  field_spec=field,
                  regex_match_expression=f"{regex_prefix}({regex_suffix}')",
                  extra_data_regex=extra_data_regex,
                  requires_crossjoin_validation=requires_crossjoin_validation)

  return dim, regex_prefix, regex_suffix


def get_last_indexes(dim_json_sorted):
  last_index: defaultdict[str, int] = defaultdict(lambda: -1)
  for json in dim_json_sorted:
    spec_name = json['taxonomy_spec_name']

    if int(json['prefix_index']) > last_index[spec_name]:
      last_index[spec_name] = int(json['prefix_index'])
  return last_index


def create_taxonomy_spec_set(specs_json: Any,
                             dims_for_specs: dict[str, OrderedDict[str,
                                                                   Dimension]],
                             fields: dict[str, Field], cloud_project_id: str,
                             bigquery_dataset: str):
  specs: dict[str, Specification] = dict()

  for json in specs_json:
    if json['name'] in dims_for_specs:
      dimensions = dims_for_specs[json['name']]
      spec = Specification(
          name=json['name'],
          field_structure_type_val=json['field_structure_type'],
          product=json['product'],
          customer_owner_id=json['customer_owner_id'],
          entity_type=json['entity_type'],
          advertiser_ids=json['advertiser_ids'],
          campaign_ids=json['campaign_ids'],
          floodlight_activity_ids=json['floodlight_activity_ids'],
          min_start_date=_from_iso_date(json['min_start_date']),
          max_start_date=_from_iso_date(json['max_start_date']),
          min_end_date=_from_iso_date(json['min_end_date']),
          max_end_date=_from_iso_date(json['max_end_date']),
          dimensions=dimensions)
      specs[spec.name] = spec

  return SpecificationSet(
      cloud_project_id=cloud_project_id,
      bigquery_dataset=bigquery_dataset,
      bq_client=_bq_client.get(),
      specs=specs,
      fields=fields,
  )


def _from_iso_date(date_value: str) -> datetime.date:
  return datetime.strptime(date_value,
                           _ISO_DATE_FORMAT).date() if date_value else None


if __name__ == '__main__':
  pass
