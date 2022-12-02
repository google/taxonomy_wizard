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
# Classes required to create set of Taxonomy Specs.
#
# Hierarchy:
# TaxonomySpecSet
#   ∟ TaxonomySpec
#       ∟ TaxonomyDimension

import datetime
import enum
from jinja_renderer import JinjaRenderer
import enum
import re
from typing import OrderedDict
from attrs import define, field
from google.cloud import bigquery  # type: ignore
from google.cloud.exceptions import NotFound

_DELIMITED_VALIDATOR_FILENAME: str = 'delimited_validator.sql'
_BIGQUERY_DATE_FORMAT: str = '%Y-%m-%d'

class FieldStructureType(enum.Enum):
  DELIMITED = enum.auto()
  FIXED_LENGTH = enum.auto()


class EntityType(enum.Enum):
  # ADVERTISER = enum.auto()
  CAMPAIGN = enum.auto()
  # INSERTION_ORDER = enum.auto()
  # LINE_ITEM = enum.auto()
  PLACEMENT = enum.auto()


@define(auto_attribs=True)
class Field:
  """Field for use with a Dimension."""
  name: str = field()
  is_freeform_text: bool = field()
  dictionary_url: str = field()
  dictionary_sheet: str = field()
  dictionary_range: str = field()
  cloud_project_id: str = field()
  bigquery_dataset: str = field()
  bq_client: bigquery.Client = field()

  normalized_name: str = field(init=False)
  table_id: str = field(init=False)
  _table_location_in_dictionary: str = field(init=False)

  _NORMALIZED_FIELD_NAME_PREFIX: str = field(init=False, default='dim_')

  def __attrs_post_init__(self):
    norm_name = re.sub(r'[^a-z]', '_', self.name)
    self.normalized_name = self._NORMALIZED_FIELD_NAME_PREFIX + norm_name
    self.table_id = f'{self.cloud_project_id}.{self.bigquery_dataset}.{self.normalized_name}'

  def create_linked_bigquery_table(self):
    """Generates (or overwrites) a BQ linked table."""
    table_def = bigquery.Table(self.table_id)
    table_def.schema = [bigquery.SchemaField(
        'id', 'STRING', mode='NULLABLE')]

    external_config = bigquery.ExternalConfig('GOOGLE_SHEETS')
    external_config.source_uris = [self.dictionary_url]
    external_config.autodetect = False

    google_sheets_options = bigquery.GoogleSheetsOptions()
    google_sheets_options.range = f'\'{self.dictionary_sheet}\'!{self.dictionary_range}'
    external_config.google_sheets_options = google_sheets_options

    table_def.external_data_configuration = external_config

    # TODO(blevitan): Pass this in to the constructor (everywhere else too).
    client = self.bq_client
    client.delete_table(self.table_id, not_found_ok=True)

    client.create_table(table_def)


@define(auto_attribs=True)
class Dimension:
  """Dimension within a Specification."""
  name: str = field()
  index: int = field()
  field_spec: Field = field()
  end_delimiter: str = field()
  regex_match_expression: str = field(default='')
  requires_crossjoin_validation: bool = field(default=False)
  extra_data_regex: str = field(default='')


@define(auto_attribs=True)
class Specification:
  """Contains structure of taxonomy."""
  name: str = field()
  field_structure_type_val: str = field()
  product: str = field(default=None)
  customer_owner_id: str = field(default=None)
  entity_type: EntityType = field(default=None)
  _advertiser_ids: str = field(default=None)
  _campaign_ids: str = field(default=None)
  min_start_date: datetime.date = field(default=None)
  max_start_date: datetime.date = field(default=None)
  min_end_date: datetime.date = field(default=None)
  max_end_date: datetime.date = field(default=None)
  field_structure_type: FieldStructureType = field(init=False)
  dimensions: OrderedDict[str, Dimension] = field(factory=list)
  validation_query_template: str = field(init=False, default='')
  # Set in post_init
  advertiser_ids: list[int] = field(init=False, factory=list)
  campaign_ids: list[int] = field(init=False, factory=list)

  def __attrs_post_init__(self):
    if self._advertiser_ids:
      self.advertiser_ids = [int(v) for v in self._advertiser_ids.split(',')]

    if self._campaign_ids:
      self.campaign_ids = [int(v) for v in self._campaign_ids.split(',')]

    if self.field_structure_type_val.upper() == 'DELIMITED':
      self.field_structure_type = FieldStructureType.DELIMITED
    else:
      raise Exception(
          f'Unsupported `field_structure_type` "{self.field_structure_type_val} in Spec "{self.name}".'
      )

  def create_validation_query_template(self, renderer: JinjaRenderer):
    if self.field_structure_type == FieldStructureType.DELIMITED:
      self.validation_query_template = \
          renderer.load_and_render_template(_DELIMITED_VALIDATOR_FILENAME,
                                            spec=self)
    else:
      raise Exception(
          f'Unsupported `field_structure_type` "{self.field_structure_type} in Spec "{self.name}".'
      )


@define(auto_attribs=True)
class SpecificationSet:
  """Set of Specifications."""
  cloud_project_id: str = field()
  bigquery_dataset: str = field()
  bq_client: bigquery.Client = field()
  specs: dict[str, Specification] = field(factory=dict)
  fields: dict[str, Field] = field(factory=dict)
  _specifications_table_name: str = field(default='specifications')
  _specifications_dataset_location: str = field(default='US')

  def table_ref(self) -> bigquery.TableReference:
    return bigquery.TableReference(
        bigquery.DatasetReference(self.cloud_project_id, self.bigquery_dataset),
        self._specifications_table_name)

  def create_in_bigquery(self):
    """Generates taxonomy tables, etc... in bigquery."""
    self._create_dataset()
    self._create_linked_tables_for_fields()
    self._create_spec_validation_query_templates()
    self._create_specs_table()

  def _create_dataset(self):
    try:
      self.bq_client.get_dataset(
          f'{self.cloud_project_id}.{self.bigquery_dataset}')
    except NotFound:
      dataset = bigquery.Dataset(
          f'{self.cloud_project_id}.{self.bigquery_dataset}')
      dataset.location = self._specifications_dataset_location
      dataset = self.bq_client.create_dataset(dataset, timeout=30)

  def _create_linked_tables_for_fields(self):
    for field in self.fields.values():
      if not field.is_freeform_text:
        field.create_linked_bigquery_table()

  def _create_spec_validation_query_templates(self):
    renderer = JinjaRenderer()
    for spec in self.specs.values():
      spec.create_validation_query_template(renderer)

  def _create_specs_table(self):
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.schema = [
        bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('field_structure_type', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('validation_query_template',
                             'STRING',
                             mode='REQUIRED'),
        bigquery.SchemaField('product', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('customer_owner_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('entity_type', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('advertiser_ids', 'INT64', mode='REPEATED'),
        bigquery.SchemaField('campaign_ids', 'INT64', mode='REPEATED'),
        bigquery.SchemaField('min_start_date', 'DATE', mode='NULLABLE'),
        bigquery.SchemaField('max_start_date', 'DATE', mode='NULLABLE'),
        bigquery.SchemaField('min_end_date', 'DATE', mode='NULLABLE'),
        bigquery.SchemaField('max_end_date', 'DATE', mode='NULLABLE'),
    ]

    data: list[dict[str, str]] = [{
        'name': spec.name,
        'field_structure_type': str(spec.field_structure_type_val),
        'validation_query_template': spec.validation_query_template,
        'product': spec.product,
        'customer_owner_id': spec.customer_owner_id,
        'entity_type': spec.entity_type,
        'advertiser_ids': spec.advertiser_ids,
        'campaign_ids': spec.campaign_ids,
        'min_start_date': self._to_bigquery_date(spec.min_start_date),
        'max_start_date': self._to_bigquery_date(spec.max_start_date),
        'min_end_date': self._to_bigquery_date(spec.min_end_date),
        'max_end_date': self._to_bigquery_date(spec.max_end_date),
    } for spec in self.specs.values()]

    job = self.bq_client.load_table_from_json(data,
                                              self.table_ref(),
                                              job_config=job_config)

    errors = job.result().errors

    if errors:
      print(f'Errors adding rows to table: {errors}')
    else:
      print(f'Added rows to table.')

  def _to_bigquery_date(self, date_value) -> datetime.date:
    return date_value.strftime(_BIGQUERY_DATE_FORMAT) if date_value else None
