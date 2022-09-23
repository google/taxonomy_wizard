# Lint as: python3
"""Classes required to create set of Taxonomy Specs.

Hierarchy:
TaxonomySpecSet
  ∟ TaxonomySpec
      ∟ TaxonomyDimension

"""
import json
import os
from random import random
from time import sleep
from jinja_renderer import JinjaRenderer
import enum
import re
from typing import OrderedDict
from attrs import define, field
from google.cloud import bigquery  # type: ignore
from google import auth

_DELIMITED_VALIDATOR_FILENAME: str = 'delimited_validator.sql'
_NUM_RETRIES: int = 5

#TODO(blevitan): Refactor as singleton.


def get_bigquery_client() -> bigquery.Client:
  credentials, project = auth.default(
      scopes=[
          "https://www.googleapis.com/auth/drive",
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/spreadsheets",
      ]
  )
  return bigquery.Client(credentials=credentials, project=project)


class FieldStructureType(enum.Enum):
  DELIMITED = 1
  # DELIMITER_AND_DIMENSION_PREFIX = 2
  # FIXED_LENGTH = 3


class TaxonomyLevel(enum.Enum):
  # ADVERTISER = 1
  CAMPAIGN = 2
  # INSERTION_ORDER = 3
  # LINE_ITEM = 4
  # PLACEMENT = 5


# class IdField(enum.Enum):
#   PARTNER_ID = 1
#   PARTNER_NAME = 2
  # ACCOUNT_ID = 3
  # ACCOUNT_NAME = 4
  # ADVERTISER_ID = 5
  # ADVERTISER_NAME = 6


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
    table_def.schema = [bigquery.SchemaField('id', 'STRING', mode='NULLABLE')]

    external_config = bigquery.ExternalConfig('GOOGLE_SHEETS')
    external_config.source_uris = [self.dictionary_url]
    external_config.autodetect = False

    google_sheets_options = bigquery.GoogleSheetsOptions()
    google_sheets_options.range = f'\'{self.dictionary_sheet}\'!{self.dictionary_range}'
    external_config.google_sheets_options = google_sheets_options

    table_def.external_data_configuration = external_config

    # TODO(blevitan): Pass this in to the constructor (everywhere else too).
    client = get_bigquery_client()
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
  # id_field: IdField = field()
  # advertiser_name: str = field()
  # gmp_product: str = field()
  # id_field_value: str = field()
  # taxonomy_level: TaxonomyLevel = field()
  # start_date: datetime.date = field()
  # end_date: datetime.date = field()
  field_structure_type_val: str = field()
  field_structure_type: FieldStructureType = field(init=False)
  dimensions: OrderedDict[str, Dimension] = field(factory=list)
  validation_query_template: str = field(init=False, default='')

  def __attrs_post_init__(self):
    if self.field_structure_type_val.upper() == "DELIMITED":
      self.field_structure_type = FieldStructureType.DELIMITED
    else:
      raise Exception(
          f'Unsupported `field_structure_type` "{self.field_structure_type_val} in Spec "{self.name}".')

  def create_validation_query_template(self, renderer: JinjaRenderer):
    if self.field_structure_type == FieldStructureType.DELIMITED:
      self.validation_query_template = \
          renderer.load_and_render_template(_DELIMITED_VALIDATOR_FILENAME,
                                            spec=self)
    else:
      raise Exception(
          f'Unsupported `field_structure_type` "{self.field_structure_type} in Spec "{self.name}".')


@define(auto_attribs=True)
class SpecificationSet:
  """Set of Specifications."""
  cloud_project_id: str = field()
  bigquery_dataset: str = field()
  # TODO(blevitan): Uncomment once implemented in Phase 3.
  # source_report_table_locations: List[str] = None
  specs: dict[str, Specification] = field(factory=dict)
  fields: dict[str, Field] = field(factory=dict)
  _specifications_table_name: str = field(default='specifications')

  def table_ref(self) -> bigquery.TableReference:
    return bigquery.TableReference(
        bigquery.DatasetReference(self.cloud_project_id,
                                  self.bigquery_dataset),
        self._specifications_table_name)

  def create_in_bigquery(self):
    """Generates taxonomy tables, etc... in bigquery."""
    self._create_linked_tables_for_fields()
    self._create_spec_validation_query_templates()
    self._create_specs_table()

  def _create_linked_tables_for_fields(self):
    for field in self.fields.values():
      if not field.is_freeform_text:
        field.create_linked_bigquery_table()

  def _create_spec_validation_query_templates(self):
      renderer = JinjaRenderer()
      for spec in self.specs.values():
        spec.create_validation_query_template(renderer)

  def _create_specs_table(self):
    client: bigquery.Client = get_bigquery_client()

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = [
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("field_structure_type",
                             "STRING", mode="REQUIRED"),
        bigquery.SchemaField("validation_query_template",
                             "STRING", mode="REQUIRED"), ]

    data: list[dict[str, str]] = [
        {
            'name': spec.name,
            'field_structure_type': str(spec.field_structure_type_val),
            'validation_query_template': spec.validation_query_template
        }
        for spec in self.specs.values()]

    job = client.load_table_from_json(data,
                                      self.table_ref(),
                                      job_config=job_config)

    errors = job.result().errors

    if errors:
      print(f'Errors adding rows to table: {errors}')
    else:
      print(f'Added rows to table.')
