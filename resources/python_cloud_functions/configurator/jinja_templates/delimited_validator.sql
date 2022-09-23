{#
  Copyright 2022 Google LLC

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 #}
WITH
  Dimensions AS (
    SELECT
      name,
{%- for dim in spec.dimensions.values() %}
      REGEXP_EXTRACT(
        name,
        {{dim.regex_match_expression}}) as d_{{ dim.index }},
  {%- if loop.last %}
      REGEXP_EXTRACT(
        name,
        {{dim.extra_data_regex}}) as d_extra_data,
  {%- endif -%}
{%- endfor %}
{# TODO(blevitan): Figure out better long-term strategy for variable here
                   (vs currently overloading SQL variable syntax ) #}
    FROM @entity_names as name
{%- for dim in spec.dimensions.values() if dim.requires_crossjoin_validation %}
    CROSS JOIN `{{ dim.field_spec.table_id }}` AS D_{{ dim.index }}
{%- endfor %}
  ),
  NullChecker AS (
    SELECT
      D.name,
{%- for dim in spec.dimensions.values() %}
      D.d_{{ dim.index }},
{%- endfor %}
{%- for dim in spec.dimensions.values() if dim.requires_crossjoin_validation %}
      MAX(d_{{ dim.index }}) OVER (PARTITION BY D.name) as d_{{ dim.index }}_nullcheck,
{%- endfor %}
      D.d_extra_data,
    FROM Dimensions AS D
  ),
  Validation AS (
    SELECT
      D.name,
{%- for dim in spec.dimensions.values() %}
      CASE
        WHEN d_{{ dim.index }} IS NULL
          THEN 'Missing value in position {{ dim.index }}.'
  {%- if not dim.field_spec.is_freeform_text %}
        WHEN T_{{dim.index}}.id IS NULL
          THEN
            FORMAT('Invalid value "%s" in position {{ dim.index }} (field "{{ dim.field_spec.name }}").', D.d_{{ dim.index }})
  {%- endif -%}
        ELSE NULL
        END AS d_{{ dim.index }}_validation,
{%- endfor %}
      IF(
        IFNULL(D.d_extra_data, '') != '',
        CONCAT(
          'Unexpected data at end of string: "',
          D.d_extra_data,
          '".'),
        CAST(NULL AS STRING)) AS d_extra_data_validation,
    FROM NullChecker AS D
{%- for dim in spec.dimensions.values() if not dim.field_spec.is_freeform_text %}
    LEFT OUTER JOIN `{{ dim.field_spec.table_id }}` AS T_{{ dim.index }}
      ON T_{{ dim.index }}.id = D.d_{{ dim.index }}
{%- endfor %}
    WHERE
      TRUE
{%- for dim in spec.dimensions.values() if dim.requires_crossjoin_validation %}
       AND (D.d_{{ dim.index }} IS NOT NULL OR D.d_{{ dim.index }}_nullcheck IS NULL)
{%- endfor %}
  )
SELECT
  name,
    ARRAY_TO_STRING([
{%- for dim in spec.dimensions.values() %}
      d_{{ dim.index }}_validation,
{%- endfor %}
      d_extra_data_validation
    ],
    '\n') as validation_message
FROM Validation
