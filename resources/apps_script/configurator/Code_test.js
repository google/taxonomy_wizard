/**  Copyright 2022 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

const TEST_RESPONSE = {};

function test_create_spec_set() {
  FLAGS.TEST_MODE = true;

  let specSets = {
      "action":"overwrite",
      "taxonomy_cloud_project_id":"taxonomy-wizard-dev",
      "taxonomy_bigquery_dataset":"taxonomy_wizard",
      "data":[
        {
          "type":"TaxonomyField",
          "data":[
            {
              "name":"yyyyq",
              "is_freeform_text":false,
              "dictionary_url":"https://docs.google.com/spreadsheets/d/1b5E9kdZfxzA0VkkOJy_4U5CsX8o9qs4XMn0LMiF1sdg/edit#gid=751493223",
              "dictionary_sheet":"Constants",
              "dictionary_range":"A3:A"
            }
          ]
        },
        {
          "type":"TaxonomySpec",
          "data":[
            {
              "name":"US Advertisers Display 2022",
              "product":"CM",
              "customer_entity_id":517303,
              "taxonomy_level":"Placement",
              "field_structure_type":"Delimited",
              "advertiser_name":"",
              "campaign_name":"",
              "start_date":"2022-01-01T05:00:00.000Z",
              "end_date":"2022-12-31T05:00:00.000Z"
            }
          ]
        },
        {
          "type":"TaxonomyDimension",
          "data":[
            {
              "taxonomy_spec_name":"US Advertisers Display 2022",
              "field_name":"yyyyq",
              "prefix_index":1,
              "end_delimiter":"_"
            }
          ]
        }
      ]
    }

  const endpoint = SpreadsheetApp.getActiveSpreadsheet().getRangeByName(_CLOUD_FUNCTION_ENDPOINT_RANGE_NAME).getValue();
  const response = submitRequest(endpoint, null, specSets);
  if (response.status >= HTTP_STATUS_MIN_ERROR_VALUE_ &&
    response.status <= HTTP_STATUS_MAX_ERROR_VALUE_) {
    return false;
  }

  return true;
}