/**
 * Description: Functions to configure Taxonomy Specification sets.
 * Contributors: blevitan@
 * Last updated: 2023-09-12
 * 
 * @OnlyCurrentDoc
 * 
 *  Copyright 2022 Google LLC
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

const HTTP_STATUS_MIN_ERROR_VALUE_ = 400;
const HTTP_STATUS_MAX_ERROR_VALUE_ = 599;
const NUM_RETRIES = 3;

const _CLOUD_FUNCTION_ENDPOINT_RANGE_NAME = 'CloudFunctionEndpoint';
const _CLOUD_PROJECT_RANGE_NAME = 'TaxonomyCloudProjectId';
const _BQ_DATASET_RANGE_NAME = 'TaxonomyBigQueryDataset';
const _CLOUD_FUNCTION_API_ENDPOINT = 'https://cloudfunctions.googleapis.com/v2/';
const _CONFIGURATOR_FUNCTION_NAME = 'configurator';

const OBJECTS_TO_GENERATE_ = [{
  sheet: 'Taxonomy Fields',
  entityType: 'TaxonomyField',
  keyRow: 1,
  dataColumnStart: 1,
  dataColumnEnd: 5,
  valueRowOffset: 2,
  filterColumn: 6,
}, {
  sheet: 'Taxonomy Specs',
  entityType: 'TaxonomySpec',
  keyRow: 1,
  dataColumnStart: 1,
  dataColumnEnd: 12,
  valueRowOffset: 2,
  filterColumn: 13,
}, {
  sheet: 'Taxonomy Field Mappings',
  entityType: 'TaxonomyDimension',
  keyRow: 1,
  dataColumnStart: 1,
  dataColumnEnd: 4,
  valueRowOffset: 2,
  filterColumn: 5,
}];


function onOpen(e) {
  SpreadsheetApp.getUi()
    .createMenu('Taxonomy Wizard')
    .addItem('Overwrite All Specs', 'overwriteSpecs')
    // TODO(blevitan): Uncomment when implemented in Cloud Function.
    // .addItem('Add/Update Specs', 'updateSpecs')
    // .addItem('Delete All Specs', 'DeleteAllSpecs')
    .addSeparator()
    .addItem('Authorize User', 'authorizeUser')
    .addItem('Revoke User Authorization', 'revokeUserAuthorization')
    .addToUi();
}


function onInstall(e) {
  onOpen(e);
}


/** Authorizes user (if needed). */
function authorizeUser() {
  ScriptApp.getOAuthToken();
}


/** Revokes authorization of current user. */
function revokeUserAuthorization() {
  ScriptApp.invalidateAuth();
  console.log("User Authorization revoked.");
}


function overwriteSpecs() {
  generateAllConfigData("overwrite");
}


// TODO(blevitan): Uncomment when implemented in Cloud Function.
// function updateSpecs() {
//   generateAllConfigData("update");
// }


// TODO(blevitan): Uncomment when implemented in Cloud Function.
// function deleteAllSpecs() {
//   // TODO: Implement.
// }

/**
 * Generates the taxonomy configuration data from the spreadsheet.
 *
 * @param {string} action What to do with the data: "append", "overwrite", "delete".
 */
function generateAllConfigData(action) {
  const project_id = SpreadsheetApp.getActiveSpreadsheet().getRangeByName(_CLOUD_PROJECT_RANGE_NAME).getValue();
  const dataset = SpreadsheetApp.getActiveSpreadsheet().getRangeByName(_BQ_DATASET_RANGE_NAME).getValue();

  let params = {
    'action': action,
  }

  let data = [];
  for (const def of OBJECTS_TO_GENERATE_) {
    data.push(getSheetConfigDataForRequest(def));
  }
  let payload = {
    'taxonomy_cloud_project_id': project_id,
    "taxonomy_bigquery_dataset": dataset,
    "data": data,
  }

  const configuratorEndpoint = SpreadsheetApp.getActiveSpreadsheet()
    .getRangeByName(_CLOUD_FUNCTION_ENDPOINT_RANGE_NAME)
    .getValue();

  var htmlOutput = HtmlService
    .createHtmlOutput('<p>Submitting request...</p>')
    .setWidth(250)
    .setHeight(150);
  SpreadsheetApp.getUi().showModelessDialog(htmlOutput, 'Status');

  const response = submitRequest(configuratorEndpoint, params, payload, null, "POST");

  if (response.status >= HTTP_STATUS_MIN_ERROR_VALUE_ &&
    response.status <= HTTP_STATUS_MAX_ERROR_VALUE_) {
    return false;
  }

  return true;
}

/**
 * Generates config data from a sheet to be added to the cloud function `request` object.
 *
 * @param {!Object} sheetDef Object containing info on how to iterate through the sheet.
 *     Object must have this structure: {
 *         sheet: str,  // Sheet name
           entityType: str,  // One of: 'TaxonomyField', 'TaxonomySpec', 'TaxonomyDimension'.
           keyRow: int,  // Row containing config key values.
           dataColumnStart: int,  // First column containing data.
           dataColumnEnd: int,  // Last column containing data.
           valueRowOffset: int, // How many rows after `keyRow` the first value is.
           filterColumn: int,  // Which column contains a boolean filter value. (True=keep value).
       }
 * 
 * @return {!Object} Entities to create config data for.
 */
function getSheetConfigDataForRequest(sheetDef) {
  const sht = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetDef.sheet);
  const valueRowStart = sheetDef.keyRow + sheetDef.valueRowOffset;
  const valueRowCount = sht.getLastRow() - valueRowStart + 1;
  const colCount = sheetDef.dataColumnEnd - sheetDef.dataColumnStart + 1;

  let keys = sht.getSheetValues(sheetDef.keyRow, sheetDef.dataColumnStart, 1, colCount)[0];
  let values = sht.getSheetValues(valueRowStart, sheetDef.dataColumnStart, valueRowCount, colCount);
  let filters = sht.getSheetValues(valueRowStart, sheetDef.filterColumn, valueRowCount, 1).flat();

  let entities = {
    'type': sheetDef.entityType,
    'data': []
  };

  for (let r = 0; r < valueRowCount; r++) {
    if (filters[r]) {
      let obj = {};
      for (let c = 0; c < colCount; c++) {
        if (keys[c]!='') {
          obj[keys[c]] = values[r][c];
        }
      }
      entities.data.push(obj);
    }
  }

  return entities;
}


/**
  * Sends a request.
  *
  * @param {!Object} endpoint Cloud function endpoint.
  * @param {!Object} queryParameters Parameters to pass to url via query (in dictionary form).
  * @param {!Object} payload Payload to send.
  * @param {!Object} options Options to pass to the endpoint.
  * @param {!string} httpMethod HTTP method to use for Request.
  *                  (i.e., PUT, GET, POST, DELETE, or PATCH)
  *
  * @return {!Object} Cloud function response.
  */
function submitRequest(endpoint,
  queryParameters = {},
  payload = null,
  options = null,
  httpMethod = "GET") {
  let response;

  if (!options) {
    options = {};
  }

  if (!options.headers) {
    options.headers = {};
  }

  if (payload) {
    options.payload = JSON.stringify(payload);
  }

  const url = endpoint + (!endpoint.endsWith('?') ? '?' : '') + objectToQueryParams(queryParameters);

  options.method = httpMethod;
  options.headers['Content-Type'] = "application/json";
  options.muteHttpExceptions = FLAGS.SHOW_HTTP_EXCEPTIONS;
  options.headers['Authorization'] = "Bearer " + ScriptApp.getIdentityToken();

  if (FLAGS.LOG_REQUESTS) {
    console.log("Request URL: " + url);
    console.log("Request Options: " + JSON.stringify(options));
    console.log("Request Payload:" + UrlFetchApp.getRequest(url, options).payload);
  }

  if (FLAGS.TEST_MODE) {
    SpreadsheetApp.getActiveSpreadsheet().getSheetByName("TEST").getRange("A1").setValue(JSON.stringify(payload));
  }

  if (FLAGS.SUBMIT_REQUESTS) {
    //exponential backoff
    for (var n = 0; n < NUM_RETRIES; n++) {
      try {
        response = UrlFetchApp.fetch(url, options);
        break;
      } catch (e) {
        if (n == NUM_RETRIES) {
          throw e;
        }
        Utilities.sleep((Math.pow(2, n) * 1000) + (Math.round(Math.random() * 1000)));
      }
    }
  } else if (FLAGS.TEST_MODE) {
    return TEST_RESPONSE;
  } else {
    return { 'status': 200, 'content': 'Ok' };
  }

  if (FLAGS.LOG_RESPONSES) {
    console.log("Response code:" + response.getResponseCode());
    console.log("Response payload: " + UrlFetchApp.getRequest(url, options).payload);
  }

  if (response.getResponseCode() != 200) {
    var err = "Error with request. Response Code " + response.getResponseCode() + ": " + response.getContentText();
    console.error(err);
    throw err
  }

  return JSON.parse(response.getContentText());
}


/** Encodes and creates string for URL search parameters.
 * 
 * (`URLSearchParams()` not supported in Apps script.)
 * 
 * @param (!Object) queryParameters object of keys & values for use as parameter string.
 * 
 * @return {str} URI encoded search parameters string.
 */
function objectToQueryParams(queryParameters) {
  return (
    Object.entries(queryParameters)
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join('&')
  );
}
