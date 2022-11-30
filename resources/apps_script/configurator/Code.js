/**
 * Description: Functions to configure Taxonomy Specification sets.
 * Contributors: blevitan@
 * Last updated: 2022-09-22
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

const _CLOUD_FUNCTION_ENDPOINT_RANGE_NAME ='CloudFunctionEndpoint';
const _CLOUD_PROJECT_RANGE_NAME ='TaxonomyCloudProjectId';
const _BQ_DATASET_RANGE_NAME ='TaxonomyBigQueryDataset';
const _CLOUD_FUNCTION_API_ENDPOINT ='https://cloudfunctions.googleapis.com/v2/';
const _CONFIGURATOR_FUNCTION_NAME ='configurator';




const OBJECTS_TO_GENERATE_ = [{
  sheet: 'Taxonomy Fields',
  entityType: 'TaxonomyField',
  keyRow: 1,
  keyColumnStart: 1,
  keyColumnEnd: 5,
  valueRowOffset: 2,
  filterColumn: 6,
}, {
  sheet: 'Taxonomy Specs',
  entityType: 'TaxonomySpec',
  keyRow: 1,
  keyColumnStart: 1,
  keyColumnEnd: 11,
  valueRowOffset: 2,
  filterColumn: 12,
}, {
  sheet: 'Taxonomy Field Mappings',
  entityType: 'TaxonomyDimension',
  keyRow: 1,
  keyColumnStart: 1,
  keyColumnEnd: 4,
  valueRowOffset: 2,
  filterColumn: 5,
}];

function onOpen(e) {
  SpreadsheetApp.getUi()
    .createMenu('Taxonomy Wizard')
    // .addItem('Add/Update Specs', 'addAdditionalSpecs')
    // .addItem('Delete All Existing Specs', 'DeleteAllExistingSpecs')
    .addItem('Overwrite All Existing Specs', 'overwriteExistingSpecs')
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


function addAdditionalSpecs() {
  generateAllConfigData("update");
  // TODO(blevitan): Implement in Cloud Function.
}


function overwriteExistingSpecs() {
  generateAllConfigData("overwrite");
}


function deleteAllExistingSpecs() {
  // TODO(blevitan): Implement.
}

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
    'taxonomy_cloud_project_id': project_id,
    "taxonomy_bigquery_dataset": dataset,
  }

  let data = [];
  for (const def of OBJECTS_TO_GENERATE_) {
    data.push(getSheetConfigDataForRequest(def));
  }

  const configuratorEndpoint = SpreadsheetApp.getActiveSpreadsheet()
    .getRangeByName(_CLOUD_FUNCTION_ENDPOINT_RANGE_NAME)
    .getValue();
  const response = submitRequest(configuratorEndpoint, params, data, null, "POST");
  if (response.status >= HTTP_STATUS_MIN_ERROR_VALUE_ &&
    response.status <= HTTP_STATUS_MAX_ERROR_VALUE_) {
    return false;
  }

  return true;
}

/**
 * Generates config data from a sheet to be added to the cloud function `request` object.
 *
 * @param {!Object} def Object containing info on how to iterate through the sheet.
 *     Object must have this structure: {
 *         sheet: str,  // Sheet name
           entityType: str,  // One of: 'TaxonomyField', 'TaxonomySpec', 'TaxonomyDimension'.
           keyRow: int,  // Row containing config key values.
           keyColumnStart: int,  // First column containing key values.
           keyColumnEnd: int,  // Last column containing key values.
           valueRowOffset: int, // How many rows after `keyRow` the first value is.
           filterColumn: int,  // Which column contains a boolean filter value. (True=keep value).
       }
 * 
 * @return {!Object} Entities to create config data for.
 */
function getSheetConfigDataForRequest(sheetDefinition) {
      let entities = {
      'type': sheetDefinition.entityType,
      'data': []
    };
    const sht = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetDefinition.sheet);
    const lastRowOffset = sht.getLastRow() - sheetDefinition.keyRow + 1;

    for (let rowOffset = sheetDefinition.valueRowOffset; rowOffset < lastRowOffset; rowOffset++) {
      let kvObject = _keyValueObjectGenerator(
          sht,
          sheetDefinition.keyRow,
          sheetDefinition.keyRow,
          sheetDefinition.keyColumnStart,
          sheetDefinition.keyColumnEnd,
          rowOffset,
          0,
          sheetDefinition.filterColumn);
      if (kvObject != null) {
        entities.data.push(kvObject);
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
      } catch (e) {
        if (n == _NUM_RETRIES) {
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
    console.log("Response payload: " +UrlFetchApp.getRequest(url, options).payload);
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
function _UrlSearchParams(queryParameters) {
  return (
      Object.entries(queryParameters)
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
        .join('&')
    );
 } 


/**
 * Generates a key-value object based on the sheet ranges determined by the parameters.
 * 
 * Iterates through a rectangular range determined by key_[row|col]_[start|end] to get
 * keys. Offsets by val_[row|col]_offset to get the values for those keys. Will ignore
 * empty key cells.
 *
 * @param {!int} payload Parameters to pass to the endpoint.
 * @param (!Object) sht Sheet to get data from.
 * @param (int) key_row_start Start row for keys.
 * @param (int) key_row_end End row for keys.
 * @param (int) key_col_start Start column for keys.
 * @param (int) key_col_end End column for keys.
 * @param (int) val_row_offset How far to offset row from key's cell to get value.
 * @param (int) val_col_offset How far to offset column from key's cell to get value.
 * @param (int) filter_column Filter out any row that has "False" in this column (only for row-based data).
 *
 * @return {!Object} Response from the specified endpoint
 */
function _keyValueObjectGenerator(sht,
  key_row_start,
  key_row_end,
  key_col_start,
  key_col_end,
  val_row_offset,
  val_col_offset,
  filter_column) {
  var obj = {};

  for (var r = key_row_start; r <= key_row_end; r++) {
    if (filter_column != null && !sht.getRange(r + val_row_offset, filter_column).getValue()) {
      return null;
    }
    // TODO(blevitan): implement with getValues and an Array.prototype.filter.
    for (var c = key_col_start; c <= key_col_end; c++) {
      var key = sht.getRange(r, c).getValue();
      if (key != '') {
        obj[key] = sht.getRange(r + val_row_offset, c + val_col_offset).getValue();
      }
    }
  }

  return obj;
}
