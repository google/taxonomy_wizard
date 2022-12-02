/**
 * Description: Taxonomy Wizard Plugin
 * Contributors: blevitan@
 * Last updated: 2022-09-09
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

const _LIST_ACTION = 'list_specs';
const _VALIDATION_ACTION = 'validate_names';
const NUM_RETRIES = 3;

function onOpen(e) {
  SpreadsheetApp.getUi()
    .createMenu('Taxonomy Wizard')
    .addItem("Show sidebar", "showSidebar")
    // .addItem("Validate Names", "validateNamesInCells")
    .addToUi();
}


/** Shows sidebar with Spec sets to choose and Validator button. */
function showSidebar() {
  const parameters = {
    'action': _LIST_ACTION,
    'taxonomy_cloud_project_id': TAXONOMY_CLOUD_PROJECT_ID,
    'taxonomy_bigquery_dataset': TAXONOMY_CLOUD_DATASET,
  };

  const specs = submitRequest(CLOUD_FUNCTION_URI, parameters);
  // const specs = [{ 'name': 'US Advertisers Display 2022' }, { 'name': 'US Advertisers 2021' }, { 'name': 'US Advertisers' }];
  const widget = createSidebarHtml(specs)
  SpreadsheetApp.getUi().showSidebar(widget);
}


/**
 * Generates HTML for sidebar.
 *
 * @param {!Object} Array of spec names.
 *
 * @return {!str} HTML for sidebar.
 */
function createSidebarHtml(specs){
    const radios = specs.reduce(
    (prev, spec) =>
      prev + `<li class='radio'><label class='field'><input type='radio' name='spec_name' value='${spec.name}'/>${spec.name}</label>\n`,
    '\n')
    // TODO(blevitan): Replace with non-hacky HTML method.
    .replace(`value='${specs[0].name}'`, `value='${specs[0].name}' checked`);

  return HtmlService.createHtmlOutput(SIDEBAR_HTML_PRE + radios + SIDEBAR_HTML_POST).setTitle('Taxonomy Wizard');

}


/**
 * Validates the names in the sheet selection against the spec set chosen in the sidebar.
 *
 * @param {!str} Name of spec to validate sheet selection against.
 */
function validateNamesInCells(specName) {
  const range = SpreadsheetApp.getActiveRange();
  const values = range.getValues();

  const flattenedArray = flatten2dArray(values);
  const parameters = {
    'action': _VALIDATION_ACTION,
    'spec_name': specName,
    'taxonomy_cloud_project_id': TAXONOMY_CLOUD_PROJECT_ID,
    'taxonomy_bigquery_dataset': TAXONOMY_CLOUD_DATASET
  };

  const flat_results = submitRequest(CLOUD_FUNCTION_URI, parameters, flattenedArray, null, "POST").results;
  const matrixed_results = unflatten2dArray(flat_results);
  range.setNotes(matrixed_results);

  for (let r = 0; r < range.getNumRows(); r++) {
    for (let c = 0; c < range.getNumColumns(); c++) {
      if (matrixed_results[r][c] === null) {
        range.getCell(r + 1, c + 1).setBackground(null);
      } else {
        range.getCell(r + 1, c + 1).setBackgroundRGB(255, 0, 0);
      }
    }
  }
}


/**
 * Flattens the 2d array into a 1d array with elements of {value, row, column}.
 *
 * @param {!Object} values 2d array.
 *
 * @return {!Object} 1d array with elements of {value, row, column}.
 */
function flatten2dArray(values) {
  let output = [];

  for (let r = 0; r < values.length; r++) {
    for (let c = 0; c < values[r].length; c++) {
      output.push({ 'value': values[r][c], 'row': r, 'column': c });
    }
  }

  return output;
}


/**
 * Unflattens the 1d array with elements of {value, row, column} into a 2d array.
 *
 * @param {!Object} values 1d array with elements of {value, row, column}.
 * @param {!int} rows Number of rows.
 * @param {!int} columns Number of columns.
 *
 * @return {!Object} Values 2d array.
 */
function unflatten2dArray(values) {
  let output = [];

  for (let i = 0; i < values.length; i++) {
    let row = values[i].row;

    if (output[row] === undefined) {
      output[row] = [];
    }
    if (values[i].validation_message === '') {
      output[row][values[i].column] = null;
    } else {
      output[row][values[i].column] = values[i].validation_message;
    }
  }

  return output;
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
function objectToQueryParams(obj) {
  return (
    Object.entries(obj)
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join('&')
  );
}
