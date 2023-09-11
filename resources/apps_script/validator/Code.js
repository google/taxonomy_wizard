/**
 * Description: Taxonomy Wizard: Validator Plugin
 * Contributors: blevitan@, djimbaye@
 * Last updated: 2023-09-07
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
const _UPDATE_ACTION = 'update_names'
const _UPDATE_SUCCESS_RESPONSE_SUFFIX = 'Updated.'
const NUM_RETRIES = 3;

/**
 * Callback for rendering the main card.
 * @return {CardService.Card} The card to show the user.
 */
function onHomepage(e) {
  return buildSpecSelectorCard();
}

/**
 * Builds the (context-free) homepage card.
 */
function buildHomepageCard() {
  const builder = CardService.newCardBuilder();
  const section = CardService.newCardSection()

  builder.setHeader(CardService.newCardHeader().setTitle('Taxonomy Wizard'));

  section.addWidget(CardService.newButtonSet()
    .addButton(CardService.newTextButton()
      .setText('Load Taxonomy Spec Chooser')
      .setOnClickAction(CardService.newAction().setFunctionName('loadSpecChooserCard'))
      .setDisabled(false)));

  builder.addSection(section);
  return builder.build();
}

/**
 * Builds the Spec selector card.
 * 
 * @param {!Object} specToSelect (Optional) Name of spec to select, else first.
 * @param (!Object) messageSection (Optional) messageSection to display.
 *
 */
function buildSpecSelectorCard(specToSelect = '', messageSection = null) {
  const builder = CardService.newCardBuilder()
    .setHeader(CardService.newCardHeader().setTitle('Taxonomy Wizard'));

  const mainSection = CardService.newCardSection();

  const selector = CardService.newSelectionInput()
    .setType(CardService.SelectionInputType.RADIO_BUTTON)
    .setTitle('Choose Taxonomy Specification Set:')
    .setFieldName('spec_name');

  const specNames = getSpecNamesFromUserProperties();

  if (specToSelect == '') {
    specToSelect = specNames[0];
  }
  for (const specName of specNames) {
    const isSelected = specName === specToSelect;
    selector.addItem(specName, specName, isSelected);
  }
  mainSection.addWidget(selector);

  mainSection.addWidget(CardService.newButtonSet()
    .addButton(CardService.newTextButton()
      .setText('Validate Selection')
      .setOnClickAction(CardService.newAction().setFunctionName('validateNamesInCells'))
      .setDisabled(false)));

  mainSection.addWidget(CardService.newButtonSet()
    .addButton(CardService.newTextButton()
      .setText('Update Selection')
      .setOnClickAction(CardService.newAction().setFunctionName('updateNamesInCells'))
      .setDisabled(false)));

  builder.addSection(mainSection);

  if (messageSection) {
    builder.addSection(messageSection);
  }
  return builder.build();
}

/** Pops stack to root, adds `card`, and displays it.
 * @param (!Object) card Card to add and display.
*/
function displayCardAboveRoot(card) {
  const nav = CardService.newNavigation()
    .popToRoot()
    .pushCard(card);

  return CardService.newActionResponseBuilder()
    .setNavigation(nav)
    .build();
}

/** Shows sidebar with Spec sets to choose and Validator & Update buttons. */
function loadSpecChooserCard(e) {
  const parameters = {
    'action': _LIST_ACTION
  };

  const payload = {
    'taxonomy_cloud_project_id': TAXONOMY_CLOUD_PROJECT_ID,
    'taxonomy_bigquery_dataset': TAXONOMY_CLOUD_DATASET,
  };

  const specs = submitRequest(CLOUD_FUNCTION_URI, parameters, payload);
  storeSpecNamesAsUserProperties(specs);

  const specCard = buildSpecSelectorCard();
  return displayCardAboveRoot(specCard);
}


function storeSpecNamesAsUserProperties(specs) {
  const userProperties = PropertiesService.getUserProperties();

  let i = 0;
  for (const spec of specs) {
    userProperties.setProperty(`specName${i}`, spec.name);
    i++;
  }

  userProperties.setProperty('specName_count', i.toString());
}

function getSpecNamesFromUserProperties() {
  const userProperties = PropertiesService.getUserProperties();
  let specNames = [];

  let specNameCount = userProperties.getProperty('specName_count');
  for (let i = 0; i < specNameCount; i++) {
    specNames.push(userProperties.getProperty(`specName${i}`));
  }

  return specNames;
}


/**
 * Validates the names in the sheet selection against the spec set passed in `e`.
 *
 * @param {!Object} e Event (containing spec name).
 */
function validateNamesInCells(e) {
  const specName = e.formInput.spec_name;
  const range = SpreadsheetApp.getActiveRange();

  const dict = validationDataToDict(range.getValues());

  const parameters = { 'action': _VALIDATION_ACTION };
  const payload = {
    'spec_name': specName,
    'taxonomy_cloud_project_id': TAXONOMY_CLOUD_PROJECT_ID,
    'taxonomy_bigquery_dataset': TAXONOMY_CLOUD_DATASET,
    'data': dict
  };
  const flat_results = submitRequest(CLOUD_FUNCTION_URI, parameters, payload, null, 'POST').results;

  const matrixed_results = unflatten2dArray(flat_results, 'validation_message');

  const failure_test = (t) => t !== '';
  outputResponse(matrixed_results, range, failure_test);

  const failureSection = formatFailureMessages(flat_results, 'value', 'validation_message', failure_test, 'Validation completed');
  return buildSpecSelectorCard(specName, failureSection);
}


/**
 * Updates the names in the sheet selection against the spec set passed in `e`.
 *
 * @param {!Object} e Event (containing spec name).
 */
function updateNamesInCells(e) {
  const specName = e.formInput.spec_name;
  const range = SpreadsheetApp.getActiveRange();

  if (!checkUpdateRange(range)) {
    let messageSection = CardService.newCardSection()
      .addWidget(CardService.newTextParagraph()
        .setText('<b><font color="#FF0000">Selection nust have exactly 2 columns AND all values in first column (key) must be integers.</font></b>'));

    return buildSpecSelectorCard(specName, messageSection);
  }

  const dict = updaterDataToDict(range.getValues());

  const parameters = { 'action': _UPDATE_ACTION };
  const payload = {
    'spec_name': specName,
    'taxonomy_cloud_project_id': TAXONOMY_CLOUD_PROJECT_ID,
    'taxonomy_bigquery_dataset': TAXONOMY_CLOUD_DATASET,
    'data': dict,
    'access_token': ScriptApp.getOAuthToken()
  };
  const flat_results = submitRequest(CLOUD_FUNCTION_URI, parameters, payload, null, 'POST').results;

  const matrixed_results = unflatten2dArray(flat_results, 'response');
  let response_range = range.offset(0, 1, range.getNumRows(), 1);

  const failure_test = (rs) => rs !== _UPDATE_SUCCESS_RESPONSE_SUFFIX
  const failures = outputResponse(matrixed_results, response_range, failure_test);


  const failureSection = formatFailureMessages(flat_results, 'key', 'response', failure_test, 'Update completed');
  return buildSpecSelectorCard(specName, failureSection);
}


function checkUpdateRange(range) {
  // Must have exactly 2 columns AND all values in first column (key) must be integers.
  return (range.getNumColumns() == 2) && (!range.offset(0, 0, range.getNumRows(), 1).getValues().some((row) => !Number.isInteger(row[0])));
}


function formatFailureMessages(results, id_field_name, response_field_name, failure_test, header_message_start) {
  let section = CardService.newCardSection();
  let widgets = [];


  for (let i = 0; i < results.length; i++) {
    const result = results[i];
    if (failure_test(result[response_field_name])) {
      widgets.push(CardService.newDecoratedText()
        .setText(`<b><font color="#770000">${result[id_field_name]}</font></b><br><font color="#000000">${result[response_field_name]}</font></br>`)
        .setWrapText(true));
    }
  }

  if (widgets.length > 0) {
    section.setHeader(`${header_message_start} with ${widgets.length} failure(s):`);
  }
  else {
    section.addWidget(CardService.newDecoratedText()
        .setText(`<font color="#007700">${`${header_message_start} successfully.`}</font></br>`)
        .setWrapText(true));
  }

  for (let i = 0; i < widgets.length; i++) {
    section.addWidget(widgets[i]);
  }

  return section
}


/**
 * Formats response and adds notes as needed.
 * @param (!Object) notes List of notes.
 * @param (!Object) range Range to apply notes to.
 * @param (!Object) failure_test Function to test whether to format the cell red.
 * 
 * @returns (!Object) Array containing failures.
 */
function outputResponse(notes, range, failure_test) {
  range.setNotes(notes);

  for (let r = 0; r < range.getNumRows(); r++) {
    for (let c = 0; c < range.getNumColumns(); c++) {
      if (failure_test(notes[r][c])) {
        range.getCell(r + 1, c + 1).setBackgroundRGB(255, 0, 0);
      } else {
        range.getCell(r + 1, c + 1).setBackground(null);
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
function validationDataToDict(values) {
  let output = [];

  for (let r = 0; r < values.length; r++) {
    for (let c = 0; c < values[r].length; c++) {
      output.push({ 'value': values[r][c].toString(), 'row': r, 'column': c });
    }
  }

  return output;
}


/**
 * Flattens the 2d array into a dict with elements of {row, key, new_value}.
 *
 * @param {!Object} values 2d array.
 *
 * @return {!Object} 1d array with elements of {row, key, value}.
 */
function updaterDataToDict(values) {
  let output = [];

  for (let r = 0; r < values.length; r++) {
    output.push({ 'row': r, 'key': values[r][0].toString(), 'new_value': values[r][1].toString() });
  }

  return output;
}

/**
 * Unflattens the 1d array with elements of {value, row, column} into a 2d array.
 *
 * @param {!Object} values 1d array with elements of {value, row, column}.
 * @param {!str} response_field_name Name of response field.
 *
 * @return {!Object} Values 2d array.
 */
function unflatten2dArray(values, response_field_name) {
  let output = [];

  for (let i = 0; i < values.length; i++) {
    let row = values[i].row;

    if (output[row] === undefined) {
      output[row] = [];
    }

    let col = values[i]['column'] !== undefined ? values[i].column : '0';

    if (values[i][response_field_name] === '') {
      output[row][col] = '';
    } else {
      output[row][col] = values[i][response_field_name];
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
  httpMethod = 'GET') {
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
  options.headers['Content-Type'] = 'application/json';
  options.muteHttpExceptions = FLAGS.SHOW_HTTP_EXCEPTIONS;
  options.headers['Authorization'] = 'Bearer ' + ScriptApp.getIdentityToken();

  if (FLAGS.LOG_REQUESTS) {
    console.log('Request URL: ' + url);
    console.log('Request Options: ' + JSON.stringify(options));
    console.log('Request Payload:' + UrlFetchApp.getRequest(url, options).payload);
  }

  if (FLAGS.LOG_SHEET != '' && SpreadsheetApp.getActiveSpreadsheet().getSheetByName('TEST') !== null) {
    SpreadsheetApp.getActiveSpreadsheet().getSheetByName('TEST').getRange('A1').setValue(JSON.stringify(payload));
    SpreadsheetApp.getActiveSpreadsheet().getSheetByName('TEST').getRange('A2').setValue(options.headers['Authorization']);
    SpreadsheetApp.getActiveSpreadsheet().getSheetByName('TEST').getRange('A3').setValue('BOOP!');
  }

  console.log('Request URL: ' + url);
  console.log('Request Options: ' + JSON.stringify(options));
  console.log('Request Payload:' + UrlFetchApp.getRequest(url, options).payload);

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

    if (FLAGS.LOG_RESPONSES) {
      console.log('Response code:' + response.getResponseCode());
      console.log('Response payload: ' + UrlFetchApp.getRequest(url, options).payload);
    }

  } else if (FLAGS.TEST_RESPONSE) {
    return FLAGS.TEST_RESPONSE;
  } else {
    return { 'status': 200, 'content': 'Ok' };
  }


  if (response.getResponseCode() != 200) {
    var err = 'Error with request. Response Code ' + response.getResponseCode() + ': ' + response.getContentText();
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
