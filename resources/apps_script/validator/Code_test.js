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

VALIDATOR_SERVICE_TEST_RESPONSE = {
  "results": [
    {
      "value":"2022q2_consumer_na_brandbuilding_nongenzbts",
      "row":0,
      "column":0,
      "validation_message":""
    },
    {
      "value":"2022q2_music_na_musicmarketing_nongenzvalid",
      "row":1,
      "column":0,
      "validation_message":""
    },
    {
      "value":"x2022q2_music_na_musicmarketing_nongenzinvalid",
      "row":2,
      "column":0,
      "validation_message":"Invalid value \"x2022q2\" in position 1 (field \"yyyyq\")."
    },
    {
      "value":"2022q2_music_na_musicmarketing_nongenzbt_extradata",
      "row":3,
      "column":0,
      "validation_message":""
    },
    {
      "value":"x2022q2_music_na_musicmarketing_nongenzbt_abc",
      "row":4,
      "column":0,
      "validation_message":"Invalid value \"x2022q2\" in position 1 (field \"yyyyq\")."
    },
    {
      "value":"2022q2_music__musicmarketing_nongenzbts",
      "row":5,
      "column":0,
      "validation_message":"Invalid value \"\" in position 3 (field \"fandom\")."
    },
    {
      "value":"2022q2_music_extra_dim_musicmarketing_nongenzbts",
      "row":6,
      "column":0,
      "validation_message":"Invalid value \"extra\" in position 3 (field \"fandom\").\nInvalid value \"dim\" in position 4 (field \"campaign_type\").\nMissing value in position 5.\nMissing value in position 6."
    }
  ]
};


function test_validateNamesInCells() {
  FLAGS.TEST_MODE = true;
  FLAGS.SUBMIT_REQUESTS = false;

  SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Display').setActiveSelection('A4:A10');
  validateNamesInCells("US Advertisers Display 2022");
}

