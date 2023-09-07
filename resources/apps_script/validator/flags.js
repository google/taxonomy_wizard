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

var FLAGS = function() {};

// Logging
FLAGS.LOG_REQUESTS = true;          // Log requests (independent of request submission).
FLAGS.LOG_RESPONSES = true;         // Log responses (when SUBMIT_REQUESTS=true).

// Debugging
FLAGS.SHOW_HTTP_EXCEPTIONS = true;  // Show full HTTP exceptions.
FLAGS.SUBMIT_REQUESTS = true        // Submit Requests.
FLAGS.LOG_SHEET = "";              // If non-empty, will write test data to sheet specified.
FLAGS.TEST_RESPONSE = false;        // When TEST_MODE=True and SUBMIT_REQUESTS=False, then use this to return the desired response.