#!/usr/bin/env bash
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Update validator script.

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

URI=$(gcloud functions describe validator --gen2 --format="value(serviceConfig.uri)" --region=us-central1)

ESCAPED_URI=$(sed 's/[&/\]/\\&/g' <<<"$URI")

sed -i \
  -e "s/const CLOUD_FUNCTION_URI = .*/const CLOUD_FUNCTION_URI = '${ESCAPED_URI}';/g" \
  -e "s/const TAXONOMY_CLOUD_PROJECT_ID = .*/const TAXONOMY_CLOUD_PROJECT_ID = '${PROJECT_ID}';/g" \
  ${SCRIPT_DIR}/CONFIG.js

sed -i \
  -e "s/\"urlFetchWhitelist\": .*/  \"urlFetchWhitelist\": \[\"${ESCAPED_URI}\"\]/g" \
  ${SCRIPT_DIR}/appsscript.json

echo "█████████████████████████████████████████████████████████████████████████
██                                                                     ██
██                VALIDATOR SHEETS PLUGIN CODE UPDATED                 ██
██                                                                     ██
██  Make sure you move the Validator Plugin's Apps Script to the       ██
██  same project you deployed to.                                      ██
██                                                                     ██
██  You may need to configure the OAuth Consent screen as well:        ██
██    User Type: Internal.                                             ██
██    Enter App name (e.g., 'Taxonomy Wizard').                        ██
██    Add 'google.com' as an 'Authorized Domain'.                      ██
██    Enter Support and Contact email address (e.g., your email).      ██
█████████████████████████████████████████████████████████████████████████"
