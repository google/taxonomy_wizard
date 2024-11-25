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
# Shows info that needs to be manually added to Configurator sheet.

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
REGION=us-central1

# URI=$(gcloud functions describe configurator --gen2 --format="value(serviceConfig.uri)" --region=$REGION)
URI=https://${REGION}-${PROJECT_ID}.cloudfunctions.net/configurator

PROJECT_NUMBER=$(gcloud projects list --filter="${PROJECT_ID}" --format="value(PROJECT_NUMBER)")
echo "*******************************************************
* Project Id: ${PROJECT_ID}
*
* Configurator Cloud Function Endpoint:
*   ${URI}
*
* Project Number: ${PROJECT_NUMBER}
*******************************************************
"

echo "█████████████████████████████████████████████████████████████████████████
██                                                                     ██
██  * MANUAL STEPS *                                                   ██
██                                                                     ██
██  Configuration Sheet:                                               ██
██                                                                     ██
██  1. Go to the "Cloud Config" tab of the Admin sheet copied at the     ██
██     end of Deployment.                                              ██
██  2. Copy the Project Id to \"Taxonomy Data: Cloud Project Id\"        ██
██     ('Cloud Config'!B1).                                            ██
██  3. Copy the the Configurator Cloud Function Endpoint to \"Cloud     ██
██     Function Endpoint\" ('Cloud Config'!B2).  (You should not need   ██
██     to update the Taxonomy Data BigQuery Dataset.)                  ██
██  4. Open Apps Script from the Sheet (Extensions→Apps Script).       ██
██  5. On the LHR, click on Project Settings (the gear icon: ⚙ ).      ██
██  6. In the \"Google Cloud Platform (GCP) Project\" section, click     ██
██     on \"Change Project\".                                            ██
██  7. Copy the Project Number to \"Project Number\".                    ██
██                                                                     ██
█████████████████████████████████████████████████████████████████████████
"
