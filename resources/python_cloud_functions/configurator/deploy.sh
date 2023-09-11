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
# Deploy configurator
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
REGION=us-central1

gcloud iam service-accounts create taxonomy-wizard-configurator \
  --description="Service account for Taxonomy Wizard Configurator component." \
  --display-name="Taxonomy Wizard Configurator Service Account"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:taxonomy-wizard-configurator@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:taxonomy-wizard-configurator@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:taxonomy-wizard-configurator@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud services enable run.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable iam.googleapis.com
gcloud services enable bigquery.googleapis.com

gcloud functions deploy configurator \
  --gen2 \
  --region=$REGION \
  --runtime=python310 \
  --source=$SCRIPT_DIR \
  --entry-point=handle_request \
  --service-account=taxonomy-wizard-configurator@${PROJECT_ID}.iam.gserviceaccount.com \
  --trigger-http \
  --no-allow-unauthenticated \
  --ignore-file=.gcloudignore

echo "█████████████████████████████████████████████████████████████████████████
██                                                                     ██
██                        ADMIN BACKEND DEPLOYED                       ██
██                                                                     ██
██  Make sure you move the Configurator sheet's Apps Script to the     ██
██  same project you deployed to.                                      ██
██                                                                     ██
██  You may need to configure the OAuth Consent screen as well:        ██
██    User Type: Internal.                                             ██
██    Enter App name (e.g., 'Taxonomy Wizard').                        ██
██    Enter Support and Contact email address (e.g., your email).      ██
█████████████████████████████████████████████████████████████████████████
"
