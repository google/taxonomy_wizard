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
# Deploy validator

PROJECT_ID=$(gcloud config get project 2>/dev/null)
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
REGION=us-central1

gcloud iam service-accounts create taxonomy-wizard-validator \
  --description="Service account for Taxonomy Wizard Validator component." \
  --display-name="Taxonomy Wizard Validator Service Account"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:taxonomy-wizard-validator@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/cloudfunctions.invoker"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:taxonomy-wizard-validator@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:taxonomy-wizard-validator@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud services enable artifactregistry.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable drive.googleapis.com
gcloud services enable dfareporting.googleapis.com
gcloud services enable iam.googleapis.com
gcloud services enable run.googleapis.com

gcloud functions deploy validator \
  --gen2 \
  --region=$REGION \
  --runtime=python310 \
  --source=$SCRIPT_DIR \
  --entry-point=handle_request \
  --service-account=taxonomy-wizard-validator@${PROJECT_ID}.iam.gserviceaccount.com \
  --trigger-http \
  --no-allow-unauthenticated \
  --ignore-file=.gcloudignore

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
URI=$(gcloud functions describe validator --gen2 --format="value(serviceConfig.uri)" --region=$REGION)

gcloud scheduler jobs create http validator-scheduler \
  --schedule="5 4 * * *" \
  --uri="${URI}/?action=validate_everything&taxonomy_cloud_project_id=${PROJECT_ID}&taxonomy_bigquery_dataset=taxonomy_wizard" \
  --location="$REGION" \
  --http-method="GET" \
  --oidc-service-account-email="taxonomy-wizard-validator@${PROJECT_ID}.iam.gserviceaccount.com"
# --uri="${URI}" \
# --message-body="{\"action\": \"validate_everything\", \"taxonomy_cloud_project_id\": \"${PROJECT_ID}\", \"taxonomy_bigquery_dataset\": \"taxonomy_wizard\" }"\

echo "█████████████████████████████████████████████████████████████████████████
██                                                                     ██
██                    VALIDATION BACKEND DEPLOYED                      ██
██                                                                     ██
█████████████████████████████████████████████████████████████████████████
"
