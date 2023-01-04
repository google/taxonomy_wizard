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

PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

URI=$(gcloud functions describe configurator --gen2 --format="value(serviceConfig.uri)" --region=us-central1)
PROJECT_NUMBER=$(gcloud projects list --filter="${PROJECT_ID}" --format="value(PROJECT_NUMBER)")
echo "*******************************************************
Project Id: ${PROJECT_ID}

Project Number: ${PROJECT_NUMBER}

Configurator Cloud Function Endpoint:
  ${URI}
*******************************************************
"
