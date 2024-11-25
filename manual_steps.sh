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

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

echo "Please perform the following steps after running the deploy.sh script:
"

/$SCRIPT_DIR/resources/python_cloud_functions/configurator/manual_steps.sh
/$SCRIPT_DIR/resources/apps_script/validator/manual_steps.sh
/$SCRIPT_DIR/resources/apps_script/configurator/manual_steps.sh
