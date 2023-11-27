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

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

printf 'Install the Configuration backend?\n'
select yn in "Yes" "No"; do
  case $yn in
  Yes)
    install_configuration=true
    break
    ;;
  No)
    install_configuration=false
    break
    ;;

  esac
  printf 'Please choose a valid option.\n'
done

printf 'Install the Validation backend?\n'
select yn in "Yes" "No"; do
  case $yn in
  Yes)
    install_validation=true
    break
    ;;
  No)
    install_validation=false
    break
    ;;

  esac
  printf 'Please choose a valid option.\n'
done

printf 'Autoconfigure the Validation Sheets Plugin (must be manually deployed)?\n'
select yn in "Yes" "No"; do
  case $yn in
  Yes)
    configure_plugin=true
    break
    ;;
  No)
    configure_plugin=false
    break
    ;;

  esac
  printf 'Please choose a valid option.\n'
done

if [ ${install_configuration} = true ]; then
  /$SCRIPT_DIR/resources/python_cloud_functions/configurator/deploy.sh
fi

if [ ${install_validation} = true ]; then
  /$SCRIPT_DIR/resources/python_cloud_functions/validator/deploy.sh
fi

if [ ${configure_plugin} = true ]; then
  /$SCRIPT_DIR/resources/apps_script/validator/deploy.sh
fi

if [ ${install_configuration} = true ]; then
  /$SCRIPT_DIR/resources/python_cloud_functions/configurator/manual_steps.sh
fi

if [ ${install_validation} = true ]; then
  /$SCRIPT_DIR/resources/python_cloud_functions/validator/manual_steps.sh
fi

if [ ${configure_plugin} = true ]; then
  /$SCRIPT_DIR/resources/apps_script/validator/manual_steps.sh
fi

if [ ${install_configuration} = true ]; then
  /$SCRIPT_DIR/resources/apps_script/configurator/manual_steps.sh
fi
