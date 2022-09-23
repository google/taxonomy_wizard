"""Wrapper class for Jinja2 renderer."""
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     https: // www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import jinja2
from typing import Any

_DEFAULT_TEMPLATES_FILE_PATH: str = 'jinja_templates/'


class JinjaRenderer:
  """Class to help with Jinja2."""
  _env: jinja2.Environment

  def __init__(self, file_path: str = _DEFAULT_TEMPLATES_FILE_PATH):
    loader = jinja2.FileSystemLoader(searchpath=file_path, followlinks=True)
    autoescape = jinja2.select_autoescape(enabled_extensions=('html', 'xml'),
                                          default_for_string=True)
    self._env = jinja2.Environment(autoescape=autoescape, loader=loader)

  def load_and_render_template(self, file_name: str, **kwargs: Any):
    template = self._env.get_template(file_name)
    return template.render(kwargs)
