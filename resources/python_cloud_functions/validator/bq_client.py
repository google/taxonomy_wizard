# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https: // www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# Helper class to minimize # of `bigquery.Client` instances (1/set of scopes).

from google import auth
from google.cloud import bigquery
import hashlib
import json
from typing import Mapping, Sequence

_MANDATORY_SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/spreadsheets',
]


class BqClient():
  """Class to minimize # of `bigquery.Client` instances (1/set of scopes)."""
  _clients: Mapping[str, bigquery.Client] = dict()

  def get(self, additional_scopes: Sequence[str] = []) -> bigquery.Client:
    """Gets/creates `bigquery.Client` with the required scopes.

    Args:
        scopes (Sequence[str], optional): Additioanl Scopes to use.

    Returns:
        bigquery.Client: bigquery.Client with the required scopes.
    """
    scopes = _MANDATORY_SCOPES + additional_scopes

    scopes.sort()
    uid: str = hashlib.md5(json.dumps(scopes).encode('utf-8')).hexdigest()

    if not self._clients.get(uid):
      credentials, project = auth.default(scopes=scopes)
      self._clients[uid] = bigquery.Client(credentials=credentials,
                                           project=project)

    return self._clients[uid]
