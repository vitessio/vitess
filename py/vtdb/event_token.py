# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Helper methods to compare EventToken objects.
"""


MARIADB_FLAVOR = 'MariaDB'
MYSQL_FLAVOR = 'MySQL56'


def fresher(ev1, ev2):
  """Returns a comparison of the tokens.

  If we can't figure it out, this method will return -1, meaning ev1
  is considered older in doubt.

  Args:
    ev1: the first event token.
    ev2: the second event token.

  Returns:
    value: a negative number of ev1<ev2, 0 if ev1 == ev2, a positive
           number if ev1>ev2.

  """
  if ev1 is None or ev2 is None:
    # Either one is None, we don't know.
    return -1

  if ev1.timestamp != ev2.timestamp:
    # The timestamp is enough to set them apart.
    return ev1.timestamp - ev2.timestamp

  if ev1.shard and ev1.shard == ev2.shard:
    # They come from the same shard, we can parse them and compare.
    if not ev1.position or not ev2.position:
      # We do not know.
      return -1

    # Split them up.
    parts1 = ev1.position.split('/', 1)
    parts2 = ev2.position.split('/', 1)
    if len(parts1) != 2 or len(parts2) != 2:
      # This should never happen, but let's just handle it.
      return -1
    if parts1[0] != parts2[0]:
      # This should never happen, but let's just handle it.
      return -1
    pos1 = parts1[1]
    pos2 = parts2[1]

    if parts1[0] == MARIADB_FLAVOR:
      # Both GTIDSets should be of the form <domain>-<serverid>-<sequence>.
      # We just compare the sequence as int.
      parts1 = pos1.split('-', 2)
      parts2 = pos2.split('-', 2)
      if len(parts1) != 3 or len(parts2) != 3:
        # This should never happen, but let's just handle it.
        return -1

      return int(parts1[2]) - int(parts2[2])

    elif parts1[0] == MYSQL_FLAVOR:
      # Not implemented yet.
      pass

  # We do not know.
  return -1
