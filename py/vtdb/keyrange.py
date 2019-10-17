# Copyright 2019 The Vitess Authors.
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

"""Contains the definition of the KeyRange object.
"""

from vtdb import dbexceptions
from vtdb import keyrange_constants


class KeyRange(object):
  """Definition of KeyRange object.

  Vitess uses range based sharding. KeyRange denotes the range
  for the sharding key.

  Attributes:
    Start: start of the keyrange.
    End: end of the keyrange.
  """

  Start = None
  End = None

  def __init__(self, kr):
    if isinstance(kr, str):
      if kr == keyrange_constants.NON_PARTIAL_KEYRANGE:
        self.Start = keyrange_constants.MIN_KEY
        self.End = keyrange_constants.MAX_KEY
        return
      else:
        kr = kr.split('-')
    if not isinstance(kr, tuple) and not isinstance(kr, list) or len(kr) != 2:
      raise dbexceptions.ProgrammingError(
          'keyrange must be a list or tuple or a '-' separated str %s' % kr)
    self.Start = kr[0].strip().decode('hex')
    self.End = kr[1].strip().decode('hex')

  def __str__(self):
    if (self.Start == keyrange_constants.MIN_KEY and
        self.End == keyrange_constants.MAX_KEY):
      return keyrange_constants.NON_PARTIAL_KEYRANGE
    return '%s-%s' % (self.Start.encode('hex'), self.End.encode('hex'))

  def __repr__(self):
    if (self.Start == keyrange_constants.MIN_KEY and
        self.End == keyrange_constants.MAX_KEY):
      return 'KeyRange(%r)' % keyrange_constants.NON_PARTIAL_KEYRANGE
    return 'KeyRange(%r-%r)' % (self.Start, self.End)
