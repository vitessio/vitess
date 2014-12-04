# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""Contains the definition of the KeyRange object.
"""

from bson import codec
from vtdb import dbexceptions
from vtdb import keyrange_constants


class KeyRange(codec.BSONCoding):
  """Definition of KeyRange object.

  Vitess uses range based sharding. KeyRange denotes the range
  for the sharding key. This class also provides bson encoding
  for this object.

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
      raise dbexceptions.ProgrammingError("keyrange must be a list or tuple or a '-' separated str %s" % kr)
    self.Start = kr[0].strip().decode('hex')
    self.End = kr[1].strip().decode('hex')

  def __str__(self):
    if self.Start == keyrange_constants.MIN_KEY and self.End == keyrange_constants.MAX_KEY:
      return keyrange_constants.NON_PARTIAL_KEYRANGE
    return '%s-%s' % (self.Start.encode('hex'), self.End.encode('hex'))

  def __repr__(self):
    if self.Start == keyrange_constants.MIN_KEY and self.End == keyrange_constants.MAX_KEY:
      return 'KeyRange(%r)' % keyrange_constants.NON_PARTIAL_KEYRANGE
    return 'KeyRange(%r-%r)' % (self.Start, self.End)

  def bson_encode(self):
    """Bson encode KeyRange.

    Returns:
      Dict of start and end values for the KeyRange.
      {"Start": start, "End": end}
    """

    return {"Start": self.Start, "End": self.End}

  def bson_init(self, raw_values):
    """Bson initialize the object with start and end dict.

    Args:
      raw_values: Dictionary of start and end values for keyrange.
    """
    self.Start = raw_values["Start"]
    self.End = raw_values["End"]
