#!/usr/bin/python2.4

import base64
import binascii
import random
import sys
import unittest

import cbson

# only base64 encoded so they aren't a million characters long and/or full
# of escape sequences
KNOWN_BAD = ["VAAAAARBAEwAAAAQMAABAAAAEDEAAgAAABAyAAMAAAAQMwAEAAAAEDQABQAAAAU1AAEAAAAANgI2AAIAAAA3AAM3AA8AAAACQwADAAAARFMAAB0A",
             "VAAAAARBAEwAAAAQMAABAAAAEDEAAgAAABAyAAMAAADTMwAEAAAAEDQABQAAAAU1AAEAAAAANgI2AAIAAAA3AAM3AA8AAAACQwADAAAARFMAAAAA",
             "VAAAAARBAEwAAAAQMAABAAAAEDEAAgAAABAyAAMAAAAQMwAEAAAAEDQABQAAAAU1AAEAAAAANgI2AAIAAAA3AAM3AA8AAAACQ2gDAAAARFMAAAAA",
            ]
KNOWN_BAD = ["VAAAAARBAEwAAAAQMAABAAAAEDEAAgAAABAyAAMAAAAQMwAEAAAAEDQABQAAAAU1AAEAAAAANgI2AAIAAAA3AAM3AA8AAAACQwADAAAARFMAAB0A",
            ]

class CbsonTest(unittest.TestCase):
  def test_short_string_segfaults(self):
    a = cbson.dumps({"A": [1, 2, 3, 4, 5, "6", u"7", {"C": u"DS"}]})
    for i in range(len(a))[1:]:
      try:
        cbson.loads(a[:-i] + (" " * i))
      except Exception:
        pass

  def test_known_bad(self):
    return
    for s in KNOWN_BAD:
      try:
        cbson.loads(base64.b64decode(s))
      except cbson.BSONError:
        pass

  def test_random_segfaults(self):
    a = cbson.dumps({"A": [1, 2, 3, 4, 5, "6", u"7", {"C": u"DS"}]})
    sys.stdout.write("\nQ: %s\n" % (binascii.hexlify(a),))
    for i in range(1000):
      l = [c for c in a]
      l[random.randint(4, len(a)-1)] = chr(random.randint(0, 255))
      try:
        s = "".join(l)
        sys.stdout.write("s: %s\n" % (binascii.hexlify(s),))
        sys.stdout.flush()
        cbson.loads(s)
      except Exception as e:
        sys.stdout.write("  ERROR: %r\n" % (e,))
        sys.stdout.flush()

if __name__ == "__main__":
  unittest.main()

