#!/usr/bin/python2.4

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import os
import os.path
import struct
import sys

sys.path.append(os.path.split(os.path.abspath(__file__))[0])
import cbson

def test_load_empty():
  """
  >>> cbson.loads('')
  Traceback (most recent call last):
  ...
  BSONError: empty buffer
  """

def test_load_binary():
  r"""
  >>> s = cbson.dumps({'world': 'hello'})
  >>> s
  '\x16\x00\x00\x00\x05world\x00\x05\x00\x00\x00\x00hello\x00'
  >>> cbson.loads(s)
  {'world': 'hello'}
  """

def test_load_string():
  r"""
  >>> s = cbson.dumps({'world': u'hello \u00fc'})
  >>> s
  '\x19\x00\x00\x00\x02world\x00\t\x00\x00\x00hello \xc3\xbc\x00\x00'
  >>> cbson.loads(s)
  {'world': u'hello \xfc'}
  """

def test_load_int():
  r"""
  >>> s = cbson.dumps({'int': 1334})
  >>> s
  '\x0e\x00\x00\x00\x10int\x006\x05\x00\x00\x00'
  >>> cbson.loads(s)
  {'int': 1334}
  """

# the library doesn't allow creation of these, so just check the unpacking
def test_unpack_uint64():
  r"""
  >>> s = '\x12\x00\x00\x00\x3fint\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00'
  >>> cbson.loads(s)
  {'int': 1L}
  >>> s = '\x12\x00\x00\x00\x3fint\x00\x00\x00\x00\x00\x00\x00\x00\x90\x00'
  >>> cbson.loads(s)
  {'int': 10376293541461622784L}
  """

def test_bool():
  """
  >>> cbson.loads(cbson.dumps({'yes': True, 'no': False}))
  {'yes': True, 'no': False}
  """

def test_none():
  r"""
  >>> s = cbson.dumps({'none': None})
  >>> s
  '\x0b\x00\x00\x00\nnone\x00\x00'
  >>> cbson.loads(s)
  {'none': None}
  """

def test_array():
  r"""
  >>> s = cbson.dumps({'lst': [1, 2, 3, 4, 'hello', None]})
  >>> s
  ';\x00\x00\x00\x04lst\x001\x00\x00\x00\x100\x00\x01\x00\x00\x00\x101\x00\x02\x00\x00\x00\x102\x00\x03\x00\x00\x00\x103\x00\x04\x00\x00\x00\x054\x00\x05\x00\x00\x00\x00hello\n5\x00\x00\x00'
  >>> cbson.loads(s)
  {'lst': [1, 2, 3, 4, 'hello', None]}
  """

def test_dict():
  """
  >>> d = {'a': None, 'b': 2, 'c': 'hello', 'd': 1.02, 'e': ['a', 'b', 'c']}
  >>> cbson.loads(cbson.dumps(d)) == d
  True
  """

def test_nested_dict():
  r"""
  >>> s = cbson.dumps({'a': {'b': {'c': 0}}})
  >>> s
  '\x1c\x00\x00\x00\x03a\x00\x14\x00\x00\x00\x03b\x00\x0c\x00\x00\x00\x10c\x00\x00\x00\x00\x00\x00\x00\x00'
  >>> cbson.loads(s)
  {'a': {'b': {'c': 0}}}
  """

def test_dict_in_array():
  r"""
  >>> s = cbson.dumps({'a': [{'b': 0}, {'c': 1}]})
  >>> s
  '+\x00\x00\x00\x04a\x00#\x00\x00\x00\x030\x00\x0c\x00\x00\x00\x10b\x00\x00\x00\x00\x00\x00\x031\x00\x0c\x00\x00\x00\x10c\x00\x01\x00\x00\x00\x00\x00\x00'
  >>> cbson.loads(s)
  {'a': [{'b': 0}, {'c': 1}]}
  """

def test_eob1():
  """
  >>> cbson.loads(BSON(BSON_TAG(1), NULL_BYTE))
  Traceback (most recent call last):
  ...
  BSONError: unexpected end of buffer: wanted 8 bytes at buffer[6] for double
  """

def test_eob2():
  """
  >>> cbson.loads(BSON(BSON_TAG(2), NULL_BYTE))
  Traceback (most recent call last):
  ...
  BSONError: unexpected end of buffer: wanted 4 bytes at buffer[6] for string-length
  """

def test_eob_cstring():
  """
  >>> cbson.loads(BSON(BSON_TAG(1)))
  Traceback (most recent call last):
  ...
  BSONError: unexpected end of buffer: non-terminated cstring at buffer[5] for element-name
  """

def test_decode_next():
  """
  >>> s = cbson.dumps({'a': 1}) + cbson.dumps({'b': 2.0}) + cbson.dumps({'c': [None]})
  >>> cbson.decode_next(s)
  (12, {'a': 1})
  >>> cbson.decode_next(s, 12)
  (28, {'b': 2.0})
  >>> cbson.decode_next(s, 28)
  (44, {'c': [None]})
  """

def test_decode_next_eob():
  """
  >>> s_full = cbson.dumps({'a': 1}) + cbson.dumps({'b': 2.0})
  >>> s = s_full[:-1]
  >>> cbson.decode_next(s)
  (12, {'a': 1})
  >>> cbson.decode_next(s, 12)
  Traceback (most recent call last):
  ...
  BSONBufferTooShort: ('buffer too short: buffer[12:] does not contain 12 bytes for document', 1)

  >>> s = s_full[:-2]
  >>> cbson.decode_next(s)
  (12, {'a': 1})
  >>> cbson.decode_next(s, 12)
  Traceback (most recent call last):
  ...
  BSONBufferTooShort: ('buffer too short: buffer[12:] does not contain 12 bytes for document', 2)
  """

def test_encode_recursive():
  """
  >>> a = []
  >>> a.append(a)
  >>> cbson.dumps({"x": a})
  Traceback (most recent call last):
  ...
  ValueError: object too deeply nested to BSON encode
  """

def test_encode_invalid():
  """
  >>> cbson.dumps(object())
  Traceback (most recent call last):
  ...
  TypeError: bson document must be a dict
  >>> cbson.dumps({'x': object()})
  Traceback (most recent call last):
  ...
  TypeError: unsupported type for BSON encode
  """

def BSON(*l):
  buf = ''.join(l)
  return struct.pack('i', len(buf)+4) + buf

def BSON_TAG(type_id):
  return struct.pack('b', type_id)

NULL_BYTE = '\x00'

if __name__ == "__main__":
  cbson

  import doctest
  print "Running selftest:"
  status = doctest.testmod(sys.modules[__name__])
  if status[0]:
    print "*** %s tests of %d failed." % status
    sys.exit(1)
  else:
    print "--- %s tests passed." % status[1]
    sys.exit(0)
