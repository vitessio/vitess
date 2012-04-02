# Copyright 2012, Google Inc.
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:

#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import os
import traceback

class MultiDict(dict):
  def __getattr__(self, name):
    v = self[name]
    if type(v)==dict:
      v=MultiDict(v)
    return v

  def mget(self, mkey, default=None):
    keys = mkey.split(".")
    try:
      v = self
      for key in keys:
        v = v[key]
    except KeyError:
      v = default
    if type(v)==dict:
      v = MultiDict(v)
    return v

class TestException(Exception):
  pass

class TestCase(object):
  def __init__(self, testcase=None, verbose=False):
    self.testcase = testcase
    self.verbose = verbose

  def run(self):
    error_count = 0
    try:
      self.setUp()
      if self.testcase is None:
        testlist = [v for k, v in self.__class__.__dict__.iteritems() if k.startswith("test_")]
      else:
        testlist = [self.__class__.__dict__[self.testcase]]
      for testfunc in testlist:
        try:
          testfunc(self)
        except TestException, e:
          print e
          error_count += 1
    finally:
      self.tearDown()
      if error_count == 0:
        print "GREAT SUCCESS"
      else:
        print "Errors:", error_count

  def assertNotEqual(self, val1, val2):
    if val1 == val2:
      raise TestException(self._format("FAIL: %s == %s"%(str(val1), str(val2))))
    elif self.verbose:
      print self._format("PASS")

  def assertEqual(self, val1, val2):
    if val1 != val2:
      raise TestException(self._format("FAIL: %s != %s"%(str(val1), str(val2))))
    elif self.verbose:
      print self._format("PASS")

  def assertFail(self, msg):
    raise TestException(self._format("FAIL: %s"%msg))

  def assertStartsWith(self, val, prefix):
    if not val.startswith(prefix):
      raise TestException(self._format("FAIL: %s does not start with %s"%(str(val), str(prefix))))

  def assertContains(self, val, substr):
    if substr not in val:
      raise TestException(self._format("FAIL: %s does not contain %s"%(str(val), str(substr))))

  def _format(self, msg):
    frame = traceback.extract_stack()[-3]
    if self.verbose:
      return "Function: %s, Line %d: %s: %s"%(frame[2], frame[1], frame[3], msg)
    else:
      return "Function: %s, Line %d: %s"%(frame[2], frame[1], msg)

  def setUp(self):
    pass

  def tearDown(self):
    pass

class Tailer(object):
  def __init__(self, f):
    self.f = f
    self.reset()

  def reset(self):
    self.f.seek(0, os.SEEK_END)
    self.pos = self.f.tell()

  def read(self):
    self.f.seek(0, os.SEEK_END)
    newpos = self.f.tell()
    if newpos < self.pos:
      return ""
    self.f.seek(self.pos, os.SEEK_SET)
    size = newpos-self.pos
    self.pos = newpos
    return self.f.read(size)
