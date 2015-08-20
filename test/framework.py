# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import os
import shlex
from subprocess import Popen, PIPE
import time
import unittest

import utils

class TestCase(unittest.TestCase):
  @classmethod
  def setenv(cls, env):
    cls.env = env

  def assertContains(self, b, a):
    self.assertIn(a, b)

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

class Tailer(object):
  def __init__(self, filepath, flush=None, sleep=0, timeout=10.0):
    self.filepath = filepath
    self.flush = flush
    self.sleep = sleep
    self.timeout = timeout
    self.f = None
    self.reset()

  def reset(self):
    """Call reset when you want to start using the tailer."""
    if self.flush:
      self.flush()
    else:
      time.sleep(self.sleep)

    # Re-open the file if open.
    if self.f:
      self.f.close()
      self.f = None
    # Wait for file to exist.
    timeout = self.timeout
    while not os.path.exists(self.filepath):
      timeout = utils.wait_step('file exists: ' + self.filepath, timeout)
    self.f = open(self.filepath)

    self.f.seek(0, os.SEEK_END)
    self.pos = self.f.tell()

  def read(self):
    """Returns a string which may contain multiple lines."""
    if self.flush:
      self.flush()
    else:
      time.sleep(self.sleep)
    self.f.seek(0, os.SEEK_END)
    newpos = self.f.tell()
    if newpos < self.pos:
      return ""
    self.f.seek(self.pos, os.SEEK_SET)
    size = newpos-self.pos
    self.pos = newpos
    return self.f.read(size)

  def readLines(self):
    """Returns a list of read lines."""
    return self.read().splitlines()

# FIXME: Hijacked from go/vt/tabletserver/test.py
# Reuse when things come together
def execute(cmd, trap_output=False, verbose=False, **kargs):
  args = shlex.split(cmd)
  if trap_output:
    kargs['stdout'] = PIPE
    kargs['stderr'] = PIPE
  if verbose:
    print "Execute:", cmd, ', '.join('%s=%s' % x for x in kargs.iteritems())
  proc = Popen(args, **kargs)
  proc.args = args
  stdout, stderr = proc.communicate()
  if proc.returncode:
    raise Exception('FAIL: %s %s %s' % (args, stdout, stderr))
  return stdout, stderr
