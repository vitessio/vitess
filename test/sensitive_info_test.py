#!/usr/bin/env python

import logging
import optparse
import subprocess
import unittest
import sys

import utils
import framework

from queryservice_tests.cases_framework import Case
from queryservice_tests import test_env

class SensitiveModeTest(framework.TestCase):
  def test_query_stats(self):
    cu = self.env.execute("select * from vtocc_test limit 2")
    stats = self.env.query_stats()
    want = "select * from vtocc_test limit ?"
    for query in stats:
      if query["Query"] == want:
        return
    self.fail("%s not found in %s" % (want, stats))

    self.env.conn._stream_execute("select intval, 1 from vtocc_test limit 2", {})
    r = self.env.conn._stream_next()
    while r:
      r = self.env.conn._stream_next()

if __name__ == "__main__":
  parser = optparse.OptionParser(usage="usage: %prog [options] [test_names]")
  parser.add_option("-e", "--env", default='vttablet,vtocc',
                    help="Environment that will be used. Valid options: vttablet, vtocc")
  parser.add_option("-q", "--quiet", action="store_const", const=0, dest="verbose", default=1)
  parser.add_option("-v", "--verbose", action="store_const", const=2, dest="verbose", default=0)
  (options, args) = parser.parse_args()
  utils.options = options
  logging.getLogger().setLevel(logging.ERROR)

  suite = unittest.TestLoader().loadTestsFromTestCase(SensitiveModeTest)

  for env_name in options.env.split(','):
    try:
      if env_name == 'vttablet':
        env = test_env.VttabletTestEnv()
        env.sensitive_mode = True
      elif env_name == 'vtocc':
        env = test_env.VtoccTestEnv()
        env.sensitive_mode = True
      else:
        raise Exception("Valid options for -e: vtocc, vttablet")

      env.setUp()
      print "Starting sensitive_info_test.py: %s" % env_name
      sys.stdout.flush()
      framework.TestCase.setenv(env)
      result = unittest.TextTestRunner(verbosity=options.verbose).run(suite)
      if not result.wasSuccessful():
        raise Exception("test failures")
    finally:
      env.tearDown()

