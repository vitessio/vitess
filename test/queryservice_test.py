#!/usr/bin/env python

import logging
import optparse
import traceback
import unittest
import sys
import os

import utils
import framework

from queryservice_tests import cache_tests
from queryservice_tests import nocache_tests
from queryservice_tests import stream_tests
from queryservice_tests import status_tests
from queryservice_tests import test_env

from mysql_flavor import set_mysql_flavor
from protocols_flavor import set_protocols_flavor
from topo_flavor.server import set_topo_server_flavor


if __name__ == "__main__":
  parser = optparse.OptionParser(usage="usage: %prog [options] [test_names]")
  parser.add_option("-m", "--memcache", action="store_true", default=False,
                    help="starts a memcache d, and tests rowcache")
  parser.add_option("-e", "--env", default='vttablet',
                    help="Environment that will be used. Valid options: vttablet, vtocc")
  utils.add_options(parser)
  (options, args) = parser.parse_args()

  logging.getLogger().setLevel(logging.ERROR)
  utils.set_options(options)

  suite = unittest.TestSuite()
  if args:
    if args[0] == 'teardown':
      test_env.TestEnv(options.env).tearDown()
      exit(0)
    for arg in args:
      if hasattr(nocache_tests.TestNocache, arg):
        suite.addTest(nocache_tests.TestNocache(arg))
      elif hasattr(stream_tests.TestStream, arg):
        suite.addTest(stream_tests.TestStream(arg))
      elif hasattr(cache_tests.TestCache, arg) and options.memcache:
        suite.addTest(cache_tests.TestCache(arg))
      elif hasattr(cache_tests.TestWillNotBeCached, arg) and options.memcache:
        suite.addTest(cache_tests.TestWillNotBeCached(arg))
      else:
        raise Exception(arg, "not found in tests")
  else:
    modules = [nocache_tests, stream_tests, status_tests]
    if options.memcache:
      modules.append(cache_tests)
    for m in modules:
      suite.addTests(unittest.TestLoader().loadTestsFromModule(m))

  env = test_env.TestEnv(options.env)
  try:
    env.memcache = options.memcache
    env.setUp()
    print "Starting queryservice_test.py: %s" % options.env
    sys.stdout.flush()
    framework.TestCase.setenv(env)
    result = unittest.TextTestRunner(verbosity=options.verbose, failfast=True).run(suite)
    if not result.wasSuccessful():
      raise Exception("test failures")
  finally:
    if not options.skip_teardown:
      env.tearDown()
    if options.keep_logs:
      print("Leaving temporary files behind (--keep-logs), please "
            "clean up before next run: " + os.environ["VTDATAROOT"])
