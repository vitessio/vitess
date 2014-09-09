#!/usr/bin/env python

import logging
import optparse
import traceback
import unittest
import sys

import utils
import framework

from queryservice_tests import cache_tests
from queryservice_tests import nocache_tests
from queryservice_tests import stream_tests
from queryservice_tests import status_tests
from queryservice_tests import test_env


if __name__ == "__main__":
  parser = optparse.OptionParser(usage="usage: %prog [options] [test_names]")
  parser.add_option("-m", "--memcache", action="store_true", default=False,
                    help="starts a memcache d, and tests rowcache")
  parser.add_option("-e", "--env", default='vttablet',
                    help="Environment that will be used. Valid options: vttablet, vtocc")
  parser.add_option("-q", "--quiet", action="store_const", const=0, dest="verbose", default=1)
  parser.add_option("-v", "--verbose", action="store_const", const=2, dest="verbose", default=0)
  parser.add_option('--skip-teardown', action='store_true')
  (options, args) = parser.parse_args()
  utils.options = options
  logging.getLogger().setLevel(logging.ERROR)

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

