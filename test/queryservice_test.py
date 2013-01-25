#!/usr/bin/env python

import logging
import optparse
import unittest
import sys

import utils
import framework

from queryservice_tests import cache_tests
from queryservice_tests import nocache_tests
from queryservice_tests import stream_tests
from queryservice_tests import test_env


if __name__ == "__main__":
  parser = optparse.OptionParser(usage="usage: %prog [options] [test_names]")
  parser.add_option("-m", "--memcache", action="store_true", default=False,
                    help="starts a memcached, and tests rowcache")
  parser.add_option("-e", "--env", default='vttablet,vtocc',
                    help="Environment that will be used. Valid options: vttablet, vtocc")
  parser.add_option("-q", "--quiet", action="store_const", const=0, dest="verbose", default=1)
  parser.add_option("-v", "--verbose", action="store_const", const=2, dest="verbose", default=1)
  parser.add_option("--no-build", action="store_true")
  (options, args) = parser.parse_args()
  utils.options = options
  logging.getLogger().setLevel(logging.ERROR)

  suite = unittest.TestSuite()
  if args:
    for arg in args:
      if hasattr(nocache_tests.TestNocache, arg):
        suite.addTest(nocache_tests.TestNocache(arg))
      elif hasattr(stream_tests.TestStream, arg):
        suite.addTest(stream_tests.TestStream(arg))
      elif hasattr(cache_tests.TestCache, arg) and options.memcache:
        suite.addTest(cache_tests.TestCache(arg))
      else:
        raise Exception(arg, "not found in tests")
  else:
    suite.addTests(unittest.TestLoader().loadTestsFromModule(nocache_tests))
    suite.addTests(unittest.TestLoader().loadTestsFromModule(stream_tests))
    if options.memcache:
      suite.addTests(unittest.TestLoader().loadTestsFromModule(cache_tests))

  for env_name in options.env.split(','):
    try:
      if env_name == 'vttablet':
        env = test_env.VttabletTestEnv()
      elif env_name == 'vtocc':
        env = test_env.VtoccTestEnv()
      else:
        raise Exception("Valid options for -e: vtocc, vttablet")

      env.setUp()
      print "Starting queryservice_test.py: %s" % env_name
      sys.stdout.flush()
      framework.TestCase.setenv(env)
      unittest.TextTestRunner(verbosity=options.verbose).run(suite)
    finally:
      env.tearDown()
