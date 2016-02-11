"""Base class for all end2end tests."""

import logging
import optparse
import unittest
import sys

import environment
import utils

_options = None


class BaseEnd2EndTest(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    environment = None
    if _options.environment_type == 'k8s':
      import k8s_environment  # pylint: disable=g-import-not-at-top
      environment = k8s_environment.K8sEnvironment
    if _options.environment_type == 'local':
      import local_environment  # pylint: disable=g-import-not-at-top
      environment = local_environment.LocalEnvironment
    elif _options.environment_type == 'aws':
      import aws_environment  # pylint: disable=g-import-not-at-top
      environment = aws_environment.AwsEnvironment

    if not environment:
      logging.fatal('No environment type selected')

    if _options.name:
      cls.env = environment()
      cls.env.use_named(_options.name)
    elif _options.environment_params:
      cls.env = environment()
      environment_params = dict((k, v) for k, v in (
        param.split('=') for param in _options.test_params.split(',')))
      cls.env.create(**environment_params)
    else:
      cls.create_default_local_environment()
    if not cls.env:
      logging.fatal('No test environment exists to test against!')
    cls.test_params = dict((k, v) for k, v in (
        param.split('=') for param in _options.test_params.split(',')))

  @classmethod
  def create_default_local_environment(cls):
    pass


def main():
  parser = optparse.OptionParser(usage='usage: %prog [options] [test_names]')
  parser.add_option('-e', '--environment_type', help='Environment type',
                    default=None)
  parser.add_option('-n', '--name', help='Environment name', default=None)
  parser.add_option('-p', '--environment_params',
                    help='Environment parameters if creating an environment '
                    'for the test', default=None)
  parser.add_option('-t', '--test_params', help='Test parameters',
                    default=None)
  utils.add_options(parser)
  global _options
  _options, args = parser.parse_args()
  del sys.argv[1:]

  utils.set_log_level(_options.verbose)
  utils.set_options(_options)
  unittest.main()
