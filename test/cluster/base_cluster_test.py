"""Base class for all cluster tests."""

import logging
import optparse
import unittest
import sys

_options = None


class BaseClusterTest(unittest.TestCase):

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

    cls.created_environment = False
    if _options.name:
      cls.env = environment()
      cls.env.use_named(_options.name)
    elif _options.environment_params:
      cls.env = environment()
      environment_params = dict((k, v.replace(':', ',')) for k, v in (
          param.split('=') for param in _options.environment_params.split(',')))
      cls.env.create(**environment_params)
      cls.created_environment = True
    else:
      cls.create_default_local_environment()
    if not cls.env:
      logging.fatal('No test environment exists to test against!')
    if _options.test_params:
      cls.test_params = dict((k, v.replace(':', ',')) for k, v in (
          param.split('=') for param in _options.test_params.split(',')))
    else:
      cls.test_params = {}

  @classmethod
  def tearDownClass(cls):
    if cls.created_environment:
      logging.info('Tearing down environment')
      cls.env.destroy()

  @classmethod
  def create_default_local_environment(cls):
    pass


def main():
  import utils  # pylint: disable=g-import-not-at-top
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
  _options, _ = parser.parse_args()
  del sys.argv[1:]

  utils.main()
