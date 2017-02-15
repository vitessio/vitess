"""Main python file."""

import argparse
import json
import logging
import os
import subprocess
import urllib2
import uuid
import yaml

keytar_args = None


def update_result(k, v):
  url = '%s/update_results' % keytar_args.server
  req = urllib2.Request(url)
  req.add_header('Content-Type', 'application/json')
  urllib2.urlopen(req, json.dumps({k: v, 'time': keytar_args.timestamp}))


def start_sandbox(environment_config, name):
  try:
    # Start a sandbox
    sandbox_file = os.path.join(repo_dir, environment_config['sandbox'])
    os.chdir(os.path.dirname(sandbox_file))
    sandbox_args = [
        './%s' % os.path.basename(sandbox_file),
        '-e', environment_config['cluster_type'], '-n', name, '-k', name,
        '-c', os.path.join(repo_dir, environment_config['config'])]
    update_result('status', 'Starting Sandbox')
    try:
      subprocess.check_call(sandbox_args + ['-a', 'Start'])
    except subprocess.CalledProcessError as e:
      logging.info('Failed to start sandbox: %s', e.output)
      update_result('status', 'Sandbox failure')

    logging.info('Running tests')
    update_result('status', 'Running Tests')
  except Exception as e:  # pylint: disable=broad-except
    logging.info('Exception caught: %s', str(e))
    update_result('status', 'System Error')


def run_test_config():
  """Runs a single test iteration from a configuration."""
  name = 'keytar%s' % format(uuid.uuid4().fields[0], 'x')
  update_result('name', name)

  environment_config = config['environment']
  if 'sandbox' in environment_config:
    start_sandbox(environment_config, name)

  try:
    # Run tests
    test_results = {}
    for test in config['tests']:
      test_file = os.path.join(repo_dir, test['file'])
      test_name = os.path.basename(test_file)
      logging.info('Running test %s', test_name)
      os.chdir(os.path.dirname(test_file))
      test_args = [
          './%s' % test_name,
          '-e', environment_config['application_type'], '-n', name]
      if 'params' in test:
        test_args += ['-t', ':'.join(
            ['%s=%s' % (k, v) for (k, v) in test['params'].iteritems()])]
      testlog = '/tmp/testlogs/%s_%s.log' % (keytar_args.timestamp, test_name)
      logging.info('Saving log to %s', testlog)
      test_results[test_name] = 'RUNNING'
      update_result('tests', test_results)
      with open(testlog, 'w') as results_file:
        if subprocess.call(test_args, stdout=results_file, stderr=results_file):
          test_results[test_name] = 'FAILED'
        else:
          test_results[os.path.basename(test_file)] = 'PASSED'
      update_result('tests', test_results)
    update_result('status', 'Tests Complete')
  except Exception as e:  # pylint: disable=broad-except
    logging.info('Exception caught: %s', str(e))
    update_result('status', 'System Error')
  finally:
    if 'sandbox' in environment_config:
      sandbox_file = os.path.join(repo_dir, environment_config['sandbox'])
      sandbox_args = [
          './%s' % os.path.basename(sandbox_file),
          '-e', environment_config['cluster_type'], '-n', name, '-k', name,
          '-c', os.path.join(repo_dir, environment_config['config'])]
      os.chdir(os.path.dirname(sandbox_file))
      update_result('status', 'Teardown')
      subprocess.call(sandbox_args + ['-a', 'Stop'])
    update_result('status', 'Complete')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  parser = argparse.ArgumentParser(description='Run keytar')
  parser.add_argument('-c', '--config', help='Keytar config json')
  parser.add_argument('-t', '--timestamp', help='Timestamp string')
  parser.add_argument('-d', '--dir', help='temp dir created for the test')
  parser.add_argument('-s', '--server', help='keytar server address')
  keytar_args = parser.parse_args()
  config = yaml.load(keytar_args.config)
  repo_prefix = 'github'
  if 'repo_prefix' in config['github']:
    repo_prefix = config['github']['repo_prefix']
  repo_dir = os.path.join(keytar_args.dir, repo_prefix)

  run_test_config()
