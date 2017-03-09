#!/usr/bin/env python
"""Keytar flask app.

This program is responsible for exposing an interface to trigger cluster level
tests. For instance, docker webhooks can be configured to point to this
application in order to trigger tests upon pushing new docker images.
"""

import argparse
import datetime
import json
import logging
import os
import Queue
import shutil
import subprocess
import tempfile
import threading
import yaml

import flask


app = flask.Flask(__name__)
keytar_args = None
keytar_config = None
results = {}


class KeytarError(Exception):
  pass


def run_test_config(config):
  """Runs a single test iteration from a configuration."""
  tempdir = tempfile.mkdtemp()
  logging.info('Fetching github repository')

  # Get the github repo and clone it.
  github_config = config['github']
  github_clone_args, github_repo_dir = _get_download_github_repo_args(
      tempdir, github_config)
  os.makedirs(github_repo_dir)
  subprocess.call(github_clone_args)

  current_dir = os.getcwd()

  timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M')
  _add_new_result(timestamp)
  results[timestamp]['docker_image'] = config['docker_image']

  # Generate a test script with the steps described in the configuration,
  # as well as the command to execute the test_runner.
  with tempfile.NamedTemporaryFile(dir=tempdir, delete=False) as f:
    tempscript = f.name
    f.write('#!/bin/bash\n')
    if 'before_test' in config:
      os.chdir(github_repo_dir)
      for before_step in config['before_test']:
        f.write('%s\n' % before_step)
    f.write(
        'python %s/test_runner.py -c "%s" -t %s -d %s -s '
        'http://localhost:%d' % (
            current_dir, yaml.dump(config), timestamp, tempdir,
            keytar_args.port))
  os.chmod(tempscript, 0775)

  try:
    subprocess.call([tempscript])
  except subprocess.CalledProcessError as e:
    logging.warn('Error running test_runner: %s', str(e))
  finally:
    os.chdir(current_dir)
    shutil.rmtree(tempdir)


@app.route('/')
def index():
  return app.send_static_file('index.html')


@app.route('/test_results')
def test_results():
  # Get all test results or a specific test result in JSON.
  if 'name' not in flask.request.args:
    return json.dumps([results[x] for x in sorted(results)])
  else:
    return json.dumps(
        [x for x in results.itervalues() if x == flask.request.args['name']])


@app.route('/test_log')
def test_log():
  # Fetch the output from a test.
  log = '%s.log' % os.path.basename(flask.request.values['log_name'])
  return (flask.send_from_directory('/tmp/testlogs', log), 200,
          {'Content-Type': 'text/css'})


@app.route('/update_results', methods=['POST'])
def update_results():
  # Update the results dict, called from the test_runner.
  update_args = flask.request.get_json()
  time = update_args['time']
  for k, v in update_args.iteritems():
    results[time][k] = v
  return 'OK'


def _validate_request(keytar_password, request_values):
  if keytar_password:
    if 'password' not in request_values:
      return 'Expected password not provided in test_request!'
    elif request_values['password'] != keytar_password:
      return 'Incorrect password passed to test_request!'


@app.route('/test_request', methods=['POST'])
def test_request():
  """Respond to a post request to execute tests.

  This expects a json payload containing the docker webhook information.
  If this app is configured to use a password, the password should be passed in
  as part of the POST request.

  Returns:
    HTML response.
  """
  validation_error = _validate_request(
      keytar_args.password, flask.request.values)
  if validation_error:
    flask.abort(400, validation_error)
  webhook_data = flask.request.get_json()
  repo_name = webhook_data['repository']['repo_name']
  test_configs = [
      c for c in keytar_config['config'] if c['docker_image'] == repo_name]
  if not test_configs:
    return 'No config found for repo_name: %s' % repo_name
  for test_config in test_configs:
    test_worker.add_test(test_config)
  return 'OK'


def handle_install_steps():
  """Runs all config installation/setup steps."""
  if 'install' not in keytar_config:
    return
  install_config = keytar_config['install']
  if 'cluster_setup' in install_config:
    # Handle any cluster setup steps, currently only GKE is supported.
    for cluster_setup in install_config['cluster_setup']:
      if cluster_setup['type'] == 'gke':
        if 'keyfile' not in cluster_setup:
          raise KeytarError('No keyfile found in GKE cluster setup!')
        # Add authentication steps to allow keytar to start clusters on GKE.
        gcloud_args = ['gcloud', 'auth', 'activate-service-account',
                       '--key-file', cluster_setup['keyfile']]
        logging.info('authenticating using keyfile: %s',
                     cluster_setup['keyfile'])
        subprocess.call(gcloud_args)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
            cluster_setup['keyfile'])

        # Ensure that a project name is correctly set. Use the name if provided
        # in the configuration, otherwise use the current project name, or else
        # the first available project name.
        if 'project_name' in cluster_setup:
          logging.info('Setting gcloud project to %s',
                       cluster_setup['project_name'])
          subprocess.call(
              ['gcloud', 'config', 'set', 'project',
               cluster_setup['project_name']])
        else:
          config = subprocess.check_output(
              ['gcloud', 'config', 'list', '--format', 'json'])
          project_name = json.loads(config)['core']['project']
          if not project_name:
            projects = subprocess.check_output(
                ['gcloud', 'projects', 'list'])
            first_project = projects[0]['projectId']
            logging.info('gcloud project is unset, setting it to %s',
                         first_project)
            subprocess.check_output(
                ['gcloud', 'config', 'set', 'project', first_project])

  # Install any dependencies using apt-get.
  if 'dependencies' in install_config:
    subprocess.call(['apt-get', 'update'])
    os.environ['DEBIAN_FRONTEND'] = 'noninteractive'
    for dep in install_config['dependencies']:
      subprocess.call(
          ['apt-get', 'install', '-y', '--no-install-recommends', dep])

  # Run any additional commands if provided.
  if 'extra' in install_config:
    for step in install_config['extra']:
      os.system(step)

  # Update path environment variable.
  if 'path' in install_config:
    for path in install_config['path']:
      os.environ['PATH'] = '%s:%s' % (path, os.environ['PATH'])


def _add_new_result(timestamp):
  result = {'time': timestamp, 'status': 'Start', 'tests': {}}
  results[timestamp] = result


def _get_download_github_repo_args(tempdir, github_config):
  """Get arguments for github actions.

  Args:
    tempdir: Base directory to git clone into.
    github_config: Configuration describing the repo, branches, etc.

  Returns:
    ([string], string) for arguments to pass to git, and the directory to
    clone into.
  """
  repo_prefix = 'github'
  if 'repo_prefix' in github_config:
    repo_prefix = github_config['repo_prefix']
  repo_dir = os.path.join(tempdir, repo_prefix)
  git_args = ['git', 'clone', 'https://github.com/%s' % github_config['repo'],
              repo_dir]
  if 'branch' in github_config:
    git_args += ['-b', github_config['branch']]
  return git_args, repo_dir


class TestWorker(object):
  """A simple test queue. HTTP requests append to this work queue."""

  def __init__(self):
    self.test_queue = Queue.Queue()
    self.worker_thread = threading.Thread(target=self.worker_loop)
    self.worker_thread.daemon = True

  def worker_loop(self):
    # Run forever, executing tests as they are added to the queue.
    while True:
      item = self.test_queue.get()
      run_test_config(item)
      self.test_queue.task_done()

  def start(self):
    self.worker_thread.start()

  def add_test(self, config):
    self.test_queue.put(config)

test_worker = TestWorker()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  parser = argparse.ArgumentParser(description='Run keytar')
  parser.add_argument('--config_file', help='Keytar config file', required=True)
  parser.add_argument('--password', help='Password', default=None)
  parser.add_argument('--port', help='Port', default=8080, type=int)
  keytar_args = parser.parse_args()
  with open(keytar_args.config_file, 'r') as yaml_file:
    yaml_config = yaml_file.read()
  if not yaml_config:
    raise ValueError('No valid yaml config!')
  keytar_config = yaml.load(yaml_config)
  handle_install_steps()

  if not os.path.isdir('/tmp/testlogs'):
    os.mkdir('/tmp/testlogs')

  test_worker.start()

  app.run(host='0.0.0.0', port=keytar_args.port, debug=True)
