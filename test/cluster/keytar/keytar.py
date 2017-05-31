#!/usr/bin/env python

# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Keytar flask app.

This program is responsible for exposing an interface to trigger cluster level
tests. For instance, docker webhooks can be configured to point to this
application in order to trigger tests upon pushing new docker images.
"""

import argparse
import collections
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
results = collections.OrderedDict()
_TEMPLATE = (
    'python {directory}/test_runner.py -c "{config}" -t {timestamp} '
    '-d {tempdir} -s {server}')


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
  results[timestamp] = {
      'timestamp': timestamp,
      'status': 'Start',
      'tests': {},
      'docker_image': config['docker_image']
  }

  # Generate a test script with the steps described in the configuration,
  # as well as the command to execute the test_runner.
  with tempfile.NamedTemporaryFile(dir=tempdir, delete=False) as f:
    tempscript = f.name
    f.write('#!/bin/bash\n')
    if 'before_test' in config:
      # Change to the github repo directory, any steps to be run before the
      # tests should be executed from there.
      os.chdir(github_repo_dir)
      for before_step in config['before_test']:
        f.write('%s\n' % before_step)
    server = 'http://localhost:%d' % app.config['port']
    f.write(_TEMPLATE.format(
        directory=current_dir, config=yaml.dump(config), timestamp=timestamp,
        tempdir=tempdir, server=server))
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
  return json.dumps([results[x] for x in sorted(results)])


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
  timestamp = update_args['timestamp']
  results[timestamp].update(update_args)
  return 'OK'


def _validate_request(keytar_password, request_values):
  """Checks a request against the password provided to the service at startup.

  Raises an exception on errors, otherwise returns None.

  Args:
    keytar_password: password provided to the service at startup.
    request_values: dict of POST request values provided to Flask.

  Raises:
    KeytarError: raised if the password is invalid.
  """
  if keytar_password:
    if 'password' not in request_values:
      raise KeytarError('Expected password not provided in test_request!')
    elif request_values['password'] != keytar_password:
      raise KeytarError('Incorrect password passed to test_request!')


@app.route('/test_request', methods=['POST'])
def test_request():
  """Respond to a post request to execute tests.

  This expects a json payload containing the docker webhook information.
  If this app is configured to use a password, the password should be passed in
  as part of the POST request.

  Returns:
    HTML response.
  """
  try:
    _validate_request(app.config['password'], flask.request.values)
  except KeytarError as e:
    flask.abort(400, str(e))
  webhook_data = flask.request.get_json()
  repo_name = webhook_data['repository']['repo_name']
  test_configs = [c for c in app.config['keytar_config']['config']
                  if c['docker_image'] == repo_name]
  if not test_configs:
    return 'No config found for repo_name: %s' % repo_name
  for test_config in test_configs:
    test_worker.add_test(test_config)
  return 'OK'


def handle_cluster_setup(cluster_setup):
  """Setups up a cluster.

  Currently only GKE is supported. This step handles setting up credentials and
  ensuring a valid project name is used.

  Args:
    cluster_setup: YAML cluster configuration.

  Raises:
    KeytarError: raised on invalid setup configurations.
  """
  if cluster_setup['type'] != 'gke':
    return

  if 'keyfile' not in cluster_setup:
    raise KeytarError('No keyfile found in GKE cluster setup!')
  # Add authentication steps to allow keytar to start clusters on GKE.
  gcloud_args = ['gcloud', 'auth', 'activate-service-account',
                 '--key-file', cluster_setup['keyfile']]
  logging.info('authenticating using keyfile: %s', cluster_setup['keyfile'])
  subprocess.call(gcloud_args)
  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cluster_setup['keyfile']

  # Ensure that a project name is correctly set. Use the name if provided
  # in the configuration, otherwise use the current project name, or else
  # the first available project name.
  if 'project_name' in cluster_setup:
    logging.info('Setting gcloud project to %s', cluster_setup['project_name'])
    subprocess.call(
        ['gcloud', 'config', 'set', 'project', cluster_setup['project_name']])
  else:
    config = subprocess.check_output(
        ['gcloud', 'config', 'list', '--format', 'json'])
    project_name = json.loads(config)['core']['project']
    if not project_name:
      projects = subprocess.check_output(['gcloud', 'projects', 'list'])
      first_project = projects[0]['projectId']
      logging.info('gcloud project is unset, setting it to %s', first_project)
      subprocess.check_output(
          ['gcloud', 'config', 'set', 'project', first_project])


def handle_install_steps(keytar_config):
  """Runs all config installation/setup steps.

  Args:
    keytar_config: YAML keytar configuration.
  """
  if 'install' not in keytar_config:
    return
  install_config = keytar_config['install']
  for cluster_setup in install_config.get('cluster_setup', []):
    handle_cluster_setup(cluster_setup)

  # Install any dependencies using apt-get.
  if 'dependencies' in install_config:
    subprocess.call(['apt-get', 'update'])
    os.environ['DEBIAN_FRONTEND'] = 'noninteractive'
    for dep in install_config['dependencies']:
      subprocess.call(
          ['apt-get', 'install', '-y', '--no-install-recommends', dep])

  # Run any additional commands if provided.
  for step in install_config.get('extra', []):
    os.system(step)

  # Update path environment variable.
  for path in install_config.get('path', []):
    os.environ['PATH'] = '%s:%s' % (path, os.environ['PATH'])


def _get_download_github_repo_args(tempdir, github_config):
  """Get arguments for github actions.

  Args:
    tempdir: Base directory to git clone into.
    github_config: Configuration describing the repo, branches, etc.

  Returns:
    ([string], string) for arguments to pass to git, and the directory to
    clone into.
  """
  repo_prefix = github_config.get('repo_prefix', 'github')
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


def main():
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
  handle_install_steps(keytar_config)

  if not os.path.isdir('/tmp/testlogs'):
    os.mkdir('/tmp/testlogs')

  test_worker.start()

  app.config['port'] = keytar_args.port
  app.config['password'] = keytar_args.password
  app.config['keytar_config'] = keytar_config

  app.run(host='0.0.0.0', port=keytar_args.port, debug=True)


if __name__ == '__main__':
  main()
