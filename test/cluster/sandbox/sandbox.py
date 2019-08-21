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

"""A cluster sandbox encompassing an application along with a cloud environment.

Sandboxes consist of sandlets, which are abstractions used to divide an
application into individual pieces. Sandlets can be dependent on other sandlets.
In addition, they can be started or stopped individually from the command line.

Users must create a new python class inheriting from this Sandbox class. This
combined with the yaml configuration defines the sandbox.
"""

import argparse
import logging
import os
import re
import yaml

from . import gke
from . import sandbox_utils
from . import sandlet


def parse_config(
    config, sandbox_name=None, cluster_name=None, cluster_type='gke'):
  random_name = sandbox_utils.generate_random_name()
  config = re.sub('{{sandbox_name}}', sandbox_name or random_name, config)
  config = re.sub('{{cluster_name}}', cluster_name or random_name, config)
  config = re.sub('{{cluster_type}}', cluster_type, config)
  return config


class SandboxError(Exception):
  pass


class Sandbox(object):
  """Sandbox class providing basic functionality.

  Derive this class in order to specify sandlets and dependencies.
  """

  _cluster_envs = {
      'gke': gke,
  }

  def __init__(self, sandbox_options):
    self.sandbox_options = sandbox_options
    self.name = sandbox_options.get('name')
    self.cluster_type = sandbox_options.get('cluster_type')
    if self.cluster_type not in list(self._cluster_envs.keys()):
      raise SandboxError('Invalid cluster type %s.' % self.cluster_type)
    cluster_config = [c for c in sandbox_options.get('clusters')
                      if c['type'] == self.cluster_type]
    if not cluster_config:
      raise SandboxError(
          'Cluster config %s not listed in sandbox config.' % cluster_config)
    self.cluster_config = cluster_config[0]
    self.cluster_env = self._cluster_envs[self.cluster_type]
    self.cluster = self.cluster_env.Cluster(self.cluster_config)
    self.sandlets = sandlet.ComponentGroup()

  def set_log_dir(self, log_dir_in=None):
    if log_dir_in:
      self.log_dir = log_dir_in
    else:
      self.log_dir = '/tmp/sandbox_logs'
    if not os.path.exists(self.log_dir):
      os.makedirs(self.log_dir)

  def start(self, sandlets=None):
    self.start_cluster()
    self.start_sandlets(sandlets)

  def stop(self, sandlets=None):
    self.stop_sandlets(sandlets)
    self.stop_cluster()

  def start_cluster(self):
    if not self.cluster_type:
      raise SandboxError('Cannot start cluster, no cluster_type defined.')
    self.cluster.start()

  def stop_cluster(self):
    if not self.cluster:
      raise SandboxError('Cannot stop cluster, no cluster_type defined.')
    self.cluster.stop()

  def start_sandlets(self, sandlets=None):
    self.sandlets.execute(sandlet.StartAction, sandlets)

  def stop_sandlets(self, sandlets=None):
    self.sandlets.execute(sandlet.StopAction, sandlets)

  def print_banner(self):
    pass

  def generate_from_config(self):
    raise NotImplementedError('Generate From Config file not implemented!')


def sandbox_main(sandbox_cls):
  """Main.

  Call this function from the main in the derived sandbox class.

  Args:
    sandbox_cls: A derived sandbox class. This will be instantiated in this
                 main function.
  """
  logging.getLogger().setLevel(logging.INFO)
  parser = argparse.ArgumentParser(description='Create a sandbox')
  parser.add_argument(
      '-e', '--environment', help='Cluster environment', default='gke')
  parser.add_argument('-c', '--config_file', help='Sandbox config file')
  parser.add_argument('-n', '--sandbox_name', help='Sandbox name')
  parser.add_argument('-k', '--cluster_name', help='Cluster name')
  parser.add_argument(
      '-l', '--log_dir', help='Directory for logs', default=None)
  parser.add_argument('-s', '--sandlets', help='Sandlets')
  available_actions = ['Start', 'StartCluster', 'StartApp', 'StopApp', 'Stop',
                       'StopCluster', 'PrintSandlets', 'PrintBanner']
  parser.add_argument('-a', '--action', choices=available_actions)
  sandbox_args = parser.parse_args()
  sandlets = []
  if sandbox_args.sandlets:
    sandlets = sandbox_args.sandlets.split(',')

  with open(sandbox_args.config_file, 'r') as yaml_file:
    yaml_config = yaml_file.read()
  sandbox_config = yaml.load(
      parse_config(
          yaml_config, sandbox_args.sandbox_name, sandbox_args.cluster_name,
          sandbox_args.environment))['sandbox']
  sandbox = sandbox_cls(sandbox_config)
  sandbox.set_log_dir(sandbox_args.log_dir)
  sandbox.generate_from_config()

  if sandbox_args.action == 'Start':
    sandbox.start(sandlets)
    sandbox.print_banner()
  elif sandbox_args.action == 'StartCluster':
    sandbox.start_cluster()
  elif sandbox_args.action == 'StopCluster':
    sandbox.stop_cluster()
  elif sandbox_args.action == 'StartApp':
    sandbox.start_sandlets(sandlets)
    sandbox.print_banner()
  elif sandbox_args.action == 'StopApp':
    sandbox.stop_sandlets(sandlets)
  elif sandbox_args.action == 'Stop':
    sandbox.stop(sandlets)
  elif sandbox_args.action == 'PrintSandlets':
    logging.info('Sandlets: %s.', ', '.join(s.name for s in sandbox.sandlets))
  elif sandbox_args.action == 'PrintBanner':
    sandbox.print_banner()
  else:
    logging.info('No available action selected. Choices are: %s.',
                 ', '.join(available_actions))
