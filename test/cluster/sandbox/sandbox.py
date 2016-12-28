#!/usr/bin/env python
"""An open source sandbox.

Users should create a new python class inheriting from the Sandbox class.
Then, the user can add sandlets to the sandbox's sandlets attribute. Users
can specify dependencies between sandlets.

"""

import argparse
import datetime
import logging
import os
import random
import re
import subprocess
import yaml

import gke
import sandbox_utils

log_dir = None


def _generate_log_file(filename):
  timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S.%f')
  symlink_name = os.path.join(log_dir, filename)
  timestamped_name = '%s.%s' % (symlink_name, timestamp)
  try:
    os.remove(symlink_name)
  except OSError:
    pass
  os.symlink(timestamped_name, symlink_name)
  return open(timestamped_name, 'w')


def _generate_random_name():
  adjectives = []
  animals = []
  with open('adjectives.txt', 'r') as f:
    adjectives = [a for a in f.read().split('\n') if a]
  with open('animals.txt', 'r') as f:
    animals = [a for a in f.read().split('\n') if a]
  return '%s%s' % (random.choice(adjectives), random.choice(animals))


def parse_config(
    config, sandbox_name=None, cluster_name=None, cluster_type='gke'):
  random_name = _generate_random_name()
  ret_val = re.sub('{{sandbox_name}}', sandbox_name or random_name, config)
  ret_val = re.sub('{{cluster_name}}', cluster_name or random_name, ret_val)
  ret_val = re.sub('{{cluster_type}}', cluster_type, ret_val)
  return ret_val


class SandboxError(Exception):
  pass


class Sandbox(object):
  """Sandbox class."""

  _cluster_envs = {
      'gke': gke,
  }

  def _get_sandlets(self, sandlets):
    """Returns sandlet objects based on input sandlet string names."""
    if not sandlets:
      return None
    retval = []
    for sandlet in sandlets:
      sandlet_obj = next((x for x in self.sandlets if x.name == sandlet), None)
      if not sandlet_obj:
        raise Exception('No sandlet found with name: %s' % sandlet)
      retval.append(sandlet_obj)
    return retval

  def __init__(self, sandbox_options):
    self.sandbox_options = sandbox_options
    self.name = sandbox_options.get('name')
    self.cluster_type = sandbox_options.get('cluster_type')
    if self.cluster_type not in self._cluster_envs.keys():
      logging.fatal('Invalid cluster type %s', self.cluster_type)
    cluster_config = [c for c in sandbox_options.get('clusters')
                      if c['type'] == self.cluster_type]
    if not cluster_config:
      logging.fatal('Cluster config %s not listed in sandbox config',
                    cluster_config)
    self.cluster_config = cluster_config[0]
    self.cluster_env = self._cluster_envs[self.cluster_type]
    self.cluster = self.cluster_env.Cluster(self.cluster_config)
    self.sandlets = []

  def set_log_dir(self, log_dir_in=None):
    global log_dir
    if log_dir_in:
      log_dir = log_dir_in
    else:
      log_dir = os.path.join(os.getenv('HOME', '/tmp'), 'sandbox_logs')
    if not os.path.exists(log_dir):
      os.makedirs(log_dir)

  def save(self, filename):
    output = self.__dict__
    for x in range(len(output['sandlets'])):
      sandlet = output['sandlets'][x].__dict__
      output['sandlets'][x] = sandlet
      for y in range(len(sandlet['components'])):
        sandlet['components'][y] = sandlet['components'][y].__dict__

    with open(filename, 'w') as f:
      f.write(yaml.dump(output, default_flow_style=False))

  def start(self, sandlets=None):
    self.start_cluster()
    self.start_sandlets(sandlets)

  def stop(self, sandlets=None):
    self.stop_sandlets(sandlets)
    self.stop_cluster()

  def start_cluster(self):
    if not self.cluster_type:
      return
    self.cluster.start()

  def stop_cluster(self):
    if not self.cluster:
      return
    self.cluster.stop()

  def _execute_sandlets(self, start=True, sandlets=None):
    sandlets_to_execute = self._get_sandlets(sandlets) or self.sandlets
    sandlet_graph = sandbox_utils.create_dependency_graph(
        sandlets_to_execute, reverse=(not start))
    while sandlet_graph:
      ready_sandlets = [
          (k, v[1]) for (k, v) in sandlet_graph.items() if not v[0]]
      for sandlet_name, sandlet in ready_sandlets:
        if start:
          sandlet.start()
        else:
          sandlet.stop()
        del sandlet_graph[sandlet_name]
        for _, (dependencies, _) in sandlet_graph.items():
          if sandlet_name in dependencies:
            dependencies.remove(sandlet_name)

  def start_sandlets(self, sandlets=None):
    self._execute_sandlets(start=True, sandlets=sandlets)

  def stop_sandlets(self, sandlets=None):
    self._execute_sandlets(start=False, sandlets=sandlets)

  def print_banner(self):
    pass

  def generate_from_config(self):
    raise NotImplementedError('Generate From Config file not implemented!')


class SandletComponent(object):
  """Top level component of a sandlet."""

  def __init__(self, name, sandbox_name):
    self.name = name
    self.sandbox_name = sandbox_name
    self.dependencies = []

  def start(self):
    logging.info('Starting component %s', self.name)

  def stop(self):
    pass

  def is_up(self):
    return True

  def is_down(self):
    return True


class Subprocess(SandletComponent):

  def __init__(self, name, sandbox_name, script, **script_kwargs):
    super(Subprocess, self).__init__(name, sandbox_name)
    self.script = script
    self.script_kwargs = script_kwargs

  def start(self):
    super(Subprocess, self).start()
    try:
      infofile = None
      errorfile = None
      script_args = (
          [item for sublist in
           [('--%s' % k, str(v)) for k, v in self.script_kwargs.items()]
           for item in sublist])
      logging.info('Executing subprocess script %s', self.script)
      infofile = _generate_log_file('%s.INFO' % self.name)
      errorfile = _generate_log_file('%s.ERROR' % self.name)
      subprocess.call(['./%s' % self.script] + script_args, stdout=infofile,
                      stderr=errorfile)
      logging.info('Done')
    except subprocess.CalledProcessError as error:
      raise SandboxError('Subprocess %s returned errorcode %d, result %s' % (
          self.script, error.returncode, error.output))
    finally:
      if infofile:
        infofile.close()
      if errorfile:
        errorfile.close()


def sandbox_main(sandbox_cls):
  """Main."""
  logging.getLogger().setLevel(logging.INFO)
  parser = argparse.ArgumentParser(description='Create a sandbox')
  parser.add_argument('-e', metavar='environment', help='Cluster environment',
                      default='gke')
  parser.add_argument('-c', metavar='config_file', help='Sandbox config file')
  parser.add_argument('-n', metavar='sandbox_name', help='Sandbox name')
  parser.add_argument('-k', metavar='cluster_name', help='Cluster name')
  parser.add_argument('-l', metavar='log_dir', help='Directory for logs',
                      default=None)
  parser.add_argument('-s', metavar='sandlets', help='Sandlets')
  parser.add_argument(
      '-a', metavar='action',
      choices=['Start', 'StartCluster', 'StartApp', 'StopApp', 'Stop',
               'StopCluster', 'PrintSandlets', 'PrintBanner'])
  sandbox_args = parser.parse_args()
  sandlets = [] if not sandbox_args.s else sandbox_args.s.split(',')

  with open(sandbox_args.c, 'r') as yaml_file:
    yaml_config = yaml_file.read()
  sandbox_config = yaml.load(
      parse_config(yaml_config, sandbox_args.n, sandbox_args.k,
                   sandbox_args.e))['sandbox']
  sandbox = sandbox_cls(sandbox_config)
  sandbox.set_log_dir(sandbox_args.l)
  sandbox.generate_from_config()

  if sandbox_args.a == 'Start':
    sandbox.start(sandlets)
    sandbox.print_banner()
  elif sandbox_args.a == 'StartCluster':
    sandbox.start_cluster()
  elif sandbox_args.a == 'StopCluster':
    sandbox.stop_cluster()
  elif sandbox_args.a == 'StartApp':
    sandbox.start_sandlets(sandlets)
    sandbox.print_banner()
  elif sandbox_args.a == 'StopApp':
    sandbox.stop_sandlets(sandlets)
  elif sandbox_args.a == 'Stop':
    sandbox.stop(sandlets)
  elif sandbox_args.a == 'PrintSandlets':
    logging.info('Sandlets: %s', ', '.join(s.name for s in sandbox.sandlets))
  elif sandbox_args.a == 'PrintBanner':
    sandbox.print_banner()
  else:
    logging.info('No action selected')
