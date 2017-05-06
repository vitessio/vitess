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

"""Google Compute Engine sandbox components."""

import logging
import os
import subprocess

import sandbox
import sandlet


class Cluster(object):
  """Google Container Engine Cluster."""

  _DEFAULT_ZONE = 'us-central1-b'
  _DEFAULT_MACHINE_TYPE = 'n1-standard-4'
  _DEFAULT_NODE_COUNT = 5

  def __init__(self, params):
    self.params = params

  def start(self):
    """Start the GKE cluster."""
    zone = self.params.get('gke_zone', self._DEFAULT_ZONE)
    machine_type = self.params.get('machine_type', self._DEFAULT_MACHINE_TYPE)
    node_count = str(self.params.get('node_count', self._DEFAULT_NODE_COUNT))
    subprocess.call(['gcloud', 'config', 'set', 'compute/zone', zone])
    cluster_create_args = [
        'gcloud', 'container', 'clusters', 'create', self.params['name'],
        '--machine-type', machine_type, '--num-nodes', node_count, '--scopes',
        'storage-rw']
    if 'cluster_version' in self.params:
      cluster_create_args += [
          '--cluster-version=%s' % self.params['cluster_version']]
    try:
      subprocess.check_call(cluster_create_args)
    except subprocess.CalledProcessError as e:
      raise sandbox.SandboxError('Failed to create GKE cluster: %s', e.output)

  def stop(self):
    zone = self.params.get('gke_zone', self._DEFAULT_ZONE)
    subprocess.call(['gcloud', 'container', 'clusters', 'delete',
                     self.params['name'], '-z', zone, '-q'])


class Port(sandlet.SandletComponent):
  """Used for forwarding ports in Google Container Engine."""

  def __init__(self, name, port):
    self.name = name
    self.port = port
    super(Port, self).__init__(name, None)

  def start(self):
    # Check for existence first.
    with open(os.devnull, 'w') as dn:
      # Suppress output for the existence check to prevent unnecessary output.
      firewall_rules = subprocess.check_output(
          ['gcloud', 'compute', 'firewall-rules', 'list', self.name],
          stderr=dn)
      if self.name in firewall_rules:
        logging.info('Firewall rule %s already exists, skipping creation.',
                     self.name)
        return
    subprocess.call(['gcloud', 'compute', 'firewall-rules', 'create',
                     self.name, '--allow', 'tcp:%s' % str(self.port)])

  def stop(self):
    try:
      subprocess.check_call(
          ['gcloud', 'compute', 'firewall-rules', 'delete', self.name, '-q'])
    except subprocess.CalledProcessError:
      logging.warn('Failed to delete firewall rule %s.', self.name)
