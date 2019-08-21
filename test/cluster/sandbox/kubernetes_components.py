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

"""Kubernetes sandbox components."""

import json
import logging
import os
import re
import subprocess
import tempfile
import time

from . import sandbox
from . import sandlet


def set_gke_cluster_context(gke_cluster_name):
  logging.info('Changing cluster to %s.', gke_cluster_name)
  clusters = subprocess.check_output(
      ['kubectl', 'config', 'get-clusters']).split('\n')
  cluster = [c for c in clusters if c.endswith('_%s' % gke_cluster_name)]
  if not cluster:
    raise sandbox.SandboxError(
        'Cannot change GKE cluster context, cluster %s not found',
        gke_cluster_name)
  with open(os.devnull, 'w') as devnull:
    subprocess.call(['kubectl', 'config', 'use-context', cluster[0]],
                    stdout=devnull)


class HelmComponent(sandlet.SandletComponent):
  """A helm resource."""

  def __init__(self, name, sandbox_name, helm_config):
    super(HelmComponent, self).__init__(name, sandbox_name)
    self.helm_config = helm_config
    try:
      subprocess.check_output(['helm'], stderr=subprocess.STDOUT)
    except OSError:
      raise sandbox.SandboxError(
          'Could not find helm binary. Please visit '
          'https://github.com/kubernetes/helm to download helm.')

  def start(self):
    logging.info('Initializing helm.')
    try:
      subprocess.check_output(['helm', 'init'], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
      raise sandbox.SandboxError('Failed to initialize helm: %s', e.output)

    # helm init on a fresh cluster takes a while to be ready.
    # Wait until 'helm list' returns cleanly.
    with open(os.devnull, 'w') as devnull:
      start_time = time.time()
      while time.time() - start_time < 120:
        try:
          subprocess.check_call(['helm', 'list'], stdout=devnull,
                                stderr=devnull)
          logging.info('Helm is ready.')
          break
        except subprocess.CalledProcessError:
          time.sleep(5)
      else:
        raise sandbox.SandboxError(
            'Timed out waiting for helm to become ready.')

    logging.info('Installing helm.')
    try:
      subprocess.check_output(
          ['helm', 'install', os.path.join(os.environ['VTTOP'], 'helm/vitess'),
           '-n', self.sandbox_name, '--namespace', self.sandbox_name,
           '--replace', '--values', self.helm_config],
          stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
      raise sandbox.SandboxError('Failed to install helm: %s' % e.output)
    logging.info('Finished installing helm.')

  def stop(self):
    subprocess.call(['helm', 'delete', self.sandbox_name, '--purge'])

  def is_up(self):
    return True

  def is_down(self):
    return not bool(subprocess.check_output(
        ['kubectl', 'get', 'pods', '--namespace', self.sandbox_name]))


class KubernetesResource(sandlet.SandletComponent):
  """A Kubernetes resource (pod, replicationcontroller, etc.)."""

  def __init__(self, name, sandbox_name, template_file, **template_params):
    super(KubernetesResource, self).__init__(name, sandbox_name)
    self.template_file = template_file
    self.template_params = template_params

  def start(self):
    super(KubernetesResource, self).start()
    with open(self.template_file, 'r') as template_file:
      template = template_file.read()
    for name, value in list(self.template_params.items()):
      template = re.sub('{{%s}}' % name, str(value), template)
    with tempfile.NamedTemporaryFile() as f:
      f.write(template)
      f.flush()
      os.system('kubectl create --namespace %s -f %s' % (
          self.sandbox_name, f.name))

  def stop(self):
    with open(self.template_file, 'r') as template_file:
      template = template_file.read()
    for name, value in list(self.template_params.items()):
      template = re.sub('{{%s}}' % name, str(value), template)
    with tempfile.NamedTemporaryFile() as f:
      f.write(template)
      f.flush()
      os.system('kubectl delete --namespace %s -f %s' % (
          self.sandbox_name, f.name))

    super(KubernetesResource, self).stop()


def get_forwarded_ip(service, namespace='default', max_wait_s=60):
  """Returns an external IP address exposed by a service."""
  start_time = time.time()
  while time.time() - start_time < max_wait_s:
    try:
      service_info = json.loads(subprocess.check_output(
          ['kubectl', 'get', 'service', service, '--namespace=%s' % namespace,
           '-o', 'json']))
      return service_info['status']['loadBalancer']['ingress'][0]['ip']
    except (KeyError, subprocess.CalledProcessError):
      time.sleep(1)
  return ''

