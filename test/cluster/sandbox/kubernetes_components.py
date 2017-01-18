#!/usr/bin/env python
"""Kubernetes sandbox components."""

import json
import logging
import os
import re
import subprocess
import time

import sandbox
import sandlet


def set_gke_cluster_context(gke_cluster_name):
  logging.info('Changing cluster to %s.', gke_cluster_name)
  clusters = subprocess.check_output(
      ['kubectl', 'config', 'get-clusters']).split('\n')
  cluster = [c for c in clusters if c.endswith('_%s' % gke_cluster_name)][0]
  with open(os.devnull, 'w') as dn:
    subprocess.call(['kubectl', 'config', 'use-context', cluster], stdout=dn)


class HelmComponent(sandlet.SandletComponent):
  """A helm resource."""

  def __init__(self, name, sandbox_name, helm_config):
    super(HelmComponent, self).__init__(name, sandbox_name)
    self.helm_config = helm_config

  def start(self):
    logging.info('Initializing helm.')
    with open(os.devnull, 'w') as dn:
      subprocess.call(['helm', 'init'], stdout=dn)
      start_time = time.time()

      # helm init on a fresh cluster takes a while to be ready.
      # Wait until 'helm list' returns cleanly.
      while time.time() - start_time < 120:
        try:
          subprocess.check_call(['helm', 'list'], stdout=dn, stderr=dn)
          logging.info('Helm is ready.')
          break
        except subprocess.CalledProcessError:
          time.sleep(5)
          continue
      else:
        raise sandbox.SandboxError(
            'Timed out waiting for helm to become ready.')
      logging.info('Installing helm.')
      subprocess.call(
          ['helm', 'install', os.path.join(os.environ['VTTOP'], 'helm/vitess'),
           '-n', self.sandbox_name, '--namespace', self.sandbox_name,
           '--replace', '--values', self.helm_config], stdout=dn)
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
    for name, value in self.template_params.items():
      template = re.sub('{{%s}}' % name, value, template)
    os.system('echo "%s" | kubectl create -f - --namespace %s' % (
        template, self.sandbox_name))

  def stop(self):
    with open(self.template_file, 'r') as template_file:
      template = template_file.read()
    for name, value in self.template_params.items():
      template = re.sub('{{%s}}' % name, value, template)
    os.system('echo "%s" | kubectl delete -f - --namespace %s' % (
        template, self.sandbox_name))

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
      continue
  return ''

