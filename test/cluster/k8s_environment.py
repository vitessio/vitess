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

"""Kubernetes environment."""

import getpass
import json
import logging
import os
import subprocess
import time

from sandbox import kubernetes_components
from vtproto import topodata_pb2
from vtdb import vtgate_client
import base_environment
import protocols_flavor
import vtctl_helper


class K8sEnvironment(base_environment.BaseEnvironment):
  """Environment for kubernetes clusters on Google Compute Engine."""

  def __init__(self):
    super(K8sEnvironment, self).__init__()

  def use_named(self, instance_name):
    # Check to make sure kubectl exists
    try:
      subprocess.check_output(['kubectl'])
    except OSError:
      raise base_environment.VitessEnvironmentError(
          'kubectl not found, please install by visiting kubernetes.io or '
          'running gcloud components update kubectl if using compute engine.')

    vtctld_ip = kubernetes_components.get_forwarded_ip(
        'vtctld', instance_name)
    self.vtctl_addr = '%s:15999' % vtctld_ip

    self.vtctl_helper = vtctl_helper.VtctlHelper('grpc', self.vtctl_addr)
    self.cluster_name = instance_name

    keyspaces = self.vtctl_helper.execute_vtctl_command(['GetKeyspaces'])
    self.mobs = filter(None, keyspaces.split('\n'))
    self.keyspaces = self.mobs

    if not self.keyspaces:
      raise base_environment.VitessEnvironmentError(
          'Invalid environment, no keyspaces found')

    self.num_shards = []
    self.shards = []

    for keyspace in self.keyspaces:
      shards = json.loads(self.vtctl_helper.execute_vtctl_command(
          ['FindAllShardsInKeyspace', keyspace]))
      self.shards.append(shards)
      self.num_shards.append(len(shards))

    # This assumes that all keyspaces use the same set of cells
    self.cells = json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetShard', '%s/%s' % (self.keyspaces[0], self.shards[0].keys()[0])]
        ))['cells']

    self.primary_cells = self.cells
    self.replica_instances = []
    self.rdonly_instances = []

    # This assumes that all cells are equivalent for k8s environments.
    all_tablets_in_a_cell = self.vtctl_helper.execute_vtctl_command(
        ['ListAllTablets', self.cells[0]])
    all_tablets_in_a_cell = [x.split(' ') for x in
                             filter(None, all_tablets_in_a_cell.split('\n'))]

    for index, keyspace in enumerate(self.keyspaces):
      keyspace_tablets_in_cell = [
          tablet for tablet in all_tablets_in_a_cell if tablet[1] == keyspace]
      replica_tablets_in_cell = [
          tablet for tablet in keyspace_tablets_in_cell
          if tablet[3] == 'master' or tablet[3] == 'replica']
      replica_instances = len(replica_tablets_in_cell) / self.num_shards[index]
      self.replica_instances.append(replica_instances)
      self.rdonly_instances.append(
          (len(keyspace_tablets_in_cell) / self.num_shards[index]) -
          replica_instances)

    # Converts keyspace name and alias to number of instances
    self.keyspace_alias_to_num_instances_dict = {}
    for index, keyspace in enumerate(self.keyspaces):
      self.keyspace_alias_to_num_instances_dict[keyspace] = {
          'replica': int(self.replica_instances[index]),
          'rdonly': int(self.rdonly_instances[index])
      }

    self.vtgate_addrs = {}
    for cell in self.cells:
      vtgate_ip = kubernetes_components.get_forwarded_ip(
          'vtgate-%s' % cell, instance_name)
      self.vtgate_addrs[cell] = '%s:15991' % vtgate_ip
    super(K8sEnvironment, self).use_named(instance_name)

  def create(self, **kwargs):
    self.create_gke_cluster = (
        kwargs.get('create_gke_cluster', 'false').lower() != 'false')
    if self.create_gke_cluster and 'GKE_NUM_NODES' not in kwargs:
      raise base_environment.VitessEnvironmentError(
          'Must specify GKE_NUM_NODES')
    if 'GKE_CLUSTER_NAME' not in kwargs:
      kwargs['GKE_CLUSTER_NAME'] = getpass.getuser()
    if 'VITESS_NAME' not in kwargs:
      kwargs['VITESS_NAME'] = getpass.getuser()
    kwargs['TEST_MODE'] = '1'
    self.script_dir = os.path.join(os.environ['VTTOP'], 'examples/kubernetes')
    try:
      subprocess.check_output(['gcloud', 'config', 'list'])
    except OSError:
      raise base_environment.VitessEnvironmentError(
          'gcloud not found, please install by visiting cloud.google.com')
    if 'project' in kwargs:
      logging.info('Setting project to %s', kwargs['project'])
      subprocess.check_output(
          ['gcloud', 'config', 'set', 'project', kwargs['project']])
    project_name_json = json.loads(subprocess.check_output(
        ['gcloud', 'config', 'list', 'project', '--format', 'json']))
    project_name = project_name_json['core']['project']
    logging.info('Current project name: %s', project_name)
    for k, v in kwargs.iteritems():
      os.environ[k] = v
    if self.create_gke_cluster:
      cluster_up_txt = subprocess.check_output(
          [os.path.join(self.script_dir, 'cluster-up.sh')],
          cwd=self.script_dir, stderr=subprocess.STDOUT)
      logging.info(cluster_up_txt)
    vitess_up_output = subprocess.check_output(
        [os.path.join(self.script_dir, 'vitess-up.sh')],
        cwd=self.script_dir, stderr=subprocess.STDOUT)
    logging.info(vitess_up_output)
    self.use_named(kwargs['VITESS_NAME'])

  def destroy(self):
    vitess_down_output = subprocess.check_output(
        [os.path.join(self.script_dir, 'vitess-down.sh')],
        cwd=self.script_dir, stderr=subprocess.STDOUT)
    logging.info(vitess_down_output)
    if self.create_gke_cluster:
      cluster_down_output = subprocess.check_output(
          [os.path.join(self.script_dir, 'cluster-down.sh')],
          cwd=self.script_dir, stderr=subprocess.STDOUT)
      logging.info(cluster_down_output)

  def get_vtgate_conn(self, cell):
    return vtgate_client.connect(
        protocols_flavor.protocols_flavor().vtgate_python_protocol(),
        self.vtgate_addrs[cell], 60)

  def restart_mysql_task(self, tablet_name, task_name, is_alloc=False):
    # Delete the whole pod, which deletes mysql + vttablet tasks.
    os.system('kubectl delete pod %s --namespace=%s' % (
        self.get_tablet_pod_name(tablet_name), self.cluster_name))
    return 0

  def wait_for_good_failover_status(
      self, keyspace, shard_name, failover_completion_timeout_s=60):
    return 0

  def poll_for_varz(self, tablet_name, varz, timeout=60.0,
                    condition_fn=None, converter=str, condition_msg=None):
    """Polls for varz to exist, or match specific conditions, within a timeout.

    Args:
      tablet_name: the name of the process that we're trying to poll vars from.
      varz: name of the vars to fetch from varz
      timeout: number of seconds that we should attempt to poll for.
      condition_fn: a function that takes the var as input, and returns a truthy
        value if it matches the success conditions.
      converter: function to convert varz value
      condition_msg: string describing the conditions that we're polling for,
        used for error messaging.

    Raises:
      VitessEnvironmentError: Raised if the varz conditions aren't met within
        the given timeout.

    Returns:
      dict of requested varz.
    """
    start_time = time.time()
    while True:
      if (time.time() - start_time) >= timeout:
        timeout_error_msg = 'Timed out polling for varz.'
        if condition_fn and condition_msg:
          timeout_error_msg += ' Condition "%s" not met.' % condition_msg
        raise base_environment.VitessEnvironmentError(timeout_error_msg)
      hostname = self.get_tablet_ip_port(tablet_name)
      host_varz = subprocess.check_output([
          'kubectl', 'exec', '-ti', self.get_tablet_pod_name(tablet_name),
          '--namespace=%s' % self.cluster_name,
          'curl', '%s/debug/vars' % hostname])
      if not host_varz:
        continue
      host_varz = json.loads(host_varz)
      if condition_fn is None or condition_fn(host_varz):
        return host_varz

  def wait_for_healthy_tablets(self):
    return 0

  def get_tablet_pod_name(self, tablet_name):
    tablet_info = json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetTablet', tablet_name]))
    # Hostname is <pod_name>.vttablet
    return tablet_info['hostname'].split('.')[0]

  def get_tablet_task_number(self, tablet_name):
    # Tablet pod name under StatefulSet is
    # "<cell>-<keyspace>-<shard_number>-<tablet_type>-<task_number>"
    # Example: test1-foo-0-replica-0.
    return int(self.get_tablet_pod_name(tablet_name).split('-')[-1])

  def automatic_reparent_available(self):
    """Checks if the environment can automatically reparent."""
    p1 = subprocess.Popen(
        ['kubectl', 'get', 'pods', '--namespace=%s' % self.cluster_name],
        stdout=subprocess.PIPE)
    p2 = subprocess.Popen(
        ['grep', 'orchestrator'], stdin=p1.stdout, stdout=subprocess.PIPE)
    output = p2.communicate()[0]
    return bool(output)

  def internal_reparent(self, keyspace, shard_name, new_master_uid,
                        emergency=False):
    reparent_command = (
        'EmergencyReparentShard' if emergency else 'PlannedReparentShard')
    self.vtctl_helper.execute_vtctl_command(
        [reparent_command, '-keyspace_shard', '%s/%s' % (keyspace, shard_name),
         '-new_master', new_master_uid])
    self.vtctl_helper.execute_vtctl_command(['RebuildKeyspaceGraph', keyspace])
    return 0, 'No output'

  def backup(self, tablet_name):
    logging.info('Backing up tablet %s', tablet_name)
    self.vtctl_helper.execute_vtctl_command(['Backup', tablet_name])

  def drain_tablet(self, tablet_name, duration_s=600):
    self.vtctl_helper.execute_vtctl_command(['StopSlave', tablet_name])
    self.vtctl_helper.execute_vtctl_command(
        ['ChangeSlaveType', tablet_name, 'drained'])

  def is_tablet_drained(self, tablet_name):
    return self.get_tablet_type(tablet_name) == topodata_pb2.DRAINED

  def undrain_tablet(self, tablet_name):
    self.vtctl_helper.execute_vtctl_command(
        ['ChangeSlaveType', tablet_name, 'replica'])
    self.vtctl_helper.execute_vtctl_command(['StartSlave', tablet_name])

  def is_tablet_undrained(self, tablet_name):
    return not self.is_tablet_drained(tablet_name)

  def get_tablet_query_total_count(self, tablet_name):
    return self.poll_for_varz(
        tablet_name, ['Queries'])['Queries']['TotalCount']

