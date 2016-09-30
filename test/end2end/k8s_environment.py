"""Kubernetes environment."""

import json
import getpass
import logging
import os
import subprocess
import time

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

    get_address_template = (
        '{{if ge (len .status.loadBalancer) 1}}'
        '{{index (index .status.loadBalancer.ingress 0) "ip"}}'
        '{{end}}')

    get_address_params = ['kubectl', 'get', '-o', 'template', '--template',
                          get_address_template, 'service', '--namespace',
                          instance_name]

    start_time = time.time()
    vtctld_addr = ''
    while time.time() - start_time < 60 and not vtctld_addr:
      vtctld_addr = subprocess.check_output(
          get_address_params + ['vtctld'], stderr=subprocess.STDOUT)
    self.vtctl_addr = '%s:15999' % vtctld_addr

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

    start_time = time.time()
    self.vtgate_addrs = {}
    self.vtgate_conns = {}
    for cell in self.cells:
      self.vtgate_addr = ''
      while time.time() - start_time < 60 and not self.vtgate_addr:
        vtgate_addr = subprocess.check_output(
            get_address_params + ['vtgate-%s' % cell], stderr=subprocess.STDOUT)
      self.vtgate_addrs[cell] = '%s:15001' % vtgate_addr
      self.vtgate_conns[cell] = vtgate_client.connect(
          protocols_flavor.protocols_flavor().vtgate_python_protocol(),
          self.vtgate_addrs[cell], 60)
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
    return self.vtgate_conns[cell]

  def wait_for_good_failover_status(
      self, keyspace, shard_name, failover_completion_timeout_s=60):
    return 0

  def wait_for_healthy_tablets(self):
    return 0

  def get_tablet_task_number(self, tablet_name):
    tablet_info = json.loads(self.vtctl_helper.execute_vtctl_command(
        ['GetTablet', tablet_name]))
    return tablet_info['alias']['uid'] % 100

  def internal_reparent(self, keyspace, shard_name, new_master_uid,
                        emergency=False):
    reparent_command = (
        'EmergencyReparentShard' if emergency else 'PlannedReparentShard')
    self.vtctl_helper.execute_vtctl_command(
        [reparent_command, '-keyspace_shard', '%s/%s' % (keyspace, shard_name),
         '-new_master', new_master_uid])
    self.vtctl_helper.execute_vtctl_command(['RebuildKeyspaceGraph', keyspace])
    return 0, 'No output'
