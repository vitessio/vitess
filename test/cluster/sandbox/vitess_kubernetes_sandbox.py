#!/usr/bin/env python
"""A Vitess sandbox with Kubernetes."""

import collections
import copy
import kubernetes_components
import logging
import os
import tempfile
import yaml
from vttest import sharding_utils

import sandbox
import sandbox_utils
import sandlet
import subprocess_component


class VitessKubernetesSandbox(sandbox.Sandbox):
  """Sandbox implementation for Vitess."""

  # Constants used for generating tablet UIDs. Each constant represents the
  # increment value for each property.
  cell_epsilon = 100000000
  keyspace_epsilon = 1000000
  shard_epsilon = 100

  def __init__(self, sandbox_options):
    super(VitessKubernetesSandbox, self).__init__(sandbox_options)

  def generate_firewall_sandlet(self):
    """Generates sandlet for firewall rules."""
    firewall_sandlet = sandlet.Sandlet('firewall')

    if 'vtctld' in self.app_options.port_forwarding:
      firewall_sandlet.components.append(
          self.cluster_env.Port('%s-vtctld' % self.name,
                                self.app_options.port_forwarding['vtctld']))
    if 'vtgate' in self.app_options.port_forwarding:
      for cell in self.app_options.cells:
        firewall_sandlet.components.append(
            self.cluster_env.Port('%s-vtgate-%s' % (self.name, cell),
                                  self.app_options.port_forwarding['vtgate']))
    if 'guestbook' in self.app_options.port_forwarding:
      firewall_sandlet.components.append(
          self.cluster_env.Port('%s-guestbook' % self.name,
                                self.app_options.port_forwarding['guestbook']))
    self.sandlets.append(firewall_sandlet)

  def generate_guestbook_sandlet(self):
    """Creates a sandlet encompassing the guestbook app built on Vitess."""
    guestbook_sandlet = sandlet.Sandlet('guestbook')
    guestbook_sandlet.dependencies = ['helm']
    template_dir = os.path.join(os.environ['VTTOP'], 'examples/kubernetes')
    for keyspace in self.app_options.keyspaces:
      create_schema_subprocess = subprocess_component.Subprocess(
          'create_schema_%s' % keyspace['name'], self.name, 'create_schema.py',
          self.log_dir, namespace=self.name, keyspace=keyspace['name'],
          drop_table='messages', sql_file=os.path.join(
              os.environ['VTTOP'], 'examples/kubernetes/create_test_table.sql'))
      guestbook_sandlet.components.append(create_schema_subprocess)
    guestbook_sandlet.components.append(
        kubernetes_components.KubernetesResource(
            'guestbook-service', self.name,
            os.path.join(template_dir, 'guestbook-service.yaml'),
            namespace=self.name))
    guestbook_sandlet.components.append(
        kubernetes_components.KubernetesResource(
            'guestbook-controller', self.name,
            os.path.join(template_dir, 'guestbook-controller.yaml'),
            namespace=self.name))
    self.sandlets.append(guestbook_sandlet)

  def generate_helm_sandlet(self):
    """Creates a helm sandlet.

    This sandlet generates a dynamic values yaml file to be used with the Vitess
    helm chart in order to encompass most of the Vitess stack.
    """
    yaml_values = dict(
        vtctld=dict(
            serviceType='LoadBalancer',  # Allows port forwarding.
            image=self.app_options.vtctld_image,
            extraFlags={'enable_queries': True},
        ),
        vttablet=dict(
            image=self.app_options.vttablet_image,
            resources=dict(
                limits=dict(
                    memory=self.app_options.vttablet_ram,
                    cpu=self.app_options.vttablet_cpu,
                ),
            ),
            mysqlResources=dict(
                limits=dict(
                    memory=self.app_options.mysql_ram,
                    cpu=self.app_options.mysql_cpu,
                ),
            ),
        ),
        vtgate=dict(
            serviceType='LoadBalancer',  # Allows port forwarding.
            image=self.app_options.vtgate_image,
            resources=dict(
                limits=dict(
                    memory=self.app_options.vtgate_ram,
                    cpu=self.app_options.vtgate_cpu,
                ),
            ),
        ),
        backupFlags=self.app_options.backup_flags,
        topology=dict(
            cells=[dict(
                name='global',
                etcd=dict(
                    replicas=self.app_options.etcd_count,
                ),
            )],
        ),
    )
    if self.app_options.enable_orchestrator:
      yaml_values['topology']['cells'][0]['orchestrator'] = dict(
          replicas=1,
      )
    starting_cell_index = 0
    if len(self.app_options.cells) > 1:
      starting_cell_index = self.cell_epsilon
    keyspaces = []
    for ks_index, ks in enumerate(self.app_options.keyspaces):
      keyspace = dict(name=ks['name'], shards=[])
      keyspaces.append(keyspace)

      for shard_index, shard_name in enumerate(
          sharding_utils.get_shard_names(ks['shard_count'])):
        shard_name = sandbox_utils.fix_shard_name(shard_name)
        shard = dict(
            name=shard_name,
            tablets=[dict(
                type='replica',
                vttablet=dict(
                    replicas=ks['replica_count'],
                ),
            )],
        )
        uid_base = (
            (100 + shard_index * self.shard_epsilon) + starting_cell_index + (
                ks_index * self.keyspace_epsilon))
        shard['tablets'][0]['uidBase'] = uid_base
        if ks['rdonly_count']:
          shard['tablets'].append(dict(
              type='rdonly',
              uidBase=uid_base + ks['replica_count'],
              replicas=ks['rdonly_count']))
        keyspace['shards'].append(shard)
    for index, cell in enumerate(self.app_options.cells):
      cell_dict = dict(
          name=cell,
          etcd=dict(replicas=self.app_options.etcd_count),
          vtgate=dict(replicas=self.app_options.vtgate_count),
          keyspaces=copy.deepcopy(keyspaces),
      )
      for keyspace in cell_dict['keyspaces']:
        for shard in keyspace['shards']:
          for tablets in shard['tablets']:
            tablets['uidBase'] += index * self.cell_epsilon

      yaml_values['topology']['cells'].append(cell_dict)
      if index == 0:
        yaml_values['topology']['cells'][-1]['vtctld'] = dict(replicas=1)

    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(yaml.dump(yaml_values, default_flow_style=False))
      yaml_filename = f.name

    helm_sandlet = sandlet.Sandlet('helm')
    helm_sandlet.components = [kubernetes_components.HelmComponent(
        'helm', self.name, yaml_filename)]
    for keyspace in self.app_options.keyspaces:
      name = keyspace['name']
      shard_count = keyspace['shard_count']
      wait_for_mysql_subprocess = subprocess_component.Subprocess(
          'wait_for_mysql_%s' % name, self.name, 'wait_for_mysql.py',
          self.log_dir, namespace=self.name,
          cells=','.join(self.app_options.cells))
      wait_for_mysql_subprocess.dependencies = ['helm']
      initial_reparent_subprocess = subprocess_component.Subprocess(
          'initial_reparent_%s' % name, self.name,
          'initial_reparent.py', self.log_dir, namespace=self.name,
          keyspace=name, shard_count=shard_count,
          master_cell=self.app_options.cells[0])
      initial_reparent_subprocess.dependencies = [
          wait_for_mysql_subprocess.name]
      helm_sandlet.components.append(wait_for_mysql_subprocess)
      helm_sandlet.components.append(initial_reparent_subprocess)
    self.sandlets.append(helm_sandlet)

  def generate_from_config(self):
    """Creates a Vitess sandbox."""
    self.app_options = collections.namedtuple(
        'Struct', self.sandbox_options['application'].keys())(
            *self.sandbox_options['application'].values())

    if any(k in self.app_options.port_forwarding
           for k in ['vtgate', 'vtctld', 'guestbook']):
      self.generate_firewall_sandlet()
    self.generate_helm_sandlet()
    if 'guestbook' in self.app_options.port_forwarding:
      self.generate_guestbook_sandlet()

  def print_banner(self):
    logging.info('Fetching forwarded ports.')
    vtctld_addr = ''
    vtctld_port = self.app_options.port_forwarding['vtctld']
    vtgate_port = self.app_options.port_forwarding['vtgate']
    vtgate_addrs = []
    vtctld_addr = kubernetes_components.get_forwarded_ip(
        'vtctld', self.name)
    for cell in self.app_options.cells:
      vtgate_addr = kubernetes_components.get_forwarded_ip(
          'vtgate-%s' % cell, self.name)
      vtgate_addrs.append('%s %s:%d' % (cell, vtgate_addr, vtgate_port))
    banner = """
        Vitess Sandbox Info:
          vtctld: %s:%d
          vtgate: %s
          logs dir: %s""" % (
              vtctld_addr, vtctld_port, ', '.join(vtgate_addrs),
              self.log_dir)
    logging.info(banner)


if __name__ == '__main__':
  sandbox.sandbox_main(VitessKubernetesSandbox)
