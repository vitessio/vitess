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

  def generate_guestbook_sandlet(self):
    """Creates a sandlet encompassing the guestbook app built on Vitess."""
    guestbook_sandlet = sandlet.Sandlet('guestbook')
    guestbook_sandlet.dependencies = ['helm']
    template_dir = os.path.join(os.environ['VTTOP'], 'examples/kubernetes')
    guestbook_sandlet.components.add_component(
        self.cluster_env.Port('%s-guestbook' % self.name, 80))
    for keyspace in self.app_options.keyspaces:
      create_schema_subprocess = subprocess_component.Subprocess(
          'create_schema_%s' % keyspace['name'], self.name, 'create_schema.py',
          self.log_dir, namespace=self.name, keyspace=keyspace['name'],
          drop_table='messages', sql_file=os.path.join(
              os.environ['VTTOP'], 'examples/kubernetes/create_test_table.sql'))
      guestbook_sandlet.components.add_component(create_schema_subprocess)
    guestbook_sandlet.components.add_component(
        kubernetes_components.KubernetesResource(
            'guestbook-service', self.name,
            os.path.join(template_dir, 'guestbook-service.yaml')))
    guestbook_sandlet.components.add_component(
        kubernetes_components.KubernetesResource(
            'guestbook-controller', self.name,
            os.path.join(template_dir, 'guestbook-controller-template.yaml'),
            port=8080, cell=self.app_options.cells[0], vtgate_port=15991,
            keyspace=self.app_options.keyspaces[0]['name']))
    self.sandlets.add_component(guestbook_sandlet)

  def _generate_helm_keyspaces(self):
    """Create helm keyspace configurations.

    These configurations include entries for keyspaces, shards, and tablets.
    Note that the logic for calculating UIDs will go away once Kubernetes 1.5
    is widely available. Then tablets will use replication controllers with
    StatefulSet, and UIDs will be calculated automatically.

    Returns:
      Configuration for keyspaces in the form of a list of dicts.
    """
    starting_cell_index = 0
    if len(self.app_options.cells) > 1:
      starting_cell_index = self.cell_epsilon
    keyspaces = []
    for ks_index, ks in enumerate(self.app_options.keyspaces):
      keyspace = dict(name=ks['name'], shards=[])
      keyspaces.append(keyspace)

      for shard_index, shard_name in enumerate(
          sharding_utils.get_shard_names(ks['shard_count'])):
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
              vttablet=dict(
                  replicas=ks['rdonly_count'],
              )))
        keyspace['shards'].append(shard)
    return keyspaces

  def _generate_helm_values_config(self):
    """Generate the values yaml config used for helm.

    Returns:
      Filename of the generated helm config.
    """

    # First override general configurations, such as which docker image to use
    # and resource usage. Set vtctld/vtgate services as LoadBalancers to allow
    # external access more easily.
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
            controllerType='StatefulSet',
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
    # Add an orchestrator entry if enabled
    if self.app_options.enable_orchestrator:
      yaml_values['topology']['cells'][0]['orchestrator'] = dict(
          replicas=1,
      )

    keyspaces = self._generate_helm_keyspaces()

    # For the sandbox, each keyspace will exist in all defined cells.
    # This means in the config, the keyspace entry is duplicated in every
    # cell entry.
    for index, cell in enumerate(self.app_options.cells):
      cell_dict = dict(
          name=cell,
          etcd=dict(replicas=self.app_options.etcd_count),
          vtgate=dict(replicas=self.app_options.vtgate_count),
          keyspaces=copy.deepcopy(keyspaces),
      )
      # Each tablet's UID must be unique, so increment the uidBase for tablets
      # by the cell epsilon value to ensure uniqueness. Also convert the UID to
      # a string, or else the parser will attempt to parse UID as a float, which
      # causes issues when UID's are large. This logic will go away once
      # StatefulSet is available.
      for keyspace in cell_dict['keyspaces']:
        for shard in keyspace['shards']:
          for tablets in shard['tablets']:
            tablets['uidBase'] = str(
                tablets['uidBase'] + index * self.cell_epsilon)
      yaml_values['topology']['cells'].append(cell_dict)

      if index == 0:
        yaml_values['topology']['cells'][-1]['vtctld'] = dict(replicas=1)

    # Create the file and return the filename
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(yaml.dump(yaml_values, default_flow_style=False))
      yaml_filename = f.name
    logging.info('Helm config generated at %s', yaml_filename)
    return yaml_filename

  def generate_helm_sandlet(self):
    """Creates a helm sandlet.

    This sandlet generates a dynamic values yaml file to be used with the Vitess
    helm chart in order to encompass most of the Vitess stack.
    """
    helm_sandlet = sandlet.Sandlet('helm')
    helm_sandlet.components.add_component(kubernetes_components.HelmComponent(
        'helm', self.name, self._generate_helm_values_config()))

    # Add a subprocess task to wait for all mysql instances to be healthy.
    tablet_count = 0
    for keyspace in self.app_options.keyspaces:
      tablet_count += (keyspace['shard_count'] * len(self.app_options.cells) * (
          keyspace['replica_count'] + keyspace['rdonly_count']))
    wait_for_mysql_subprocess = subprocess_component.Subprocess(
        'wait_for_mysql', self.name, 'wait_for_mysql.py',
        self.log_dir, namespace=self.name,
        cells=','.join(self.app_options.cells),
        tablet_count=tablet_count)
    wait_for_mysql_subprocess.dependencies = ['helm']
    helm_sandlet.components.add_component(wait_for_mysql_subprocess)

    # Add a subprocess task for each keyspace to perform the initial reparent.
    for keyspace in self.app_options.keyspaces:
      name = keyspace['name']
      shard_count = keyspace['shard_count']
      initial_reparent_subprocess = subprocess_component.Subprocess(
          'initial_reparent_%s_%d' % (name, shard_count), self.name,
          'initial_reparent.py', self.log_dir, namespace=self.name,
          keyspace=name, shard_count=shard_count,
          master_cell=self.app_options.cells[0])
      initial_reparent_subprocess.dependencies = [
          wait_for_mysql_subprocess.name]
      helm_sandlet.components.add_component(initial_reparent_subprocess)
    self.sandlets.add_component(helm_sandlet)

  def generate_from_config(self):
    """Creates a Vitess sandbox."""
    self.app_options = collections.namedtuple(
        'Struct', self.sandbox_options['application'].keys())(
            *self.sandbox_options['application'].values())

    self.generate_helm_sandlet()
    if self.app_options.enable_guestbook:
      self.generate_guestbook_sandlet()

  def print_banner(self):
    logging.info('Fetching forwarded ports.')
    banner = '\nVitess Sandbox Info:\n'
    vtctld_ip = kubernetes_components.get_forwarded_ip(
        'vtctld', self.name)
    banner += '  vtctld: http://%s:15000\n' % vtctld_ip
    for cell in self.app_options.cells:
      vtgate_ip = kubernetes_components.get_forwarded_ip(
          'vtgate-%s' % cell, self.name)
      banner += '  vtgate-%s: http://%s:15001\n' % (cell, vtgate_ip)
    if self.app_options.enable_guestbook:
      guestbook_ip = kubernetes_components.get_forwarded_ip(
          'guestbook', self.name)
      banner += '  guestbook: http://%s:80\n' % guestbook_ip
    banner += '  logs dir: %s\n' % self.log_dir
    logging.info(banner)


if __name__ == '__main__':
  sandbox.sandbox_main(VitessKubernetesSandbox)
