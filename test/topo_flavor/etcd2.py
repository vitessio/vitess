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

"""Etcd2 specific configuration."""

import os
import shutil

from . import server


class Etcd2Cluster(object):
  """Sets up a global or cell-local etcd cluster."""

  def __init__(self, name):
    import environment  # pylint: disable=g-import-not-at-top

    self.port_base = environment.reserve_ports(2)

    self.name = name
    self.hostname = 'localhost'
    self.client_port = self.port_base
    self.peer_port = self.port_base + 1
    self.client_addr = 'http://%s:%d' % (self.hostname, self.client_port)
    self.peer_addr = 'http://%s:%d' % (self.hostname, self.peer_port)
    self.api_url = self.client_addr + '/v2'

    dirname = 'etcd_' + self.name
    self.data_dir = os.path.join(environment.vtdataroot, dirname)
    self.log_base = os.path.join(environment.vtlogroot, dirname)

    self.start()

  def start(self):
    import utils  # pylint: disable=g-import-not-at-top

    self.proc = utils.run_bg([
        'etcd', '-name', self.name,
        '-advertise-client-urls', self.client_addr,
        '-initial-advertise-peer-urls', self.peer_addr,
        '-listen-client-urls', self.client_addr,
        '-listen-peer-urls', self.peer_addr,
        '-initial-cluster', '%s=%s' % (self.name, self.peer_addr),
        '-data-dir', self.data_dir],
                             stdout=open(self.log_base + '.stdout', 'a'),
                             stderr=open(self.log_base + '.stderr', 'a'))

  def restart(self):
    self.stop()
    self.start()

  def stop(self):
    import utils  # pylint: disable=g-import-not-at-top

    utils.kill_sub_process(self.proc)
    self.proc.wait()
    shutil.rmtree(self.data_dir)

  def wait_until_up(self):
    import utils  # pylint: disable=g-import-not-at-top

    # Wait for global cluster to come up.
    # We create a dummy directory using v2 API, won't be visible to v3.
    utils.curl(
        self.api_url + '/keys/test', request='PUT',
        data='dir=true', retry_timeout=10)


class Etcd2TopoServer(server.TopoServer):
  """Implementation of TopoServer for etcd2."""

  clusters = {}

  def setup(self, add_bad_host=False):
    for cell in ['global', 'test_ca', 'test_nj', 'test_ny']:
      self.clusters[cell] = Etcd2Cluster(cell)

    self.wait_until_up_add_cells()

  def teardown(self):
    for cluster in self.clusters.values():
      cluster.stop()

  def flags(self):
    return [
        '-topo_implementation', 'etcd2',
        '-topo_global_server_address', self.clusters['global'].client_addr,
        '-topo_global_root', '/global',
    ]

  def wipe(self):
    for cluster in self.clusters.values():
      cluster.restart()

    self.wait_until_up_add_cells()

  def update_addr(self, cell, keyspace, shard, tablet_index, port):
    pass

  def wait_until_up_add_cells(self):
    import utils  # pylint: disable=g-import-not-at-top

    for cluster in self.clusters.values():
      cluster.wait_until_up()

    # Add entries in global cell list.
    for cell, cluster in self.clusters.items():
      if cell != 'global':
        utils.run_vtctl_vtctl(['AddCellInfo',
                               '-root', '/',
                               '-server_address', cluster.client_addr,
                               cell])

server.flavor_map['etcd2'] = Etcd2TopoServer()
