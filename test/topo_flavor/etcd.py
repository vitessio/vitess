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

"""Etcd specific configuration."""

import os
import shutil

import server


class EtcdCluster(object):
  """Sets up a global or cell-local etcd cluster."""

  def __init__(self, name):
    import environment  # pylint: disable=g-import-not-at-top
    import utils  # pylint: disable=g-import-not-at-top

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

    self.proc = utils.run_bg([
        'etcd', '-name', self.name,
        '-advertise-client-urls', self.client_addr,
        '-initial-advertise-peer-urls', self.peer_addr,
        '-listen-client-urls', self.client_addr,
        '-listen-peer-urls', self.peer_addr,
        '-initial-cluster', '%s=%s' % (self.name, self.peer_addr),
        '-data-dir', self.data_dir],
                             stdout=open(os.path.join(
                                 environment.vtlogroot,
                                 dirname + '.stdout'),
                                         'w'),
                             stderr=open(os.path.join(
                                 environment.vtlogroot,
                                 dirname + '.stderr'),
                                         'w'),)


class EtcdTopoServer(server.TopoServer):
  """Implementation of TopoServer for etcd."""

  clusters = {}

  def setup(self):
    import utils  # pylint: disable=g-import-not-at-top

    for cell in ['global', 'test_ca', 'test_nj', 'test_ny']:
      self.clusters[cell] = EtcdCluster(cell)

    # Wait for global cluster to come up.
    utils.curl(
        self.clusters['global'].api_url + '/keys/vt', request='PUT',
        data='dir=true', retry_timeout=10)

    # Add entries in global cell list.
    for cell, cluster in self.clusters.iteritems():
      if cell != 'global':
        utils.curl(
            '%s/keys/vt/cells/%s' %
            (self.clusters['global'].api_url, cell), request='PUT',
            data='value=' + cluster.client_addr)

  def teardown(self):
    import utils  # pylint: disable=g-import-not-at-top

    for cluster in self.clusters.itervalues():
      utils.kill_sub_process(cluster.proc)
      if not utils.options.keep_logs:
        shutil.rmtree(cluster.data_dir)

  def flags(self):
    return [
        '-topo_implementation', 'etcd',
        '-etcd_global_addrs', self.clusters['global'].client_addr,
    ]

  def wipe(self):
    import utils  # pylint: disable=g-import-not-at-top

    for cell, cluster in self.clusters.iteritems():
      if cell == 'global':
        utils.curl(
            cluster.api_url + '/keys/vt/keyspaces?recursive=true',
            request='DELETE')
      else:
        utils.curl(
            cluster.api_url + '/keys/vt/ns?recursive=true', request='DELETE')
        utils.curl(
            cluster.api_url + '/keys/vt/tablets?recursive=true',
            request='DELETE')
        utils.curl(
            cluster.api_url + '/keys/vt/replication?recursive=true',
            request='DELETE')

  def update_addr(self, cell, keyspace, shard, tablet_index, port):
    pass

server.flavor_map['etcd'] = EtcdTopoServer()
