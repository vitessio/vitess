#!/usr/bin/env python

# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import os
import shutil

import server


class EtcdCluster:
  """Sets up a global or cell-local etcd cluster"""

  def __init__(self, name):
    import environment
    import utils

    self.port_base = environment.reserve_ports(2)

    self.name = name
    self.hostname = 'localhost'
    self.client_port = self.port_base
    self.peer_port = self.port_base + 1
    self.client_addr = '%s:%u' % (self.hostname, self.client_port)
    self.peer_addr = '%s:%u' % (self.hostname, self.peer_port)
    self.api_url = 'http://%s/v2' % (self.client_addr)

    dirname = 'etcd_' + self.name
    self.data_dir = os.path.join(environment.vtdataroot, dirname)

    self.proc = utils.run_bg([
        'etcd', '-name', self.name, '-addr',
        self.client_addr, '-peer-addr',
        self.peer_addr, '-data-dir', self.data_dir],
                             stdout=open(os.path.join(
                                 environment.vtlogroot,
                                 dirname + '.stdout'),
                                         'w'),
                             stderr=open(os.path.join(
                                 environment.vtlogroot,
                                 dirname + '.stderr'),
                                         'w'),)


class EtcdTopoServer(server.TopoServer):
  """Implementation of TopoServer for etcd"""

  clusters = {}

  def setup(self, add_bad_host=False):
    import utils

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
            data='value=http://' + cluster.client_addr)

  def teardown(self):
    import utils

    for cluster in self.clusters.itervalues():
      utils.kill_sub_process(cluster.proc)
      if not utils.options.keep_logs:
        shutil.rmtree(cluster.data_dir)

  def flags(self):
    return [
        '-topo_implementation', 'etcd',
        '-etcd_global_addrs', 'http://' + self.clusters['global'].client_addr,
    ]

  def wipe(self):
    import utils

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


server.flavor_map['etcd'] = EtcdTopoServer()
