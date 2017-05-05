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

"""ZooKeeper specific configuration."""

import json
import logging
import os

import server


class ZkTopoServer(server.TopoServer):
  """Implementation of TopoServer for ZooKeeper."""

  def __init__(self):
    self.ports_assigned = False

  def assign_ports(self):
    """Assign ports if not already assigned."""

    if self.ports_assigned:
      return

    from environment import reserve_ports  # pylint: disable=g-import-not-at-top
    import utils  # pylint: disable=g-import-not-at-top

    self.zk_port_base = reserve_ports(3)
    self.hostname = utils.hostname
    self.zk_ports = ':'.join(str(self.zk_port_base + i) for i in range(3))
    self.addr = 'localhost:%d' % (self.zk_port_base + 2)
    self.ports_assigned = True

  def setup(self, add_bad_host=False):
    from environment import run, binary_args, vtlogroot, tmproot  # pylint: disable=g-import-not-at-top,g-multiple-import

    self.assign_ports()
    run(binary_args('zkctl') + [
        '-log_dir', vtlogroot,
        '-zk.cfg', '1@%s:%s' % (self.hostname, self.zk_ports),
        'init'])
    config = tmproot + '/test-zk-client-conf.json'
    with open(config, 'w') as f:
      ca_server = self.addr
      if add_bad_host:
        ca_server += ',does.not.exists:1234'
      zk_cell_mapping = {
          'test_nj': self.addr,
          'test_ny': self.addr,
          'test_ca': ca_server,
          'global': self.addr,
      }
      json.dump(zk_cell_mapping, f)
    os.environ['ZK_CLIENT_CONFIG'] = config
    logging.debug('Using ZK_CLIENT_CONFIG=%s', str(config))
    run(binary_args('zk') + ['-server', self.addr,
                             'touch', '-p', '/zk/test_nj/vt'])
    run(binary_args('zk') + ['-server', self.addr,
                             'touch', '-p', '/zk/test_ny/vt'])
    run(binary_args('zk') + ['-server', self.addr,
                             'touch', '-p', '/zk/test_ca/vt'])

  def teardown(self):
    from environment import run, binary_args, vtlogroot  # pylint: disable=g-import-not-at-top,g-multiple-import
    import utils  # pylint: disable=g-import-not-at-top

    self.assign_ports()
    run(binary_args('zkctl') + [
        '-log_dir', vtlogroot,
        '-zk.cfg', '1@%s:%s' % (self.hostname, self.zk_ports),
        'shutdown' if utils.options.keep_logs else 'teardown'],
        raise_on_error=False)

  def flags(self):
    return ['-topo_implementation', 'zookeeper']

  def wipe(self):
    from environment import run, binary_args  # pylint: disable=g-import-not-at-top,g-multiple-import

    run(binary_args('zk') + ['-server', self.addr,
                             'rm', '-rf', '/zk/test_nj/vt/*'])
    run(binary_args('zk') + ['-server', self.addr,
                             'rm', '-rf', '/zk/test_ny/vt/*'])
    run(binary_args('zk') + ['-server', self.addr,
                             'rm', '-rf', '/zk/global/vt/*'])

  def update_addr(self, cell, keyspace, shard, tablet_index, port):
    pass

server.flavor_map['zookeeper'] = ZkTopoServer()
