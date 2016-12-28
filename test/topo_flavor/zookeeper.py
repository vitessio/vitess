#!/usr/bin/env python

# Copyright 2014 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

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
