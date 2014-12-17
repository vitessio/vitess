#!/usr/bin/env python

# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import os
import socket
import json

import server


class ZkTopoServer(server.TopoServer):
  """Implementation of TopoServer for ZooKeeper"""
  def __init__(self):
    self.ports_assigned = False

  def assign_ports(self):
    """Assign ports if not already assigned"""

    if self.ports_assigned:
      return

    from environment import reserve_ports

    self.zk_port_base = reserve_ports(3)
    self.hostname = socket.getfqdn()
    self.zk_ports = ':'.join(str(self.zk_port_base + i) for i in range(3))
    self.zk_client_port = self.zk_port_base + 2
    self.ports_assigned = True

  def setup(self, add_bad_host=False):
    from environment import run, binary_args, vtlogroot, tmproot

    self.assign_ports()
    run(binary_args('zkctl') + [
        '-log_dir', vtlogroot,
        '-zk.cfg', '1@%s:%s' % (self.hostname, self.zk_ports),
        'init'])
    config = tmproot + '/test-zk-client-conf.json'
    with open(config, 'w') as f:
      ca_server = 'localhost:%u' % (self.zk_client_port)
      if add_bad_host:
        ca_server += ',does.not.exists:1234'
      zk_cell_mapping = {
          'test_nj': 'localhost:%u' % (self.zk_client_port),
          'test_ny': 'localhost:%u' % (self.zk_client_port),
          'test_ca': ca_server,
          'global': 'localhost:%u' % (self.zk_client_port),
      }
      json.dump(zk_cell_mapping, f)
    os.environ['ZK_CLIENT_CONFIG'] = config
    logging.debug('Using ZK_CLIENT_CONFIG=%s', str(config))
    run(binary_args('zk') + ['touch', '-p', '/zk/test_nj/vt'])
    run(binary_args('zk') + ['touch', '-p', '/zk/test_ny/vt'])
    run(binary_args('zk') + ['touch', '-p', '/zk/test_ca/vt'])

  def teardown(self):
    from environment import run, binary_args, vtlogroot
    import utils

    self.assign_ports()
    run(binary_args('zkctl') + [
        '-log_dir', vtlogroot,
        '-zk.cfg', '1@%s:%s' % (self.hostname, self.zk_ports),
        'shutdown' if utils.options.keep_logs else 'teardown'],
        raise_on_error=False)

  def flags(self):
    return ['-topo_implementation', 'zookeeper']

  def wipe(self):
    from environment import run, binary_args

    # Work around safety check on recursive delete.
    run(binary_args('zk') + ['rm', '-rf', '/zk/test_nj/vt/*'])
    run(binary_args('zk') + ['rm', '-rf', '/zk/test_ny/vt/*'])
    run(binary_args('zk') + ['rm', '-rf', '/zk/global/vt/*'])

    run(binary_args('zk') + ['rm', '-f', '/zk/test_nj/vt'])
    run(binary_args('zk') + ['rm', '-f', '/zk/test_ny/vt'])
    run(binary_args('zk') + ['rm', '-f', '/zk/global/vt'])


server.flavor_map['zookeeper'] = ZkTopoServer()
