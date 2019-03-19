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

"""zk2 specific configuration."""

import server


class Zk2TopoServer(server.TopoServer):
  """Implementation of TopoServer for zk2."""

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

  def setup(self):
    from environment import run, binary_args, vtlogroot  # pylint: disable=g-import-not-at-top,g-multiple-import
    import utils  # pylint: disable=g-import-not-at-top

    self.assign_ports()
    run(binary_args('zkctl') + [
        '-log_dir', vtlogroot,
        '-zk.cfg', '1@%s:%s' % (self.hostname, self.zk_ports),
        'init'])

    # Create the cell configurations using 'vtctl AddCellInfo'
    utils.run_vtctl_vtctl(['AddCellInfo',
                           '-root', '/test_nj',
                           '-server_address', self.addr,
                           'test_nj'])
    utils.run_vtctl_vtctl(['AddCellInfo',
                           '-root', '/test_ny',
                           '-server_address', self.addr,
                           'test_ny'])
    ca_addr = self.addr
    # Use UpdateCellInfo for this one, more coverage.
    utils.run_vtctl_vtctl(['UpdateCellInfo',
                           '-root', '/test_ca',
                           '-server_address', ca_addr,
                           'test_ca'])

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
    return [
        '-topo_implementation', 'zk2',
        '-topo_global_server_address', self.addr,
        '-topo_global_root', '/global',
    ]

  def wipe(self):
    from environment import run, binary_args  # pylint: disable=g-import-not-at-top,g-multiple-import

    # Only delete keyspaces/ in the global topology service, to keep
    # the 'cells' directory. So we don't need to re-add the CellInfo records.
    run(binary_args('zk') + ['-server', self.addr, 'rm', '-rf',
                             '/global/keyspaces'])
    run(binary_args('zk') + ['-server', self.addr, 'rm', '-rf', '/test_nj/*'])
    run(binary_args('zk') + ['-server', self.addr, 'rm', '-rf', '/test_ny/*'])
    run(binary_args('zk') + ['-server', self.addr, 'rm', '-rf', '/test_ca/*'])

  def update_addr(self, cell, keyspace, shard, tablet_index, port):
    pass

server.flavor_map['zk2'] = Zk2TopoServer()
