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

"""Consul specific configuration."""

import json
import os

from . import server


class ConsulTopoServer(server.TopoServer):
  """Implementation of TopoServer for consul."""

  def setup(self):
    import environment  # pylint: disable=g-import-not-at-top
    import utils  # pylint: disable=g-import-not-at-top

    self.port_base = environment.reserve_ports(4)
    self.server_addr = 'localhost:%d' % (self.port_base + 1)

    # Write our config file.
    self.config_file = os.path.join(environment.vtdataroot, 'consul.json')
    config = {
        'ports': {
            'dns': self.port_base,
            'http': self.port_base + 1,
            'serf_lan': self.port_base + 2,
            'serf_wan': self.port_base + 3,
        },
    }
    with open(self.config_file, 'w') as fd:
      fd.write(json.dumps(config))

    log_base = os.path.join(environment.vtlogroot, 'consul')
    self.proc = utils.run_bg([
        'consul', 'agent',
        '-dev',
        '-config-file', self.config_file],
                             stdout=open(log_base + '.stdout', 'a'),
                             stderr=open(log_base + '.stderr', 'a'))

    # Wait until the daemon is ready.
    utils.curl(
        'http://' + self.server_addr + '/v1/kv/?keys', retry_timeout=10)

    # Create the cell configurations using 'vtctl AddCellInfo'
    for cell in ['test_nj', 'test_ny', 'test_ca']:
      utils.run_vtctl_vtctl(['AddCellInfo',
                             '-root', cell,
                             '-server_address', self.server_addr,
                             cell])

  def teardown(self):
    import utils  # pylint: disable=g-import-not-at-top

    utils.kill_sub_process(self.proc)
    self.proc.wait()

  def flags(self):
    return [
        '-topo_implementation', 'consul',
        '-topo_global_server_address', self.server_addr,
        '-topo_global_root', 'global',
    ]

  def wipe(self):
    import utils  # pylint: disable=g-import-not-at-top

    utils.curl('http://' + self.server_addr + '/v1/kv/global/keyspaces?recurse',
               request='DELETE')
    for cell in ['test_nj', 'test_ny', 'test_ca']:
      utils.curl('http://' + self.server_addr + '/v1/kv/' + cell + '?recurse',
                 request='DELETE')

  def update_addr(self, cell, keyspace, shard, tablet_index, port):
    pass

server.flavor_map['consul'] = ConsulTopoServer()
