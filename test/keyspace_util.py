#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
"""This module allows you to bring up and tear down keyspaces.
"""

import os

import environment
import tablet
import utils


class TestEnv(object):
  """Main class for this module."""

  def __init__(self):
    self.tablet_map = {}

  def launch(
      self, keyspace, shards=None,
      replica_count=1, rdonly_count=0, ddls=None,
      twopc_coordinator_address=None):
    """Launch test environment."""

    if replica_count < 1:
      raise Exception('replica_count=%d < 1; tests now use semi-sync'
                      ' and must have at least one replica' % replica_count)
    self.tablets = []
    self.master_tablets = []
    utils.run_vtctl(['CreateKeyspace', keyspace])
    if not shards or shards[0] == '0':
      shards = ['0']

    # Create tablets and start mysqld.
    procs = []
    for shard in shards:
      procs.append(self._new_tablet(keyspace, shard, 'master', None))
      for i in xrange(replica_count):
        procs.append(self._new_tablet(keyspace, shard, 'replica', i))
      for i in xrange(rdonly_count):
        procs.append(self._new_tablet(keyspace, shard, 'rdonly', i))
    utils.wait_procs(procs)

    # init tablets.
    for shard in shards:
      tablet_index = 0
      self._init_tablet(keyspace, shard, 'master', None, tablet_index)
      tablet_index += 1
      for i in xrange(replica_count):
        self._init_tablet(keyspace, shard, 'replica', i, tablet_index)
        tablet_index += 1
      for i in xrange(rdonly_count):
        self._init_tablet(keyspace, shard, 'rdonly', i, tablet_index)
        tablet_index += 1

    # Start tablets.
    for shard in shards:
      self._start_tablet(
          keyspace, shard, 'master', None, twopc_coordinator_address)
      for i in xrange(replica_count):
        self._start_tablet(
            keyspace, shard, 'replica', i, twopc_coordinator_address)
      for i in xrange(rdonly_count):
        self._start_tablet(
            keyspace, shard, 'rdonly', i, twopc_coordinator_address)

    for t in self.tablets:
      t.wait_for_vttablet_state('NOT_SERVING')

    for t in self.master_tablets:
      utils.run_vtctl(['InitShardMaster', '-force', keyspace+'/'+t.shard,
                       t.tablet_alias], auto_log=True)
      t.tablet_type = 'master'

    for t in self.tablets:
      t.wait_for_vttablet_state('SERVING')

    for ddl in ddls:
      fname = os.path.join(environment.tmproot, 'ddl.sql')
      with open(fname, 'w') as f:
        f.write(ddl)
      utils.run_vtctl(['ApplySchema', '-sql-file', fname, keyspace])

  def teardown(self):
    all_tablets = self.tablet_map.values()
    tablet.kill_tablets(all_tablets)
    teardown_procs = [t.teardown_mysql() for t in all_tablets]
    utils.wait_procs(teardown_procs, raise_on_error=False)
    for t in all_tablets:
      t.remove_tree()

  def _new_tablet(self, keyspace, shard, tablet_type, index):
    """Create a tablet and start mysqld."""
    t = tablet.Tablet()
    self.tablets.append(t)
    if tablet_type == 'master':
      self.master_tablets.append(t)
      key = '%s.%s.%s' % (keyspace, shard, tablet_type)
    else:
      key = '%s.%s.%s.%s' % (keyspace, shard, tablet_type, index)
    self.tablet_map[key] = t
    return t.init_mysql()

  def _init_tablet(self, keyspace, shard, tablet_type, index, tablet_index):
    init_tablet_type = tablet_type
    if tablet_type == 'master':
      init_tablet_type = 'replica'
      key = '%s.%s.%s' % (keyspace, shard, tablet_type)
    else:
      key = '%s.%s.%s.%s' % (keyspace, shard, tablet_type, index)
    t = self.tablet_map[key]
    t.init_tablet(init_tablet_type, keyspace, shard, tablet_index=tablet_index)

  def _start_tablet(
      self, keyspace, shard, tablet_type, index, twopc_coordinator_address):
    """Start a tablet."""
    init_tablet_type = tablet_type
    if tablet_type == 'master':
      init_tablet_type = 'replica'
      key = '%s.%s.%s' % (keyspace, shard, tablet_type)
    else:
      key = '%s.%s.%s.%s' % (keyspace, shard, tablet_type, index)
    t = self.tablet_map[key]
    t.create_db('vt_' + keyspace)
    extra_args = ['-queryserver-config-schema-reload-time', '1']
    if twopc_coordinator_address:
      extra_args.extend([
          '-twopc_enable',
          '-twopc_coordinator_address', twopc_coordinator_address,
          '-twopc_abandon_age', '3600',
      ])
    return t.start_vttablet(
        wait_for_state=None, init_tablet_type=init_tablet_type,
        init_keyspace=keyspace, init_shard=shard,
        extra_args=extra_args)
