#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""
This module allows you to bring up and tear down keyspaces.
"""

import os

import environment
import tablet
import utils

class TestEnv(object):
  def __init__(self):
    self.tablet_map={}

  def launch(self, keyspace, shards=None, replica_count=0, rdonly_count=0, ddls=None):
    self.tablets=[]
    utils.run_vtctl(['CreateKeyspace', keyspace])
    if not shards or shards[0] == "0":
      shards = ["0"]
    else:
      utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', keyspace, 'keyspace_id', 'uint64'])

    for shard in shards:
      procs = []
      procs.append(self._start_tablet(keyspace, shard, "master", None))
      for i in xrange(replica_count):
        procs.append(self._start_tablet(keyspace, shard, "replica", i))
      for i in xrange(rdonly_count):
        procs.append(self._start_tablet(keyspace, shard, "rdonly", i))
      utils.wait_procs(procs)

    utils.run_vtctl(['RebuildKeyspaceGraph', keyspace], auto_log=True)

    for t in self.tablets:
      t.create_db('vt_' + keyspace)
      t.start_vttablet(
        wait_for_state=None,
        extra_args=['-queryserver-config-schema-reload-time', '1'],
      )
    for t in self.tablets:
      t.wait_for_vttablet_state('SERVING')
    for t in self.tablets:
      if t.tablet_type == "master":
        utils.run_vtctl(['ReparentShard', '-force', keyspace+'/'+t.shard, t.tablet_alias], auto_log=True)
        # Force read-write even if there are no replicas.
        utils.run_vtctl(['SetReadWrite', t.tablet_alias], auto_log=True)

    utils.run_vtctl(['RebuildKeyspaceGraph', keyspace], auto_log=True)

    for ddl in ddls:
      fname = os.path.join(environment.tmproot, "ddl.sql")
      with open(fname, "w") as f:
        f.write(ddl)
      utils.run_vtctl(['ApplySchemaKeyspace', '-simple', '-sql-file', fname, keyspace])

  def teardown(self):
    all_tablets = self.tablet_map.values()
    tablet.kill_tablets(all_tablets)
    teardown_procs = [t.teardown_mysql() for t in all_tablets]
    utils.wait_procs(teardown_procs, raise_on_error=False)
    for t in all_tablets:
      t.remove_tree()

  def _start_tablet(self, keyspace, shard, tablet_type, index):
    t = tablet.Tablet()
    self.tablets.append(t)
    if tablet_type == "master":
      key = "%s.%s.%s" %(keyspace, shard, tablet_type)
    else:
      key = "%s.%s.%s.%s" %(keyspace, shard, tablet_type, index)
    self.tablet_map[key] = t
    proc = t.init_mysql()
    t.init_tablet(tablet_type, keyspace=keyspace, shard=shard)
    return proc
