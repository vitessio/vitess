#!/usr/bin/python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import threading
import struct
import time
import unittest

import environment
import utils
import tablet

# source keyspace, with 4 tables
source_master = tablet.Tablet()
source_replica = tablet.Tablet()
source_rdonly = tablet.Tablet()

# destination keyspace, with just two tables
destination_master = tablet.Tablet()
destination_replica = tablet.Tablet()
destination_rdonly = tablet.Tablet()

def setUpModule():
  try:
    environment.topo_server_setup()

    setup_procs = [
        source_master.init_mysql(),
        source_replica.init_mysql(),
        source_rdonly.init_mysql(),
        destination_master.init_mysql(),
        destination_replica.init_mysql(),
        destination_rdonly.init_mysql(),

        ]
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  teardown_procs = [
        source_master.teardown_mysql(),
        source_replica.teardown_mysql(),
        source_rdonly.teardown_mysql(),
        destination_master.teardown_mysql(),
        destination_replica.teardown_mysql(),
        destination_rdonly.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  source_master.remove_tree()
  source_replica.remove_tree()
  source_rdonly.remove_tree()
  destination_master.remove_tree()
  destination_replica.remove_tree()
  destination_rdonly.remove_tree()

class TestVerticalSplit(unittest.TestCase):

  def _create_source_schema(self):
    create_table_template = '''create table %s(
id bigint auto_increment,
msg varchar(64),
primary key (id),
index by_msg (msg)
) Engine=InnoDB'''
    create_view_template = '''create view %s(id, msg) as select id, msg from %s'''

    for t in ['moving1', 'moving2', 'staying1', 'staying2']:
      utils.run_vtctl(['ApplySchemaKeyspace',
                       '-simple',
                       '-sql=' + create_table_template % (t),
                       'source_keyspace'],
                      auto_log=True)
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + create_view_template % ('view1', 'moving1'),
                     'source_keyspace'],
                    auto_log=True)

  def test_vertical_split(self):
    utils.run_vtctl(['CreateKeyspace',
                     'source_keyspace'])
    utils.run_vtctl(['CreateKeyspace',
                     'destination_keyspace'])
    source_master.init_tablet('master', 'source_keyspace', '0')
    source_replica.init_tablet('replica', 'source_keyspace', '0')
    source_rdonly.init_tablet('rdonly', 'source_keyspace', '0')
    destination_master.init_tablet('master', 'destination_keyspace', '0')
    destination_replica.init_tablet('replica', 'destination_keyspace', '0')
    destination_rdonly.init_tablet('rdonly', 'destination_keyspace', '0')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'source_keyspace'], auto_log=True)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'destination_keyspace'],
                    auto_log=True)

    # create databases so vttablet can start behaving normally
    for t in [source_master, source_replica, source_rdonly]:
      t.create_db('vt_source_keyspace')
      t.start_vttablet(wait_for_state=None)
    for t in [destination_master, destination_replica, destination_rdonly]:
      t.start_vttablet(wait_for_state=None)

    # wait for the tablets
    for t in [source_master, source_replica, source_rdonly]:
      t.wait_for_vttablet_state('SERVING')
    for t in [destination_master, destination_replica, destination_rdonly]:
      t.wait_for_vttablet_state('CONNECTING')

    # reparent to make the tablets work
    utils.run_vtctl(['ReparentShard', '-force', 'source_keyspace/0',
                     source_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['ReparentShard', '-force', 'destination_keyspace/0',
                     destination_master.tablet_alias], auto_log=True)

    # create the schema on the source keyspace
    self._create_source_schema()

    # take the snapshot for the split
    utils.run_vtctl(['MultiSnapshot',
                     '--tables', 'moving1,moving2,view1',
                     source_rdonly.tablet_alias], auto_log=True)

    # perform the restore.
    utils.run_vtctl(['ShardMultiRestore',
                     '--strategy' ,'populateBlpCheckpoint',
                     '--tables', 'moving1,moving2',
                     'destination_keyspace/0', source_rdonly.tablet_alias],
                    auto_log=True)

    utils.pause("Good time to test vtworker for diffs")

    # kill everything
    tablet.kill_tablets([source_master, source_replica, source_rdonly,
                         destination_master, destination_replica,
                         destination_rdonly])

if __name__ == '__main__':
  utils.main()
