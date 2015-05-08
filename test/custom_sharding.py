#!/usr/bin/env python
#
# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# WORK IN PROGRESS: python client doesn't support ExecuteShard yet,
# so this is not possible. So this is not enabled anywhere.

import unittest

import environment
import utils
import tablet

from vtdb import vtgatev2

# shards
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()

shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()

vtgate_server = None
vtgate_port = None

conn_class = vtgatev2

def setUpModule():
  global vtgate_server
  global vtgate_port

  try:
    environment.topo_server().setup()

    setup_procs = [
        shard_0_master.init_mysql(),
        shard_0_replica.init_mysql(),
        shard_1_master.init_mysql(),
        shard_1_replica.init_mysql(),
        ]
    utils.Vtctld().start()
    vtgate_server, vtgate_port = utils.vtgate_start()
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise

def tearDownModule():
  global vtgate_server

  if utils.options.skip_teardown:
    return

  utils.vtgate_kill(vtgate_server)
  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_replica.teardown_mysql(),
      shard_1_master.teardown_mysql(),
      shard_1_replica.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica.remove_tree()

class TestCustomSharding(unittest.TestCase):

  def _get_connection(self):
    global vtgate_port
    vtgate_addrs = {"vt": ["localhost:%s" % (vtgate_port),]}
    return conn_class.connect(vtgate_addrs, timeout,
                              user=user, password=password)

  def _insert_data(self, shard, start, count):
    # FIXME(alainjobart) no way to target a shard right now, API
    # is not exposed.
    return
    vtgate_conn = self._get_connection()
    vtgate_conn.begin()
    for x in xrange(count):
      vtgate_conn._execute("insert into data(id, name) values (:id, :name)",
                           {'id': start+x,
                            'name': 'row %u' % (start+x)},
                           'test_keyspace', 'master',
                           shards = ['0'])
    vtgate_conn.commit()

  def test_custom_end_to_end(self):
    """This test case runs through the common operations of a custom
    sharded keyspace: creation with one shard, schema change, reading
    / writing data, adding one more shard, reading / writing data from
    both shards, applying schema changes again, ...
    """

    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # start the first shard only for now
    shard_0_master.init_tablet( 'master',  'test_keyspace', '0')
    shard_0_replica.init_tablet('replica', 'test_keyspace', '0')
    for t in [shard_0_master, shard_0_replica]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None)
    for t in [shard_0_master, shard_0_replica]:
      t.wait_for_vttablet_state('SERVING')

    utils.run_vtctl(['InitShardMaster', 'test_keyspace/0',
                     shard_0_master.tablet_alias], auto_log=True)

    # create a table on shard 0
    sql = '''create table data(
id bigint auto_increment,
name varchar(64),
primary key (id)
) Engine=InnoDB'''
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + sql,
                     'test_keyspace'],
                    auto_log=True)

    # insert data on shard 0
    self._insert_data('0', 100, 10)

    # re-read shard 0 data

if __name__ == '__main__':
  utils.main()
