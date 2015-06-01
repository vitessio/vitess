#!/usr/bin/env python
#
# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import base64
import unittest

import environment
import utils
import tablet

# shards
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()

shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()

vtgate_server = None
vtgate_port = None

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

  def _insert_data(self, shard, start, count, table='data'):
    sql = 'insert into %s(id, name) values (:id, :name)' % table
    for x in xrange(count):
      bindvars = {
        'id':   start+x,
        'name': 'row %u' % (start+x),
        }
      utils.vtgate_execute_shard(vtgate_port, sql, 'test_keyspace', shard,
                                 bindvars=bindvars)

  def _check_data(self, shard, start, count, table='data'):
    sql = 'select name from %s where id=:id' % table
    for x in xrange(count):
      bindvars = {
        'id':   start+x,
        }
      qr = utils.vtgate_execute_shard(vtgate_port, sql, 'test_keyspace', shard,
                                      bindvars=bindvars)
      self.assertEqual(len(qr['Rows']), 1)
      # vtctl_json will print the JSON-encoded version of QueryResult,
      # which is a []byte. That translates into a base64-endoded string.
      v = base64.b64decode(qr['Rows'][0][0])
      self.assertEqual(v, 'row %u' % (start+x))

  def test_custom_end_to_end(self):
    """This test case runs through the common operations of a custom
    sharded keyspace: creation with one shard, schema change, reading
    / writing data, adding one more shard, reading / writing data from
    both shards, applying schema changes again, and reading / writing data from
    both shards again.
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
    utils.run_vtctl(['ApplySchema', '-sql=' + sql, 'test_keyspace'],
                    auto_log=True)

    # insert data on shard 0
    self._insert_data('0', 100, 10)

    # re-read shard 0 data
    self._check_data('0', 100, 10)

    # create shard 1
    shard_1_master.init_tablet( 'master',  'test_keyspace', '1')
    shard_1_replica.init_tablet('replica', 'test_keyspace', '1')
    for t in [shard_1_master, shard_1_replica]:
      t.start_vttablet(wait_for_state=None)
    for t in [shard_1_master, shard_1_replica]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', 'test_keyspace/1',
                     shard_1_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['CopySchemaShard', shard_0_replica.tablet_alias,
                     'test_keyspace/1'], auto_log=True)
    for t in [shard_1_master, shard_1_replica]:
      utils.run_vtctl(['RefreshState', t.tablet_alias], auto_log=True)
      t.wait_for_vttablet_state('SERVING')

    # insert data on shard 1
    self._insert_data('1', 200, 10)

    # re-read shard 1 data
    self._check_data('1', 200, 10)

    # create a second table on all shards
    sql = '''create table data2(
id bigint auto_increment,
name varchar(64),
primary key (id)
) Engine=InnoDB'''
    utils.run_vtctl(['ApplySchema', '-sql=' + sql, 'test_keyspace'],
                    auto_log=True)

    # insert and read data on all shards
    self._insert_data('0', 300, 10, table='data2')
    self._insert_data('1', 400, 10, table='data2')
    self._check_data('0', 300, 10, table='data2')
    self._check_data('1', 400, 10, table='data2')

if __name__ == '__main__':
  utils.main()
