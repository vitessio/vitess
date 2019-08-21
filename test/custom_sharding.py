#!/usr/bin/env python
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

import base64
import unittest

import environment
import tablet
import utils

from vtproto import topodata_pb2

from vtdb import vtgate_client

# shards need at least 1 replica for semi-sync ACK, and 1 rdonly for SplitQuery.
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_rdonly = tablet.Tablet()

shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()
shard_1_rdonly = tablet.Tablet()

all_tablets = [shard_0_master, shard_0_replica, shard_0_rdonly,
               shard_1_master, shard_1_replica, shard_1_rdonly]


def setUpModule():
  try:
    environment.topo_server().setup()

    setup_procs = [t.init_mysql() for t in all_tablets]
    utils.Vtctld().start()
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  teardown_procs = [t.teardown_mysql() for t in all_tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for t in all_tablets:
    t.remove_tree()


class TestCustomSharding(unittest.TestCase):
  """Test a custom-shared keyspace."""

  def _vtdb_conn(self):
    protocol, addr = utils.vtgate.rpc_endpoint(python=True)
    return vtgate_client.connect(protocol, addr, 30.0)

  def _insert_data(self, shard, start, count, table='data'):
    sql = 'insert into ' + table + '(id, name) values (:id, :name)'
    conn = self._vtdb_conn()
    cursor = conn.cursor(
        tablet_type='master', keyspace='test_keyspace',
        shards=[shard],
        writable=True)
    for x in range(count):
      bindvars = {
          'id': start+x,
          'name': 'row %d' % (start+x),
      }
      conn.begin()
      cursor.execute(sql, bindvars)
      conn.commit()
    conn.close()

  def _check_data(self, shard, start, count, table='data'):
    sql = 'select name from ' + table + ' where id=:id'
    conn = self._vtdb_conn()
    cursor = conn.cursor(
        tablet_type='master', keyspace='test_keyspace',
        shards=[shard])
    for x in range(count):
      bindvars = {
          'id': start+x,
      }
      cursor.execute(sql, bindvars)
      qr = cursor.fetchall()
      self.assertEqual(len(qr), 1)
      v = qr[0][0]
      self.assertEqual(v, 'row %d' % (start+x))
    conn.close()

  def test_custom_end_to_end(self):
    """Runs through the common operations of a custom sharded keyspace.

    Tests creation with one shard, schema change, reading / writing
    data, adding one more shard, reading / writing data from both
    shards, applying schema changes again, and reading / writing data
    from both shards again.
    """

    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    # start the first shard only for now
    shard_0_master.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='0',
        tablet_index=0)
    shard_0_replica.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='0',
        tablet_index=1)
    shard_0_rdonly.init_tablet(
        'rdonly',
        keyspace='test_keyspace',
        shard='0',
        tablet_index=2)

    for t in [shard_0_master, shard_0_replica, shard_0_rdonly]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None)

    for t in [shard_0_master, shard_0_replica, shard_0_rdonly]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.wait_for_tablet_type(shard_0_replica.tablet_alias, 'replica')
    utils.wait_for_tablet_type(shard_0_rdonly.tablet_alias, 'rdonly')
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly]:
      t.wait_for_vttablet_state('SERVING')

    self._check_shards_count_in_srv_keyspace(1)
    s = utils.run_vtctl_json(['GetShard', 'test_keyspace/0'])
    self.assertEqual(s['is_master_serving'], True)

    # create a table on shard 0
    sql = '''create table data(
id bigint auto_increment,
name varchar(64),
primary key (id)
) Engine=InnoDB'''
    utils.run_vtctl(['ApplySchema', '-sql=' + sql, 'test_keyspace'],
                    auto_log=True)

    # reload schema everywhere so the QueryService knows about the tables
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly]:
      utils.run_vtctl(['ReloadSchema', t.tablet_alias], auto_log=True)

    # create shard 1
    shard_1_master.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='1',
        tablet_index=0)
    shard_1_replica.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='1',
        tablet_index=1)
    shard_1_rdonly.init_tablet(
        'rdonly',
        keyspace='test_keyspace',
        shard='1',
        tablet_index=2)

    for t in [shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None)

    for t in [shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.wait_for_vttablet_state('NOT_SERVING')

    s = utils.run_vtctl_json(['GetShard', 'test_keyspace/1'])
    self.assertEqual(s['is_master_serving'], True)

    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/1',
                     shard_1_master.tablet_alias], auto_log=True)
    utils.wait_for_tablet_type(shard_1_replica.tablet_alias, 'replica')
    utils.wait_for_tablet_type(shard_1_rdonly.tablet_alias, 'rdonly')
    for t in [shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.wait_for_vttablet_state('SERVING')
    utils.run_vtctl(['CopySchemaShard', shard_0_rdonly.tablet_alias,
                     'test_keyspace/1'], auto_log=True)

    # we need to rebuild SrvKeyspace here to account for the new shards.
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    self._check_shards_count_in_srv_keyspace(2)

    # must start vtgate after tablets are up, or else wait until 1min refresh
    utils.VtGate().start(tablets=[
        shard_0_master, shard_0_replica, shard_0_rdonly,
        shard_1_master, shard_1_replica, shard_1_rdonly])
    utils.vtgate.wait_for_endpoints('test_keyspace.0.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.0.replica', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.0.rdonly', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.1.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.1.replica', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.1.rdonly', 1)

    # insert and check data on shard 0
    self._insert_data('0', 100, 10)
    self._check_data('0', 100, 10)

    # insert and check data on shard 1
    self._insert_data('1', 200, 10)
    self._check_data('1', 200, 10)

    # create a second table on all shards
    sql = '''create table data2(
id bigint auto_increment,
name varchar(64),
primary key (id)
) Engine=InnoDB'''
    utils.run_vtctl(['ApplySchema', '-sql=' + sql, 'test_keyspace'],
                    auto_log=True)

    # reload schema everywhere so the QueryService knows about the tables
    for t in all_tablets:
      utils.run_vtctl(['ReloadSchema', t.tablet_alias], auto_log=True)

    # insert and read data on all shards
    self._insert_data('0', 300, 10, table='data2')
    self._insert_data('1', 400, 10, table='data2')
    self._check_data('0', 300, 10, table='data2')
    self._check_data('1', 400, 10, table='data2')

    # Now test SplitQuery API works (used in MapReduce usually, but bringing
    # up a full MR-capable cluster is too much for this test environment)
    sql = 'select id, name from data'
    s = utils.vtgate.split_query(sql, 'test_keyspace', 4)
    self.assertEqual(len(s), 4)
    shard0count = 0
    shard1count = 0
    for q in s:
      if q['shard_part']['shards'][0] == '0':
        shard0count += 1
      if q['shard_part']['shards'][0] == '1':
        shard1count += 1
    self.assertEqual(shard0count, 2)
    self.assertEqual(shard1count, 2)

    # run the queries, aggregate the results, make sure we have all rows
    rows = {}
    for q in s:
      bindvars = {}
      for name, value in q['query']['bind_variables'].items():
        # vtctl encodes bytes as base64.
        bindvars[name] = int(base64.standard_b64decode(value['value']))
      qr = utils.vtgate.execute_shards(
          q['query']['sql'],
          'test_keyspace', ','.join(q['shard_part']['shards']),
          tablet_type='master', bindvars=bindvars)
      for r in qr['rows']:
        rows[int(r[0])] = r[1]
    self.assertEqual(len(rows), 20)
    expected = {}
    for i in range(10):
      expected[100 + i] = 'row %d' % (100 + i)
      expected[200 + i] = 'row %d' % (200 + i)
    self.assertEqual(rows, expected)

  def _check_shards_count_in_srv_keyspace(self, shard_count):
    ks = utils.run_vtctl_json(['GetSrvKeyspace', 'test_nj', 'test_keyspace'])
    check_types = set([topodata_pb2.MASTER, topodata_pb2.REPLICA,
                       topodata_pb2.RDONLY])
    for p in ks['partitions']:
      if p['served_type'] in check_types:
        self.assertEqual(len(p['shard_references']), shard_count)
        check_types.remove(p['served_type'])

    self.assertEqual(len(check_types), 0,
                     'The number of expected shard_references in GetSrvKeyspace'
                     ' was not equal %d for all expected tablet types.'
                     % shard_count)


if __name__ == '__main__':
  utils.main()
