#!/usr/bin/env python
#
# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import base64
import unittest

from vtproto import topodata_pb2

from vtdb import vtgate_client

import environment
import tablet
import utils

# shards
shard_0_master = tablet.Tablet()
shard_0_rdonly = tablet.Tablet()

shard_1_master = tablet.Tablet()
shard_1_rdonly = tablet.Tablet()


def setUpModule():
  try:
    environment.topo_server().setup()

    setup_procs = [
        shard_0_master.init_mysql(),
        shard_0_rdonly.init_mysql(),
        shard_1_master.init_mysql(),
        shard_1_rdonly.init_mysql(),
        ]
    utils.Vtctld().start()
    utils.VtGate().start()
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_rdonly.teardown_mysql(),
      shard_1_master.teardown_mysql(),
      shard_1_rdonly.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_rdonly.remove_tree()
  shard_1_master.remove_tree()
  shard_1_rdonly.remove_tree()


class TestCustomSharding(unittest.TestCase):
  """Test a custom-shared keyspace."""

  def _vtdb_conn(self):
    protocol, addr = utils.vtgate.rpc_endpoint(python=True)
    return vtgate_client.connect(protocol, addr, 30.0)

  def _insert_data(self, shard, start, count, table='data'):
    sql = 'insert into ' + table + '(id, name) values (%(id)s, %(name)s)'
    conn = self._vtdb_conn()
    cursor = conn.cursor(
        tablet_type='master', keyspace='test_keyspace',
        shards=[shard],
        writable=True)
    for x in xrange(count):
      bindvars = {
          'id': start+x,
          'name': 'row %d' % (start+x),
      }
      conn.begin()
      cursor.execute(sql, bindvars)
      conn.commit()
    conn.close()

  def _check_data(self, shard, start, count, table='data'):
    sql = 'select name from ' + table + ' where id=%(id)s'
    conn = self._vtdb_conn()
    cursor = conn.cursor(
        tablet_type='master', keyspace='test_keyspace',
        shards=[shard])
    for x in xrange(count):
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
    shard_0_master.init_tablet('master', 'test_keyspace', '0')
    shard_0_rdonly.init_tablet('rdonly', 'test_keyspace', '0')
    for t in [shard_0_master, shard_0_rdonly]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None)
    for t in [shard_0_master, shard_0_rdonly]:
      t.wait_for_vttablet_state('SERVING')

    utils.run_vtctl(['InitShardMaster', 'test_keyspace/0',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    self._check_shards_count_in_srv_keyspace(1)
    s = utils.run_vtctl_json(['GetShard', 'test_keyspace/0'])
    self.assertEqual(len(s['served_types']), 3)

    # create a table on shard 0
    sql = '''create table data(
id bigint auto_increment,
name varchar(64),
primary key (id)
) Engine=InnoDB'''
    utils.run_vtctl(['ApplySchema', '-sql=' + sql, 'test_keyspace'],
                    auto_log=True)

    # reload schema everywhere so the QueryService knows about the tables
    for t in [shard_0_master, shard_0_rdonly]:
      utils.run_vtctl(['ReloadSchema', t.tablet_alias], auto_log=True)

    # insert data on shard 0
    self._insert_data('0', 100, 10)

    # re-read shard 0 data
    self._check_data('0', 100, 10)

    # create shard 1
    shard_1_master.init_tablet('master', 'test_keyspace', '1')
    shard_1_rdonly.init_tablet('rdonly', 'test_keyspace', '1')
    for t in [shard_1_master, shard_1_rdonly]:
      t.start_vttablet(wait_for_state=None)
    for t in [shard_1_master, shard_1_rdonly]:
      t.wait_for_vttablet_state('NOT_SERVING')
    s = utils.run_vtctl_json(['GetShard', 'test_keyspace/1'])
    self.assertEqual(len(s['served_types']), 3)

    utils.run_vtctl(['InitShardMaster', 'test_keyspace/1',
                     shard_1_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['CopySchemaShard', shard_0_rdonly.tablet_alias,
                     'test_keyspace/1'], auto_log=True)
    for t in [shard_1_master, shard_1_rdonly]:
      utils.run_vtctl(['RefreshState', t.tablet_alias], auto_log=True)
      t.wait_for_vttablet_state('SERVING')

    # rebuild the keyspace serving graph now that the new shard was added
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

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

    # reload schema everywhere so the QueryService knows about the tables
    for t in [shard_0_master, shard_0_rdonly, shard_1_master, shard_1_rdonly]:
      utils.run_vtctl(['ReloadSchema', t.tablet_alias], auto_log=True)

    # insert and read data on all shards
    self._insert_data('0', 300, 10, table='data2')
    self._insert_data('1', 400, 10, table='data2')
    self._check_data('0', 300, 10, table='data2')
    self._check_data('1', 400, 10, table='data2')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    self._check_shards_count_in_srv_keyspace(2)

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
      for name, value in q['query']['bind_variables'].iteritems():
        # vtctl encodes bytes as base64.
        bindvars[name] = int(base64.standard_b64decode(value['value']))
      qr = utils.vtgate.execute_shards(
          q['query']['sql'],
          'test_keyspace', ','.join(q['shard_part']['shards']),
          tablet_type='master', bindvars=bindvars)
      for r in qr['Rows']:
        rows[int(r[0])] = r[1]
    self.assertEqual(len(rows), 20)
    expected = {}
    for i in xrange(10):
      expected[100 + i] = 'row %d' % (100 + i)
      expected[200 + i] = 'row %d' % (200 + i)
    self.assertEqual(rows, expected)

    self._test_vtclient_execute_shards_fallback()

  def _check_shards_count_in_srv_keyspace(self, shard_count):
    ks = utils.run_vtctl_json(['GetSrvKeyspace', 'test_nj', 'test_keyspace'])
    check_types = set([topodata_pb2.MASTER, topodata_pb2.RDONLY])
    for p in ks['partitions']:
      if p['served_type'] in check_types:
        self.assertEqual(len(p['shard_references']), shard_count)
        check_types.remove(p['served_type'])

    self.assertEqual(len(check_types), 0,
                     'The number of expected shard_references in GetSrvKeyspace'
                     ' was not equal %d for all expected tablet types.'
                     % shard_count)

  def _test_vtclient_execute_shards_fallback(self):
    """Test per-shard mode of Go SQL driver (through vtclient)."""
    for shard in [0, 1]:
      id_val = (shard + 1) * 1000  # example: 1000, 2000
      name_val = 'row %d' % id_val

      # write
      utils.vtgate.vtclient('insert into data(id, name) values (:v1, :v2)',
                            bindvars=[id_val, name_val],
                            keyspace='test_keyspace', shard=str(shard))

      want = {
          u'Fields': [u'id', u'name'],
          u'Rows': [[unicode(id_val), unicode(name_val)]]
          }
      # read non-streaming
      out, _ = utils.vtgate.vtclient(
          'select * from data where id = :v1', bindvars=[id_val],
          keyspace='test_keyspace', shard=str(shard), json_output=True)
      self.assertEqual(out, want)

      # read streaming
      out, _ = utils.vtgate.vtclient(
          'select * from data where id = :v1', bindvars=[id_val],
          keyspace='test_keyspace', shard=str(shard), streaming=True,
          json_output=True)
      self.assertEqual(out, want)


if __name__ == '__main__':
  utils.main()
