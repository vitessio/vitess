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

import logging
import time
import unittest

import base_sharding
import environment
import tablet
import utils

from vtproto import topodata_pb2
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgate_client

# source keyspace, with 4 tables
source_master = tablet.Tablet()
source_replica = tablet.Tablet()
source_rdonly1 = tablet.Tablet()
source_rdonly2 = tablet.Tablet()

# destination keyspace, with just two tables
destination_master = tablet.Tablet()
destination_replica = tablet.Tablet()
destination_rdonly1 = tablet.Tablet()
destination_rdonly2 = tablet.Tablet()

all_tablets = [source_master, source_replica, source_rdonly1, source_rdonly2,
               destination_master, destination_replica, destination_rdonly1,
               destination_rdonly2]


def setUpModule():
  try:
    environment.topo_server().setup()
    setup_procs = [t.init_mysql(use_rbr=base_sharding.use_rbr)
                   for t in all_tablets]
    utils.Vtctld().start()
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  if utils.vtgate:
    utils.vtgate.kill()
  teardown_procs = [t.teardown_mysql() for t in all_tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)
  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  for t in all_tablets:
    t.remove_tree()


class TestVerticalSplit(unittest.TestCase, base_sharding.BaseShardingTest):

  def setUp(self):
    self.insert_index = 0

    self._init_keyspaces_and_tablets()
    utils.VtGate().start(cache_ttl='0s', tablets=[
        source_master, source_replica, source_rdonly1,
        source_rdonly2, destination_master,
        destination_replica, destination_rdonly1,
        destination_rdonly2])

    utils.vtgate.wait_for_endpoints(
        '%s.%s.master' % ('source_keyspace', '0'),
        1)
    utils.vtgate.wait_for_endpoints(
        '%s.%s.replica' % ('source_keyspace', '0'),
        1)
    utils.vtgate.wait_for_endpoints(
        '%s.%s.rdonly' % ('source_keyspace', '0'),
        2)
    utils.vtgate.wait_for_endpoints(
        '%s.%s.master' % ('destination_keyspace', '0'),
        1)
    utils.vtgate.wait_for_endpoints(
        '%s.%s.replica' % ('destination_keyspace', '0'),
        1)
    utils.vtgate.wait_for_endpoints(
        '%s.%s.rdonly' % ('destination_keyspace', '0'),
        2)

    # create the schema on the source keyspace, add some values
    self._insert_initial_values()

  def tearDown(self):
    # kill everything
    tablet.kill_tablets([source_master, source_replica, source_rdonly1,
                         source_rdonly2, destination_master,
                         destination_replica, destination_rdonly1,
                         destination_rdonly2])
    utils.vtgate.kill()

  def _init_keyspaces_and_tablets(self):
    utils.run_vtctl(['CreateKeyspace', 'source_keyspace'])
    utils.run_vtctl(
        ['CreateKeyspace', '--served_from',
         'master:source_keyspace,replica:source_keyspace,rdonly:'
         'source_keyspace',
         'destination_keyspace'])

    source_master.init_tablet(
        'replica',
        keyspace='source_keyspace',
        shard='0',
        tablet_index=0)
    source_replica.init_tablet(
        'replica',
        keyspace='source_keyspace',
        shard='0',
        tablet_index=1)
    source_rdonly1.init_tablet(
        'rdonly',
        keyspace='source_keyspace',
        shard='0',
        tablet_index=2)
    source_rdonly2.init_tablet(
        'rdonly',
        keyspace='source_keyspace',
        shard='0',
        tablet_index=3)
    destination_master.init_tablet(
        'replica',
        keyspace='destination_keyspace',
        shard='0',
        tablet_index=0)
    destination_replica.init_tablet(
        'replica',
        keyspace='destination_keyspace',
        shard='0',
        tablet_index=1)
    destination_rdonly1.init_tablet(
        'rdonly',
        keyspace='destination_keyspace',
        shard='0',
        tablet_index=2)
    destination_rdonly2.init_tablet(
        'rdonly',
        keyspace='destination_keyspace',
        shard='0',
        tablet_index=3)

    utils.run_vtctl(
        ['RebuildKeyspaceGraph', 'source_keyspace'], auto_log=True)
    utils.run_vtctl(
        ['RebuildKeyspaceGraph', 'destination_keyspace'], auto_log=True)

    self._create_source_schema()

    for t in [source_master, source_replica,
              destination_master, destination_replica]:
      t.start_vttablet(wait_for_state=None)
    for t in [source_rdonly1, source_rdonly2,
              destination_rdonly1, destination_rdonly2]:
      t.start_vttablet(wait_for_state=None)

    # wait for the tablets
    master_tablets = [source_master, destination_master]
    replica_tablets = [
        source_replica, source_rdonly1, source_rdonly2,
        destination_replica, destination_rdonly1,
        destination_rdonly2]
    for t in master_tablets + replica_tablets:
      t.wait_for_vttablet_state('NOT_SERVING')

    # check SrvKeyspace
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n'
                             'ServedFrom(rdonly): source_keyspace\n'
                             'ServedFrom(replica): source_keyspace\n')

    # reparent to make the tablets work (we use health check, fix their types)
    utils.run_vtctl(['InitShardMaster', '-force', 'source_keyspace/0',
                     source_master.tablet_alias], auto_log=True)
    source_master.tablet_type = 'master'
    utils.run_vtctl(['InitShardMaster', '-force', 'destination_keyspace/0',
                     destination_master.tablet_alias], auto_log=True)
    destination_master.tablet_type = 'master'

    for t in [source_replica, destination_replica]:
      utils.wait_for_tablet_type(t.tablet_alias, 'replica')
    for t in [source_rdonly1, source_rdonly2,
              destination_rdonly1, destination_rdonly2]:
      utils.wait_for_tablet_type(t.tablet_alias, 'rdonly')

    for t in master_tablets + replica_tablets:
      t.wait_for_vttablet_state('SERVING')

  def _create_source_schema(self):
    create_table_template = '''create table %s(
id bigint not null,
msg varchar(64),
primary key (id),
index by_msg (msg)
) Engine=InnoDB'''
    create_view_template = 'create view %s(id, msg) as select id, msg from %s'
    # RBR only because Vitess requires the primary key for query rewrites if
    # it is running with statement based replication.
    create_moving3_no_pk_table = '''create table moving3_no_pk (
id bigint not null,
msg varchar(64)
) Engine=InnoDB'''

    for t in [source_master, source_replica, source_rdonly1, source_rdonly2]:
      t.create_db('vt_source_keyspace')
      for n in ['moving1', 'moving2', 'staying1', 'staying2']:
        t.mquery(source_master.dbname, create_table_template % (n))
      t.mquery(source_master.dbname,
               create_view_template % ('view1', 'moving1'))
      if base_sharding.use_rbr:
        t.mquery(source_master.dbname, create_moving3_no_pk_table)

    for t in [destination_master, destination_replica, destination_rdonly1,
              destination_rdonly2]:
      t.create_db('vt_destination_keyspace')
      t.mquery(destination_master.dbname, create_table_template % 'extra1')

  def _insert_initial_values(self):
    self.moving1_first = self._insert_values('moving1', 100)
    self.moving2_first = self._insert_values('moving2', 100)
    staying1_first = self._insert_values('staying1', 100)
    staying2_first = self._insert_values('staying2', 100)
    self._check_values(source_master, 'vt_source_keyspace', 'moving1',
                       self.moving1_first, 100)
    self._check_values(source_master, 'vt_source_keyspace', 'moving2',
                       self.moving2_first, 100)
    self._check_values(source_master, 'vt_source_keyspace', 'staying1',
                       staying1_first, 100)
    self._check_values(source_master, 'vt_source_keyspace', 'staying2',
                       staying2_first, 100)
    self._check_values(source_master, 'vt_source_keyspace', 'view1',
                       self.moving1_first, 100)

    if base_sharding.use_rbr:
      self.moving3_no_pk_first = self._insert_values('moving3_no_pk', 100)
      self._check_values(source_master, 'vt_source_keyspace', 'moving3_no_pk',
                         self.moving3_no_pk_first, 100)

    # Insert data directly because vtgate would redirect us.
    destination_master.mquery(
        'vt_destination_keyspace',
        "insert into %s (id, msg) values(%d, 'value %d')" % ('extra1', 1, 1),
        write=True)
    self._check_values(destination_master, 'vt_destination_keyspace', 'extra1',
                       1, 1)

  def _vtdb_conn(self):
    protocol, addr = utils.vtgate.rpc_endpoint(python=True)
    return vtgate_client.connect(protocol, addr, 30.0)

  # insert some values in the source master db, return the first id used
  def _insert_values(self, table, count):
    result = self.insert_index
    conn = self._vtdb_conn()
    cursor = conn.cursor(
        tablet_type='master', keyspace='source_keyspace',
        keyranges=[keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)],
        writable=True)
    for _ in xrange(count):
      conn.begin()
      cursor.execute("insert into %s (id, msg) values(%d, 'value %d')" % (
          table, self.insert_index, self.insert_index), {})
      conn.commit()
      self.insert_index += 1
    conn.close()
    return result

  def _check_values(self, t, dbname, table, first, count):
    logging.debug(
        'Checking %d values from %s/%s starting at %d', count, dbname,
        table, first)
    rows = t.mquery(
        dbname, 'select id, msg from %s where id>=%d order by id limit %d' %
        (table, first, count))
    self.assertEqual(count, len(rows), 'got wrong number of rows: %d != %d' %
                     (len(rows), count))
    for i in xrange(count):
      self.assertEqual(first + i, rows[i][0], 'invalid id[%d]: %d != %d' %
                       (i, first + i, rows[i][0]))
      self.assertEqual('value %d' % (first + i), rows[i][1],
                       "invalid msg[%d]: 'value %d' != '%s'" %
                       (i, first + i, rows[i][1]))

  def _check_values_timeout(self, t, dbname, table, first, count,
                            timeout=30):
    while True:
      try:
        self._check_values(t, dbname, table, first, count)
        return
      except Exception:  # pylint: disable=broad-except
        timeout -= 1
        if timeout == 0:
          raise
        logging.debug('Sleeping for 1s waiting for data in %s/%s', dbname,
                      table)
        time.sleep(1)

  def _check_srv_keyspace(self, expected):
    cell = 'test_nj'
    keyspace = 'destination_keyspace'
    ks = utils.run_vtctl_json(['GetSrvKeyspace', cell, keyspace])
    result = ''
    if 'served_from' in ks and ks['served_from']:
      a = []
      for served_from in sorted(ks['served_from']):
        tt = topodata_pb2.TabletType.Name(served_from['tablet_type']).lower()
        if tt == 'batch':
          tt = 'rdonly'
        a.append('ServedFrom(%s): %s\n' % (tt, served_from['keyspace']))
      for line in sorted(a):
        result += line
    logging.debug('Cell %s keyspace %s has data:\n%s', cell, keyspace, result)
    self.assertEqual(
        expected, result,
        'Mismatch in srv keyspace for cell %s keyspace %s, expected:\n'
        '%s\ngot:\n%s' % (
            cell, keyspace, expected, result))
    self.assertEqual('', ks.get('sharding_column_name', ''),
                     'Got a sharding_column_name in SrvKeyspace: %s' %
                     str(ks))
    self.assertEqual(0, ks.get('sharding_column_type', 0),
                     'Got a sharding_column_type in SrvKeyspace: %s' %
                     str(ks))

  def _check_blacklisted_tables(self, t, expected):
    status = t.get_status()
    if expected:
      self.assertIn('BlacklistedTables: %s' % ' '.join(expected), status)
    else:
      self.assertNotIn('BlacklistedTables', status)

    # check we can or cannot access the tables
    for table in ['moving1', 'moving2']:
      if expected and '/moving/' in expected:
        # table is blacklisted, should get the error
        _, stderr = utils.run_vtctl(['VtTabletExecute', '-json',
                                     t.tablet_alias,
                                     'select count(1) from %s' % table],
                                    expect_fail=True)
        self.assertIn(
            'disallowed due to rule: enforce blacklisted tables',
            stderr)
      else:
        # table is not blacklisted, should just work
        qr = t.execute('select count(1) from %s' % table)
        logging.debug('Got %s rows from table %s on tablet %s',
                      qr['rows'][0][0], table, t.tablet_alias)

  def _check_client_conn_redirection(
      self, destination_ks, servedfrom_db_types,
      moved_tables=None):
    # check that the ServedFrom indirection worked correctly.
    if moved_tables is None:
      moved_tables = []
    conn = self._vtdb_conn()
    for db_type in servedfrom_db_types:
      for tbl in moved_tables:
        try:
          rows = conn._execute(
              'select * from %s' % tbl, {}, tablet_type=db_type,
              keyspace_name=destination_ks,
              keyranges=[keyrange.KeyRange(
                  keyrange_constants.NON_PARTIAL_KEYRANGE)])
          logging.debug(
              'Select on %s.%s returned %d rows', db_type, tbl, len(rows))
        except Exception, e:  # pylint: disable=broad-except
          self.fail('Execute failed w/ exception %s' % str(e))

  def _check_stats(self):
    v = utils.vtgate.get_vars()
    self.assertEqual(
        v['VttabletCall']['Histograms']['Execute.source_keyspace.0.replica'][
            'Count'],
        2
        , 'unexpected value for VttabletCall('
        'Execute.source_keyspace.0.replica) inside %s' % str(v))
    # Verify master reads done by self._check_client_conn_redirection().
    self.assertEqual(
        v['VtgateApi']['Histograms'][
            'ExecuteKeyRanges.destination_keyspace.master']['Count'],
        6,
        'unexpected value for VtgateApi('
        'ExecuteKeyRanges.destination_keyspace.master) inside %s' % str(v))
    self.assertEqual(
        len(v['VtgateApiErrorCounts']), 0,
        'unexpected errors for VtgateApiErrorCounts inside %s' % str(v))

  def test_vertical_split(self):
    utils.run_vtctl(['CopySchemaShard', '--tables', '/moving/,view1',
                     source_rdonly1.tablet_alias, 'destination_keyspace/0'],
                    auto_log=True)

    utils.run_vtworker(['--cell', 'test_nj',
                        '--command_display_interval', '10ms',
                        '--use_v3_resharding_mode=false',
                        'VerticalSplitClone',
                        '--tables', '/moving/,view1',
                        '--chunk_count', '10',
                        '--min_rows_per_chunk', '1',
                        '--min_healthy_tablets', '1',
                        'destination_keyspace/0'],
                       auto_log=True)

    # test Cancel first
    utils.run_vtctl(['CancelResharding', 'destination_keyspace/0'], auto_log=True)
    self.check_no_binlog_player(destination_master)
    # master should be in serving state after cancel
    utils.check_tablet_query_service(self, destination_master, True, False)

    # redo VerticalSplitClone
    utils.run_vtworker(['--cell', 'test_nj',
                        '--command_display_interval', '10ms',
                        '--use_v3_resharding_mode=false',
                        'VerticalSplitClone',
                        '--tables', '/moving/,view1',
                        '--chunk_count', '10',
                        '--min_rows_per_chunk', '1',
                        '--min_healthy_tablets', '1',
                        'destination_keyspace/0'],
                       auto_log=True)

    # check values are present
    self._check_values(destination_master, 'vt_destination_keyspace', 'moving1',
                       self.moving1_first, 100)
    self._check_values(destination_master, 'vt_destination_keyspace', 'moving2',
                       self.moving2_first, 100)
    self._check_values(destination_master, 'vt_destination_keyspace', 'view1',
                       self.moving1_first, 100)
    if base_sharding.use_rbr:
      self._check_values(destination_master, 'vt_destination_keyspace',
                         'moving3_no_pk', self.moving3_no_pk_first, 100)

    # Verify vreplication table entries
    result = destination_master.mquery('_vt', 'select * from vreplication')
    self.assertEqual(len(result), 1)
    self.assertEqual(result[0][1], 'SplitClone')
    self.assertEqual(result[0][2],
      'keyspace:"source_keyspace" shard:"0" tables:"/moving/" tables:"view1" ')

    # check the binlog player is running and exporting vars
    self.check_destination_master(destination_master, ['source_keyspace/0'])

    # check that binlog server exported the stats vars
    self.check_binlog_server_vars(source_replica, horizontal=False)

    # add values to source, make sure they're replicated
    moving1_first_add1 = self._insert_values('moving1', 100)
    _ = self._insert_values('staying1', 100)
    moving2_first_add1 = self._insert_values('moving2', 100)
    self._check_values_timeout(destination_master, 'vt_destination_keyspace',
                               'moving1', moving1_first_add1, 100)
    self._check_values_timeout(destination_master, 'vt_destination_keyspace',
                               'moving2', moving2_first_add1, 100)
    self.check_binlog_player_vars(destination_master, ['source_keyspace/0'],
                                  seconds_behind_master_max=30)
    self.check_binlog_server_vars(source_replica, horizontal=False,
                                  min_statements=100, min_transactions=100)

    # use vtworker to compare the data
    logging.debug('Running vtworker VerticalSplitDiff')
    utils.run_vtworker(['-cell', 'test_nj',
                        '--use_v3_resharding_mode=false',
                        'VerticalSplitDiff',
                        '--min_healthy_rdonly_tablets', '1',
                        'destination_keyspace/0'], auto_log=True)

    utils.pause('Good time to test vtworker for diffs')

    # get status for destination master tablet, make sure we have it all
    self.check_running_binlog_player(destination_master, 700, 300,
                                     extra_text='moving')

    # check query service is off on destination master, as filtered
    # replication is enabled. Even health check should not interfere.
    destination_master_vars = utils.get_vars(destination_master.port)
    self.assertEqual(destination_master_vars['TabletStateName'], 'NOT_SERVING')

    # check we can't migrate the master just yet
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'master'],
                    expect_fail=True)

    # migrate rdonly only in test_ny cell, make sure nothing is migrated
    # in test_nj
    utils.run_vtctl(['MigrateServedFrom', '--cells=test_ny',
                     'destination_keyspace/0', 'rdonly'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n'
                             'ServedFrom(rdonly): source_keyspace\n'
                             'ServedFrom(replica): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, None)
    self._check_blacklisted_tables(source_rdonly1, None)
    self._check_blacklisted_tables(source_rdonly2, None)

    # migrate test_nj only, using command line manual fix command,
    # and restore it back.
    keyspace_json = utils.run_vtctl_json(
        ['GetKeyspace', 'destination_keyspace'])
    found = False
    for ksf in keyspace_json['served_froms']:
      if ksf['tablet_type'] == topodata_pb2.RDONLY:
        found = True
        self.assertEqual(sorted(ksf['cells']), ['test_ca', 'test_nj'])
    self.assertTrue(found)
    utils.run_vtctl(['SetKeyspaceServedFrom', '-source=source_keyspace',
                     '-remove', '-cells=test_nj,test_ca', 'destination_keyspace',
                     'rdonly'], auto_log=True)
    keyspace_json = utils.run_vtctl_json(
        ['GetKeyspace', 'destination_keyspace'])
    found = False
    for ksf in keyspace_json['served_froms']:
      if ksf['tablet_type'] == topodata_pb2.RDONLY:
        found = True
    self.assertFalse(found)
    utils.run_vtctl(['SetKeyspaceServedFrom', '-source=source_keyspace',
                     'destination_keyspace', 'rdonly'],
                    auto_log=True)
    keyspace_json = utils.run_vtctl_json(
        ['GetKeyspace', 'destination_keyspace'])
    found = False
    for ksf in keyspace_json['served_froms']:
      if ksf['tablet_type'] == topodata_pb2.RDONLY:
        found = True
        self.assertTrue('cells' not in ksf or not ksf['cells'])
    self.assertTrue(found)

    # now serve rdonly from the destination shards
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'rdonly'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n'
                             'ServedFrom(replica): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, None)
    self._check_blacklisted_tables(source_rdonly1, ['/moving/', 'view1'])
    self._check_blacklisted_tables(source_rdonly2, ['/moving/', 'view1'])
    self._check_client_conn_redirection(
        'destination_keyspace',
        ['master', 'replica'], ['moving1', 'moving2'])

    # then serve replica from the destination shards
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'replica'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, ['/moving/', 'view1'])
    self._check_blacklisted_tables(source_rdonly1, ['/moving/', 'view1'])
    self._check_blacklisted_tables(source_rdonly2, ['/moving/', 'view1'])
    self._check_client_conn_redirection(
        'destination_keyspace',
        ['master'], ['moving1', 'moving2'])

    # move replica back and forth
    utils.run_vtctl(['MigrateServedFrom', '-reverse',
                     'destination_keyspace/0', 'replica'], auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n'
                             'ServedFrom(replica): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, None)
    self._check_blacklisted_tables(source_rdonly1, ['/moving/', 'view1'])
    self._check_blacklisted_tables(source_rdonly2, ['/moving/', 'view1'])
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'replica'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, ['/moving/', 'view1'])
    self._check_blacklisted_tables(source_rdonly1, ['/moving/', 'view1'])
    self._check_blacklisted_tables(source_rdonly2, ['/moving/', 'view1'])
    self._check_client_conn_redirection(
        'destination_keyspace',
        ['master'], ['moving1', 'moving2'])

    # Cancel should fail now
    utils.run_vtctl(['CancelResharding', 'destination_keyspace/0'],
                    auto_log=True, expect_fail=True)

    # then serve master from the destination shards
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'master'],
                    auto_log=True)
    self._check_srv_keyspace('')
    self._check_blacklisted_tables(source_master, ['/moving/', 'view1'])
    self._check_blacklisted_tables(source_replica, ['/moving/', 'view1'])
    self._check_blacklisted_tables(source_rdonly1, ['/moving/', 'view1'])
    self._check_blacklisted_tables(source_rdonly2, ['/moving/', 'view1'])

    # check the binlog player is gone now
    self.check_no_binlog_player(destination_master)

    # check the stats are correct
    self._check_stats()

    # now remove the tables on the source shard. The blacklisted tables
    # in the source shard won't match any table, make sure that works.
    utils.run_vtctl(['ApplySchema',
                     '-sql=drop view view1',
                     'source_keyspace'],
                    auto_log=True)
    for t in ['moving1', 'moving2']:
      utils.run_vtctl(['ApplySchema',
                       '-sql=drop table %s' % (t),
                       'source_keyspace'],
                      auto_log=True)
    for t in [source_master, source_replica, source_rdonly1, source_rdonly2]:
      utils.run_vtctl(['ReloadSchema', t.tablet_alias])
    qr = source_master.execute('select count(1) from staying1')
    self.assertEqual(len(qr['rows']), 1,
                     'cannot read staying1: got %s' % str(qr))

    # test SetShardTabletControl
    self._verify_vtctl_set_shard_tablet_control()

  def _verify_vtctl_set_shard_tablet_control(self):
    """Test that manually editing the blacklisted tables works correctly.

    TODO(mberlin): This is more an integration test and should be moved to the
    Go codebase eventually.
    """
    # check 'vtctl SetShardTabletControl' command works as expected:
    # clear the rdonly entry:
    utils.run_vtctl(['SetShardTabletControl', '--remove', 'source_keyspace/0',
                     'rdonly'], auto_log=True)
    self._assert_tablet_controls([topodata_pb2.MASTER, topodata_pb2.REPLICA])

    # re-add rdonly:
    utils.run_vtctl(['SetShardTabletControl',
                     '--blacklisted_tables=/moving/,view1',
                     'source_keyspace/0', 'rdonly'], auto_log=True)
    self._assert_tablet_controls([topodata_pb2.MASTER, topodata_pb2.REPLICA,
                                  topodata_pb2.RDONLY])

    # and then clear all entries:
    utils.run_vtctl(['SetShardTabletControl', '--remove', 'source_keyspace/0',
                     'rdonly'], auto_log=True)
    utils.run_vtctl(['SetShardTabletControl', '--remove', 'source_keyspace/0',
                     'replica'], auto_log=True)
    utils.run_vtctl(['SetShardTabletControl', '--remove', 'source_keyspace/0',
                     'master'], auto_log=True)
    shard_json = utils.run_vtctl_json(['GetShard', 'source_keyspace/0'])
    self.assertTrue('tablet_controls' not in shard_json or
                    not shard_json['tablet_controls'])

  def _assert_tablet_controls(self, expected_dbtypes):
    shard_json = utils.run_vtctl_json(['GetShard', 'source_keyspace/0'])
    self.assertEqual(len(shard_json['tablet_controls']), len(expected_dbtypes))

    expected_dbtypes_set = set(expected_dbtypes)
    for tc in shard_json['tablet_controls']:
      self.assertIn(tc['tablet_type'], expected_dbtypes_set)
      self.assertEqual(['/moving/', 'view1'], tc['blacklisted_tables'])
      expected_dbtypes_set.remove(tc['tablet_type'])
    self.assertEqual(0, len(expected_dbtypes_set),
                     'Not all expected db types were blacklisted')


if __name__ == '__main__':
  utils.main()
