#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import time
import unittest

from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgate_client

import environment
import utils
import tablet
from protocols_flavor import protocols_flavor

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

def setUpModule():
  try:
    environment.topo_server().setup()

    setup_procs = [
        source_master.init_mysql(),
        source_replica.init_mysql(),
        source_rdonly1.init_mysql(),
        source_rdonly2.init_mysql(),
        destination_master.init_mysql(),
        destination_replica.init_mysql(),
        destination_rdonly1.init_mysql(),
        destination_rdonly2.init_mysql(),
        ]
    utils.Vtctld().start()
    utils.VtGate().start(cache_ttl='0s')
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  utils.vtgate.kill()
  teardown_procs = [
        source_master.teardown_mysql(),
        source_replica.teardown_mysql(),
        source_rdonly1.teardown_mysql(),
        source_rdonly2.teardown_mysql(),
        destination_master.teardown_mysql(),
        destination_replica.teardown_mysql(),
        destination_rdonly1.teardown_mysql(),
        destination_rdonly2.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  source_master.remove_tree()
  source_replica.remove_tree()
  source_rdonly1.remove_tree()
  source_rdonly2.remove_tree()
  destination_master.remove_tree()
  destination_replica.remove_tree()
  destination_rdonly1.remove_tree()
  destination_rdonly2.remove_tree()

class TestVerticalSplit(unittest.TestCase):
  def setUp(self):
    self.insert_index = 0

  def _create_source_schema(self):
    create_table_template = '''create table %s(
id bigint not null,
msg varchar(64),
primary key (id),
index by_msg (msg)
) Engine=InnoDB'''
    create_view_template = '''create view %s(id, msg) as select id, msg from %s'''

    for t in ['moving1', 'moving2', 'staying1', 'staying2']:
      utils.run_vtctl(['ApplySchema',
                       '-sql=' + create_table_template % (t),
                       'source_keyspace'],
                      auto_log=True)
    utils.run_vtctl(['ApplySchema',
                     '-sql=' + create_view_template % ('view1', 'moving1'),
                     'source_keyspace'],
                    auto_log=True)

  def _vtdb_conn(self):
    addr = utils.vtgate.rpc_endpoint()
    protocol = protocols_flavor().vtgate_python_protocol()
    return vtgate_client.connect(protocol, addr, 30.0)

  # insert some values in the source master db, return the first id used
  def _insert_values(self, table, count):
    result = self.insert_index
    conn = self._vtdb_conn()
    cursor = conn.cursor('source_keyspace', 'master', keyranges=[keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)], writable=True)
    for i in xrange(count):
      conn.begin()
      cursor.execute("insert into %s (id, msg) values(%d, 'value %d')" % (
          table, self.insert_index, self.insert_index), {})
      conn.commit()
      self.insert_index += 1
    conn.close()
    return result

  def _check_values(self, tablet, dbname, table, first, count):
    logging.debug('Checking %d values from %s/%s starting at %d', count, dbname,
                  table, first)
    rows = tablet.mquery(dbname, 'select id, msg from %s where id>=%d order by id limit %d' % (table, first, count))
    self.assertEqual(count, len(rows), 'got wrong number of rows: %d != %d' %
                     (len(rows), count))
    for i in xrange(count):
      self.assertEqual(first + i, rows[i][0], 'invalid id[%d]: %d != %d' %
                       (i, first + i, rows[i][0]))
      self.assertEqual('value %d' % (first + i), rows[i][1],
                       "invalid msg[%d]: 'value %d' != '%s'" %
                       (i, first + i, rows[i][1]))

  def _check_values_timeout(self, tablet, dbname, table, first, count,
                            timeout=30):
    while True:
      try:
        self._check_values(tablet, dbname, table, first, count)
        return
      except:
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
    if 'ServedFrom' in ks and ks['ServedFrom']:
      for served_from in sorted(ks['ServedFrom'].keys()):
        result += 'ServedFrom(%s): %s\n' % (served_from,
                                            ks['ServedFrom'][served_from])
    logging.debug('Cell %s keyspace %s has data:\n%s', cell, keyspace, result)
    self.assertEqual(expected, result,
                     'Mismatch in srv keyspace for cell %s keyspace %s, expected:\n%s\ngot:\n%s' % (
                     cell, keyspace, expected, result))
    self.assertEqual('', ks.get('ShardingColumnName'),
                     'Got wrong ShardingColumnName in SrvKeyspace: %s' %
                     str(ks))
    self.assertEqual('', ks.get('ShardingColumnType'),
                     'Got wrong ShardingColumnType in SrvKeyspace: %s' %
                     str(ks))

  def _check_blacklisted_tables(self, tablet, expected):
    status = tablet.get_status()
    if expected:
      self.assertIn('BlacklistedTables: %s' % ' '.join(expected), status)
    else:
      self.assertNotIn('BlacklistedTables', status)

    # check we can or cannot access the tables
    utils.run_vtctl(['ReloadSchema', tablet.tablet_alias])
    for t in ['moving1', 'moving2']:
      if expected and 'moving.*' in expected:
        # table is blacklisted, should get the error
        _, stderr = utils.run_vtctl(['VtTabletExecute',
                                     '-keyspace', tablet.keyspace,
                                     '-shard', tablet.shard,
                                     tablet.tablet_alias,
                                     'select count(1) from %s' % t],
                                    expect_fail=True)
        self.assertIn('retry: Query disallowed due to rule: enforce blacklisted tables', stderr)
      else:
        # table is not blacklisted, should just work
        qr = tablet.execute('select count(1) from %s' % t)
        logging.debug('Got %s rows from table %s on tablet %s',
                      qr['Rows'][0][0], t, tablet.tablet_alias)

  def _check_client_conn_redirection(self, source_ks, destination_ks, db_types, servedfrom_db_types, moved_tables=None):
    # check that the ServedFrom indirection worked correctly.
    if moved_tables is None:
      moved_tables = []
    conn = self._vtdb_conn()
    for db_type in servedfrom_db_types:
      for tbl in moved_tables:
        try:
          rows = conn._execute("select * from %s" % tbl, {}, destination_ks, db_type, keyranges=[keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)])
          logging.debug("Select on %s.%s returned %d rows" % (db_type, tbl, len(rows)))
        except Exception, e:
          self.fail("Execute failed w/ exception %s" % str(e))

  def _check_stats(self):
    v = utils.vtgate.get_vars()
    self.assertEqual(v['VttabletCall']['Histograms']['Execute.source_keyspace.0.replica']['Count'], 2, "unexpected value for VttabletCall(Execute.source_keyspace.0.replica) inside %s" % str(v))
    self.assertEqual(v['VtgateApi']['Histograms']['ExecuteKeyRanges.destination_keyspace.master']['Count'], 6, "unexpected value for VtgateApi(ExecuteKeyRanges.destination_keyspace.master) inside %s" % str(v))
    self.assertEqual(len(v['VtgateApiErrorCounts']), 0, "unexpected errors for VtgateApiErrorCounts inside %s" % str(v))
    self.assertEqual(
            v['ResilientSrvTopoServerEndPointsReturnedCount']['test_nj.source_keyspace.0.master'] /
              v['ResilientSrvTopoServerEndPointQueryCount']['test_nj.source_keyspace.0.master'],
            1, "unexpected EndPointsReturnedCount inside %s" % str(v))
    self.assertNotIn(
            'test_nj.source_keyspace.0.master', v['ResilientSrvTopoServerEndPointDegradedResultCount'],
            "unexpected EndPointDegradedResultCount inside %s" % str(v))

  def test_vertical_split(self):
    utils.run_vtctl(['CreateKeyspace', 'source_keyspace'])
    utils.run_vtctl(['CreateKeyspace',
                     '--served_from', 'master:source_keyspace,replica:source_keyspace,rdonly:source_keyspace',
                     'destination_keyspace'])
    source_master.init_tablet('master', 'source_keyspace', '0')
    source_replica.init_tablet('replica', 'source_keyspace', '0')
    source_rdonly1.init_tablet('rdonly', 'source_keyspace', '0')
    source_rdonly2.init_tablet('rdonly', 'source_keyspace', '0')

    # rebuild destination keyspace to make sure there is a serving
    # graph entry, even though there is no tablet yet.
    utils.run_vtctl(['RebuildKeyspaceGraph', 'source_keyspace'], auto_log=True)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'destination_keyspace'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n' +
                             'ServedFrom(rdonly): source_keyspace\n' +
                             'ServedFrom(replica): source_keyspace\n')

    destination_master.init_tablet('master', 'destination_keyspace', '0')
    destination_replica.init_tablet('replica', 'destination_keyspace', '0')
    destination_rdonly1.init_tablet('rdonly', 'destination_keyspace', '0')
    destination_rdonly2.init_tablet('rdonly', 'destination_keyspace', '0')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'source_keyspace'], auto_log=True)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'destination_keyspace'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n' +
                             'ServedFrom(rdonly): source_keyspace\n' +
                             'ServedFrom(replica): source_keyspace\n')

    # create databases so vttablet can start behaving normally
    for t in [source_master, source_replica, source_rdonly1, source_rdonly2]:
      t.create_db('vt_source_keyspace')
      t.start_vttablet(wait_for_state=None)
    destination_master.start_vttablet(wait_for_state=None,
                                      target_tablet_type='replica')
    for t in [destination_replica, destination_rdonly1, destination_rdonly2]:
      t.start_vttablet(wait_for_state=None)

    # wait for the tablets
    for t in [source_master, source_replica, source_rdonly1, source_rdonly2]:
      t.wait_for_vttablet_state('SERVING')
    for t in [destination_master, destination_replica, destination_rdonly1,
              destination_rdonly2]:
      t.wait_for_vttablet_state('NOT_SERVING')

    # reparent to make the tablets work
    utils.run_vtctl(['InitShardMaster', 'source_keyspace/0',
                     source_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', 'destination_keyspace/0',
                     destination_master.tablet_alias], auto_log=True)

    # create the schema on the source keyspace, add some values
    self._create_source_schema()
    moving1_first = self._insert_values('moving1', 100)
    moving2_first = self._insert_values('moving2', 100)
    staying1_first = self._insert_values('staying1', 100)
    staying2_first = self._insert_values('staying2', 100)
    self._check_values(source_master, 'vt_source_keyspace', 'moving1',
                       moving1_first, 100)
    self._check_values(source_master, 'vt_source_keyspace', 'moving2',
                       moving2_first, 100)
    self._check_values(source_master, 'vt_source_keyspace', 'staying1',
                       staying1_first, 100)
    self._check_values(source_master, 'vt_source_keyspace', 'staying2',
                       staying2_first, 100)
    self._check_values(source_master, 'vt_source_keyspace', 'view1',
                       moving1_first, 100)

    # the worker will do everything. We test with source_reader_count=10
    # (down from default=20) as connection pool is not big enough for 20.
    # min_table_size_for_split is set to 1 as to force a split even on the
    # small table we have.
    utils.run_vtctl(['CopySchemaShard', '--tables', 'moving.*,view1',
                     source_rdonly1.tablet_alias, 'destination_keyspace/0'],
                    auto_log=True)

    utils.run_vtworker(['--cell', 'test_nj',
                        '--command_display_interval', '10ms',
                        'VerticalSplitClone',
                        '--tables', 'moving.*,view1',
                        '--strategy=-populate_blp_checkpoint',
                        '--source_reader_count', '10',
                        '--min_table_size_for_split', '1',
                        'destination_keyspace/0'],
                       auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', source_rdonly1.tablet_alias,
                     'rdonly'], auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', source_rdonly2.tablet_alias,
                     'rdonly'], auto_log=True)

    # check values are present
    self._check_values(destination_master, 'vt_destination_keyspace', 'moving1',
                       moving1_first, 100)
    self._check_values(destination_master, 'vt_destination_keyspace', 'moving2',
                       moving2_first, 100)
    self._check_values(destination_master, 'vt_destination_keyspace', 'view1',
                       moving1_first, 100)

    # check the binlog players is running
    destination_master.wait_for_binlog_player_count(1)

    # add values to source, make sure they're replicated
    moving1_first_add1 = self._insert_values('moving1', 100)
    staying1_first_add1 = self._insert_values('staying1', 100)
    moving2_first_add1 = self._insert_values('moving2', 100)
    self._check_values_timeout(destination_master, 'vt_destination_keyspace',
                               'moving1', moving1_first_add1, 100)
    self._check_values_timeout(destination_master, 'vt_destination_keyspace',
                               'moving2', moving2_first_add1, 100)

    # use vtworker to compare the data
    logging.debug('Running vtworker VerticalSplitDiff')
    utils.run_vtworker(['-cell', 'test_nj', 'VerticalSplitDiff',
                        'destination_keyspace/0'], auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', source_rdonly1.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', source_rdonly2.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', destination_rdonly1.tablet_alias,
                     'rdonly'], auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', destination_rdonly2.tablet_alias,
                     'rdonly'], auto_log=True)

    utils.pause('Good time to test vtworker for diffs')

    # get status for destination master tablet, make sure we have it all
    destination_master_status = destination_master.get_status()
    self.assertIn('Binlog player state: Running', destination_master_status)
    self.assertIn('moving.*', destination_master_status)
    self.assertIn('<td><b>All</b>: 1000<br><b>Query</b>: 700<br><b>Transaction</b>: 300<br></td>', destination_master_status)
    self.assertIn('</html>', destination_master_status)

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
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n' +
                             'ServedFrom(rdonly): source_keyspace\n' +
                             'ServedFrom(replica): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, None)
    self._check_blacklisted_tables(source_rdonly1, None)
    self._check_blacklisted_tables(source_rdonly2, None)

    # migrate test_nj only, using command line manual fix command,
    # and restore it back.
    keyspace_json = utils.run_vtctl_json(['GetKeyspace', 'destination_keyspace'])
    found = False
    for ksf in keyspace_json['served_froms']:
      if ksf['tablet_type'] == 4:
        found = True
        self.assertEqual(ksf['cells'], ['test_nj'])
    self.assertTrue(found)
    utils.run_vtctl(['SetKeyspaceServedFrom', '-source=source_keyspace',
                     '-remove', '-cells=test_nj', 'destination_keyspace',
                     'rdonly'], auto_log=True)
    keyspace_json = utils.run_vtctl_json(['GetKeyspace', 'destination_keyspace'])
    found = False
    for ksf in keyspace_json['served_froms']:
      if ksf['tablet_type'] == 4:
        found = True
    self.assertFalse(found)
    utils.run_vtctl(['SetKeyspaceServedFrom', '-source=source_keyspace',
                     'destination_keyspace', 'rdonly'],
                    auto_log=True)
    keyspace_json = utils.run_vtctl_json(['GetKeyspace', 'destination_keyspace'])
    found = False
    for ksf in keyspace_json['served_froms']:
      if ksf['tablet_type'] == 4:
        found = True
        self.assertNotIn('cells', ksf)
    self.assertTrue(found)

    # now serve rdonly from the destination shards
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'rdonly'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n' +
                             'ServedFrom(replica): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, None)
    self._check_blacklisted_tables(source_rdonly1, ['moving.*', 'view1'])
    self._check_blacklisted_tables(source_rdonly2, ['moving.*', 'view1'])
    self._check_client_conn_redirection(
        'source_keyspace', 'destination_keyspace', ['rdonly'],
        ['master', 'replica'], ['moving1', 'moving2'])

    # then serve replica from the destination shards
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'replica'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, ['moving.*', 'view1'])
    self._check_blacklisted_tables(source_rdonly1, ['moving.*', 'view1'])
    self._check_blacklisted_tables(source_rdonly2, ['moving.*', 'view1'])
    self._check_client_conn_redirection('source_keyspace', 'destination_keyspace', ['replica', 'rdonly'], ['master'], ['moving1', 'moving2'])

    # move replica back and forth
    utils.run_vtctl(['MigrateServedFrom', '-reverse',
                     'destination_keyspace/0', 'replica'], auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n' +
                             'ServedFrom(replica): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, None)
    self._check_blacklisted_tables(source_rdonly1, ['moving.*', 'view1'])
    self._check_blacklisted_tables(source_rdonly2, ['moving.*', 'view1'])
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'replica'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, ['moving.*', 'view1'])
    self._check_blacklisted_tables(source_rdonly1, ['moving.*', 'view1'])
    self._check_blacklisted_tables(source_rdonly2, ['moving.*', 'view1'])
    self._check_client_conn_redirection(
        'source_keyspace', 'destination_keyspace', ['replica', 'rdonly'],
        ['master'], ['moving1', 'moving2'])

    # then serve master from the destination shards
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'master'],
                    auto_log=True)
    self._check_srv_keyspace('')
    self._check_blacklisted_tables(source_master, ['moving.*', 'view1'])
    self._check_blacklisted_tables(source_replica, ['moving.*', 'view1'])
    self._check_blacklisted_tables(source_rdonly1, ['moving.*', 'view1'])
    self._check_blacklisted_tables(source_rdonly2, ['moving.*', 'view1'])
    self._check_client_conn_redirection(
        'source_keyspace', 'destination_keyspace',
        ['replica', 'rdonly', 'master'], [], ['moving1', 'moving2'])

    # check 'vtctl SetShardTabletControl' command works as expected:
    # clear the rdonly entry, re-add it, and then clear all entries.
    utils.run_vtctl(['SetShardTabletControl', '--remove', 'source_keyspace/0',
                     'rdonly'], auto_log=True)
    shard_json = utils.run_vtctl_json(['GetShard', 'source_keyspace/0'])
    self.assertEqual(len(shard_json['tablet_controls']), 2)
    for tc in shard_json['tablet_controls']:
      self.assertIn(tc['tablet_type'], [
          tablet.Tablet.tablet_type_value['MASTER'],
          tablet.Tablet.tablet_type_value['REPLICA']])
    utils.run_vtctl(['SetShardTabletControl', '--tables=moving.*,view1',
                     'source_keyspace/0', 'rdonly'], auto_log=True)
    shard_json = utils.run_vtctl_json(['GetShard', 'source_keyspace/0'])
    for tc in shard_json['tablet_controls']:
      if tc['tablet_type'] == 4:
        break
    self.assertEqual(['moving.*', 'view1'], tc['blacklisted_tables'])
    utils.run_vtctl(['SetShardTabletControl', '--remove', 'source_keyspace/0',
                     'rdonly'], auto_log=True)
    utils.run_vtctl(['SetShardTabletControl', '--remove', 'source_keyspace/0',
                     'replica'], auto_log=True)
    utils.run_vtctl(['SetShardTabletControl', '--remove', 'source_keyspace/0',
                     'master'], auto_log=True)
    shard_json = utils.run_vtctl_json(['GetShard', 'source_keyspace/0'])
    self.assertNotIn('tablet_controls', shard_json)

    # check the binlog player is gone now
    destination_master.wait_for_binlog_player_count(0)

    # check the stats are correct
    self._check_stats()

    # kill everything
    tablet.kill_tablets([source_master, source_replica, source_rdonly1,
                         source_rdonly2, destination_master,
                         destination_replica, destination_rdonly1,
                         destination_rdonly2])

if __name__ == '__main__':
  utils.main()
