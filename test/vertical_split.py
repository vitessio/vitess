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

from zk import zkocc
from vtdb import vtclient

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
  def setUp(self):
    self.vtgate_server, self.vtgate_port = utils.vtgate_start(cache_ttl='0s')
    self.vtgate_client = zkocc.ZkOccConnection("localhost:%u"%self.vtgate_port,
                                               "test_nj", 30.0)
    self.insert_index = 0

  def tearDown(self):
    self.vtgate_client.close()
    utils.vtgate_kill(self.vtgate_server)

  def _create_source_schema(self):
    create_table_template = '''create table %s(
id bigint not null,
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
      # Two schema updates in the same second are not properly processed.
      # So sleep a little bit between updates. This is crazy.
      time.sleep(1)
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + create_view_template % ('view1', 'moving1'),
                     'source_keyspace'],
                    auto_log=True)

  def _vtdb_conn(self, db_type='master', keyspace='source_keyspace', vtgate_protocol='v0', vtgate_addrs=None):
    if vtgate_addrs is None:
      vtgate_addrs = []
    conn = vtclient.VtOCCConnection(self.vtgate_client, keyspace, '0',
                                    db_type, 30,
                                    vtgate_protocol=vtgate_protocol,
                                    vtgate_addrs=vtgate_addrs)
    conn.connect()
    return conn

  # insert some values in the source master db, return the first id used
  def _insert_values(self, table, count):
    result = self.insert_index
    conn = self._vtdb_conn()
    cursor = conn.cursor()
    for i in xrange(count):
      conn.begin()
      cursor.execute("insert into %s (id, msg) values(%u, 'value %u')" % (
          table, self.insert_index, self.insert_index), {})
      conn.commit()
      self.insert_index += 1
    conn.close()
    return result

  def _check_values(self, tablet, dbname, table, first, count):
    logging.info("Checking %u values from %s/%s starting at %u", count, dbname,
                 table, first)
    rows = tablet.mquery(dbname, 'select id, msg from %s where id>=%u order by id limit %u' % (table, first, count))
    self.assertEqual(count, len(rows), "got wrong number of rows: %u != %u" %
                     (len(rows), count))
    for i in xrange(count):
      self.assertEqual(first + i, rows[i][0], "invalid id[%u]: %u != %u" %
                       (i, first + i, rows[i][0]))
      self.assertEqual('value %u' % (first + i), rows[i][1],
                       "invalid msg[%u]: 'value %u' != '%s'" %
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
        logging.debug("Sleeping for 1s waiting for data in %s/%s", dbname,
                      table)
        time.sleep(1)

  def _check_srv_keyspace(self, expected):
    cell = 'test_nj'
    keyspace = 'destination_keyspace'
    ks = utils.run_vtctl_json(['GetSrvKeyspace', cell, keyspace])
    result = ""
    if 'ServedFrom' in ks and ks['ServedFrom']:
      for served_from in sorted(ks['ServedFrom'].keys()):
        result += "ServedFrom(%s): %s\n" % (served_from,
                                            ks['ServedFrom'][served_from])
    logging.debug("Cell %s keyspace %s has data:\n%s", cell, keyspace, result)
    self.assertEqual(expected, result,
                     "Mismatch in srv keyspace for cell %s keyspace %s, expected:\n%s\ngot:\n%s" % (
                     cell, keyspace, expected, result))
    self.assertEqual('', ks.get('ShardingColumnName'),
                     "Got wrong ShardingColumnName in SrvKeyspace: %s" %
                     str(ks))
    self.assertEqual('', ks.get('ShardingColumnType'),
                     "Got wrong ShardingColumnType in SrvKeyspace: %s" %
                     str(ks))

  def _check_blacklisted_tables(self, tablet, expected):
    ti = utils.run_vtctl_json(['GetTablet', tablet.tablet_alias])
    logging.debug("Tablet %s has balcklisted tables: %s", tablet.tablet_alias,
                  ti['BlacklistedTables'])
    self.assertEqual(ti['BlacklistedTables'], expected,
                     "Got unexpected BlacklistedTables: %s (expecting %s)" %(
                         ti['BlacklistedTables'], expected))

  def _check_client_conn_redirection(self, source_ks, destination_ks, db_types, servedfrom_db_types):
    # check that the ServedFrom indirection worked correctly.
    for db_type in servedfrom_db_types:
      conn = self._vtdb_conn(db_type, keyspace=destination_ks)
      self.assertEqual(conn.db_params['keyspace'], source_ks)

    # check that the connection to db_type for destination keyspace works too.
    for db_type in db_types:
      dest_conn = self._vtdb_conn(db_type, keyspace=destination_ks)
      self.assertEqual(dest_conn.db_params['keyspace'], destination_ks)

  def test_vertical_split(self):
    utils.run_vtctl(['CreateKeyspace',
                     'source_keyspace'])
    utils.run_vtctl(['CreateKeyspace',
                     '--served-from', 'master:source_keyspace,replica:source_keyspace,rdonly:source_keyspace',
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
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n' +
                             'ServedFrom(rdonly): source_keyspace\n' +
                             'ServedFrom(replica): source_keyspace\n')

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

    # use the vtworker checker to compare the data
    logging.debug("Running vtworker VerticalSplitDiff")
    utils.run_vtworker(['-cell', 'test_nj', 'VerticalSplitDiff', 'destination_keyspace/0'],
                       auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', source_rdonly.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', destination_rdonly.tablet_alias,
                     'rdonly'], auto_log=True)

    utils.pause("Good time to test vtworker for diffs")

    # check we can't migrate the master just yet
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'master'],
                    expect_fail=True)

    # now serve rdonly from the destination shards
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'rdonly'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n' +
                             'ServedFrom(replica): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, None)
    self._check_blacklisted_tables(source_rdonly, ['moving1', 'moving2'])
    self._check_client_conn_redirection('source_keyspace', 'destination_keyspace', ['rdonly'], ['master', 'replica'])

    # then serve replica from the destination shards
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'replica'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, ['moving1', 'moving2'])
    self._check_blacklisted_tables(source_rdonly, ['moving1', 'moving2'])
    self._check_client_conn_redirection('source_keyspace', 'destination_keyspace', ['replica', 'rdonly'], ['master'])

    # move replica back and forth
    utils.run_vtctl(['MigrateServedFrom', '-reverse', 'destination_keyspace/0', 'replica'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n' +
                             'ServedFrom(replica): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, None)
    self._check_blacklisted_tables(source_rdonly, ['moving1', 'moving2'])
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'replica'],
                    auto_log=True)
    self._check_srv_keyspace('ServedFrom(master): source_keyspace\n')
    self._check_blacklisted_tables(source_master, None)
    self._check_blacklisted_tables(source_replica, ['moving1', 'moving2'])
    self._check_blacklisted_tables(source_rdonly, ['moving1', 'moving2'])
    self._check_client_conn_redirection('source_keyspace', 'destination_keyspace', ['replica', 'rdonly'], ['master'])

    # then serve master from the destination shards
    utils.run_vtctl(['MigrateServedFrom', 'destination_keyspace/0', 'master'],
                    auto_log=True)
    self._check_srv_keyspace('')
    self._check_blacklisted_tables(source_master, ['moving1', 'moving2'])
    self._check_blacklisted_tables(source_replica, ['moving1', 'moving2'])
    self._check_blacklisted_tables(source_rdonly, ['moving1', 'moving2'])
    self._check_client_conn_redirection('source_keyspace', 'destination_keyspace', ['replica', 'rdonly', 'master'], [])

    # check 'vtctl SetBlacklistedTables' command works as expected
    utils.run_vtctl(['SetBlacklistedTables', source_master.tablet_alias,
                     'moving1,moving2,view1'], auto_log=True)
    self._check_blacklisted_tables(source_master, ['moving1', 'moving2',
                                                   'view1'])
    utils.run_vtctl(['SetBlacklistedTables', source_master.tablet_alias],
                    auto_log=True)
    self._check_blacklisted_tables(source_master, None)

    # check the binlog player is gone now
    destination_master.wait_for_binlog_player_count(0)

    # kill everything
    tablet.kill_tablets([source_master, source_replica, source_rdonly,
                         destination_master, destination_replica,
                         destination_rdonly])

if __name__ == '__main__':
  utils.main()
