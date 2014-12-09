#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import base64
import logging
import os
import threading
import struct
import time
import unittest

from vtdb import dbexceptions
from vtdb import keyrange_constants

import environment
import utils
import tablet

use_clone_worker = False

keyspace_id_type = keyrange_constants.KIT_UINT64
pack_keyspace_id = struct.Struct('!Q').pack

# initial shards
# range "" - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_ny_rdonly = tablet.Tablet(cell='ny')
# range 80 - ""
shard_1_master = tablet.Tablet()
shard_1_slave1 = tablet.Tablet()
shard_1_slave2 = tablet.Tablet()
shard_1_ny_rdonly = tablet.Tablet(cell='ny')
shard_1_rdonly1 = tablet.Tablet()
shard_1_rdonly2 = tablet.Tablet()

# split shards
# range 80 - c0
shard_2_master = tablet.Tablet()
shard_2_replica1 = tablet.Tablet()
shard_2_replica2 = tablet.Tablet()
# range c0 - ""
shard_3_master = tablet.Tablet()
shard_3_replica = tablet.Tablet()
shard_3_rdonly1 = tablet.Tablet()
shard_3_rdonly2 = tablet.Tablet()


def setUpModule():
  try:
    environment.topo_server().setup()

    setup_procs = [
        shard_0_master.init_mysql(),
        shard_0_replica.init_mysql(),
        shard_0_ny_rdonly.init_mysql(),
        shard_1_master.init_mysql(),
        shard_1_slave1.init_mysql(),
        shard_1_slave2.init_mysql(),
        shard_1_ny_rdonly.init_mysql(),
        shard_1_rdonly1.init_mysql(),
        shard_1_rdonly2.init_mysql(),
        shard_2_master.init_mysql(),
        shard_2_replica1.init_mysql(),
        shard_2_replica2.init_mysql(),
        shard_3_master.init_mysql(),
        shard_3_replica.init_mysql(),
        shard_3_rdonly1.init_mysql(),
        shard_3_rdonly2.init_mysql(),
        ]
    utils.Vtctld().start()
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_replica.teardown_mysql(),
      shard_0_ny_rdonly.teardown_mysql(),
      shard_1_master.teardown_mysql(),
      shard_1_slave1.teardown_mysql(),
      shard_1_slave2.teardown_mysql(),
      shard_1_ny_rdonly.teardown_mysql(),
      shard_1_rdonly1.teardown_mysql(),
      shard_1_rdonly2.teardown_mysql(),
      shard_2_master.teardown_mysql(),
      shard_2_replica1.teardown_mysql(),
      shard_2_replica2.teardown_mysql(),
      shard_3_master.teardown_mysql(),
      shard_3_replica.teardown_mysql(),
      shard_3_rdonly1.teardown_mysql(),
      shard_3_rdonly2.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_0_ny_rdonly.remove_tree()
  shard_1_master.remove_tree()
  shard_1_slave1.remove_tree()
  shard_1_slave2.remove_tree()
  shard_1_ny_rdonly.remove_tree()
  shard_1_rdonly1.remove_tree()
  shard_1_rdonly2.remove_tree()
  shard_2_master.remove_tree()
  shard_2_replica1.remove_tree()
  shard_2_replica2.remove_tree()
  shard_3_master.remove_tree()
  shard_3_replica.remove_tree()
  shard_3_rdonly1.remove_tree()
  shard_3_rdonly2.remove_tree()


# InsertThread will insert a value into the timestamps table, and then
# every 1/5s will update its value with the current timestamp
class InsertThread(threading.Thread):

  def __init__(self, tablet, object_name, user_id, keyspace_id):
    threading.Thread.__init__(self)
    self.tablet = tablet
    self.object_name = object_name
    self.user_id = user_id
    self.keyspace_id = keyspace_id
    if keyspace_id_type == keyrange_constants.KIT_BYTES:
      self.str_keyspace_id = base64.b64encode(pack_keyspace_id(keyspace_id))
    else:
      self.str_keyspace_id = "%u" % keyspace_id
    self.done = False

    self.tablet.mquery('vt_test_keyspace', [
        'begin',
        'insert into timestamps(name, time_milli, keyspace_id) values("%s", %u, 0x%x) /* EMD keyspace_id:%s user_id:%u */' %
        (self.object_name, long(time.time() * 1000), self.keyspace_id,
         self.str_keyspace_id, self.user_id),
        'commit'
        ], write=True, user='vt_app')
    self.start()

  def run(self):
    try:
      while not self.done:
        self.tablet.mquery('vt_test_keyspace', [
            'begin',
            'update timestamps set time_milli=%u where name="%s" /* EMD keyspace_id:%s user_id:%u */' % (long(time.time() * 1000), self.object_name, self.str_keyspace_id, self.user_id),
            'commit'
            ], write=True, user='vt_app')
        time.sleep(0.2)
    except Exception as e:
      logging.error("InsertThread got exception: %s", e)


# MonitorLagThread will get values from a database, and compare the timestamp
# to evaluate lag. Since the qps is really low, and we send binlogs as chuncks,
# the latency is pretty high (a few seconds).
class MonitorLagThread(threading.Thread):

  def __init__(self, tablet, object_name):
    threading.Thread.__init__(self)
    self.tablet = tablet
    self.object_name = object_name
    self.done = False
    self.max_lag = 0
    self.lag_sum = 0
    self.sample_count = 0
    self.start()

  def run(self):
    try:
      while not self.done:
        result = self.tablet.mquery('vt_test_keyspace', 'select time_milli from timestamps where name="%s"' % self.object_name)
        if result:
          lag = long(time.time() * 1000) - long(result[0][0])
          logging.debug("MonitorLagThread(%s) got %u", self.object_name, lag)
          self.sample_count += 1
          self.lag_sum += lag
          if lag > self.max_lag:
            self.max_lag = lag
        time.sleep(1.0)
    except Exception as e:
      logging.error("MonitorLagThread got exception: %s", e)


class TestResharding(unittest.TestCase):

  # create_schema will create the same schema on the keyspace
  # then insert some values
  def _create_schema(self):
    if keyspace_id_type == keyrange_constants.KIT_BYTES:
      t = 'varbinary(64)'
    else:
      t = 'bigint(20) unsigned'
    create_table_template = '''create table %s(
id bigint auto_increment,
msg varchar(64),
keyspace_id ''' + t + ''' not null,
primary key (id),
index by_msg (msg)
) Engine=InnoDB'''
    create_view_template = '''create view %s(id, msg, keyspace_id) as select id, msg, keyspace_id from %s'''
    create_timestamp_table = '''create table timestamps(
name varchar(64),
time_milli bigint(20) unsigned not null,
keyspace_id ''' + t + ''' not null,
primary key (name)
) Engine=InnoDB'''
    create_unrelated_table = '''create table unrelated(
name varchar(64),
primary key (name)
) Engine=InnoDB'''

    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + create_table_template % ("resharding1"),
                     'test_keyspace'],
                    auto_log=True)
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + create_table_template % ("resharding2"),
                     'test_keyspace'],
                    auto_log=True)
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + create_view_template % ("view1", "resharding1"),
                     'test_keyspace'],
                    auto_log=True)
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + create_timestamp_table,
                     'test_keyspace'],
                    auto_log=True)
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + create_unrelated_table,
                     'test_keyspace'],
                    auto_log=True)

  # _insert_value inserts a value in the MySQL database along with the comments
  # required for routing.
  def _insert_value(self, tablet, table, id, msg, keyspace_id):
    if keyspace_id_type == keyrange_constants.KIT_BYTES:
      k = base64.b64encode(pack_keyspace_id(keyspace_id))
    else:
      k = "%u" % keyspace_id
    tablet.mquery('vt_test_keyspace', [
        'begin',
        'insert into %s(id, msg, keyspace_id) values(%u, "%s", 0x%x) /* EMD keyspace_id:%s user_id:%u */' % (table, id, msg, keyspace_id, k, id),
        'commit'
        ], write=True)

  def _get_value(self, tablet, table, id):
    return tablet.mquery('vt_test_keyspace', 'select id, msg, keyspace_id from %s where id=%u' % (table, id))

  def _check_value(self, tablet, table, id, msg, keyspace_id,
                   should_be_here=True):
    result = self._get_value(tablet, table, id)
    if keyspace_id_type == keyrange_constants.KIT_BYTES:
      fmt = "%s"
      keyspace_id = pack_keyspace_id(keyspace_id)
    else:
      fmt = "%x"
    if should_be_here:
      self.assertEqual(result, ((id, msg, keyspace_id),),
                       ("Bad row in tablet %s for id=%u, keyspace_id=" +
                        fmt + ", row=%s") % (tablet.tablet_alias, id,
                                             keyspace_id, str(result)))
    else:
      self.assertEqual(len(result), 0,
                       ("Extra row in tablet %s for id=%u, keyspace_id=" +
                        fmt + ": %s") % (tablet.tablet_alias, id, keyspace_id,
                                         str(result)))

  # _is_value_present_and_correct tries to read a value.
  # if it is there, it will check it is correct and return True if it is.
  # if not correct, it will self.fail.
  # if not there, it will return False.
  def _is_value_present_and_correct(self, tablet, table, id, msg, keyspace_id):
    result = self._get_value(tablet, table, id)
    if len(result) == 0:
      return False
    if keyspace_id_type == keyrange_constants.KIT_BYTES:
      fmt = "%s"
      keyspace_id = pack_keyspace_id(keyspace_id)
    else:
      fmt = "%x"
    self.assertEqual(result, ((id, msg, keyspace_id),),
                     ("Bad row in tablet %s for id=%u, keyspace_id=" + fmt) % (
                         tablet.tablet_alias, id, keyspace_id))
    return True

  def _insert_startup_values(self):
    self._insert_value(shard_0_master, 'resharding1', 1, 'msg1',
                       0x1000000000000000)
    self._insert_value(shard_1_master, 'resharding1', 2, 'msg2',
                       0x9000000000000000)
    self._insert_value(shard_1_master, 'resharding1', 3, 'msg3',
                       0xD000000000000000)

  def _exec_dml(self, tablet, sql, bindvars):
    conn = tablet.conn()
    conn.begin()
    try:
      results = conn._execute(sql, bindvars)
      conn.commit()
      return results(0)
    finally:
      conn.close()

  def _check_startup_values(self):
    # check first value is in the right shard
    self._check_value(shard_2_master, 'resharding1', 2, 'msg2',
                      0x9000000000000000)
    self._check_value(shard_2_replica1, 'resharding1', 2, 'msg2',
                      0x9000000000000000)
    self._check_value(shard_2_replica2, 'resharding1', 2, 'msg2',
                      0x9000000000000000)
    self._check_value(shard_3_master, 'resharding1', 2, 'msg2',
                      0x9000000000000000, should_be_here=False)
    self._check_value(shard_3_replica, 'resharding1', 2, 'msg2',
                      0x9000000000000000, should_be_here=False)
    self._check_value(shard_3_rdonly1, 'resharding1', 2, 'msg2',
                      0x9000000000000000, should_be_here=False)
    self._check_value(shard_3_rdonly2, 'resharding1', 2, 'msg2',
                      0x9000000000000000, should_be_here=False)

    # check second value is in the right shard too
    self._check_value(shard_2_master, 'resharding1', 3, 'msg3',
                      0xD000000000000000, should_be_here=False)
    self._check_value(shard_2_replica1, 'resharding1', 3, 'msg3',
                      0xD000000000000000, should_be_here=False)
    self._check_value(shard_2_replica2, 'resharding1', 3, 'msg3',
                      0xD000000000000000, should_be_here=False)
    self._check_value(shard_3_master, 'resharding1', 3, 'msg3',
                      0xD000000000000000)
    self._check_value(shard_3_replica, 'resharding1', 3, 'msg3',
                      0xD000000000000000)
    self._check_value(shard_3_rdonly1, 'resharding1', 3, 'msg3',
                      0xD000000000000000)
    self._check_value(shard_3_rdonly2, 'resharding1', 3, 'msg3',
                      0xD000000000000000)

  def _insert_lots(self, count, base=0):
    for i in xrange(count):
      self._insert_value(shard_1_master, 'resharding1', 10000 + base + i,
                         'msg-range1-%u' % i, 0xA000000000000000 + base + i)
      self._insert_value(shard_1_master, 'resharding1', 20000 + base + i,
                         'msg-range2-%u' % i, 0xE000000000000000 + base + i)

  # _check_lots returns how many of the values we have, in percents.
  def _check_lots(self, count, base=0):
    found = 0
    for i in xrange(count):
      if self._is_value_present_and_correct(shard_2_replica2, 'resharding1',
                                            10000 + base + i, 'msg-range1-%u' %
                                            i, 0xA000000000000000 + base + i):
        found += 1
      if self._is_value_present_and_correct(shard_3_replica, 'resharding1',
                                            20000 + base + i, 'msg-range2-%u' %
                                            i, 0xE000000000000000 + base + i):
        found += 1
    percent = found * 100 / count / 2
    logging.debug("I have %u%% of the data", percent)
    return percent

  def _check_lots_timeout(self, count, threshold, timeout, base=0):
    while True:
      value = self._check_lots(count, base=base)
      if value >= threshold:
        return
      if timeout == 0:
        self.fail("timeout waiting for %u%% of the data" % threshold)
      logging.debug("sleeping until we get %u%%", threshold)
      time.sleep(1)
      timeout -= 1

  # _check_lots_not_present makes sure no data is in the wrong shard
  def _check_lots_not_present(self, count, base=0):
    found = 0
    for i in xrange(count):
      self._check_value(shard_3_replica, 'resharding1', 10000 + base + i,
                        'msg-range1-%u' % i, 0xA000000000000000 + base + i,
                        should_be_here=False)
      self._check_value(shard_2_replica2, 'resharding1', 20000 + base + i,
                        'msg-range2-%u' % i, 0xE000000000000000 + base + i,
                        should_be_here=False)

  def _check_binlog_server_vars(self, tablet):
    v = utils.get_vars(tablet.port)
    self.assertTrue('UpdateStreamKeyRangeStatements' in v)
    self.assertTrue('UpdateStreamKeyRangeTransactions' in v)

  def _check_binlog_player_vars(self, tablet, seconds_behind_master_max = 0):
    v = utils.get_vars(tablet.port)
    self.assertTrue('BinlogPlayerMapSize' in v)
    self.assertTrue('BinlogPlayerSecondsBehindMaster' in v)
    self.assertTrue('BinlogPlayerSecondsBehindMasterMap' in v)
    self.assertTrue('BinlogPlayerSourceShardNameMap' in v)
    self.assertTrue('0' in v['BinlogPlayerSourceShardNameMap'])
    self.assertEquals(v['BinlogPlayerSourceShardNameMap']['0'], 'test_keyspace/80-')
    self.assertTrue('BinlogPlayerSourceTabletAliasMap' in v)
    self.assertTrue('0' in v['BinlogPlayerSourceTabletAliasMap'])
    if seconds_behind_master_max != 0:
      self.assertTrue(v['BinlogPlayerSecondsBehindMaster'] <
                      seconds_behind_master_max,
                      'BinlogPlayerSecondsBehindMaster is too high: %u > %u' % (
                          v['BinlogPlayerSecondsBehindMaster'],
                          seconds_behind_master_max))
      self.assertTrue(v['BinlogPlayerSecondsBehindMasterMap']['0'] <
                      seconds_behind_master_max,
                      'BinlogPlayerSecondsBehindMasterMap is too high: %u > %u' % (
                          v['BinlogPlayerSecondsBehindMasterMap']['0'],
                          seconds_behind_master_max))

  def _test_keyrange_constraints(self):
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*enforce keyspace_id range.*'):
      self._exec_dml(
        shard_0_master,
        "insert into resharding1(id, msg, keyspace_id) values(1, 'msg', :keyspace_id)",
        {"keyspace_id": 0x9000000000000000},
      )
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*enforce keyspace_id range.*'):
      self._exec_dml(
        shard_0_master,
        "update resharding1 set msg = 'msg' where id = 1",
        {"keyspace_id": 0x9000000000000000},
      )
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*enforce keyspace_id range.*'):
      self._exec_dml(
        shard_0_master,
        "delete from resharding1 where id = 1",
        {"keyspace_id": 0x9000000000000000},
      )

  def _check_query_service(self, tablet, serving, tablet_control_disabled):
    """_check_query_service will check that the query service is enabled
    or disabled on the tablet. It will also check if the tablet control
    status is the reason for being enabled / disabled.

    It will also run a remote RunHealthCheck to be sure it doesn't change
    the serving state.
    """
    tablet_vars = utils.get_vars(tablet.port)
    if serving:
      expected_state = 'SERVING'
    else:
      expected_state = 'NOT_SERVING'
    self.assertEqual(tablet_vars['TabletStateName'], expected_state, 'tablet %s is not in the right serving state: got %s expected %s' % (tablet.tablet_alias, tablet_vars['TabletStateName'], expected_state))

    status = tablet.get_status()
    if tablet_control_disabled:
      self.assertIn("Query Service disabled by TabletControl", status)
    else:
      self.assertNotIn("Query Service disabled by TabletControl", status)

    if tablet.tablet_type == 'rdonly':
      utils.run_vtctl(['RunHealthCheck', tablet.tablet_alias, 'rdonly'],
                      auto_log=True)

      tablet_vars = utils.get_vars(tablet.port)
      if serving:
        expected_state = 'SERVING'
      else:
        expected_state = 'NOT_SERVING'
      self.assertEqual(tablet_vars['TabletStateName'], expected_state, 'tablet %s is not in the right serving state after health check: got %s expected %s' % (tablet.tablet_alias, tablet_vars['TabletStateName'], expected_state))

  def test_resharding(self):
    utils.run_vtctl(['CreateKeyspace',
                     '--sharding_column_name', 'bad_column',
                     '--sharding_column_type', 'bytes',
                     '--split_shard_count', '2',
                     'test_keyspace'])
    utils.run_vtctl(['SetKeyspaceShardingInfo', 'test_keyspace',
                     'keyspace_id', 'uint64'], expect_fail=True)
    utils.run_vtctl(['SetKeyspaceShardingInfo',
                     '-force', '-split_shard_count', '4',
                     'test_keyspace', 'keyspace_id', keyspace_id_type])

    shard_0_master.init_tablet( 'master',  'test_keyspace', '-80')
    shard_0_replica.init_tablet('replica', 'test_keyspace', '-80')
    shard_0_ny_rdonly.init_tablet('rdonly', 'test_keyspace', '-80')
    shard_1_master.init_tablet( 'master',  'test_keyspace', '80-')
    shard_1_slave1.init_tablet('replica', 'test_keyspace', '80-')
    shard_1_slave2.init_tablet('spare', 'test_keyspace', '80-')
    shard_1_ny_rdonly.init_tablet('rdonly', 'test_keyspace', '80-')
    shard_1_rdonly1.init_tablet('rdonly', 'test_keyspace', '80-')
    shard_1_rdonly2.init_tablet('rdonly', 'test_keyspace', '80-')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    ks = utils.run_vtctl_json(['GetSrvKeyspace', 'test_nj', 'test_keyspace'])
    self.assertEqual(ks['SplitShardCount'], 4)

    # we set full_mycnf_args to True as a test in the KIT_BYTES case
    full_mycnf_args = keyspace_id_type == keyrange_constants.KIT_BYTES

    # create databases so vttablet can start behaving normally
    for t in [shard_0_master, shard_0_replica, shard_0_ny_rdonly,
              shard_1_master, shard_1_slave1, shard_1_slave2, shard_1_ny_rdonly,
              shard_1_rdonly1, shard_1_rdonly2]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None, full_mycnf_args=full_mycnf_args)

    # wait for the tablets
    shard_0_master.wait_for_vttablet_state('SERVING')
    shard_0_replica.wait_for_vttablet_state('SERVING')
    shard_0_ny_rdonly.wait_for_vttablet_state('SERVING')
    shard_1_master.wait_for_vttablet_state('SERVING')
    shard_1_slave1.wait_for_vttablet_state('SERVING')
    shard_1_slave2.wait_for_vttablet_state('NOT_SERVING') # spare
    shard_1_ny_rdonly.wait_for_vttablet_state('SERVING')
    shard_1_rdonly1.wait_for_vttablet_state('SERVING')
    shard_1_rdonly2.wait_for_vttablet_state('SERVING')

    # reparent to make the tablets work
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)

    # create the tables
    self._create_schema()
    self._insert_startup_values()
    self._test_keyrange_constraints()

    # create the split shards
    shard_2_master.init_tablet(  'master', 'test_keyspace', '80-c0')
    shard_2_replica1.init_tablet('spare',  'test_keyspace', '80-c0')
    shard_2_replica2.init_tablet('spare',  'test_keyspace', '80-c0')
    shard_3_master.init_tablet(  'master', 'test_keyspace', 'c0-')
    shard_3_replica.init_tablet( 'spare',  'test_keyspace', 'c0-')
    shard_3_rdonly1.init_tablet(  'rdonly', 'test_keyspace', 'c0-')
    shard_3_rdonly2.init_tablet(  'rdonly', 'test_keyspace', 'c0-')

    # start vttablet on the split shards (no db created,
    # so they're all not serving)
    shard_3_master.start_vttablet(wait_for_state=None,
                                  target_tablet_type='replica')
    for t in [shard_2_master, shard_2_replica1, shard_2_replica2,
              shard_3_replica, shard_3_rdonly1, shard_3_rdonly2]:
      t.start_vttablet(wait_for_state=None)
    for t in [shard_2_master, shard_2_replica1, shard_2_replica2,
              shard_3_master, shard_3_replica, shard_3_rdonly1,
              shard_3_rdonly2]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/80-c0',
                     shard_2_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/c0-',
                     shard_3_master.tablet_alias], auto_log=True)

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n' +
                             'Partitions(rdonly): -80 80-\n' +
                             'Partitions(replica): -80 80-\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)

    if use_clone_worker:
      # the worker will do everything. We test with source_reader_count=10
      # (down from default=20) as connection pool is not big enough for 20.
      # min_table_size_for_split is set to 1 as to force a split even on the
      # small table we have.
      utils.run_vtworker(['--cell', 'test_nj',
                          '--command_display_interval', '10ms',
                          'SplitClone',
                          '--exclude_tables' ,'unrelated',
                          '--strategy=-populate_blp_checkpoint -write_masters_only',
                          '--source_reader_count', '10',
                          '--min_table_size_for_split', '1',
                          'test_keyspace/80-c0'],
                         auto_log=True)
      utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly1.tablet_alias,
                       'rdonly'], auto_log=True)
      utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly2.tablet_alias,
                       'rdonly'], auto_log=True)

      # TODO(alainjobart): experiment with the dontStartBinlogPlayer option

    else:
      # take the snapshot for the split
      utils.run_vtctl(['MultiSnapshot', '--spec=80-c0-',
                       '--exclude_tables=unrelated',
                       shard_1_slave1.tablet_alias], auto_log=True)

      # the snapshot_copy hook will copy the snapshot files to
      # VTDATAROOT/tmp/... as a test. We want to use these for one half,
      # but not for the other, so we test both scenarios.
      os.unlink(os.path.join(environment.tmproot, "snapshot-from-%s-for-%s.tar" %
                             (shard_1_slave1.tablet_alias, "80-c0")))

      # wait for tablet's binlog server service to be enabled after snapshot
      shard_1_slave1.wait_for_binlog_server_state("Enabled")

      # perform the restores: first one from source tablet. We removed the
      # storage backup, so it's coming from the tablet itself.
      # we also delay starting the binlog player, then enable it.
      utils.run_vtctl(['ShardMultiRestore',
                       '-strategy=-populate_blp_checkpoint -dont_start_binlog_player',
                       'test_keyspace/80-c0', shard_1_slave1.tablet_alias],
                      auto_log=True)

      timeout = 10
      while True:
        shard_2_master_status = shard_2_master.get_status()
        if not "not starting because flag &#39;DontStart&#39; is set" in shard_2_master_status:
          timeout = utils.wait_step('shard 2 master has not failed starting yet', timeout)
          continue
        logging.debug("shard 2 master is waiting on flag removal, good")
        break

      qr = utils.run_vtctl_json(['ExecuteFetch', shard_2_master.tablet_alias, 'update _vt.blp_checkpoint set flags="" where source_shard_uid=0'])
      self.assertEqual(qr['RowsAffected'], 1)

      timeout = 10
      while True:
        shard_2_master_status = shard_2_master.get_status()
        if "not starting because flag &#39;DontStart&#39; is set" in shard_2_master_status:
          timeout = utils.wait_step('shard 2 master has not started replication yet', timeout)
          continue
        logging.debug("shard 2 master has started replication, good")
        break

      # second restore from storage: to be sure, we stop vttablet, and restart
      # it afterwards
      shard_1_slave1.kill_vttablet()
      utils.run_vtctl(['ShardMultiRestore', '-strategy=-populate_blp_checkpoint',
                       'test_keyspace/c0-', shard_1_slave1.tablet_alias],
                      auto_log=True)
      shard_1_slave1.start_vttablet(wait_for_state=None)
      shard_1_slave1.wait_for_binlog_server_state("Enabled")

    # check the startup values are in the right place
    self._check_startup_values()

    # check the schema too
    utils.run_vtctl(['ValidateSchemaKeyspace', '--exclude_tables=unrelated',
                     'test_keyspace'], auto_log=True)

    # check the binlog players are running and exporting vars
    shard_2_master.wait_for_binlog_player_count(1)
    shard_3_master.wait_for_binlog_player_count(1)
    self._check_binlog_player_vars(shard_2_master)
    self._check_binlog_player_vars(shard_3_master)

    # check that binlog server exported the stats vars
    self._check_binlog_server_vars(shard_1_slave1)

    # testing filtered replication: insert a bunch of data on shard 1,
    # check we get most of it after a few seconds, wait for binlog server
    # timeout, check we get all of it.
    logging.debug("Inserting lots of data on source shard")
    self._insert_lots(1000)
    logging.debug("Checking 80 percent of data is sent quickly")
    self._check_lots_timeout(1000, 80, 5)
    logging.debug("Checking all data goes through eventually")
    self._check_lots_timeout(1000, 100, 20)
    logging.debug("Checking no data was sent the wrong way")
    self._check_lots_not_present(1000)
    self._check_binlog_player_vars(shard_2_master, seconds_behind_master_max=30)
    self._check_binlog_player_vars(shard_3_master, seconds_behind_master_max=30)

    # use the vtworker checker to compare the data
    logging.debug("Running vtworker SplitDiff")
    utils.run_vtworker(['-cell', 'test_nj', 'SplitDiff', 'test_keyspace/c0-'],
                       auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly1.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly2.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_3_rdonly1.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_3_rdonly2.tablet_alias, 'rdonly'],
                    auto_log=True)

    utils.pause("Good time to test vtworker for diffs")

    # get status for a destination master tablet, make sure we have it all
    shard_2_master_status = shard_2_master.get_status()
    self.assertIn('Binlog player state: Running', shard_2_master_status)
    self.assertIn('<td><b>All</b>: 6000<br><b>Query</b>: 4000<br><b>Transaction</b>: 2000<br></td>', shard_2_master_status)
    self.assertIn('</html>', shard_2_master_status)

    # start a thread to insert data into shard_1 in the background
    # with current time, and monitor the delay
    insert_thread_1 = InsertThread(shard_1_master, "insert_low", 10000,
                                   0x9000000000000000)
    insert_thread_2 = InsertThread(shard_1_master, "insert_high", 10001,
                                   0xD000000000000000)
    monitor_thread_1 = MonitorLagThread(shard_2_replica2, "insert_low")
    monitor_thread_2 = MonitorLagThread(shard_3_replica, "insert_high")

    # tests a failover switching serving to a different replica
    utils.run_vtctl(['ChangeSlaveType', shard_1_slave2.tablet_alias, 'replica'])
    utils.run_vtctl(['ChangeSlaveType', shard_1_slave1.tablet_alias, 'spare'])
    shard_1_slave2.wait_for_vttablet_state('SERVING')
    shard_1_slave1.wait_for_vttablet_state('NOT_SERVING')

    # test data goes through again
    logging.debug("Inserting lots of data on source shard")
    self._insert_lots(1000, base=1000)
    logging.debug("Checking 80 percent of data was sent quickly")
    self._check_lots_timeout(1000, 80, 5, base=1000)

    # check we can't migrate the master just yet
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'master'],
                    expect_fail=True)

    # check query service is off on master 2 and master 3, as filtered
    # replication is enabled. Even health check that is enabled on
    # master 3 should not interfere.
    self._check_query_service(shard_2_master, False, False)
    self._check_query_service(shard_3_master, False, False)

    # now serve rdonly from the split shards, in test_nj only
    utils.run_vtctl(['MigrateServedTypes', '--cells=test_nj',
                     'test_keyspace/80-', 'rdonly'], auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n' +
                             'Partitions(rdonly): -80 80-c0 c0-\n' +
                             'Partitions(replica): -80 80-\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)
    utils.check_srv_keyspace('test_ny', 'test_keyspace',
                             'Partitions(rdonly): -80 80-\n' +
                             'TabletTypes: rdonly',
                             keyspace_id_type=keyspace_id_type)
    self._check_query_service(shard_0_ny_rdonly, True, False)
    self._check_query_service(shard_1_ny_rdonly, True, False)
    self._check_query_service(shard_1_rdonly1, False, True)
    self._check_query_service(shard_1_rdonly2, False, True)

    # now serve rdonly from the split shards, everywhere
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'rdonly'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n' +
                             'Partitions(rdonly): -80 80-c0 c0-\n' +
                             'Partitions(replica): -80 80-\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)
    utils.check_srv_keyspace('test_ny', 'test_keyspace',
                             'Partitions(rdonly): -80 80-c0 c0-\n' +
                             'TabletTypes: rdonly',
                             keyspace_id_type=keyspace_id_type)
    self._check_query_service(shard_0_ny_rdonly, True, False)
    self._check_query_service(shard_1_ny_rdonly, False, True)
    self._check_query_service(shard_1_rdonly1, False, True)
    self._check_query_service(shard_1_rdonly2, False, True)

    # then serve replica from the split shards
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'replica'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n' +
                             'Partitions(rdonly): -80 80-c0 c0-\n' +
                             'Partitions(replica): -80 80-c0 c0-\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)
    self._check_query_service(shard_1_slave2, False, True)

    # move replica back and forth
    utils.run_vtctl(['MigrateServedTypes', '-reverse', 'test_keyspace/80-', 'replica'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n' +
                             'Partitions(rdonly): -80 80-c0 c0-\n' +
                             'Partitions(replica): -80 80-\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)
    self._check_query_service(shard_1_slave2, True, False)
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'replica'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n' +
                             'Partitions(rdonly): -80 80-c0 c0-\n' +
                             'Partitions(replica): -80 80-c0 c0-\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)
    self._check_query_service(shard_1_slave2, False, True)

    # reparent shard_2 to shard_2_replica1, then insert more data and
    # see it flow through still
    utils.run_vtctl(['ReparentShard', 'test_keyspace/80-c0',
                    shard_2_replica1.tablet_alias])
    logging.debug("Inserting lots of data on source shard after reparenting")
    self._insert_lots(3000, base=2000)
    logging.debug("Checking 80 percent of data was sent fairly quickly")
    self._check_lots_timeout(3000, 80, 10, base=2000)

    # use the vtworker checker to compare the data again
    logging.debug("Running vtworker SplitDiff")
    utils.run_vtworker(['-cell', 'test_nj', 'SplitDiff', 'test_keyspace/c0-'],
                       auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly1.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly2.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_3_rdonly1.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_3_rdonly2.tablet_alias, 'rdonly'],
                    auto_log=True)

    # going to migrate the master now, check the delays
    monitor_thread_1.done = True
    monitor_thread_2.done = True
    insert_thread_1.done = True
    insert_thread_2.done = True
    logging.debug("DELAY 1: %s max_lag=%u avg_lag=%u",
                  monitor_thread_1.object_name,
                  monitor_thread_1.max_lag,
                  monitor_thread_1.lag_sum / monitor_thread_1.sample_count)
    logging.debug("DELAY 2: %s max_lag=%u avg_lag=%u",
                  monitor_thread_2.object_name,
                  monitor_thread_2.max_lag,
                  monitor_thread_2.lag_sum / monitor_thread_2.sample_count)

    # mock with the SourceShard records to test 'vtctl SourceShardDelete'
    # and 'vtctl SourceShardAdd'
    utils.run_vtctl(['SourceShardDelete', 'test_keyspace/c0-', '0'],
                    auto_log=True)
    utils.run_vtctl(['SourceShardAdd', '--key_range=80-',
                     'test_keyspace/c0-', '0', 'test_keyspace/80-'],
                    auto_log=True)

    # then serve master from the split shards, make sure the source master's
    # query service is now turned off
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'master'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-c0 c0-\n' +
                             'Partitions(rdonly): -80 80-c0 c0-\n' +
                             'Partitions(replica): -80 80-c0 c0-\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)
    self._check_query_service(shard_1_master, False, True)

    # check the binlog players are gone now
    shard_2_master.wait_for_binlog_player_count(0)
    shard_3_master.wait_for_binlog_player_count(0)

    # get status for a destination master tablet, make sure it's good
    shard_2_master_status = shard_2_master.get_status()
    self.assertIn('No binlog player is running', shard_2_master_status)
    self.assertIn('</html>', shard_2_master_status)

    # scrap the original tablets in the original shard
    for t in [shard_1_master, shard_1_slave1, shard_1_slave2, shard_1_ny_rdonly,
              shard_1_rdonly1, shard_1_rdonly2]:
      utils.run_vtctl(['ScrapTablet', t.tablet_alias], auto_log=True)
    tablet.kill_tablets([shard_1_master, shard_1_slave1, shard_1_slave2,
                         shard_1_ny_rdonly, shard_1_rdonly1, shard_1_rdonly2])
    for t in [shard_1_master, shard_1_slave1, shard_1_slave2, shard_1_ny_rdonly,
              shard_1_rdonly1, shard_1_rdonly2]:
      utils.run_vtctl(['DeleteTablet', t.tablet_alias], auto_log=True)

    # rebuild the serving graph, all mentions of the old shards shoud be gone
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # test RemoveShardCell
    utils.run_vtctl(['RemoveShardCell', 'test_keyspace/-80', 'test_nj'], auto_log=True, expect_fail=True)
    utils.run_vtctl(['RemoveShardCell', 'test_keyspace/80-', 'test_nj'], auto_log=True)
    utils.run_vtctl(['RemoveShardCell', 'test_keyspace/80-', 'test_ny'], auto_log=True)
    shard = utils.run_vtctl_json(['GetShard', 'test_keyspace/80-'])
    if shard['Cells']:
      self.fail("Non-empty Cells record for shard: %s" % str(shard))

    # delete the original shard
    utils.run_vtctl(['DeleteShard', 'test_keyspace/80-'], auto_log=True)

    # kill everything
    tablet.kill_tablets([shard_0_master, shard_0_replica, shard_0_ny_rdonly,
                         shard_2_master, shard_2_replica1, shard_2_replica2,
                         shard_3_master, shard_3_replica, shard_3_rdonly1,
                         shard_3_rdonly2])

if __name__ == '__main__':
  utils.main()
