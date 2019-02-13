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

# TODO(mberlin): Remove this file when SplitClone supports merge-sorting
# primary key columns based on the MySQL collation.

"""This test covers the vtworker LegacySplitClone command.

The vtworker LegacySplitClone should only be used when it is necessary to
reshard a table that has textual primary key columns (e.g. VARCHAR).
This is the case for the "timestamps" table in this end-to-end test.

The reason why only LegacySplitClone supports this use case is because the new
resharding clone code (as of https://github.com/vitessio/vitess/pull/1796)
requires to sort rows by their primary key. Whereas LegacySplitClone does a
simple copy and always assumes that the tables on the destination are empty,
the SplitClone command can diff the source and destination tables. In case of
a horizontal resharding this requires merge-sorting multiple destination shards.
Since we currently do not support sorting VARCHAR primary key columns in
SplitClone (due to missing support for MySQL collations), you'll have to resort
to LegacySplitClone only for this use case.

Note that this file was copied from the original resharding.py file.

We start with shards -80 and 80-. We then split 80- into 80-c0 and c0-.

This test is the main resharding test. It not only tests the regular resharding
workflow for an horizontal split, but also a lot of error cases and side
effects, like:
- migrating the traffic one cell at a time.
- migrating rdonly traffic back and forth.
- making sure we can't migrate the master until replica and rdonly are migrated.
- has a background thread to insert data during migration.
- tests a destination shard master failover while replication is running.
- tests a filtered replication source replacement while filtered replication
  is running.
- tests 'vtctl SourceShardAdd' and 'vtctl SourceShardDelete'.
- makes sure the key range rules are properly enforced on masters.
"""

import struct

import logging
import unittest

import base_sharding
import environment
import tablet
import utils

from vtproto import topodata_pb2
from vtdb import keyrange_constants

keyspace_id_type = keyrange_constants.KIT_UINT64
pack_keyspace_id = struct.Struct('!Q').pack

# initial shards
# range '' - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_ny_rdonly = tablet.Tablet(cell='ny')
# range 80 - ''
shard_1_master = tablet.Tablet()
shard_1_slave1 = tablet.Tablet()
shard_1_slave2 = tablet.Tablet()
shard_1_ny_rdonly = tablet.Tablet(cell='ny')
shard_1_rdonly1 = tablet.Tablet()

# split shards
# range 80 - c0
shard_2_master = tablet.Tablet()
shard_2_replica1 = tablet.Tablet()
shard_2_replica2 = tablet.Tablet()
# range c0 - ''
shard_3_master = tablet.Tablet()
shard_3_replica = tablet.Tablet()
shard_3_rdonly1 = tablet.Tablet()

all_tablets = [shard_0_master, shard_0_replica, shard_0_ny_rdonly,
               shard_1_master, shard_1_slave1, shard_1_slave2,
               shard_1_ny_rdonly, shard_1_rdonly1,
               shard_2_master, shard_2_replica1, shard_2_replica2,
               shard_3_master, shard_3_replica, shard_3_rdonly1]


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


class TestResharding(unittest.TestCase, base_sharding.BaseShardingTest):

  # create_schema will create the same schema on the keyspace
  # then insert some values
  def _create_schema(self):
    if keyspace_id_type == keyrange_constants.KIT_BYTES:
      t = 'varbinary(64)'
    else:
      t = 'bigint(20) unsigned'
    # Note that the primary key columns are not defined first on purpose to test
    # that a reordered column list is correctly used everywhere in vtworker.
    create_table_template = '''create table %s(
msg varchar(64),
custom_ksid_col ''' + t + ''' not null,
id bigint not null,
parent_id bigint not null,
primary key (parent_id, id),
index by_msg (msg)
) Engine=InnoDB'''
    create_view_template = (
        'create view %s'
        '(id, msg, custom_ksid_col) as select id, msg, custom_ksid_col '
        'from %s')
    create_timestamp_table = '''create table timestamps(
name varchar(64),
time_milli bigint(20) unsigned not null,
custom_ksid_col ''' + t + ''' not null,
primary key (name)
) Engine=InnoDB'''
    create_unrelated_table = '''create table unrelated(
name varchar(64),
primary key (name)
) Engine=InnoDB'''

    utils.run_vtctl(['ApplySchema',
                     '-sql=' + create_table_template % ('resharding1'),
                     'test_keyspace'],
                    auto_log=True)
    utils.run_vtctl(['ApplySchema',
                     '-sql=' + create_table_template % ('resharding2'),
                     'test_keyspace'],
                    auto_log=True)
    utils.run_vtctl(['ApplySchema',
                     '-sql=' + create_view_template % ('view1', 'resharding1'),
                     'test_keyspace'],
                    auto_log=True)
    utils.run_vtctl(['ApplySchema',
                     '-sql=' + create_timestamp_table,
                     'test_keyspace'],
                    auto_log=True)
    utils.run_vtctl(['ApplySchema',
                     '-sql=' + create_unrelated_table,
                     'test_keyspace'],
                    auto_log=True)

  def _insert_startup_values(self):
    self._insert_value(shard_0_master, 'resharding1', 1, 'msg1',
                       0x1000000000000000)
    self._insert_value(shard_1_master, 'resharding1', 2, 'msg2',
                       0x9000000000000000)
    self._insert_value(shard_1_master, 'resharding1', 3, 'msg3',
                       0xD000000000000000)

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

  def _insert_lots(self, count, base=0):
    for i in xrange(count):
      self._insert_value(shard_1_master, 'resharding1', 10000 + base + i,
                         'msg-range1-%d' % i, 0xA000000000000000 + base + i)
      self._insert_value(shard_1_master, 'resharding1', 20000 + base + i,
                         'msg-range2-%d' % i, 0xE000000000000000 + base + i)

  # _check_lots returns how many of the values we have, in percents.
  def _check_lots(self, count, base=0):
    found = 0
    for i in xrange(count):
      if self._is_value_present_and_correct(shard_2_replica2, 'resharding1',
                                            10000 + base + i, 'msg-range1-%d' %
                                            i, 0xA000000000000000 + base + i):
        found += 1
      if self._is_value_present_and_correct(shard_3_replica, 'resharding1',
                                            20000 + base + i, 'msg-range2-%d' %
                                            i, 0xE000000000000000 + base + i):
        found += 1
    percent = found * 100 / count / 2
    logging.debug('I have %d%% of the data', percent)
    return percent

  def _check_lots_timeout(self, count, threshold, timeout, base=0):
    while True:
      value = self._check_lots(count, base=base)
      if value >= threshold:
        return value
      timeout = utils.wait_step('waiting for %d%% of the data' % threshold,
                                timeout, sleep_time=1)

  # _check_lots_not_present makes sure no data is in the wrong shard
  def _check_lots_not_present(self, count, base=0):
    for i in xrange(count):
      self._check_value(shard_3_replica, 'resharding1', 10000 + base + i,
                        'msg-range1-%d' % i, 0xA000000000000000 + base + i,
                        should_be_here=False)
      self._check_value(shard_2_replica2, 'resharding1', 20000 + base + i,
                        'msg-range2-%d' % i, 0xE000000000000000 + base + i,
                        should_be_here=False)

  def test_resharding(self):
    utils.run_vtctl(['CreateKeyspace',
                     '--sharding_column_name', 'bad_column',
                     '--sharding_column_type', 'bytes',
                     'test_keyspace'])
    utils.run_vtctl(['SetKeyspaceShardingInfo', 'test_keyspace',
                     'custom_ksid_col', 'uint64'], expect_fail=True)
    utils.run_vtctl(['SetKeyspaceShardingInfo', '-force',
                     'test_keyspace', 'custom_ksid_col', keyspace_id_type])

    shard_0_master.init_tablet('replica', 'test_keyspace', '-80')
    shard_0_replica.init_tablet('replica', 'test_keyspace', '-80')
    shard_0_ny_rdonly.init_tablet('rdonly', 'test_keyspace', '-80')
    shard_1_master.init_tablet('replica', 'test_keyspace', '80-')
    shard_1_slave1.init_tablet('replica', 'test_keyspace', '80-')
    shard_1_slave2.init_tablet('replica', 'test_keyspace', '80-')
    shard_1_ny_rdonly.init_tablet('rdonly', 'test_keyspace', '80-')
    shard_1_rdonly1.init_tablet('rdonly', 'test_keyspace', '80-')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    ks = utils.run_vtctl_json(['GetSrvKeyspace', 'test_nj', 'test_keyspace'])
    self.assertEqual(ks['sharding_column_name'], 'custom_ksid_col')

    # we set full_mycnf_args to True as a test in the KIT_BYTES case
    full_mycnf_args = keyspace_id_type == keyrange_constants.KIT_BYTES

    # create databases so vttablet can start behaving somewhat normally
    for t in [shard_0_master, shard_0_replica, shard_0_ny_rdonly,
              shard_1_master, shard_1_slave1, shard_1_slave2, shard_1_ny_rdonly,
              shard_1_rdonly1]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None, full_mycnf_args=full_mycnf_args,
                       binlog_use_v3_resharding_mode=False)

    # wait for the tablets (replication is not setup, they won't be healthy)
    for t in [shard_0_master, shard_0_replica, shard_0_ny_rdonly,
              shard_1_master, shard_1_slave1, shard_1_slave2, shard_1_ny_rdonly,
              shard_1_rdonly1]:
      t.wait_for_vttablet_state('NOT_SERVING')

    # reparent to make the tablets work
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)

    # check the shards
    shards = utils.run_vtctl_json(['FindAllShardsInKeyspace', 'test_keyspace'])
    self.assertIn('-80', shards, 'unexpected shards: %s' % str(shards))
    self.assertIn('80-', shards, 'unexpected shards: %s' % str(shards))
    self.assertEqual(len(shards), 2, 'unexpected shards: %s' % str(shards))

    # create the tables
    self._create_schema()
    self._insert_startup_values()

    # run a health check on source replicas so they respond to discovery
    # (for binlog players) and on the source rdonlys (for workers)
    for t in [shard_0_replica, shard_1_slave1]:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias])
    for t in [shard_0_ny_rdonly, shard_1_ny_rdonly, shard_1_rdonly1]:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias])

    # create the split shards
    shard_2_master.init_tablet('replica', 'test_keyspace', '80-c0')
    shard_2_replica1.init_tablet('replica', 'test_keyspace', '80-c0')
    shard_2_replica2.init_tablet('replica', 'test_keyspace', '80-c0')
    shard_3_master.init_tablet('replica', 'test_keyspace', 'c0-')
    shard_3_replica.init_tablet('replica', 'test_keyspace', 'c0-')
    shard_3_rdonly1.init_tablet('rdonly', 'test_keyspace', 'c0-')

    # start vttablet on the split shards (no db created,
    # so they're all not serving)
    shard_2_master.start_vttablet(wait_for_state=None,
                                  binlog_use_v3_resharding_mode=False)
    shard_3_master.start_vttablet(wait_for_state=None,
                                  binlog_use_v3_resharding_mode=False)
    for t in [shard_2_replica1, shard_2_replica2,
              shard_3_replica, shard_3_rdonly1]:
      t.start_vttablet(wait_for_state=None,
                       binlog_use_v3_resharding_mode=False)
    for t in [shard_2_master, shard_2_replica1, shard_2_replica2,
              shard_3_master, shard_3_replica, shard_3_rdonly1]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/80-c0',
                     shard_2_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/c0-',
                     shard_3_master.tablet_alias], auto_log=True)

    # check the shards
    shards = utils.run_vtctl_json(['FindAllShardsInKeyspace', 'test_keyspace'])
    for s in ['-80', '80-', '80-c0', 'c0-']:
      self.assertIn(s, shards, 'unexpected shards: %s' % str(shards))
    self.assertEqual(len(shards), 4, 'unexpected shards: %s' % str(shards))

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'],
                    auto_log=True)
    utils.check_srv_keyspace(
        'test_nj', 'test_keyspace',
        'Partitions(master): -80 80-\n'
        'Partitions(rdonly): -80 80-\n'
        'Partitions(replica): -80 80-\n',
        keyspace_id_type=keyspace_id_type,
        sharding_column_name='custom_ksid_col')

    # disable shard_1_slave2, so we're sure filtered replication will go
    # from shard_1_slave1
    utils.run_vtctl(['ChangeSlaveType', shard_1_slave2.tablet_alias, 'spare'])
    shard_1_slave2.wait_for_vttablet_state('NOT_SERVING')

    # we need to create the schema, and the worker will do data copying
    for keyspace_shard in ('test_keyspace/80-c0', 'test_keyspace/c0-'):
      utils.run_vtctl(['CopySchemaShard', '--exclude_tables', 'unrelated',
                       shard_1_rdonly1.tablet_alias, keyspace_shard],
                      auto_log=True)

    # --max_tps is only specified to enable the throttler and ensure that the
    # code is executed. But the intent here is not to throttle the test, hence
    # the rate limit is set very high.
    utils.run_vtworker(['--cell', 'test_nj',
                        '--command_display_interval', '10ms',
                        '--use_v3_resharding_mode=false',
                        'LegacySplitClone',
                        '--exclude_tables', 'unrelated',
                        '--min_healthy_rdonly_tablets', '1',
                        '--max_tps', '9999',
                        'test_keyspace/80-'],
                       auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly1.tablet_alias,
                     'rdonly'], auto_log=True)

    # check the startup values are in the right place
    self._check_startup_values()

    # check the schema too
    utils.run_vtctl(['ValidateSchemaKeyspace', '--exclude_tables=unrelated',
                     'test_keyspace'], auto_log=True)

    # check the binlog players are running and exporting vars
    self.check_destination_master(shard_2_master, ['test_keyspace/80-'])
    self.check_destination_master(shard_3_master, ['test_keyspace/80-'])

    # check that binlog server exported the stats vars
    self.check_binlog_server_vars(shard_1_slave1, horizontal=True)

    # Check that the throttler was enabled.
    # The stream id is hard-coded as 1, which is the first id generated
    # through auto-inc.
    self.check_throttler_service(shard_2_master.rpc_endpoint(),
                                 ['BinlogPlayer/1'], 9999)
    self.check_throttler_service(shard_3_master.rpc_endpoint(),
                                 ['BinlogPlayer/1'], 9999)

    # testing filtered replication: insert a bunch of data on shard 1,
    # check we get most of it after a few seconds, wait for binlog server
    # timeout, check we get all of it.
    logging.debug('Inserting lots of data on source shard')
    self._insert_lots(1000)
    logging.debug('Checking 80 percent of data is sent quickly')
    v = self._check_lots_timeout(1000, 80, 5)
    if v != 100:
      # small optimization: only do this check if we don't have all the data
      # already anyway.
      logging.debug('Checking all data goes through eventually')
      self._check_lots_timeout(1000, 100, 20)
    logging.debug('Checking no data was sent the wrong way')
    self._check_lots_not_present(1000)
    self.check_binlog_player_vars(shard_2_master, ['test_keyspace/80-'],
                                  seconds_behind_master_max=30)
    self.check_binlog_player_vars(shard_3_master, ['test_keyspace/80-'],
                                  seconds_behind_master_max=30)
    self.check_binlog_server_vars(shard_1_slave1, horizontal=True,
                                  min_statements=1000, min_transactions=1000)

    # use vtworker to compare the data (after health-checking the destination
    # rdonly tablets so discovery works)
    utils.run_vtctl(['RunHealthCheck', shard_3_rdonly1.tablet_alias])
    logging.debug('Running vtworker SplitDiff')
    utils.run_vtworker(['-cell', 'test_nj',
                        '--use_v3_resharding_mode=false',
                        'SplitDiff',
                        '--exclude_tables', 'unrelated',
                        '--min_healthy_rdonly_tablets', '1',
                        'test_keyspace/c0-'],
                       auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly1.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_3_rdonly1.tablet_alias, 'rdonly'],
                    auto_log=True)

    utils.pause('Good time to test vtworker for diffs')

    # get status for destination master tablets, make sure we have it all
    self.check_running_binlog_player(shard_2_master, 4000, 2000)
    self.check_running_binlog_player(shard_3_master, 4000, 2000)

    # tests a failover switching serving to a different replica
    utils.run_vtctl(['ChangeSlaveType', shard_1_slave2.tablet_alias, 'replica'])
    utils.run_vtctl(['ChangeSlaveType', shard_1_slave1.tablet_alias, 'spare'])
    shard_1_slave2.wait_for_vttablet_state('SERVING')
    shard_1_slave1.wait_for_vttablet_state('NOT_SERVING')
    utils.run_vtctl(['RunHealthCheck', shard_1_slave2.tablet_alias])

    # test data goes through again
    logging.debug('Inserting lots of data on source shard')
    self._insert_lots(1000, base=1000)
    logging.debug('Checking 80 percent of data was sent quickly')
    self._check_lots_timeout(1000, 80, 5, base=1000)
    self.check_binlog_server_vars(shard_1_slave2, horizontal=True,
                                  min_statements=800, min_transactions=800)

    # check we can't migrate the master just yet
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'master'],
                    expect_fail=True)

    # check query service is off on master 2 and master 3, as filtered
    # replication is enabled. Even health check that is enabled on
    # master 3 should not interfere (we run it to be sure).
    utils.run_vtctl(['RunHealthCheck', shard_3_master.tablet_alias],
                    auto_log=True)
    for master in [shard_2_master, shard_3_master]:
      utils.check_tablet_query_service(self, master, False, False)
      stream_health = utils.run_vtctl_json(['VtTabletStreamHealth',
                                            '-count', '1',
                                            master.tablet_alias])
      logging.debug('Got health: %s', str(stream_health))
      self.assertIn('realtime_stats', stream_health)
      self.assertNotIn('serving', stream_health)

    # check the destination master 3 is healthy, even though its query
    # service is not running (if not healthy this would exception out)
    shard_3_master.get_healthz()

    # now serve rdonly from the split shards, in test_nj only
    utils.run_vtctl(['MigrateServedTypes', '--cells=test_nj',
                     'test_keyspace/80-', 'rdonly'], auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_srv_keyspace('test_ny', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_tablet_query_service(self, shard_0_ny_rdonly, True, False)
    utils.check_tablet_query_service(self, shard_1_ny_rdonly, True, False)
    utils.check_tablet_query_service(self, shard_1_rdonly1, False, True)

    # now serve rdonly from the split shards, everywhere
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'rdonly'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_srv_keyspace('test_ny', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_tablet_query_service(self, shard_0_ny_rdonly, True, False)
    utils.check_tablet_query_service(self, shard_1_ny_rdonly, False, True)
    utils.check_tablet_query_service(self, shard_1_rdonly1, False, True)

    # then serve replica from the split shards
    destination_shards = ['80-c0', 'c0-']

    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'replica'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-c0 c0-\n',
                             keyspace_id_type=keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_tablet_query_service(self, shard_1_slave2, False, True)

    # move replica back and forth
    utils.run_vtctl(
        ['MigrateServedTypes', '-reverse', 'test_keyspace/80-', 'replica'],
        auto_log=True)
    # After a backwards migration, queryservice should be enabled on
    # source and disabled on destinations
    utils.check_tablet_query_service(self, shard_1_slave2, True, False)
    # Destination tablets would have query service disabled for other
    # reasons than the migration, so check the shard record instead of
    # the tablets directly.
    utils.check_shard_query_services(self, 'test_nj', 'test_keyspace', destination_shards,
                                     topodata_pb2.REPLICA, False)
    utils.check_shard_query_services(self, 'test_ny', 'test_keyspace', destination_shards,
                                     topodata_pb2.REPLICA, False)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'replica'],
                    auto_log=True)
    # After a forwards migration, queryservice should be disabled on
    # source and enabled on destinations
    utils.check_tablet_query_service(self, shard_1_slave2, False, True)
    # Destination tablets would have query service disabled for other
    # reasons than the migration, so check the shard record instead of
    # the tablets directly
    utils.check_shard_query_services(self, 'test_nj', 'test_keyspace', destination_shards,
                                     topodata_pb2.REPLICA, True)
    utils.check_shard_query_services(self, 'test_ny', 'test_keyspace', destination_shards,
                                     topodata_pb2.REPLICA, True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-c0 c0-\n',
                             keyspace_id_type=keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # use vtworker to compare the data again
    logging.debug('Running vtworker SplitDiff')
    utils.run_vtworker(['-cell', 'test_nj',
                        '--use_v3_resharding_mode=false',
                        'SplitDiff',
                        '--exclude_tables', 'unrelated',
                        '--min_healthy_rdonly_tablets', '1',
                        'test_keyspace/c0-'],
                       auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly1.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_3_rdonly1.tablet_alias, 'rdonly'],
                    auto_log=True)

    # mock with the SourceShard records to test 'vtctl SourceShardDelete'
    # and 'vtctl SourceShardAdd'
    utils.run_vtctl(['SourceShardDelete', 'test_keyspace/c0-', '1'],
                    auto_log=True)
    utils.run_vtctl(['SourceShardAdd', '--key_range=80-',
                     'test_keyspace/c0-', '1', 'test_keyspace/80-'],
                    auto_log=True)

    # then serve master from the split shards, make sure the source master's
    # query service is now turned off
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'master'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-c0 c0-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-c0 c0-\n',
                             keyspace_id_type=keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_tablet_query_service(self, shard_1_master, False, True)

    # check the binlog players are gone now
    self.check_no_binlog_player(shard_2_master)
    self.check_no_binlog_player(shard_3_master)

    # delete the original tablets in the original shard
    tablet.kill_tablets([shard_1_master, shard_1_slave1, shard_1_slave2,
                         shard_1_ny_rdonly, shard_1_rdonly1])
    for t in [shard_1_slave1, shard_1_slave2, shard_1_ny_rdonly,
              shard_1_rdonly1]:
      utils.run_vtctl(['DeleteTablet', t.tablet_alias], auto_log=True)
    utils.run_vtctl(['DeleteTablet', '-allow_master',
                     shard_1_master.tablet_alias], auto_log=True)

    # rebuild the serving graph, all mentions of the old shards shoud be gone
    utils.run_vtctl(
        ['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # test RemoveShardCell
    utils.run_vtctl(
        ['RemoveShardCell', 'test_keyspace/-80', 'test_nj'], auto_log=True,
        expect_fail=True)
    utils.run_vtctl(
        ['RemoveShardCell', 'test_keyspace/80-', 'test_nj'], auto_log=True)
    utils.run_vtctl(
        ['RemoveShardCell', 'test_keyspace/80-', 'test_ny'], auto_log=True)
    shard = utils.run_vtctl_json(['GetShard', 'test_keyspace/80-'])
    self.assertTrue('cells' not in shard or not shard['cells'])

    # delete the original shard
    utils.run_vtctl(['DeleteShard', 'test_keyspace/80-'], auto_log=True)

    # kill everything
    tablet.kill_tablets([shard_0_master, shard_0_replica, shard_0_ny_rdonly,
                         shard_2_master, shard_2_replica1, shard_2_replica2,
                         shard_3_master, shard_3_replica, shard_3_rdonly1])

if __name__ == '__main__':
  utils.main()
