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

"""This test covers the workflow for a sharding merge.

We start with 3 shards: -40, 40-80, and 80-. We then merge -40 and 40-80
into -80.

Note this test is just testing the full workflow, not corner cases or error
cases. These are mostly done by the other resharding tests.
"""

import logging
import unittest

from vtdb import keyrange_constants

import base_sharding
import environment
import tablet
import utils


# initial shards
# shard -40
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_rdonly = tablet.Tablet()
# shard 40-80
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()
shard_1_rdonly = tablet.Tablet()
# shard 80-
shard_2_master = tablet.Tablet()
shard_2_replica = tablet.Tablet()
shard_2_rdonly = tablet.Tablet()

# merged shard -80
shard_dest_master = tablet.Tablet()
shard_dest_replica = tablet.Tablet()
shard_dest_rdonly = tablet.Tablet()

all_tablets = [shard_0_master, shard_0_replica, shard_0_rdonly,
               shard_1_master, shard_1_replica, shard_1_rdonly,
               shard_2_master, shard_2_replica, shard_2_rdonly,
               shard_dest_master, shard_dest_replica, shard_dest_rdonly]


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


class TestMergeSharding(unittest.TestCase, base_sharding.BaseShardingTest):

  # create_schema will create the same schema on the keyspace
  # then insert some values
  def _create_schema(self):
    if base_sharding.keyspace_id_type == keyrange_constants.KIT_BYTES:
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

  def _insert_startup_values(self):
    # row covered by shard -40 (should be merged).
    self._insert_value(shard_0_master, 'resharding1', 1, 'msg1',
                       0x1000000000000000)
    # row covered by shard 40-80 (should be merged).
    self._insert_value(shard_1_master, 'resharding1', 2, 'msg2',
                       0x5000000000000000)
    # row covered by shard 80- (must not be merged).
    self._insert_value(shard_2_master, 'resharding1', 3, 'msg3',
                       0xD000000000000000)

  def _check_startup_values(self):
    # check first two values are in the right shard
    self._check_value(shard_dest_master, 'resharding1', 1, 'msg1',
                      0x1000000000000000)
    self._check_value(shard_dest_replica, 'resharding1', 1, 'msg1',
                      0x1000000000000000)
    self._check_value(shard_dest_rdonly, 'resharding1', 1, 'msg1',
                      0x1000000000000000)

    self._check_value(shard_dest_master, 'resharding1', 2, 'msg2',
                      0x5000000000000000)
    self._check_value(shard_dest_replica, 'resharding1', 2, 'msg2',
                      0x5000000000000000)
    self._check_value(shard_dest_rdonly, 'resharding1', 2, 'msg2',
                      0x5000000000000000)

  def _insert_lots(self, count, base=0):
    if count > 10000:
      self.assertFail('bad count passed in, only support up to 10000')
    for i in xrange(count):
      self._insert_value(shard_0_master, 'resharding1', 1000000 + base + i,
                         'msg-range0-%d' % i, 0x2000000000000000 + base + i)
      self._insert_value(shard_1_master, 'resharding1', 1010000 + base + i,
                         'msg-range1-%d' % i, 0x6000000000000000 + base + i)

  # _check_lots returns how many of the values we have, in percents.
  def _check_lots(self, count, base=0):
    found = 0
    for i in xrange(count):
      if self._is_value_present_and_correct(shard_dest_replica, 'resharding1',
                                            1000000 + base + i,
                                            'msg-range0-%d' % i,
                                            0x2000000000000000 + base + i):
        found += 1
      if self._is_value_present_and_correct(shard_dest_replica, 'resharding1',
                                            1010000 + base + i,
                                            'msg-range1-%d' % i,
                                            0x6000000000000000 + base + i):
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

  def test_merge_sharding(self):
    utils.run_vtctl(['CreateKeyspace',
                     '--sharding_column_name', 'custom_ksid_col',
                     '--sharding_column_type', base_sharding.keyspace_id_type,
                     'test_keyspace'])

    shard_0_master.init_tablet('replica', 'test_keyspace', '-40')
    shard_0_replica.init_tablet('replica', 'test_keyspace', '-40')
    shard_0_rdonly.init_tablet('rdonly', 'test_keyspace', '-40')
    shard_1_master.init_tablet('replica', 'test_keyspace', '40-80')
    shard_1_replica.init_tablet('replica', 'test_keyspace', '40-80')
    shard_1_rdonly.init_tablet('rdonly', 'test_keyspace', '40-80')
    shard_2_master.init_tablet('replica', 'test_keyspace', '80-')
    shard_2_replica.init_tablet('replica', 'test_keyspace', '80-')
    shard_2_rdonly.init_tablet('rdonly', 'test_keyspace', '80-')

    # rebuild and check SrvKeyspace
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    ks = utils.run_vtctl_json(['GetSrvKeyspace', 'test_nj', 'test_keyspace'])
    self.assertEqual(ks['sharding_column_name'], 'custom_ksid_col')

    # create databases so vttablet can start behaving normally
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly,
              shard_1_master, shard_1_replica, shard_1_rdonly,
              shard_2_master, shard_2_replica, shard_2_rdonly]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None,
                       binlog_use_v3_resharding_mode=False)

    # won't be serving, no replication state
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly,
              shard_1_master, shard_1_replica, shard_1_rdonly,
              shard_2_master, shard_2_replica, shard_2_rdonly]:
      t.wait_for_vttablet_state('NOT_SERVING')

    # reparent to make the tablets work
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/-40',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/40-80',
                     shard_1_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/80-',
                     shard_2_master.tablet_alias], auto_log=True)

    # create the tables
    self._create_schema()
    self._insert_startup_values()

    # run a health check on source replicas so they respond to discovery
    # (for binlog players) and on the source rdonlys (for workers)
    for t in [shard_0_replica, shard_1_replica]:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias])
    for t in [shard_0_rdonly, shard_1_rdonly]:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias])

    # create the merge shards
    shard_dest_master.init_tablet('replica', 'test_keyspace', '-80')
    shard_dest_replica.init_tablet('replica', 'test_keyspace', '-80')
    shard_dest_rdonly.init_tablet('rdonly', 'test_keyspace', '-80')

    # start vttablet on the destination shard (no db created,
    # so they're all not serving)
    for t in [shard_dest_master, shard_dest_replica, shard_dest_rdonly]:
      t.start_vttablet(wait_for_state=None,
                       binlog_use_v3_resharding_mode=False)
    for t in [shard_dest_master, shard_dest_replica, shard_dest_rdonly]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/-80',
                     shard_dest_master.tablet_alias], auto_log=True)

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'],
                    auto_log=True)
    utils.check_srv_keyspace(
        'test_nj', 'test_keyspace',
        'Partitions(master): -40 40-80 80-\n'
        'Partitions(rdonly): -40 40-80 80-\n'
        'Partitions(replica): -40 40-80 80-\n',
        keyspace_id_type=base_sharding.keyspace_id_type,
        sharding_column_name='custom_ksid_col')

    # copy the schema
    utils.run_vtctl(['CopySchemaShard', shard_0_rdonly.tablet_alias,
                     'test_keyspace/-80'], auto_log=True)

    # copy the data (will also start filtered replication), reset source
    # Run vtworker as daemon for the following SplitClone commands.
    worker_proc, worker_port, worker_rpc_port = utils.run_vtworker_bg(
        ['--cell', 'test_nj', '--command_display_interval', '10ms',
          '--use_v3_resharding_mode=false'],
        auto_log=True)

    # Initial clone (online).
    workerclient_proc = utils.run_vtworker_client_bg(
        ['SplitClone',
         '--offline=false',
         '--chunk_count', '10',
         '--min_rows_per_chunk', '1',
         '--min_healthy_rdonly_tablets', '1',
         'test_keyspace/-80'],
        worker_rpc_port)
    utils.wait_procs([workerclient_proc])
    self.verify_reconciliation_counters(worker_port, 'Online', 'resharding1',
                                        2, 0, 0, 0)

    # Reset vtworker such that we can run the next command.
    workerclient_proc = utils.run_vtworker_client_bg(['Reset'], worker_rpc_port)
    utils.wait_procs([workerclient_proc])

    # Modify the destination shard. SplitClone will revert the changes.
    # Delete row 1 (provokes an insert).
    shard_dest_master.mquery('vt_test_keyspace',
                             'delete from resharding1 where id=1', write=True)
    # Update row 2 (provokes an update).
    shard_dest_master.mquery(
        'vt_test_keyspace', "update resharding1 set msg='msg-not-2' where id=2",
        write=True)
    # Insert row 0 (provokes a delete).
    self._insert_value(shard_dest_master, 'resharding1', 0, 'msg0',
                       0x5000000000000000)

    workerclient_proc = utils.run_vtworker_client_bg(
        ['SplitClone',
         '--chunk_count', '10',
         '--min_rows_per_chunk', '1',
         '--min_healthy_rdonly_tablets', '1',
         'test_keyspace/-80'],
        worker_rpc_port)
    utils.wait_procs([workerclient_proc])
    # Change tablets, which were taken offline, back to rdonly.
    utils.run_vtctl(['ChangeSlaveType', shard_0_rdonly.tablet_alias,
                     'rdonly'], auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly.tablet_alias,
                     'rdonly'], auto_log=True)
    self.verify_reconciliation_counters(worker_port, 'Online', 'resharding1',
                                        1, 1, 1, 0)
    self.verify_reconciliation_counters(worker_port, 'Offline', 'resharding1',
                                        0, 0, 0, 2)
    # Terminate worker daemon because it is no longer needed.
    utils.kill_sub_process(worker_proc, soft=True)

    # check the startup values are in the right place
    self._check_startup_values()

    # check the schema too
    utils.run_vtctl(['ValidateSchemaKeyspace', 'test_keyspace'], auto_log=True)

    # check binlog player variables
    self.check_destination_master(shard_dest_master,
                                  ['test_keyspace/-40', 'test_keyspace/40-80'])

    # check that binlog server exported the stats vars
    self.check_binlog_server_vars(shard_0_replica, horizontal=True)
    self.check_binlog_server_vars(shard_1_replica, horizontal=True)

    # testing filtered replication: insert a bunch of data on shard 0 and 1,
    # check we get most of it after a few seconds, wait for binlog server
    # timeout, check we get all of it.
    logging.debug('Inserting lots of data on source shards')
    self._insert_lots(1000)
    logging.debug('Checking 80 percent of data is sent quickly')
    v = self._check_lots_timeout(1000, 80, 10)
    if v != 100:
      # small optimization: only do this check if we don't have all the data
      # already anyway.
      logging.debug('Checking all data goes through eventually')
      self._check_lots_timeout(1000, 100, 30)
    self.check_binlog_player_vars(shard_dest_master,
                                  ['test_keyspace/-40', 'test_keyspace/40-80'],
                                  seconds_behind_master_max=30)
    self.check_binlog_server_vars(shard_0_replica, horizontal=True,
                                  min_statements=1000, min_transactions=1000)
    self.check_binlog_server_vars(shard_1_replica, horizontal=True,
                                  min_statements=1000, min_transactions=1000)

    # use vtworker to compare the data (after health-checking the destination
    # rdonly tablets so discovery works)
    utils.run_vtctl(['RunHealthCheck', shard_dest_rdonly.tablet_alias])
    logging.debug('Running vtworker SplitDiff on first half')
    utils.run_vtworker(['-cell', 'test_nj',
                        '--use_v3_resharding_mode=false',
                        'SplitDiff',
                        '--exclude_tables', 'unrelated',
                        '--min_healthy_rdonly_tablets', '1',
                        '--source_uid', '0',
                        'test_keyspace/-80'],
                       auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_0_rdonly.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_dest_rdonly.tablet_alias,
                     'rdonly'], auto_log=True)
    logging.debug('Running vtworker SplitDiff on second half')
    utils.run_vtworker(['-cell', 'test_nj',
                        '--use_v3_resharding_mode=false',
                        'SplitDiff',
                        '--exclude_tables', 'unrelated',
                        '--min_healthy_rdonly_tablets', '1',
                        '--source_uid', '1',
                        'test_keyspace/-80'],
                       auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_dest_rdonly.tablet_alias,
                     'rdonly'], auto_log=True)

    # get status for the destination master tablet, make sure we have it all
    self.check_running_binlog_player(shard_dest_master, 3000, 1000)

    # check destination master query service is not running
    utils.check_tablet_query_service(self, shard_dest_master, False, False)
    stream_health = utils.run_vtctl_json(['VtTabletStreamHealth',
                                          '-count', '1',
                                          shard_dest_master.tablet_alias])
    logging.debug('Got health: %s', str(stream_health))
    self.assertIn('realtime_stats', stream_health)
    self.assertNotIn('serving', stream_health)

    # check the destination master 3 is healthy, even though its query
    # service is not running (if not healthy this would exception out)
    shard_dest_master.get_healthz()

    # now serve rdonly from the split shards
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/-80', 'rdonly'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -40 40-80 80-\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -40 40-80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # now serve replica from the split shards
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/-80', 'replica'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -40 40-80 80-\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # now serve master from the split shards
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/-80', 'master'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_tablet_query_service(self, shard_0_master, False, True)
    utils.check_tablet_query_service(self, shard_1_master, False, True)

    # check the binlog players are gone now
    self.check_no_binlog_player(shard_dest_master)

    # kill the original tablets in the original shards
    tablet.kill_tablets([shard_0_master, shard_0_replica, shard_0_rdonly,
                         shard_1_master, shard_1_replica, shard_1_rdonly])
    for t in [shard_0_replica, shard_0_rdonly,
              shard_1_replica, shard_1_rdonly]:
      utils.run_vtctl(['DeleteTablet', t.tablet_alias], auto_log=True)
    for t in [shard_0_master, shard_1_master]:
      utils.run_vtctl(['DeleteTablet', '-allow_master', t.tablet_alias],
                      auto_log=True)

    # delete the original shards
    utils.run_vtctl(['DeleteShard', 'test_keyspace/-40'], auto_log=True)
    utils.run_vtctl(['DeleteShard', 'test_keyspace/40-80'], auto_log=True)

    # rebuild the serving graph, all mentions of the old shards shoud be gone
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # kill everything else
    tablet.kill_tablets([shard_2_master, shard_2_replica, shard_2_rdonly,
                         shard_dest_master, shard_dest_replica,
                         shard_dest_rdonly])


if __name__ == '__main__':
  utils.main()
