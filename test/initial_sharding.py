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

"""This test simulates the first time a database has to be split.

- we start with a keyspace with a single shard and a single table
- we add and populate the sharding key
- we set the sharding key in the topology
- we clone into 2 instances
- we enable filtered replication
- we move all serving types
- we remove the source tablets
- we remove the original shard
"""

import logging
import unittest
from vtdb import keyrange_constants

import base_sharding
import environment
import tablet
import utils

# initial shard, covers everything
shard_master = tablet.Tablet()
shard_replica = tablet.Tablet()
shard_rdonly1 = tablet.Tablet()

# split shards
# range '' - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_rdonly1 = tablet.Tablet()
# range 80 - ''
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()
shard_1_rdonly1 = tablet.Tablet()

all_tablets = [shard_master, shard_replica, shard_rdonly1,
               shard_0_master, shard_0_replica, shard_0_rdonly1,
               shard_1_master, shard_1_replica, shard_1_rdonly1]


def setUpModule():
  try:
    environment.topo_server().setup()
    setup_procs = [t.init_mysql() for t in all_tablets]
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


class TestInitialSharding(unittest.TestCase, base_sharding.BaseShardingTest):

  # create_schema will create the same schema on the keyspace
  def _create_schema(self):
    # Note that the primary key columns are not defined first on purpose to test
    # that a reordered column list is correctly used everywhere in vtworker.
    create_table_template = '''create table %s(
msg varchar(64),
id bigint not null,
parent_id bigint not null,
primary key (parent_id, id),
index by_msg (msg)
) Engine=InnoDB'''

    utils.run_vtctl(['ApplySchema',
                     '-sql=' + create_table_template % ('resharding1'),
                     'test_keyspace'],
                    auto_log=True)

  def _add_sharding_key_to_schema(self):
    if base_sharding.keyspace_id_type == keyrange_constants.KIT_BYTES:
      t = 'varbinary(64)'
    else:
      t = 'bigint(20) unsigned'
    sql = 'alter table %s add custom_ksid_col ' + t
    utils.run_vtctl(['ApplySchema',
                     '-sql=' + sql % ('resharding1'),
                     'test_keyspace'],
                    auto_log=True)

  def _mark_sharding_key_not_null(self):
    if base_sharding.keyspace_id_type == keyrange_constants.KIT_BYTES:
      t = 'varbinary(64)'
    else:
      t = 'bigint(20) unsigned'
    sql = 'alter table %s modify custom_ksid_col ' + t + ' not null'
    utils.run_vtctl(['ApplySchema',
                     '-sql=' + sql % ('resharding1'),
                     'test_keyspace'],
                    auto_log=True)

  # _insert_startup_value inserts a value in the MySQL database before it
  # is sharded
  def _insert_startup_value(self, tablet_obj, table, mid, msg):
    tablet_obj.mquery('vt_test_keyspace', [
        'begin',
        'insert into %s(parent_id, id, msg) values(%d, %d, "%s")' %
        (table, base_sharding.fixed_parent_id, mid, msg),
        'commit'
        ], write=True)

  def _insert_startup_values(self):
    self._insert_startup_value(shard_master, 'resharding1', 1, 'msg1')
    self._insert_startup_value(shard_master, 'resharding1', 2, 'msg2')
    self._insert_startup_value(shard_master, 'resharding1', 3, 'msg3')

  def _backfill_keyspace_id(self, tablet_obj):
    tablet_obj.mquery('vt_test_keyspace', [
        'begin',
        'update resharding1 set custom_ksid_col=0x1000000000000000 where id=1',
        'update resharding1 set custom_ksid_col=0x9000000000000000 where id=2',
        'update resharding1 set custom_ksid_col=0xD000000000000000 where id=3',
        'commit'
        ], write=True)

  def _check_startup_values(self):
    # check first value is in the left shard
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly1]:
      self._check_value(t, 'resharding1', 1, 'msg1', 0x1000000000000000)
    for t in [shard_1_master, shard_1_replica, shard_1_rdonly1]:
      self._check_value(t, 'resharding1', 1, 'msg1',
                        0x1000000000000000, should_be_here=False)

    # check second value is in the right shard
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly1]:
      self._check_value(t, 'resharding1', 2, 'msg2', 0x9000000000000000,
                        should_be_here=False)
    for t in [shard_1_master, shard_1_replica, shard_1_rdonly1]:
      self._check_value(t, 'resharding1', 2, 'msg2', 0x9000000000000000)

    # check third value is in the right shard too
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly1]:
      self._check_value(t, 'resharding1', 3, 'msg3', 0xD000000000000000,
                        should_be_here=False)
    for t in [shard_1_master, shard_1_replica, shard_1_rdonly1]:
      self._check_value(t, 'resharding1', 3, 'msg3', 0xD000000000000000)

  def _insert_lots(self, count, base=0):
    for i in xrange(count):
      self._insert_value(shard_master, 'resharding1', 10000 + base + i,
                         'msg-range1-%d' % i, 0xA000000000000000 + base + i)
      self._insert_value(shard_master, 'resharding1', 20000 + base + i,
                         'msg-range2-%d' % i, 0xE000000000000000 + base + i)

  # _check_lots returns how many of the values we have, in percents.
  def _check_lots(self, count, base=0):
    found = 0
    for i in xrange(count):
      if self._is_value_present_and_correct(shard_1_replica, 'resharding1',
                                            10000 + base + i, 'msg-range1-%d' %
                                            i, 0xA000000000000000 + base + i):
        found += 1
      if self._is_value_present_and_correct(shard_1_replica, 'resharding1',
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
      timeout = utils.wait_step('enough data went through', timeout)

  # _check_lots_not_present makes sure no data is in the wrong shard
  def _check_lots_not_present(self, count, base=0):
    for i in xrange(count):
      self._check_value(shard_0_replica, 'resharding1', 10000 + base + i,
                        'msg-range1-%d' % i, 0xA000000000000000 + base + i,
                        should_be_here=False)
      self._check_value(shard_0_replica, 'resharding1', 20000 + base + i,
                        'msg-range2-%d' % i, 0xE000000000000000 + base + i,
                        should_be_here=False)

  def test_resharding(self):
    # create the keyspace with just one shard
    shard_master.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='0',
        tablet_index=0)
    shard_replica.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='0',
        tablet_index=1)
    shard_rdonly1.init_tablet(
        'rdonly',
        keyspace='test_keyspace',
        shard='0',
        tablet_index=2)

    for t in [shard_master, shard_replica, shard_rdonly1]:
      t.create_db('vt_test_keyspace')

    # replica is not started, InitShardMaster should timeout
    shard_master.start_vttablet(wait_for_state=None,
                                binlog_use_v3_resharding_mode=False)
    shard_rdonly1.start_vttablet(wait_for_state=None,
                                 binlog_use_v3_resharding_mode=False)

    for t in [shard_master, shard_rdonly1]:
      t.wait_for_vttablet_state('NOT_SERVING')

    # reparent to make the tablets work - expect fail
    # because replica tablet is not up
    _, stderr = utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     shard_master.tablet_alias], auto_log=True, expect_fail=True)

    self.assertIn('Tablet test_nj-0000062345 ResetReplication failed', stderr)
    # start replica
    shard_replica.start_vttablet(wait_for_state=None,
                                 binlog_use_v3_resharding_mode=False)

    shard_replica.wait_for_vttablet_state('NOT_SERVING')

    # reparent to make the tablets work
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     shard_master.tablet_alias], auto_log=True)

    utils.wait_for_tablet_type(shard_replica.tablet_alias, 'replica')
    utils.wait_for_tablet_type(shard_rdonly1.tablet_alias, 'rdonly')
    for t in [shard_master, shard_replica, shard_rdonly1]:
      t.wait_for_vttablet_state('SERVING')

    # create the tables and add startup values
    self._create_schema()
    self._insert_startup_values()

    # reload schema on all tablets so we can query them
    for t in [shard_master, shard_replica, shard_rdonly1]:
      utils.run_vtctl(['ReloadSchema', t.tablet_alias], auto_log=True)

    # We must start vtgate after tablets are up, or else wait until 1min refresh
    # (that is the tablet_refresh_interval parameter for discovery gateway)
    # we want cache_ttl at zero so we re-read the topology for every test query.

    utils.VtGate().start(cache_ttl='0', tablets=[
        shard_master, shard_replica, shard_rdonly1])
    utils.vtgate.wait_for_endpoints('test_keyspace.0.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.0.replica', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.0.rdonly', 1)

    # check the Map Reduce API works correctly, should use ExecuteShards,
    # as we're not sharded yet.
    # we have 3 values in the database, asking for 4 splits will get us
    # a single query.
    sql = 'select id, msg from resharding1'
    s = utils.vtgate.split_query(sql, 'test_keyspace', 4)
    self.assertEqual(len(s), 1)
    self.assertEqual(s[0]['shard_part']['shards'][0], '0')

    # change the schema, backfill keyspace_id, and change schema again
    self._add_sharding_key_to_schema()
    self._backfill_keyspace_id(shard_master)
    self._mark_sharding_key_not_null()

    # now we can be a sharded keyspace (and propagate to SrvKeyspace)
    utils.run_vtctl(['SetKeyspaceShardingInfo', 'test_keyspace',
                     'custom_ksid_col', base_sharding.keyspace_id_type])
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'],
                    auto_log=True)

    # run a health check on source replica so it responds to discovery
    utils.run_vtctl(['RunHealthCheck', shard_replica.tablet_alias])

    # create the split shards
    shard_0_master.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='-80',
        tablet_index=0)
    shard_0_replica.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='-80',
        tablet_index=1)
    shard_0_rdonly1.init_tablet(
        'rdonly',
        keyspace='test_keyspace',
        shard='-80',
        tablet_index=2)
    shard_1_master.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='80-',
        tablet_index=0)
    shard_1_replica.init_tablet(
        'replica',
        keyspace='test_keyspace',
        shard='80-',
        tablet_index=1)
    shard_1_rdonly1.init_tablet(
        'rdonly',
        keyspace='test_keyspace',
        shard='80-',
        tablet_index=2)

    for t in [shard_0_master, shard_0_replica, shard_0_rdonly1,
              shard_1_master, shard_1_replica, shard_1_rdonly1]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None,
                       binlog_use_v3_resharding_mode=False)

    for t in [shard_0_master, shard_0_replica, shard_0_rdonly1,
              shard_1_master, shard_1_replica, shard_1_rdonly1]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)

    for t in [shard_0_replica, shard_1_replica]:
      utils.wait_for_tablet_type(t.tablet_alias, 'replica')
    for t in [shard_0_rdonly1, shard_1_rdonly1]:
      utils.wait_for_tablet_type(t.tablet_alias, 'rdonly')

    sharded_tablets = [shard_0_master, shard_0_replica, shard_0_rdonly1,
                       shard_1_master, shard_1_replica, shard_1_rdonly1]
    for t in sharded_tablets:
      t.wait_for_vttablet_state('SERVING')

    # must restart vtgate after tablets are up, or else wait until 1min refresh
    # we want cache_ttl at zero so we re-read the topology for every test query.
    utils.vtgate.kill()

    utils.vtgate = None
    utils.VtGate().start(cache_ttl='0', tablets=[
        shard_master, shard_replica, shard_rdonly1,
        shard_0_master, shard_0_replica, shard_0_rdonly1,
        shard_1_master, shard_1_replica, shard_1_rdonly1])
    var = None

    # Wait for the endpoints, either local or remote.
    utils.vtgate.wait_for_endpoints('test_keyspace.0.master', 1, var=var)
    utils.vtgate.wait_for_endpoints('test_keyspace.0.replica', 1, var=var)
    utils.vtgate.wait_for_endpoints('test_keyspace.0.rdonly', 1, var=var)
    utils.vtgate.wait_for_endpoints('test_keyspace.-80.master', 1, var=var)
    utils.vtgate.wait_for_endpoints('test_keyspace.-80.replica', 1, var=var)
    utils.vtgate.wait_for_endpoints('test_keyspace.-80.rdonly', 1, var=var)
    utils.vtgate.wait_for_endpoints('test_keyspace.80-.master', 1, var=var)
    utils.vtgate.wait_for_endpoints('test_keyspace.80-.replica', 1, var=var)
    utils.vtgate.wait_for_endpoints('test_keyspace.80-.rdonly', 1, var=var)

    # check the Map Reduce API works correctly, should use ExecuteKeyRanges now,
    # as we are sharded (with just one shard).
    # again, we have 3 values in the database, asking for 4 splits will get us
    # a single query.
    sql = 'select id, msg from resharding1'
    s = utils.vtgate.split_query(sql, 'test_keyspace', 4)
    self.assertEqual(len(s), 1)
    self.assertEqual(s[0]['key_range_part']['keyspace'], 'test_keyspace')
    # There must be one empty KeyRange which represents the full keyspace.
    self.assertEqual(len(s[0]['key_range_part']['key_ranges']), 1)
    self.assertEqual(s[0]['key_range_part']['key_ranges'][0], {})

    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -\n'
                             'Partitions(rdonly): -\n'
                             'Partitions(replica): -\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # we need to create the schema, and the worker will do data copying
    for keyspace_shard in ('test_keyspace/-80', 'test_keyspace/80-'):
      utils.run_vtctl(['CopySchemaShard',
                       '--exclude_tables', 'unrelated',
                       shard_rdonly1.tablet_alias,
                       keyspace_shard],
                      auto_log=True)
    utils.run_vtctl(['RunHealthCheck', shard_rdonly1.tablet_alias])

    # Run vtworker as daemon for the following SplitClone commands.
    worker_proc, worker_port, worker_rpc_port = utils.run_vtworker_bg(
        ['--cell', 'test_nj', '--command_display_interval', '10ms',
         '--use_v3_resharding_mode=false'],
        auto_log=True)

    # Initial clone (online).
    workerclient_proc = utils.run_vtworker_client_bg(
        ['SplitClone',
         '--offline=false',
         '--exclude_tables', 'unrelated',
         '--chunk_count', '10',
         '--min_rows_per_chunk', '1',
         '--min_healthy_rdonly_tablets', '1',
         'test_keyspace/0'],
        worker_rpc_port)
    utils.wait_procs([workerclient_proc])
    self.verify_reconciliation_counters(worker_port, 'Online', 'resharding1',
                                        3, 0, 0, 0)

    # Reset vtworker such that we can run the next command.
    workerclient_proc = utils.run_vtworker_client_bg(['Reset'], worker_rpc_port)
    utils.wait_procs([workerclient_proc])

    # Modify the destination shard. SplitClone will revert the changes.
    # Delete row 1 (provokes an insert).
    shard_0_master.mquery('vt_test_keyspace',
                          'delete from resharding1 where id=1', write=True)
    # Delete row 2 (provokes an insert).
    shard_1_master.mquery('vt_test_keyspace',
                          'delete from resharding1 where id=2', write=True)
    # Update row 3 (provokes an update).
    shard_1_master.mquery('vt_test_keyspace',
                          "update resharding1 set msg='msg-not-3' where id=3",
                          write=True)
    # Insert row 4 (provokes a delete).
    self._insert_value(shard_1_master, 'resharding1', 4, 'msg4',
                       0xD000000000000000)

    workerclient_proc = utils.run_vtworker_client_bg(
        ['SplitClone',
         '--exclude_tables', 'unrelated',
         '--chunk_count', '10',
         '--min_rows_per_chunk', '1',
         '--min_healthy_rdonly_tablets', '1',
         'test_keyspace/0'],
        worker_rpc_port)
    utils.wait_procs([workerclient_proc])
    self.verify_reconciliation_counters(worker_port, 'Online', 'resharding1',
                                        2, 1, 1, 0)
    self.verify_reconciliation_counters(worker_port, 'Offline', 'resharding1',
                                        0, 0, 0, 3)
    # Terminate worker daemon because it is no longer needed.
    utils.kill_sub_process(worker_proc, soft=True)

    # check the startup values are in the right place
    self._check_startup_values()

    # check the schema too
    utils.run_vtctl(['ValidateSchemaKeyspace', 'test_keyspace'], auto_log=True)

    # check the binlog players are running
    logging.debug('Waiting for binlog players to start on new masters...')
    self.check_destination_master(shard_0_master, ['test_keyspace/0'])
    self.check_destination_master(shard_1_master, ['test_keyspace/0'])

    # check that binlog server exported the stats vars
    self.check_binlog_server_vars(shard_replica, horizontal=True)

    # testing filtered replication: insert a bunch of data on shard 1,
    # check we get most of it after a few seconds, wait for binlog server
    # timeout, check we get all of it.
    logging.debug('Inserting lots of data on source shard')
    self._insert_lots(1000)
    logging.debug('Checking 80 percent of data is sent quickly')
    v = self._check_lots_timeout(1000, 80, 5)
    if v != 100:
      logging.debug('Checking all data goes through eventually')
      self._check_lots_timeout(1000, 100, 20)
    logging.debug('Checking no data was sent the wrong way')
    self._check_lots_not_present(1000)
    self.check_binlog_player_vars(shard_0_master, ['test_keyspace/0'],
                                  seconds_behind_master_max=30)
    self.check_binlog_player_vars(shard_1_master, ['test_keyspace/0'],
                                  seconds_behind_master_max=30)
    self.check_binlog_server_vars(shard_replica, horizontal=True,
                                  min_statements=1000, min_transactions=1000)

    # use vtworker to compare the data
    for t in [shard_0_rdonly1, shard_1_rdonly1]:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias])

    if base_sharding.use_multi_split_diff:
        logging.debug('Running vtworker MultiSplitDiff for 0')
        utils.run_vtworker(['-cell', 'test_nj',
                            '--use_v3_resharding_mode=false',
                            'MultiSplitDiff',
                            '--min_healthy_rdonly_tablets', '1',
                            'test_keyspace/0'],
                           auto_log=True)
    else:
        logging.debug('Running vtworker SplitDiff for -80')
        utils.run_vtworker(['-cell', 'test_nj',
                            '--use_v3_resharding_mode=false',
                            'SplitDiff',
                            '--min_healthy_rdonly_tablets', '1',
                            'test_keyspace/-80'],
                           auto_log=True)
        logging.debug('Running vtworker SplitDiff for 80-')
        utils.run_vtworker(['-cell', 'test_nj',
                            '--use_v3_resharding_mode=false',
                            'SplitDiff',
                            '--min_healthy_rdonly_tablets', '1',
                            'test_keyspace/80-'],
                           auto_log=True)

    utils.pause('Good time to test vtworker for diffs')

    # get status for the destination master tablet, make sure we have it all
    self.check_running_binlog_player(shard_0_master, 2000, 2000)
    self.check_running_binlog_player(shard_1_master, 6000, 2000)

    # check we can't migrate the master just yet
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/0', 'master'],
                    expect_fail=True)

    # now serve rdonly from the split shards
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/0', 'rdonly'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # make sure rdonly tablets are back to serving before hitting vtgate.
    for t in [shard_0_rdonly1, shard_1_rdonly1]:
      t.wait_for_vttablet_state('SERVING')

    utils.vtgate.wait_for_endpoints('test_keyspace.-80.rdonly', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.80-.rdonly', 1)

    # check the Map Reduce API works correctly, should use ExecuteKeyRanges
    # on both destination shards now.
    # we ask for 2 splits to only have one per shard
    sql = 'select id, msg from resharding1'
    timeout = 10.0
    while True:
      try:
        s = utils.vtgate.split_query(sql, 'test_keyspace', 2)
        break
      except Exception:  # pylint: disable=broad-except
        timeout = utils.wait_step(
            'vtgate executes split_query properly', timeout)
    self.assertEqual(len(s), 2)
    self.assertEqual(s[0]['key_range_part']['keyspace'], 'test_keyspace')
    self.assertEqual(s[1]['key_range_part']['keyspace'], 'test_keyspace')
    self.assertEqual(len(s[0]['key_range_part']['key_ranges']), 1)
    self.assertEqual(len(s[1]['key_range_part']['key_ranges']), 1)

    # then serve replica from the split shards
    source_tablet = shard_replica
    destination_tablets = [shard_0_replica, shard_1_replica]

    utils.run_vtctl(
        ['MigrateServedTypes', 'test_keyspace/0', 'replica'], auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # move replica back and forth
    utils.run_vtctl(
        ['MigrateServedTypes', '-reverse', 'test_keyspace/0', 'replica'],
        auto_log=True)
    # After a backwards migration, queryservice should be enabled on
    # source and disabled on destinations
    utils.check_tablet_query_service(self, source_tablet, True, False)
    utils.check_tablet_query_services(self, destination_tablets, False, True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/0', 'replica'],
                    auto_log=True)
    # After a forwards migration, queryservice should be disabled on
    # source and enabled on destinations
    utils.check_tablet_query_service(self, source_tablet, False, True)
    utils.check_tablet_query_services(self, destination_tablets, True, False)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # then serve master from the split shards
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/0', 'master'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # check the binlog players are gone now
    self.check_no_binlog_player(shard_0_master)
    self.check_no_binlog_player(shard_1_master)

    # make sure we can't delete a shard with tablets
    utils.run_vtctl(['DeleteShard', 'test_keyspace/0'], expect_fail=True)

    # remove the original tablets in the original shard
    tablet.kill_tablets([shard_master, shard_replica, shard_rdonly1])
    for t in [shard_replica, shard_rdonly1]:
      utils.run_vtctl(['DeleteTablet', t.tablet_alias], auto_log=True)
    utils.run_vtctl(['DeleteTablet', '-allow_master',
                     shard_master.tablet_alias], auto_log=True)

    # rebuild the serving graph, all mentions of the old shards shoud be gone
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # delete the original shard
    utils.run_vtctl(['DeleteShard', 'test_keyspace/0'], auto_log=True)

    # kill everything else
    tablet.kill_tablets([shard_0_master, shard_0_replica, shard_0_rdonly1,
                         shard_1_master, shard_1_replica, shard_1_rdonly1])


if __name__ == '__main__':
  utils.main()
