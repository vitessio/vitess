#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# This test simulates the first time a database has to be split:
# - we start with a keyspace with a single shard and a single table
# - we add and populate the sharding key
# - we set the sharding key in the topology
# - we backup / restore into 2 instances
# - we enable filtered replication
# - we move all serving types
# - we scrap the source tablets
# - we remove the original shard

import base64
import logging
import threading
import struct
import time
import unittest

from vtdb import keyrange_constants

import environment
import utils
import tablet

use_clone_worker = False
keyspace_id_type = keyrange_constants.KIT_UINT64
pack_keyspace_id = struct.Struct('!Q').pack

# initial shard, covers everything
shard_master = tablet.Tablet()
shard_replica = tablet.Tablet()
shard_rdonly = tablet.Tablet()

# split shards
# range "" - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_rdonly = tablet.Tablet()
# range 80 - ""
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()
shard_1_rdonly = tablet.Tablet()


def setUpModule():
  try:
    environment.topo_server_setup()

    setup_procs = [
        shard_master.init_mysql(),
        shard_replica.init_mysql(),
        shard_rdonly.init_mysql(),
        shard_0_master.init_mysql(),
        shard_0_replica.init_mysql(),
        shard_0_rdonly.init_mysql(),
        shard_1_master.init_mysql(),
        shard_1_replica.init_mysql(),
        shard_1_rdonly.init_mysql(),
        ]
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      shard_master.teardown_mysql(),
      shard_replica.teardown_mysql(),
      shard_rdonly.teardown_mysql(),
      shard_0_master.teardown_mysql(),
      shard_0_replica.teardown_mysql(),
      shard_0_rdonly.teardown_mysql(),
      shard_1_master.teardown_mysql(),
      shard_1_replica.teardown_mysql(),
      shard_1_rdonly.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_master.remove_tree()
  shard_replica.remove_tree()
  shard_rdonly.remove_tree()
  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_0_rdonly.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica.remove_tree()
  shard_1_rdonly.remove_tree()


class TestInitialSharding(unittest.TestCase):

  # create_schema will create the same schema on the keyspace
  def _create_schema(self):
    create_table_template = '''create table %s(
id bigint auto_increment,
msg varchar(64),
primary key (id),
index by_msg (msg)
) Engine=InnoDB'''

    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + create_table_template % ("resharding1"),
                     'test_keyspace'],
                    auto_log=True)

  def _add_sharding_key_to_schema(self):
    if keyspace_id_type == keyrange_constants.KIT_BYTES:
      t = 'varbinary(64)'
    else:
      t = 'bigint(20) unsigned'
    sql = 'alter table %s add keyspace_id ' + t
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + sql % ("resharding1"),
                     'test_keyspace'],
                    auto_log=True)

  def _mark_sharding_key_not_null(self):
    if keyspace_id_type == keyrange_constants.KIT_BYTES:
      t = 'varbinary(64)'
    else:
      t = 'bigint(20) unsigned'
    sql = 'alter table %s modify keyspace_id ' + t + ' not null'
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + sql % ("resharding1"),
                     'test_keyspace'],
                    auto_log=True)

  # _insert_startup_value inserts a value in the MySQL database before it
  # is sharded
  def _insert_startup_value(self, tablet, table, id, msg):
    tablet.mquery('vt_test_keyspace', [
        'begin',
        'insert into %s(id, msg) values(%u, "%s")' % (table, id, msg),
        'commit'
        ], write=True)

  def _insert_startup_values(self):
    self._insert_startup_value(shard_master, 'resharding1', 1, 'msg1')
    self._insert_startup_value(shard_master, 'resharding1', 2, 'msg2')
    self._insert_startup_value(shard_master, 'resharding1', 3, 'msg3')

  def _backfill_keyspace_id(self, tablet):
    tablet.mquery('vt_test_keyspace', [
        'begin',
        'update resharding1 set keyspace_id=0x1000000000000000 where id=1',
        'update resharding1 set keyspace_id=0x9000000000000000 where id=2',
        'update resharding1 set keyspace_id=0xD000000000000000 where id=3',
        'commit'
        ], write=True)

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

  def _check_startup_values(self):
    # check first value is in the right shard
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly]:
      self._check_value(t, 'resharding1', 1, 'msg1', 0x1000000000000000)
    for t in [shard_1_master, shard_1_replica, shard_1_rdonly]:
      self._check_value(t, 'resharding1', 1, 'msg1',
                        0x1000000000000000, should_be_here=False)

    # check second value is in the right shard
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly]:
      self._check_value(t, 'resharding1', 2, 'msg2', 0x9000000000000000,
                        should_be_here=False)
    for t in [shard_1_master, shard_1_replica, shard_1_rdonly]:
      self._check_value(t, 'resharding1', 2, 'msg2', 0x9000000000000000)

    # check third value is in the right shard too
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly]:
      self._check_value(t, 'resharding1', 3, 'msg3', 0xD000000000000000,
                        should_be_here=False)
    for t in [shard_1_master, shard_1_replica, shard_1_rdonly]:
      self._check_value(t, 'resharding1', 3, 'msg3', 0xD000000000000000)

  def _insert_lots(self, count, base=0):
    for i in xrange(count):
      self._insert_value(shard_master, 'resharding1', 10000 + base + i,
                         'msg-range1-%u' % i, 0xA000000000000000 + base + i)
      self._insert_value(shard_master, 'resharding1', 20000 + base + i,
                         'msg-range2-%u' % i, 0xE000000000000000 + base + i)

  # _check_lots returns how many of the values we have, in percents.
  def _check_lots(self, count, base=0):
    found = 0
    for i in xrange(count):
      if self._is_value_present_and_correct(shard_1_replica, 'resharding1',
                                            10000 + base + i, 'msg-range1-%u' %
                                            i, 0xA000000000000000 + base + i):
        found += 1
      if self._is_value_present_and_correct(shard_1_replica, 'resharding1',
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
      timeout = utils.wait_step('enough data went through', timeout)

  # _check_lots_not_present makes sure no data is in the wrong shard
  def _check_lots_not_present(self, count, base=0):
    found = 0
    for i in xrange(count):
      self._check_value(shard_0_replica, 'resharding1', 10000 + base + i,
                        'msg-range1-%u' % i, 0xA000000000000000 + base + i,
                        should_be_here=False)
      self._check_value(shard_0_replica, 'resharding1', 20000 + base + i,
                        'msg-range2-%u' % i, 0xE000000000000000 + base + i,
                        should_be_here=False)

  def test_resharding(self):
    # create the keyspace with just one shard
    utils.run_vtctl(['CreateKeyspace',
                     'test_keyspace'])
    utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', 'test_keyspace',
                     'keyspace_id', keyspace_id_type])

    shard_master.init_tablet( 'master',  'test_keyspace', '0')
    shard_replica.init_tablet('replica', 'test_keyspace', '0')
    shard_rdonly.init_tablet( 'rdonly',  'test_keyspace', '0')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # create databases so vttablet can start behaving normally
    for t in [shard_master, shard_replica, shard_rdonly]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None)

    # wait for the tablets
    shard_master.wait_for_vttablet_state('SERVING')
    shard_replica.wait_for_vttablet_state('SERVING')
    shard_rdonly.wait_for_vttablet_state('SERVING')

    # reparent to make the tablets work
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/0',
                     shard_master.tablet_alias], auto_log=True)

    # create the tables and add startup values
    self._create_schema()
    self._insert_startup_values()

    # change the schema, backfill keyspace_id, and change schema again
    self._add_sharding_key_to_schema()
    self._backfill_keyspace_id(shard_master)
    self._mark_sharding_key_not_null()

    # create the split shards
    shard_0_master.init_tablet( 'master',  'test_keyspace', '-80')
    shard_0_replica.init_tablet('replica', 'test_keyspace', '-80')
    shard_0_rdonly.init_tablet( 'rdonly',  'test_keyspace', '-80')
    shard_1_master.init_tablet( 'master',  'test_keyspace', '80-')
    shard_1_replica.init_tablet('replica', 'test_keyspace', '80-')
    shard_1_rdonly.init_tablet( 'rdonly',  'test_keyspace', '80-')

    # start vttablet on the split shards (no db created,
    # so they're all not serving)
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly,
              shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.start_vttablet(wait_for_state=None)
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly,
              shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -\n' +
                             'Partitions(rdonly): -\n' +
                             'Partitions(replica): -\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)

    if use_clone_worker:
      # the worker will do snapshot / restore
      utils.run_vtworker(['--cell', 'test_nj',
                          '--command_display_interval', '10ms',
                          'SplitClone',
                          '--exclude_tables' ,'unrelated',
                          '--strategy=-populate_blp_checkpoint -write_masters_only',
                          '--source_reader_count', '10',
                          '--min_table_size_for_split', '1',
                          'test_keyspace/0'],
                         auto_log=True)
      utils.run_vtctl(['ChangeSlaveType', shard_rdonly.tablet_alias, 'rdonly'],
                      auto_log=True)

    else:
      # take the snapshot for the split
      utils.run_vtctl(['MultiSnapshot', '--spec=-80-',
                       shard_replica.tablet_alias], auto_log=True)

      # wait for tablet's binlog server service to be enabled after snapshot
      shard_replica.wait_for_binlog_server_state("Enabled")

      # perform the restore.
      utils.run_vtctl(['ShardMultiRestore', '-strategy=-populate_blp_checkpoint',
                       'test_keyspace/-80', shard_replica.tablet_alias],
                      auto_log=True)
      utils.run_vtctl(['ShardMultiRestore', '-strategy=-populate_blp_checkpoint',
                       'test_keyspace/80-', shard_replica.tablet_alias],
                      auto_log=True)

    # check the startup values are in the right place
    self._check_startup_values()

    # check the schema too
    utils.run_vtctl(['ValidateSchemaKeyspace', 'test_keyspace'], auto_log=True)

    # check the binlog players are running
    logging.debug("Waiting for binlog players to start on new masters...")
    shard_0_master.wait_for_binlog_player_count(1)
    shard_1_master.wait_for_binlog_player_count(1)

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

    # use the vtworker checker to compare the data
    logging.debug("Running vtworker SplitDiff for -80")
    utils.run_vtworker(['-cell', 'test_nj', 'SplitDiff', 'test_keyspace/-80'],
                       auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_rdonly.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_0_rdonly.tablet_alias, 'rdonly'],
                    auto_log=True)

    logging.debug("Running vtworker SplitDiff for 80-")
    utils.run_vtworker(['-cell', 'test_nj', 'SplitDiff', 'test_keyspace/80-'],
                       auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_rdonly.tablet_alias, 'rdonly'],
                    auto_log=True)
    utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly.tablet_alias, 'rdonly'],
                    auto_log=True)

    utils.pause("Good time to test vtworker for diffs")

    # check we can't migrate the master just yet
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/0', 'master'],
                    expect_fail=True)

    # now serve rdonly from the split shards
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/0', 'rdonly'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -\n' +
                             'Partitions(rdonly): -80 80-\n' +
                             'Partitions(replica): -\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)

    # then serve replica from the split shards
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/0', 'replica'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -\n' +
                             'Partitions(rdonly): -80 80-\n' +
                             'Partitions(replica): -80 80-\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)

    # move replica back and forth
    utils.run_vtctl(['MigrateServedTypes', '-reverse', 'test_keyspace/0', 'replica'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -\n' +
                             'Partitions(rdonly): -80 80-\n' +
                             'Partitions(replica): -\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/0', 'replica'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -\n' +
                             'Partitions(rdonly): -80 80-\n' +
                             'Partitions(replica): -80 80-\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)

    # then serve master from the split shards
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/0', 'master'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n' +
                             'Partitions(rdonly): -80 80-\n' +
                             'Partitions(replica): -80 80-\n' +
                             'TabletTypes: master,rdonly,replica',
                             keyspace_id_type=keyspace_id_type)

    # check the binlog players are gone now
    shard_0_master.wait_for_binlog_player_count(0)
    shard_1_master.wait_for_binlog_player_count(0)

    # make sure we can't delete a shard with tablets
    utils.run_vtctl(['DeleteShard', 'test_keyspace/0'], expect_fail=True)

    # scrap the original tablets in the original shard
    for t in [shard_master, shard_replica, shard_rdonly]:
      utils.run_vtctl(['ScrapTablet', t.tablet_alias], auto_log=True)
    tablet.kill_tablets([shard_master, shard_replica, shard_rdonly])
    for t in [shard_master, shard_replica, shard_rdonly]:
      utils.run_vtctl(['DeleteTablet', t.tablet_alias], auto_log=True)

    # rebuild the serving graph, all mentions of the old shards shoud be gone
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # delete the original shard
    utils.run_vtctl(['DeleteShard', 'test_keyspace/0'], auto_log=True)

    # kill everything else
    tablet.kill_tablets([shard_0_master, shard_0_replica, shard_0_rdonly,
                         shard_1_master, shard_1_replica, shard_1_rdonly])

if __name__ == '__main__':
  utils.main()
