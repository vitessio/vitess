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

"""This test covers a resharding scenario of an already sharded keyspace.

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

import threading
import time

import logging
import unittest

import base_sharding
import environment
import tablet
import utils

from vtproto import topodata_pb2
from vtdb import keyrange_constants

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
shard_2_rdonly1 = tablet.Tablet()
# range c0 - ''
shard_3_master = tablet.Tablet()
shard_3_replica = tablet.Tablet()
shard_3_rdonly1 = tablet.Tablet()

shard_2_tablets = [shard_2_master, shard_2_replica1, shard_2_replica2,
                   shard_2_rdonly1]
shard_3_tablets = [shard_3_master, shard_3_replica, shard_3_rdonly1]
all_tablets = ([shard_0_master, shard_0_replica, shard_0_ny_rdonly,
                shard_1_master, shard_1_slave1, shard_1_slave2,
                shard_1_ny_rdonly, shard_1_rdonly1] +
               shard_2_tablets + shard_3_tablets)


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

  teardown_procs = [t.teardown_mysql() for t in all_tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)
  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  for t in all_tablets:
    t.remove_tree()


# InsertThread will insert a value into the timestamps table, and then
# every 1/5s will update its value with the current timestamp
class InsertThread(threading.Thread):

  def __init__(self, tablet_obj, thread_name, thread_id, user_id,
               keyspace_id):
    threading.Thread.__init__(self)
    self.tablet = tablet_obj
    self.thread_name = thread_name
    self.thread_id = thread_id
    self.user_id = user_id
    self.keyspace_id = keyspace_id
    self.str_keyspace_id = utils.uint64_to_hex(keyspace_id)
    self.done = False

    self.tablet.mquery(
        'vt_test_keyspace',
        ['begin',
         'insert into timestamps(id, time_milli, custom_ksid_col) '
         'values(%d, %d, 0x%x) '
         '/* vtgate:: keyspace_id:%s */ /* user_id:%d */' %
         (self.thread_id, long(time.time() * 1000), self.keyspace_id,
          self.str_keyspace_id, self.user_id),
         'commit'],
        write=True, user='vt_app')
    self.start()

  def run(self):
    try:
      while not self.done:
        self.tablet.mquery(
            'vt_test_keyspace',
            ['begin',
             'update timestamps set time_milli=%d '
             'where id=%d /* vtgate:: keyspace_id:%s */ /* user_id:%d */' %
             (long(time.time() * 1000), self.thread_id,
              self.str_keyspace_id, self.user_id),
             'commit'],
            write=True, user='vt_app')
        time.sleep(0.2)
    except Exception:  # pylint: disable=broad-except
      logging.exception('InsertThread got exception.')


# MonitorLagThread will get values from a database, and compare the timestamp
# to evaluate lag. Since the qps is really low, and we send binlogs as chunks,
# the latency is pretty high (a few seconds).
class MonitorLagThread(threading.Thread):

  def __init__(self, tablet_obj, thread_name, thread_id):
    threading.Thread.__init__(self)
    self.tablet = tablet_obj
    self.thread_name = thread_name
    self.thread_id = thread_id
    self.done = False
    self.max_lag_ms = 0
    self.lag_sum_ms = 0
    self.sample_count = 0
    self.start()

  def run(self):
    try:
      while not self.done:
        result = self.tablet.mquery(
            'vt_test_keyspace',
            'select time_milli from timestamps where id=%d' %
            self.thread_id)
        if result:
          lag_ms = long(time.time() * 1000) - long(result[0][0])
          logging.debug('MonitorLagThread(%s) got %d ms',
                        self.thread_name, lag_ms)
          self.sample_count += 1
          self.lag_sum_ms += lag_ms
          if lag_ms > self.max_lag_ms:
            self.max_lag_ms = lag_ms
        time.sleep(5.0)
    except Exception:  # pylint: disable=broad-except
      logging.exception('MonitorLagThread got exception.')


class TestResharding(unittest.TestCase, base_sharding.BaseShardingTest):

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
custom_ksid_col ''' + t + ''' not null,
msg varchar(64),
id bigint not null,
parent_id bigint not null,
primary key (parent_id, id),
index by_msg (msg)
) Engine=InnoDB'''
    create_table_bindata_template = '''create table %s(
custom_ksid_col ''' + t + ''' not null,
id bigint not null,
parent_id bigint not null,
msg bit(8),
primary key (parent_id, id),
index by_msg (msg)
) Engine=InnoDB'''
    create_view_template = (
        'create view %s'
        '(parent_id, id, msg, custom_ksid_col)'
        'as select parent_id, id, msg, custom_ksid_col '
        'from %s')
    create_timestamp_table = '''create table timestamps(
id int not null,
time_milli bigint(20) unsigned not null,
custom_ksid_col ''' + t + ''' not null,
primary key (id)
) Engine=InnoDB'''
    # Make sure that clone and diff work with tables which have no primary key.
    # RBR only because Vitess requires the primary key for query rewrites if
    # it is running with statement based replication.
    create_no_pk_table = '''create table no_pk(
custom_ksid_col ''' + t + ''' not null,
msg varchar(64),
id bigint not null,
parent_id bigint not null
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
                     '-sql=' + create_table_bindata_template % ('resharding3'),
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
    if base_sharding.use_rbr:
      utils.run_vtctl(['ApplySchema', '-sql=' + create_no_pk_table,
                       'test_keyspace'], auto_log=True)

  def _insert_startup_values(self):
    self._insert_value(shard_0_master, 'resharding1', 1, 'msg1',
                       0x1000000000000000)
    self._insert_value(shard_1_master, 'resharding1', 2, 'msg2',
                       0x9000000000000000)
    self._insert_value(shard_1_master, 'resharding1', 3, 'msg3',
                       0xD000000000000000)
    self._insert_value(shard_0_master, 'resharding3', 1, 'a',
                       0x1000000000000000)
    self._insert_value(shard_1_master, 'resharding3', 2, 'b',
                       0x9000000000000000)
    self._insert_value(shard_1_master, 'resharding3', 3, 'c',
                       0xD000000000000000)
    if base_sharding.use_rbr:
      self._insert_value(shard_1_master, 'no_pk', 1, 'msg1',
                         0xA000000000000000)
      # TODO(github.com/vitessio/vitess/issues/2880): Add more rows here such
      # clone and diff would break when the insertion order on source and
      # dest shards is different.

  def _check_startup_values(self):
    # check first value is in the right shard
    for t in shard_2_tablets:
      self._check_value(t, 'resharding1', 2, 'msg2', 0x9000000000000000)
      self._check_value(t, 'resharding3', 2, 'b', 0x9000000000000000)
    for t in shard_3_tablets:
      self._check_value(t, 'resharding1', 2, 'msg2', 0x9000000000000000,
                        should_be_here=False)
      self._check_value(t, 'resharding3', 2, 'b', 0x9000000000000000,
                        should_be_here=False)

    # check second value is in the right shard too
    for t in shard_2_tablets:
      self._check_value(t, 'resharding1', 3, 'msg3', 0xD000000000000000,
                        should_be_here=False)
      self._check_value(t, 'resharding3', 3, 'c', 0xD000000000000000,
                        should_be_here=False)
    for t in shard_3_tablets:
      self._check_value(t, 'resharding1', 3, 'msg3', 0xD000000000000000)
      self._check_value(t, 'resharding3', 3, 'c', 0xD000000000000000)

    if base_sharding.use_rbr:
      for t in shard_2_tablets:
        self._check_value(t, 'no_pk', 1, 'msg1', 0xA000000000000000)
      for t in shard_3_tablets:
        self._check_value(t, 'no_pk', 1, 'msg1', 0xA000000000000000,
                          should_be_here=False)

  def _insert_lots(self, count, base=0):
    for i in xrange(count):
      self._insert_value(shard_1_master, 'resharding1', 10000 + base + i,
                         'msg-range1-%d' % i, 0xA000000000000000 + base + i)
      self._insert_value(shard_1_master, 'resharding1', 20000 + base + i,
                         'msg-range2-%d' % i, 0xE000000000000000 + base + i)

  def _exec_multi_shard_dmls(self):
    mids = [10000001, 10000002, 10000003]
    msg_ids = ['msg-id10000001', 'msg-id10000002', 'msg-id10000003']
    keyspace_ids = [0x9000000000000000, 0xD000000000000000,
                    0xE000000000000000]
    self._insert_multi_value(shard_1_master, 'resharding1', mids,
                             msg_ids, keyspace_ids)

    mids = [10000004, 10000005]
    msg_ids = ['msg-id10000004', 'msg-id10000005']
    keyspace_ids = [0xD000000000000000, 0xE000000000000000]
    self._insert_multi_value(shard_1_master, 'resharding1', mids,
                             msg_ids, keyspace_ids)

    mids = [10000011, 10000012, 10000013]
    msg_ids = ['msg-id10000011', 'msg-id10000012', 'msg-id10000013']
    keyspace_ids = [0x9000000000000000, 0xD000000000000000, 0xE000000000000000]
    self._insert_multi_value(shard_1_master, 'resharding1', mids,
                             msg_ids, keyspace_ids)

    # This update targets two shards.
    self._exec_non_annotated_update(shard_1_master, 'resharding1',
                                    [10000011, 10000012], 'update1')
    # This update targets one shard.
    self._exec_non_annotated_update(shard_1_master, 'resharding1',
                                    [10000013], 'update2')

    mids = [10000014, 10000015, 10000016]
    msg_ids = ['msg-id10000014', 'msg-id10000015', 'msg-id10000016']
    keyspace_ids = [0x9000000000000000, 0xD000000000000000, 0xE000000000000000]
    self._insert_multi_value(shard_1_master, 'resharding1', mids,
                             msg_ids, keyspace_ids)
    
    # This delete targets two shards.
    self._exec_non_annotated_delete(shard_1_master, 'resharding1',
                                    [10000014, 10000015])

    # This delete targets one shard.
    self._exec_non_annotated_delete(shard_1_master, 'resharding1', [10000016])

    # repeat DMLs for table with msg as bit(8)
    mids = [10000001, 10000002, 10000003]
    keyspace_ids = [0x9000000000000000, 0xD000000000000000,
                    0xE000000000000000]
    self._insert_multi_value(shard_1_master, 'resharding3', mids,
                             ['a','b','c'], keyspace_ids)

    mids = [10000004, 10000005]
    keyspace_ids = [0xD000000000000000, 0xE000000000000000]
    self._insert_multi_value(shard_1_master, 'resharding3', mids,
                            ['d', 'e'], keyspace_ids)
    mids = [10000011, 10000012, 10000013]
    keyspace_ids = [0x9000000000000000, 0xD000000000000000, 0xE000000000000000]

    self._insert_multi_value(shard_1_master, 'resharding3', mids,
                             ['k', 'l', 'm'], keyspace_ids)
    
    # This update targets two shards.
    self._exec_non_annotated_update(shard_1_master, 'resharding3',
                                    [10000011, 10000012], 'g')

    # This update targets one shard.
    self._exec_non_annotated_update(shard_1_master, 'resharding3',
                                    [10000013], 'h')

    mids = [10000014, 10000015, 10000016]
    keyspace_ids = [0x9000000000000000, 0xD000000000000000, 0xE000000000000000]
    self._insert_multi_value(shard_1_master, 'resharding3', mids,
                             ['n', 'o', 'p'], keyspace_ids)

    # This delete targets two shards.
    self._exec_non_annotated_delete(shard_1_master, 'resharding3',
                                    [10000014, 10000015])

    # This delete targets one shard.
    self._exec_non_annotated_delete(shard_1_master, 'resharding3', [10000016])
    
  def _check_multi_shard_values(self):
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding1', 10000001, 'msg-id10000001', 0x9000000000000000)
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding1', 10000002, 'msg-id10000002', 0xD000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding1', 10000003, 'msg-id10000003', 0xE000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding1', 10000001, 'msg-id10000001', 0x9000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding1', 10000002, 'msg-id10000002', 0xD000000000000000)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding1', 10000003, 'msg-id10000003', 0xE000000000000000)

    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding1', 10000004, 'msg-id10000004', 0xD000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding1', 10000005, 'msg-id10000005', 0xE000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding1', 10000004, 'msg-id10000004', 0xD000000000000000)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding1', 10000005, 'msg-id10000005', 0xE000000000000000)

    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding1', 10000011, 'update1', 0x9000000000000000)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding1', 10000012, 'update1', 0xD000000000000000)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding1', 10000013, 'update2', 0xE000000000000000)

    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2,
         shard_3_master, shard_3_replica],
        'resharding1', 10000014, 'msg-id10000014', 0x9000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2,
         shard_3_master, shard_3_replica],
        'resharding1', 10000015, 'msg-id10000015', 0xD000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2,
         shard_3_master, shard_3_replica],
        'resharding1', 10000016, 'msg-id10000016', 0xF000000000000000,
        should_be_here=False)

  # checks for bit(8) table
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding3', 10000001, 'a', 0x9000000000000000)
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding3', 10000002, 'b', 0xD000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding3', 10000003, 'c', 0xE000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding3', 10000001, 'a', 0x9000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding3', 10000002, 'b', 0xD000000000000000)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding3', 10000003, 'c', 0xE000000000000000)

    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding3', 10000004, 'd', 0xD000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding3', 10000005, 'e', 0xE000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding3', 10000004, 'd', 0xD000000000000000)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding3', 10000005, 'e', 0xE000000000000000)

    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2],
        'resharding3', 10000011, 'g', 0x9000000000000000)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding3', 10000012, 'g', 0xD000000000000000)
    self._check_multi_dbs(
        [shard_3_master, shard_3_replica],
        'resharding3', 10000013, 'h', 0xE000000000000000)

    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2,
         shard_3_master, shard_3_replica],
        'resharding3', 10000014, 'n', 0x9000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2,
         shard_3_master, shard_3_replica],
        'resharding3', 10000015, 'o', 0xD000000000000000,
        should_be_here=False)
    self._check_multi_dbs(
        [shard_2_master, shard_2_replica1, shard_2_replica2,
         shard_3_master, shard_3_replica],
        'resharding3', 10000016, 'p', 0xF000000000000000,
        should_be_here=False)
    
  # _check_multi_dbs checks the row in multiple dbs.
  def _check_multi_dbs(self, dblist, table, mid, msg, keyspace_id,
                       should_be_here=True):
    for db in dblist:
      self._check_value(db, table, mid, msg, keyspace_id, should_be_here)

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
    # we're going to reparent and swap these two
    global shard_2_master, shard_2_replica1

    utils.run_vtctl(['CreateKeyspace',
                     '--sharding_column_name', 'bad_column',
                     '--sharding_column_type', 'bytes',
                     'test_keyspace'])
    utils.run_vtctl(['SetKeyspaceShardingInfo', 'test_keyspace',
                     'custom_ksid_col', 'uint64'], expect_fail=True)
    utils.run_vtctl(['SetKeyspaceShardingInfo', '-force',
                     'test_keyspace',
                     'custom_ksid_col', base_sharding.keyspace_id_type])

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
    full_mycnf_args = (base_sharding.keyspace_id_type ==
                       keyrange_constants.KIT_BYTES)

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
    shard_2_rdonly1.init_tablet('rdonly', 'test_keyspace', '80-c0')
    shard_3_master.init_tablet('replica', 'test_keyspace', 'c0-')
    shard_3_replica.init_tablet('replica', 'test_keyspace', 'c0-')
    shard_3_rdonly1.init_tablet('rdonly', 'test_keyspace', 'c0-')

    # start vttablet on the split shards (no db created,
    # so they're all not serving)
    shard_2_master.start_vttablet(wait_for_state=None,
                                  binlog_use_v3_resharding_mode=False)
    shard_3_master.start_vttablet(wait_for_state=None,
                                  binlog_use_v3_resharding_mode=False)
    for t in [shard_2_replica1, shard_2_replica2, shard_2_rdonly1,
              shard_3_replica, shard_3_rdonly1]:
      t.start_vttablet(wait_for_state=None,
                       binlog_use_v3_resharding_mode=False)
    for t in [shard_2_master, shard_2_replica1, shard_2_replica2,
              shard_2_rdonly1,
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
        keyspace_id_type=base_sharding.keyspace_id_type,
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

    # Run vtworker as daemon for the following SplitClone commands.
    worker_proc, worker_port, worker_rpc_port = utils.run_vtworker_bg(
        ['--cell', 'test_nj', '--command_display_interval', '10ms',
         '--use_v3_resharding_mode=false'],
        auto_log=True)

    # Copy the data from the source to the destination shards.
    # --max_tps is only specified to enable the throttler and ensure that the
    # code is executed. But the intent here is not to throttle the test, hence
    # the rate limit is set very high.
    #
    # Initial clone (online).
    workerclient_proc = utils.run_vtworker_client_bg(
        ['SplitClone',
         '--offline=false',
         '--exclude_tables', 'unrelated',
         '--chunk_count', '10',
         '--min_rows_per_chunk', '1',
         '--min_healthy_rdonly_tablets', '1',
         '--max_tps', '9999',
         'test_keyspace/80-'],
        worker_rpc_port)
    utils.wait_procs([workerclient_proc])
    self.verify_reconciliation_counters(worker_port, 'Online', 'resharding1',
                                        2, 0, 0, 0)

    # Reset vtworker such that we can run the next command.
    workerclient_proc = utils.run_vtworker_client_bg(['Reset'], worker_rpc_port)
    utils.wait_procs([workerclient_proc])

    # Test the correct handling of keyspace_id changes which happen after
    # the first clone.
    # Let row 2 go to shard 3 instead of shard 2.
    shard_1_master.mquery('vt_test_keyspace',
                          'update resharding1 set'
                          ' custom_ksid_col=0xD000000000000000 WHERE id=2',
                          write=True)
    workerclient_proc = utils.run_vtworker_client_bg(
        ['SplitClone',
         '--offline=false',
         '--exclude_tables', 'unrelated',
         '--chunk_count', '10',
         '--min_rows_per_chunk', '1',
         '--min_healthy_rdonly_tablets', '1',
         '--max_tps', '9999',
         'test_keyspace/80-'],
        worker_rpc_port)
    utils.wait_procs([workerclient_proc])
    # Row 2 will be deleted from shard 2 and inserted to shard 3.
    self.verify_reconciliation_counters(worker_port, 'Online', 'resharding1',
                                        1, 0, 1, 1)
    self._check_value(shard_2_master, 'resharding1', 2, 'msg2',
                      0xD000000000000000, should_be_here=False)
    self._check_value(shard_3_master, 'resharding1', 2, 'msg2',
                      0xD000000000000000)
    # Reset vtworker such that we can run the next command.
    workerclient_proc = utils.run_vtworker_client_bg(['Reset'], worker_rpc_port)
    utils.wait_procs([workerclient_proc])

    # Move row 2 back to shard 2 from shard 3 by changing the keyspace_id again.
    shard_1_master.mquery('vt_test_keyspace',
                          'update resharding1 set'
                          ' custom_ksid_col=0x9000000000000000 WHERE id=2',
                          write=True)
    workerclient_proc = utils.run_vtworker_client_bg(
        ['SplitClone',
         '--offline=false',
         '--exclude_tables', 'unrelated',
         '--chunk_count', '10',
         '--min_rows_per_chunk', '1',
         '--min_healthy_rdonly_tablets', '1',
         '--max_tps', '9999',
         'test_keyspace/80-'],
        worker_rpc_port)
    utils.wait_procs([workerclient_proc])
    # Row 2 will be deleted from shard 3 and inserted to shard 2.
    self.verify_reconciliation_counters(worker_port, 'Online', 'resharding1',
                                        1, 0, 1, 1)
    self._check_value(shard_2_master, 'resharding1', 2, 'msg2',
                      0x9000000000000000)
    self._check_value(shard_3_master, 'resharding1', 2, 'msg2',
                      0x9000000000000000, should_be_here=False)
    # Reset vtworker such that we can run the next command.
    workerclient_proc = utils.run_vtworker_client_bg(['Reset'], worker_rpc_port)
    utils.wait_procs([workerclient_proc])

    # Modify the destination shard. SplitClone will revert the changes.
    # Delete row 2 (provokes an insert).
    shard_2_master.mquery('vt_test_keyspace',
                          'delete from resharding1 where id=2', write=True)
    # Update row 3 (provokes an update).
    shard_3_master.mquery('vt_test_keyspace',
                          "update resharding1 set msg='msg-not-3' where id=3",
                          write=True)
    # Insert row 4 and 5 (provokes a delete).
    self._insert_value(shard_3_master, 'resharding1', 4, 'msg4',
                       0xD000000000000000)
    self._insert_value(shard_3_master, 'resharding1', 5, 'msg5',
                       0xD000000000000000)

    workerclient_proc = utils.run_vtworker_client_bg(
        ['SplitClone',
         '--exclude_tables', 'unrelated',
         '--chunk_count', '10',
         '--min_rows_per_chunk', '1',
         '--min_healthy_rdonly_tablets', '1',
         '--max_tps', '9999',
         'test_keyspace/80-'],
        worker_rpc_port)
    utils.wait_procs([workerclient_proc])
    # Change tablet, which was taken offline, back to rdonly.
    utils.run_vtctl(['ChangeSlaveType', shard_1_rdonly1.tablet_alias,
                     'rdonly'], auto_log=True)
    self.verify_reconciliation_counters(worker_port, 'Online', 'resharding1',
                                        1, 1, 2, 0)
    self.verify_reconciliation_counters(worker_port, 'Offline', 'resharding1',
                                        0, 0, 0, 2)
    # Terminate worker daemon because it is no longer needed.
    utils.kill_sub_process(worker_proc, soft=True)

    # check the startup values are in the right place
    self._check_startup_values()

    # check the schema too
    utils.run_vtctl(['ValidateSchemaKeyspace', '--exclude_tables=unrelated',
                     'test_keyspace'], auto_log=True)

    # Verify vreplication table entries
    result = shard_2_master.mquery('_vt', 'select * from vreplication')
    self.assertEqual(len(result), 1)
    self.assertEqual(result[0][1], 'SplitClone')
    self.assertEqual(result[0][2],
      'keyspace:"test_keyspace" shard:"80-" '
      'key_range:<start:"\\200" end:"\\300" > ')

    result = shard_3_master.mquery('_vt', 'select * from vreplication')
    self.assertEqual(len(result), 1)
    self.assertEqual(result[0][1], 'SplitClone')
    self.assertEqual(result[0][2],
      'keyspace:"test_keyspace" shard:"80-" key_range:<start:"\\300" > ')

    # check the binlog players are running and exporting vars
    self.check_destination_master(shard_2_master, ['test_keyspace/80-'])
    self.check_destination_master(shard_3_master, ['test_keyspace/80-'])
    # When the binlog players/filtered replication is turned on, the query
    # service must be turned off on the destination masters.
    # The tested behavior is a safeguard to prevent that somebody can
    # accidentally modify data on the destination masters while they are not
    # migrated yet and the source shards are still the source of truth.
    shard_2_master.wait_for_vttablet_state('NOT_SERVING')
    shard_3_master.wait_for_vttablet_state('NOT_SERVING')

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
    logging.debug('Executing MultiValue Insert Queries')
    self._exec_multi_shard_dmls()
    logging.debug('Checking 80 percent of data is sent quickly')
    v = self._check_lots_timeout(1000, 80, 5)
    if v != 100:
      # small optimization: only do this check if we don't have all the data
      # already anyway.
      logging.debug('Checking all data goes through eventually')
      self._check_lots_timeout(1000, 100, 20)
    logging.debug('Checking no data was sent the wrong way')
    self._check_lots_not_present(1000)

    logging.debug('Checking MultiValue Insert Queries')
    self._check_multi_shard_values()
    self.check_binlog_player_vars(shard_2_master, ['test_keyspace/80-'],
                                  seconds_behind_master_max=30)
    self.check_binlog_player_vars(shard_3_master, ['test_keyspace/80-'],
                                  seconds_behind_master_max=30)
    self.check_binlog_server_vars(shard_1_slave1, horizontal=True,
                                  min_statements=1000, min_transactions=1000)

    # use vtworker to compare the data (after health-checking the destination
    # rdonly tablets so discovery works)
    utils.run_vtctl(['RunHealthCheck', shard_3_rdonly1.tablet_alias])

    if base_sharding.use_multi_split_diff:
        logging.debug('Running vtworker MultiSplitDiff')
        utils.run_vtworker(['-cell', 'test_nj',
                            '--use_v3_resharding_mode=false',
                            'MultiSplitDiff',
                            '--exclude_tables', 'unrelated',
                            '--min_healthy_rdonly_tablets', '1',
                            'test_keyspace/80-'],
                           auto_log=True)
    else:
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
    if base_sharding.use_rbr:
      # We submitted non-annotated DMLs, that are properly routed
      # with RBR, but not with SBR. So the first shard counts
      # are smaller. In the second shard, we submitted statements
      # that affect more than one keyspace id. These will result
      # in two queries with RBR. So the count there is higher.
      self.check_running_binlog_player(shard_2_master, 4036, 2016)
      self.check_running_binlog_player(shard_3_master, 4056, 2016)
    else:
      self.check_running_binlog_player(shard_2_master, 4044, 2016)
      self.check_running_binlog_player(shard_3_master, 4048, 2016)

    # start a thread to insert data into shard_1 in the background
    # with current time, and monitor the delay
    insert_thread_1 = InsertThread(shard_1_master, 'insert_low', 1, 10000,
                                   0x9000000000000000)
    insert_thread_2 = InsertThread(shard_1_master, 'insert_high', 2, 10001,
                                   0xD000000000000000)
    monitor_thread_1 = MonitorLagThread(shard_2_replica2, 'insert_low', 1)
    monitor_thread_2 = MonitorLagThread(shard_3_replica, 'insert_high', 2)

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
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_srv_keyspace('test_ny', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_tablet_query_service(self, shard_0_ny_rdonly, True, False)
    utils.check_tablet_query_service(self, shard_1_ny_rdonly, True, False)
    utils.check_tablet_query_service(self, shard_1_rdonly1, False, True)

    # rerun migrate to ensure it doesn't fail
    # skip refresh to make it go faster
    utils.run_vtctl(['MigrateServedTypes', '--cells=test_nj',
                     '-skip-refresh-state=true',
                     'test_keyspace/80-', 'rdonly'], auto_log=True)

    # now serve rdonly from the split shards, everywhere
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'rdonly'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_srv_keyspace('test_ny', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_tablet_query_service(self, shard_0_ny_rdonly, True, False)
    utils.check_tablet_query_service(self, shard_1_ny_rdonly, False, True)
    utils.check_tablet_query_service(self, shard_1_rdonly1, False, True)

    # rerun migrate to ensure it doesn't fail
    # skip refresh to make it go faster
    utils.run_vtctl(['MigrateServedTypes', '-skip-refresh-state=true',
                     'test_keyspace/80-', 'rdonly'], auto_log=True)

    # then serve replica from the split shards
    destination_shards = ['test_keyspace/80-c0', 'test_keyspace/c0-']

    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'replica'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-c0 c0-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
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
    utils.check_shard_query_services(self, destination_shards,
                                     topodata_pb2.REPLICA, False)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'replica'],
                    auto_log=True)
    # After a forwards migration, queryservice should be disabled on
    # source and enabled on destinations
    utils.check_tablet_query_service(self, shard_1_slave2, False, True)
    # Destination tablets would have query service disabled for other
    # reasons than the migration, so check the shard record instead of
    # the tablets directly
    utils.check_shard_query_services(self, destination_shards,
                                     topodata_pb2.REPLICA, True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-c0 c0-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')

    # reparent shard_2 to shard_2_replica1, then insert more data and
    # see it flow through still
    utils.run_vtctl(['PlannedReparentShard',
                     '-keyspace_shard', 'test_keyspace/80-c0',
                     '-new_master', shard_2_replica1.tablet_alias])

    # update our test variables to point at the new master
    shard_2_master, shard_2_replica1 = shard_2_replica1, shard_2_master

    logging.debug('Inserting lots of data on source shard after reparenting')
    self._insert_lots(3000, base=2000)
    logging.debug('Checking 80 percent of data was sent fairly quickly')
    self._check_lots_timeout(3000, 80, 10, base=2000)

    # use vtworker to compare the data again
    if base_sharding.use_multi_split_diff:
        logging.debug('Running vtworker MultiSplitDiff')
        utils.run_vtworker(['-cell', 'test_nj',
                            '--use_v3_resharding_mode=false',
                            'MultiSplitDiff',
                            '--exclude_tables', 'unrelated',
                            '--min_healthy_rdonly_tablets', '1',
                            'test_keyspace/80-'],
                           auto_log=True)
    else:
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

    # going to migrate the master now, check the delays
    monitor_thread_1.done = True
    monitor_thread_2.done = True
    insert_thread_1.done = True
    insert_thread_2.done = True
    logging.debug('DELAY 1: %s max_lag=%d ms avg_lag=%d ms',
                  monitor_thread_1.thread_name,
                  monitor_thread_1.max_lag_ms,
                  monitor_thread_1.lag_sum_ms / monitor_thread_1.sample_count)
    logging.debug('DELAY 2: %s max_lag=%d ms avg_lag=%d ms',
                  monitor_thread_2.thread_name,
                  monitor_thread_2.max_lag_ms,
                  monitor_thread_2.lag_sum_ms / monitor_thread_2.sample_count)

    # mock with the SourceShard records to test 'vtctl SourceShardDelete'
    # and 'vtctl SourceShardAdd'
    utils.run_vtctl(['SourceShardDelete', 'test_keyspace/c0-', '1'],
                    auto_log=True)
    utils.run_vtctl(['SourceShardAdd', '--key_range=80-',
                     'test_keyspace/c0-', '1', 'test_keyspace/80-'],
                    auto_log=True)

    # CancelResharding should fail because migration has started.
    utils.run_vtctl(['CancelResharding', 'test_keyspace/80-'],
                    auto_log=True, expect_fail=True)

    # do a Migrate that will fail waiting for replication
    # which should cause the Migrate to be canceled and the source
    # master to be serving again.
    utils.run_vtctl(['MigrateServedTypes',
                     '-filtered_replication_wait_time', '0s',
                     'test_keyspace/80-', 'master'],
                     auto_log=True, expect_fail=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-c0 c0-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_tablet_query_service(self, shard_1_master, True, False)

    # sabotage master migration and make it fail in an unfinished state
    utils.run_vtctl(['SetShardTabletControl', '-blacklisted_tables=t',
                     'test_keyspace/c0-', 'master'], auto_log=True)
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'master'],
                    auto_log=True, expect_fail=True)

    # remove sabotage, but make it fail early. This should not result
    # in the source master serving, because this failure is past the
    # point of no return.
    utils.run_vtctl(['SetShardTabletControl', '-blacklisted_tables=t',
                     '-remove', 'test_keyspace/c0-', 'master'], auto_log=True)
    utils.run_vtctl(['MigrateServedTypes',
                     '-filtered_replication_wait_time', '0s',
                     'test_keyspace/80-', 'master'],
                     auto_log=True, expect_fail=True)
    utils.check_tablet_query_service(self, shard_1_master, False, True)

    # do the migration that's expected to succeed
    utils.run_vtctl(['MigrateServedTypes', 'test_keyspace/80-', 'master'],
                    auto_log=True)
    utils.check_srv_keyspace('test_nj', 'test_keyspace',
                             'Partitions(master): -80 80-c0 c0-\n'
                             'Partitions(rdonly): -80 80-c0 c0-\n'
                             'Partitions(replica): -80 80-c0 c0-\n',
                             keyspace_id_type=base_sharding.keyspace_id_type,
                             sharding_column_name='custom_ksid_col')
    utils.check_tablet_query_service(self, shard_1_master, False, True)

    # check the binlog players are gone now
    self.check_no_binlog_player(shard_2_master)
    self.check_no_binlog_player(shard_3_master)

    # test reverse_replication
    # start with inserting a row in each destination shard
    self._insert_value(shard_2_master, 'resharding2', 2, 'msg2',
                       0x9000000000000000)
    self._insert_value(shard_3_master, 'resharding2', 3, 'msg3',
                       0xD000000000000000)
    # ensure the rows are not present yet
    self._check_value(shard_1_master, 'resharding2', 2, 'msg2',
                      0x9000000000000000, should_be_here=False)
    self._check_value(shard_1_master, 'resharding2', 3, 'msg3',
                      0xD000000000000000, should_be_here=False)
    # repeat the migration with reverse_replication
    utils.run_vtctl(['MigrateServedTypes', '-reverse_replication=true',
                     'test_keyspace/80-', 'master'], auto_log=True)
    # look for the rows in the original master after a short wait
    time.sleep(1.0)
    self._check_value(shard_1_master, 'resharding2', 2, 'msg2',
                      0x9000000000000000)
    self._check_value(shard_1_master, 'resharding2', 3, 'msg3',
                      0xD000000000000000)

    # retry the migration to ensure it now fails
    utils.run_vtctl(['MigrateServedTypes', '-reverse_replication=true',
                     'test_keyspace/80-', 'master'],
                    auto_log=True, expect_fail=True)

    # CancelResharding should now succeed
    utils.run_vtctl(['CancelResharding', 'test_keyspace/80-'], auto_log=True)
    self.check_no_binlog_player(shard_1_master)

    # delete the original tablets in the original shard
    tablet.kill_tablets([shard_1_master, shard_1_slave1, shard_1_slave2,
                         shard_1_ny_rdonly, shard_1_rdonly1])
    for t in [shard_1_slave1, shard_1_slave2, shard_1_ny_rdonly,
              shard_1_rdonly1]:
      utils.run_vtctl(['DeleteTablet', t.tablet_alias], auto_log=True)
    utils.run_vtctl(['DeleteTablet', '-allow_master',
                     shard_1_master.tablet_alias], auto_log=True)

    # rebuild the serving graph, all mentions of the old shards should be gone
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

    # make sure we can't delete the destination shard now that it's serving
    _, stderr = utils.run_vtctl(['DeleteShard', 'test_keyspace/80-c0'],
                                expect_fail=True)
    self.assertIn('is still serving, cannot delete it', stderr)

    # kill everything
    tablet.kill_tablets([shard_0_master, shard_0_replica, shard_0_ny_rdonly,
                         shard_2_master, shard_2_replica1, shard_2_replica2,
                         shard_2_rdonly1,
                         shard_3_master, shard_3_replica, shard_3_rdonly1])

if __name__ == '__main__':
  utils.main()
