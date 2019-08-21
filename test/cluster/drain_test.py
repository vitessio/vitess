#!/usr/bin/env python

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

"""Tests that the topology updates when performing drains."""

import json
import os
import random
import time

import logging
import base_cluster_test
from vtdb import keyrange
from vttest import sharding_utils


def tearDownModule():
  pass


class DrainTest(base_cluster_test.BaseClusterTest):

  @classmethod
  def setUpClass(cls):
    super(DrainTest, cls).setUpClass()

    # number of drain iterations
    cls.num_drains = int(cls.test_params.get('num_drains', '10'))

    # seconds to wait for adding/removing a drain to take effect
    cls.drain_timeout_threshold = (
        int(cls.test_params.get('drain_timeout_threshold', '90')))

    cls.num_inserts = int(cls.test_params.get('num_inserts', 100))

  @classmethod
  def tearDownClass(cls):
    for keyspace, num_shards in zip(cls.env.keyspaces, cls.env.num_shards):
      for shard_name in sharding_utils.get_shard_names(num_shards):
        drained_tablets = [x[0] for x in cls.env.get_tablet_types_for_shard(
            keyspace, shard_name) if x[1] == 'drained']
        for tablet in drained_tablets:
          logging.info('Undraining tablet %s', tablet)
          cls.env.undrain_tablet(tablet)

  def insert_rows(self, keyspace, num_rows, starting_index=0):
    logging.info('Inserting %d rows into %s', num_rows, self.table_name)
    master_cell = self.env.get_current_master_cell(keyspace)
    conn = self.env.get_vtgate_conn(master_cell)
    cursor = conn.cursor(tablet_type='master', keyspace=keyspace,
                         keyranges=[keyrange.KeyRange('')], writable=True)

    for i in range(starting_index, starting_index + num_rows):
      cursor.begin()
      cursor.execute(
          'insert into %s (msg, keyspace_id) values (:msg, :keyspace_id)' %
          self.table_name, {'msg': 'test %d' % i, 'keyspace_id': 0})
      cursor.commit()
    cursor.close()
    logging.info('Data insertion complete')

  def read_rows(self, keyspace, num_reads, num_rows, cell,
                tablet_type='replica'):
    logging.info('Reading %d rows from %s', num_reads, self.table_name)

    conn = self.env.get_vtgate_conn(cell)
    cursor = conn.cursor(
        tablet_type=tablet_type, keyspace=keyspace,
        keyranges=[keyrange.KeyRange('')])

    for i in [random.randint(0, num_rows - 1) for _ in range(num_reads)]:
      cursor.execute(
          'select * from %s where msg = "test %d"' % (self.table_name, i), {})
    cursor.close()
    logging.info('Data reads complete')

  def get_row_count(self, tablet_name):
    fetch = self.env.vtctl_helper.execute_vtctl_command(
        ['ExecuteFetchAsDba', '-json', tablet_name,
         'select count(*) from %s' % self.table_name])
    return json.loads(fetch)['rows'][0][0]

  def get_tablet_to_drain(self, keyspace, num_shards):
    # obtain information on all tablets in a keyspace
    keyspace_tablets = self.env.get_all_tablet_types(keyspace, num_shards)

    # filter only the tablets whose type we care about.
    available_tablets = [x for x in keyspace_tablets if x[1] == 'replica']
    self.assertNotEqual(len(available_tablets), 0,
                        'No available tablets found to drain!')

    # choose a tablet randomly and get its name
    tablet_to_drain_info = random.choice(available_tablets)
    return str(tablet_to_drain_info[0])

  def wait_for_drained_tablet(self, tablet_to_drain):
    # Check tablet type until the tablet becomes drained
    drain_added_time = time.time()
    while time.time() - drain_added_time < self.drain_timeout_threshold:
      if self.env.is_tablet_drained(tablet_to_drain):
        logging.info('Tablet %s is now drained (took %f seconds)',
                     tablet_to_drain, time.time() - drain_added_time)
        break
      time.sleep(1)
    else:
      logging.info('Timed out waiting for tablet %s to go drained!',
                   tablet_to_drain)

  def wait_for_undrained_tablet(self, tablet_to_drain):
    # Check tablet type until the tablet is no longer drained
    drain_removed_time = time.time()
    while time.time() - drain_removed_time < self.drain_timeout_threshold:
      if self.env.is_tablet_undrained(tablet_to_drain):
        logging.info('Tablet %s is now undrained (took %f seconds)',
                     tablet_to_drain, time.time() - drain_removed_time)
        break
      time.sleep(1)
    else:
      logging.info('Timed out waiting for tablet %s to be undrained!',
                   tablet_to_drain)

  def setUp(self):
    super(DrainTest, self).setUp()
    os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'cpp'
    os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION_VERSION'] = '2'
    self.table_name = 'vt_insert_test'

    self.env.delete_table(self.table_name)
    self.env.create_table(self.table_name)

  def test_drain(self):
    """Drain tablets and verify topology is updated.

    Each iteration drains a random tablet in each keyspace.  Once it is verified
    that the tablet is unhealthy, the drain is removed and it is verified that
    the tablet returns to a healthy state.
    """
    logging.info('Performing %s drain cycles', self.num_drains)
    for attempt in range(self.num_drains):
      logging.info('Drain iteration %d of %d', attempt + 1, self.num_drains)
      for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
        tablet_to_drain = self.get_tablet_to_drain(keyspace, num_shards)
        num_rows0 = self.get_row_count(tablet_to_drain)
        self.env.drain_tablet(tablet_to_drain)
        self.wait_for_drained_tablet(tablet_to_drain)
        self.insert_rows(keyspace, self.num_inserts,
                         starting_index=self.num_inserts * attempt)
        num_rows1 = self.get_row_count(tablet_to_drain)
        self.assertEqual(num_rows0, num_rows1)
        query_count0 = self.env.get_tablet_query_total_count(tablet_to_drain)
        self.read_rows(
            keyspace, self.num_inserts, self.num_inserts * (attempt + 1),
            self.env.get_tablet_cell(tablet_to_drain))
        self.env.undrain_tablet(tablet_to_drain)
        self.wait_for_undrained_tablet(tablet_to_drain)
        query_count1 = self.env.get_tablet_query_total_count(tablet_to_drain)
        logging.info('%s total query count before/after reads: %d/%d',
                     tablet_to_drain, query_count0, query_count1)
        self.assertEqual(query_count0, query_count1)


if __name__ == '__main__':
  base_cluster_test.main()

