#!/usr/bin/env python
"""Tests backup/restore when a tablet is restarted."""

import json
import os
import random
import time

import logging

from vtdb import keyrange
from vttest import sharding_utils
import base_cluster_test


def tearDownModule():
  pass


class BackupTest(base_cluster_test.BaseClusterTest):

  @classmethod
  def setUpClass(cls):
    super(BackupTest, cls).setUpClass()

    # number of backup iterations
    cls.num_backups = int(cls.test_params.get('num_backups', '1'))

    # number of insert statements
    cls.num_inserts = int(cls.test_params.get('num_inserts', '100'))

  def setUp(self):
    """Updates schema, adding a table and populating it with data."""
    super(BackupTest, self).setUp()
    os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'cpp'
    os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION_VERSION'] = '2'
    self.table_name = 'vt_insert_test'

    self.env.delete_table(self.table_name)
    self.env.create_table(self.table_name, validate_deadline_s=120)

    for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
      for shard_name in sharding_utils.get_shard_names(num_shards):
        logging.info('Inserting %d rows into %s',
                     self.num_inserts, self.table_name)
        kr = keyrange.KeyRange('') if num_shards == 1 else keyrange.KeyRange(
            shard_name)

        master_tablet = self.env.get_current_master_name(keyspace, shard_name)
        master_cell = self.env.get_tablet_cell(master_tablet)
        conn = self.env.get_vtgate_conn(master_cell)
        cursor = conn.cursor(tablet_type='master', keyspace=keyspace,
                             keyranges=[kr], writable=True)
        for i in xrange(self.num_inserts):
          cursor.begin()
          cursor.execute(
              'insert into %s (msg, keyspace_id) values (:msg, :keyspace_id)' %
              self.table_name, {'msg': 'test %d' % i, 'keyspace_id': 0})
          cursor.commit()
        cursor.close()
        logging.info('Data insertion complete')

  def tearDown(self):
    self.env.delete_table(self.table_name)
    super(BackupTest, self).tearDown()

  def perform_backup(self, tablets):
    """Backup specific tablets.

    Args:
      tablets: List of tablet names to be backed up
    """
    for tablet in tablets:
      self.env.backup(tablet)

  def perform_restore(self, tablets, num_shards):
    """Restore tablets by restarting the alloc.

    Args:
      tablets: List of tablet names to be restored
      num_shards: Number of shards for the specific keyspace (int)
    """
    # First call restart on all tablets being restored
    for tablet in tablets:
      self.env.restart_mysql_task(tablet, 'mysql', is_alloc=True)

    # Wait for the tablets to be unhealthy
    start_time = time.time()
    unhealthy_tablets = []
    while time.time() - start_time < 120:
      if len(unhealthy_tablets) == len(tablets):
        break
      for tablet_name in tablets:
        if tablet_name in unhealthy_tablets:
          continue
        logging.info('Waiting for tablet %s to be unhealthy', tablet_name)
        if not self.env.is_tablet_healthy(tablet_name):
          unhealthy_tablets.append(tablet_name)
          logging.info('Tablet %s is now unhealthy', tablet_name)
      time.sleep(1)
    else:
      logging.info('Timed out, some tablets did not become unhealthy: %s',
                   ' '.join([x for x in tablets if x not in unhealthy_tablets]))

    # Wait for the tablet to be healthy according to vttablet health
    start_time = time.time()
    healthy_tablets = []
    while time.time() - start_time < 600:
      if len(healthy_tablets) == len(tablets):
        break
      for tablet_name in tablets:
        if tablet_name in healthy_tablets:
          continue
        logging.info('Waiting for tablet %s to be healthy', tablet_name)
        if self.env.is_tablet_healthy(tablet_name):
          healthy_tablets.append(tablet_name)
          logging.info('Tablet %s is now healthy', tablet_name)
      time.sleep(1)
    else:
      logging.info('Timed out, some tablets are still unhealthy: %s',
                   ' '.join([x for x in tablets if x not in healthy_tablets]))

    for tablet_name in tablets:
      logging.info('Waiting for tablet %s to enter serving state', tablet_name)
      self.env.poll_for_varz(
          tablet_name, ['TabletStateName'], timeout=300.0,
          condition_fn=lambda v: v['TabletStateName'] == 'SERVING',
          condition_msg='TabletStateName == Serving')
      logging.info('Done')
      count = json.loads(self.env.vtctl_helper.execute_vtctl_command(
          ['ExecuteFetchAsDba', '-json', tablet_name,
           'select * from %s' % self.table_name]))['rows_affected']
      logging.info('Select count: %d', count)
      self.assertEquals(count, self.num_inserts)

  def test_backup(self):
    logging.info('Performing %s backup cycles', self.num_backups)
    for attempt in xrange(self.num_backups):
      logging.info('Backup iteration %d of %d', attempt + 1, self.num_backups)
      for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
        backup_tablets = []
        for shard in xrange(num_shards):
          # Pick a random replica tablet in each shard
          tablets = self.env.get_tablet_types_for_shard(
              keyspace, sharding_utils.get_shard_name(shard, num_shards))
          available_tablets = [x for x in tablets if x[1] == 'replica']
          self.assertTrue(
              len(available_tablets), 'No available tablets found to backup!')
          tablet_to_backup_name = random.choice(available_tablets)[0]
          backup_tablets.append(tablet_to_backup_name)

        self.perform_backup(backup_tablets)
        self.perform_restore(backup_tablets, num_shards)


if __name__ == '__main__':
  base_cluster_test.main()

