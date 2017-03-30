#!/usr/bin/env python
"""Tests reparenting is picked up in topology."""

import json
import threading
import time

import numpy

import logging

import base_cluster_test
import vtctl_helper

from vtproto import topodata_pb2
from vttest import sharding_utils


def tearDownModule():
  pass


class ReparentTest(base_cluster_test.BaseClusterTest):

  @classmethod
  def setUpClass(cls):
    super(ReparentTest, cls).setUpClass()

    # number of reparent iterations
    cls.num_reparents = int(cls.test_params.get('num_reparents', '1'))

    # max allowable median master downtime in seconds
    cls.master_downtime_threshold = int(cls.test_params.get(
        'master_downtime_threshold', '20'))

    # seconds to wait for reparent to result in a new master
    cls.reparent_timeout_threshold = int(cls.test_params.get(
        'reparent_timeout_threshold', '60'))

    for keyspace, num_shards in zip(cls.env.keyspaces, cls.env.num_shards):
      for shard in xrange(num_shards):
        shard_name = sharding_utils.get_shard_name(shard, num_shards)
        backup_tablet_uid = cls.env.get_random_tablet(
            keyspace, shard_name, tablet_type='replica')
        logging.info('Taking a backup on tablet %s for %s/%s',
                     backup_tablet_uid, keyspace, shard_name)
        cls.env.vtctl_helper.execute_vtctl_command(
            ['Backup', backup_tablet_uid])

  @classmethod
  def tearDownClass(cls):
    logging.info('Tearing down ReparentTest, verifying tablets are healthy.')
    cls.env.wait_for_healthy_tablets()
    super(ReparentTest, cls).tearDownClass()

  @classmethod
  def create_default_local_environment(cls):
    pass

  def explicit_reparent(self, keyspace, num_shards, external=False,
                        cross_cell=False):
    """Performs an explicit reparent.

    This function will explicitly select a new master and verify that the
    topology is updated.

    Args:
      keyspace: Name of the keyspace to reparent (string)
      num_shards: Total number of shards (int)
      external: Whether the reparent should be external or through vtctl (bool)
      cross_cell: Whether to reparent to a different cell (bool)

    Returns:
      How long we waited for the reparent.
      The time begins just before calling an explicit reparent.
      This is a list of floats, one for each shard.
      For cross-cell reparents, it returns [].
    """
    next_masters = []
    durations = []

    for shard in xrange(num_shards):
      shard_name = sharding_utils.get_shard_name(shard, num_shards)
      original_master = self.env.get_current_master_name(keyspace, shard_name)

      next_master = self.env.get_next_master(keyspace, shard_name, cross_cell)
      next_masters.append(next_master)

      self.env.wait_for_good_failover_status(keyspace, shard_name)

      # Call Reparent in a separate thread.
      def reparent_shard(shard_name, original_master, next_master):
        logging.info('Reparenting %s/%s from %s to %s', keyspace, shard_name,
                     original_master, next_master[2])
        reparent_fn = self.env.external_reparent if external else (
            self.env.internal_reparent)
        return_code, return_output = reparent_fn(
            keyspace, shard_name, next_master[2])
        logging.info('Reparent returned %d for %s/%s: %s',
                     return_code, keyspace, shard_name, return_output)

      thread = threading.Thread(target=reparent_shard,
                                args=[shard_name, original_master, next_master])
      start_time = time.time()
      thread.start()

      # Wait for the reparent.
      while time.time() - start_time < self.reparent_timeout_threshold:
        try:
          tablet_health = json.loads(
              self.env.vtctl_helper.execute_vtctl_command(
                  ['VtTabletStreamHealth', next_master[2]]))
          if tablet_health['target']['tablet_type'] == topodata_pb2.MASTER:
            duration = time.time() - start_time
            durations.append(duration)
            logging.info('Reparent took %f seconds', duration)
            break
        except (IndexError, KeyError, vtctl_helper.VtctlClientError) as e:
          logging.info(
              'While waiting for reparent, got the following error: %s', e)
      else:
        self.fail('Timed out waiting for reparent on %s/%s' % (
            keyspace, shard_name))

      thread.join()

    return durations

  def implicit_reparent(
      self, keyspace, shard, num_shards, perform_emergency_reparent=False):
    """Performs an implicit reparent.

    This function will restart the current master task and verify that a new
    task was selected to be the master.

    Args:
      keyspace: Name of the keyspace to reparent (string)
      shard: Numeric ID of the shard to reparent (zero based int)
      num_shards: Total number of shards (int)
      perform_emergency_reparent: Do an emergency reparent as well (bool)
    """

    shard_name = sharding_utils.get_shard_name(shard, num_shards)

    original_master_name = (
        self.env.get_current_master_name(keyspace, shard_name))

    logging.info('Restarting %s/%s, current master: %s',
                 keyspace, shard_name, original_master_name)
    ret_val = self.env.restart_mysql_task(original_master_name, 'mysql', True)

    self.assertEquals(ret_val, 0,
                      msg='restart failed (returned %d)' % ret_val)

    if perform_emergency_reparent:
      next_master = self.env.get_next_master(keyspace, shard_name)[2]
      logging.info('Emergency reparenting %s/%s to %s', keyspace, shard_name,
                   next_master)
      self.env.internal_reparent(
          keyspace, shard_name, next_master, emergency=True)

    start_time = time.time()
    while time.time() - start_time < self.reparent_timeout_threshold:
      new_master_name = self.env.get_current_master_name(keyspace, shard_name)
      if new_master_name != original_master_name:
        break
      time.sleep(1)
    self.assertNotEquals(
        new_master_name, original_master_name,
        msg='Expected master tablet to change, but it remained as %s' % (
            new_master_name))
    logging.info('restart on %s/%s resulted in new master: %s',
                 keyspace, shard_name, new_master_name)

  def test_implicit_reparent(self):
    logging.info('Performing %s implicit reparents', self.num_reparents)
    if not self.env.automatic_reparent_available():
      logging.info('Automatic reparents are unavailable, skipping test!')
      return
    for attempt in xrange(1, self.num_reparents + 1):
      logging.info('Implicit reparent iteration number %d of %d', attempt,
                   self.num_reparents)
      for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
        for shard in xrange(num_shards):
          self.implicit_reparent(keyspace, shard, num_shards)
      self.env.wait_for_healthy_tablets()

  def _test_explicit_emergency_reparent(self):
    # This test is currently disabled until the emergency reparent can be
    # fleshed out better. If a master tablet is killed and there is no
    # tool performing automatic reparents (like Orchestrator), then there may be
    # a race condition between restarting the tablet (in which it would resume
    # being the master), and the EmergencyReparentShard call. This can sometimes
    # result in two tablets being master.
    logging.info('Performing %s explicit emergency reparents',
                 self.num_reparents)
    if self.env.automatic_reparent_available():
      logging.info('Automatic reparents are available, skipping test!')
      return
    if not self.env.internal_reparent_available():
      logging.info('Internal reparent unavailable, skipping test!')
      return
    for attempt in xrange(1, self.num_reparents + 1):
      logging.info('Explicit emergency reparent iteration number %d of %d',
                   attempt, self.num_reparents)
      for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
        for shard in xrange(num_shards):
          self.implicit_reparent(keyspace, shard, num_shards, True)
      self.env.wait_for_healthy_tablets()

  def test_explicit_reparent(self):
    logging.info('Performing %s explicit reparents', self.num_reparents)
    if not self.env.internal_reparent_available():
      logging.info('Internal reparent unavailable, skipping test!')
      return
    durations = []
    for attempt in xrange(1, self.num_reparents + 1):
      logging.info('Explicit reparent iteration number %d of %d', attempt,
                   self.num_reparents)
      for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
        durations.extend(self.explicit_reparent(keyspace, num_shards))

    durations = numpy.array(durations)
    median_duration = numpy.median(durations)
    logging.info('%d total reparents, median duration %f seconds',
                 len(durations), median_duration)
    self.assertLessEqual(median_duration, self.master_downtime_threshold,
                         'master downtime too high (performance regression)')

  def test_explicit_external_reparent(self):
    logging.info('Performing %s explicit external reparents',
                 self.num_reparents)
    if not self.env.explicit_external_reparent_available():
      logging.info('Explicit external reparent unavailable, skipping test!')
      return
    durations = []
    for attempt in xrange(1, self.num_reparents + 1):
      logging.info('Explicit external reparent iteration number %d of %d',
                   attempt, self.num_reparents)
      for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
        durations.extend(
            self.explicit_reparent(keyspace, num_shards, external=True))

    durations = numpy.array(durations)
    median_duration = numpy.median(durations)
    logging.info('%d total reparents, median duration %f seconds',
                 len(durations), median_duration)
    self.assertLessEqual(median_duration, self.master_downtime_threshold,
                         'master downtime too high (performance regression)')

  def test_explicit_reparent_cross_cell(self):
    if len(self.env.cells) < 2:
      logging.info('Not enough cells to test cross_cell reparents!')
      return
    if not self.env.internal_reparent_available():
      logging.info('Internal reparent unavailable, skipping test!')
      return
    logging.info('Performing %s cross-cell explicit reparents',
                 self.num_reparents)
    for attempt in xrange(1, self.num_reparents + 1):
      logging.info('Cross-cell explicit reparent iteration number %d of %d',
                   attempt, self.num_reparents)
      for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
        self.explicit_reparent(keyspace, num_shards, cross_cell=True)

  def test_explicit_external_reparent_cross_cell(self):
    logging.info('Performing %s cross-cell explicit external reparents',
                 self.num_reparents)
    if len(self.env.cells) < 2:
      logging.info('Not enough cells to test cross_cell reparents!')
      return
    if not self.env.explicit_external_reparent_available():
      logging.info('Explicit external reparent unavailable, skipping test!')
      return
    for attempt in xrange(1, self.num_reparents + 1):
      logging.info('Cross-cell explicit external reparent iteration number %d '
                   'of %d', attempt, self.num_reparents)
      for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
        self.explicit_reparent(
            keyspace, num_shards, external=True, cross_cell=True)


if __name__ == '__main__':
  base_cluster_test.main()

