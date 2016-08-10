#!/usr/bin/env python
"""Tests reparenting is picked up in topology."""

import json
import threading
import time

import numpy

import logging

import base_end2end_test
import utils
import vtctl_helper

from vtproto import topodata_pb2
from vttest import sharding_utils


def setUpModule():
  pass


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return
  utils.kill_sub_processes()
  utils.remove_tmp_files()


class ReparentTest(base_end2end_test.BaseEnd2EndTest):

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
        'reparent_timeout_threshold', '300'))

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
      def reparent_shard(shard, shard_name, original_master, next_master):
        logging.info('Reparenting %s/%s from %s to %s', keyspace, shard_name,
                     original_master, next_master[2])
        if external:
          return_code, return_output = self.env.external_reparent(
              keyspace, next_master[0], shard, new_task_num=next_master[1])
        else:
          return_code, return_output = self.env.internal_reparent(
              keyspace, shard_name, next_master[2])
        logging.info('Reparent returned %d for %s/%s: %s',
                     return_code, keyspace, shard_name, return_output)

      thread = threading.Thread(target=reparent_shard,
                                args=[shard, shard_name, original_master,
                                      next_master])
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
        except (IndexError, KeyError, vtctl_helper.VtctlClientError):
          pass
      else:
        self.fail('Timed out waiting for reparent on %s/%s' % (
            keyspace, shard_name))

      thread.join()

    return durations

  def implicit_reparent(self, keyspace, shard, num_shards):
    """Performs an implicit reparent.

    This function will call borg restart on the current master task and
    verify that a new task was selected to be the master.

    Args:
      keyspace: Name of the keyspace to reparent (string)
      shard: Numeric ID of the shard to reparent (zero based int)
      num_shards: Total number of shards (int)
    """

    shard_name = sharding_utils.get_shard_name(shard, num_shards)

    original_master_name = (
        self.env.get_current_master_name(keyspace, shard_name))
    original_master_cell = self.env.get_tablet_cell(original_master_name)
    master_task_num = self.env.get_tablet_task_number(original_master_name)

    logging.info('Restarting %s/%s, current master: %s, task: %d',
                 keyspace, shard_name, original_master_name, master_task_num)
    ret_val = self.env.restart_mysql_task(
        original_master_cell, keyspace, shard, master_task_num, 'replica',
        'mysql-alloc', True)

    self.assertEquals(ret_val, 0,
                      msg='restartalloc failed (returned %d)' % ret_val)

    start_time = time.time()
    while time.time() - start_time < self.reparent_timeout_threshold:
      new_master_name = self.env.get_current_master_name(keyspace, shard_name)
      new_master_task_num = self.env.get_tablet_task_number(new_master_name)
      if new_master_name != original_master_name:
        break
      time.sleep(1)
    self.assertNotEquals(
        new_master_name, original_master_name,
        msg='Expected master tablet to change, but it remained as %s' % (
            new_master_name))
    logging.info('restartalloc on %s/%s resulted in new master: %s, task: %d',
                 keyspace, shard_name, new_master_name, new_master_task_num)

  # TODO(thompsonja): re-enable this test after Orchestrator integration
  def _test_implicit_reparent(self):
    logging.info('Performing %s implicit reparents', self.num_reparents)
    for attempt in xrange(1, self.num_reparents + 1):
      logging.info('Implicit reparent iteration number %d of %d', attempt,
                   self.num_reparents)
      for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
        for shard in xrange(num_shards):
          self.implicit_reparent(keyspace, shard, num_shards)
      self.env.wait_for_healthy_tablets()

  def test_explicit_reparent(self):
    logging.info('Performing %s explicit reparents', self.num_reparents)
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

  # TODO(thompsonja): re-enable this test after Orchestrator integration
  def _test_explicit_external_reparent(self):
    logging.info('Performing %s explicit external reparents',
                 self.num_reparents)
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
    logging.info('Performing %s cross-cell explicit reparents',
                 self.num_reparents)
    for attempt in xrange(1, self.num_reparents + 1):
      logging.info('Cross-cell explicit reparent iteration number %d of %d',
                   attempt, self.num_reparents)
      for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
        self.explicit_reparent(keyspace, num_shards, cross_cell=True)

  # TODO(thompsonja): re-enable this test after Orchestrator integration
  def _test_explicit_external_reparent_cross_cell(self):
    if len(self.env.cells) < 2:
      logging.info('Not enough cells to test cross_cell reparents!')
      return
    logging.info('Performing %s cross-cell explicit external reparents',
                 self.num_reparents)
    for attempt in xrange(1, self.num_reparents + 1):
      logging.info('Cross-cell explicit external reparent iteration number %d '
                   'of %d', attempt, self.num_reparents)
      for keyspace, num_shards in zip(self.env.keyspaces, self.env.num_shards):
        self.explicit_reparent(
            keyspace, num_shards, external=True, cross_cell=True)


if __name__ == '__main__':
  base_end2end_test.main()
