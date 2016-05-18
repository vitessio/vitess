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


class ReparentTest(base_end2end_test.BaseEnd2EndTest):

  @classmethod
  def setUpClass(cls):
    super(ReparentTest, cls).setUpClass()

    # number of reparent iterations
    cls.num_reparents = int(cls.test_params.get('num_reparents', '1'))

    # max allowable median master downtime in seconds
    cls.master_downtime_threshold = int(cls.test_params.get(
        'master_downtime_threshold', '20'))

    # seconds to wait for decider to result in a new master
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

  def verify_new_master(self, keyspace, shard_name, desired_master_cell,
                        desired_master_task):
    """Verify that the new master is the correct task in the correct cell.

    This function uses vtctl to call GetShard in order to determine
    that everyone agrees where the master is.

    Args:
      keyspace: Name of the keyspace to reparent (string)
      shard_name: name of the shard to verify (e.g. '-80') (string)
      desired_master_cell: cell where the new master should be (string)
      desired_master_task: task number the new master should be (int)

    Returns:
      True if desired_master is the consensus new master

    """
    # First verify that GetShard shows the correct master
    new_master_name = self.env.get_current_master_name(keyspace, shard_name)
    if self.env.get_tablet_cell(new_master_name) != desired_master_cell:
      return False
    if self.env.get_tablet_task_number(new_master_name) != desired_master_task:
      return False

    return True

  def explicit_reparent(self, keyspace, num_shards, external=False,
                        cross_cell=False):
    """Performs an explicit reparent.

    This function will call decider to explicity select a new master and verify
    that the topology is updated.

    Args:
      keyspace: Name of the keyspace to reparent (string)
      num_shards: Total number of shards (int)
      external: Whether the reparent should be external or through vtctl (bool)
      cross_cell: Whether to reparent to a different cell (bool)

    Returns:
      How long we waited for the serving graph to be updated.
      The time begins just before calling Decider.
      This is a list of floats, one for each shard.
      For cross-cell reparents, it returns [].
    """
    original_masters = []
    next_masters = []
    shard_names = []
    durations = []

    for shard in xrange(num_shards):
      shard_name = utils.get_shard_name(shard, num_shards)
      shard_names.append(shard_name)

      original_master_name = self.env.get_current_master_name(
          keyspace, shard_name)
      original_master = {
          'cell': self.env.get_tablet_cell(original_master_name),
          'task': self.env.get_tablet_task_number(original_master_name),
          }
      original_masters.append(original_master)

      next_master_cell, next_master_task = self.env.get_next_master(
          keyspace, shard_name, cross_cell)
      next_master = {
          'cell': next_master_cell,
          'task': next_master_task,
      }
      next_masters.append(next_master)

      self.env.wait_for_good_failover_status(keyspace, shard_name)

      # Call Reparent in a separate thread.
      def reparent_shard(shard, shard_name, original_master, next_master):
        logging.info('Reparenting %s/%s from %s to %s', keyspace, shard_name,
                     original_master, next_master)
        reparent_fn = (
            self.env.external_reparent if external else
            self.env.internal_reparent)

        return_code, return_output = reparent_fn(
            keyspace, next_master['cell'], shard, num_shards,
            new_task_num=next_master['task'])
        logging.info('Reparent returned %d for %s/%s: %s',
                     return_code, keyspace, shard_name, return_output)

      thread = threading.Thread(target=reparent_shard,
                                args=[shard, shard_name, original_master,
                                      next_master])
      start_time = time.time()
      thread.start()

      if not cross_cell:
        # Wait for the shard to be updated.
        # This doesn't work for cross-cell, because mapping a task
        # number to a tablet UID is more trouble than it's worth.
        uid = (self.env.get_tablet_uid(original_master_name)
               - original_master['task'] + next_master['task'])
        while True:
          if time.time() - start_time > self.reparent_timeout_threshold:
            self.fail('Timed out waiting for serving graph update on %s/%s' % (
                keyspace, shard_name))
          try:
            shard_info = json.loads(self.env.vtctl_helper.execute_vtctl_command(
                ['GetShard', '%s/%s' % (keyspace, shard_name)]))
            if int(shard_info['master_alias']['uid']) == uid:
              duration = time.time() - start_time
              durations.append(duration)
              logging.info('Shard record updated for %s/%s after %f seconds',
                           keyspace, shard_name, duration)
              break
          except (IndexError, KeyError, vtctl_helper.VtctlClientError):
            pass

      thread.join()

    for shard_name, next_master in zip(shard_names, next_masters):
      start_time = time.time()
      while True:
        if time.time() - start_time > self.reparent_timeout_threshold:
          self.fail('%s/%s master was not updated to %s within %d seconds' % (
              keyspace, shard_name, next_master,
              self.reparent_timeout_threshold))
        if self.verify_new_master(
            keyspace, shard_name, next_master['cell'], next_master['task']):
          logging.info('%s/%s\'s new master is %s', keyspace, shard_name,
                       next_master)
          break
        time.sleep(1)

    return durations

  def implicit_reparent(self, keyspace, shard, num_shards):
    """Performs an implicit reparent.

    This function will call borg restart on the current master task and
    verify that decider selected a new task to be the master.

    Args:
      keyspace: Name of the keyspace to reparent (string)
      shard: Numeric ID of the shard to reparent (zero based int)
      num_shards: Total number of shards (int)
    """

    shard_name = utils.get_shard_name(shard, num_shards)

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

  # TODO: re-enable this test after Orchestrator integration
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

  # TODO: re-enable this test after Orchestrator integration
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

  # TODO: re-enable this test after Orchestrator integration
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
