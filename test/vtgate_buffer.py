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

"""Test the vtgate master buffer.

During a master failover, vtgate should automatically buffer (stall) requests
for a configured time and retry them after the failover is over.

The test reproduces such a scenario as follows:
- two threads constantly execute a critical read respectively a write (UPDATE)
- vtctl PlannedReparentShard runs a master failover
- both threads should not see any error during despite the failover
"""

import logging
import queue
import random
import threading
import time

import unittest

import environment
import tablet
import utils
from mysql_flavor import mysql_flavor

KEYSPACE = 'ks1'
SHARD = '0'
SCHEMA = '''CREATE TABLE buffer(
  id BIGINT NOT NULL,
  msg VARCHAR(64) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB'''
CRITICAL_READ_ROW_ID = 1
UPDATE_ROW_ID = 2


class AbstractVtgateThread(threading.Thread):
  """Thread which constantly executes a query on vtgate.

  Implement the execute() method for the specific query.
  """

  def __init__(self, vtgate, name, writable=False):
    super(AbstractVtgateThread, self).__init__(name=name)
    self.vtgate = vtgate
    self.writable = writable
    self.quit = False

    # Number of queries successfully executed.
    self.rpcs = 0
    # Number of failed queries.
    self.errors = 0

    # Queue used to notify the main thread that this thread executed
    # "self.notify_after_n_successful_rpcs" RPCs successfully.
    # Then "True" will be put exactly once on the queue.
    self.wait_for_notification = queue.Queue(maxsize=1)

    # notify_lock guards the two fields below.
    self.notify_lock = threading.Lock()
    # If 0, notifications are disabled.
    self.notify_after_n_successful_rpcs = 0
    # Number of RPCs at the time a notification was requested.
    self.rpcs_so_far = 0

    self.start()

  def run(self):
    with self.vtgate.create_connection() as conn:
      c = conn.cursor(keyspace=KEYSPACE, shards=[SHARD], tablet_type='master',
                      writable=self.writable)
      while not self.quit:
        try:
          self.execute(c)

          self.rpcs += 1
          # If notifications are requested, check if we already executed the
          # required number of successful RPCs.
          # Use >= instead of == because we can miss the exact point due to
          # slow thread scheduling.
          with self.notify_lock:
            if (self.notify_after_n_successful_rpcs != 0 and
                self.rpcs >= (self.notify_after_n_successful_rpcs +
                              self.rpcs_so_far)):
              self.wait_for_notification.put(True)
              self.notify_after_n_successful_rpcs = 0
        except Exception as e:  # pylint: disable=broad-except
          self.errors += 1
          logging.debug('thread: %s query failed: %s', self.name, str(e))

        # Wait 10ms seconds between two attempts.
        time.sleep(0.01)

  def execute(self, cursor):
    raise NotImplementedError('Child class needs to implement this')

  def set_notify_after_n_successful_rpcs(self, n):
    with self.notify_lock:
      self.notify_after_n_successful_rpcs = n
      self.rpcs_so_far = self.rpcs

  def stop(self):
    self.quit = True


class ReadThread(AbstractVtgateThread):

  def __init__(self, vtgate):
    super(ReadThread, self).__init__(vtgate, 'ReadThread')

  def execute(self, cursor):
    row_count = cursor.execute('SELECT * FROM buffer WHERE id = :id',
                               {'id': CRITICAL_READ_ROW_ID})
    logging.debug('read returned   %d row(s).', row_count)


class UpdateThread(AbstractVtgateThread):

  def __init__(self, vtgate, ignore_error_func=None):
    self.ignore_error_func = ignore_error_func
    # Value used in next UPDATE query. Increased after every query.
    self.i = 1
    self._commit_errors = 0
    super(UpdateThread, self).__init__(vtgate, 'UpdateThread', writable=True)

  def execute(self, cursor):
    attempt = self.i
    self.i += 1
    try:
      commit_started = False
      cursor.begin()
      # Do not use a bind variable for "msg" to make sure that the value shows
      # up in the logs.
      row_count = cursor.execute('UPDATE buffer SET msg=\'update %d\' '
                                 'WHERE id = :id' % attempt,
                                 {'id': UPDATE_ROW_ID})

      # Sleep between [0, 1] seconds to prolong the time the transaction is in
      # flight. This is more realistic because applications are going to keep
      # their transactions open for longer as well.
      time.sleep(random.randint(0, 1000) / 1000.0)

      commit_started = True
      cursor.commit()
      logging.debug('UPDATE %d affected %d row(s).', attempt, row_count)
    except Exception as e:  # pylint: disable=broad-except
      try:
        # Rollback to free the transaction in vttablet.
        cursor.rollback()
      except Exception as e:  # pylint: disable=broad-except
        logging.warn('rollback failed: %s', str(e))

      if not commit_started:
        logging.debug('UPDATE %d failed before COMMIT. This should not happen.'
                      ' Re-raising exception.', attempt)
        raise

      if self.ignore_error_func and self.ignore_error_func(e):
        logging.debug('UPDATE %d failed during COMMIT. But we cannot buffer'
                      ' this error and ignore it. err: %s', attempt, str(e))
      else:
        self._commit_errors += 1
        if self._commit_errors > 1:
          raise
        logging.debug('UPDATE %d failed during COMMIT. This is okay once'
                      ' because we do not support buffering it. err: %s',
                      attempt, str(e))

  def commit_errors(self):
    return self._commit_errors

master = tablet.Tablet()
replica = tablet.Tablet()
all_tablets = [master, replica]


def setUpModule():
  try:
    environment.topo_server().setup()

    setup_procs = [t.init_mysql() for t in all_tablets]
    utils.Vtctld().start()
    utils.wait_procs(setup_procs)

    utils.run_vtctl(['CreateKeyspace', KEYSPACE])

    # Start tablets.
    db_name = 'vt_' + KEYSPACE
    for t in all_tablets:
      t.create_db(db_name)
    master.start_vttablet(wait_for_state=None,
                          init_tablet_type='replica',
                          init_keyspace=KEYSPACE, init_shard=SHARD,
                          tablet_index=0)
    replica.start_vttablet(wait_for_state=None,
                           init_tablet_type='replica',
                           init_keyspace=KEYSPACE, init_shard=SHARD,
                           tablet_index=1)
    for t in all_tablets:
      t.wait_for_vttablet_state('NOT_SERVING')

    # Reparent to choose an initial master and enable replication.
    utils.run_vtctl(['InitShardMaster', '-force', '%s/%s' % (KEYSPACE, SHARD),
                     master.tablet_alias])

    # Create the schema.
    utils.run_vtctl(['ApplySchema', '-sql=' + SCHEMA, KEYSPACE])

    start_vtgate()

    # Insert two rows for the later threads (critical read, update).
    with utils.vtgate.write_transaction(keyspace=KEYSPACE, shards=[SHARD],
                                        tablet_type='master') as tx:
      tx.execute('INSERT INTO buffer (id, msg) VALUES (:id, :msg)',
                 {'id': CRITICAL_READ_ROW_ID, 'msg': 'critical read'})
      tx.execute('INSERT INTO buffer (id, msg) VALUES (:id, :msg)',
                 {'id': UPDATE_ROW_ID, 'msg': 'update'})
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  teardown_procs = [t.teardown_mysql() for t in [master, replica]]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  for t in all_tablets:
    t.remove_tree()


def start_vtgate():
  utils.VtGate().start(extra_args=[
      '-enable_buffer',
      # Long timeout in case failover is slow.
      '-buffer_window', '10m',
      '-buffer_max_failover_duration', '10m',
      '-buffer_min_time_between_failovers', '20m'],
                       tablets=all_tablets)


class TestBufferBase(unittest.TestCase):

  def _test_buffer(self, reparent_func, enable_read_thread=True,
                   ignore_error_func=None):
    # Start both threads.
    if enable_read_thread:
      read_thread = ReadThread(utils.vtgate)
    else:
      logging.debug('ReadThread explicitly disabled in this test.')
    update_thread = UpdateThread(utils.vtgate, ignore_error_func)

    try:
      # Verify they got at least 2 RPCs through.
      if enable_read_thread:
        read_thread.set_notify_after_n_successful_rpcs(2)
      update_thread.set_notify_after_n_successful_rpcs(2)
      if enable_read_thread:
        read_thread.wait_for_notification.get()
      update_thread.wait_for_notification.get()

      # Execute the failover.
      if enable_read_thread:
        read_thread.set_notify_after_n_successful_rpcs(10)
      update_thread.set_notify_after_n_successful_rpcs(10)

      reparent_func()

      # Failover is done. Swap master and replica for the next test.
      global master, replica
      master, replica = replica, master

      if enable_read_thread:
        read_thread.wait_for_notification.get()
      update_thread.wait_for_notification.get()
    except:
      # Something went wrong. Kill vtgate first to unblock any buffered requests
      # which would further block the two threads.
      utils.vtgate.kill()
      raise
    finally:
      # Stop threads.
      if enable_read_thread:
        read_thread.stop()
      update_thread.stop()
      if enable_read_thread:
        read_thread.join()
      update_thread.join()

    # Both threads must not see any error.
    if enable_read_thread:
      self.assertEqual(0, read_thread.errors)
    self.assertEqual(0, update_thread.errors)
    # At least one thread should have been buffered.
    # TODO(mberlin): This may fail if a failover is too fast. Add retries then.
    v = utils.vtgate.get_vars()
    labels = '%s.%s' % (KEYSPACE, SHARD)
    in_flight_max = v['BufferLastRequestsInFlightMax'].get(labels, 0)
    if in_flight_max == 0:
      # Missed buffering is okay when we observed the failover during the
      # COMMIT (which cannot trigger the buffering).
      self.assertGreater(update_thread.commit_errors(), 0,
                         'No buffering took place and the update thread saw no'
                         ' error during COMMIT. But one of it must happen.')
    else:
      self.assertGreater(in_flight_max, 0)

    # There was a failover and the HealthCheck module must have seen it.
    master_promoted_count = v['HealthcheckMasterPromoted'].get(labels, 0)
    self.assertGreater(master_promoted_count, 0)

    duration_ms = v['BufferFailoverDurationSumMs'].get(labels, 0)
    if duration_ms > 0:
      # Buffering was actually started.
      logging.debug('Failover was buffered for %d milliseconds.', duration_ms)
      # Number of buffering stops must be equal to the number of seen failovers.
      buffering_stops = v['BufferStops'].get('%s.NewMasterSeen' % labels, 0)
      self.assertEqual(master_promoted_count, buffering_stops)

  def external_reparent(self):
    # Demote master.
    start = time.time()
    master.mquery('', mysql_flavor().demote_master_commands(), log_query=True)
    if master.semi_sync_enabled():
      master.set_semi_sync_enabled(master=False)

    # Wait for replica to catch up to master.
    utils.wait_for_replication_pos(master, replica)

    # Wait for at least one second to artificially prolong the failover and give
    # the buffer a chance to observe it.
    d = time.time() - start
    min_unavailability_s = 1
    if d < min_unavailability_s:
      w = min_unavailability_s - d
      logging.debug('Waiting for %.1f seconds because the failover was too fast'
                    ' (took only %.3f seconds)', w, d)
      time.sleep(w)

    # Promote replica to new master.
    replica.mquery('', mysql_flavor().promote_slave_commands(),
                   log_query=True)
    if replica.semi_sync_enabled():
      replica.set_semi_sync_enabled(master=True)
    old_master = master
    new_master = replica

    # Configure old master to use new master.
    new_pos = mysql_flavor().master_position(new_master)
    logging.debug('New master position: %s', str(new_pos))
    # Use 'localhost' as hostname because Travis CI worker hostnames
    # are too long for MySQL replication.
    change_master_cmds = mysql_flavor().change_master_commands(
        'localhost', new_master.mysql_port, new_pos)
    old_master.mquery('', ['RESET SLAVE'] + change_master_cmds +
                      ['START SLAVE'], log_query=True)

    # Notify the new vttablet master about the reparent.
    utils.run_vtctl(['TabletExternallyReparented', new_master.tablet_alias],
                    auto_log=True)


class TestBuffer(TestBufferBase):

  def setUp(self):
    utils.vtgate.kill()
    # Restart vtgate between each test or the feature
    # --buffer_min_time_between_failovers
    # will ignore subsequent failovers.
    start_vtgate()

  def test_buffer_planned_reparent(self):
    def planned_reparent():
      utils.run_vtctl(['PlannedReparentShard', '-keyspace_shard',
                       '%s/%s' % (KEYSPACE, SHARD),
                       '-new_master', replica.tablet_alias])
    self._test_buffer(planned_reparent)

  def test_buffer_external_reparent(self):
    self._test_buffer(self.external_reparent)

if __name__ == '__main__':
  utils.main()
