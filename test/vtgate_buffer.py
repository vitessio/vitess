#!/usr/bin/env python
#
# Copyright 2016, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""Test the vtgate master buffer.

During a master failover, vtgate should automatically buffer (stall) requests
for a configured time and retry them after the failover is over.

The test reproduces such a scenario as follows:
- two threads constantly execute a critical read respectively a write (UPDATE)
- vtctl PlannedReparentShard runs a master failover
- both threads should not see any error during despite the failover
"""

import logging
import Queue
import threading
import time

import unittest

import environment
import tablet
import utils

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
    self.wait_for_notification = Queue.Queue(maxsize=1)

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

  def __init__(self, vtgate):
    super(UpdateThread, self).__init__(vtgate, 'UpdateThread', writable=True)
    # Number of executed UPDATE queries.
    self.i = 0
    self.commit_errors = 0

  def execute(self, cursor):
    cursor.begin()
    row_count = cursor.execute('UPDATE buffer SET msg=:msg WHERE id = :id',
                               {'msg': 'update %d' % self.i,
                                'id': UPDATE_ROW_ID})
    try:
      cursor.commit()
    except Exception as e:  # pylint: disable=broad-except
      self.commit_errors += 1
      if self.commit_errors > 1:
        raise
      logging.debug('COMMIT failed. This is okay once because we do not support'
                    ' buffering it. err: %s', str(e))
    self.i += 1
    logging.debug('UPDATE affected %d row(s).', row_count)


class TestBuffer(unittest.TestCase):

  def setUp(self):
    self.master = tablet.Tablet()
    self.replica = tablet.Tablet()
    self.all_tablets = [self.master, self.replica]

    try:
      environment.topo_server().setup()

      setup_procs = [t.init_mysql() for t in self.all_tablets]
      utils.Vtctld().start()
      utils.wait_procs(setup_procs)

      utils.run_vtctl(['CreateKeyspace', KEYSPACE])

      # Start tablets.
      db_name = 'vt_' + KEYSPACE
      for t in self.all_tablets:
        t.create_db(db_name)
      self.master.start_vttablet(wait_for_state=None,
                                 init_tablet_type='replica',
                                 init_keyspace=KEYSPACE, init_shard=SHARD,
                                 tablet_index=0)
      self.replica.start_vttablet(wait_for_state=None,
                                  init_tablet_type='replica',
                                  init_keyspace=KEYSPACE, init_shard=SHARD,
                                  tablet_index=1)
      for t in self.all_tablets:
        t.wait_for_vttablet_state('NOT_SERVING')

      # Reparent to choose an initial master and enable replication.
      utils.run_vtctl(['InitShardMaster', '-force', '%s/%s' % (KEYSPACE, SHARD),
                       self.master.tablet_alias])

      # Create the schema.
      utils.run_vtctl(['ApplySchema', '-sql=' + SCHEMA, KEYSPACE])

      # Start vtgate.
      utils.VtGate().start(extra_args=[
          '-enable_vtgate_buffer',
          # Long timeout in case failover is slow.
          '-vtgate_buffer_window', '10m',
          '-vtgate_buffer_max_failover_duration', '10m',
          '-vtgate_buffer_min_time_between_failovers', '20m'],
                           tablets=self.all_tablets)

      # Insert two rows for the later threads (critical read, update).
      with utils.vtgate.write_transaction(keyspace=KEYSPACE, shards=[SHARD],
                                          tablet_type='master') as tx:
        tx.execute('INSERT INTO buffer (id, msg) VALUES (:id, :msg)',
                   {'id': CRITICAL_READ_ROW_ID, 'msg': 'critical read'})
        tx.execute('INSERT INTO buffer (id, msg) VALUES (:id, :msg)',
                   {'id': UPDATE_ROW_ID, 'msg': 'update'})
    except:
      self.tearDown()
      raise

  def tearDown(self):
    utils.required_teardown()
    if utils.options.skip_teardown:
      return

    teardown_procs = [t.teardown_mysql() for t in self.all_tablets]
    utils.wait_procs(teardown_procs, raise_on_error=False)

    environment.topo_server().teardown()
    utils.kill_sub_processes()
    utils.remove_tmp_files()
    for t in self.all_tablets:
      t.remove_tree()

  def test_buffer(self):
    # Start both threads.
    read_thread = ReadThread(utils.vtgate)
    update_thread = UpdateThread(utils.vtgate)

    # Verify they got at least 2 RPCs through.
    read_thread.set_notify_after_n_successful_rpcs(2)
    update_thread.set_notify_after_n_successful_rpcs(2)
    read_thread.wait_for_notification.get()
    update_thread.wait_for_notification.get()

    # Execute the failover.
    read_thread.set_notify_after_n_successful_rpcs(10)
    update_thread.set_notify_after_n_successful_rpcs(10)
    utils.run_vtctl(['PlannedReparentShard', '-keyspace_shard',
                     '%s/%s' % (KEYSPACE, SHARD),
                     '-new_master', self.replica.tablet_alias])
    read_thread.wait_for_notification.get()
    update_thread.wait_for_notification.get()

    # Stop threads.
    read_thread.stop()
    update_thread.stop()
    read_thread.join()
    update_thread.join()

    # Both threads must not see any error.
    self.assertEqual(0, read_thread.errors)
    self.assertEqual(0, update_thread.errors)
    # At least one thread should have been buffered.
    # TODO(mberlin): This may fail if a failover is too fast. Add retries then.
    v = utils.vtgate.get_vars()
    labels = '%s.%s' % (KEYSPACE, SHARD)
    self.assertGreater(v['BufferRequestsInFlightMax'][labels], 0)
    logging.debug('Failover was buffered for %d milliseconds.',
                  v['BufferFailoverDurationMs'][labels])

if __name__ == '__main__':
  utils.main()
