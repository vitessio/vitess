#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""Tests the robustness and resiliency of vtworkers."""

import logging
import unittest
import urllib
import urllib2
from collections import namedtuple

from vtdb import keyrange_constants

import environment
import utils
import tablet


KEYSPACE_ID_TYPE = keyrange_constants.KIT_UINT64


class ShardTablets(namedtuple('ShardTablets', 'master replicas rdonlys')):
  """ShardTablets is a container for all the tablet.Tablets of a shard.

  `master` should be a single Tablet, while `replicas` and `rdonlys` should be
  lists of Tablets of the appropriate types.
  """

  @property
  def all_tablets(self):
    """Returns a list of all the tablets of the shard.

    Does not guarantee any ordering on the returned tablets.
    """
    return [self.master] + self.replicas + self.rdonlys

  @property
  def replica(self):
    """Returns the first replica Tablet instance for the shard, or None."""
    if self.replicas:
      return self.replicas[0]
    else:
      return None

  @property
  def rdonly(self):
    """Returns the first replica Tablet instance for the shard, or None."""
    if self.rdonlys:
      return self.rdonlys[0]
    else:
      return None

# initial shard, covers everything
shard_master = tablet.Tablet()
shard_replica = tablet.Tablet()
shard_rdonly1 = tablet.Tablet()

# split shards
# range "" - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_rdonly1 = tablet.Tablet()
# range 80 - ""
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()
shard_1_rdonly1 = tablet.Tablet()

shard_tablets = ShardTablets(shard_master, [shard_replica], [shard_rdonly1])
shard_0_tablets = ShardTablets(shard_0_master, [shard_0_replica], [shard_0_rdonly1])
shard_1_tablets = ShardTablets(shard_1_master, [shard_1_replica], [shard_1_rdonly1])


def init_keyspace():
  """Creates a `test_keyspace` keyspace with a sharding key."""
  utils.run_vtctl(['CreateKeyspace', '-sharding_column_name', 'keyspace_id',
    '-sharding_column_type', KEYSPACE_ID_TYPE,'test_keyspace'])


def setUpModule():
  try:
    environment.topo_server().setup()

    setup_procs = [
        shard_master.init_mysql(),
        shard_replica.init_mysql(),
        shard_rdonly1.init_mysql(),
        shard_0_master.init_mysql(),
        shard_0_replica.init_mysql(),
        shard_0_rdonly1.init_mysql(),
        shard_1_master.init_mysql(),
        shard_1_replica.init_mysql(),
        shard_1_rdonly1.init_mysql(),
        ]
    utils.wait_procs(setup_procs)
    init_keyspace()
  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      shard_master.teardown_mysql(),
      shard_replica.teardown_mysql(),
      shard_rdonly1.teardown_mysql(),
      shard_0_master.teardown_mysql(),
      shard_0_replica.teardown_mysql(),
      shard_0_rdonly1.teardown_mysql(),
      shard_1_master.teardown_mysql(),
      shard_1_replica.teardown_mysql(),
      shard_1_rdonly1.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_master.remove_tree()
  shard_replica.remove_tree()
  shard_rdonly1.remove_tree()
  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_0_rdonly1.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica.remove_tree()
  shard_1_rdonly1.remove_tree()

class TestBaseSplitClone(unittest.TestCase):
  """Abstract test base class for testing the SplitClone worker."""

  def run_shard_tablets(self, shard_name, shard_tablets, create_db=True, create_table=True, wait_state='SERVING'):
    """Handles all the necessary work for initially running a shard's tablets.

    This encompasses the following steps:
      1. (optional) Create db
      2. Starting vttablets and let themselves init them
      3. Waiting for the appropriate vttablet state
      4. Force reparent to the master tablet
      5. RebuildKeyspaceGraph
      7. (optional) Running initial schema setup

    Args:
      shard_name - the name of the shard to start tablets in
      shard_tablets - an instance of ShardTablets for the given shard
      wait_state - string, the vttablet state that we should wait for
      create_db - boolean, True iff we should create a db on the tablets
      create_table - boolean, True iff we should create a table on the tablets
    """
    # If requested, create databases.
    for tablet in shard_tablets.all_tablets:
      if create_db:
        tablet.create_db('vt_test_keyspace')

    # Start tablets.
    # Specifying "target_tablet_type" enables the health check i.e. tablets will
    # be automatically returned to the serving graph after a SplitClone or SplitDiff.
    # NOTE: The future master has to be started with type "replica".
    shard_tablets.master.start_vttablet(wait_for_state=None, target_tablet_type='replica',
                                        init_keyspace='test_keyspace', init_shard=shard_name)
    for tablet in shard_tablets.replicas:
      tablet.start_vttablet(wait_for_state=None, target_tablet_type='replica',
                            init_keyspace='test_keyspace', init_shard=shard_name)
    for tablet in shard_tablets.rdonlys:
      tablet.start_vttablet(wait_for_state=None, target_tablet_type='rdonly',
                            init_keyspace='test_keyspace', init_shard=shard_name)
    # Block until tablets are up and we can enable replication.
    # We don't care about the tablets' state which may have been changed by the
    # health check from SERVING to NOT_SERVING anyway.
    for tablet in shard_tablets.all_tablets:
      tablet.wait_for_vttablet_state('NOT_SERVING')

    # Reparent to choose an initial master and enable replication.
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/%s' % shard_name,
                     shard_tablets.master.tablet_alias], auto_log=True)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    # Enforce a health check instead of waiting for the next periodic one.
    # (saves up to 1 second execution time on average)
    if wait_state == 'SERVING':
      for tablet in shard_tablets.replicas:
        utils.run_vtctl(["RunHealthCheck", tablet.tablet_alias, "replica"])
      for tablet in shard_tablets.rdonlys:
        utils.run_vtctl(["RunHealthCheck", tablet.tablet_alias, "rdonly"])

    # Wait for tablet state to change after starting all tablets. This allows
    # us to start all tablets at once, instead of sequentially waiting.
    # NOTE: Replication has to be enabled first or the health check will
    #       set a a replica or rdonly tablet back to NOT_SERVING. 
    for tablet in shard_tablets.all_tablets:
      tablet.wait_for_vttablet_state(wait_state)

    create_table_sql = (
      'create table worker_test('
      'id bigint unsigned,'
      'msg varchar(64),'
      'keyspace_id bigint(20) unsigned not null,'
      'primary key (id),'
      'index by_msg (msg)'
      ') Engine=InnoDB'
    )

    if create_table:
      utils.run_vtctl(['ApplySchema',
                       '-sql=' + create_table_sql,
                       'test_keyspace'],
                      auto_log=True)

  def copy_schema_to_destination_shards(self):
    for keyspace_shard in ('test_keyspace/-80', 'test_keyspace/80-'):
      utils.run_vtctl(['CopySchemaShard',
                       '--exclude_tables', 'unrelated',
                       shard_rdonly1.tablet_alias,
                       keyspace_shard],
                      auto_log=True)

  def _insert_values(self, tablet, id_offset, msg, keyspace_id, num_values):
    """Inserts values in the MySQL database along with the required routing comments.

    Args:
      tablet - the Tablet instance to insert into
      id - the value of `id` column
      msg - the value of `msg` column
      keyspace_id - the value of `keyspace_id` column
    """
    k = "%u" % keyspace_id
    values_str = ''
    for i in xrange(num_values):
      if i != 0:
        values_str += ','
      values_str += '(%u, "%s", 0x%x)' % (id_offset + i, msg, keyspace_id)
    tablet.mquery('vt_test_keyspace', [
        'begin',
        'insert into worker_test(id, msg, keyspace_id) values%s /* EMD keyspace_id:%s*/' % (values_str, k),
        'commit'
        ], write=True)

  def insert_values(self, tablet, num_values, num_shards, offset=0, keyspace_id_range=2**64):
    """Inserts simple values, one for each potential shard.

    Each row is given a message that contains the shard number, so we can easily
    verify that the source and destination shards have the same data.

    Args:
      tablet - the Tablet instance to insert into
      num_values - the number of values to insert
      num_shards - the number of shards that we expect to have
      offset - amount that we should offset the `id`s by. This is useful for
        inserting values multiple times.
      keyspace_id_range - the number of distinct values that the keyspace id can have
    """
    shard_width = keyspace_id_range / num_shards
    shard_offsets = [i * shard_width for i in xrange(num_shards)]
    for shard_num in xrange(num_shards):
        self._insert_values(tablet,
                            shard_offsets[shard_num] + offset,
                            'msg-shard-%u' % shard_num,
                            shard_offsets[shard_num],
                            num_values)

  def assert_shard_data_equal(self, shard_num, source_tablet, destination_tablet):
    """Asserts that a shard's data is identical on source and destination tablets.

    Args:
      shard_num - the shard number of the shard that we want to verify the data of
      source_tablet - Tablet instance of the source shard
      destination_tablet - Tablet instance of the destination shard
    """
    select_query = 'select * from worker_test where msg="msg-shard-%s" order by id asc' % shard_num

    # Make sure all the right rows made it from the source to the destination
    source_rows = source_tablet.mquery('vt_test_keyspace', select_query)
    destination_rows = destination_tablet.mquery('vt_test_keyspace', select_query)
    self.assertEqual(source_rows, destination_rows)

    # Make sure that there are no extra rows on the destination
    count_query = 'select count(*) from worker_test'
    destination_count = destination_tablet.mquery('vt_test_keyspace', count_query)[0][0]
    self.assertEqual(destination_count, len(destination_rows))

  def run_split_diff(self, keyspace_shard, source_tablets, destination_tablets):
    """Runs a vtworker SplitDiff on the given keyspace/shard, and then sets all
    former rdonly slaves back to rdonly.

    Args:
      keyspace_shard - keyspace/shard to run SplitDiff on (string)
      source_tablets - ShardTablets instance for the source shard
      destination_tablets - ShardTablets instance for the destination shard
    """
    logging.debug("Running vtworker SplitDiff for %s" % keyspace_shard)
    stdout, stderr = utils.run_vtworker(['-cell', 'test_nj', 'SplitDiff',
      keyspace_shard], auto_log=True)

  def setUp(self):
    """Creates the necessary shards, starts the tablets, and inserts some data."""
    self.run_shard_tablets('0', shard_tablets)
    # create the split shards
    self.run_shard_tablets('-80', shard_0_tablets, create_db=False,
      create_table=False, wait_state='NOT_SERVING')
    self.run_shard_tablets('80-', shard_1_tablets, create_db=False,
      create_table=False, wait_state='NOT_SERVING')

    logging.debug("Start inserting initial data: %s rows", utils.options.num_insert_rows)
    self.insert_values(shard_master, utils.options.num_insert_rows, 2)
    logging.debug("Done inserting initial data, waiting for replication to catch up")
    utils.wait_for_replication_pos(shard_master, shard_rdonly1)
    logging.debug("Replication on source rdonly tablet is caught up")

  def tearDown(self):
    """Tries to do the minimum to reset topology and tablets to their initial states.

    When benchmarked, this seemed to take around 30% of the time of (setupModule +
    tearDownModule).
    """
    for shard_tablet in [shard_tablets, shard_0_tablets, shard_1_tablets]:
      for tablet in shard_tablet.all_tablets:
        tablet.reset_replication()
        tablet.clean_dbs()
        tablet.scrap(force=True, skip_rebuild=True)
        utils.run_vtctl(['DeleteTablet', tablet.tablet_alias], auto_log=True)
        tablet.kill_vttablet()
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    for shard in ['0', '-80', '80-']:
      utils.run_vtctl(['DeleteShard', 'test_keyspace/%s' % shard], auto_log=True)

class TestBaseSplitCloneResiliency(TestBaseSplitClone):
  """Tests that the SplitClone worker is resilient to particular failures."""
  
  def setUp(self):
    super(TestBaseSplitCloneResiliency, self).setUp()
    self.copy_schema_to_destination_shards()
    
  def tearDown(self):
    super(TestBaseSplitCloneResiliency, self).tearDown()
  
  def verify_successful_worker_copy_with_reparent(self, mysql_down=False):
    """Verifies that vtworker can successfully copy data for a SplitClone.

    Order of operations:
    1. Run a background vtworker
    2. Wait until the worker successfully resolves the destination masters.
    3. Reparent the destination tablets
    4. Wait until the vtworker copy is finished
    5. Verify that the worker was forced to reresolve topology and retry writes
      due to the reparent.
    6. Verify that the data was copied successfully to both new shards

    Args:
      mysql_down - boolean, True iff we expect the MySQL instances on the
        destination masters to be down.

    Raises:
      AssertionError if things didn't go as expected.
    """
    worker_proc, worker_port, _ = utils.run_vtworker_bg(['--cell', 'test_nj',
                        'SplitClone',
                        '--source_reader_count', '1',
                        '--destination_pack_count', '1',
                        '--destination_writer_count', '1',
                        '--strategy=-populate_blp_checkpoint',
                        'test_keyspace/0'],
                       auto_log=True)

    if mysql_down:
      # If MySQL is down, we wait until resolving at least twice (to verify that
      # we do reresolve and retry due to MySQL being down).
      worker_vars = utils.poll_for_vars('vtworker', worker_port,
        'WorkerDestinationActualResolves >= 2',
        condition_fn=lambda v: v.get('WorkerDestinationActualResolves') >= 2)
      self.assertNotEqual(worker_vars['WorkerRetryCount'], {},
        "expected vtworker to retry, but it didn't")
      logging.debug("Worker has resolved at least twice, starting reparent now")

      # Original masters have no running MySQL, so need to force the reparent
      utils.run_vtctl(['EmergencyReparentShard', 'test_keyspace/-80',
        shard_0_replica.tablet_alias], auto_log=True)
      utils.run_vtctl(['EmergencyReparentShard', 'test_keyspace/80-',
        shard_1_replica.tablet_alias], auto_log=True)

    else:
      utils.poll_for_vars('vtworker', worker_port,
        'WorkerDestinationActualResolves >= 1',
        condition_fn=lambda v: v.get('WorkerDestinationActualResolves') >= 1)
      logging.debug("Worker has resolved at least once, starting reparent now")

      utils.run_vtctl(['PlannedReparentShard', 'test_keyspace/-80',
        shard_0_replica.tablet_alias], auto_log=True)
      utils.run_vtctl(['PlannedReparentShard', 'test_keyspace/80-',
        shard_1_replica.tablet_alias], auto_log=True)

    logging.debug("Polling for worker state")
    # There are a couple of race conditions around this, that we need to be careful of:
    # 1. It's possible for the reparent step to take so long that the worker will
    #   actually finish before we get to the polling step. To workaround this,
    #   the test takes a parameter to increase the number of rows that the worker
    #   has to copy (with the idea being to slow the worker down).
    # 2. If the worker has a huge number of rows to copy, it's possible for the
    #   polling to timeout before the worker has finished copying the data.
    #
    # You should choose a value for num_insert_rows, such that this test passes
    # for your environment (trial-and-error...)
    worker_vars = utils.poll_for_vars('vtworker', worker_port,
      'WorkerState == cleaning up',
      condition_fn=lambda v: v.get('WorkerState') == 'cleaning up',
      # We know that vars should already be ready, since we read them earlier
      require_vars=True,
      # We're willing to let the test run for longer to make it less flaky.
      # This should still fail fast if something goes wrong with vtworker,
      # because of the require_vars flag above.
      timeout=5*60)

    # Verify that we were forced to reresolve and retry.
    self.assertGreater(worker_vars['WorkerDestinationActualResolves'], 1)
    self.assertGreater(worker_vars['WorkerDestinationAttemptedResolves'], 1)
    self.assertNotEqual(worker_vars['WorkerRetryCount'], {},
      "expected vtworker to retry, but it didn't")

    utils.wait_procs([worker_proc])

    # Make sure that everything is caught up to the same replication point
    self.run_split_diff('test_keyspace/-80', shard_tablets, shard_0_tablets)
    self.run_split_diff('test_keyspace/80-', shard_tablets, shard_1_tablets)

    self.assert_shard_data_equal(0, shard_master, shard_0_tablets.replica)
    self.assert_shard_data_equal(1, shard_master, shard_1_tablets.replica)


class TestReparentDuringWorkerCopy(TestBaseSplitCloneResiliency):

  def test_reparent_during_worker_copy(self):
    """This test simulates a destination reparent during a worker SplitClone copy.

    The SplitClone command should be able to gracefully handle the reparent and
    end up with the correct data on the destination.

    Note: this test has a small possibility of flaking, due to the timing issues
    involved. It's possible for the worker to finish the copy step before the
    reparent succeeds, in which case there are assertions that will fail. This
    seems better than having the test silently pass.
    """
    self.verify_successful_worker_copy_with_reparent()


class TestMysqlDownDuringWorkerCopy(TestBaseSplitCloneResiliency):

  def setUp(self):
    """Shuts down MySQL on the destination masters (in addition to the base setup)"""
    logging.debug("Starting base setup for MysqlDownDuringWorkerCopy")
    super(TestMysqlDownDuringWorkerCopy, self).setUp()

    logging.debug("Starting MysqlDownDuringWorkerCopy-specific setup")
    utils.wait_procs([shard_0_master.shutdown_mysql(),
      shard_1_master.shutdown_mysql()])
    logging.debug("Finished MysqlDownDuringWorkerCopy-specific setup")

  def tearDown(self):
    """Restarts the MySQL processes that were killed during the setup."""
    logging.debug("Starting MysqlDownDuringWorkerCopy-specific tearDown")
    utils.wait_procs([shard_0_master.start_mysql(),
      shard_1_master.start_mysql()])
    logging.debug("Finished MysqlDownDuringWorkerCopy-specific tearDown")

    super(TestMysqlDownDuringWorkerCopy, self).tearDown()
    logging.debug("Finished base tearDown for MysqlDownDuringWorkerCopy")

  def test_mysql_down_during_worker_copy(self):
    """This test simulates MySQL being down on the destination masters."""
    self.verify_successful_worker_copy_with_reparent(mysql_down=True)

class TestVtworkerWebinterface(unittest.TestCase):
  def setUp(self):
    # Run vtworker without any optional arguments to start in interactive mode.
    self.worker_proc, self.worker_port, _ = utils.run_vtworker_bg([])

  def tearDown(self):
    utils.kill_sub_process(self.worker_proc)

  def test_webinterface(self):
    worker_base_url = 'http://localhost:%u' % int(self.worker_port)
    # Wait for /status to become available.
    timeout = 10
    while True:
      done = False
      try:
        urllib2.urlopen(worker_base_url + '/status').read()
        done = True
      except:
        pass
      if done:
        break
      timeout = utils.wait_step('worker /status webpage must be available', timeout)
      
    # Run the command twice to make sure it's idempotent.
    for _ in range(2):
      # Run Ping command.
      try:
        urllib2.urlopen(worker_base_url + '/Debugging/Ping', data=urllib.urlencode({'message':'pong'})).read()
        raise Exception("Should have thrown an HTTPError for the redirect.")
      except urllib2.HTTPError as e:
        self.assertEqual(e.code, 307)
      # Verify that the command logged something and its available at /status.
      status = urllib2.urlopen(worker_base_url + '/status').read()
      self.assertIn("Ping command was called with message: 'pong'", status, "Command did not log output to /status")
      
      # Reset the job.
      urllib2.urlopen(worker_base_url + '/reset').read()
      status_after_reset = urllib2.urlopen(worker_base_url + '/status').read()
      self.assertIn("This worker is idle.", status_after_reset, "/status does not indicate that the reset was successful")

def add_test_options(parser):
  parser.add_option('--num_insert_rows', type="int", default=3000,
    help="The number of rows, per shard, that we should insert before resharding for this test.")

if __name__ == '__main__':
  utils.main(test_options=add_test_options)