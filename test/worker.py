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
"""Tests the robustness and resiliency of vtworkers."""

from collections import namedtuple
import urllib
import urllib2

import logging
import unittest

from vtdb import keyrange_constants

import base_sharding
import environment
import tablet
import utils


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

    Returns:
      List of all tablets of the shard.
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

  def __str__(self):
    return """master %s
replicas:
%s
rdonlys:
%s
""" % (self.master,
       '\n'.join('       %s' % replica for replica in self.replicas),
       '\n'.join('       %s' % rdonly for rdonly in self.rdonlys))

# initial shard, covers everything
shard_master = tablet.Tablet()
shard_replica = tablet.Tablet()
shard_rdonly1 = tablet.Tablet()

# split shards
# range '' - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
shard_0_rdonly1 = tablet.Tablet()
# range 80 - ''
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()
shard_1_rdonly1 = tablet.Tablet()

all_shard_tablets = ShardTablets(shard_master, [shard_replica], [shard_rdonly1])
shard_0_tablets = ShardTablets(
    shard_0_master, [shard_0_replica], [shard_0_rdonly1])
shard_1_tablets = ShardTablets(
    shard_1_master, [shard_1_replica], [shard_1_rdonly1])


def init_keyspace():
  """Creates a `test_keyspace` keyspace with a sharding key."""
  utils.run_vtctl(
      ['CreateKeyspace', '-sharding_column_name', 'keyspace_id',
       '-sharding_column_type', KEYSPACE_ID_TYPE, 'test_keyspace'])


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
    logging.debug('environment set up with the following shards and tablets:')
    logging.debug('=========================================================')
    logging.debug('TABLETS: test_keyspace/0:\n%s', all_shard_tablets)
    logging.debug('TABLETS: test_keyspace/-80:\n%s', shard_0_tablets)
    logging.debug('TABLETS: test_keyspace/80-:\n%s', shard_1_tablets)
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
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


class TestBaseSplitClone(unittest.TestCase, base_sharding.BaseShardingTest):
  """Abstract test base class for testing the SplitClone worker."""

  def __init__(self, *args, **kwargs):
    super(TestBaseSplitClone, self).__init__(*args, **kwargs)
    self.num_insert_rows = utils.options.num_insert_rows

  def run_shard_tablets(
      self, shard_name, shard_tablets, create_table=True):
    """Handles all the necessary work for initially running a shard's tablets.

    This encompasses the following steps:
      1. (optional) Create db
      2. Starting vttablets and let themselves init them
      3. Waiting for the appropriate vttablet state
      4. Force reparent to the master tablet
      5. RebuildKeyspaceGraph
      7. (optional) Running initial schema setup

    Args:
      shard_name: the name of the shard to start tablets in
      shard_tablets: an instance of ShardTablets for the given shard
      create_table: boolean, True iff we should create a table on the tablets
    """
    # Start tablets.
    #
    # NOTE: The future master has to be started with type 'replica'.
    shard_tablets.master.start_vttablet(
        wait_for_state=None, init_tablet_type='replica',
        init_keyspace='test_keyspace', init_shard=shard_name,
        binlog_use_v3_resharding_mode=False)
    for t in shard_tablets.replicas:
      t.start_vttablet(
          wait_for_state=None, init_tablet_type='replica',
          init_keyspace='test_keyspace', init_shard=shard_name,
          binlog_use_v3_resharding_mode=False)
    for t in shard_tablets.rdonlys:
      t.start_vttablet(
          wait_for_state=None, init_tablet_type='rdonly',
          init_keyspace='test_keyspace', init_shard=shard_name,
          binlog_use_v3_resharding_mode=False)

    # Block until tablets are up and we can enable replication.
    # All tables should be NOT_SERVING until we run InitShardMaster.
    for t in shard_tablets.all_tablets:
      t.wait_for_vttablet_state('NOT_SERVING')

    # Reparent to choose an initial master and enable replication.
    utils.run_vtctl(
        ['InitShardMaster', '-force', 'test_keyspace/%s' % shard_name,
         shard_tablets.master.tablet_alias], auto_log=True)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # Enforce a health check instead of waiting for the next periodic one.
    # (saves up to 1 second execution time on average)
    for t in shard_tablets.replicas:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias])
    for t in shard_tablets.rdonlys:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias])

    # Wait for tablet state to change after starting all tablets. This allows
    # us to start all tablets at once, instead of sequentially waiting.
    # NOTE: Replication has to be enabled first or the health check will
    #       set a a replica or rdonly tablet back to NOT_SERVING.
    for t in shard_tablets.all_tablets:
      t.wait_for_vttablet_state('SERVING')

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

  def _insert_values(self, vttablet, id_offset, msg, keyspace_id, num_values):
    """Inserts values into MySQL along with the required routing comments.

    Args:
      vttablet: the Tablet instance to modify.
      id_offset: offset for the value of `id` column.
      msg: the value of `msg` column.
      keyspace_id: the value of `keyspace_id` column.
      num_values: number of rows to be inserted.
    """

    # For maximum performance, multiple values are inserted in one statement.
    # However, when the statements are too long, queries will timeout and
    # vttablet will kill them. Therefore, we chunk it into multiple statements.
    def chunks(full_list, n):
      """Yield successive n-sized chunks from full_list."""
      for i in xrange(0, len(full_list), n):
        yield full_list[i:i+n]

    max_chunk_size = 100*1000
    k = utils.uint64_to_hex(keyspace_id)
    for chunk in chunks(range(1, num_values+1), max_chunk_size):
      logging.debug('Inserting values for range [%d, %d].', chunk[0], chunk[-1])
      values_str = ''
      for i in chunk:
        if i != chunk[0]:
          values_str += ','
        values_str += "(%d, '%s', 0x%x)" % (id_offset + i, msg, keyspace_id)
      vttablet.mquery(
          'vt_test_keyspace', [
              'begin',
              'insert into worker_test(id, msg, keyspace_id) values%s '
              '/* vtgate:: keyspace_id:%s */' % (values_str, k),
              'commit'],
          write=True)

  def insert_values(self, vttablet, num_values, num_shards, offset=0,
                    keyspace_id_range=2**64):
    """Inserts simple values, one for each potential shard.

    Each row is given a message that contains the shard number, so we can easily
    verify that the source and destination shards have the same data.

    Args:
      vttablet: the Tablet instance to modify.
      num_values: The number of values to insert.
      num_shards: the number of shards that we expect to have.
      offset: amount that we should offset the `id`s by. This is useful for
        inserting values multiple times.
      keyspace_id_range: the number of distinct values that the keyspace id
        can have.
    """
    shard_width = keyspace_id_range / num_shards
    shard_offsets = [i * shard_width for i in xrange(num_shards)]
    # TODO(mberlin): Change the "id" column values from the keyspace id to a
    #                counter starting at 1. The incrementing ids must
    #                alternate between the two shards. Without this, the
    #                vtworker chunking won't be well balanced across shards.
    for shard_num in xrange(num_shards):
      self._insert_values(
          vttablet,
          shard_offsets[shard_num] + offset,
          'msg-shard-%d' % shard_num,
          shard_offsets[shard_num],
          num_values)

  def assert_shard_data_equal(
      self, shard_num, source_tablet, destination_tablet):
    """Asserts source and destination tablets have identical shard data.

    Args:
      shard_num: The shard number of the shard that we want to verify.
      source_tablet: Tablet instance of the source shard.
      destination_tablet: Tablet instance of the destination shard.
    """
    select_query = (
        'select * from worker_test where msg="msg-shard-%s" order by id asc' %
        shard_num)

    # Make sure all the right rows made it from the source to the destination
    source_rows = source_tablet.mquery('vt_test_keyspace', select_query)
    destination_rows = destination_tablet.mquery(
        'vt_test_keyspace', select_query)
    self.assertEqual(source_rows, destination_rows)

    # Make sure that there are no extra rows on the destination
    count_query = 'select count(*) from worker_test'
    destination_count = destination_tablet.mquery(
        'vt_test_keyspace', count_query)[0][0]
    self.assertEqual(destination_count, len(destination_rows))

  def run_split_diff(self, keyspace_shard, source_tablets, destination_tablets):
    """Runs a vtworker SplitDiff on the given keyspace/shard.

    Sets all former rdonly slaves back to rdonly.

    Args:
      keyspace_shard: keyspace/shard to run SplitDiff on (string)
      source_tablets: ShardTablets instance for the source shard
      destination_tablets: ShardTablets instance for the destination shard
    """
    _ = source_tablets, destination_tablets
    logging.debug('Running vtworker SplitDiff for %s', keyspace_shard)
    _, _ = utils.run_vtworker(
        ['-cell', 'test_nj',
         '--use_v3_resharding_mode=false',
         'SplitDiff',
         '--min_healthy_rdonly_tablets', '1',
         keyspace_shard], auto_log=True)

  def setUp(self):
    """Creates shards, starts the tablets, and inserts some data."""
    try:
      self.run_shard_tablets('0', all_shard_tablets)
      # create the split shards
      self.run_shard_tablets(
          '-80', shard_0_tablets, create_table=False)
      self.run_shard_tablets(
          '80-', shard_1_tablets, create_table=False)

      logging.debug('Start inserting initial data: %s rows',
                    self.num_insert_rows)
      self.insert_values(shard_master, self.num_insert_rows, 2)
      logging.debug(
          'Done inserting initial data, waiting for replication to catch up')
      utils.wait_for_replication_pos(shard_master, shard_rdonly1)
      logging.debug('Replication on source rdonly tablet is caught up')
    except:
      self.tearDown()
      raise

  def tearDown(self):
    """Does the minimum to reset topology and tablets to their initial states.

    When benchmarked, this seemed to take around 30% of the time of
    (setupModule + tearDownModule).

    FIXME(aaijazi): doing this in parallel greatly reduces the time it takes.
    See the kill_tablets method in tablet.py.
    """

    for shard_tablet in [all_shard_tablets, shard_0_tablets, shard_1_tablets]:
      for t in shard_tablet.all_tablets:
        t.reset_replication()
        t.set_semi_sync_enabled(master=False)
        t.clean_dbs()
        # _vt.vreplication should be dropped to avoid interference between
        # test cases
        t.mquery('', 'drop table if exists _vt.vreplication')
        t.kill_vttablet()
        # we allow failures here as some tablets will be gone sometimes
        # (the master tablets after an emergency reparent)
        utils.run_vtctl(['DeleteTablet', '-allow_master', t.tablet_alias],
                        auto_log=True, raise_on_error=False)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    for shard in ['0', '-80', '80-']:
      utils.run_vtctl(
          ['DeleteShard', '-even_if_serving', 'test_keyspace/%s' % shard],
          auto_log=True)


class TestBaseSplitCloneResiliency(TestBaseSplitClone):
  """Tests that the SplitClone worker is resilient to particular failures."""

  def setUp(self):
    try:
      super(TestBaseSplitCloneResiliency, self).setUp()
      self.copy_schema_to_destination_shards()
    except:
      self.tearDown()
      raise

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
      mysql_down: boolean. If True, we take down the MySQL instances on the
        destination masters at first, then bring them back and reparent away.

    Raises:
      AssertionError if things didn't go as expected.
    """
    worker_proc, worker_port, worker_rpc_port = utils.run_vtworker_bg(
        ['--cell', 'test_nj', '--use_v3_resharding_mode=false'],
        auto_log=True)

    # --max_tps is only specified to enable the throttler and ensure that the
    # code is executed. But the intent here is not to throttle the test, hence
    # the rate limit is set very high.
    # --chunk_count is 2 because rows are currently ordered by primary key such
    # that all rows of the first shard come first and then the second shard.
    # TODO(mberlin): Remove --offline=false once vtworker ensures that the
    #                destination shards are not behind the master's replication
    #                position.
    args = ['SplitClone',
            '--offline=false',
            '--destination_writer_count', '1',
            '--min_healthy_tablets', '1',
            '--max_tps', '9999']
    # Make the clone as slow as necessary such that there is enough time to
    # run PlannedReparent in the meantime.
    # TODO(mberlin): Once insert_values is fixed to uniformly distribute the
    #                rows across shards when sorted by primary key, remove
    #                --chunk_count 2, --min_rows_per_chunk 1 and set
    #                --source_reader_count back to 1.
    args.extend(['--source_reader_count', '2',
                   '--chunk_count', '2',
                   '--min_rows_per_chunk', '1',
                   '--write_query_max_rows', '1'])
    args.append('test_keyspace/0')
    workerclient_proc = utils.run_vtworker_client_bg(args, worker_rpc_port)

    if mysql_down:
      # vtworker is blocked at this point. This is a good time to test that its
      # throttler server is reacting to RPCs.
      self.check_throttler_service('localhost:%d' % worker_rpc_port,
                                   ['test_keyspace/-80', 'test_keyspace/80-'],
                                   9999)

      utils.poll_for_vars(
          'vtworker', worker_port,
          'WorkerState == cloning the data (online)',
          condition_fn=lambda v: v.get('WorkerState') == 'cloning the'
          ' data (online)')

      logging.debug('Worker is in copy state, Shutting down mysqld on destination masters.')
      utils.wait_procs(
          [shard_0_master.shutdown_mysql(),
           shard_1_master.shutdown_mysql()])

      # If MySQL is down, we wait until vtworker retried at least once to make
      # sure it reached the point where a write failed due to MySQL being down.
      # There should be two retries at least, one for each destination shard.
      utils.poll_for_vars(
          'vtworker', worker_port,
          'WorkerRetryCount >= 2',
          condition_fn=lambda v: v.get('WorkerRetryCount') >= 2)
      logging.debug('Worker has retried at least once per shard, starting reparent now')

      # Bring back masters. Since we test with semi-sync now, we need at least
      # one replica for the new master. This test is already quite expensive,
      # so we bring back the old master as a replica rather than having a third
      # replica up the whole time.
      logging.debug('Restarting mysqld on destination masters')
      utils.wait_procs(
          [shard_0_master.start_mysql(),
           shard_1_master.start_mysql()])

      # Reparent away from the old masters.
      utils.run_vtctl(
          ['PlannedReparentShard', '-keyspace_shard', 'test_keyspace/-80',
           '-new_master', shard_0_replica.tablet_alias], auto_log=True)
      utils.run_vtctl(
          ['PlannedReparentShard', '-keyspace_shard', 'test_keyspace/80-',
           '-new_master', shard_1_replica.tablet_alias], auto_log=True)

    else:
      # NOTE: There is a race condition around this:
      #   It's possible that the SplitClone vtworker command finishes before the
      #   PlannedReparentShard vtctl command, which we start below, succeeds.
      #   Then the test would fail because vtworker did not have to retry.
      #
      # To workaround this, the test takes a parameter to increase the number of
      # rows that the worker has to copy (with the idea being to slow the worker
      # down).
      # You should choose a value for num_insert_rows, such that this test
      # passes for your environment (trial-and-error...)
      # Make sure that vtworker got past the point where it picked a master
      # for each destination shard ("finding targets" state).
      utils.poll_for_vars(
          'vtworker', worker_port,
          'WorkerState == cloning the data (online)',
          condition_fn=lambda v: v.get('WorkerState') == 'cloning the'
          ' data (online)')
      logging.debug('Worker is in copy state, starting reparent now')

      utils.run_vtctl(
          ['PlannedReparentShard', '-keyspace_shard', 'test_keyspace/-80',
           '-new_master', shard_0_replica.tablet_alias], auto_log=True)
      utils.run_vtctl(
          ['PlannedReparentShard', '-keyspace_shard', 'test_keyspace/80-',
           '-new_master', shard_1_replica.tablet_alias], auto_log=True)

    utils.wait_procs([workerclient_proc])

    # Verify that we were forced to re-resolve and retry.
    worker_vars = utils.get_vars(worker_port)
    self.assertGreater(worker_vars['WorkerRetryCount'], 1,
                       "expected vtworker to retry each of the two reparented"
                       " destination masters at least once, but it didn't")
    self.assertNotEqual(worker_vars['WorkerRetryCount'], {},
                        "expected vtworker to retry, but it didn't")
    utils.kill_sub_process(worker_proc, soft=True)

    # Wait for the destination RDONLYs to catch up or the following offline
    # clone will try to insert rows which already exist.
    # TODO(mberlin): Remove this once SplitClone supports it natively.
    utils.wait_for_replication_pos(shard_0_replica, shard_0_rdonly1)
    utils.wait_for_replication_pos(shard_1_replica, shard_1_rdonly1)
    # Run final offline clone to enable filtered replication.
    _, _ = utils.run_vtworker(['-cell', 'test_nj',
                               '--use_v3_resharding_mode=false',
                               'SplitClone',
                               '--online=false',
                               '--min_healthy_tablets', '1',
                               'test_keyspace/0'], auto_log=True)

    # Make sure that everything is caught up to the same replication point
    self.run_split_diff('test_keyspace/-80', all_shard_tablets, shard_0_tablets)
    self.run_split_diff('test_keyspace/80-', all_shard_tablets, shard_1_tablets)

    self.assert_shard_data_equal(0, shard_master, shard_0_tablets.replica)
    self.assert_shard_data_equal(1, shard_master, shard_1_tablets.replica)


class TestReparentDuringWorkerCopy(TestBaseSplitCloneResiliency):

  def __init__(self, *args, **kwargs):
    super(TestReparentDuringWorkerCopy, self).__init__(*args, **kwargs)
    self.num_insert_rows = utils.options.num_insert_rows_before_reparent_test

  def test_reparent_during_worker_copy(self):
    """Simulates a destination reparent during a worker SplitClone copy.

    The SplitClone command should be able to gracefully handle the reparent and
    end up with the correct data on the destination.

    Note: this test has a small possibility of flaking, due to the timing issues
    involved. It's possible for the worker to finish the copy step before the
    reparent succeeds, in which case there are assertions that will fail. This
    seems better than having the test silently pass.
    """
    self.verify_successful_worker_copy_with_reparent()


class TestMysqlDownDuringWorkerCopy(TestBaseSplitCloneResiliency):

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
    worker_base_url = 'http://localhost:%d' % int(self.worker_port)
    # Wait for /status to become available.
    timeout = 10
    while True:
      done = False
      try:
        urllib2.urlopen(worker_base_url + '/status').read()
        done = True
      except urllib2.URLError:
        pass
      if done:
        break
      timeout = utils.wait_step(
          'worker /status webpage must be available', timeout)

    # Run the command twice to make sure it's idempotent.
    for _ in range(2):
      # Run Ping command.
      try:
        urllib2.urlopen(
            worker_base_url + '/Debugging/Ping',
            data=urllib.urlencode({'message': 'pong'})).read()
        raise Exception('Should have thrown an HTTPError for the redirect.')
      except urllib2.HTTPError as e:
        self.assertEqual(e.code, 307)
      # Wait for the Ping command to finish.
      utils.poll_for_vars(
          'vtworker', self.worker_port,
          'WorkerState == done',
          condition_fn=lambda v: v.get('WorkerState') == 'done')
      # Verify that the command logged something and it's available at /status.
      status = urllib2.urlopen(worker_base_url + '/status').read()
      self.assertIn(
          "Ping command was called with message: 'pong'", status,
          'Command did not log output to /status: %s' % status)

      # Reset the job.
      urllib2.urlopen(worker_base_url + '/reset').read()
      status_after_reset = urllib2.urlopen(worker_base_url + '/status').read()
      self.assertIn(
          'This worker is idle.', status_after_reset,
          '/status does not indicate that the reset was successful')


class TestMinHealthyRdonlyTablets(TestBaseSplitCloneResiliency):

  def split_clone_fails_not_enough_health_rdonly_tablets(self):
    """Verify vtworker errors if there aren't enough healthy RDONLY tablets."""

    _, stderr = utils.run_vtworker(
        ['-cell', 'test_nj',
         '--wait_for_healthy_rdonly_tablets_timeout', '1s',
         '--use_v3_resharding_mode=false',
         'SplitClone',
         '--min_healthy_tablets', '2',
         'test_keyspace/0'],
        auto_log=True,
        expect_fail=True)
    self.assertIn('findTargets() failed: FindWorkerTablet() failed for'
                  ' test_nj/test_keyspace/0: not enough healthy RDONLY'
                  ' tablets to choose from in (test_nj,test_keyspace/0),'
                  ' have 1 healthy ones, need at least 2', stderr)


def add_test_options(parser):
  parser.add_option(
      '--num_insert_rows', type='int', default=100,
      help='The number of rows, per shard, that we should insert before '
      'resharding for this test.')
  parser.add_option(
      '--num_insert_rows_before_reparent_test', type='int', default=4500,
      help='The number of rows, per shard, that we should insert before '
      'running TestReparentDuringWorkerCopy (supersedes --num_insert_rows in '
      'that test). There must be enough rows such that SplitClone takes '
      'several seconds to run while we run a planned reparent.')

if __name__ == '__main__':
  utils.main(test_options=add_test_options)
