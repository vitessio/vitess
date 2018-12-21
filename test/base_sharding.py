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

"""This module contains a base class and utility functions for sharding tests.
"""

import struct

import logging

from vtdb import keyrange_constants

import utils


keyspace_id_type = keyrange_constants.KIT_UINT64
use_rbr = False
use_multi_split_diff = False
pack_keyspace_id = struct.Struct('!Q').pack

# fixed_parent_id is used as fixed value for the "parent_id" column in all rows.
# All tests assume a multi-column primary key (parent_id, id) but only adjust
# the "id" column and use this fixed value for "parent_id".
# Since parent_id is fixed, not all test code has to include parent_id in a
# WHERE clause (at the price of a full table scan).
fixed_parent_id = 86


class BaseShardingTest(object):
  """This base class uses unittest.TestCase methods to check various things.

  All sharding tests should inherit from this base class, and use the
  methods as needed.
  """

  # _insert_value inserts a value in the MySQL database along with the comments
  # required for routing.
  # NOTE: We assume that the column name for the keyspace_id is called
  #       'custom_ksid_col'. This is a regression test which tests for
  #       places which previously hardcoded the column name to 'keyspace_id'.
  def _insert_value(self, tablet_obj, table, mid, msg, keyspace_id):
    k = utils.uint64_to_hex(keyspace_id)
    tablet_obj.mquery(
        'vt_test_keyspace',
        ['begin',
         'insert into %s(parent_id, id, msg, custom_ksid_col) '
         'values(%d, %d, "%s", 0x%x) /* vtgate:: keyspace_id:%s */ '
         '/* id:%d */' %
         (table, fixed_parent_id, mid, msg, keyspace_id, k, mid),
         'commit'],
        write=True)

  def _insert_multi_value(self, tablet_obj, table, mids, msgs, keyspace_ids):
    """Generate multi-shard insert statements."""
    comma_sep = ','
    querystr = ('insert into %s(parent_id, id, msg, custom_ksid_col) values'
                %(table))
    values_str = ''
    id_str = '/* id:'
    ksid_str = ''

    for mid, msg, keyspace_id in zip(mids, msgs, keyspace_ids):
      ksid_str += utils.uint64_to_hex(keyspace_id)+comma_sep
      values_str += ('(%d, %d, "%s", 0x%x)' %
                     (fixed_parent_id, mid, msg, keyspace_id) + comma_sep)
      id_str += '%d' % (mid) + comma_sep

    values_str = values_str.rstrip(comma_sep)
    values_str += '/* vtgate:: keyspace_id:%s */ ' %(ksid_str.rstrip(comma_sep))
    values_str += id_str.rstrip(comma_sep) + '*/'

    querystr += values_str
    tablet_obj.mquery(
        'vt_test_keyspace',
        ['begin',
         querystr,
         'commit'],
        write=True)

  def _exec_non_annotated_update(self, tablet_obj, table, mids, new_val):
    tablet_obj.mquery(
        'vt_test_keyspace',
        ['begin',
         'update %s set msg = "%s" where parent_id = %d and id in (%s)' %
         (table, new_val, fixed_parent_id, ','.join([str(i) for i in mids])),
         'commit'],
        write=True)

  def _exec_non_annotated_delete(self, tablet_obj, table, mids):
    tablet_obj.mquery(
        'vt_test_keyspace',
        ['begin',
         'delete from %s where parent_id = %d and id in (%s)' %
         (table, fixed_parent_id, ','.join([str(i) for i in mids])),
         'commit'],
        write=True)

  def _get_value(self, tablet_obj, table, mid):
    """Returns the row(s) from the table for the provided id, using MySQL.

    Args:
      tablet_obj: the tablet to get data from.
      table: the table to query.
      mid: id field of the table.
    Returns:
      A tuple of results.
    """
    return tablet_obj.mquery(
        'vt_test_keyspace',
        'select parent_id, id, msg, custom_ksid_col from %s '
        'where parent_id=%d and id=%d' %
        (table, fixed_parent_id, mid))

  def _check_value(self, tablet_obj, table, mid, msg, keyspace_id,
                   should_be_here=True):
    result = self._get_value(tablet_obj, table, mid)
    if keyspace_id_type == keyrange_constants.KIT_BYTES:
      fmt = '%s'
      keyspace_id = pack_keyspace_id(keyspace_id)
    else:
      fmt = '%x'
    if should_be_here:
      self.assertEqual(result, ((fixed_parent_id, mid, msg, keyspace_id),),
                       ('Bad row in tablet %s for id=%d, custom_ksid_col=' +
                        fmt + ', row=%s') % (tablet_obj.tablet_alias, mid,
                                             keyspace_id, str(result)))
    else:
      self.assertEqual(
          len(result), 0,
          ('Extra row in tablet %s for id=%d, custom_ksid_col=' +
           fmt + ': %s') % (tablet_obj.tablet_alias, mid, keyspace_id,
                            str(result)))

  def _is_value_present_and_correct(
      self, tablet_obj, table, mid, msg, keyspace_id):
    """_is_value_present_and_correct tries to read a value.

    Args:
      tablet_obj: the tablet to get data from.
      table: the table to query.
      mid: the id of the row to query.
      msg: expected value of the msg column in the row.
      keyspace_id: expected value of the keyspace_id column in the row.
    Returns:
      True if the value (row) is there and correct.
      False if the value is not there.
      If the value is not correct, the method will call self.fail.
    """
    result = self._get_value(tablet_obj, table, mid)
    if not result:
      return False
    if keyspace_id_type == keyrange_constants.KIT_BYTES:
      fmt = '%s'
      keyspace_id = pack_keyspace_id(keyspace_id)
    else:
      fmt = '%x'
    self.assertEqual(result, ((fixed_parent_id, mid, msg, keyspace_id),),
                     ('Bad row in tablet %s for id=%d, '
                      'custom_ksid_col=' + fmt) % (
                          tablet_obj.tablet_alias, mid, keyspace_id))
    return True

  def check_binlog_player_vars(self, tablet_obj, source_shards,
                               seconds_behind_master_max=0):
    """Checks the binlog player variables are correctly exported.

    Args:
      tablet_obj: the tablet to check.
      source_shards: the shards to check we are replicating from.
      seconds_behind_master_max: if non-zero, the lag should be smaller than
                                 this value.
    """
    v = utils.get_vars(tablet_obj.port)
    self.assertIn('VReplicationStreamCount', v)
    self.assertEquals(v['VReplicationStreamCount'], len(source_shards))
    self.assertIn('VReplicationSecondsBehindMasterMax', v)
    self.assertIn('VReplicationSecondsBehindMaster', v)
    self.assertIn('VReplicationSource', v)
    shards = v['VReplicationSource'].values()
    self.assertEquals(sorted(shards), sorted(source_shards))
    self.assertIn('VReplicationSourceTablet', v)
    for uid in v['VReplicationSource']:
      self.assertIn(uid, v['VReplicationSourceTablet'])
    if seconds_behind_master_max != 0:
      self.assertTrue(
          v['VReplicationSecondsBehindMasterMax'] <
          seconds_behind_master_max,
          'VReplicationSecondsBehindMasterMax is too high: %d > %d' % (
              v['VReplicationSecondsBehindMasterMax'],
              seconds_behind_master_max))
      for uid in v['VReplicationSource']:
        self.assertTrue(
            v['VReplicationSecondsBehindMaster'][uid] <
            seconds_behind_master_max,
            'VReplicationSecondsBehindMaster is too high: %d > %d' % (
                v['VReplicationSecondsBehindMaster'][uid],
                seconds_behind_master_max))

  def check_binlog_server_vars(self, tablet_obj, horizontal=True,
                               min_statements=0, min_transactions=0):
    """Checks the binlog server variables are correctly exported.

    Args:
      tablet_obj: the tablet to check.
      horizontal: true if horizontal split, false for vertical split.
      min_statements: check the statement count is greater or equal to this.
      min_transactions: check the transaction count is greater or equal to this.
    """
    v = utils.get_vars(tablet_obj.port)
    if horizontal:
      skey = 'UpdateStreamKeyRangeStatements'
      tkey = 'UpdateStreamKeyRangeTransactions'
    else:
      skey = 'UpdateStreamTablesStatements'
      tkey = 'UpdateStreamTablesTransactions'

    self.assertIn(skey, v)
    self.assertIn(tkey, v)
    if min_statements > 0:
      self.assertTrue(v[skey] >= min_statements,
                      'only got %d < %d statements' % (v[skey], min_statements))
    if min_transactions > 0:
      self.assertTrue(v[tkey] >= min_transactions,
                      'only got %d < %d transactions' % (v[tkey],
                                                         min_transactions))

  def check_stream_health_equals_binlog_player_vars(self, tablet_obj, count):
    """Checks the variables exported by streaming health check match vars.

    Args:
      tablet_obj: the tablet to check.
      count: number of binlog players to expect.
    """

    blp_stats = utils.get_vars(tablet_obj.port)
    self.assertEqual(blp_stats['VReplicationStreamCount'], count)

    # Enforce health check because it's not running by default as
    # tablets may not be started with it, or may not run it in time.
    utils.run_vtctl(['RunHealthCheck', tablet_obj.tablet_alias])
    stream_health = utils.run_vtctl_json(['VtTabletStreamHealth',
                                          '-count', '1',
                                          tablet_obj.tablet_alias])
    logging.debug('Got health: %s', str(stream_health))
    self.assertNotIn('serving', stream_health)
    self.assertIn('realtime_stats', stream_health)
    self.assertNotIn('health_error', stream_health['realtime_stats'])
    self.assertIn('binlog_players_count', stream_health['realtime_stats'])
    self.assertEqual(blp_stats['VReplicationStreamCount'],
                     stream_health['realtime_stats']['binlog_players_count'])
    self.assertEqual(blp_stats['VReplicationSecondsBehindMasterMax'],
                     stream_health['realtime_stats'].get(
                         'seconds_behind_master_filtered_replication', 0))

  def check_destination_master(self, tablet_obj, source_shards):
    """Performs multiple checks on a destination master.

    Combines the following:
      - wait_for_binlog_player_count
      - check_binlog_player_vars
      - check_stream_health_equals_binlog_player_vars

    Args:
      tablet_obj: the tablet to check.
      source_shards: the shards to check we are replicating from.
    """
    tablet_obj.wait_for_binlog_player_count(len(source_shards))
    self.check_binlog_player_vars(tablet_obj, source_shards)
    self.check_stream_health_equals_binlog_player_vars(tablet_obj,
                                                       len(source_shards))

  def check_running_binlog_player(self, tablet_obj, query, transaction,
                                  extra_text=None):
    """Checks binlog player is running and showing in status.

    Args:
      tablet_obj: the tablet to check.
      query: number of expected queries.
      transaction: number of expected transactions.
      extra_text: if present, look for it in status too.
    """
    status = tablet_obj.get_status()
    self.assertIn('VReplication state: Open', status)
    self.assertIn(
        '<td><b>All</b>: %d<br><b>Query</b>: %d<br>'
        '<b>Transaction</b>: %d<br></td>' % (query+transaction, query,
                                             transaction), status)
    self.assertIn('</html>', status)
    if extra_text:
      self.assertIn(extra_text, status)

  def check_no_binlog_player(self, tablet_obj):
    """Checks no binlog player is running.

    Also checks the tablet is not showing any binlog player in its status page.

    Args:
      tablet_obj: the tablet to check.
    """
    tablet_obj.wait_for_binlog_player_count(0)

  def check_throttler_service(self, throttler_server, names, rate):
    """Checks that the throttler responds to RPC requests.

    We assume it was enabled by SplitClone with the flag --max_tps 9999.

    Args:
      throttler_server: vtworker or vttablet RPC endpoint. Format: host:port
      names: Names of the throttlers e.g. BinlogPlayer/0 or <keyspace>/<shard>.
      rate: Expected initial rate the throttler was started with.
    """
    self.check_throttler_service_maxrates(throttler_server, names, rate)

    self.check_throttler_service_configuration(throttler_server, names)

  def check_throttler_service_maxrates(self, throttler_server, names, rate):
    """Checks the vtctl ThrottlerMaxRates and ThrottlerSetRate commands."""
    # Avoid flakes by waiting for all throttlers. (Necessary because filtered
    # replication on vttablet will register the throttler asynchronously.)
    timeout_s = 10
    while True:
      stdout, _ = utils.run_vtctl(['ThrottlerMaxRates', '--server',
                                   throttler_server], auto_log=True,
                                  trap_output=True)
      if '%d active throttler(s)' % len(names) in stdout:
        break
      timeout_s = utils.wait_step('all throttlers registered', timeout_s)
    for name in names:
      self.assertIn('| %s | %d |' % (name, rate), stdout)
    self.assertIn('%d active throttler(s)' % len(names), stdout)

    # Check that it's possible to change the max rate on the throttler.
    new_rate = 'unlimited'
    stdout, _ = utils.run_vtctl(['ThrottlerSetMaxRate', '--server',
                                 throttler_server, new_rate],
                                auto_log=True, trap_output=True)
    self.assertIn('%d active throttler(s)' % len(names), stdout)
    stdout, _ = utils.run_vtctl(['ThrottlerMaxRates', '--server',
                                 throttler_server], auto_log=True,
                                trap_output=True)
    for name in names:
      self.assertIn('| %s | %s |' % (name, new_rate), stdout)
    self.assertIn('%d active throttler(s)' % len(names), stdout)

  def check_throttler_service_configuration(self, throttler_server, names):
    """Checks the vtctl (Get|Update|Reset)ThrottlerConfiguration commands."""
    # Verify updating the throttler configuration.
    stdout, _ = utils.run_vtctl(['UpdateThrottlerConfiguration',
                                 '--server', throttler_server,
                                 '--copy_zero_values',
                                 'target_replication_lag_sec:12345 '
                                 'max_replication_lag_sec:65789 '
                                 'initial_rate:3 '
                                 'max_increase:0.4 '
                                 'emergency_decrease:0.5 '
                                 'min_duration_between_increases_sec:6 '
                                 'max_duration_between_increases_sec:7 '
                                 'min_duration_between_decreases_sec:8 '
                                 'spread_backlog_across_sec:9 '
                                 'ignore_n_slowest_replicas:0 '
                                 'ignore_n_slowest_rdonlys:0 '
                                 'age_bad_rate_after_sec:12 '
                                 'bad_rate_increase:0.13 '
                                 'max_rate_approach_threshold: 0.9 '],
                                auto_log=True, trap_output=True)
    self.assertIn('%d active throttler(s)' % len(names), stdout)
    # Check the updated configuration.
    stdout, _ = utils.run_vtctl(['GetThrottlerConfiguration',
                                 '--server', throttler_server],
                                auto_log=True, trap_output=True)
    for name in names:
      # The max should be set and have a non-zero value.
      # We test only the the first field 'target_replication_lag_sec'.
      self.assertIn('| %s | target_replication_lag_sec:12345 ' % (name), stdout)
      # protobuf omits fields with a zero value in the text output.
      self.assertNotIn('ignore_n_slowest_replicas', stdout)
    self.assertIn('%d active throttler(s)' % len(names), stdout)

    # Reset clears our configuration values.
    stdout, _ = utils.run_vtctl(['ResetThrottlerConfiguration',
                                 '--server', throttler_server],
                                auto_log=True, trap_output=True)
    self.assertIn('%d active throttler(s)' % len(names), stdout)
    # Check that the reset configuration no longer has our values.
    stdout, _ = utils.run_vtctl(['GetThrottlerConfiguration',
                                 '--server', throttler_server],
                                auto_log=True, trap_output=True)
    for name in names:
      # Target lag value should no longer be 12345 and be back to the default.
      self.assertNotIn('target_replication_lag_sec:12345', stdout)
    self.assertIn('%d active throttler(s)' % len(names), stdout)

  def verify_reconciliation_counters(self, worker_port, online_or_offline,
                                     table, inserts, updates, deletes, equal):
    """Checks that the reconciliation Counters have the expected values."""
    worker_vars = utils.get_vars(worker_port)

    i = worker_vars['Worker' + online_or_offline + 'InsertsCounters']
    if inserts == 0:
      self.assertNotIn(table, i)
    else:
      self.assertEqual(i[table], inserts)

    u = worker_vars['Worker' + online_or_offline + 'UpdatesCounters']
    if updates == 0:
      self.assertNotIn(table, u)
    else:
      self.assertEqual(u[table], updates)

    d = worker_vars['Worker' + online_or_offline + 'DeletesCounters']
    if deletes == 0:
      self.assertNotIn(table, d)
    else:
      self.assertEqual(d[table], deletes)

    e = worker_vars['Worker' + online_or_offline + 'EqualRowsCounters']
    if equal == 0:
      self.assertNotIn(table, e)
    else:
      self.assertEqual(e[table], equal)
