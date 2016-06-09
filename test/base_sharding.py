#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""This module contains a base class and utility functions for sharding tests.
"""

import logging

import utils


class BaseShardingTest(object):
  """This base class uses unittest.TestCase methods to check various things.

  All sharding tests should inherit from this base class, and use the
  methods as needed.
  """

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
    self.assertIn('BinlogPlayerMapSize', v)
    self.assertEquals(v['BinlogPlayerMapSize'], len(source_shards))
    self.assertIn('BinlogPlayerSecondsBehindMaster', v)
    self.assertIn('BinlogPlayerSecondsBehindMasterMap', v)
    self.assertIn('BinlogPlayerSourceShardNameMap', v)
    shards = v['BinlogPlayerSourceShardNameMap'].values()
    self.assertEquals(sorted(shards), sorted(source_shards))
    self.assertIn('BinlogPlayerSourceTabletAliasMap', v)
    for i in xrange(len(source_shards)):
      self.assertIn('%d' % i, v['BinlogPlayerSourceTabletAliasMap'])
    if seconds_behind_master_max != 0:
      self.assertTrue(
          v['BinlogPlayerSecondsBehindMaster'] <
          seconds_behind_master_max,
          'BinlogPlayerSecondsBehindMaster is too high: %d > %d' % (
              v['BinlogPlayerSecondsBehindMaster'],
              seconds_behind_master_max))
      for i in xrange(len(source_shards)):
        self.assertTrue(
            v['BinlogPlayerSecondsBehindMasterMap']['%d' % i] <
            seconds_behind_master_max,
            'BinlogPlayerSecondsBehindMasterMap is too high: %d > %d' % (
                v['BinlogPlayerSecondsBehindMasterMap']['%d' % i],
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
    self.assertEqual(blp_stats['BinlogPlayerMapSize'], count)

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
    self.assertEqual(blp_stats['BinlogPlayerMapSize'],
                     stream_health['realtime_stats']['binlog_players_count'])
    self.assertEqual(blp_stats['BinlogPlayerSecondsBehindMaster'],
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
    self.assertIn('Binlog player state: Running', status)
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

    status = tablet_obj.get_status()
    self.assertIn('No binlog player is running', status)
    self.assertIn('</html>', status)

  def check_binlog_throttler(self, dest_master_addr, names, rate):
    """Checks that the throttler responds to RPC requests.

    We assume it was enabled by SplitClone with the flag --max_tps 9999.

    Args:
      dest_master_addr: vttablet endpoint. Format: host:port
      names: Names of the throttlers e.g. BinlogPlayer/0 or <keyspace>/<shard>.
      rate: Expected initial rate the throttler was started with.
    """
    stdout, _ = utils.run_vtctl(['ThrottlerMaxRates', '--server',
                                 dest_master_addr], auto_log=True,
                                trap_output=True)
    for name in names:
      self.assertIn('| %s | %d |' % (name, rate), stdout)
    self.assertIn('%d active throttler(s)' % len(names), stdout)

    # Check that it's possible to change the max rate on the throttler.
    new_rate = 'unlimited'
    stdout, _ = utils.run_vtctl(['ThrottlerSetMaxRate', '--server',
                                 dest_master_addr, new_rate],
                                auto_log=True, trap_output=True)
    self.assertIn('%d active throttler(s)' % len(names), stdout)
    stdout, _ = utils.run_vtctl(['ThrottlerMaxRates', '--server',
                                 dest_master_addr], auto_log=True,
                                trap_output=True)
    for name in names:
      self.assertIn('| %s | %s |' % (name, new_rate), stdout)
    self.assertIn('%d active throttler(s)' % len(names), stdout)
