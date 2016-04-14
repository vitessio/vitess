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
    utils.run_vtctl(['RunHealthCheck', tablet_obj.tablet_alias, 'replica'])
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
