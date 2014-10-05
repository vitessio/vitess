#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# This file contains integration tests for the go/vt/binlog package.
# It sets up filtered replication between two shards and checks how data flows
# through binlog streamer.

import logging
import unittest

import environment
import tablet
import utils
from mysql_flavor import mysql_flavor

from vtdb import keyrange_constants
from vtdb import update_stream_service

src_master = tablet.Tablet()
src_replica = tablet.Tablet()
dst_master = tablet.Tablet()
dst_replica = tablet.Tablet()


def setUpModule():
  try:
    environment.topo_server_setup()

    setup_procs = [
        src_master.init_mysql(),
        src_replica.init_mysql(),
        dst_master.init_mysql(),
        dst_replica.init_mysql(),
        ]
    utils.Vtctld().start()
    utils.wait_procs(setup_procs)

    # Set up binlog stream from shard 0 to shard 1.
    # Modeled after initial_sharding.py.
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', 'test_keyspace',
                     'keyspace_id', keyrange_constants.KIT_UINT64])

    src_master.init_tablet('master', 'test_keyspace', '0')
    src_replica.init_tablet('replica', 'test_keyspace', '0')

    utils.run_vtctl(['RebuildShardGraph', 'test_keyspace/0'])
    utils.validate_topology()

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    src_master.create_db('vt_test_keyspace')
    src_master.start_vttablet(wait_for_state=None)
    src_replica.create_db('vt_test_keyspace')
    src_replica.start_vttablet(wait_for_state=None)

    src_master.wait_for_vttablet_state('SERVING')
    src_replica.wait_for_vttablet_state('SERVING')

    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/0',
                     src_master.tablet_alias], auto_log=True)

    # Create schema
    logging.debug("Creating schema...")
    create_table = '''create table test_table(
        id bigint auto_increment,
        keyspace_id bigint(20) unsigned,
        msg varchar(64),
        primary key (id),
        index by_msg (msg)
        ) Engine=InnoDB'''

    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + create_table,
                     'test_keyspace'], auto_log=True)

    # Create destination shard.
    dst_master.init_tablet('master', 'test_keyspace', '1')
    dst_replica.init_tablet('replica', 'test_keyspace', '1')
    dst_master.start_vttablet(wait_for_state='NOT_SERVING')
    dst_replica.start_vttablet(wait_for_state='NOT_SERVING')

    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/1',
                     dst_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # Start binlog stream from src_replica to dst_master.
    logging.debug("Starting binlog stream...")
    utils.run_vtctl(['MultiSnapshot', src_replica.tablet_alias], auto_log=True)
    src_replica.wait_for_binlog_server_state("Enabled")
    utils.run_vtctl(['ShardMultiRestore', '-strategy=populateBlpCheckpoint',
                     'test_keyspace/1', src_replica.tablet_alias], auto_log=True)
    dst_master.wait_for_binlog_player_count(1)

    # Wait for dst_replica to be ready.
    dst_replica.wait_for_binlog_server_state("Enabled")
  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return

  tablet.kill_tablets([src_master, src_replica, dst_master, dst_replica])

  teardown_procs = [
      src_master.teardown_mysql(),
      src_replica.teardown_mysql(),
      dst_master.teardown_mysql(),
      dst_replica.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  src_master.remove_tree()
  src_replica.remove_tree()
  dst_master.remove_tree()
  dst_replica.remove_tree()


def _get_update_stream(tblt):
  return update_stream_service.UpdateStreamConnection('localhost:%u' %
                                                      tblt.port, 30)


class TestBinlog(unittest.TestCase):

  def test_charset(self):
    start_position = mysql_flavor().master_position(dst_replica)
    logging.debug('test_charset: starting @ %s', start_position)

    # Insert something that will replicate incorrectly if the charset is not
    # propagated through binlog streamer to the destination.
    #
    # Vitess tablets default to using utf8, so we insert something crazy and
    # pretend it's latin1. If the binlog player doesn't also pretend it's
    # latin1, it will be inserted as utf8, which will change its value.
    src_master.mquery("vt_test_keyspace",
        "INSERT INTO test_table (id, keyspace_id, msg) VALUES (41523, 1, 'Šṛ́rỏé') /* EMD keyspace_id:1 */",
        conn_params={'charset': 'latin1'}, write=True)

    # Wait for it to replicate.
    stream = _get_update_stream(dst_replica)
    stream.dial()
    data = stream.stream_start(start_position)
    while data:
      if data['Category'] == 'POS':
        break
      data = stream.stream_next()
    stream.close()

    # Check the value.
    data = dst_master.mquery("vt_test_keyspace",
        "SELECT id, keyspace_id, msg FROM test_table WHERE id=41523 LIMIT 1")
    self.assertEqual(len(data), 1, 'No data replicated.')
    self.assertEqual(len(data[0]), 3, 'Wrong number of columns.')
    self.assertEqual(data[0][2], 'Šṛ́rỏé', 'Data corrupted due to wrong charset.')


if __name__ == '__main__':
  utils.main()
