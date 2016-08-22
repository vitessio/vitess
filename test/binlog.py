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

from vtdb import keyrange_constants
from vtdb import update_stream

import environment
import tablet
import utils
from mysql_flavor import mysql_flavor

src_master = tablet.Tablet()
src_replica = tablet.Tablet()
src_rdonly = tablet.Tablet()
dst_master = tablet.Tablet()
dst_replica = tablet.Tablet()
dst_rdonly = tablet.Tablet()

all_tablets = [src_master, src_replica, src_rdonly, dst_master, dst_replica,
               dst_rdonly]


def setUpModule():
  try:
    environment.topo_server().setup()

    setup_procs = [t.init_mysql() for t in all_tablets]
    utils.Vtctld().start()
    utils.wait_procs(setup_procs)

    # Set up binlog stream from shard 0 to shard 1.
    # Modeled after initial_sharding.py.
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', 'test_keyspace',
                     'keyspace_id', keyrange_constants.KIT_UINT64])

    src_master.init_tablet('master', 'test_keyspace', '0')
    src_replica.init_tablet('replica', 'test_keyspace', '0')
    src_rdonly.init_tablet('rdonly', 'test_keyspace', '0')

    utils.validate_topology()

    for t in [src_master, src_replica, src_rdonly]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None)

    src_master.wait_for_vttablet_state('SERVING')
    for t in [src_replica, src_rdonly]:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', 'test_keyspace/0',
                     src_master.tablet_alias], auto_log=True)

    # Create schema
    logging.debug('Creating schema...')
    create_table = '''create table test_table(
        id bigint auto_increment,
        keyspace_id bigint(20) unsigned,
        msg varchar(64),
        primary key (id),
        index by_msg (msg)
        ) Engine=InnoDB'''

    utils.run_vtctl(['ApplySchema',
                     '-sql=' + create_table,
                     'test_keyspace'], auto_log=True)

    # run a health check on source replica so it responds to discovery
    # (for binlog players) and on the source rdonlys (for workers)
    utils.run_vtctl(['RunHealthCheck', src_replica.tablet_alias])
    utils.run_vtctl(['RunHealthCheck', src_rdonly.tablet_alias])

    # Create destination shard (won't be serving as there is no DB)
    dst_master.init_tablet('master', 'test_keyspace', '-')
    dst_replica.init_tablet('replica', 'test_keyspace', '-')
    dst_rdonly.init_tablet('rdonly', 'test_keyspace', '-')
    dst_master.start_vttablet(wait_for_state='NOT_SERVING')
    dst_replica.start_vttablet(wait_for_state='NOT_SERVING')
    dst_rdonly.start_vttablet(wait_for_state='NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', 'test_keyspace/-',
                     dst_master.tablet_alias], auto_log=True)

    # copy the schema
    utils.run_vtctl(['CopySchemaShard', src_replica.tablet_alias,
                     'test_keyspace/-'], auto_log=True)

    # run the clone worker (this is a degenerate case, source and destination
    # both have the full keyrange. Happens to work correctly).
    logging.debug('Running the clone worker to start binlog stream...')
    utils.run_vtworker(['--cell', 'test_nj',
                        'SplitClone',
                        '--chunk_count', '10',
                        '--min_rows_per_chunk', '1',
                        '--min_healthy_rdonly_tablets', '1',
                        'test_keyspace/0'],
                       auto_log=True)
    dst_master.wait_for_binlog_player_count(1)

    # Wait for dst_replica to be ready.
    dst_replica.wait_for_binlog_server_state('Enabled')
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  tablet.kill_tablets(all_tablets)

  teardown_procs = [t.teardown_mysql() for t in all_tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for t in all_tablets:
    t.remove_tree()


def _get_update_stream(tblt):
  protocol, endpoint = tblt.update_stream_python_endpoint()
  return update_stream.connect(protocol, endpoint, 30)


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
    src_master.mquery(
        'vt_test_keyspace',
        "INSERT INTO test_table (id, keyspace_id, msg) "
        "VALUES (41523, 1, 'Šṛ́rỏé') /* vtgate:: keyspace_id:00000001 */",
        conn_params={'charset': 'latin1'}, write=True)

    # Wait for it to replicate.
    stream = _get_update_stream(dst_replica)
    for stream_event in stream.stream_update(start_position):
      if stream_event.category == update_stream.StreamEvent.POS:
        break
    stream.close()

    # Check the value.
    data = dst_master.mquery(
        'vt_test_keyspace',
        'SELECT id, keyspace_id, msg FROM test_table WHERE id=41523 LIMIT 1')
    self.assertEqual(len(data), 1, 'No data replicated.')
    self.assertEqual(len(data[0]), 3, 'Wrong number of columns.')
    self.assertEqual(data[0][2], 'Šṛ́rỏé',
                     'Data corrupted due to wrong charset.')

  def test_checksum_enabled(self):
    start_position = mysql_flavor().master_position(dst_replica)
    logging.debug('test_checksum_enabled: starting @ %s', start_position)

    # Enable binlog_checksum, which will also force a log rotation that should
    # cause binlog streamer to notice the new checksum setting.
    if not mysql_flavor().enable_binlog_checksum(dst_replica):
      logging.debug(
          'skipping checksum test on flavor without binlog_checksum setting')
      return

    # Insert something and make sure it comes through intact.
    sql = (
        "INSERT INTO test_table (id, keyspace_id, msg) "
        "VALUES (19283, 1, 'testing checksum enabled') "
        "/* vtgate:: keyspace_id:00000001 */")
    src_master.mquery('vt_test_keyspace', sql, write=True)

    # Look for it using update stream to see if binlog streamer can talk to
    # dst_replica, which now has binlog_checksum enabled.
    stream = _get_update_stream(dst_replica)
    found = False
    for stream_event in stream.stream_update(start_position):
      if stream_event.category == update_stream.StreamEvent.POS:
        break
      if stream_event.sql == sql:
        found = True
        break
    stream.close()
    self.assertEqual(found, True, 'expected query not found in update stream')

  def test_checksum_disabled(self):
    # Disable binlog_checksum to make sure we can also talk to a server without
    # checksums enabled, in case they are enabled by default.
    start_position = mysql_flavor().master_position(dst_replica)
    logging.debug('test_checksum_disabled: starting @ %s', start_position)

    # For flavors that don't support checksums, this is a no-op.
    mysql_flavor().disable_binlog_checksum(dst_replica)

    # Insert something and make sure it comes through intact.
    sql = (
        "INSERT INTO test_table (id, keyspace_id, msg) "
        "VALUES (58812, 1, 'testing checksum disabled') "
        "/* vtgate:: keyspace_id:00000001 */")
    src_master.mquery(
        'vt_test_keyspace', sql, write=True)

    # Look for it using update stream to see if binlog streamer can talk to
    # dst_replica, which now has binlog_checksum disabled.
    stream = _get_update_stream(dst_replica)
    found = False
    for stream_event in stream.stream_update(start_position):
      if stream_event.category == update_stream.StreamEvent.POS:
        break
      if stream_event.sql == sql:
        found = True
        break
    stream.close()
    self.assertEqual(found, True, 'expected query not found in update stream')


if __name__ == '__main__':
  utils.main()
