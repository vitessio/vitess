#!/usr/bin/env python
# coding: utf-8

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


"""Tests that VTGate buffers master traffic when expected."""

import logging
import struct
import unittest

from vtdb import keyrange
from vtdb import vtgate_client

import environment
import tablet
import utils


shard_0_master = tablet.Tablet()
shard_0_replica1 = tablet.Tablet()

KEYSPACE_NAME = 'test_keyspace'
SHARD_NAMES = ['0']
SHARD_KID_MAP = {
    '0': [
        527875958493693904, 626750931627689502,
        345387386794260318, 332484755310826578,
        1842642426274125671, 1326307661227634652,
        1761124146422844620, 1661669973250483744,
        3361397649937244239, 2444880764308344533,
        9767889778372766922, 9742070682920810358,
        10296850775085416642, 9537430901666854108,
        10440455099304929791, 11454183276974683945,
        11185910247776122031, 10460396697869122981,
        13379616110062597001, 12826553979133932576],
}

CREATE_VT_INSERT_TEST = '''create table vt_insert_test (
  id bigint auto_increment,
  msg varchar(64),
  keyspace_id bigint(20) unsigned NOT NULL,
  primary key (id)
) Engine=InnoDB'''

create_tables = [
    CREATE_VT_INSERT_TEST,
]
pack_kid = struct.Struct('!Q').pack


def setUpModule():
  logging.debug('in setUpModule')
  try:
    environment.topo_server().setup()

    # start mysql instance external to the test
    setup_procs = [shard_0_master.init_mysql(),
                   shard_0_replica1.init_mysql(),
                  ]
    utils.wait_procs(setup_procs)
    setup_tablets()
    setup_vtgate()
    # After VTGate comes up, populate it with some initial data
    initial_writes(0, keyrange.KeyRange(''))
  except Exception, e:
    logging.exception('error during set up: %s', e)
    tearDownModule()
    raise


def tearDownModule():
  logging.debug('in tearDownModule')
  utils.required_teardown()
  if utils.options.skip_teardown:
    return
  logging.debug('Tearing down the servers and setup')
  tablet.kill_tablets([shard_0_master,
                       shard_0_replica1])
  teardown_procs = [shard_0_master.teardown_mysql(),
                    shard_0_replica1.teardown_mysql(),
                   ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()

  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica1.remove_tree()


def setup_tablets():
  # Start up a master mysql and vttablet
  logging.debug('Setting up tablets')
  utils.run_vtctl(['CreateKeyspace', KEYSPACE_NAME])
  utils.run_vtctl(['SetKeyspaceShardingInfo', '-force', KEYSPACE_NAME,
                   'keyspace_id', 'uint64'])
  shard_0_master.init_tablet(
      'replica',
      keyspace=KEYSPACE_NAME,
      shard='0',
      tablet_index=0)
  shard_0_replica1.init_tablet(
      'replica',
      keyspace=KEYSPACE_NAME,
      shard='0',
      tablet_index=1)

  for t in [shard_0_master, shard_0_replica1]:
    t.create_db('vt_test_keyspace')
    for create_table in create_tables:
      t.mquery(shard_0_master.dbname, create_table)
    t.start_vttablet(wait_for_state=None)

  for t in [shard_0_master, shard_0_replica1]:
    t.wait_for_vttablet_state('NOT_SERVING')

  utils.run_vtctl(['InitShardMaster', '-force', KEYSPACE_NAME+'/0',
                   shard_0_master.tablet_alias], auto_log=True)

  for t in [shard_0_replica1]:
    utils.wait_for_tablet_type(t.tablet_alias, 'replica')

  for t in [shard_0_master, shard_0_replica1]:
    t.wait_for_vttablet_state('SERVING')

  utils.check_srv_keyspace(
      'test_nj', KEYSPACE_NAME,
      'Partitions(master): -\n'
      'Partitions(rdonly): -\n'
      'Partitions(replica): -\n')


def setup_vtgate(port=None, extra_args=None):
  utils.VtGate(port=port).start(
      extra_args=extra_args,
      tablets=[shard_0_master, shard_0_replica1])
  utils.vtgate.wait_for_endpoints(
      '%s.%s.master' % (KEYSPACE_NAME, SHARD_NAMES[0]),
      1)
  utils.vtgate.wait_for_endpoints(
      '%s.%s.replica' % (KEYSPACE_NAME, SHARD_NAMES[0]),
      1)


def initial_writes(shard_index, writes_keyrange):
  vtgate_conn = get_connection()
  _delete_all('vt_insert_test')
  count = 10
  kid_list = SHARD_KID_MAP[SHARD_NAMES[shard_index]]
  for x in xrange(count):
    keyspace_id = kid_list[count%len(kid_list)]
    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyspace_ids=[pack_kid(keyspace_id)],
        writable=True)
    cursor.begin()
    cursor.execute(
        'insert into vt_insert_test (msg, keyspace_id) '
        'values (:msg, :keyspace_id)',
        {'msg': 'test %s' % x, 'keyspace_id': keyspace_id})
    cursor.commit()
  cursor = vtgate_conn.cursor(
      tablet_type='master', keyspace=KEYSPACE_NAME,
      keyranges=[writes_keyrange])
  rowcount = cursor.execute('select * from vt_insert_test', {})
  assert rowcount == count, 'master fetch works'


def get_connection(timeout=10.0):
  protocol, endpoint = utils.vtgate.rpc_endpoint(python=True)
  try:
    return vtgate_client.connect(protocol, endpoint, timeout)
  except Exception:
    logging.exception('Connection to vtgate (timeout=%s) failed.', timeout)
    raise


def _delete_all(table_name):
  vtgate_conn = get_connection()
  # This write is to set up the test with fresh insert
  # and hence performing it directly on the connection.
  vtgate_conn.begin()
  vtgate_conn._execute(
      'delete from %s' % table_name, {},
      tablet_type='master', keyspace_name=KEYSPACE_NAME,
      keyranges=[keyrange.KeyRange('')])
  vtgate_conn.commit()


def restart_vtgate(extra_args=None):
  if extra_args is None:
    extra_args = []
  port = utils.vtgate.port
  utils.vtgate.kill()
  setup_vtgate(port=port, extra_args=extra_args)


class BaseTestCase(unittest.TestCase):

  def setUp(self):
    super(BaseTestCase, self).setUp()
    logging.info('Start: %s.', '.'.join(self.id().split('.')[-2:]))


# TODO(liguo): once we have the final master buffering code in place, these
# tests should verify that we only buffer when the master is unavailable.
class TestMasterBuffering(BaseTestCase):

  shard_index = 0
  keyrange = keyrange.KeyRange('')

  def setUp(self):
    super(TestMasterBuffering, self).setUp()
    restart_vtgate(extra_args=[
        '-enable_fake_master_buffer',
        '-buffer_keyspace', KEYSPACE_NAME,
        '-buffer_shard', SHARD_NAMES[self.shard_index],
        '-fake_buffer_delay', '1ms',
        ])

  def get_sucessful_buffered_requests(self):
    return utils.vtgate.get_vars()['BufferedRequestsSuccessful']

  def test_tx_is_buffered(self):
    """Tests that for a transaction, we buffer exactly one request."""
    vtgate_conn = get_connection()
    kid_list = SHARD_KID_MAP[SHARD_NAMES[self.shard_index]]
    keyspace_id = kid_list[0]

    initial_buffered = self.get_sucessful_buffered_requests()

    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyspace_ids=[pack_kid(keyspace_id)],
        writable=True)
    cursor.begin()
    cursor.execute(
        'insert into vt_insert_test (msg, keyspace_id) '
        'values (:msg, :keyspace_id)',
        {'msg': 'test %s' % 1000, 'keyspace_id': keyspace_id})
    cursor.execute('select * from vt_insert_test', {})
    cursor.rollback()

    num_buffered = self.get_sucessful_buffered_requests() - initial_buffered
    # No matter how many requests there were in the transaction, we should only
    # buffer one request (the Begin to the vttablet).
    self.assertEqual(num_buffered, 1)

  def test_master_read_is_buffered(self):
    """Tests that we buffer master reads."""
    vtgate_conn = get_connection()
    kid_list = SHARD_KID_MAP[SHARD_NAMES[self.shard_index]]
    keyspace_id = kid_list[0]

    initial_buffered = self.get_sucessful_buffered_requests()

    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=KEYSPACE_NAME,
        keyspace_ids=[pack_kid(keyspace_id)])
    cursor.execute('select * from vt_insert_test', {})

    num_buffered = self.get_sucessful_buffered_requests() - initial_buffered
    self.assertEqual(num_buffered, 1)

  def test_replica_read_is_not_buffered(self):
    """Tests that we do not buffer replica reads."""
    vtgate_conn = get_connection()

    initial_buffered = self.get_sucessful_buffered_requests()
    vtgate_conn._execute(
        'select * from vt_insert_test', {},
        tablet_type='replica', keyspace_name=KEYSPACE_NAME,
        keyranges=[self.keyrange]
        )
    num_buffered = self.get_sucessful_buffered_requests() - initial_buffered
    self.assertEqual(num_buffered, 0)


if __name__ == '__main__':
  utils.main()
