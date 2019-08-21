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


import logging
import time
import unittest

import environment
import keyspace_util
import utils

from vtproto import query_pb2

from vtdb import vtgate_client


shard_0_master = None
shard_0_replica = None
shard_1_master = None
lookup_master = None

keyspace_env = None

create_sharded_message = '''create table sharded_message(
time_scheduled bigint,
id bigint,
time_next bigint,
epoch bigint,
time_created bigint,
time_acked bigint,
message varchar(128),
primary key(time_scheduled, id),
unique index id_idx(id),
index next_idx(time_next, epoch)
) comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'
'''

create_unsharded_message = '''create table unsharded_message(
time_scheduled bigint,
id bigint,
time_next bigint,
epoch bigint,
time_created bigint,
time_acked bigint,
message varchar(128),
primary key(time_scheduled, id),
unique index id_idx(id),
index next_idx(time_next, epoch)
) comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'
'''

vschema = {
    'user': '''{
      "sharded": true,
      "vindexes": {
        "hash_index": {
          "type": "hash"
        }
      },
      "tables": {
        "sharded_message": {
          "column_vindexes": [
            {
              "column": "id",
              "name": "hash_index"
            }
          ]
        }
      }
    }''',
    'lookup': '''{
      "sharded": false,
      "tables": {
        "unsharded_message": {
          "type": "sequence"
        }
      }
    }''',
}


def setUpModule():
  global keyspace_env
  global shard_0_master
  global shard_0_replica
  global shard_1_master
  global lookup_master
  logging.debug('in setUpModule')

  try:
    environment.topo_server().setup()
    logging.debug('Setting up tablets')
    keyspace_env = keyspace_util.TestEnv()
    keyspace_env.launch(
        'user',
        shards=['-80', '80-'],
        ddls=[
            create_sharded_message,
            ],
        )
    keyspace_env.launch(
        'lookup',
        ddls=[
            create_unsharded_message,
            ],
        )
    shard_0_master = keyspace_env.tablet_map['user.-80.master']
    shard_0_replica = keyspace_env.tablet_map['user.-80.replica.0']
    shard_1_master = keyspace_env.tablet_map['user.80-.master']
    lookup_master = keyspace_env.tablet_map['lookup.0.master']

    utils.apply_vschema(vschema)
    utils.VtGate().start(
        tablets=[shard_0_master, shard_0_replica, shard_1_master, lookup_master])
    utils.vtgate.wait_for_endpoints('user.-80.master', 1)
    utils.vtgate.wait_for_endpoints('user.-80.replica', 1)
    utils.vtgate.wait_for_endpoints('user.80-.master', 1)
    utils.vtgate.wait_for_endpoints('lookup.0.master', 1)
  except:
    tearDownModule()
    raise


def tearDownModule():
  logging.debug('in tearDownModule')
  utils.required_teardown()
  if utils.options.skip_teardown:
    return
  logging.debug('Tearing down the servers and setup')
  if keyspace_env:
    keyspace_env.teardown()

  environment.topo_server().teardown()

  utils.kill_sub_processes()
  utils.remove_tmp_files()


def get_connection(timeout=15.0):
  protocol, endpoint = utils.vtgate.rpc_endpoint(python=True)
  try:
    return vtgate_client.connect(protocol, endpoint, timeout)
  except Exception:
    logging.exception('Connection to vtgate (timeout=%s) failed.', timeout)
    raise


def get_client_count(tablet):
    debugvars = utils.get_vars(tablet.port)
    return debugvars['Messages'].get('sharded_message.ClientCount', 0)


class TestMessaging(unittest.TestCase):

  def test_sharded(self):
    self._test_messaging('sharded_message', 'user')

  def test_unsharded(self):
    self._test_messaging('unsharded_message', 'lookup')

  def _test_messaging(self, name, keyspace):
    vtgate_conn = get_connection()
    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=None, writable=True)

    query = 'insert into %s(id, message) values(:id, :message)'%(name)
    cursor.begin()
    # If sharded, these will go to two different shards.
    # If unsharded, they'll both be sent as a batch, but there's
    # no way to verify this because the vtgate_conn API abstracts
    # that part out and issues only one row at a time. However,
    # there are unit tests for that part.
    cursor.execute(query, {'id': 1, 'message': 'hello world 1'})
    cursor.execute(query, {'id': 4, 'message': 'hello world 4'})
    cursor.commit()

    (it, fields) = vtgate_conn.message_stream(
        keyspace, name)
    self.assertEqual(
        fields,
        [
          ('id', query_pb2.INT64),
          ('time_scheduled', query_pb2.INT64),
          ('message', query_pb2.VARCHAR),
        ])

    # We should get both messages.
    result = {}
    for _ in range(2):
      row = next(it)
      result[row[0]] = row[2]
    self.assertEqual(result, {1: 'hello world 1', 4: 'hello world 4'})

    # After ack, we should get only one message.
    count = vtgate_conn.message_ack(name, [4])
    self.assertEqual(count, 1)
    result = {}
    for _ in range(2):
      row = next(it)
      result[row[0]] = row[2]
    self.assertEqual(result, {1: 'hello world 1'})
    # Only one should be acked.
    count = vtgate_conn.message_ack(name, [1, 4])
    self.assertEqual(count, 1)
    it.close()

  def test_connections(self):
    name = 'sharded_message'
    keyspace = 'user'

    self.assertEqual(get_client_count(shard_0_master), 0)
    self.assertEqual(get_client_count(shard_1_master), 0)

    vtgate_conn1 = get_connection()
    (it1, fields1) = vtgate_conn1.message_stream(keyspace, name)
    self.assertEqual(get_client_count(shard_0_master), 1)
    self.assertEqual(get_client_count(shard_1_master), 1)

    vtgate_conn2 = get_connection()
    (it2, fields2) = vtgate_conn2.message_stream(keyspace, name)
    self.assertEqual(get_client_count(shard_0_master), 2)
    self.assertEqual(get_client_count(shard_1_master), 2)

    cursor = vtgate_conn1.cursor(
        tablet_type='master', keyspace=None, writable=True)
    query = 'insert into %s(id, message) values(:id, :message)'%(name)
    cursor.begin()
    cursor.execute(query, {'id': 2, 'message': 'hello world 2'})
    cursor.execute(query, {'id': 5, 'message': 'hello world 5'})
    cursor.commit()

    # Each connection should get one message.
    next(it1)
    next(it2)

    # Ack the messages.
    count = vtgate_conn1.message_ack(name, [2, 5])

    # After closing one stream, ensure vttablets have dropped it.
    it1.close()
    time.sleep(1)
    self.assertEqual(get_client_count(shard_0_master), 1)
    self.assertEqual(get_client_count(shard_1_master), 1)

    it2.close()

  def test_reparent(self):
    name = 'sharded_message'
    keyspace = 'user'

    # Start a stream. Use a timeout that's greater than how long
    # the test will take to run.
    vtgate_conn = get_connection(120)
    (it, fields) = vtgate_conn.message_stream(keyspace, name)
    self.assertEqual(get_client_count(shard_0_master), 1)
    self.assertEqual(get_client_count(shard_0_replica), 0)
    self.assertEqual(get_client_count(shard_1_master), 1)

    # Perform a graceful reparent to the replica.
    utils.run_vtctl(['PlannedReparentShard',
                     '-keyspace_shard', 'user/-80',
                     '-new_master', shard_0_replica.tablet_alias], auto_log=True)
    utils.validate_topology()

    # Verify connection has migrated.
    # The wait must be at least 6s which is how long vtgate will
    # wait before retrying: that is 30s/5 where 30s is the default
    # message_stream_grace_period.
    time.sleep(10)
    self.assertEqual(get_client_count(shard_0_master), 0)
    self.assertEqual(get_client_count(shard_0_replica), 1)
    self.assertEqual(get_client_count(shard_1_master), 1)

    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=None, writable=True)
    query = 'insert into %s(id, message) values(:id, :message)'%(name)
    cursor.begin()
    cursor.execute(query, {'id': 3, 'message': 'hello world 3'})
    cursor.commit()

    # Receive the message.
    next(it)

    # Reparent back to old master.
    utils.run_vtctl(['PlannedReparentShard',
                     '-keyspace_shard', 'user/-80',
                     '-new_master', shard_0_master.tablet_alias], auto_log=True)
    utils.validate_topology()

    time.sleep(10)
    self.assertEqual(get_client_count(shard_0_master), 1)
    self.assertEqual(get_client_count(shard_0_replica), 0)
    self.assertEqual(get_client_count(shard_1_master), 1)

    # Ack the message.
    count = vtgate_conn.message_ack(name, [3])


if __name__ == '__main__':
  utils.main()
