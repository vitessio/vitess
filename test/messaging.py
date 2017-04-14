#!/usr/bin/env python
# coding: utf-8

import logging
import unittest

import environment
import keyspace_util
import utils

from vtproto import query_pb2

from vtdb import vtgate_client


shard_0_master = None
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
    shard_1_master = keyspace_env.tablet_map['user.80-.master']
    lookup_master = keyspace_env.tablet_map['lookup.0.master']

    utils.apply_vschema(vschema)
    utils.VtGate().start(
        tablets=[shard_0_master, shard_1_master, lookup_master])
    utils.vtgate.wait_for_endpoints('user.-80.master', 1)
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
        [('id', query_pb2.INT64), ('message', query_pb2.VARCHAR)])

    # We should get both messages.
    result = {}
    for _ in xrange(2):
      row = it.next()
      result[row[0]] = row[1]
    self.assertEqual(result, {1: 'hello world 1', 4: 'hello world 4'})

    # After ack, we should get only one message.
    count = vtgate_conn.message_ack(name, [4])
    self.assertEqual(count, 1)
    result = {}
    for _ in xrange(2):
      row = it.next()
      result[row[0]] = row[1]
    self.assertEqual(result, {1: 'hello world 1'})
    # Only one should be acked.
    count = vtgate_conn.message_ack(name, [1, 4])
    self.assertEqual(count, 1)


if __name__ == '__main__':
  utils.main()
