#!/usr/bin/env python

# Copyright 2019 The Vitess Authors.
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

"""This test cell aliases feature

We start with no aliases and assert that vtgates can't route to replicas/rondly tablets.
Then we add an alias, and these tablets should be routable

"""

import threading
import time

import logging
import unittest

import base_sharding
import environment
import tablet
import utils

from vtproto import topodata_pb2
from vtdb import keyrange_constants
from vtdb import vtgate_client

use_alias = False

# initial shards
# range '' - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet(cell='ny')
shard_0_rdonly = tablet.Tablet(cell='ny')

#shard_0_replica = tablet.Tablet()
#shard_0_rdonly = tablet.Tablet()
# range 80 - ''
shard_1_master = tablet.Tablet()
#shard_1_replica = tablet.Tablet()
#shard_1_rdonly = tablet.Tablet()

shard_1_replica = tablet.Tablet(cell='ny')
shard_1_rdonly = tablet.Tablet(cell='ny')

all_tablets = ([shard_0_master, shard_0_replica, shard_0_rdonly,
                shard_1_master, shard_1_replica,shard_1_rdonly])

vschema = {
    'test_keyspace': '''{
      "sharded": true,
      "vindexes": {
        "hash_index": {
          "type": "hash"
        }
      },
      "tables": {
        "test_table": {
          "column_vindexes": [
            {
              "column": "custom_ksid_col",
              "name": "hash_index"
            }
          ]
        }
      }
    }''',
}

def setUpModule():
  try:
    environment.topo_server().setup()
    setup_procs = [t.init_mysql()
                   for t in all_tablets]
    utils.Vtctld().start()
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise

def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  teardown_procs = [t.teardown_mysql() for t in all_tablets]
  utils.wait_procs(teardown_procs, raise_on_error=False)
  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  for t in all_tablets:
    t.remove_tree()

class TestCellsAliases(unittest.TestCase, base_sharding.BaseShardingTest):

  int_type = 265

  # Gets a vtgate connection
  def _get_connection(self, timeout=10.0):
    protocol, endpoint = utils.vtgate.rpc_endpoint(python=True)
    try:
      return vtgate_client.connect(protocol, endpoint, timeout)
    except Exception:
      logging.exception('Connection to vtgate (timeout=%s) failed.', timeout)
      raise

  # executes requetest in tablet type
  def _execute_on_tablet_type(self, vtgate_conn, tablet_type, sql, bind_vars):
    return vtgate_conn._execute(
        sql, bind_vars, tablet_type=tablet_type, keyspace_name=None)


  # create_schema will create the same schema on the keyspace
  # then insert some values
  def _create_schema(self):
    if base_sharding.keyspace_id_type == keyrange_constants.KIT_BYTES:
      t = 'varbinary(64)'
    else:
      t = 'bigint(20) unsigned'
    # Note that the primary key columns are not defined first on purpose to test
    # that a reordered column list is correctly used everywhere in vtworker.
    create_table_template = '''create table %s(
custom_ksid_col ''' + t + ''' not null,
msg varchar(64),
id bigint not null,
parent_id bigint not null,
primary key (parent_id, id),
index by_msg (msg)
) Engine=InnoDB'''

    utils.run_vtctl(['ApplySchema',
                     '-sql=' + create_table_template % ('test_table'),
                     'test_keyspace'],
                    auto_log=True)

  def _insert_startup_values(self):
    self._insert_value(shard_0_master, 'test_table', 1, 'msg1',
                       0x1000000000000000)
    self._insert_value(shard_1_master, 'test_table', 2, 'msg2',
                       0x9000000000000000)
    self._insert_value(shard_1_master, 'test_table', 3, 'msg3',
                       0xD000000000000000)

  def test_cells_aliases(self):
    utils.run_vtctl(['CreateKeyspace',
                     '--sharding_column_name', 'custom_ksid_col',
                     '--sharding_column_type', base_sharding.keyspace_id_type,
                     'test_keyspace'])

    shard_0_master.init_tablet('replica', 'test_keyspace', '-80')
    shard_0_replica.init_tablet('replica', 'test_keyspace', '-80')
    shard_0_rdonly.init_tablet('rdonly', 'test_keyspace', '-80')
    shard_1_master.init_tablet('replica', 'test_keyspace', '80-')
    shard_1_replica.init_tablet('replica', 'test_keyspace', '80-')
    shard_1_rdonly.init_tablet('rdonly', 'test_keyspace', '80-')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    ks = utils.run_vtctl_json(['GetSrvKeyspace', 'test_nj', 'test_keyspace'])
    self.assertEqual(ks['sharding_column_name'], 'custom_ksid_col')

    # we set full_mycnf_args to True as a test in the KIT_BYTES case
    full_mycnf_args = (base_sharding.keyspace_id_type ==
                       keyrange_constants.KIT_BYTES)

    # create databases so vttablet can start behaving somewhat normally
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly,
              shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None, full_mycnf_args=full_mycnf_args,
                       binlog_use_v3_resharding_mode=False)

    # wait for the tablets (replication is not setup, they won't be healthy)
    for t in [shard_0_master, shard_0_replica, shard_0_rdonly,
              shard_1_master, shard_1_replica, shard_1_rdonly]:
      t.wait_for_vttablet_state('NOT_SERVING')

    # reparent to make the tablets work
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)

    # check the shards
    shards = utils.run_vtctl_json(['FindAllShardsInKeyspace', 'test_keyspace'])
    self.assertIn('-80', shards, 'unexpected shards: %s' % str(shards))
    self.assertIn('80-', shards, 'unexpected shards: %s' % str(shards))
    self.assertEqual(len(shards), 2, 'unexpected shards: %s' % str(shards))

    # create the tables
    self._create_schema()
    self._insert_startup_values()


    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # Make sure srv keyspace graph looks as expected
    utils.check_srv_keyspace(
        'test_nj', 'test_keyspace',
        'Partitions(master): -80 80-\n'
        'Partitions(rdonly): -80 80-\n'
        'Partitions(replica): -80 80-\n',
        keyspace_id_type=base_sharding.keyspace_id_type,
        sharding_column_name='custom_ksid_col')

    utils.check_srv_keyspace(
        'test_ny', 'test_keyspace',
        'Partitions(master): -80 80-\n'
        'Partitions(rdonly): -80 80-\n'
        'Partitions(replica): -80 80-\n',
        keyspace_id_type=base_sharding.keyspace_id_type,
        sharding_column_name='custom_ksid_col')

    # Bootstrap vtgate

    utils.apply_vschema(vschema)

    # Adds alias so vtgate can route to replica/rdonly tablets that are not in the same cell, but same alias

    if use_alias:
      utils.run_vtctl(['AddCellsAlias', '-cells', 'test_nj,test_ny','region_east_coast'], auto_log=True)
      tablet_types_to_wait='MASTER,REPLICA'
    else:
      tablet_types_to_wait='MASTER'

    utils.VtGate().start(
      tablets=[shard_0_master, shard_1_master],
      tablet_types_to_wait=tablet_types_to_wait,
      cells_to_watch='test_nj,test_ny',
    )
    utils.vtgate.wait_for_endpoints('test_keyspace.-80.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.80-.master', 1)

    vtgate_conn = self._get_connection()
    result = self._execute_on_tablet_type(
        vtgate_conn,
        'master',
        'select count(*) from test_table', {})
    self.assertEqual(
        result,
        ([(3,)], 1, 0,
         [('count(*)', self.int_type)]))

    if use_alias:
      vtgate_conn = self._get_connection()
      result = self._execute_on_tablet_type(
          vtgate_conn,
          'master',
          'select count(*) from test_table', {})
      self.assertEqual(
          result,
          ([(3,)], 1, 0,
           [('count(*)', self.int_type)]))

      vtgate_conn = self._get_connection()
      result = self._execute_on_tablet_type(
          vtgate_conn,
          'replica',
          'select count(*) from test_table', {})
      self.assertEqual(
          result,
          ([(3,)], 1, 0,
           [('count(*)', self.int_type)]))

      vtgate_conn = self._get_connection()
      result = self._execute_on_tablet_type(
          vtgate_conn,
          'rdonly',
          'select count(*) from test_table', {})
      self.assertEqual(
          result,
          ([(3,)], 1, 0,
           [('count(*)', self.int_type)]))
    else:
      vtgate_conn = self._get_connection()
      try:
        self._execute_on_tablet_type(
            vtgate_conn,
            'replica',
            'select count(*) from test_table', {})
        self.fail('Expected execute to fail, did not get error')
      except Exception as e:
        s = str(e)
        self.assertIn('80.replica, no valid tablet: node', s)

      vtgate_conn = self._get_connection()
      try:
        self._execute_on_tablet_type(
            vtgate_conn,
            'rdonly',
            'select count(*) from test_table', {})
        self.fail('Expected execute to fail, did not get error')
      except Exception as e:
        s = str(e)
        self.assertIn('80.rdonly, no valid tablet: node', s)

if __name__ == '__main__':
  utils.main()
