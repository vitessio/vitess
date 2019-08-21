#!/usr/bin/env python

# Copyright 2019 The Vitess Authors
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

"""Tests vschema creation and manipulation
"""

import unittest

import environment
import logging
import tablet
import utils
import keyspace_util

from vtdb import dbexceptions
from vtdb import vtgate_cursor
from vtdb import vtgate_client

shard_0_master = None

keyspace_env = None

create_vt_user = '''create table vt_user (
id bigint,
name varchar(64),
primary key (id)
) Engine=InnoDB'''

create_main = '''create table main (
id bigint,
val varchar(128),
primary key(id)
) Engine=InnoDB'''

def setUpModule():
  global keyspace_env
  global shard_0_master
  
  try:
    environment.topo_server().setup()
    keyspace_env = keyspace_util.TestEnv()
    keyspace_env.launch(
        'user',
        ddls=[
            create_vt_user,
            create_main,
            ]
        )
    shard_0_master = keyspace_env.tablet_map['user.0.master']
    utils.VtGate().start(
        tablets=[shard_0_master],
        extra_args=['-vschema_ddl_authorized_users','%'],
    )
    utils.vtgate.wait_for_endpoints('user.0.master', 1)

  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return
  if keyspace_env:
    keyspace_env.teardown()

  environment.topo_server().teardown()

  utils.kill_sub_processes()
  utils.remove_tmp_files()

def get_connection(timeout=10.0):
  protocol, endpoint = utils.vtgate.rpc_endpoint(python=True)
  try:
    return vtgate_client.connect(protocol, endpoint, timeout)
  except Exception:
    logging.exception('Connection to vtgate (timeout=%s) failed.', timeout)
    raise

class TestDDLVSchema(unittest.TestCase):

  decimal_type = 18
  int_type = 265
  string_type = 6165
  varbinary_type = 10262
  
  def _test_queries(self,cursor,count=4):
    for x in range(count):
      i = x+1
      cursor.begin()
      cursor.execute(
          'insert into vt_user (id,name) values (:id,:name)',
          {'id': i, 'name': 'test %s' % i})
      cursor.commit()

    # Test select equal
    for x in range(count):
      i = x+1
      cursor.execute('select id, name from vt_user where id = :id', {'id': i})
      self.assertEqual(
          (cursor.fetchall(), cursor.rowcount, cursor.lastrowid,cursor.description),
          ([(i, 'test %s' % i)], 1, 0,[('id', self.int_type), ('name', self.string_type)]))

    cursor.begin()
    cursor.execute(
      'DELETE FROM vt_user',
      {}
    )
    cursor.commit()

  def _read_vschema(self, cursor):
    # Test Showing Tables
    cursor.execute(
      'SHOW VSCHEMA TABLES',{}
    )
    self.assertEqual(
      [ x[0] for x in cursor.fetchall() ],
      [ 'dual', 'main', 'vt_user' ],
    )

    # Test Showing Vindexes
    cursor.execute(
      'SHOW VSCHEMA VINDEXES',{}
    )
    self.assertEqual(
      [ x[0] for x in cursor.fetchall() ],
      [ ],
    )

  def _create_vschema(self,cursor):
    cursor.begin()
    cursor.execute(
      'ALTER VSCHEMA ADD TABLE vt_user',{}
    )
    cursor.execute(
      'ALTER VSCHEMA ADD TABLE main',{}
    )    
    cursor.commit()
    
  def test_unsharded_vschema(self):
    vtgate_conn = get_connection()
    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace=None, writable=True)
    # Test the blank database with no vschema
    self._test_queries(cursor)

    # Use the DDL to create an unsharded vschema and test again
    self._create_vschema(cursor)
    self._read_vschema(cursor)
    self._test_queries(cursor)  

if __name__ == '__main__':
  utils.main()
