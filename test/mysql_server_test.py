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

"""Ensures the vtgate MySQL server protocol plugin works as expected.

We use table ACLs to verify the user name authenticated by the connector is
set properly.
"""


import socket
import unittest

import MySQLdb

import environment
import utils
import tablet
import warnings

# single shard / 2 tablets
shard_0_master = tablet.Tablet()
shard_0_slave = tablet.Tablet()

table_acl_config = environment.tmproot + '/table_acl_config.json'
mysql_auth_server_static = (environment.tmproot +
                            '/mysql_auth_server_static.json')


def setUpModule():
  try:
    environment.topo_server().setup()

    # setup all processes
    setup_procs = [
        shard_0_master.init_mysql(),
        shard_0_slave.init_mysql(),
        ]
    utils.wait_procs(setup_procs)

    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    shard_0_master.init_tablet('replica', 'test_keyspace', '0')
    shard_0_slave.init_tablet('replica', 'test_keyspace', '0')

    # create databases so vttablet can start behaving normally
    shard_0_master.create_db('vt_test_keyspace')
    shard_0_slave.create_db('vt_test_keyspace')

  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  shard_0_master.kill_vttablet()
  shard_0_slave.kill_vttablet()

  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_slave.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_slave.remove_tree()


create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
keyspace_id bigint(20) unsigned NOT NULL,
data longblob,
primary key (id)
) Engine=InnoDB'''


class TestMySQL(unittest.TestCase):
  """This test makes sure the MySQL server connector is correct.
  """

  MYSQL_OPTION_MULTI_STATEMENTS_ON = 0
  MYSQL_OPTION_MULTI_STATEMENTS_OFF = 1

  def test_mysql_connector(self):
    with open(table_acl_config, 'w') as fd:
      fd.write("""{
      "table_groups": [
          {
             "table_names_or_prefixes": ["vt_insert_test", "dual"],
             "readers": ["vtgate client 1"],
             "writers": ["vtgate client 1"],
             "admins": ["vtgate client 1"]
          }
      ]
}
""")

    with open(mysql_auth_server_static, 'w') as fd:
      fd.write("""{
      "testuser1": {
        "Password": "testpassword1",
        "UserData": "vtgate client 1"
      },
      "testuser2": {
        "Password": "testpassword2",
        "UserData": "vtgate client 2"
      }
}
""")

    # start the tablets
    shard_0_master.start_vttablet(wait_for_state='NOT_SERVING',
                                  table_acl_config=table_acl_config)
    shard_0_slave.start_vttablet(wait_for_state='NOT_SERVING',
                                 table_acl_config=table_acl_config)

    # setup replication
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['ApplySchema', '-sql', create_vt_insert_test,
                     'test_keyspace'])
    for t in [shard_0_master, shard_0_slave]:
      utils.run_vtctl(['RunHealthCheck', t.tablet_alias])

    # start vtgate
    utils.VtGate(mysql_server=True).start(
        extra_args=['-mysql_auth_server_impl', 'static',
                    '-mysql_server_query_timeout', '1s',
                    '-mysql_auth_server_static_file', mysql_auth_server_static])
    # We use gethostbyname('localhost') so we don't presume
    # of the IP format (travis is only IP v4, really).
    params = dict(host=socket.gethostbyname('localhost'),
                  port=utils.vtgate.mysql_port,
                  user='testuser1',
                  passwd='testpassword1',
                  db='test_keyspace')

    # 'vtgate client 1' is authorized to access vt_insert_test
    conn = MySQLdb.Connect(**params)
    cursor = conn.cursor()
    cursor.execute('select * from vt_insert_test', {})
    cursor.close()

    # Test multi-statement support. It should only work when
    # COM_SET_OPTION has set the options to 0
    conn.set_server_option(self.MYSQL_OPTION_MULTI_STATEMENTS_ON)
    cursor = conn.cursor()
    cursor.execute("select 1; select 2")
    self.assertEqual(((1,),), cursor.fetchall())
    self.assertEqual(1, cursor.nextset())
    self.assertEqual(((2,),), cursor.fetchall())
    self.assertEqual(None, cursor.nextset())
    cursor.close()
    conn.set_server_option(self.MYSQL_OPTION_MULTI_STATEMENTS_OFF)

    # Multi-statement support should not work without the
    # option enabled
    cursor = conn.cursor()
    try:
        cursor.execute("select 1; select 2")
        self.fail('Execute went through')
    except MySQLdb.OperationalError as e:
      s = str(e)
      self.assertIn('syntax error', s)
    cursor.close()

    # verify that queries work end-to-end with large grpc messages
    largeComment = 'L' * ((4 * 1024 * 1024) + 1)
    cursor = conn.cursor()
    cursor.execute('insert into vt_insert_test (id, msg, keyspace_id, data) values(%s, %s, %s, %s) /* %s */',
        (1, 'large blob', 123, 'LLL', largeComment))
    cursor.close()

    cursor = conn.cursor()
    cursor.execute('select * from vt_insert_test where id = 1');
    if cursor.rowcount != 1:
        self.fail('expected 1 row got ' + str(cursor.rowcount))

    for (id, msg, keyspace_id, blob) in cursor:
        if blob != 'LLL':
            self.fail('blob did not match \'LLL\'')

    cursor.close()

    hugeBlob = 'L' * (environment.grpc_max_message_size + 1)

    cursor = conn.cursor()
    try:
        cursor.execute('insert into vt_insert_test (id, msg, keyspace_id, data) values(%s, %s, %s, %s)',
            (2, 'huge blob', 123, hugeBlob))
        self.fail('Execute went through')
    except MySQLdb.OperationalError as e:
      s = str(e)
      self.assertIn('trying to send message larger than max', s)

    conn.close()

    # 'vtgate client' this query should timeout
    conn = MySQLdb.Connect(**params)
    try:
      cursor = conn.cursor()
      cursor.execute('SELECT SLEEP(5)', {})
      self.fail('Execute went through')
    except MySQLdb.OperationalError as e:
      s = str(e)
      # 1317 is DeadlineExceeded error code
      self.assertIn('1317', s)
    conn.close()

    # this query should fail due to the bogus field
    conn = MySQLdb.Connect(**params)
    try:
      cursor = conn.cursor()
      cursor.execute('SELECT invalid_field from vt_insert_test', {})
      self.fail('Execute went through')
    except MySQLdb.OperationalError as e:
      s = str(e)
      # 1054 is BadFieldError code
      self.assertIn('1054', s)

    # this query should trigger a warning not an error
    with warnings.catch_warnings(record=True) as w:
      warnings.simplefilter("always")

      cursor.execute('SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS */ invalid_field from vt_insert_test', {})
      if cursor.rowcount != 0:
        self.fail('expected 0 rows got ' + str(cursor.rowcount))

      if len(w) != 1:
        print('unexpected warnings: ', w)

    # and the next query should get the warnings
    cursor.execute('SHOW WARNINGS', {})
    if cursor.rowcount != 1:
      print('expected 1 warning row, got ' + str(cursor.rowcount))

    for (_, code, message) in cursor:
      self.assertEqual(code, 1054)
      self.assertIn('errno 1054', message)
      self.assertIn('Unknown column', message)

    # test with a query timeout error
    with warnings.catch_warnings(record=True) as w:
      warnings.simplefilter("always")

      cursor.execute('SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS QUERY_TIMEOUT_MS=1 */ sleep(1) from vt_insert_test', {})
      if cursor.rowcount != 0:
        self.fail('expected 0 rows got ' + str(cursor.rowcount))

      if len(w) != 1:
        print('unexpected warnings: ', w)

    cursor.execute('SHOW WARNINGS', {})
    if cursor.rowcount != 1:
      print('expected 1 warning row, got ' + str(cursor.rowcount))

    for (_, code, message) in cursor:
      self.assertEqual(code, 1317)
      self.assertIn('context deadline exceeded', message)

    # any non-show query clears the warnings
    cursor.execute('SELECT 1 from vt_insert_test limit 1', {})
    cursor.execute('SHOW WARNINGS', {})
    if cursor.rowcount != 0:
      print('expected 0 warnings row, got ' + str(cursor.rowcount))

    # 'vtgate client 2' is not authorized to access vt_insert_test
    params['user'] = 'testuser2'
    params['passwd'] = 'testpassword2'
    conn = MySQLdb.Connect(**params)
    try:
      cursor = conn.cursor()
      cursor.execute('select * from vt_insert_test', {})
      self.fail('Execute went through')
    except MySQLdb.OperationalError as e:
      s = str(e)
      self.assertIn('table acl error', s)
      self.assertIn('cannot run PASS_SELECT on table', s)
    conn.close()

if __name__ == '__main__':
  utils.main()
