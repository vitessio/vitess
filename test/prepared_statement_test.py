#!/usr/bin/env python
#
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

"""Ensures the vtgate MySQL server protocol plugin works as expected with prepared statments.

We use table ACLs to verify the user name authenticated by the connector is
set properly.
"""

import datetime
import socket
import unittest

import mysql.connector
from mysql.connector import FieldType
from mysql.connector.cursor import MySQLCursorPrepared
from mysql.connector.errors import Error

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


json_example = '''{
    "quiz": {
        "sport": {
            "q1": {
                "question": "Which one is correct team name in NBA?",
                "options": [
                    "New York Bulls",
                    "Los Angeles Kings",
                    "Golden State Warriors",
                    "Huston Rocket"
                ],
                "answer": "Huston Rocket"
            }
        },
        "maths": {
            "q1": {
                "question": "5 + 7 = ?",
                "options": [
                    "10",
                    "11",
                    "12",
                    "13"
                ],
                "answer": "12"
            },
            "q2": {
                "question": "12 - 8 = ?",
                "options": [
                    "1",
                    "2",
                    "3",
                    "4"
                ],
                "answer": "4"
            }
        }
    }
}'''

insert_stmt = '''insert into vt_prepare_stmt_test values(%s,  %s,  %s,  %s,  %s,  %s,  %s,  
  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s, %s,  %s,  %s,  %s,  %s,  %s, %s, %s, %s)'''

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


create_vt_prepare_test = '''create table vt_prepare_stmt_test (
id bigint auto_increment,
msg varchar(64),
keyspace_id bigint(20) unsigned NOT NULL,
tinyint_unsigned TINYINT,
bool_signed BOOL,
smallint_unsigned SMALLINT,
mediumint_unsigned MEDIUMINT,
int_unsigned INT,
float_unsigned FLOAT(10,2),
double_unsigned DOUBLE(16,2),
decimal_unsigned DECIMAL,
t_date DATE,
t_datetime DATETIME,
t_time TIME,
t_timestamp TIMESTAMP,
c8 bit(8) DEFAULT NULL,
c16 bit(16) DEFAULT NULL,
c24 bit(24) DEFAULT NULL,
c32 bit(32) DEFAULT NULL,
c40 bit(40) DEFAULT NULL,
c48 bit(48) DEFAULT NULL,
c56 bit(56) DEFAULT NULL,
c63 bit(63) DEFAULT NULL,
c64 bit(64) DEFAULT NULL,
json_col JSON,
text_col TEXT,
data longblob,
primary key (id)
) Engine=InnoDB'''


class TestPreparedStatements(unittest.TestCase):
  """This test makes sure that prepared statements is working correctly.
  """

  def test_prepared_statements(self):
    with open(table_acl_config, 'w') as fd:
      fd.write("""{
      "table_groups": [
          {
             "table_names_or_prefixes": ["vt_prepare_stmt_test", "dual"],
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
    utils.run_vtctl(['ApplySchema', '-sql', create_vt_prepare_test,
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
                  db='test_keyspace',
                  use_pure=True)

    # 'vtgate client 1' is authorized to access vt_prepare_insert_test
    conn = mysql.connector.Connect(**params)
    cursor = conn.cursor()
    cursor.execute('select * from vt_prepare_stmt_test', {})
    cursor.fetchone()
    cursor.close()

    cursor = conn.cursor()
    try:
      cursor.execute('selet * from vt_prepare_stmt_test', {})
      cursor.close()
    except mysql.connector.Error as err:
      if err.errno == 1105:
        print "Captured the error"
      else:
        raise

    # Insert several rows using prepared statements
    text_value = "text" * 100 # Large text value
    largeComment = 'L' * ((4 * 1024) + 1) # Large blob
    
    # Set up the values for the prepared statement
    cursor = conn.cursor(cursor_class=MySQLCursorPrepared)
    for i in range(1, 100):
      insert_values = (i, str(i) + "21", i * 100, 127, 1, 32767, 8388607, 2147483647, 2.55, 64.9,55.5,
      datetime.date(2009, 5, 5), datetime.date(2009, 5, 5), datetime.datetime.now().time(), datetime.date(2009, 5, 5),
      1,1,1,1,1,1,1,1,1, json_example, text_value, largeComment)
      cursor.execute(insert_stmt, insert_values)

    cursor.fetchone()
    cursor.close()

    cursor = conn.cursor(cursor_class=MySQLCursorPrepared)
    cursor.execute('select * from vt_prepare_stmt_test where id = %s', (1,))
    result = cursor.fetchall()

    # Validate the query results.
    if cursor.rowcount != 1:
      self.fail('expected 1 row got ' + str(cursor.rowcount))
    
    if result[0][1] != "121":
      self.fail('Received incorrect value, wanted: 121, got ' + result[1])
    
    cursor.close()

    # Update a row using prepared statements
    updated_text_value = "text_col_msg"
    updated_data_value = "updated"

    cursor = conn.cursor(cursor_class=MySQLCursorPrepared)
    cursor.execute('update vt_prepare_stmt_test set data = %s , text_col = %s where id = %s', (updated_data_value, updated_text_value, 1))
    cursor.close()

    # Validate the update results
    cursor = conn.cursor(cursor_class=MySQLCursorPrepared)
    cursor.execute('select * from vt_prepare_stmt_test where id = %s', (1,))
    result = cursor.fetchone()
    if result[-1] != updated_data_value or result[-2] != updated_text_value:
      self.fail("Received incorrect values")
    cursor.close()

    # Delete from table using prepared statements
    cursor = conn.cursor(cursor_class=MySQLCursorPrepared)
    cursor.execute('delete from vt_prepare_stmt_test where text_col = %s', (text_value,))
    cursor.close()

    # Validate Deletion
    cursor = conn.cursor(cursor_class=MySQLCursorPrepared)
    cursor.execute('select count(*) from vt_prepare_stmt_test')
    res = cursor.fetchone()
    if res[0] != 1:
      self.fail("Delete failed")
    cursor.close()

if __name__ == '__main__':
  utils.main()
