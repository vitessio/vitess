#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""Ensures the vtgate MySQL server protocol plugin works as expected.

We use table ACLs to verify the user name authenticated by the connector is
set properly.
"""


import unittest

import MySQLdb

import environment
import utils
import tablet

# single shard / 2 tablets
shard_0_master = tablet.Tablet()
shard_0_slave = tablet.Tablet()

table_acl_config = environment.tmproot + '/table_acl_config.json'
mysql_auth_server_config = (environment.tmproot +
                            '/mysql_auth_server_config.json')


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
primary key (id)
) Engine=InnoDB'''


class TestMySQL(unittest.TestCase):
  """This test makes sure the MySQL server connector is correct.
  """

  def test_mysql_connector(self):
    with open(table_acl_config, 'w') as fd:
      fd.write("""{
      "table_groups": [
          {
             "table_names_or_prefixes": ["vt_insert_test"],
             "readers": ["vtgate client 1"],
             "writers": ["vtgate client 1"],
             "admins": ["vtgate client 1"]
          }
      ]
}
""")

    with open(mysql_auth_server_config, 'w') as fd:
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
        extra_args=['-mysql_auth_server_impl', 'config',
                    '-mysql_auth_server_config_file', mysql_auth_server_config])
    params = dict(host='::',
                  port=utils.vtgate.mysql_port,
                  user='testuser1',
                  passwd='testpassword1',
                  db='test_keyspace')

    # 'vtgate client 1' is authorized to access vt_insert_test
    conn = MySQLdb.Connect(**params)
    cursor = conn.cursor()
    cursor.execute('select * from vt_insert_test', {})
    conn.close()

    # 'vtgate client 2' is not authorized to access vt_insert_test
    params['user'] = 'testuser2'
    params['passwd'] = 'testpassword2'
    conn = MySQLdb.Connect(**params)
    try:
      cursor = conn.cursor()
      cursor.execute('select * from vt_insert_test', {})
      self.fail('Execute went through')
    except MySQLdb.OperationalError, e:
      s = str(e)
      self.assertIn('table acl error', s)
      self.assertIn('cannot run PASS_SELECT on table', s)
    conn.close()


if __name__ == '__main__':
  utils.main()
