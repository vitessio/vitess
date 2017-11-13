#!/usr/bin/env python

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

"""Tests a sharded setup works and routes queries correctly.
"""

import unittest

import environment
import tablet
import utils

# range "" - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
# range 80 - ""
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()


def setUpModule():
  try:
    environment.topo_server().setup()

    setup_procs = [
        shard_0_master.init_mysql(),
        shard_0_replica.init_mysql(),
        shard_1_master.init_mysql(),
        shard_1_replica.init_mysql(),
        ]
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_replica.teardown_mysql(),
      shard_1_master.teardown_mysql(),
      shard_1_replica.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica.remove_tree()

# both shards will have similar tables, but with different column order,
# so we can test column mismatches by doing a 'select *',
# and also check the good case by doing a 'select id, msg'
create_vt_select_test = '''create table vt_select_test (
id bigint not null,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

create_vt_select_test_reverse = '''create table vt_select_test (
msg varchar(64),
id bigint not null,
primary key (id)
) Engine=InnoDB'''


class TestSharded(unittest.TestCase):

  def test_sharding(self):

    utils.run_vtctl(['CreateKeyspace',
                     '--sharding_column_name', 'keyspace_id',
                     '--sharding_column_type', 'uint64',
                     'test_keyspace'])

    shard_0_master.init_tablet('replica', 'test_keyspace', '-80')
    shard_0_replica.init_tablet('replica', 'test_keyspace', '-80')
    shard_1_master.init_tablet('replica', 'test_keyspace', '80-')
    shard_1_replica.init_tablet('replica', 'test_keyspace', '80-')

    # create databases, start the tablets, wait for them to start
    for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
      t.create_db('vt_test_keyspace')
      t.start_vttablet(wait_for_state=None)
    for t in [shard_0_master, shard_1_master, shard_0_replica, shard_1_replica]:
      t.wait_for_vttablet_state('NOT_SERVING')

    # apply the schema on the first shard through vtctl, so all tablets
    # are the same.
    shard_0_master.mquery('vt_test_keyspace',
                          create_vt_select_test.replace('\n', ''), write=True)
    shard_0_replica.mquery('vt_test_keyspace',
                           create_vt_select_test.replace('\n', ''), write=True)

    # apply the schema on the second shard.
    shard_1_master.mquery(
        'vt_test_keyspace',
        create_vt_select_test_reverse.replace('\n', ''), write=True)
    shard_1_replica.mquery(
        'vt_test_keyspace',
        create_vt_select_test_reverse.replace('\n', ''), write=True)

    for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
      utils.run_vtctl(['ReloadSchema', t.tablet_alias])

    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/-80',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/80-',
                     shard_1_master.tablet_alias], auto_log=True)

    # insert some values directly (db is RO after minority reparent)
    # FIXME(alainjobart) these values don't match the shard map
    utils.run_vtctl(['SetReadWrite', shard_0_master.tablet_alias])
    utils.run_vtctl(['SetReadWrite', shard_1_master.tablet_alias])
    shard_0_master.mquery(
        'vt_test_keyspace',
        "insert into vt_select_test (id, msg) values (1, 'test 1')",
        write=True)
    shard_1_master.mquery(
        'vt_test_keyspace',
        "insert into vt_select_test (id, msg) values (10, 'test 10')",
        write=True)

    utils.validate_topology(ping_tablets=True)

    utils.pause('Before the sql scatter query')

    # make sure the '1' value was written on first shard
    rows = shard_0_master.mquery(
        'vt_test_keyspace', 'select id, msg from vt_select_test order by id')
    self.assertEqual(rows, ((1, 'test 1'),),
                     'wrong mysql_query output: %s' % str(rows))

    utils.pause('After db writes')

    # throw in some schema validation step
    # we created the schema differently, so it should show
    utils.run_vtctl(['ValidateSchemaShard', 'test_keyspace/-80'])
    utils.run_vtctl(['ValidateSchemaShard', 'test_keyspace/80-'])
    _, err = utils.run_vtctl(['ValidateSchemaKeyspace', 'test_keyspace'],
                             trap_output=True, raise_on_error=False)
    if ('schemas differ on table vt_select_test:\n'
        'test_nj-0000062344: CREATE TABLE' not in err):
      self.fail('wrong ValidateSchemaKeyspace output: ' + err)

    # validate versions
    utils.run_vtctl(['ValidateVersionShard', 'test_keyspace/-80'],
                    auto_log=True)
    utils.run_vtctl(['ValidateVersionKeyspace', 'test_keyspace'], auto_log=True)

    # show and validate permissions
    utils.run_vtctl(['GetPermissions', 'test_nj-0000062344'], auto_log=True)
    utils.run_vtctl(['ValidatePermissionsShard', 'test_keyspace/-80'],
                    auto_log=True)
    utils.run_vtctl(['ValidatePermissionsKeyspace', 'test_keyspace'],
                    auto_log=True)

    # connect to the tablets directly, make sure they know / validate
    # their own shard
    sql = 'select id, msg from vt_select_test order by id'

    qr = shard_0_master.execute(sql)
    self.assertEqual(qr['rows'], [[1, 'test 1'],])

    qr = shard_1_master.execute(sql)
    self.assertEqual(qr['rows'], [[10, 'test 10'],])

    tablet.kill_tablets([shard_0_master, shard_0_replica, shard_1_master,
                         shard_1_replica])

if __name__ == '__main__':
  utils.main()
