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


import os

import logging
import unittest

import environment
import tablet
import utils

shard_0_master = tablet.Tablet()
shard_0_replica1 = tablet.Tablet()
shard_0_replica2 = tablet.Tablet()
shard_0_rdonly = tablet.Tablet()
shard_0_backup = tablet.Tablet()
shard_1_master = tablet.Tablet()
shard_1_replica1 = tablet.Tablet()
shard_2_master = tablet.Tablet()
shard_2_replica1 = tablet.Tablet()

# shard_2 tablets shouldn't exist yet when _apply_initial_schema() is called.
initial_tablets = [
    shard_0_master, shard_0_replica1, shard_0_replica2, shard_0_rdonly,
    shard_0_backup, shard_1_master, shard_1_replica1,
]
shard_2_tablets = [shard_2_master, shard_2_replica1]
all_tablets = initial_tablets + shard_2_tablets

test_keyspace = 'test_keyspace'
db_name = 'vt_' + test_keyspace


def setUpModule():
  try:
    environment.topo_server().setup()

    _init_mysql(all_tablets)

    utils.run_vtctl(['CreateKeyspace', test_keyspace])

    utils.Vtctld().start(enable_schema_change_dir=True)

  except Exception as setup_exception:  # pylint: disable=broad-except
    try:
      tearDownModule()
    except Exception as e:  # pylint: disable=broad-except
      logging.exception('Tearing down a failed setUpModule() failed: %s', e)
    raise setup_exception


def _init_mysql(tablets):
  setup_procs = []
  for t in tablets:
    setup_procs.append(t.init_mysql())
  utils.wait_procs(setup_procs)


def _setup_shard_2():
  shard_2_master.init_tablet('replica', test_keyspace, '2')
  shard_2_replica1.init_tablet('replica', test_keyspace, '2')

  # create databases, start the tablets
  for t in shard_2_tablets:
    t.create_db(db_name)
    t.start_vttablet(wait_for_state=None)

  # wait for the tablets to start
  shard_2_master.wait_for_vttablet_state('NOT_SERVING')
  shard_2_replica1.wait_for_vttablet_state('NOT_SERVING')

  utils.run_vtctl(['InitShardMaster', '-force', test_keyspace + '/2',
                   shard_2_master.tablet_alias], auto_log=True)
  utils.run_vtctl(['ValidateKeyspace', '-ping-tablets', test_keyspace])


def _teardown_shard_2():
  tablet.kill_tablets(shard_2_tablets)

  utils.run_vtctl(
      ['DeleteShard', '-recursive', '-even_if_serving', 'test_keyspace/2'],
      auto_log=True)

  for t in shard_2_tablets:
    t.reset_replication()
    t.set_semi_sync_enabled(master=False)
    t.clean_dbs()


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return

  teardown_procs = []
  for t in all_tablets:
    teardown_procs.append(t.teardown_mysql())
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for t in all_tablets:
    t.remove_tree()


class TestSchema(unittest.TestCase):

  def setUp(self):
    shard_0_master.init_tablet('replica', test_keyspace, '0')
    shard_0_replica1.init_tablet('replica', test_keyspace, '0')
    shard_0_replica2.init_tablet('replica', test_keyspace, '0')
    shard_0_rdonly.init_tablet('rdonly', test_keyspace, '0')
    shard_0_backup.init_tablet('backup', test_keyspace, '0')
    shard_1_master.init_tablet('replica', test_keyspace, '1')
    shard_1_replica1.init_tablet('replica', test_keyspace, '1')

    # create databases, start the tablets
    for t in initial_tablets:
      t.create_db(db_name)
      t.start_vttablet(wait_for_state=None)

    # wait for the tablets to start
    for t in initial_tablets:
      t.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', '-force', test_keyspace + '/0',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', '-force', test_keyspace + '/1',
                     shard_1_master.tablet_alias], auto_log=True)

  def tearDown(self):
    # kill all tablets
    tablet.kill_tablets(initial_tablets)

    for t in initial_tablets:
      t.reset_replication()
      t.set_semi_sync_enabled(master=False)
      t.clean_dbs()

    utils.run_vtctl(['DeleteShard', '-recursive', '-even_if_serving',
                     test_keyspace + '/0'], auto_log=True)
    utils.run_vtctl(['DeleteShard', '-recursive', '-even_if_serving',
                     test_keyspace + '/1'], auto_log=True)

  def _check_tables(self, tablet_obj, expected_count):
    tables = tablet_obj.mquery(db_name, 'show tables')
    self.assertEqual(
        len(tables), expected_count,
        'Unexpected table count on %s (not %d): got tables: %s' %
        (tablet_obj.tablet_alias, expected_count, str(tables)))

  def _apply_schema(self, keyspace, sql, expect_fail=False):
    return utils.run_vtctl(['ApplySchema',
                            '-sql=' + sql,
                            keyspace],
                           expect_fail=expect_fail, auto_log=True)

  def _get_schema(self, tablet_alias):
    return utils.run_vtctl_json(['GetSchema',
                                 tablet_alias])

  def _create_test_table_sql(self, table):
    return (
        'CREATE TABLE %s (\n'
        '`id` BIGINT(20) not NULL,\n'
        '`msg` varchar(64),\n'
        'PRIMARY KEY (`id`)\n'
        ') ENGINE=InnoDB') % table

  def _alter_test_table_sql(self, table, index_column_name):
    return (
        'ALTER TABLE %s\n'
        'ADD COLUMN new_id bigint(20) NOT NULL AUTO_INCREMENT FIRST,\n'
        'DROP PRIMARY KEY,\n'
        'ADD PRIMARY KEY (new_id),\n'
        'ADD INDEX idx_column(%s)\n') % (table, index_column_name)

  def _apply_initial_schema(self):
    schema_changes = ';'.join([
        self._create_test_table_sql('vt_select_test01'),
        self._create_test_table_sql('vt_select_test02'),
        self._create_test_table_sql('vt_select_test03'),
        self._create_test_table_sql('vt_select_test04')])

    # apply schema changes to the test keyspace
    self._apply_schema(test_keyspace, schema_changes)

    # check number of tables
    self._check_tables(shard_0_master, 4)
    self._check_tables(shard_1_master, 4)

    # get schema for each shard
    shard_0_schema = self._get_schema(shard_0_master.tablet_alias)
    shard_1_schema = self._get_schema(shard_1_master.tablet_alias)

    # all shards should have the same schema
    self.assertEqual(shard_0_schema, shard_1_schema)

  def test_schema_changes(self):
    self._apply_initial_schema()

    self._apply_schema(
        test_keyspace, self._alter_test_table_sql('vt_select_test03', 'msg'))

    shard_0_schema = self._get_schema(shard_0_master.tablet_alias)
    shard_1_schema = self._get_schema(shard_1_master.tablet_alias)

    # all shards should have the same schema
    self.assertEqual(shard_0_schema, shard_1_schema)

    # test schema changes
    os.makedirs(os.path.join(utils.vtctld.schema_change_dir, test_keyspace))
    input_path = os.path.join(
        utils.vtctld.schema_change_dir, test_keyspace, 'input')
    os.makedirs(input_path)
    sql_path = os.path.join(input_path, 'create_test_table_x.sql')
    with open(sql_path, 'w') as handler:
      handler.write('create table test_table_x (id int)')

    # wait until this sql file being consumed by autoschema
    timeout = 10
    while os.path.isfile(sql_path):
      timeout = utils.wait_step(
          'waiting for vtctld to pick up schema changes',
          timeout, sleep_time=0.2)

    # check number of tables
    self._check_tables(shard_0_master, 5)
    self._check_tables(shard_1_master, 5)

  def test_schema_changes_drop_and_create(self):
    """Tests that a DROP and CREATE table will pass PreflightSchema check.

    PreflightSchema checks each SQL statement separately. When doing so, it must
    consider previous statements within the same ApplySchema command. For
    example, a CREATE after DROP must not fail: When CREATE is checked, DROP
    must have been executed first.
    See: https://github.com/youtube/vitess/issues/1731#issuecomment-222914389
    """
    self._apply_initial_schema()
    self._check_tables(shard_0_master, 4)
    self._check_tables(shard_1_master, 4)

    drop_and_create = ('DROP TABLE vt_select_test01;\n' +
                       self._create_test_table_sql('vt_select_test01'))
    self._apply_schema(test_keyspace, drop_and_create)
    # check number of tables
    self._check_tables(shard_0_master, 4)
    self._check_tables(shard_1_master, 4)

  def test_schema_changes_preflight_errors_partially(self):
    """Tests that some SQL statements fail properly during PreflightSchema."""
    self._apply_initial_schema()
    self._check_tables(shard_0_master, 4)
    self._check_tables(shard_1_master, 4)

    # Second statement will fail because the table already exists.
    create_error = (self._create_test_table_sql('vt_select_test05') + ';\n' +
                    self._create_test_table_sql('vt_select_test01'))
    stdout = self._apply_schema(test_keyspace, create_error, expect_fail=True)
    self.assertIn('already exists', ''.join(stdout))
    # check number of tables
    self._check_tables(shard_0_master, 4)
    self._check_tables(shard_1_master, 4)

  def test_schema_changes_drop_nonexistent_tables(self):
    """Tests the PreflightSchema logic for dropping nonexistent tables.

    If a table does not exist, DROP TABLE should error during preflight
    because the statement does not change the schema as there is
    nothing to drop.
    In case of DROP TABLE IF EXISTS though, it should not error as this
    is the MySQL behavior the user expects.
    """
    self._apply_initial_schema()
    self._check_tables(shard_0_master, 4)
    self._check_tables(shard_1_master, 4)

    drop_table = ('DROP TABLE nonexistent_table;')
    stdout = self._apply_schema(test_keyspace, drop_table, expect_fail=True)
    self.assertIn('Unknown table', ''.join(stdout))

    # This Query may not result in schema change and should be allowed.
    drop_if_exists = ('DROP TABLE IF EXISTS nonexistent_table;')
    self._apply_schema(test_keyspace, drop_if_exists)

    self._check_tables(shard_0_master, 4)
    self._check_tables(shard_1_master, 4)

  def test_vtctl_copyschemashard_use_tablet_as_source(self):
    self._test_vtctl_copyschemashard(shard_0_master.tablet_alias)

  def test_vtctl_copyschemashard_use_shard_as_source(self):
    self._test_vtctl_copyschemashard('test_keyspace/0')

  def _test_vtctl_copyschemashard(self, source):
    # Apply initial schema to the whole keyspace before creating shard 2.
    self._apply_initial_schema()

    _setup_shard_2()

    try:
      # InitShardMaster creates the db, but there shouldn't be any tables yet.
      self._check_tables(shard_2_master, 0)
      self._check_tables(shard_2_replica1, 0)

      # Run the command twice to make sure it's idempotent.
      for _ in range(2):
        utils.run_vtctl(['CopySchemaShard',
                         source,
                         'test_keyspace/2'],
                        auto_log=True)

        # shard_2_master should look the same as the replica we copied from
        self._check_tables(shard_2_master, 4)
        utils.wait_for_replication_pos(shard_2_master, shard_2_replica1)
        self._check_tables(shard_2_replica1, 4)
        shard_0_schema = self._get_schema(shard_0_master.tablet_alias)
        shard_2_schema = self._get_schema(shard_2_master.tablet_alias)
        self.assertEqual(shard_0_schema, shard_2_schema)
    finally:
      _teardown_shard_2()

  def test_vtctl_copyschemashard_different_dbs_should_fail(self):
    # Apply initial schema to the whole keyspace before creating shard 2.
    self._apply_initial_schema()

    _setup_shard_2()

    try:
      # InitShardMaster creates the db, but there shouldn't be any tables yet.
      self._check_tables(shard_2_master, 0)
      self._check_tables(shard_2_replica1, 0)

      # Change the db charset on the destination shard from utf8 to latin1.
      # This will make CopySchemaShard fail during its final diff.
      # (The different charset won't be corrected on the destination shard
      #  because we use "CREATE DATABASE IF NOT EXISTS" and this doesn't fail if
      #  there are differences in the options e.g. the character set.)
      shard_2_schema = self._get_schema(shard_2_master.tablet_alias)
      self.assertIn('utf8', shard_2_schema['database_schema'])
      utils.run_vtctl_json(
          ['ExecuteFetchAsDba', '-json', shard_2_master.tablet_alias,
           'ALTER DATABASE vt_test_keyspace CHARACTER SET latin1'])

      _, stderr = utils.run_vtctl(['CopySchemaShard',
                                   'test_keyspace/0',
                                   'test_keyspace/2'],
                                  expect_fail=True,
                                  auto_log=True)
      self.assertIn('source and dest don\'t agree on database creation command',
                    stderr)

      # shard_2_master should have the same number of tables. Only the db
      # character set is different.
      self._check_tables(shard_2_master, 4)
    finally:
      _teardown_shard_2()

if __name__ == '__main__':
  utils.main()
