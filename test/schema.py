#!/usr/bin/env python

import logging
import unittest
import os

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
# shard_2 tablets are not used by all tests and not included by default.
tablets = [shard_0_master, shard_0_replica1, shard_0_replica2, shard_0_rdonly,
           shard_0_backup, shard_1_master, shard_1_replica1]
tablets_shard2 = [shard_2_master, shard_2_replica1]
test_keyspace = 'test_keyspace'
db_name = 'vt_' + test_keyspace


def setUpModule():
  try:
    environment.topo_server().setup()

    _init_mysql(tablets)

    utils.run_vtctl(['CreateKeyspace', test_keyspace])

    shard_0_master.init_tablet('master', test_keyspace, '0')
    shard_0_replica1.init_tablet('replica', test_keyspace, '0')
    shard_0_replica2.init_tablet('replica', test_keyspace, '0')
    shard_0_rdonly.init_tablet('rdonly', test_keyspace, '0')
    shard_0_backup.init_tablet('backup', test_keyspace, '0')
    shard_1_master.init_tablet('master', test_keyspace, '1')
    shard_1_replica1.init_tablet('replica', test_keyspace, '1')

    # run checks now before we start the tablets
    utils.validate_topology()

    utils.Vtctld().start()

    # create databases, start the tablets
    for t in tablets:
      t.create_db(db_name)
      t.start_vttablet(wait_for_state=None)

    # wait for the tablets to start
    shard_0_master.wait_for_vttablet_state('SERVING')
    shard_0_replica1.wait_for_vttablet_state('SERVING')
    shard_0_replica2.wait_for_vttablet_state('SERVING')
    shard_0_rdonly.wait_for_vttablet_state('SERVING')
    shard_0_backup.wait_for_vttablet_state('NOT_SERVING')
    shard_1_master.wait_for_vttablet_state('SERVING')
    shard_1_replica1.wait_for_vttablet_state('SERVING')

    # make sure all replication is good
    for t in tablets:
      t.reset_replication()

    utils.run_vtctl(['InitShardMaster', test_keyspace+'/0',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', test_keyspace+'/1',
                     shard_1_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['ValidateKeyspace', '-ping-tablets', test_keyspace])

    # check after all tablets are here and replication is fixed
    utils.validate_topology(ping_tablets=True)
  except Exception as setup_exception:
    try:
      tearDownModule()
    except Exception as e:
      logging.exception('Tearing down a failed setUpModule() failed: %s', e)
    raise setup_exception


def _init_mysql(tablets):
  setup_procs = []
  for t in tablets:
    setup_procs.append(t.init_mysql())
  utils.wait_procs(setup_procs)


def tearDownModule():
  if utils.options.skip_teardown:
    return

  tablet.kill_tablets(tablets)

  teardown_procs = []
  for t in tablets:
    teardown_procs.append(t.teardown_mysql())
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for t in tablets:
    t.remove_tree()


class TestSchema(unittest.TestCase):

  def setUp(self):
    for t in tablets:
      t.create_db(db_name)

  def tearDown(self):
    # This test assumes that it can reset the tablets by simply cleaning their
    # databases without restarting the tablets.
    for t in tablets:
      t.clean_dbs()
    # Tablets from shard 2 are always started during the test. Shut
    # them down now.
    if shard_2_master in tablets:
      for t in tablets_shard2:
        t.kill_vttablet()
        utils.run_vtctl(['DeleteTablet', '-allow_master', t.tablet_alias],
                        auto_log=True)
        tablets.remove(t)
      utils.run_vtctl(['DeleteShard', 'test_keyspace/2'], auto_log=True)

  def _check_tables(self, tablet, expected_count):
    tables = tablet.mquery(db_name, 'show tables')
    self.assertEqual(len(tables), expected_count,
                     'Unexpected table count on %s (not %d): got tables: %s' %
                     (tablet.tablet_alias, expected_count, str(tables)))

  def _check_db_not_created(self, tablet):
    # Broadly catch all exceptions, since the exception being raised
    # is internal to MySQL.  We're strictly checking the error message
    # though, so should be fine.
    with self.assertRaisesRegexp(
        Exception, '(1049, "Unknown database \'%s\'")' % db_name):
      tablet.mquery(db_name, 'show tables')

  def _apply_schema(self, keyspace, sql):
    utils.run_vtctl(['ApplySchema',
                     '-sql='+sql,
                     keyspace])

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

    timeout = 10
    # wait until this sql file being consumed by autoschema
    while os.path.isfile(sql_path):
      timeout = utils.wait_step(
          'waiting for vtctld to pick up schema changes',
          timeout, sleep_time=0.2)

    # check number of tables
    self._check_tables(shard_0_master, 5)
    self._check_tables(shard_1_master, 5)

  def _setup_tablets_shard_2(self):
    try:
      _init_mysql(tablets_shard2)
    finally:
      # Include shard2 tablets for tearDown.
      tablets.extend(tablets_shard2)

    shard_2_master.init_tablet('master', 'test_keyspace', '2')
    shard_2_replica1.init_tablet('replica', 'test_keyspace', '2')

    # We intentionally don't want to create a db on these tablets.
    shard_2_master.start_vttablet(wait_for_state=None)
    shard_2_replica1.start_vttablet(wait_for_state=None)

    shard_2_master.wait_for_vttablet_state('NOT_SERVING')
    shard_2_replica1.wait_for_vttablet_state('NOT_SERVING')

    for t in tablets_shard2:
      t.reset_replication()
    utils.run_vtctl(['InitShardMaster', test_keyspace+'/2',
                     shard_2_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['ValidateKeyspace', '-ping-tablets', test_keyspace])

  def test_vtctl_copyschemashard_use_tablet_as_source(self):
    self._test_vtctl_copyschemashard(shard_0_master.tablet_alias)

  def test_vtctl_copyschemashard_use_shard_as_source(self):
    self._test_vtctl_copyschemashard('test_keyspace/0')

  def _test_vtctl_copyschemashard(self, source):
    self._apply_initial_schema()

    self._setup_tablets_shard_2()

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

if __name__ == '__main__':
  utils.main()
