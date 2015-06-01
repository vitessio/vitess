#!/usr/bin/env python

import logging
import unittest
import os
import time

import environment
import utils
import tablet

shard_0_master = tablet.Tablet()
shard_0_replica1 = tablet.Tablet()
shard_0_replica2 = tablet.Tablet()
shard_0_rdonly = tablet.Tablet()
shard_0_backup = tablet.Tablet()
shard_1_master = tablet.Tablet()
shard_1_replica1 = tablet.Tablet()
shard_2_master = tablet.Tablet()
shard_2_replica1 = tablet.Tablet()
test_keyspace = 'test_keyspace'
db_name = 'vt_' + test_keyspace

def setUpModule():
  try:
    environment.topo_server().setup()

    setup_procs = [
        shard_0_master.init_mysql(),
        shard_0_replica1.init_mysql(),
        shard_0_replica2.init_mysql(),
        shard_0_rdonly.init_mysql(),
        shard_0_backup.init_mysql(),
        shard_1_master.init_mysql(),
        shard_1_replica1.init_mysql(),
        shard_2_master.init_mysql(),
        shard_2_replica1.init_mysql(),
        ]
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise

  utils.run_vtctl(['CreateKeyspace', test_keyspace])

def tearDownModule():
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_replica1.teardown_mysql(),
      shard_0_replica2.teardown_mysql(),
      shard_0_rdonly.teardown_mysql(),
      shard_0_backup.teardown_mysql(),
      shard_1_master.teardown_mysql(),
      shard_1_replica1.teardown_mysql(),
      shard_2_master.teardown_mysql(),
      shard_2_replica1.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica1.remove_tree()
  shard_0_replica2.remove_tree()
  shard_0_rdonly.remove_tree()
  shard_0_backup.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica1.remove_tree()
  shard_2_master.remove_tree()
  shard_2_replica1.remove_tree()

class TestSchema(unittest.TestCase):

  def setUp(self):
    shard_0_master.init_tablet(  'master',  test_keyspace, '0')
    shard_0_replica1.init_tablet('replica', test_keyspace, '0')
    shard_0_replica2.init_tablet('replica', test_keyspace, '0')
    shard_0_rdonly.init_tablet(  'rdonly',  test_keyspace, '0')
    shard_0_backup.init_tablet(  'backup',  test_keyspace, '0')
    shard_1_master.init_tablet(  'master',  test_keyspace, '1')
    shard_1_replica1.init_tablet('replica', test_keyspace, '1')
    shard_2_master.init_tablet(  'master',  test_keyspace, '2')
    shard_2_replica1.init_tablet('replica', test_keyspace, '2')

    utils.run_vtctl(['RebuildKeyspaceGraph', test_keyspace], auto_log=True)

    # run checks now before we start the tablets
    utils.validate_topology()

    utils.Vtctld().start()

    # create databases, start the tablets
    for t in [shard_0_master, shard_0_replica1, shard_0_replica2,
           shard_0_rdonly, shard_0_backup, shard_1_master, shard_1_replica1,
           shard_2_master, shard_2_replica1]:
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
    shard_2_master.wait_for_vttablet_state('SERVING')
    shard_2_replica1.wait_for_vttablet_state('SERVING')

    # make sure all replication is good
    for t in [shard_0_master, shard_0_replica1, shard_0_replica2,
              shard_0_rdonly, shard_0_backup, shard_1_master, shard_1_replica1, shard_2_master, shard_2_replica1]:
      t.reset_replication()

    utils.run_vtctl(['InitShardMaster', test_keyspace+'/0',
                     shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', test_keyspace+'/1',
                     shard_1_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['InitShardMaster', test_keyspace+'/2',
                     shard_2_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['ValidateKeyspace', '-ping-tablets', test_keyspace])

    # check after all tablets are here and replication is fixed
    utils.validate_topology(ping_tablets=True)

  def tearDown(self):
    tablet.kill_tablets([shard_0_master, shard_0_replica1, shard_0_replica2,
                         shard_0_rdonly, shard_0_backup, shard_1_master,
                         shard_1_replica1, shard_2_master, shard_2_replica1])

  def _check_tables(self, tablet, expectedCount):
    tables = tablet.mquery(db_name, 'show tables')
    self.assertEqual(len(tables), expectedCount,
                     'Unexpected table count on %s (not %u): %s' %
                     (tablet.tablet_alias, expectedCount, str(tables)))

  def _check_db_not_created(self, tablet):
    # Broadly catch all exceptions, since the exception being raised is internal to MySQL.
    # We're strictly checking the error message though, so should be fine.
    with self.assertRaisesRegexp(Exception, '(1049, "Unknown database \'%s\'")' % db_name):
      tables = tablet.mquery(db_name, 'show tables')

  def _apply_schema(self, keyspace, sql):
    out, err  = utils.run_vtctl(['ApplySchema',
                                 '-sql='+sql,
                                 keyspace],
                                 trap_output=True,
                                 log_level='INFO',
                                 raise_on_error=True)

    return out

  def _get_schema(self, tablet_alias, tables):
    out, err = utils.run_vtctl(['GetSchema',
                                '-tables='+tables,
                                tablet_alias],
                                trap_output=True,
                                log_level='INFO',
                                raise_on_error=True)
    return out

  def _create_test_table_sql(self, table):
    return 'CREATE TABLE %s ( \
            `id` BIGINT(20) not NULL, \
            `msg` varchar(64), \
            PRIMARY KEY (`id`) \
            ) ENGINE=InnoDB' % table

  def _alter_test_table_sql(self, table, index_column_name):
    return 'ALTER TABLE %s \
            ADD COLUMN new_id bigint(20) NOT NULL AUTO_INCREMENT FIRST, \
            DROP PRIMARY KEY, \
            ADD PRIMARY KEY (new_id), \
            ADD INDEX idx_column(%s) \
            ' % (table, index_column_name)

  def test_schema_changes(self):
    schema_changes = ';'.join([
      self._create_test_table_sql('vt_select_test01'),
      self._create_test_table_sql('vt_select_test02'),
      self._create_test_table_sql('vt_select_test03'),
      self._create_test_table_sql('vt_select_test04')])

    tables = ','.join([
      'vt_select_test01', 'vt_select_test02',
      'vt_select_test03', 'vt_select_test04'])

    # apply schema changes to the test keyspace
    self._apply_schema(test_keyspace, schema_changes)

    # check number of tables
    self._check_tables(shard_0_master, 4)
    self._check_tables(shard_1_master, 4)
    self._check_tables(shard_2_master, 4)

    # get schema for each shard
    shard_0_schema = self._get_schema(shard_0_master.tablet_alias, tables)
    shard_1_schema = self._get_schema(shard_1_master.tablet_alias, tables)
    shard_2_schema = self._get_schema(shard_2_master.tablet_alias, tables)

    # all shards should have the same schema
    self.assertEqual(shard_0_schema, shard_1_schema)
    self.assertEqual(shard_0_schema, shard_2_schema)

    self._apply_schema(test_keyspace, self._alter_test_table_sql('vt_select_test03', 'msg'))

    shard_0_schema = self._get_schema(shard_0_master.tablet_alias, tables)
    shard_1_schema = self._get_schema(shard_1_master.tablet_alias, tables)
    shard_2_schema = self._get_schema(shard_2_master.tablet_alias, tables)

    # all shards should have the same schema
    self.assertEqual(shard_0_schema, shard_1_schema)
    self.assertEqual(shard_0_schema, shard_2_schema)

    # test schema changes
    os.makedirs(os.path.join(utils.vtctld.schema_change_dir, test_keyspace))
    input_path = os.path.join(utils.vtctld.schema_change_dir, test_keyspace, "input")
    os.makedirs(input_path)
    sql_path = os.path.join(input_path, "create_test_table_x.sql")
    with open(sql_path, 'w') as handler:
      handler.write("create table test_table_x (id int)")

    timeout = 10
    # wait until this sql file being consumed by autoschema
    while os.path.isfile(sql_path):
        timeout = utils.wait_step('waiting for vtctld to pick up schema changes',
                                  timeout,
                                  sleep_time=0.2)

    # check number of tables
    self._check_tables(shard_0_master, 5)
    self._check_tables(shard_1_master, 5)
    self._check_tables(shard_2_master, 5)

if __name__ == '__main__':
  utils.main()
