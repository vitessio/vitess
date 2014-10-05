#!/usr/bin/env python

import logging
import unittest

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


def setUpModule():
  try:
    environment.topo_server_setup()

    setup_procs = [
        shard_0_master.init_mysql(),
        shard_0_replica1.init_mysql(),
        shard_0_replica2.init_mysql(),
        shard_0_rdonly.init_mysql(),
        shard_0_backup.init_mysql(),
        shard_1_master.init_mysql(),
        shard_1_replica1.init_mysql(),
        ]
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise

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
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica1.remove_tree()
  shard_0_replica2.remove_tree()
  shard_0_rdonly.remove_tree()
  shard_0_backup.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica1.remove_tree()

# statements to create the table
create_vt_select_test = [
    ('''create table vt_select_test%d (
id bigint not null,
msg varchar(64),
primary key (id)
) Engine=InnoDB''' % x).replace("\n", "")
    for x in xrange(4)]

class TestSchema(unittest.TestCase):

  def _check_tables(self, tablet, expectedCount):
    tables = tablet.mquery('vt_test_keyspace', 'show tables')
    self.assertEqual(len(tables), expectedCount,
                     'Unexpected table count on %s (not %u): %s' %
                     (tablet.tablet_alias, expectedCount, str(tables)))

  def test_complex_schema(self):

    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    shard_0_master.init_tablet(  'master',  'test_keyspace', '0')
    shard_0_replica1.init_tablet('replica', 'test_keyspace', '0')
    shard_0_replica2.init_tablet('replica', 'test_keyspace', '0')
    shard_0_rdonly.init_tablet(  'rdonly',  'test_keyspace', '0')
    shard_0_backup.init_tablet(  'backup',  'test_keyspace', '0')
    shard_1_master.init_tablet(  'master',  'test_keyspace', '1')
    shard_1_replica1.init_tablet('replica', 'test_keyspace', '1')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    # run checks now before we start the tablets
    utils.validate_topology()

    # create databases, start the tablets
    for t in [shard_0_master, shard_0_replica1, shard_0_replica2,
              shard_0_rdonly, shard_0_backup, shard_1_master, shard_1_replica1]:
      t.create_db('vt_test_keyspace')
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
    for t in [shard_0_master, shard_0_replica1, shard_0_replica2,
              shard_0_rdonly, shard_0_backup, shard_1_master, shard_1_replica1]:
      t.reset_replication()
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/0', shard_0_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['ReparentShard', '-force', 'test_keyspace/1', shard_1_master.tablet_alias], auto_log=True)
    utils.run_vtctl(['ValidateKeyspace', '-ping-tablets', 'test_keyspace'])

    # check after all tablets are here and replication is fixed
    utils.validate_topology(ping_tablets=True)

    # shard 0: apply the schema using a complex schema upgrade, no
    # reparenting yet
    utils.run_vtctl(['ApplySchemaShard',
                     '-sql='+create_vt_select_test[0],
                     'test_keyspace/0'],
                    auto_log=True)

    # check all expected hosts have the change:
    # - master won't have it as it's a complex change
    self._check_tables(shard_0_master, 0)
    self._check_tables(shard_0_replica1, 1)
    self._check_tables(shard_0_replica2, 1)
    self._check_tables(shard_0_rdonly, 1)
    self._check_tables(shard_0_backup, 1)
    self._check_tables(shard_1_master, 0)
    self._check_tables(shard_1_replica1, 0)

    # shard 0: apply schema change to just master directly
    # (to test its state is not changed)
    utils.run_vtctl(['ApplySchema',
                     '-stop-replication',
                     '-sql='+create_vt_select_test[0],
                     shard_0_master.tablet_alias],
                    auto_log=True)
    self._check_tables(shard_0_master, 1)

    # shard 0: apply new schema change, with reparenting
    utils.run_vtctl(['ApplySchemaShard',
                     '-new-parent='+shard_0_replica1.tablet_alias,
                     '-sql='+create_vt_select_test[1],
                     'test_keyspace/0'],
                    auto_log=True)
    self._check_tables(shard_0_master, 1)
    self._check_tables(shard_0_replica1, 2)
    self._check_tables(shard_0_replica2, 2)
    self._check_tables(shard_0_rdonly, 2)
    self._check_tables(shard_0_backup, 2)

    # verify GetSchema --tables works
    s = utils.run_vtctl_json(['GetSchema', '--tables=vt_select_test0',
                               shard_0_replica1.tablet_alias])
    self.assertEqual(len(s['TableDefinitions']), 1)
    self.assertEqual(s['TableDefinitions'][0]['Name'], 'vt_select_test0')

    # keyspace: try to apply a keyspace-wide schema change, should fail
    # as the preflight would be different in both shards
    out, err = utils.run_vtctl(['ApplySchemaKeyspace',
                                '-sql='+create_vt_select_test[2],
                                'test_keyspace'],
                               trap_output=True,
                               log_level='INFO',
                               raise_on_error=False)
    if err.find('ApplySchemaKeyspace Shard 1 has inconsistent schema') == -1:
      self.fail('Unexpected ApplySchemaKeyspace output: %s' % err)

    # shard 1: catch it up with simple updates
    utils.run_vtctl(['ApplySchemaShard',
                     '-simple',
                     '-sql='+create_vt_select_test[0],
                     'test_keyspace/1'],
                    auto_log=True)
    utils.run_vtctl(['ApplySchemaShard',
                     '-simple',
                     '-sql='+create_vt_select_test[1],
                     'test_keyspace/1'],
                    auto_log=True)
    self._check_tables(shard_1_master, 2)
    self._check_tables(shard_1_replica1, 2)

    # keyspace: apply a keyspace-wide simple schema change, should work now
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql='+create_vt_select_test[2],
                     'test_keyspace'],
                    auto_log=True)

    # check all expected hosts have the change
    self._check_tables(shard_0_master, 1) # was stuck a long time ago as scrap
    self._check_tables(shard_0_replica1, 3) # current master
    self._check_tables(shard_0_replica2, 3)
    self._check_tables(shard_0_rdonly, 3)
    self._check_tables(shard_0_backup, 3)
    self._check_tables(shard_1_master, 3) # current master
    self._check_tables(shard_1_replica1, 3)

    # keyspace: apply a keyspace-wide complex schema change, should work too
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-sql='+create_vt_select_test[3],
                     'test_keyspace'],
                    auto_log=True)

    # check all expected hosts have the change:
    # - master won't have it as it's a complex change
    # - backup won't have it as IsReplicatingType is false
    self._check_tables(shard_0_master, 1) # was stuck a long time ago as scrap
    self._check_tables(shard_0_replica1, 3) # current master
    self._check_tables(shard_0_replica2, 4)
    self._check_tables(shard_0_rdonly, 4)
    self._check_tables(shard_0_backup, 4)
    self._check_tables(shard_1_master, 3) # current master
    self._check_tables(shard_1_replica1, 4)

    utils.pause("Look at schema now!")

    tablet.kill_tablets([shard_0_master, shard_0_replica1, shard_0_replica2,
                         shard_0_rdonly, shard_0_backup, shard_1_master,
                         shard_1_replica1])

if __name__ == '__main__':
  utils.main()
