#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import gzip
import os
import shutil
import signal
import socket
from subprocess import PIPE, call
import sys
import time

import MySQLdb

import utils
import tablet

devnull = open('/dev/null', 'w')

tablet_62344 = tablet.Tablet(62344)
tablet_62044 = tablet.Tablet(62044)
tablet_41983 = tablet.Tablet(41983)
tablet_31981 = tablet.Tablet(31981)

def setup():
  utils.zk_setup()

  # start mysql instance external to the test
  setup_procs = [
      tablet_62344.init_mysql(),
      tablet_62044.init_mysql(),
      tablet_41983.init_mysql(),
      tablet_31981.init_mysql(),
      ]
  utils.wait_procs(setup_procs)

def teardown():
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      tablet_62344.teardown_mysql(),
      tablet_62044.teardown_mysql(),
      tablet_41983.teardown_mysql(),
      tablet_31981.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  tablet_62344.remove_tree()
  tablet_62044.remove_tree()
  tablet_41983.remove_tree()
  tablet_31981.remove_tree()

  path = os.path.join(utils.vtdataroot, 'snapshot')
  try:
    shutil.rmtree(path)
  except OSError as e:
    if utils.options.verbose:
      print >> sys.stderr, e, path

def _check_db_addr(db_addr, expected_addr):
  # Run in the background to capture output.
  proc = utils.run_bg(utils.vtroot+'/bin/vtctl -logfile=/dev/null -log.level=WARNING -zk.local-cell=test_nj Resolve ' + db_addr, stdout=PIPE)
  stdout = proc.communicate()[0].strip()
  if stdout != expected_addr:
    raise utils.TestError('wrong zk address', db_addr, stdout, expected_addr)

@utils.test_case
def run_test_sanity():
  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force test_keyspace')
  tablet_62344.init_tablet('master', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph test_keyspace/0')
  utils.run_vtctl('RebuildKeyspaceGraph test_keyspace')
  utils.validate_topology()

  # if these statements don't run before the tablet it will wedge waiting for the
  # db to become accessible. this is more a bug than a feature.
  tablet_62344.populate('vt_test_keyspace', create_vt_select_test,
                        populate_vt_select_test)

  tablet_62344.start_vttablet()

  # make sure the query service is started right away
  result, _ = utils.run_vtctl('Query test_nj test_keyspace "select * from vt_select_test"', trap_output=True)
  rows = result.splitlines()
  if len(rows) != 5:
    raise utils.TestError("expected 5 rows in vt_select_test", rows, result)

  # check Pings
  utils.run_vtctl('Ping ' + tablet_62344.tablet_alias)
  utils.run_vtctl('RpcPing ' + tablet_62344.tablet_alias)

  # Quickly check basic actions.
  utils.run_vtctl('SetReadOnly ' + tablet_62344.tablet_alias)
  utils.wait_db_read_only(62344)

  utils.run_vtctl('SetReadWrite ' + tablet_62344.tablet_alias)
  utils.check_db_read_write(62344)

  utils.run_vtctl('DemoteMaster ' + tablet_62344.tablet_alias)
  utils.wait_db_read_only(62344)

  utils.validate_topology()
  utils.run_vtctl('ValidateKeyspace test_keyspace')
  # not pinging tablets, as it enables replication checks, and they
  # break because we only have a single master, no slaves
  utils.run_vtctl('ValidateShard -ping-tablets=false test_keyspace/0')

  tablet_62344.kill_vttablet()

  tablet_62344.init_tablet('idle')
  tablet_62344.scrap(force=True)

@utils.test_case
def run_test_rebuild():
  utils.run_vtctl('CreateKeyspace -force test_keyspace')
  tablet_62344.init_tablet('master', 'test_keyspace', '0')
  tablet_62044.init_tablet('replica', 'test_keyspace', '0')
  tablet_31981.init_tablet('experimental', 'test_keyspace', '0') # in ny by default

  utils.run_vtctl('RebuildKeyspaceGraph -cells=test_nj test_keyspace',
                  auto_log=True)
  utils.run_fail(utils.vtroot+'/bin/zk cat /zk/test_ny/vt/ns/test_keyspace/0/master')

  utils.run_vtctl('RebuildKeyspaceGraph -cells=test_ny test_keyspace',
                  auto_log=True)

  real_master = utils.zk_cat('/zk/test_nj/vt/ns/test_keyspace/0/master')
  master_alias = utils.zk_cat('/zk/test_ny/vt/ns/test_keyspace/0/master')
  if real_master != master_alias:
    raise utils.TestError('master serving graph in all cells failed:\n%s!=\n%s'
                          % (real_master, master_alias))

@utils.test_case
def run_test_scrap():
  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  tablet_62344.init_tablet('master', 'test_keyspace', '0')
  tablet_62044.init_tablet('replica', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph test_keyspace/0')
  utils.validate_topology()

  tablet_62044.scrap(force=True)
  utils.validate_topology()


create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

populate_vt_insert_test = [
    "insert into vt_insert_test (msg) values ('test %s')" % x
    for x in xrange(4)]

# the varbinary test uses 10 bytes long ids, stored in a 64 bytes column
# (using 10 bytes so it's longer than a uint64 8 bytes)
create_vt_insert_test_varbinary = '''create table vt_insert_test (
id varbinary(64),
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

populate_vt_insert_test_varbinary = [
    "insert into vt_insert_test (id, msg) values (0x%02x000000000000000000, 'test %s')" % (x+1, x+1)
    for x in xrange(4)]

create_vt_select_test = '''create table vt_select_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

populate_vt_select_test = [
    "insert into vt_select_test (msg) values ('test %s')" % x
    for x in xrange(4)]


def _run_test_mysqlctl_clone(server_mode):
  if server_mode:
    snapshot_cmd = "snapshotsourcestart -concurrency=8"
    restore_flags = "-dont-wait-for-slave-start"
  else:
    snapshot_cmd = "snapshot -concurrency=5"
    restore_flags = ""

  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force snapshot_test')

  tablet_62344.init_tablet('master', 'snapshot_test', '0')
  utils.run_vtctl('RebuildShardGraph snapshot_test/0')
  utils.validate_topology()

  tablet_62344.populate('vt_snapshot_test', create_vt_insert_test,
                        populate_vt_insert_test)

  tablet_62344.start_vttablet()

  err = tablet_62344.mysqlctl('-port %u -mysql-port %u %s vt_snapshot_test' % (tablet_62344.port, tablet_62344.mysql_port, snapshot_cmd)).wait()
  if err != 0:
    raise utils.TestError('mysqlctl %s failed' % snapshot_cmd)

  utils.pause("%s finished" % snapshot_cmd)

  call(["touch", "/tmp/vtSimulateFetchFailures"])
  err = tablet_62044.mysqlctl('-port %u -mysql-port %u restore -fetch-concurrency=2 -fetch-retry-count=4 %s %s/snapshot/vt_0000062344/snapshot_manifest.json' % (tablet_62044.port, tablet_62044.mysql_port, restore_flags, utils.vtdataroot)).wait()
  if err != 0:
    raise utils.TestError('mysqlctl restore failed')

  tablet_62044.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)

  if server_mode:
    err = tablet_62344.mysqlctl('-port %u -mysql-port %u snapshotsourceend -read-write vt_snapshot_test' % (tablet_62344.port, tablet_62344.mysql_port)).wait()
    if err != 0:
      raise utils.TestError('mysqlctl snapshotsourceend failed')

    # see if server restarted properly
    tablet_62344.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)

  tablet_62344.kill_vttablet()

@utils.test_case
def run_test_mysqlctl_clone():
  _run_test_mysqlctl_clone(False)

@utils.test_case
def run_test_mysqlctl_clone_server():
  _run_test_mysqlctl_clone(True)

def _run_test_vtctl_snapshot_restore(server_mode):
  if server_mode:
    snapshot_flags = '-server-mode -concurrency=8'
    restore_flags = '-dont-wait-for-slave-start'
  else:
    snapshot_flags = '-concurrency=4'
    restore_flags = ''
  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force snapshot_test')

  tablet_62344.init_tablet('master', 'snapshot_test', '0')
  utils.run_vtctl('RebuildShardGraph snapshot_test/0')
  utils.validate_topology()

  tablet_62344.populate('vt_snapshot_test', create_vt_insert_test,
                        populate_vt_insert_test)

  tablet_62044.create_db('vt_snapshot_test')

  tablet_62344.start_vttablet()

  # Need to force snapshot since this is a master db.
  out, err = utils.run_vtctl('Snapshot -force %s %s ' % (snapshot_flags, tablet_62344.tablet_alias), log_level='INFO', trap_output=True)
  results = {}
  for name in ['Manifest', 'ParentAlias', 'SlaveStartRequired', 'ReadOnly', 'OriginalType']:
    sepPos = err.find(name + ": ")
    if sepPos != -1:
      results[name] = err[sepPos+len(name)+2:].splitlines()[0]
  if "Manifest" not in results:
    raise utils.TestError("Snapshot didn't echo Manifest file", err)
  if "ParentAlias" not in results:
    raise utils.TestError("Snapshot didn't echo ParentAlias", err)
  utils.pause("snapshot finished: " + results['Manifest'] + " " + results['ParentAlias'])
  if server_mode:
    if "SlaveStartRequired" not in results:
      raise utils.TestError("Snapshot didn't echo SlaveStartRequired", err)
    if "ReadOnly" not in results:
      raise utils.TestError("Snapshot didn't echo ReadOnly", err)
    if "OriginalType" not in results:
      raise utils.TestError("Snapshot didn't echo OriginalType", err)
    if (results['SlaveStartRequired'] != 'false' or
        results['ReadOnly'] != 'true' or
        results['OriginalType'] != 'master'):
      raise utils.TestError("Bad values returned by Snapshot", err)
  tablet_62044.init_tablet('idle', start=True)

  # do not specify a MANIFEST, see if 'default' works
  call(["touch", "/tmp/vtSimulateFetchFailures"])
  utils.run_vtctl('Restore -fetch-concurrency=2 -fetch-retry-count=4 %s %s default %s %s' %
                  (restore_flags, tablet_62344.tablet_alias,
                   tablet_62044.tablet_alias, results['ParentAlias']), auto_log=True)
  utils.pause("restore finished")

  tablet_62044.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)

  utils.validate_topology()

  # in server_mode, get the server out of it and check it
  if server_mode:
    utils.run_vtctl('SnapshotSourceEnd %s %s' % (tablet_62344.tablet_alias, results['OriginalType']), auto_log=True)
    tablet_62344.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)
    utils.validate_topology()

  tablet_62344.kill_vttablet()
  tablet_62044.kill_vttablet()

@utils.test_case
def run_test_vtctl_snapshot_restore():
  _run_test_vtctl_snapshot_restore(server_mode=False)

@utils.test_case
def run_test_vtctl_snapshot_restore_server():
  _run_test_vtctl_snapshot_restore(server_mode=True)

def _run_test_vtctl_clone(server_mode):
  if server_mode:
    clone_flags = '-server-mode'
  else:
    clone_flags = ''
  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force snapshot_test')

  tablet_62344.init_tablet('master', 'snapshot_test', '0')
  utils.run_vtctl('RebuildShardGraph snapshot_test/0')
  utils.validate_topology()

  tablet_62344.populate('vt_snapshot_test', create_vt_insert_test,
                        populate_vt_insert_test)
  tablet_62344.start_vttablet()

  tablet_62044.create_db('vt_snapshot_test')
  tablet_62044.init_tablet('idle', start=True)

  # small test to make sure the directory validation works
  snapshot_dir = os.path.join(utils.vtdataroot, 'snapshot')
  utils.run("rm -rf %s" % snapshot_dir)
  utils.run("mkdir -p %s" % snapshot_dir)
  utils.run("chmod -w %s" % snapshot_dir)
  out, err = utils.run(utils.vtroot+'/bin/vtctl -logfile=/dev/null Clone -force %s %s %s' %
                       (clone_flags, tablet_62344.tablet_alias,
                        tablet_62044.tablet_alias),
                       trap_output=True, raise_on_error=False)
  if "Cannot validate snapshot directory" not in err:
    raise utils.TestError("expected validation error", err)
  if "Un-reserved test_nj-0000062044" not in err:
    raise utils.TestError("expected Un-reserved", err)
  utils.debug("Failed Clone output: " + err)
  utils.run("chmod +w %s" % snapshot_dir)

  call(["touch", "/tmp/vtSimulateFetchFailures"])
  utils.run_vtctl('Clone -force %s %s %s' %
                  (clone_flags, tablet_62344.tablet_alias,
                   tablet_62044.tablet_alias))

  utils.pause("look at logs!")
  tablet_62044.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)
  tablet_62344.assert_table_count('vt_snapshot_test', 'vt_insert_test', 4)

  utils.validate_topology()

  tablet_62344.kill_vttablet()
  tablet_62044.kill_vttablet()

@utils.test_case
def run_test_vtctl_clone():
  _run_test_vtctl_clone(server_mode=False)

@utils.test_case
def run_test_vtctl_clone_server():
  _run_test_vtctl_clone(server_mode=True)

@utils.test_case
def test_multisnapshot_and_restore():
  tables = ['vt_insert_test', 'vt_insert_test1']
  create_template = '''create table %s (
id bigint auto_increment,
msg varchar(64),
primary key (id),
index by_msg (msg)
) Engine=InnoDB'''
  create_view = '''create view vt_insert_view(id, msg) as select id, msg from vt_insert_test'''
  insert_template = "insert into %s (id, msg) values (%s, 'test %s')"

  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  # Start three tablets for three different shards. At this point the
  # sharding schema is not really important, as long as it is
  # consistent.
  new_spec = '-0000000000000028-'
  old_tablets = [tablet_62044, tablet_41983, tablet_31981]
  for i, tablet in enumerate(old_tablets):
    tablet.init_tablet('master', 'test_keyspace', str(i))
    utils.run_vtctl('RebuildShardGraph test_keyspace/%s' % i)
  utils.validate_topology()

  for i, tablet in enumerate(old_tablets):
    tablet.populate(
      "vt_test_keyspace",
      [create_template % table for table in tables] + [create_view],
      sum([[insert_template % (table, 10*j + i, 10*j + i) for j in range(1, 8)] for table in tables], []))
    tablet.start_vttablet()
    utils.run_vtctl('MultiSnapshot --force  --spec=%s %s id' % (new_spec, tablet.tablet_alias), trap_output=True)

  utils.pause("After snapshot")

  # try to get the schema on the source, make sure the view is there
  out, err = utils.run_vtctl('GetSchema --include-views ' +
                             tablet_62044.tablet_alias,
                             log_level='INFO', trap_output=True)
  if 'vt_insert_view' not in err or 'VIEW `{{.DatabaseName}}`.`vt_insert_view` AS select' not in err:
    raise utils.TestError('Unexpected GetSchema --include-views output: %s' % err)
  out, err = utils.run_vtctl('GetSchema ' +
                             tablet_62044.tablet_alias,
                             log_level='INFO', trap_output=True)
  if 'vt_insert_view' in err:
    raise utils.TestError('Unexpected GetSchema output: %s' % err)

  utils.run_vtctl('CreateKeyspace -force test_keyspace_new')
  tablet_62344.init_tablet('master', 'test_keyspace_new', "0", dbname="not_vt_test_keyspace")
  utils.run_vtctl('RebuildShardGraph test_keyspace_new/0')
  utils.validate_topology()
  tablet_62344.mquery('', 'DROP DATABASE IF EXISTS not_vt_test_keyspace')
  tablet_62344.start_vttablet(wait_for_state='CONNECTING') # db not created

  tablet_urls = ' '.join("vttp://localhost:%s/vt_test_keyspace" % tablet.port for tablet in old_tablets)

  # 0x28 = 40
  err = tablet_62344.mysqlctl("multirestore --end=0000000000000028 -strategy=skipAutoIncrement(vt_insert_test1),delayPrimaryKey,delaySecondaryIndexes,useMyIsam,populateBlpRecovery(6514) not_vt_test_keyspace %s" % tablet_urls).wait()
  if err != 0:
    raise utils.TestError("mysqlctl failed: %u" % err)
  for table in tables:
    rows = tablet_62344.mquery('not_vt_test_keyspace', 'select id from %s' % table)
    if len(rows) == 0:
      raise utils.TestError("There are no rows in the restored database.")
    for row in rows:
      if row[0] > 32:
        raise utils.TestError("Bad row: %s" % row)
  rows = tablet_62344.mquery('_vt', 'select * from blp_checkpoint')
  if len(rows) != 3:
    raise utils.TestError("Was expecting 3 rows in blp_checkpoint but got: %s" % str(rows))

  # try to get the schema on multi-restored guy, make sure the view is there
  out, err = utils.run_vtctl('GetSchema --include-views ' +
                             tablet_62344.tablet_alias,
                             log_level='INFO', trap_output=True)
  if 'vt_insert_view' not in err or 'VIEW `{{.DatabaseName}}`.`vt_insert_view` AS select' not in err:
    raise utils.TestError('Unexpected GetSchema --include-views output after multirestore: %s' % err)

  for tablet in tablet_62044, tablet_41983, tablet_31981, tablet_62344:
    tablet.kill_vttablet()


@utils.test_case
def test_multisnapshot_and_restore_vtctl():
  tables = ['vt_insert_test', 'vt_insert_test1']
  create_template = '''create table %s (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''
  insert_template = "insert into %s (id, msg) values (%s, 'test %s')"
  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  # Start three tablets for three different shards. At this point the
  # sharding schema is not really important, as long as it is
  # consistent.
  new_spec = '-0000000000000028-'
  old_tablets = [tablet_62044, tablet_41983, tablet_31981]
  for i, tablet in enumerate(old_tablets):
    tablet.init_tablet('master', 'test_keyspace', str(i))
    utils.run_vtctl('RebuildShardGraph test_keyspace/%s' % i)
  utils.validate_topology()

  for i, tablet in enumerate(old_tablets):
    tablet.populate(
      "vt_test_keyspace",
      [create_template % table for table in tables],
      sum([[insert_template % (table, 10*j + i, 10*j + i) for j in range(1, 8)] for table in tables], []))
    tablet.start_vttablet()
    utils.run_vtctl('MultiSnapshot -force -maximum-file-size=1 -spec=%s %s id' % (new_spec, tablet.tablet_alias), trap_output=True)

  utils.run_vtctl('CreateKeyspace -force test_keyspace_new')
  tablet_62344.init_tablet('master', 'test_keyspace_new', '-0000000000000028', dbname='not_vt_test_keyspace')
  utils.run_vtctl('RebuildShardGraph test_keyspace_new/-0000000000000028')
  utils.validate_topology()
  tablet_62344.mquery('', 'DROP DATABASE IF EXISTS not_vt_test_keyspace')
  tablet_62344.start_vttablet(wait_for_state='CONNECTING') # db not created

  # 0x28 = 40
  source_aliases = ' '.join(t.tablet_alias for t in old_tablets)
  utils.run_vtctl('MultiRestore %s %s' % (tablet_62344.tablet_alias, source_aliases), auto_log=True, raise_on_error=True)
  time.sleep(1)
  for table in tables:
    rows = tablet_62344.mquery('not_vt_test_keyspace', 'select id from %s' % table)
    if len(rows) == 0:
      raise utils.TestError("There are no rows in the restored database.")
    for row in rows:
      if row[0] > 32:
        raise utils.TestError("Bad row: %s" % row)
  for tablet in tablet_62044, tablet_41983, tablet_31981, tablet_62344:
    tablet.kill_vttablet()


@utils.test_case
def test_multisnapshot_mysqlctl():
  populate = sum([[
    "insert into vt_insert_test_%s (msg) values ('test %s')" % (i, x)
    for x in xrange(4)] for i in range(6)], [])
  create = ['''create table vt_insert_test_%s (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB''' % i for i in range(6)]

  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  tablet_62344.init_tablet('master', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph test_keyspace/0')
  utils.validate_topology()

  tablet_62344.populate('vt_test_keyspace', create,
                        populate)

  tablet_62344.start_vttablet()
  err = tablet_62344.mysqlctl('-port %u -mysql-port %u multisnapshot --tables=vt_insert_test_1,vt_insert_test_2,vt_insert_test_3 --spec=-0000000000000003- vt_test_keyspace id' % (tablet_62344.port, tablet_62344.mysql_port)).wait()
  if err != 0:
    raise utils.TestError('mysqlctl multisnapshot failed')
  if os.path.exists(os.path.join(utils.vtdataroot, 'snapshot/vt_0000062344/data/vt_test_keyspace-,0000000000000003/vt_insert_test_4.csv.gz')):
    raise utils.TestError("Table vt_insert_test_4 wasn't supposed to be dumped.")
  for kr in 'vt_test_keyspace-,0000000000000003', 'vt_test_keyspace-0000000000000003,':
    path = os.path.join(utils.vtdataroot, 'snapshot/vt_0000062344/data/', kr, 'vt_insert_test_1.0.csv.gz')
    with gzip.open(path) as f:
      if len(f.readlines()) != 2:
        raise utils.TestError("Data looks wrong in %s" % path)


@utils.test_case
def test_multisnapshot_vtctl():
  populate = sum([[
    "insert into vt_insert_test_%s (msg) values ('test %s')" % (i, x)
    for x in xrange(4)] for i in range(6)], [])
  create = ['''create table vt_insert_test_%s (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB''' % i for i in range(6)]

  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  tablet_62344.init_tablet('master', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph test_keyspace/0')
  utils.validate_topology()

  tablet_62344.populate('vt_test_keyspace', create,
                        populate)

  tablet_62344.start_vttablet()

  utils.run_vtctl('MultiSnapshot --force --tables=vt_insert_test_1,vt_insert_test_2,vt_insert_test_3 --spec=-0000000000000003- %s id' % tablet_62344.tablet_alias)

  # if err != 0:
  #   raise utils.TestError('mysqlctl multisnapshot failed')
  if os.path.exists(os.path.join(utils.vtdataroot, 'snapshot/vt_0000062344/data/vt_test_keyspace-,0000000000000003/vt_insert_test_4.0.csv.gz')):
    raise utils.TestError("Table vt_insert_test_4 wasn't supposed to be dumped.")
  for kr in 'vt_test_keyspace-,0000000000000003', 'vt_test_keyspace-0000000000000003,':
    path = os.path.join(utils.vtdataroot, 'snapshot/vt_0000062344/data/', kr, 'vt_insert_test_1.0.csv.gz')
    with gzip.open(path) as f:
      if len(f.readlines()) != 2:
        raise utils.TestError("Data looks wrong in %s" % path)


@utils.test_case
def run_test_mysqlctl_split():
  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  tablet_62344.init_tablet('master', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph test_keyspace/0')
  utils.validate_topology()

  tablet_62344.populate('vt_test_keyspace', create_vt_insert_test,
                        populate_vt_insert_test)

  tablet_62344.start_vttablet()

  err = tablet_62344.mysqlctl('-port %u -mysql-port %u partialsnapshot -end=0000000000000003 vt_test_keyspace id' % (tablet_62344.port, tablet_62344.mysql_port)).wait()
  if err != 0:
    raise utils.TestError('mysqlctl partialsnapshot failed')


  utils.pause("partialsnapshot finished")

  tablet_62044.mquery('', 'stop slave')
  tablet_62044.create_db('vt_test_keyspace')
  call(["touch", "/tmp/vtSimulateFetchFailures"])
  err = tablet_62044.mysqlctl('-port %u -mysql-port %u partialrestore %s/snapshot/vt_0000062344/data/vt_test_keyspace-,0000000000000003/partial_snapshot_manifest.json' % (tablet_62044.port, tablet_62044.mysql_port, utils.vtdataroot)).wait()
  if err != 0:
    raise utils.TestError('mysqlctl partialrestore failed')

  tablet_62044.assert_table_count('vt_test_keyspace', 'vt_insert_test', 2)

  # change/add two values on the master, one in range, one out of range, make
  # sure the right one propagate and not the other
  utils.run_vtctl('SetReadWrite ' + tablet_62344.tablet_alias)
  tablet_62344.mquery('vt_test_keyspace', "insert into vt_insert_test (id, msg) values (5, 'test should not propagate')", write=True)
  tablet_62344.mquery('vt_test_keyspace', "update vt_insert_test set msg='test should propagate' where id=2", write=True)

  utils.pause("look at db now!")

  # wait until value that should have been changed is here
  timeout = 10
  while timeout > 0:
    result = tablet_62044.mquery('vt_test_keyspace', 'select msg from vt_insert_test where id=2')
    if result[0][0] == "test should propagate":
      break
    timeout -= 1
    time.sleep(1)
  if timeout == 0:
    raise utils.TestError("expected propagation to happen", result)

  # test value that should not propagate
  # this part is disabled now, as the replication pruning is only enabled
  # for row-based replication, but the mysql server is statement based.
  # will re-enable once we get statement-based pruning patch into mysql.
#  tablet_62044.assert_table_count('vt_test_keyspace', 'vt_insert_test', 0, 'where id=5')

  tablet_62344.kill_vttablet()

def _run_test_vtctl_partial_clone(create, populate,
                                  start, end):
  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force snapshot_test')

  tablet_62344.init_tablet('master', 'snapshot_test', '0')
  utils.run_vtctl('RebuildShardGraph snapshot_test/0')
  utils.validate_topology()

  tablet_62344.populate('vt_snapshot_test', create, populate)

  tablet_62344.start_vttablet()

  tablet_62044.init_tablet('idle', start=True)

  # FIXME(alainjobart): not sure where the right place for this is,
  # but it doesn't seem it should right here. It should be either in
  # InitTablet (running an action on the vttablet), or in PartialClone
  # (instead of doing a 'USE dbname' it could do a 'CREATE DATABASE
  # dbname').
  tablet_62044.mquery('', 'stop slave')
  tablet_62044.create_db('vt_snapshot_test')
  call(["touch", "/tmp/vtSimulateFetchFailures"])
  utils.run_vtctl('PartialClone -force %s %s id %s %s' %
                  (tablet_62344.tablet_alias, tablet_62044.tablet_alias,
                   start, end))

  utils.pause("after PartialClone")

  # grab the new tablet definition from zk, make sure the start and
  # end keys are set properly
  out = utils.zk_cat(tablet_62044.zk_tablet_path)
  if '"Start": "%s"' % start not in out or '"End": "%s"' % end not in out:
    print "Tablet output:"
    print "out"
    raise utils.TestError('wrong Start or End')

  tablet_62044.assert_table_count('vt_snapshot_test', 'vt_insert_test', 2)

  utils.validate_topology()

  tablet_62344.kill_vttablet()
  tablet_62044.kill_vttablet()

@utils.test_case
def run_test_vtctl_partial_clone():
  _run_test_vtctl_partial_clone(create_vt_insert_test,
                                populate_vt_insert_test,
                                '0000000000000000',
                                '0000000000000003')

@utils.test_case
def run_test_vtctl_partial_clone_varbinary():
  _run_test_vtctl_partial_clone(create_vt_insert_test_varbinary,
                                populate_vt_insert_test_varbinary,
                                '00000000000000000000',
                                '03000000000000000000')

@utils.test_case
def run_test_restart_during_action():
  # Start up a master mysql and vttablet
  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  tablet_62344.init_tablet('master', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph test_keyspace/0')
  utils.validate_topology()
  tablet_62344.create_db('vt_test_keyspace')
  tablet_62344.start_vttablet()

  utils.run_vtctl('Ping ' + tablet_62344.tablet_alias)

  # schedule long action
  utils.run_vtctl('-no-wait Sleep %s 15s' % tablet_62344.tablet_alias, stdout=devnull)
  # ping blocks until the sleep finishes unless we have a schedule race
  action_path, _ = utils.run_vtctl('-no-wait Ping ' + tablet_62344.tablet_alias, trap_output=True)

  # kill agent leaving vtaction running
  tablet_62344.kill_vttablet()

  # restart agent
  tablet_62344.start_vttablet()

  # we expect this action with a short wait time to fail. this isn't the best
  # and has some potential for flakiness.
  utils.run_fail(utils.vtroot+'/bin/vtctl -logfile=/dev/null -log.level=WARNING -wait-time 2s WaitForAction ' + action_path)

  # wait until the background sleep action is done, otherwise there will be
  # a leftover vtaction whose result may overwrite running actions
  # NOTE(alainjobart): Yes, I've seen it happen, it's a pain to debug:
  # the zombie Sleep clobbers the Clone command in the following tests
  utils.run(utils.vtroot+'/bin/vtctl -logfile=/dev/null -log.level=WARNING -wait-time 20s WaitForAction ' + action_path)

  # extra small test: we ran for a while, get the states we were in,
  # make sure they're accounted for properly
  # first the query engine States
  v = utils.get_vars(tablet_62344.port)
  if v['Voltron']['States']['DurationOPEN'] < 10e9:
    raise utils.TestError('not enough time in Open state', v['Voltron']['States']['DurationOPEN'])
  # then the Zookeeper connections
  if v['ZkMetaConn']['test_nj']['Current'] != 'Connected':
    raise utils.TestError('invalid zk test_nj state: ', v['ZkMetaConn']['test_nj']['Current'])
  if v['ZkMetaConn']['global']['Current'] != 'Connected':
    raise utils.TestError('invalid zk global state: ', v['ZkMetaConn']['global']['Current'])
  if v['ZkMetaConn']['test_nj']['DurationConnected'] < 10e9:
    raise utils.TestError('not enough time in Connected state', v['ZkMetaConn']['test_nj']['DurationConnected'])

  tablet_62344.kill_vttablet()

@utils.test_case
def run_test_reparent_down_master():
  utils.zk_wipe()

  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  # create the database so vttablets start, as they are serving
  tablet_62344.create_db('vt_test_keyspace')
  tablet_62044.create_db('vt_test_keyspace')
  tablet_41983.create_db('vt_test_keyspace')
  tablet_31981.create_db('vt_test_keyspace')

  # Start up a master mysql and vttablet
  tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

  # Create a few slaves for testing reparenting.
  tablet_62044.init_tablet('replica', 'test_keyspace', '0', start=True)
  tablet_41983.init_tablet('replica', 'test_keyspace', '0', start=True)
  tablet_31981.init_tablet('replica', 'test_keyspace', '0', start=True)

  # Recompute the shard layout node - until you do that, it might not be valid.
  utils.run_vtctl('RebuildShardGraph test_keyspace/0')
  utils.validate_topology()

  # Force the slaves to reparent assuming that all the datasets are identical.
  utils.run_vtctl('ReparentShard -force test_keyspace/0 ' + tablet_62344.tablet_alias, auto_log=True)
  utils.validate_topology()

  # Make the master agent and database unavailable.
  tablet_62344.kill_vttablet()
  tablet_62344.shutdown_mysql().wait()

  expected_addr = utils.hostname + ':' + str(tablet_62344.port)
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  # Perform a reparent operation - the Validate part will try to ping
  # the master and fail somewhat quickly
  stdout, stderr = utils.run_fail(utils.vtroot+'/bin/vtctl -logfile=/dev/null -log.level=INFO -wait-time 5s ReparentShard test_keyspace/0 ' + tablet_62044.tablet_alias)
  utils.debug("Failed ReparentShard output:\n" + stderr)
  if 'ValidateShard verification failed: timed out during validate' not in stderr:
    raise utils.TestError("didn't find the right error strings in failed ReparentShard: " + stderr)

  # Should timeout and fail
  stdout, stderr = utils.run_fail(utils.vtroot+'/bin/vtctl -logfile=/dev/null -log.level=INFO -wait-time 5s ScrapTablet ' + tablet_62344.tablet_alias)
  utils.debug("Failed ScrapTablet output:\n" + stderr)
  if 'deadline exceeded' not in stderr:
    raise utils.TestError("didn't find the right error strings in failed ScrapTablet: " + stderr)

  # Should interrupt and fail
  sp = utils.run_bg(utils.vtroot+'/bin/vtctl -log.level=INFO -wait-time 10s ScrapTablet ' + tablet_62344.tablet_alias, stdout=PIPE, stderr=PIPE)
  # Need time for the process to start before killing it.
  time.sleep(0.1)
  os.kill(sp.pid, signal.SIGINT)
  stdout, stderr = sp.communicate()

  utils.debug("Failed ScrapTablet output:\n" + stderr)
  if 'interrupted' not in stderr:
    raise utils.TestError("didn't find the right error strings in failed ScrapTablet: " + stderr)

  # Force the scrap action in zk even though tablet is not accessible.
  tablet_62344.scrap(force=True)

  utils.run_fail(utils.vtroot+'/bin/vtctl -logfile=/dev/null -log.level=WARNING ChangeSlaveType -force %s idle' %
                 tablet_62344.tablet_alias)

  # Remove pending locks (make this the force option to ReparentShard?)
  utils.run_vtctl('PurgeActions /zk/global/vt/keyspaces/test_keyspace/shards/0/action')

  # Re-run reparent operation, this shoud now proceed unimpeded.
  utils.run_vtctl('-wait-time 1m ReparentShard test_keyspace/0 ' + tablet_62044.tablet_alias, auto_log=True)

  utils.validate_topology()
  expected_addr = utils.hostname + ':' + str(tablet_62044.port)
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  utils.run_vtctl('ChangeSlaveType -force %s idle' % tablet_62344.tablet_alias)

  idle_tablets, _ = utils.run_vtctl('ListAllTablets test_nj', trap_output=True)
  if '0000062344 <null> <null> idle' not in idle_tablets:
    raise utils.TestError('idle tablet not found', idle_tablets)

  tablet_62044.kill_vttablet()
  tablet_41983.kill_vttablet()
  tablet_31981.kill_vttablet()

  # sothe other tests don't have any surprise
  tablet_62344.start_mysql().wait()


@utils.test_case
def run_test_reparent_graceful_range_based():
  shard_id = '0000000000000000-FFFFFFFFFFFFFFFF'
  _run_test_reparent_graceful(shard_id)

@utils.test_case
def run_test_reparent_graceful():
  shard_id = '0'
  _run_test_reparent_graceful(shard_id)

def _run_test_reparent_graceful(shard_id):
  utils.zk_wipe()

  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  # create the database so vttablets start, as they are serving
  tablet_62344.create_db('vt_test_keyspace')
  tablet_62044.create_db('vt_test_keyspace')
  tablet_41983.create_db('vt_test_keyspace')
  tablet_31981.create_db('vt_test_keyspace')

  # Start up a master mysql and vttablet
  tablet_62344.init_tablet('master', 'test_keyspace', shard_id, start=True)

  # Create a few slaves for testing reparenting.
  tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True)
  tablet_41983.init_tablet('replica', 'test_keyspace', shard_id, start=True)
  tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True)

  # Recompute the shard layout node - until you do that, it might not be valid.
  utils.run_vtctl('RebuildShardGraph test_keyspace/' + shard_id)
  utils.validate_topology()

  # Force the slaves to reparent assuming that all the datasets are identical.
  utils.pause("force ReparentShard?")
  utils.run_vtctl('ReparentShard -force test_keyspace/%s %s' % (shard_id, tablet_62344.tablet_alias))
  utils.validate_topology(ping_tablets=True)

  expected_addr = utils.hostname + ':' + str(tablet_62344.port)
  _check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

  # Convert two replica to spare. That should leave only one node serving traffic,
  # but still needs to appear in the replication graph.
  utils.run_vtctl('ChangeSlaveType ' + tablet_41983.tablet_alias + ' spare')
  utils.run_vtctl('ChangeSlaveType ' + tablet_31981.tablet_alias + ' spare')
  utils.validate_topology()
  expected_addr = utils.hostname + ':' + str(tablet_62044.port)
  _check_db_addr('test_keyspace.%s.replica:_vtocc' % shard_id, expected_addr)

  # Run this to make sure it succeeds.
  utils.run_vtctl('ShardReplicationPositions test_keyspace/%s' % shard_id, stdout=devnull)

  # Perform a graceful reparent operation.
  utils.pause("graceful ReparentShard?")
  utils.run_vtctl('ReparentShard test_keyspace/%s %s' % (shard_id, tablet_62044.tablet_alias), auto_log=True)
  utils.validate_topology()

  expected_addr = utils.hostname + ':' + str(tablet_62044.port)
  _check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

  tablet_62344.kill_vttablet()
  tablet_62044.kill_vttablet()
  tablet_41983.kill_vttablet()
  tablet_31981.kill_vttablet()

  # Test address correction.
  new_port = utils.reserve_ports(1)
  tablet_62044.start_vttablet(port=new_port)
  # Wait a moment for address to reregister.
  time.sleep(1.0)

  expected_addr = utils.hostname + ':' + str(new_port)
  _check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

  tablet_62044.kill_vttablet()


# This is a manual test to check error formatting.
@utils.test_case
def run_test_reparent_slave_offline(shard_id='0'):
  utils.zk_wipe()

  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  # create the database so vttablets start, as they are serving
  tablet_62344.create_db('vt_test_keyspace')
  tablet_62044.create_db('vt_test_keyspace')
  tablet_41983.create_db('vt_test_keyspace')
  tablet_31981.create_db('vt_test_keyspace')

  # Start up a master mysql and vttablet
  tablet_62344.init_tablet('master', 'test_keyspace', shard_id, start=True)

  # Create a few slaves for testing reparenting.
  tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True)
  tablet_41983.init_tablet('replica', 'test_keyspace', shard_id, start=True)
  tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True)

  # Recompute the shard layout node - until you do that, it might not be valid.
  utils.run_vtctl('RebuildShardGraph test_keyspace/' + shard_id)
  utils.validate_topology()

  # Force the slaves to reparent assuming that all the datasets are identical.
  utils.run_vtctl('ReparentShard -force test_keyspace/%s %s' % (shard_id, tablet_62344.tablet_alias))
  utils.validate_topology(ping_tablets=True)

  expected_addr = utils.hostname + ':' + str(tablet_62344.port)
  _check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

  # Kill one tablet so we seem offline
  tablet_31981.kill_vttablet()

  # Perform a graceful reparent operation.
  utils.run_vtctl('ReparentShard test_keyspace/%s %s' % (shard_id, tablet_62044.tablet_alias))

  tablet_62344.kill_vttablet()
  tablet_62044.kill_vttablet()
  tablet_41983.kill_vttablet()


# assume a different entity is doing the reparent, and telling us it was done
@utils.test_case
def run_test_reparent_from_outside():
  utils.zk_wipe()

  utils.run_vtctl('CreateKeyspace test_keyspace')

  # create the database so vttablets start, as they are serving
  tablet_62344.create_db('vt_test_keyspace')
  tablet_62044.create_db('vt_test_keyspace')
  tablet_41983.create_db('vt_test_keyspace')
  tablet_31981.create_db('vt_test_keyspace')

  # Start up a master mysql and vttablet
  tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

  # Create a few slaves for testing reparenting.
  tablet_62044.init_tablet('replica', 'test_keyspace', '0', start=True)
  tablet_41983.init_tablet('replica', 'test_keyspace', '0', start=True)
  tablet_31981.init_tablet('replica', 'test_keyspace', '0', start=True)

  # Reparent as a starting point
  utils.run_vtctl('ReparentShard -force test_keyspace/0 %s' % tablet_62344.tablet_alias)

  # now manually reparent 1 out of 2 tablets
  # 62044 will be the new master
  # 31981 won't be re-parented, so it w2ill be busted
  tablet_62044.mquery('', [
      "RESET MASTER",
      "STOP SLAVE",
      "RESET SLAVE",
      "CHANGE MASTER TO MASTER_HOST = ''",
      ])
  new_pos = tablet_62044.mquery('', 'show master status')
  utils.debug("New master position: %s" % str(new_pos))

  # 62344 will now be a slave of 62044
  tablet_62344.mquery('', [
      "RESET MASTER",
      "RESET SLAVE",
      "change master to master_host='%s', master_port=%u, master_log_file='%s', master_log_pos=%u" % (utils.hostname, tablet_62044.mysql_port, new_pos[0][0], new_pos[0][1]),
      'start slave'
      ])

  # 41983 will be a slave of 62044
  tablet_41983.mquery('', [
      'stop slave',
      "change master to master_port=%u, master_log_file='%s', master_log_pos=%u" % (tablet_62044.mysql_port, new_pos[0][0], new_pos[0][1]),
      'start slave'
      ])

  # update zk with the new graph
  utils.run_vtctl('ShardExternallyReparented -scrap-stragglers test_keyspace/0 %s' % tablet_62044.tablet_alias, auto_log=True)

  # make sure the replication graph is fine
  shard_files = utils.zk_ls('/zk/global/vt/keyspaces/test_keyspace/shards/0')
  utils.debug('shard_files: %s' % " ".join(shard_files))
  if shard_files != ['action', 'actionlog', 'test_nj-0000062044']:
    raise utils.TestError('unexpected zk content: %s' % " ".join(shard_files))

  slave_files = utils.zk_ls('/zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-0000062044')
  utils.debug('slave_files: %s' % " ".join(slave_files))
  if slave_files != ['test_nj-0000041983', 'test_nj-0000062344']:
    raise utils.TestError('unexpected zk content: %s' % " ".join(slave_files))

  tablet_31981.kill_vttablet()
  tablet_62344.kill_vttablet()
  tablet_62044.kill_vttablet()
  tablet_41983.kill_vttablet()


# See if a lag slave can be safely reparent.
@utils.test_case
def run_test_reparent_lag_slave(shard_id='0'):
  utils.zk_wipe()

  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  # create the database so vttablets start, as they are serving
  tablet_62344.create_db('vt_test_keyspace')
  tablet_62044.create_db('vt_test_keyspace')
  tablet_41983.create_db('vt_test_keyspace')
  tablet_31981.create_db('vt_test_keyspace')

  # Start up a master mysql and vttablet
  tablet_62344.init_tablet('master', 'test_keyspace', shard_id, start=True)

  # Create a few slaves for testing reparenting.
  tablet_62044.init_tablet('replica', 'test_keyspace', shard_id, start=True)
  tablet_31981.init_tablet('replica', 'test_keyspace', shard_id, start=True)
  tablet_41983.init_tablet('lag', 'test_keyspace', shard_id, start=True)

  # Recompute the shard layout node - until you do that, it might not be valid.
  utils.run_vtctl('RebuildShardGraph test_keyspace/' + shard_id)
  utils.validate_topology()

  # Force the slaves to reparent assuming that all the datasets are identical.
  utils.run_vtctl('ReparentShard -force test_keyspace/%s %s' % (shard_id, tablet_62344.tablet_alias))
  utils.validate_topology(ping_tablets=True)

  tablet_62344.create_db('vt_test_keyspace')
  tablet_62344.mquery('vt_test_keyspace', create_vt_insert_test)

  tablet_41983.mquery('', 'stop slave')
  for q in populate_vt_insert_test:
    tablet_62344.mquery('vt_test_keyspace', q, write=True)

  # Perform a graceful reparent operation.
  utils.run_vtctl('ReparentShard test_keyspace/%s %s' % (shard_id, tablet_62044.tablet_alias))

  tablet_41983.mquery('', 'start slave')
  time.sleep(1)

  utils.pause("check orphan")

  utils.run_vtctl('ReparentTablet %s' % tablet_41983.tablet_alias)

  result = tablet_41983.mquery('vt_test_keyspace', 'select msg from vt_insert_test where id=1')
  if len(result) != 1:
    raise utils.TestError('expected 1 row from vt_insert_test', result)

  utils.pause("check lag reparent")

  tablet_62344.kill_vttablet()
  tablet_62044.kill_vttablet()
  tablet_41983.kill_vttablet()
  tablet_31981.kill_vttablet()



@utils.test_case
def run_test_vttablet_authenticated():
  utils.zk_wipe()
  utils.run_vtctl('CreateKeyspace -force test_keyspace')
  tablet_62344.init_tablet('master', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph test_keyspace/0')
  utils.validate_topology()

  tablet_62344.populate('vt_test_keyspace', create_vt_select_test,
                        populate_vt_select_test)
  agent = tablet_62344.start_vttablet(auth=True)
  utils.run_vtctl('SetReadWrite ' + tablet_62344.tablet_alias)

  err, out = tablet_62344.vquery('select * from vt_select_test', path='test_keyspace/0', user='ala', password=r'ma kota')
  utils.debug("Got rows: " + out)
  if 'Row count: ' not in out:
    raise utils.TestError("query didn't go through: %s, %s" % (err, out))

  utils.kill_sub_process(agent)
  # TODO(szopa): Test that non-authenticated queries do not pass
  # through (when we get to that point).

def _check_string_in_hook_result(text, expected):
  if isinstance(expected, basestring):
    expected = [expected]
  for exp in expected:
    if exp in text:
      return
  print "ExecuteHook output:"
  print text
  raise utils.TestError("ExecuteHook returned unexpected result, no string: '" + "', '".join(expected) + "'")

def _run_hook(params, expectedStrings):
  out, err = utils.run(utils.vtroot+'/bin/vtctl -logfile=/dev/null -log.level=INFO ExecuteHook %s %s' % (tablet_62344.tablet_alias, params), trap_output=True, raise_on_error=False)
  for expected in expectedStrings:
    _check_string_in_hook_result(err, expected)

@utils.test_case
def run_test_hook():
  utils.zk_wipe()
  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  # create the database so vttablets start, as it is serving
  tablet_62344.create_db('vt_test_keyspace')

  tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

  # test a regular program works
  _run_hook("test.sh flag1 param1=hello", [
      '"ExitStatus": 0',
      ['"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --flag1\\nPARAM: --param1=hello\\n"',
       '"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --param1=hello\\nPARAM: --flag1\\n"',
       ],
      '"Stderr": ""',
      ])

  # test stderr output
  _run_hook("test.sh to-stderr", [
      '"ExitStatus": 0',
      '"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --to-stderr\\n"',
      '"Stderr": "ERR: --to-stderr\\n"',
      ])

  # test commands that fail
  _run_hook("test.sh exit-error", [
      '"ExitStatus": 1',
      '"Stdout": "TABLET_ALIAS: test_nj-0000062344\\nPARAM: --exit-error\\n"',
      '"Stderr": "ERROR: exit status 1\\n"',
      ])

  # test hook that is not present
  _run_hook("not_here.sh", [
      '"ExitStatus": -1',
      '"Stdout": "Skipping missing hook: /', # cannot go further, local path
      '"Stderr": ""',
      ])

  # test hook with invalid name
  _run_hook("/bin/ls", [
      "FATAL: action failed: ExecuteHook hook name cannot have a '/' in it",
      ])

  tablet_62344.kill_vttablet()

@utils.test_case
def run_test_sigterm():
  utils.zk_wipe()
  utils.run_vtctl('CreateKeyspace -force test_keyspace')

  # create the database so vttablets start, as it is serving
  tablet_62344.create_db('vt_test_keyspace')

  tablet_62344.init_tablet('master', 'test_keyspace', '0', start=True)

  # start a 'vtctl Sleep' command in the background
  sp = utils.run_bg(utils.vtroot+'/bin/vtctl -logfile=/dev/null Sleep %s 60s' %
                    tablet_62344.tablet_alias,
                    stdout=PIPE, stderr=PIPE)

  # wait for it to start, and let's kill it
  time.sleep(2.0)
  utils.run(['pkill', 'vtaction'])
  out, err = sp.communicate()

  # check the vtctl command got the right remote error back
  if "vtaction interrupted by signal" not in err:
    raise utils.TestError("cannot find expected output in error:", err)
  utils.debug("vtaction was interrupted correctly:\n" + err)

  tablet_62344.kill_vttablet()

def run_all():
  run_test_sanity()
  run_test_sanity() # run twice to check behavior with existing znode data
  run_test_rebuild()
  run_test_scrap()
  run_test_restart_during_action()

  # Subsumed by vtctl_clone* tests.
  # run_test_mysqlctl_clone()
  # run_test_mysqlctl_clone_server()
  # run_test_vtctl_snapshot_restore()
  # run_test_vtctl_snapshot_restore_server()
  run_test_vtctl_clone()
  run_test_vtctl_clone_server()

  # This test does not pass as it requires an experimental mysql patch.
  #run_test_vtctl_partial_clone()
  #run_test_vtctl_partial_clone_varbinary()

  test_multisnapshot_vtctl()
  test_multisnapshot_mysqlctl()
  test_multisnapshot_and_restore()
  test_multisnapshot_and_restore_vtctl()

  run_test_reparent_graceful()
  run_test_reparent_graceful_range_based()
  run_test_reparent_down_master()
  run_test_vttablet_authenticated()
  run_test_reparent_from_outside()
  run_test_reparent_lag_slave()
  run_test_hook()
  run_test_sigterm()

def main():
  args = utils.get_args()

  try:
    if args[0] != 'teardown':
      setup()
      if args[0] != 'setup':
        for arg in args:
          globals()[arg]()
          print "GREAT SUCCESS"
  except KeyboardInterrupt:
    pass
  except utils.Break:
    utils.options.skip_teardown = True
  finally:
    teardown()


if __name__ == '__main__':
  main()
