#!/usr/bin/python

import json
from optparse import OptionParser
import os
import socket
from subprocess import check_call, Popen, CalledProcessError, PIPE
import tempfile

from zk import zkocc

import utils
import tablet

vttop = os.environ['VTTOP']
vtroot = os.environ['VTROOT']
hostname = socket.gethostname()

shard_0_master = tablet.Tablet()
shard_0_replica1 = tablet.Tablet()
shard_0_replica2 = tablet.Tablet()
shard_0_rdonly = tablet.Tablet()
shard_0_backup = tablet.Tablet()

def setup():
  utils.zk_setup()

  setup_procs = [
      shard_0_master.start_mysql(),
      shard_0_replica1.start_mysql(),
      shard_0_replica2.start_mysql(),
      shard_0_rdonly.start_mysql(),
      shard_0_backup.start_mysql(),
      ]
  utils.wait_procs(setup_procs)

def teardown():
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_replica1.teardown_mysql(),
      shard_0_replica2.teardown_mysql(),
      shard_0_rdonly.teardown_mysql(),
      shard_0_backup.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica1.remove_tree()
  shard_0_replica2.remove_tree()
  shard_0_rdonly.remove_tree()
  shard_0_backup.remove_tree()

# create the table
create_vt_select_test1 = '''create table vt_select_test1 (
id bigint not null,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

create_vt_select_test2 = '''create table vt_select_test2 (
id bigint not null,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

def check_tables(tablet, expectedCount):
  tables = tablet.mquery('vt_test_keyspace', 'show tables')
  if len(tables) != expectedCount:
    raise utils.TestError('Unexpected table count on %s (not %u): %s' %
                          (tablet.zk_tablet_alias, expectedCount, str(tables)))

def run_test_complex_schema():

  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  shard_0_master.init_tablet(  'master',  'test_keyspace', '0')
  shard_0_replica1.init_tablet('replica', 'test_keyspace', '0')
  shard_0_replica2.init_tablet('replica', 'test_keyspace', '0')
  shard_0_rdonly.init_tablet(  'rdonly',  'test_keyspace', '0')
  shard_0_backup.init_tablet(  'backup',  'test_keyspace', '0')

  utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/0')

  utils.run_vtctl('RebuildKeyspaceGraph /zk/global/vt/keyspaces/test_keyspace')

  # run checks now before we start the tablets
  utils.zk_check()

  # create databases
  shard_0_master.create_db('vt_test_keyspace')
  shard_0_replica1.create_db('vt_test_keyspace')
  shard_0_replica2.create_db('vt_test_keyspace')
  shard_0_rdonly.create_db('vt_test_keyspace')
  shard_0_backup.create_db('vt_test_keyspace')

  # start the tablets
  shard_0_master.start_vttablet()
  shard_0_replica1.start_vttablet()
  shard_0_replica2.start_vttablet()
  shard_0_rdonly.start_vttablet()
  shard_0_backup.start_vttablet()

  # make sure all replication is good
  utils.run_vtctl('-force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 ' + shard_0_master.zk_tablet_path)

  # apply the schema on the shard using a complex schema upgrade, no
  # reparenting yet
  utils.run_vtctl('ApplySchemaShard zk_shard_path=/zk/global/vt/keyspaces/test_keyspace/shards/0 sql="' + create_vt_select_test1.replace("\n", "") + '"', log_level='INFO')

  # check all expected hosts have the change:
  # - master won't have it as it's a complex change
  # - backup won't have it as IsReplicatingType is false
  check_tables(shard_0_master, 0)
  check_tables(shard_0_replica1, 1)
  check_tables(shard_0_replica2, 1)
  check_tables(shard_0_rdonly, 1)
  check_tables(shard_0_backup, 0)

  # apply schema change to just master directly
  # (to test its state is not changed)
  utils.run_vtctl('ApplySchema zk_tablet_path=' + shard_0_master.zk_tablet_path + ' sql="' + create_vt_select_test1.replace("\n", "") + '" allow_replication=false', log_level='INFO')
  check_tables(shard_0_master, 1)

  # apply schema change to just backup directly
  # (to test its state is not changed)
  utils.run_vtctl('ApplySchema zk_tablet_path=' + shard_0_backup.zk_tablet_path + ' sql="' + create_vt_select_test1.replace("\n", "") + '" allow_replication=false', log_level='INFO')
  check_tables(shard_0_backup, 1)

  # and apply new schema change, with reparenting
  utils.run_vtctl('ApplySchemaShard zk_shard_path=/zk/global/vt/keyspaces/test_keyspace/shards/0 sql="' + create_vt_select_test2.replace("\n", "") + '" new_parent=' + shard_0_replica1.zk_tablet_path, log_level='INFO')
  check_tables(shard_0_master, 1)
  check_tables(shard_0_replica1, 2)
  check_tables(shard_0_replica2, 2)
  check_tables(shard_0_rdonly, 2)
  check_tables(shard_0_backup, 1)

  out, err = utils.run(vtroot+'/bin/zk ls '+shard_0_replica1.zk_tablet_path+'/actionlog', trap_output=True)
  oldLines = out.splitlines()
  oldCount = len(oldLines)
  if utils.options.verbose:
    print "I have %u actionlog before" % oldCount
  if oldCount <= 5:
    raise utils.TestError('Not enough actionlog before: %u' % oldCount)

  utils.run_vtctl('PruneActionLogs '+shard_0_replica1.zk_tablet_path+'/actionlog 5', log_level='INFO')

  out, err = utils.run(vtroot+'/bin/zk ls '+shard_0_replica1.zk_tablet_path+'/actionlog', trap_output=True)
  newLines = out.splitlines()
  newCount = len(newLines)
  if utils.options.verbose:
    print "I have %u actionlog after" % newCount

  if newCount != 5:
    raise utils.TestError('Unexpected actionlog count after: %u' % newCount)
  if oldLines[-5:] != newLines:
    raise utils.TestError('Unexpected actionlog values:\n%s\n%s' %
                          (' '.join(oldLines[-5:]), ' '.join(newLines)))

  utils.pause("Look at schema now!")

  shard_0_master.kill_vttablet()
  shard_0_replica1.kill_vttablet()
  shard_0_replica2.kill_vttablet()
  shard_0_rdonly.kill_vttablet()
  shard_0_backup.kill_vttablet()

def run_all():
  run_test_complex_schema()

def main():
  parser = OptionParser()
  parser.add_option('-v', '--verbose', action='store_true')
  parser.add_option('-d', '--debug', action='store_true')
  parser.add_option('--skip-teardown', action='store_true')
  (utils.options, args) = parser.parse_args()

  if not args:
    args = ['run_all']

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
