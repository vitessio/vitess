#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import json
from optparse import OptionParser
import os
import shlex
import shutil
import signal
import socket
from subprocess import check_call, Popen, CalledProcessError, PIPE
import sys
import time

import MySQLdb

import utils

devnull = open('/dev/null', 'w')
vttop = os.environ['VTTOP']
vtroot = os.environ['VTROOT']
hostname = socket.gethostname()

def mysql_query(uid, dbname, query):
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket='/vt/vt_%010d/mysql.sock' % uid,
                         db=dbname)
  cursor = conn.cursor()
  cursor.execute(query)
  try:
    return cursor.fetchall()
  finally:
    conn.close()

def mysql_write_query(uid, dbname, query):
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket='/vt/vt_%010d/mysql.sock' % uid,
                         db=dbname)
  cursor = conn.cursor()
  conn.begin()
  cursor.execute(query)
  conn.commit()
  try:
    return cursor.fetchall()
  finally:
    conn.close()

def vttablet_write_query(uid, dbname, query, user=None, password=None):
  if (user is None) != (password is None):
    raise TypeError("you should provide either both or none of user and password")

  server = "localhost:%u/%s" % (uid, dbname)
  if user is not None:
    server = "%s:%s@%s" % (user, password, server)

  if query.lower().startswith("insert") or query.lower().startswith("update"):
    dml = "-dml"
  else:
    dml = ""


  cmdline = [vtroot+'/bin/vtclient2', '-server', server, dml, '"%s"' % query]

  return utils.run(' '.join(cmdline), trap_output=True)

def check_db_var(uid, name, value):
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket='/vt/vt_%010d/mysql.sock' % uid)
  cursor = conn.cursor()
  cursor.execute("show variables like '%s'" % name)
  row = cursor.fetchone()
  if row != (name, value):
    raise utils.TestError('variable not set correctly', name, row)
  conn.close()


def check_db_read_only(uid):
  return check_db_var(uid, 'read_only', 'ON')


def check_db_read_write(uid):
  return check_db_var(uid, 'read_only', 'OFF')


def wait_db_read_only(uid):
  for x in xrange(3):
    try:
      check_db_read_only(uid)
      return
    except utils.TestError as e:
      print >> sys.stderr, 'WARNING: ', e
      time.sleep(1.0)
  raise e

def setup():
  utils.prog_compile(['mysqlctl',
                      'vtaction',
                      'vtclient2',
                      'vtctl',
                      'vttablet',
                      ])
  utils.zk_setup()

  # start mysql instance external to the test
  setup_procs = []
  setup_procs.append(utils.run_bg(vtroot+'/bin/mysqlctl -tablet-uid 62344 -port 6700 -mysql-port 3700 init'))
  setup_procs.append(utils.run_bg(vtroot+'/bin/mysqlctl -tablet-uid 62044 -port 6701 -mysql-port 3701 init'))
  setup_procs.append(utils.run_bg(vtroot+'/bin/mysqlctl -tablet-uid 41983 -port 6702 -mysql-port 3702 init'))
  setup_procs.append(utils.run_bg(vtroot+'/bin/mysqlctl -tablet-uid 31981 -port 6703 -mysql-port 3703 init'))
  utils.wait_procs(setup_procs)

def teardown():
  if utils.options.skip_teardown:
    return
  teardown_procs = []
  for x in (62344, 62044, 41983, 31981):
    teardown_procs.append(utils.run_bg(vtroot+'/bin/mysqlctl -tablet-uid %u -force teardown' % x))
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  for path in ('/vt/snapshot', '/vt/vt_0000062344', '/vt/vt_0000062044', '/vt/vt_0000031981', '/vt/vt_0000041983'):
    try:
      shutil.rmtree(path)
    except OSError as e:
      if utils.options.verbose:
        print >> sys.stderr, e, path

def _check_db_addr(db_addr, expected_addr):
  # Run in the background to capture output.
  proc = utils.run_bg(vtroot+'/bin/vtctl -logfile=/dev/null -log.level=WARNING -zk.local-cell=test_nj Resolve ' + db_addr, stdout=PIPE)
  stdout = proc.communicate()[0].strip()
  if stdout != expected_addr:
    raise utils.TestError('wrong zk address', db_addr, stdout, expected_addr)

def run_test_sanity():
  # Start up a master mysql and vttablet
  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master')
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  utils.run_vtctl('RebuildKeyspace /zk/global/vt/keyspaces/test_keyspace')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')


  # if these statements don't run before the tablet it will wedge waiting for the
  # db to become accessible. this is more a bug than a feature.
  mysql_query(62344, '', 'drop database if exists vt_test_keyspace')
  mysql_query(62344, '', 'create database vt_test_keyspace')
  mysql_query(62344, 'vt_test_keyspace', create_vt_select_test)
  for q in populate_vt_select_test:
    mysql_write_query(62344, 'vt_test_keyspace', q)

  agent_62344 = utils.run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  utils.run_vtctl('Ping /zk/test_nj/vt/tablets/0000062344')

  # Quickly check basic actions.
  utils.run_vtctl('SetReadOnly /zk/test_nj/vt/tablets/0000062344')
  wait_db_read_only(62344)

  utils.run_vtctl('SetReadWrite /zk/test_nj/vt/tablets/0000062344')
  check_db_read_write(62344)

  result, _ = utils.run_vtctl('Query /zk/test_nj/vt/ns/test_keyspace "select * from vt_select_test"', trap_output=True)
  rows = result.splitlines()
  if len(rows) != 5:
    raise utils.TestError("expected 5 rows in vt_select_test", rows, result)

  utils.run_vtctl('DemoteMaster /zk/test_nj/vt/tablets/0000062344')
  wait_db_read_only(62344)

  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  agent_62344.kill()

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 "" "" idle')
  utils.run_vtctl('-force ScrapTablet /zk/test_nj/vt/tablets/0000062344')




def run_test_scrap():
  # Start up a master mysql and vttablet
  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master')
  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3701 6701 test_keyspace 0 replica')
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  utils.run_vtctl('-force ScrapTablet /zk/test_nj/vt/tablets/0000062044')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')


create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

populate_vt_insert_test = [
    "insert into vt_insert_test (msg) values ('test %s')" % x
    for x in xrange(4)]

create_vt_select_test = '''create table vt_select_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

populate_vt_select_test = [
    "insert into vt_select_test (msg) values ('test %s')" % x
    for x in xrange(4)]


def run_test_mysqlctl_clone():
  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master')
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  agent_62344 = utils.run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  mysql_query(62344, '', 'drop database if exists vt_snapshot_test')
  mysql_query(62344, '', 'create database vt_snapshot_test')
  mysql_query(62344, 'vt_snapshot_test', create_vt_insert_test)
  for q in populate_vt_insert_test:
    mysql_write_query(62344, 'vt_snapshot_test', q)

  utils.run(vtroot+'/bin/mysqlctl -tablet-uid 62344 -port 6700 -mysql-port 3700 snapshot vt_snapshot_test')

  utils.pause("snapshot finished")

  utils.run(vtroot+'/bin/mysqlctl -tablet-uid 62044 -port 6701 -mysql-port 3701 restore /vt/snapshot/vt_0000062344/replica_source.json')

  result = mysql_query(62044, 'vt_snapshot_test', 'select count(*) from vt_insert_test')
  if result[0][0] != 4:
    raise utils.TestError("expected 4 rows in vt_insert_test", result)

  agent_62344.kill()

def run_test_vtctl_snapshot_restore():
  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/snapshot_test')

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 snapshot_test 0 master')
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/snapshot_test/shards/0')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  agent_62344 = utils.run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  mysql_query(62344, '', 'drop database if exists vt_snapshot_test')
  mysql_query(62344, '', 'create database vt_snapshot_test')
  mysql_query(62344, 'vt_snapshot_test', create_vt_insert_test)
  for q in populate_vt_insert_test:
    mysql_write_query(62344, 'vt_snapshot_test', q)

  # Need to force snapshot since this is a master db.
  utils.run_vtctl('-force Snapshot /zk/test_nj/vt/tablets/0000062344')
  utils.pause("snapshot finished")

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3700 6700 "" "" idle')
  agent_62044 = utils.run_bg(vtroot+'/bin/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')

  utils.run_vtctl('Restore /zk/test_nj/vt/tablets/0000062344 /zk/test_nj/vt/tablets/0000062044')
  utils.pause("restore finished")

  result = mysql_query(62044, 'vt_snapshot_test', 'select count(*) from vt_insert_test')
  if result[0][0] != 4:
    raise utils.TestError("expected 4 rows in vt_insert_test", result)

  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  agent_62344.kill()
  agent_62044.kill()


def run_test_vtctl_clone():
  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/snapshot_test')

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 snapshot_test 0 master')
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/snapshot_test/shards/0')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  vttablet_start_watcher = utils.run_bg(vtroot+'/bin/zk wait /zk/test_nj/vt/tablets/0000062044/pid', stdout=devnull)

  agent_62344 = utils.run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  mysql_query(62344, '', 'drop database if exists vt_snapshot_test')
  mysql_query(62344, '', 'create database vt_snapshot_test')
  mysql_query(62344, 'vt_snapshot_test', create_vt_insert_test)
  for q in populate_vt_insert_test:
    mysql_write_query(62344, 'vt_snapshot_test', q)

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3700 6700 "" "" idle')
  agent_62044 = utils.run_bg(vtroot+'/bin/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')

  # remove flakiness by ensuring the target tablet comes up, otherwise we get validation
  # errors a result of forcing the clone.
  vttablet_start_watcher.wait()

  utils.run_vtctl('-force Clone /zk/test_nj/vt/tablets/0000062344 /zk/test_nj/vt/tablets/0000062044')

  result = mysql_query(62044, 'vt_snapshot_test', 'select count(*) from vt_insert_test')
  if result[0][0] != 4:
    raise utils.TestError("expected 4 rows in vt_insert_test", result)

  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  agent_62344.kill()
  agent_62044.kill()

def run_test_mysqlctl_split():
  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master ""')
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  agent_62344 = utils.run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  mysql_query(62344, '', 'drop database if exists vt_test_keyspace')
  mysql_query(62344, '', 'create database vt_test_keyspace')
  mysql_query(62344, 'vt_test_keyspace', create_vt_insert_test)
  for q in populate_vt_insert_test:
    mysql_write_query(62344, 'vt_test_keyspace', q)

  utils.run(vtroot+'/bin/mysqlctl -tablet-uid 62344 -port 6700 -mysql-port 3700 partialsnapshot vt_test_keyspace id 0 3')

  utils.pause("partialsnapshot finished")

  mysql_query(62044, '', 'stop slave')
  mysql_query(62044, '', 'drop database if exists vt_test_keyspace')
  mysql_query(62044, '', 'create database vt_test_keyspace')
  utils.run(vtroot+'/bin/mysqlctl -tablet-uid 62044 -port 6701 -mysql-port 3701 partialrestore /vt/snapshot/vt_0000062344/replica_source.json')

  result = mysql_query(62044, 'vt_test_keyspace', 'select count(*) from vt_insert_test')
  if result[0][0] != 2:
    raise utils.TestError("expected 2 rows in vt_insert_test", result)

  # change/add two values on the master, one in range, one out of range, make
  # sure the right one propagate and not the other
  utils.run_vtctl('SetReadWrite /zk/test_nj/vt/tablets/0000062344')
  vttablet_write_query(6700, 'vt_test_keyspace', "insert into vt_insert_test (id, msg) values (5, 'test should not propagate')")
  vttablet_write_query(6700, 'vt_test_keyspace', "update vt_insert_test set msg='test should propagate' where id=2")

  utils.pause("look at db now!")

  # wait until value that should have been changed is here
  timeout = 10
  while timeout > 0:
    result = mysql_query(62044, 'vt_test_keyspace', 'select msg from vt_insert_test where id=2')
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
#  result = mysql_query(62044, 'vt_test_keyspace', 'select count(*) from vt_insert_test where id=5')
#  if result[0][0] != 0:
#    raise utils.TestError("expected propagation not to happen", result)

  agent_62344.kill()

def run_test_vtctl_partial_clone():
  utils.zk_wipe()

  # Start up a master mysql and vttablet
  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/snapshot_test')

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 snapshot_test 0 master ""')
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/snapshot_test/shards/0')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  agent_62344 = utils.run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  mysql_query(62344, '', 'drop database if exists vt_snapshot_test')
  mysql_query(62344, '', 'create database vt_snapshot_test')
  mysql_query(62344, 'vt_snapshot_test', create_vt_insert_test)
  for q in populate_vt_insert_test:
    mysql_write_query(62344, 'vt_snapshot_test', q)

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3700 6700 "" "" idle ""')
  agent_62044 = utils.run_bg(vtroot+'/bin/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')

  # FIXME(alainjobart): not sure where the right place for this is,
  # but it doesn't seem it should right here. It should be either in
  # InitTablet (running an action on the vttablet), or in PartialClone
  # (instead of doing a 'USE dbname' it could do a 'CREATE DATABASE
  # dbname').
  mysql_query(62044, '', 'stop slave')
  mysql_query(62044, '', 'drop database if exists vt_snapshot_test')
  mysql_query(62044, '', 'create database vt_snapshot_test')
  utils.run_vtctl('-force PartialClone /zk/test_nj/vt/tablets/0000062344 /zk/test_nj/vt/tablets/0000062044 id 0 3')

  result = mysql_query(62044, 'vt_snapshot_test', 'select count(*) from vt_insert_test')
  if result[0][0] != 2:
    raise utils.TestError("expected 2 rows in vt_insert_test", result)

  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  agent_62344.kill()
  agent_62044.kill()

def run_test_restart_during_action():
  # Start up a master mysql and vttablet
  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master')
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')
  agent_62344 = utils.run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  utils.run_vtctl('Ping /zk/test_nj/vt/tablets/0000062344')

  # schedule long action
  utils.run_vtctl('-no-wait Sleep /zk/test_nj/vt/tablets/0000062344 15s', stdout=devnull)
  # ping blocks until the sleep finishes unless we have a schedule race
  action_path, _ = utils.run_vtctl('-no-wait Ping /zk/test_nj/vt/tablets/0000062344', trap_output=True)

  # kill agent leaving vtaction running
  agent_62344.kill()

  # restart agent
  agent_62344 = utils.run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  # we expect this action with a short wait time to fail. this isn't the best
  # and has some potential for flakiness.
  utils.run_fail(vtroot+'/bin/vtctl -wait-time 2s WaitForAction ' + action_path)
  agent_62344.kill()





def run_test_reparent_down_master():
  utils.zk_wipe()

  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  # Start up a master mysql and vttablet
  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master')
  agent_62344 = utils.run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')
  # Create a few slaves for testing reparenting.
  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3701 6701 test_keyspace 0 replica')
  agent_62044 = utils.run_bg(vtroot+'/bin/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000041983 localhost 3702 6702 test_keyspace 0 replica')
  agent_41983 = utils.run_bg(vtroot+'/bin/vttablet -port 6702 -tablet-path /zk/test_nj/vt/tablets/0000041983 -logfile /vt/vt_0000041983/vttablet.log')

  utils.run_vtctl('-force InitTablet /zk/test_ny/vt/tablets/0000031981 localhost 3703 6703 test_keyspace 0 replica')
  agent_31983 = utils.run_bg(vtroot+'/bin/vttablet -port 6703 -tablet-path /zk/test_ny/vt/tablets/0000031981 -logfile /vt/vt_0000031981/vttablet.log')

  # Recompute the shard layout node - until you do that, it might not be valid.
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  utils.zk_check()

  # Force the slaves to reparent assuming that all the datasets are identical.
  utils.run_vtctl('-force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062344')
  utils.zk_check()

  # Make the master agent unavailable.
  agent_62344.kill()


  expected_addr = hostname + ':6700'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  # Perform a reparent operation - this will hang for some amount of time.
  utils.run_fail(vtroot+'/bin/vtctl -wait-time 5s ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062044')

  # Should timeout and fail
  utils.run_fail(vtroot+'/bin/vtctl -wait-time 5s ScrapTablet /zk/test_nj/vt/tablets/0000062344')

  # Force the scrap action in zk even though tablet is not accessible.
  utils.run_vtctl('-force ScrapTablet /zk/test_nj/vt/tablets/0000062344')

  utils.run_fail(vtroot+'/bin/vtctl -force ChangeType /zk/test_nj/vt/tablets/0000062344 idle')

  # Remove pending locks (make this the force option to ReparentShard?)
  utils.run_vtctl('-force PurgeActions /zk/global/vt/keyspaces/test_keyspace/shards/0/action')

  # Scrapping a tablet shouldn't take it out of the serving graph.
  expected_addr = hostname + ':6700'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  # Re-run reparent operation, this shoud now proceed unimpeded.
  utils.run_vtctl('ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062044')

  utils.zk_check()
  expected_addr = hostname + ':6701'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  utils.run_vtctl('-force ChangeType /zk/test_nj/vt/tablets/0000062344 idle')

  idle_tablets, _ = utils.run_vtctl('ListIdle /zk/test_nj/vt', trap_output=True)
  if '0000062344' not in idle_tablets:
    raise utils.TestError('idle tablet not found', idle_tablets)

  agent_62044.kill()
  agent_41983.kill()
  agent_31983.kill()


def run_test_reparent_graceful_range_based():
  shard_id = '0000000000000000-FFFFFFFFFFFFFFFF'
  _run_test_reparent_graceful(shard_id)

def run_test_reparent_graceful():
  shard_id = '0'
  _run_test_reparent_graceful(shard_id)

def _run_test_reparent_graceful(shard_id):
  utils.zk_wipe()

  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  # Start up a master mysql and vttablet
  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace %(shard_id)s master' % vars())
  agent_62344 = utils.run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')
  # Create a few slaves for testing reparenting.
  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3701 6701 test_keyspace %(shard_id)s replica' % vars())
  agent_62044 = utils.run_bg(vtroot+'/bin/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')

  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000041983 localhost 3702 6702 test_keyspace %(shard_id)s replica' % vars())
  agent_41983 = utils.run_bg(vtroot+'/bin/vttablet -port 6702 -tablet-path /zk/test_nj/vt/tablets/0000041983 -logfile /vt/vt_0000041983/vttablet.log')

  utils.run_vtctl('-force InitTablet /zk/test_ny/vt/tablets/0000031981 localhost 3703 6703 test_keyspace %(shard_id)s replica' % vars())
  agent_31983 = utils.run_bg(vtroot+'/bin/vttablet -port 6703 -tablet-path /zk/test_ny/vt/tablets/0000031981 -logfile /vt/vt_0000031981/vttablet.log')

  # Recompute the shard layout node - until you do that, it might not be valid.
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/' + shard_id)
  utils.zk_check()

  # Force the slaves to reparent assuming that all the datasets are identical.
  utils.pause("force ReparentShard?")
  utils.run_vtctl('-force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/%s /zk/test_nj/vt/tablets/0000062344' % shard_id)
  utils.zk_check(ping_tablets=True)

  expected_addr = hostname + ':6700'
  _check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

  # Convert a replica to a spare. That should leave only one node serving traffic,
  # but still needs to appear in the replication graph.
  utils.run_vtctl('ChangeType /zk/test_nj/vt/tablets/0000041983 spare')
  utils.zk_check()
  expected_addr = hostname + ':6701'
  _check_db_addr('test_keyspace.%s.replica:_vtocc' % shard_id, expected_addr)

  # Perform a graceful reparent operation.
  utils.pause("graceful ReparentShard?")
  utils.run_vtctl('ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/%s /zk/test_nj/vt/tablets/0000062044' % shard_id)
  utils.zk_check()

  expected_addr = hostname + ':6701'
  _check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

  agent_62344.kill()
  agent_62044.kill()
  agent_41983.kill()
  agent_31983.kill()

  # Test address correction.
  agent_62044 = utils.run_bg(vtroot+'/bin/vttablet -port 6773 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')
  # Wait a moment for address to reregister.
  time.sleep(1.0)

  expected_addr = hostname + ':6773'
  _check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

  agent_62044.kill()

def run_all():
  run_test_sanity()
  run_test_sanity() # run twice to check behavior with existing znode data
  run_test_scrap()
  run_test_restart_during_action()

  # Subsumed by vtctl_clone test.
  # run_test_mysqlctl_clone()
  run_test_vtctl_clone()

  # This test does not pass as it requires an experimental mysql patch.
  #run_test_vtctl_partial_clone()

  run_test_reparent_graceful()
  run_test_reparent_graceful_range_based()
  run_test_reparent_down_master()
  run_test_vttablet_authenticated()

def run_test_vttablet_authenticated():
  utils.zk_wipe()
  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')
  utils.run_vtctl('-force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master ""')
  utils.run_vtctl('RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  agent = utils.run_bg(' '.join([vtroot+'/bin/vttablet',
                  '-port 6770',
                  '-tablet-path /zk/test_nj/vt/tablets/0000062344',
                  '-logfile /vt/vt_0000062344/vttablet.log',
                  '-auth-credentials', vttop + '/py/vttest/authcredentials_test.json']))
  time.sleep(0.1)
  try:
    mysql_query(62344, '', 'create database vt_test_keyspace')
  except MySQLdb.ProgrammingError as e:
    if e.args[0] != 1007:
      raise
  try:
    mysql_query(62344, 'vt_test_keyspace', create_vt_select_test)
  except MySQLdb.OperationalError as e:
    if e.args[0] != 1050:
      raise

  for q in populate_vt_select_test:
    mysql_write_query(62344, 'vt_test_keyspace', q)

  utils.run_vtctl('SetReadWrite /zk/test_nj/vt/tablets/0000062344')
  time.sleep(0.1)
  err, out = vttablet_write_query(uid=6770, dbname='vt_test_keyspace', user="ala", password=r"ma\ kota", query="select * from vt_select_test")
  if "Row count: " not in out:
    raise utils.TestError("query didn't go through: %s, %s" % (err, out))

  agent.kill()
  # TODO(szopa): Test that non-authenticated queries do not pass
  # through (when we get to that point).



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
