#!/usr/bin/python

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


devnull = open('/dev/null', 'w')

class TestError(Exception):
  pass

class Break(Exception):
  pass

def pause(prompt):
  if options.debug:
    raw_input(prompt)

pid_map = {}
def _add_proc(proc):
  pid_map[proc.pid] = proc
  with open('.test-pids', 'a') as f:
    print >> f, proc.pid, os.path.basename(proc.args[0])

vttop = os.environ['VTTOP']
vtroot = os.environ['VTROOT']
hostname = socket.gethostname()


def run(cmd, trap_output=False, **kargs):
  args = shlex.split(cmd)
  if trap_output:
    kargs['stdout'] = PIPE
    kargs['stderr'] = PIPE
  if options.verbose:
    print "run:", cmd, ', '.join('%s=%s' % x for x in kargs.iteritems())
  proc = Popen(args, **kargs)
  proc.args = args
  stdout, stderr = proc.communicate()
  if proc.returncode:
    raise TestError('cmd fail:', args, stdout, stderr)
  return stdout, stderr


def run_fail(cmd, **kargs):
  args = shlex.split(cmd)
  kargs['stdout'] = PIPE
  kargs['stderr'] = PIPE
  if options.verbose:
    print "run: (expect fail)", cmd, ', '.join('%s=%s' % x for x in kargs.iteritems())
  proc = Popen(args, **kargs)
  proc.args = args
  stdout, stderr = proc.communicate()
  if proc.returncode == 0:
    raise TestError('expected fail:', args, stdout, stderr)
  return stdout, stderr


# run a daemon - kill when this script exits
def run_bg(cmd, **kargs):
  if options.verbose:
    print "run:", cmd, ', '.join('%s=%s' % x for x in kargs.iteritems())
  args = shlex.split(cmd)
  proc = Popen(args=args, **kargs)
  proc.args = args
  _add_proc(proc)
  return proc


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


def check_db_var(uid, name, value):
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket='/vt/vt_%010d/mysql.sock' % uid)
  cursor = conn.cursor()
  cursor.execute("show variables like '%s'" % name)
  row = cursor.fetchone()
  if row != (name, value):
    raise TestError('variable not set correctly', name, row)
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
    except TestError as e:
      print >> sys.stderr, 'WARNING: ', e
      time.sleep(1.0)
  raise e

def wait_procs(proc_list, raise_on_error=True):
  for proc in proc_list:
    proc.wait()
  for proc in proc_list:
    if proc.returncode:
      if options.verbose and proc.returncode not in (-9,):
        sys.stderr.write("proc failed: %s %s\n" % (proc.returncode, proc.args))
      if raise_on_error:
        raise CalledProcessError(proc.returncode, proc.args)

def setup():
  setup_procs = []

  # compile all the tools
  run('go build', cwd=vttop+'/go/cmd/mysqlctl')
  run('go build', cwd=vttop+'/go/cmd/vtaction')
  run('go build', cwd=vttop+'/go/cmd/vtctl')
  run('go build', cwd=vttop+'/go/cmd/vttablet')
  run('go build', cwd=vttop+'/go/cmd/zkctl')
  run('go build', cwd=vttop+'/go/cmd/zk')

  # start mysql instance external to the test
  setup_procs.append(run_bg(vtroot+'/bin/mysqlctl -tablet-uid 62344 -port 6700 -mysql-port 3700 init'))
  setup_procs.append(run_bg(vtroot+'/bin/mysqlctl -tablet-uid 62044 -port 6701 -mysql-port 3701 init'))
  setup_procs.append(run_bg(vtroot+'/bin/mysqlctl -tablet-uid 41983 -port 6702 -mysql-port 3702 init'))
  setup_procs.append(run_bg(vtroot+'/bin/mysqlctl -tablet-uid 31981 -port 6703 -mysql-port 3703 init'))
  setup_procs.append(run_bg(vtroot+'/bin/zkctl -zk.cfg 1@'+hostname+':3801:3802:3803 init'))
  wait_procs(setup_procs)

  with open('.test-zk-client-conf.json', 'w') as f:
    zk_cell_mapping = {'test_nj': 'localhost:3803',
                       'test_ny': 'localhost:3803',
                       'test_ca': 'localhost:3803',
                       'global': 'localhost:3803',}
    json.dump(zk_cell_mapping, f)
  os.putenv('ZK_CLIENT_CONFIG', '.test-zk-client-conf.json')

  run(vtroot+'/bin/zk touch -p /zk/test_nj/vt')
  run(vtroot+'/bin/zk touch -p /zk/test_ny/vt')
  run(vtroot+'/bin/zk touch -p /zk/test_ca/vt')

def teardown():
  if options.skip_teardown:
    return
  teardown_procs = []
  for x in (62344, 62044, 41983, 31981):
    teardown_procs.append(run_bg(vtroot+'/bin/mysqlctl -tablet-uid %u -force teardown' % x))

  teardown_procs.append(run_bg(vtroot+'/bin/zkctl -zk.cfg 1@'+hostname+':3801:3802:3803 teardown'))
  wait_procs(teardown_procs, raise_on_error=False)
  for proc in pid_map.values():
    if proc.pid and proc.returncode is None:
      proc.kill()
  with open('.test-pids') as f:
    for line in f:
      try:
        parts = line.strip().split()
        pid = int(parts[0])
        proc_name = parts[1]
        proc = pid_map.get(pid)
        if not proc or (proc and proc.pid and proc.returncode is None):
          os.kill(pid, signal.SIGTERM)
      except OSError as e:
        if options.verbose:
          print >> sys.stderr, e
  for path in ('.test-pids', '.test-zk-client-conf.json'):
    try:
      os.remove(path)
    except OSError as e:
      if options.verbose:
        print >> sys.stderr, e, path

  for path in ('/vt/snapshot', '/vt/vt_0000062344', '/vt/vt_0000062044', '/vt/vt_0000031981', '/vt/vt_0000041983'):
    try:
      shutil.rmtree(path)
    except OSError as e:
      if options.verbose:
        print >> sys.stderr, e, path

def _wipe_zk():
  run(vtroot+'/bin/zk rm -rf /zk/test_nj/vt')
  run(vtroot+'/bin/zk rm -rf /zk/test_ny/vt')
  #run(vtroot+'/bin/zk rm -rf /zk/test_ca/vt')
  run(vtroot+'/bin/zk rm -rf /zk/global/vt')

def _check_zk(ping_tablets=False):
  if ping_tablets:
    run(vtroot+'/bin/vtctl -ping-tablets Validate /zk/global/vt/keyspaces')
  else:
    run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')

def _check_db_addr(db_addr, expected_addr):
  # Run in the background to capture output.
  proc = run_bg(vtroot+'/bin/vtctl -zk.local-cell=test_nj Resolve ' + db_addr, stdout=PIPE)
  stdout = proc.communicate()[0].strip()
  if stdout != expected_addr:
    raise TestError('wrong zk address', db_addr, stdout, expected_addr)


def run_test_sanity():
  # Start up a master mysql and vttablet
  run(vtroot+'/bin/vtctl -force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master ""')
  run(vtroot+'/bin/vtctl RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')
  agent_62344 = run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  run(vtroot+'/bin/vtctl Ping /zk/test_nj/vt/tablets/0000062344')

  # Quickly check basic actions.
  run(vtroot+'/bin/vtctl SetReadOnly /zk/test_nj/vt/tablets/0000062344')
  wait_db_read_only(62344)

  run(vtroot+'/bin/vtctl SetReadWrite /zk/test_nj/vt/tablets/0000062344')
  check_db_read_write(62344)

  run(vtroot+'/bin/vtctl DemoteMaster /zk/test_nj/vt/tablets/0000062344')
  wait_db_read_only(62344)

  run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')

  agent_62344.kill()


def run_test_scrap():
  # Start up a master mysql and vttablet
  run(vtroot+'/bin/vtctl -force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master ""')
  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3701 6701 test_keyspace 0 replica /zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-0000062344')
  run(vtroot+'/bin/vtctl RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')

  run(vtroot+'/bin/vtctl -force ScrapTablet /zk/test_nj/vt/tablets/0000062044')
  run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')


create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

populate_vt_insert_test = [
    "insert into vt_insert_test (msg) values ('test %s')"
    for x in xrange(4)]

def run_test_mysqlctl_clone():
  _wipe_zk()

  # Start up a master mysql and vttablet
  run(vtroot+'/bin/vtctl -force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master ""')
  run(vtroot+'/bin/vtctl RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')

  agent_62344 = run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  mysql_query(62344, '', 'create database vt_snapshot_test')
  mysql_query(62344, 'vt_snapshot_test', create_vt_insert_test)
  for q in populate_vt_insert_test:
    mysql_write_query(62344, 'vt_snapshot_test', q)

  run(vtroot+'/bin/mysqlctl -tablet-uid 62344 -port 6700 -mysql-port 3700 snapshot vt_snapshot_test')

  pause("snapshot finished")

  run(vtroot+'/bin/mysqlctl -tablet-uid 62044 -port 6701 -mysql-port 3701 restore /vt/snapshot/vt_0000062344/replica_source.json')

  result = mysql_query(62044, 'vt_snapshot_test', 'select count(*) from vt_insert_test')
  if result[0][0] != 4:
    raise TestError("expected 4 rowsin vt_insert_test", result)

  agent_62344.kill()


def run_test_vtctl_snapshot_restore():
  _wipe_zk()

  # Start up a master mysql and vttablet
  run(vtroot+'/bin/vtctl -force CreateKeyspace /zk/global/vt/keyspaces/snapshot_test')

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 snapshot_test 0 master ""')
  run(vtroot+'/bin/vtctl RebuildShard /zk/global/vt/keyspaces/snapshot_test/shards/0')
  run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')

  agent_62344 = run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  mysql_query(62344, '', 'create database vt_snapshot_test')
  mysql_query(62344, 'vt_snapshot_test', create_vt_insert_test)
  for q in populate_vt_insert_test:
    mysql_write_query(62344, 'vt_snapshot_test', q)

  # Need to force snapshot since this is a master db.
  run(vtroot+'/bin/vtctl -force Snapshot /zk/test_nj/vt/tablets/0000062344')
  pause("snapshot finished")

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3700 6700 "" "" idle ""')
  agent_62044 = run_bg(vtroot+'/bin/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')

  run(vtroot+'/bin/vtctl Restore /zk/test_nj/vt/tablets/0000062344 /zk/test_nj/vt/tablets/0000062044')
  pause("restore finished")

  result = mysql_query(62044, 'vt_snapshot_test', 'select count(*) from vt_insert_test')
  if result[0][0] != 4:
    raise TestError("expected 4 rowsin vt_insert_test", result)

  run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')

  agent_62344.kill()
  agent_62044.kill()


def run_test_vtctl_clone():
  _wipe_zk()

  # Start up a master mysql and vttablet
  run(vtroot+'/bin/vtctl -force CreateKeyspace /zk/global/vt/keyspaces/snapshot_test')

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 snapshot_test 0 master ""')
  run(vtroot+'/bin/vtctl RebuildShard /zk/global/vt/keyspaces/snapshot_test/shards/0')
  run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')

  agent_62344 = run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  mysql_query(62344, '', 'create database vt_snapshot_test')
  mysql_query(62344, 'vt_snapshot_test', create_vt_insert_test)
  for q in populate_vt_insert_test:
    mysql_write_query(62344, 'vt_snapshot_test', q)

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3700 6700 "" "" idle ""')
  agent_62044 = run_bg(vtroot+'/bin/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')

  run(vtroot+'/bin/vtctl -force Clone /zk/test_nj/vt/tablets/0000062344 /zk/test_nj/vt/tablets/0000062044')

  result = mysql_query(62044, 'vt_snapshot_test', 'select count(*) from vt_insert_test')
  if result[0][0] != 4:
    raise TestError("expected 4 rowsin vt_insert_test", result)

  run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')

  agent_62344.kill()
  agent_62044.kill()


def run_test_restart_during_action():
  # Start up a master mysql and vttablet
  run(vtroot+'/bin/vtctl -force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master ""')
  run(vtroot+'/bin/vtctl RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')
  agent_62344 = run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  run(vtroot+'/bin/vtctl Ping /zk/test_nj/vt/tablets/0000062344')

  # schedule long action
  run(vtroot+'/bin/vtctl -no-wait Sleep /zk/test_nj/vt/tablets/0000062344 15s', stdout=devnull)
  # ping blocks until the sleep finishes unless we have a schedule race
  action_path, _ = run(vtroot+'/bin/vtctl -no-wait Ping /zk/test_nj/vt/tablets/0000062344', trap_output=True)

  # kill agent leaving vtaction running
  agent_62344.kill()

  # restart agent
  agent_62344 = run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  # we expect this action with a short wait time to fail. this isn't the best
  # and has some potential for flakiness.
  run_fail(vtroot+'/bin/vtctl -wait-time 2s WaitForAction ' + action_path)
  agent_62344.kill()





def run_test_reparent_down_master():
  _wipe_zk()

  run(vtroot+'/bin/vtctl -force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  # Start up a master mysql and vttablet
  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master ""')
  agent_62344 = run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')
  # Create a few slaves for testing reparenting.
  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3701 6701 test_keyspace 0 replica /zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-62344')
  agent_62044 = run_bg(vtroot+'/bin/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000041983 localhost 3702 6702 test_keyspace 0 replica /zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-62344')
  agent_41983 = run_bg(vtroot+'/bin/vttablet -port 6702 -tablet-path /zk/test_nj/vt/tablets/0000041983 -logfile /vt/vt_0000041983/vttablet.log')

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_ny/vt/tablets/0000031981 localhost 3703 6703 test_keyspace 0 replica /zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-62344')
  agent_31983 = run_bg(vtroot+'/bin/vttablet -port 6703 -tablet-path /zk/test_ny/vt/tablets/0000031981 -logfile /vt/vt_0000031981/vttablet.log')

  # Recompute the shard layout node - until you do that, it might not be valid.
  run(vtroot+'/bin/vtctl RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  _check_zk()

  # Force the slaves to reparent assuming that all the datasets are identical.
  run(vtroot+'/bin/vtctl -force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062344')
  _check_zk()

  # Make the master agent unavailable.
  agent_62344.kill()


  expected_addr = hostname + ':6700'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  # Perform a reparent operation - this will hang for some amount of time.
  run_fail(vtroot+'/bin/vtctl -wait-time 5s ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062044')

  # Should timeout and fail
  run_fail(vtroot+'/bin/vtctl -wait-time 5s ScrapTablet /zk/test_nj/vt/tablets/0000062344')

  # Force the scrap action in zk even though tablet is not accessible.
  run(vtroot+'/bin/vtctl -force ScrapTablet /zk/test_nj/vt/tablets/0000062344')

  run_fail(vtroot+'/bin/vtctl -force ChangeType /zk/test_nj/vt/tablets/0000062344 idle')

  # Remove pending locks (make this the force option to ReparentShard?)
  run(vtroot+'/bin/vtctl -force PurgeActions /zk/global/vt/keyspaces/test_keyspace/shards/0/action')

  # Scrapping a tablet shouldn't take it out of the serving graph.
  expected_addr = hostname + ':6700'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  # Re-run reparent operation, this shoud now proceed unimpeded.
  run(vtroot+'/bin/vtctl ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062044')

  _check_zk()
  expected_addr = hostname + ':6701'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  run(vtroot+'/bin/vtctl -force ChangeType /zk/test_nj/vt/tablets/0000062344 idle')

  idle_tablets, _ = run(vtroot+'/bin/vtctl ListIdle /zk/test_nj/vt', trap_output=True)
  if '0000062344' not in idle_tablets:
    raise TestError('idle tablet not found', idle_tablets)

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
  _wipe_zk()

  run(vtroot+'/bin/vtctl -force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  # Start up a master mysql and vttablet
  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace %(shard_id)s master ""' % vars())
  agent_62344 = run_bg(vtroot+'/bin/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')
  # Create a few slaves for testing reparenting.
  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3701 6701 test_keyspace %(shard_id)s replica /zk/global/vt/keyspaces/test_keyspace/shards/%(shard_id)s/test_nj-62344' % vars())
  agent_62044 = run_bg(vtroot+'/bin/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000041983 localhost 3702 6702 test_keyspace %(shard_id)s replica /zk/global/vt/keyspaces/test_keyspace/shards/%(shard_id)s/test_nj-62344' % vars())
  agent_41983 = run_bg(vtroot+'/bin/vttablet -port 6702 -tablet-path /zk/test_nj/vt/tablets/0000041983 -logfile /vt/vt_0000041983/vttablet.log')

  run(vtroot+'/bin/vtctl -force InitTablet /zk/test_ny/vt/tablets/0000031981 localhost 3703 6703 test_keyspace %(shard_id)s replica /zk/global/vt/keyspaces/test_keyspace/shards/%(shard_id)s/test_nj-62344' % vars())
  agent_31983 = run_bg(vtroot+'/bin/vttablet -port 6703 -tablet-path /zk/test_ny/vt/tablets/0000031981 -logfile /vt/vt_0000031981/vttablet.log')

  # Recompute the shard layout node - until you do that, it might not be valid.
  run(vtroot+'/bin/vtctl RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/' + shard_id)
  _check_zk()

  # Force the slaves to reparent assuming that all the datasets are identical.
  pause("force ReparentShard?")
  run(vtroot+'/bin/vtctl -force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/%s /zk/test_nj/vt/tablets/0000062344' % shard_id)
  _check_zk(ping_tablets=True)

  expected_addr = hostname + ':6700'
  _check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

  # Convert a replica to a spare. That should leave only one node serving traffic,
  # but still needs to appear in the replication graph.
  run(vtroot+'/bin/vtctl ChangeType /zk/test_nj/vt/tablets/0000041983 spare')
  _check_zk()
  expected_addr = hostname + ':6701'
  _check_db_addr('test_keyspace.%s.replica:_vtocc' % shard_id, expected_addr)

  # Perform a graceful reparent operation.
  pause("graceful ReparentShard?")
  run(vtroot+'/bin/vtctl ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/%s /zk/test_nj/vt/tablets/0000062044' % shard_id)
  _check_zk()

  expected_addr = hostname + ':6701'
  _check_db_addr('test_keyspace.%s.master:_vtocc' % shard_id, expected_addr)

  agent_62344.kill()
  agent_62044.kill()
  agent_41983.kill()
  agent_31983.kill()

  # Test address correction.
  agent_62044 = run_bg(vtroot+'/bin/vttablet -port 6773 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')
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

  run_test_reparent_graceful()
  run_test_reparent_graceful_range_based()
  run_test_reparent_down_master()


options = None
def main():
  global options
  parser = OptionParser()
  parser.add_option('-v', '--verbose', action='store_true')
  parser.add_option('--debug', action='store_true')
  parser.add_option('--skip-teardown', action='store_true')
  (options, args) = parser.parse_args()

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
  except Break:
    options.skip_teardown = True
  finally:
    teardown()


if __name__ == '__main__':
  main()
