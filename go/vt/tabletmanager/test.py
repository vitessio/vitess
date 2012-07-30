#!/usr/bin/python

import json
from optparse import OptionParser
import os
import shlex
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
    except TestError, e:
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
  run('go build', cwd=vttop+'/go/cmd/vttablet')
  run('go build', cwd=vttop+'/go/cmd/vtctl')
  run('go build', cwd=vttop+'/go/cmd/zkctl')
  run('go build', cwd=vttop+'/go/cmd/zk')

  # start mysql instance external to the test
  setup_procs.append(run_bg(vttop+'/go/cmd/mysqlctl/mysqlctl -tablet-uid 62344 -port 6700 -mysql-port 3700 init'))
  setup_procs.append(run_bg(vttop+'/go/cmd/mysqlctl/mysqlctl -tablet-uid 62044 -port 6701 -mysql-port 3701 init'))
  setup_procs.append(run_bg(vttop+'/go/cmd/mysqlctl/mysqlctl -tablet-uid 41983 -port 6702 -mysql-port 3702 init'))
  setup_procs.append(run_bg(vttop+'/go/cmd/mysqlctl/mysqlctl -tablet-uid 31981 -port 6703 -mysql-port 3703 init'))
  setup_procs.append(run_bg(vttop+'/go/cmd/zkctl/zkctl -zk.cfg 1@'+hostname+':3801:3802:3803 init'))
  wait_procs(setup_procs)

  with open('.test-zk-client-conf.json', 'w') as f:
    zk_cell_mapping = {'test_nj': 'localhost:3803',
                       'test_ny': 'localhost:3803',
                       'test_ca': 'localhost:3803',
                       'global': 'localhost:3803',}
    json.dump(zk_cell_mapping, f)
  os.putenv('ZK_CLIENT_CONFIG', '.test-zk-client-conf.json')

  run(vttop+'/go/cmd/zk/zk touch -p /zk/test_nj/vt')
  run(vttop+'/go/cmd/zk/zk touch -p /zk/test_ny/vt')
  run(vttop+'/go/cmd/zk/zk touch -p /zk/test_ca/vt')

def teardown():
  if options.skip_teardown:
    return
  teardown_procs = []
  for x in (62344, 62044, 41983, 31981):
    teardown_procs.append(run_bg(vttop+'/go/cmd/mysqlctl/mysqlctl -tablet-uid %u -force teardown' % x))

  teardown_procs.append(run_bg(vttop+'/go/cmd/zkctl/zkctl -zk.cfg 1@'+hostname+':3801:3802:3803 teardown'))
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
      except OSError, e:
        if options.verbose:
          print >> sys.stderr, e
  for path in ('.test-pids', '.test-zk-client-conf.json'):
    try:
      os.remove(path)
    except OSError, e:
      if options.verbose:
        print >> sys.stderr, e, path

def run_test_sanity():
  # Start up a master mysql and vttablet
  run(vttop+'/go/cmd/vtctl/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master ""')
  run(vttop+'/go/cmd/vtctl/vtctl RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  run(vttop+'/go/cmd/vtctl/vtctl Validate /zk/test_nj/vt')
  agent_62344 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  run(vttop+'/go/cmd/vtctl/vtctl Ping /zk/test_nj/vt/tablets/0000062344')

  # Quickly check basic actions.
  run(vttop+'/go/cmd/vtctl/vtctl SetReadOnly /zk/test_nj/vt/tablets/0000062344')
  wait_db_read_only(62344)

  run(vttop+'/go/cmd/vtctl/vtctl SetReadWrite /zk/test_nj/vt/tablets/0000062344')
  check_db_read_write(62344)

  run(vttop+'/go/cmd/vtctl/vtctl DemoteMaster /zk/test_nj/vt/tablets/0000062344')
  wait_db_read_only(62344)

  run(vttop+'/go/cmd/vtctl/vtctl Validate /zk/test_nj/vt')

  agent_62344.kill()


def run_test_restart_during_action():
  # Start up a master mysql and vttablet
  run(vttop+'/go/cmd/vtctl/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master ""')
  run(vttop+'/go/cmd/vtctl/vtctl RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  run(vttop+'/go/cmd/vtctl/vtctl Validate /zk/test_nj/vt')
  agent_62344 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  run(vttop+'/go/cmd/vtctl/vtctl Ping /zk/test_nj/vt/tablets/0000062344')

  # schedule long action
  run(vttop+'/go/cmd/vtctl/vtctl -no-wait Sleep /zk/test_nj/vt/tablets/0000062344 15s', stdout=devnull)
  # ping blocks until the sleep finishes unless we have a schedule race
  action_path, _ = run(vttop+'/go/cmd/vtctl/vtctl -no-wait Ping /zk/test_nj/vt/tablets/0000062344', trap_output=True)

  # kill agent leaving vtaction running
  agent_62344.kill()

  # restart agent
  agent_62344 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')

  # we expect this action with a short wait time to fail. this isn't the best
  # and has some potential for flakiness.
  run_fail(vttop+'/go/cmd/vtctl/vtctl -wait-time 2s WaitForAction ' + action_path)
  agent_62344.kill()




def _wipe_zk():
  run(vttop+'/go/cmd/zk/zk rm -rf /zk/test_nj/vt')
  run(vttop+'/go/cmd/zk/zk rm -rf /zk/test_ny/vt')
  #run(vttop+'/go/cmd/zk/zk rm -rf /zk/test_ca/vt')
  run(vttop+'/go/cmd/zk/zk rm -rf /zk/global/vt')

def _check_zk(ping_tablets=False):
  if ping_tablets:
    run(vttop+'/go/cmd/vtctl/vtctl -ping-tablets Validate /zk/test_nj/vt')
    run(vttop+'/go/cmd/vtctl/vtctl -ping-tablets Validate /zk/test_ny/vt')
  else:
    run(vttop+'/go/cmd/vtctl/vtctl Validate /zk/test_nj/vt')
    run(vttop+'/go/cmd/vtctl/vtctl Validate /zk/test_ny/vt')
  #run(vttop+'/go/cmd/vtctl/vtctl Validate /zk/test_ca/vt')

def _check_db_addr(db_addr, expected_addr):
  # Run in the background to capture output.
  proc = run_bg(vttop+'/go/cmd/vtctl/vtctl -zk.local-cell=test_nj Resolve ' + db_addr, stdout=PIPE)
  stdout = proc.communicate()[0].strip()
  if stdout != expected_addr:
    raise TestError('wrong zk address', db_addr, stdout, expected_addr)

def run_test_reparent_graceful():
  _wipe_zk()

  # Start up a master mysql and vttablet
  run(vttop+'/go/cmd/vtctl/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master ""')
  agent_62344 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')
  # Create a few slaves for testing reparenting.
  run(vttop+'/go/cmd/vtctl/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3701 6701 test_keyspace 0 replica /zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-62344')
  agent_62044 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')

  run(vttop+'/go/cmd/vtctl/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000041983 localhost 3702 6702 test_keyspace 0 replica /zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-62344')
  agent_41983 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6702 -tablet-path /zk/test_nj/vt/tablets/0000041983 -logfile /vt/vt_0000041983/vttablet.log')

  run(vttop+'/go/cmd/vtctl/vtctl -force InitTablet /zk/test_ny/vt/tablets/0000031981 localhost 3703 6703 test_keyspace 0 replica /zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-62344')
  agent_31983 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6703 -tablet-path /zk/test_ny/vt/tablets/0000031981 -logfile /vt/vt_0000031981/vttablet.log')

  # Recompute the shard layout node - until you do that, it might not be valid.
  run(vttop+'/go/cmd/vtctl/vtctl RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  _check_zk(ping_tablets=True)

  # Force the slaves to reparent assuming that all the datasets are identical.
  pause("force ReparentShard?")
  run(vttop+'/go/cmd/vtctl/vtctl -force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062344')
  _check_zk()

  expected_addr = hostname + ':6700'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  # Convert a replica to a spare. That should leave only one node serving traffic,
  # but still needs to appear in the replication graph.
  run(vttop+'/go/cmd/vtctl/vtctl ChangeType /zk/test_nj/vt/tablets/0000041983 spare')
  _check_zk()
  expected_addr = hostname + ':6701'
  _check_db_addr('test_keyspace.0.replica:_vtocc', expected_addr)

  # Perform a graceful reparent operation.
  pause("graceful ReparentShard?")
  run(vttop+'/go/cmd/vtctl/vtctl ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062044')
  _check_zk()

  expected_addr = hostname + ':6701'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  agent_62344.kill()
  agent_62044.kill()
  agent_41983.kill()
  agent_31983.kill()

  # Test address correction.
  agent_62044 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6773 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')
  # Wait a moment for address to reregister.
  time.sleep(1.0)

  expected_addr = hostname + ':6773'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  agent_62044.kill()

def run_test_reparent_down_master():
  _wipe_zk()


  run(vttop+'/go/cmd/vtctl/vtctl -force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  # Start up a master mysql and vttablet
  run(vttop+'/go/cmd/vtctl/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062344 localhost 3700 6700 test_keyspace 0 master ""')
  agent_62344 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6700 -tablet-path /zk/test_nj/vt/tablets/0000062344 -logfile /vt/vt_0000062344/vttablet.log')
  # Create a few slaves for testing reparenting.
  run(vttop+'/go/cmd/vtctl/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000062044 localhost 3701 6701 test_keyspace 0 replica /zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-62344')
  agent_62044 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6701 -tablet-path /zk/test_nj/vt/tablets/0000062044 -logfile /vt/vt_0000062044/vttablet.log')

  run(vttop+'/go/cmd/vtctl/vtctl -force InitTablet /zk/test_nj/vt/tablets/0000041983 localhost 3702 6702 test_keyspace 0 replica /zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-62344')
  agent_41983 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6702 -tablet-path /zk/test_nj/vt/tablets/0000041983 -logfile /vt/vt_0000041983/vttablet.log')

  run(vttop+'/go/cmd/vtctl/vtctl -force InitTablet /zk/test_ny/vt/tablets/0000031981 localhost 3703 6703 test_keyspace 0 replica /zk/global/vt/keyspaces/test_keyspace/shards/0/test_nj-62344')
  agent_31983 = run_bg(vttop+'/go/cmd/vttablet/vttablet -port 6703 -tablet-path /zk/test_ny/vt/tablets/0000031981 -logfile /vt/vt_0000031981/vttablet.log')

  # Recompute the shard layout node - until you do that, it might not be valid.
  run(vttop+'/go/cmd/vtctl/vtctl RebuildShard /zk/global/vt/keyspaces/test_keyspace/shards/0')
  _check_zk()

  # Force the slaves to reparent assuming that all the datasets are identical.
  run(vttop+'/go/cmd/vtctl/vtctl -force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062344')
  _check_zk()

  # Make the master agent unavailable.
  agent_62344.kill()


  expected_addr = hostname + ':6700'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  # Perform a reparent operation - this will hang for some amount of time.
  run_fail(vttop+'/go/cmd/vtctl/vtctl -wait-time 5s ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062044')

  # Should timeout and fail
  run_fail(vttop+'/go/cmd/vtctl/vtctl -wait-time 5s ScrapTablet /zk/test_nj/vt/tablets/0000062344')

  # Force the scrap action in zk even though tablet is not accessible.
  run(vttop+'/go/cmd/vtctl/vtctl -force ScrapTablet /zk/test_nj/vt/tablets/0000062344')

  run(vttop+'/go/cmd/vtctl/vtctl -force ChangeType /zk/test_nj/vt/tablets/0000062344 idle')

  idle_tablets, _ = run(vttop+'/go/cmd/vtctl/vtctl ListIdle /zk/test_nj/vt', trap_output=True)
  if '0000062344' not in idle_tablets:
    raise TestError('idle tablet not found', idle_tablets)

  # Remove pending locks (make this the force option to ReparentShard?)
  run(vttop+'/go/cmd/vtctl/vtctl -force PurgeActions /zk/global/vt/keyspaces/test_keyspace/shards/0/action')

  # Scrapping a tablet shouldn't take it out of the servering graph.
  expected_addr = hostname + ':6700'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  # Re-run reparent operation, this shoud now proceed unimpeded.
  run(vttop+'/go/cmd/vtctl/vtctl ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0 /zk/test_nj/vt/tablets/0000062044')

  _check_zk()
  expected_addr = hostname + ':6701'
  _check_db_addr('test_keyspace.0.master:_vtocc', expected_addr)

  agent_62044.kill()
  agent_41983.kill()
  agent_31983.kill()


def run_all():
  run_test_sanity()
  run_test_sanity() # run twice to check behavior with existing znode data
  run_test_restart_during_action()
  run_test_reparent_graceful()
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
