#!/usr/bin/python

import json
import os
import shlex
import shutil
import signal
import socket
from subprocess import Popen, CalledProcessError, PIPE
import sys
import time
import types

import MySQLdb

options = None
devnull = open('/dev/null', 'w')
vttop = os.environ['VTTOP']
vtroot = os.environ['VTROOT']
hostname = socket.gethostname()

class TestError(Exception):
  pass

class Break(Exception):
  pass

# tmp files management: all under /vt/tmp
tmp_root = '/vt/tmp'
try:
  os.makedirs(tmp_root)
except OSError:
  # directory already exists
  pass

def remove_tmp_files():
  try:
    shutil.rmtree(tmp_root)
  except OSError as e:
      if options.verbose:
        print >> sys.stderr, e, tmp_root

def pause(prompt):
  if options.debug:
    raw_input(prompt)

# sub-process management
pid_map = {}
already_killed = []
def _add_proc(proc):
  pid_map[proc.pid] = proc
  with open(tmp_root+'/test-pids', 'a') as f:
    print >> f, proc.pid, os.path.basename(proc.args[0])

def kill_sub_processes():
  for proc in pid_map.values():
    if proc.pid and proc.returncode is None:
      proc.kill()
  if not os.path.exists(tmp_root+'/test-pids'):
    return
  with open(tmp_root+'/test-pids') as f:
    for line in f:
      try:
        parts = line.strip().split()
        pid = int(parts[0])
        proc = pid_map.get(pid)
        if not proc or (proc and proc.pid and proc.returncode is None):
          if not pid in already_killed:
            os.kill(pid, signal.SIGTERM)
      except OSError as e:
        if options.verbose:
          print >> sys.stderr, e

def kill_sub_process(proc):
  pid = proc.pid
  proc.kill()
  if pid and pid in pid_map:
    del pid_map[pid]
    already_killed.append(pid)

# run in foreground, possibly capturing output
def run(cmd, trap_output=False, raise_on_error=True, **kargs):
  if isinstance(cmd, types.StringTypes):
    args = shlex.split(cmd)
  else:
    args = cmd
  if trap_output:
    kargs['stdout'] = PIPE
    kargs['stderr'] = PIPE
  if options.verbose:
    print "run:", cmd, ', '.join('%s=%s' % x for x in kargs.iteritems())
  proc = Popen(args, **kargs)
  proc.args = args
  stdout, stderr = proc.communicate()
  if proc.returncode:
    if raise_on_error:
      raise TestError('cmd fail:', args, stdout, stderr)
    else:
      if options.verbose:
        print 'cmd fail:', args, stdout, stderr
  return stdout, stderr

# run sub-process, expects failure
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

def wait_procs(proc_list, raise_on_error=True):
  for proc in proc_list:
    pid = proc.pid
    if pid:
      already_killed.append(pid)
  for proc in proc_list:
    proc.wait()
  for proc in proc_list:
    if proc.returncode:
      if options.verbose and proc.returncode not in (-9,):
        sys.stderr.write("proc failed: %s %s\n" % (proc.returncode, proc.args))
      if raise_on_error:
        raise CalledProcessError(proc.returncode, proc.args)

def run_procs(cmds, raise_on_error=True):
  procs = []
  for cmd in cmds:
    procs.append(run_bg(cmd))
  wait_procs(procs, raise_on_error=raise_on_error)

# compile command line programs
compiled_progs = []
def prog_compile(names):
  for name in names:
    if name in compiled_progs:
      continue
    compiled_progs.append(name)
    run('go build', cwd=vttop+'/go/cmd/'+name)

# background zk process
# (note the zkocc addresses will only work with an extra zkocc process)
def zk_setup():
  prog_compile(['zkctl', 'zk'])
  run(vtroot+'/bin/zkctl -zk.cfg 1@'+hostname+':3801:3802:3803 init')
  config = tmp_root+'/test-zk-client-conf.json'
  with open(config, 'w') as f:
    zk_cell_mapping = {'test_nj': 'localhost:3803',
                       'test_ny': 'localhost:3803',
                       'test_ca': 'localhost:3803',
                       'global': 'localhost:3803',
                       'test_nj:_zkocc': 'localhost:14850,localhost:14851,localhost:14852',
                       'test_ny:_zkocc': 'localhost:14850',
                       'test_ca:_zkocc': 'localhost:14850',
                       'global:_zkocc': 'localhost:14850',}
    json.dump(zk_cell_mapping, f)
  os.putenv('ZK_CLIENT_CONFIG', config)
  run(vtroot+'/bin/zk touch -p /zk/test_nj/vt')
  run(vtroot+'/bin/zk touch -p /zk/test_ny/vt')
  run(vtroot+'/bin/zk touch -p /zk/test_ca/vt')

def zk_teardown():
  run(vtroot+'/bin/zkctl -zk.cfg 1@'+hostname+':3801:3802:3803 teardown', raise_on_error=False)

def zk_wipe():
  run(vtroot+'/bin/zk rm -rf /zk/test_nj/vt')
  run(vtroot+'/bin/zk rm -rf /zk/test_ny/vt')
  #run(vtroot+'/bin/zk rm -rf /zk/test_ca/vt')
  run(vtroot+'/bin/zk rm -rf /zk/global/vt')

def zk_check(ping_tablets=False):
  if ping_tablets:
    run_vtctl('Validate -ping-tablets /zk/global/vt/keyspaces')
  else:
    run_vtctl('Validate /zk/global/vt/keyspaces')

# vtctl helpers
def run_vtctl(clargs, log_level='WARNING', **kwargs):
  prog_compile(['vtctl'])
  args = [vtroot+'/bin/vtctl',
          '-log.level='+log_level,
          '-logfile=/dev/null']
  if isinstance(clargs, types.StringTypes):
    cmd = " ".join(args) + ' ' + clargs
  else:
    cmd = args + clargs
  return run(cmd, **kwargs)

# vtclient2 helpers
# driver is one of vttablet (default), vttablet-streaming, vtdb, vtdb-streaming
def vttablet_query(uid, dbname, query, user=None, password=None, driver=None,
                   verbose=False, raise_on_error=True):
  prog_compile(['vtclient2'])
  if (user is None) != (password is None):
    raise TypeError("you should provide either both or none of user and password")

  # for ZK paths to not have // in the path, that confuses things
  if dbname.startswith('/'):
    dbname = dbname[1:]
  server = "localhost:%u/%s" % (uid, dbname)
  if user is not None:
    server = "%s:%s@%s" % (user, password, server)

  cmdline = [vtroot+'/bin/vtclient2', '-server', server]
  if driver:
    cmdline.extend(["-driver", driver])
  if verbose:
    cmdline.append("-verbose")
  cmdline.append('"%s"' % query)

  return run(' '.join(cmdline), raise_on_error=raise_on_error, trap_output=True)

# mysql helpers
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
