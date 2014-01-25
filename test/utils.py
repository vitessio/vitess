#!/usr/bin/python

import json
import logging
import optparse
import os
import shlex
import shutil
import signal
import socket
from subprocess import Popen, CalledProcessError, PIPE
import sys
import time
import unittest
import urllib

import MySQLdb

import environment

options = None
devnull = open('/dev/null', 'w')
hostname = socket.gethostname()

class TestError(Exception):
  pass

class Break(Exception):
  pass

environment.setup()

class LoggingStream(object):
  def __init__(self):
    self.line = ""

  def write(self, value):
    if value == "\n":
      # we already printed it
      self.line = ""
      return
    self.line += value
    logging.info("===== " + self.line)
    if value.endswith("\n"):
      self.line = ""

  def writeln(self, value):
    self.write(value)
    self.line = ""

  def flush(self):
    pass

# main executes the test classes contained in the passed module, or
# __main__ if empty.
def main(mod=None):
  if mod == None:
    mod = sys.modules['__main__']

  global options

  parser = optparse.OptionParser(usage="usage: %prog [options] [test_names]")
  parser.add_option('-d', '--debug', action='store_true', help='utils.pause() statements will wait for user input')
  parser.add_option('--skip-teardown', action='store_true')
  parser.add_option("-q", "--quiet", action="store_const", const=0, dest="verbose", default=1)
  parser.add_option("-v", "--verbose", action="store_const", const=2, dest="verbose", default=1)

  (options, args) = parser.parse_args()

  if options.verbose == 0:
    level = logging.WARNING
  elif options.verbose == 1:
    level = logging.INFO
  else:
    level = logging.DEBUG
  logging.basicConfig(format='-- %(asctime)s %(module)s:%(lineno)d %(levelname)s %(message)s', level=level)

  try:
    suite = unittest.TestSuite()
    if not args:
      # this will run the setup and teardown
      suite.addTests(unittest.TestLoader().loadTestsFromModule(mod))
    else:
      if args[0] == 'teardown':
        mod.tearDownModule()

      elif args[0] == 'setup':
        mod.setUpModule()

      else:
        for arg in args:
          # this will run the setup and teardown
          suite.addTests(unittest.TestLoader().loadTestsFromName(arg, mod))

    if suite.countTestCases() > 0:
      logger = LoggingStream()
      result = unittest.TextTestRunner(stream=logger, verbosity=options.verbose).run(suite)
      if not result.wasSuccessful():
        sys.exit(-1)
  except KeyboardInterrupt:
    logging.warning("======== Tests interrupted, cleaning up ========")
    mod.tearDownModule()

def remove_tmp_files():
  try:
    shutil.rmtree(environment.tmproot)
  except OSError as e:
    logging.debug("remove_tmp_files: %s", str(e))

def pause(prompt):
  if options.debug:
    raw_input(prompt)

# sub-process management
pid_map = {}
already_killed = []
def _add_proc(proc):
  pid_map[proc.pid] = proc
  with open(environment.tmproot+'/test-pids', 'a') as f:
    print >> f, proc.pid, os.path.basename(proc.args[0])

def kill_sub_processes():
  for proc in pid_map.values():
    if proc.pid and proc.returncode is None:
      proc.kill()
  if not os.path.exists(environment.tmproot+'/test-pids'):
    return
  with open(environment.tmproot+'/test-pids') as f:
    for line in f:
      try:
        parts = line.strip().split()
        pid = int(parts[0])
        proc = pid_map.get(pid)
        if not proc or (proc and proc.pid and proc.returncode is None):
          if pid not in already_killed:
            os.kill(pid, signal.SIGTERM)
      except OSError as e:
        logging.debug("kill_sub_processes: %s", str(e))

def kill_sub_process(proc):
  if proc is None:
    return
  pid = proc.pid
  proc.kill()
  if pid and pid in pid_map:
    del pid_map[pid]
    already_killed.append(pid)

# run in foreground, possibly capturing output
def run(cmd, trap_output=False, raise_on_error=True, **kargs):
  if isinstance(cmd, str):
    args = shlex.split(cmd)
  else:
    args = cmd
  if trap_output:
    kargs['stdout'] = PIPE
    kargs['stderr'] = PIPE
  logging.debug("run: %s %s", str(cmd), ', '.join('%s=%s' % x for x in kargs.iteritems()))
  proc = Popen(args, **kargs)
  proc.args = args
  stdout, stderr = proc.communicate()
  if proc.returncode:
    if raise_on_error:
      raise TestError('cmd fail:', args, stdout, stderr)
    else:
      logging.debug('cmd fail: %s %s %s', str(args), stdout, stderr)
  return stdout, stderr

# run sub-process, expects failure
def run_fail(cmd, **kargs):
  if isinstance(cmd, str):
    args = shlex.split(cmd)
  else:
    args = cmd
  kargs['stdout'] = PIPE
  kargs['stderr'] = PIPE
  if options.verbose == 2:
    logging.debug("run: (expect fail) %s %s", cmd, ', '.join('%s=%s' % x for x in kargs.iteritems()))
  proc = Popen(args, **kargs)
  proc.args = args
  stdout, stderr = proc.communicate()
  if proc.returncode == 0:
    logging.info("stdout:\n%sstderr:\n%s", stdout, stderr)
    raise TestError('expected fail:', args, stdout, stderr)
  return stdout, stderr

# run a daemon - kill when this script exits
def run_bg(cmd, **kargs):
  if options.verbose == 2:
    logging.debug("run: %s %s", cmd, ', '.join('%s=%s' % x for x in kargs.iteritems()))
  if isinstance(cmd, str):
    args = shlex.split(cmd)
  else:
    args = cmd
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
      if options.verbose >= 1 and proc.returncode not in (-9,):
        sys.stderr.write("proc failed: %s %s\n" % (proc.returncode, proc.args))
      if raise_on_error:
        raise CalledProcessError(proc.returncode, ' '.join(proc.args))

def run_procs(cmds, raise_on_error=True):
  procs = []
  for cmd in cmds:
    procs.append(run_bg(cmd))
  wait_procs(procs, raise_on_error=raise_on_error)

# background zk process
# (note the zkocc addresses will only work with an extra zkocc process)
zk_port_base = environment.reserve_ports(3)
def zk_setup(add_bad_host=False):
  global zk_port_base
  zk_ports = ":".join([str(zk_port_base), str(zk_port_base+1), str(zk_port_base+2)])
  run('%s -log_dir %s -zk.cfg 1@%s:%s init' % (environment.binary_path('zkctl'), environment.vtlogroot, hostname, zk_ports))
  config = environment.tmproot+'/test-zk-client-conf.json'
  with open(config, 'w') as f:
    ca_server = 'localhost:%u' % (zk_port_base+2)
    if add_bad_host:
      ca_server += ',does.not.exists:1234'
    zk_cell_mapping = {'test_nj': 'localhost:%u'%(zk_port_base+2),
                       'test_ny': 'localhost:%u'%(zk_port_base+2),
                       'test_ca': ca_server,
                       'global': 'localhost:%u'%(zk_port_base+2),
                       'test_nj:_zkocc': 'localhost:%u,localhost:%u,localhost:%u'%(environment.zkocc_port_base,environment.zkocc_port_base+1,environment.zkocc_port_base+2),
                       'test_ny:_zkocc': 'localhost:%u'%(environment.zkocc_port_base),
                       'test_ca:_zkocc': 'localhost:%u'%(environment.zkocc_port_base),
                       'global:_zkocc': 'localhost:%u'%(environment.zkocc_port_base),}
    json.dump(zk_cell_mapping, f)
  os.putenv('ZK_CLIENT_CONFIG', config)
  run(environment.binary_path('zk')+' touch -p /zk/test_nj/vt')
  run(environment.binary_path('zk')+' touch -p /zk/test_ny/vt')
  run(environment.binary_path('zk')+' touch -p /zk/test_ca/vt')

def zk_teardown():
  global zk_port_base
  zk_ports = ":".join([str(zk_port_base), str(zk_port_base+1), str(zk_port_base+2)])
  run('%s -log_dir %s -zk.cfg 1@%s:%s teardown' % (environment.binary_path('zkctl'), environment.vtlogroot, hostname, zk_ports), raise_on_error=False)

def zk_wipe():
  # Work around safety check on recursive delete.
  run(environment.binary_path('zk')+' rm -rf /zk/test_nj/vt/*')
  run(environment.binary_path('zk')+' rm -rf /zk/test_ny/vt/*')
  run(environment.binary_path('zk')+' rm -rf /zk/global/vt/*')

  run(environment.binary_path('zk')+' rm -f /zk/test_nj/vt')
  run(environment.binary_path('zk')+' rm -f /zk/test_ny/vt')
  run(environment.binary_path('zk')+' rm -f /zk/global/vt')

def validate_topology(ping_tablets=False):
  if ping_tablets:
    run_vtctl('Validate -ping-tablets')
  else:
    run_vtctl('Validate')

def zk_ls(path):
  out, err = run(environment.binary_path('zk')+' ls '+path, trap_output=True)
  return sorted(out.splitlines())

def zk_cat(path):
  out, err = run(environment.binary_path('zk')+' cat '+path, trap_output=True)
  return out

def zk_cat_json(path):
  data = zk_cat(path)
  return json.loads(data)


# vars helpers
def get_vars(port):
  """
  Returns the dict for vars, from a vtxxx process, or None
  if we can't get them.
  """
  try:
    f = urllib.urlopen('http://localhost:%u/debug/vars' % port)
    data = f.read()
    f.close()
  except:
    return None
  try:
    return json.loads(data)
  except ValueError:
    print data
    raise

def wait_for_vars(name, port):
  timeout = 5.0
  while True:
    v = get_vars(port)
    if v == None:
      logging.debug("  %s not answering at /debug/vars, waiting..." % (name))
    else:
      break

    logging.debug("sleeping a bit while we wait")
    time.sleep(0.1)
    timeout -= 0.1
    if timeout <= 0:
      raise TestError("timeout waiting for %s" % (name))

# zkocc helpers
def zkocc_start(cells=['test_nj'], extra_params=[]):
  args = [environment.binary_path('zkocc'),
          '-port', str(environment.zkocc_port_base),
          '-stderrthreshold=ERROR',
          ] + extra_params + cells
  sp = run_bg(args)
  wait_for_vars("zkocc", environment.zkocc_port_base)
  return sp

def zkocc_kill(sp):
  kill_sub_process(sp)
  sp.wait()

# vtgate helpers
def vtgate_start(cell='test_nj', retry_delay=1, retry_count=1, topo_impl=None, tablet_bson_encrypted=False):
  port = environment.reserve_ports(1)
  args = [environment.binary_path('vtgate'),
          '-port', str(port),
          '-cell', cell,
          '-retry-delay', '%ss' % (str(retry_delay)),
          '-retry-count', str(retry_count),
          '-log_dir', environment.vtlogroot,
          ] + environment.tabletconn_protocol_flags()
  if topo_impl:
    args.extend(['-topo_implementation', topo_impl])
  else:
    args.extend(environment.topo_server_flags())
  if tablet_bson_encrypted:
    args.append('-tablet-bson-encrypted')
  sp = run_bg(args)
  wait_for_vars("vtgate", port)
  return sp, port

def vtgate_kill(sp):
  if sp is None:
    return
  kill_sub_process(sp)
  sp.wait()

# vtctl helpers
def run_vtctl(clargs, log_level='', auto_log=False, expect_fail=False, **kwargs):
  args = [environment.binary_path('vtctl'), '-log_dir', environment.vtlogroot]
  args.extend(environment.topo_server_flags())
  args.extend(environment.tablet_manager_protocol_flags())
  args.extend(environment.tabletconn_protocol_flags())

  if auto_log:
    if options.verbose == 2:
      log_level='INFO'
    elif options.verbose == 1:
      log_level='WARNING'
    else:
      log_level='ERROR'

  if log_level:
    args.append('--stderrthreshold=%s' % log_level)

  if isinstance(clargs, str):
    cmd = " ".join(args) + ' ' + clargs
  else:
    cmd = args + clargs

  if expect_fail:
    return run_fail(cmd, **kwargs)
  return run(cmd, **kwargs)

# run_vtctl_json runs the provided vtctl command and returns the result
# parsed as json
def run_vtctl_json(clargs):
    stdout, stderr = run_vtctl(clargs, trap_output=True, auto_log=True)
    return json.loads(stdout)

# vtworker helpers
def run_vtworker(clargs, log_level='', auto_log=False, expect_fail=False, **kwargs):
  args = [environment.binary_path('vtworker'),
          '-log_dir', environment.vtlogroot,
          '-port', str(environment.reserve_ports(1))]

  if auto_log:
    if options.verbose == 2:
      log_level='INFO'
    elif options.verbose == 1:
      log_level='WARNING'
    else:
      log_level='ERROR'
  if log_level:
    args.append('--stderrthreshold=%s' % log_level)

  cmd = args + clargs
  if expect_fail:
    return run_fail(cmd, **kwargs)
  return run(cmd, **kwargs)

# vtclient2 helpers
# driver is one of:
# - vttablet (default), vttablet-streaming
# - vtdb, vtdb-streaming (default topo server)
# - vtdb-zk, vtdb-zk-streaming (forced zk topo server)
# - vtdb-zkocc, vtdb-zkocc-streaming (forced zkocc topo server)
# path is either: keyspace/shard for vttablet* or zk path for vtdb*
def vtclient2(uid, path, query, bindvars=None, user=None, password=None, driver=None,
              verbose=False, raise_on_error=True):
  if (user is None) != (password is None):
    raise TypeError("you should provide either both or none of user and password")

  # for ZK paths to not have // in the path, that confuses things
  if path.startswith('/'):
    path = path[1:]
  server = "localhost:%u/%s" % (uid, path)

  cmdline = [environment.binary_path('vtclient2'), '-server', server]
  cmdline += environment.topo_server_flags()
  cmdline += environment.tabletconn_protocol_flags()
  if user is not None:
    cmdline.extend(['-tablet-bson-username', user,
                    '-tablet-bson-password', password])
  if bindvars:
    cmdline.extend(['-bindvars', bindvars])
  if driver:
    cmdline.extend(['-driver', driver])
  if verbose:
    cmdline.extend(['-alsologtostderr', '-verbose'])
  cmdline.append(query)

  return run(cmdline, raise_on_error=raise_on_error, trap_output=True)

# mysql helpers
def mysql_query(uid, dbname, query):
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket='%s/vt_%010d/mysql.sock' % (environment.vtdataroot, uid),
                         db=dbname)
  cursor = conn.cursor()
  cursor.execute(query)
  try:
    return cursor.fetchall()
  finally:
    conn.close()

def mysql_write_query(uid, dbname, query):
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket='%s/vt_%010d/mysql.sock' % (environment.vtdataroot, uid),
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
                         unix_socket='%s/vt_%010d/mysql.sock' % (environment.vtdataroot, uid))
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
      logging.warning("wait_db_read_only: %s", str(e))
      time.sleep(1.0)
  raise e
