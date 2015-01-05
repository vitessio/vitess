#!/usr/bin/env python

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
import urllib2

import MySQLdb

import environment

from vtctl import vtctl_client
from mysql_flavor import set_mysql_flavor
from protocols_flavor import set_protocols_flavor, protocols_flavor
from topo_flavor.server import set_topo_server_flavor

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

def add_options(parser):
  parser.add_option('-d', '--debug', action='store_true',
                    help='utils.pause() statements will wait for user input')
  parser.add_option('-k', '--keep-logs', action='store_true',
                    help="Don't delete log files on teardown.")
  parser.add_option("-q", "--quiet", action="store_const", const=0, dest="verbose", default=1)
  parser.add_option("-v", "--verbose", action="store_const", const=2, dest="verbose", default=1)
  parser.add_option('--skip-teardown', action='store_true')
  parser.add_option("--mysql-flavor")
  parser.add_option("--protocols-flavor")
  parser.add_option("--topo-server-flavor", default="zookeeper")

def set_options(opts):
  global options
  options = opts

  set_mysql_flavor(options.mysql_flavor)
  set_protocols_flavor(options.protocols_flavor)
  set_topo_server_flavor(options.topo_server_flavor)

# main executes the test classes contained in the passed module, or
# __main__ if empty.
def main(mod=None):
  if mod == None:
    mod = sys.modules['__main__']

  global options

  parser = optparse.OptionParser(usage="usage: %prog [options] [test_names]")
  add_options(parser)
  (options, args) = parser.parse_args()

  if options.verbose == 0:
    level = logging.WARNING
  elif options.verbose == 1:
    level = logging.INFO
  else:
    level = logging.DEBUG
  logging.getLogger().setLevel(level)
  logging.basicConfig(format='-- %(asctime)s %(module)s:%(lineno)d %(levelname)s %(message)s')

  set_options(options)

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
      result = unittest.TextTestRunner(stream=logger, verbosity=options.verbose, failfast=True).run(suite)
      if not result.wasSuccessful():
        sys.exit(-1)
  except KeyboardInterrupt:
    logging.warning("======== Tests interrupted, cleaning up ========")
    mod.tearDownModule()
    # If you interrupt a test, you probably want to stop evaluating the rest.
    sys.exit(1)
  finally:
    if options.keep_logs:
      logging.warning("Leaving temporary files behind (--keep-logs), please "
                      "clean up before next run: " + os.environ["VTDATAROOT"])

def remove_tmp_files():
  if options.keep_logs:
    return
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

def kill_sub_process(proc, soft=False):
  if proc is None:
    return
  pid = proc.pid
  if soft:
    proc.terminate()
  else:
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
  if 'extra_env' in kargs:
    kargs['env'] = os.environ.copy()
    if kargs['extra_env']:
      kargs['env'].update(kargs['extra_env'])
    del(kargs['extra_env'])
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

def validate_topology(ping_tablets=False):
  if ping_tablets:
    run_vtctl(['Validate', '-ping-tablets'])
  else:
    run_vtctl(['Validate'])

def zk_ls(path):
  out, err = run(environment.binary_argstr('zk')+' ls '+path, trap_output=True)
  return sorted(out.splitlines())

def zk_cat(path):
  out, err = run(environment.binary_argstr('zk')+' cat '+path, trap_output=True)
  return out

def zk_cat_json(path):
  data = zk_cat(path)
  return json.loads(data)

# wait_step is a helper for looping until a condition is true.
# use as follow:
#    timeout = 10
#    while True:
#      if done:
#        break
#      timeout = utils.wait_step('condition', timeout)
def wait_step(msg, timeout, sleep_time=1.0):
  timeout -= sleep_time
  if timeout <= 0:
    raise TestError("timeout waiting for condition '%s'" % msg)
  logging.debug("Sleeping for %f seconds waiting for condition '%s'" %
               (sleep_time, msg))
  time.sleep(sleep_time)
  return timeout

# vars helpers
def get_vars(port):
  """
  Returns the dict for vars, from a vtxxx process, or None
  if we can't get them.
  """
  try:
    url = 'http://localhost:%u/debug/vars' % int(port)
    f = urllib2.urlopen(url)
    data = f.read()
    f.close()
  except:
    return None
  try:
    return json.loads(data)
  except ValueError:
    print data
    raise

# wait_for_vars will wait until we can actually get the vars from a process,
# and if var is specified, will wait until that var is in vars
def wait_for_vars(name, port, var=None):
  timeout = 5.0
  while True:
    v = get_vars(port)
    if v and (var is None or var in v):
      break
    timeout = wait_step('waiting for /debug/vars of %s' % name, timeout)

# vtgate helpers, assuming it always restarts on the same port
def vtgate_start(vtport=None, cell='test_nj', retry_delay=1, retry_count=1,
                 topo_impl=None, tablet_bson_encrypted=False, cache_ttl='1s',
                 auth=False, timeout="5s", cert=None, key=None, ca_cert=None,
                 socket_file=None, schema=None, extra_args=None):
  port = vtport or environment.reserve_ports(1)
  secure_port = None
  args = environment.binary_args('vtgate') + [
          '-port', str(port),
          '-cell', cell,
          '-retry-delay', '%ss' % (str(retry_delay)),
          '-retry-count', str(retry_count),
          '-log_dir', environment.vtlogroot,
          '-srv_topo_cache_ttl', cache_ttl,
          '-timeout', timeout,
          ] + protocols_flavor().tabletconn_protocol_flags()
  if topo_impl:
    args.extend(['-topo_implementation', topo_impl])
  else:
    args.extend(environment.topo_server().flags())
  if tablet_bson_encrypted:
    args.append('-tablet-bson-encrypted')
  if auth:
    args.extend(['-auth-credentials', os.path.join(environment.vttop, 'test', 'test_data', 'authcredentials_test.json')])
  if cert:
    secure_port = environment.reserve_ports(1)
    args.extend(['-secure-port', '%s' % secure_port,
                 '-cert', cert,
                 '-key', key])
    if ca_cert:
      args.extend(['-ca_cert', ca_cert])
  if socket_file:
    args.extend(['-socket_file', socket_file])
  if schema:
    fname = os.path.join(environment.tmproot, "vtgate_schema.json")
    with open(fname, "w") as f:
      f.write(schema)
    args.extend(['-schema-file', fname])

  if extra_args:
    args.extend(extra_args)

  sp = run_bg(args)
  if cert:
    wait_for_vars("vtgate", port, "SecureConnections")
    return sp, port, secure_port
  else:
    wait_for_vars("vtgate", port)
    return sp, port

def vtgate_kill(sp):
  if sp is None:
    return
  kill_sub_process(sp, soft=True)
  sp.wait()

# vtctl helpers
# The modes are not all equivalent, and we don't really thrive for it.
# If a client needs to rely on vtctl's command line behavior, make
# sure to use mode=utils.VTCTL_VTCTL
VTCTL_AUTO        = 0
VTCTL_VTCTL       = 1
VTCTL_VTCTLCLIENT = 2
VTCTL_RPC         = 3
def run_vtctl(clargs, log_level='', auto_log=False, expect_fail=False,
              mode=VTCTL_AUTO, **kwargs):
  if mode == VTCTL_AUTO:
    if not expect_fail and vtctld:
      mode = VTCTL_RPC
    else:
      mode = VTCTL_VTCTL

  if mode == VTCTL_VTCTL:
    return run_vtctl_vtctl(clargs, log_level=log_level, auto_log=auto_log,
                           expect_fail=expect_fail, **kwargs)
  elif mode == VTCTL_VTCTLCLIENT:
    result = vtctld.vtctl_client(clargs)
    return result, ""
  elif mode == VTCTL_RPC:
    logging.debug("vtctl: %s", " ".join(clargs))
    result = vtctld_connection.execute_vtctl_command(clargs, info_to_debug=True, action_timeout=120)
    return result, ""

  raise Exception('Unknown mode: %s', mode)

def run_vtctl_vtctl(clargs, log_level='', auto_log=False, expect_fail=False,
                    **kwargs):
  args = environment.binary_args('vtctl') + ['-log_dir', environment.vtlogroot]
  args.extend(environment.topo_server().flags())
  args.extend(protocols_flavor().tablet_manager_protocol_flags())
  args.extend(protocols_flavor().tabletconn_protocol_flags())

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
  args = environment.binary_args('vtworker') + [
          '-log_dir', environment.vtlogroot,
          '-min_healthy_rdonly_endpoints', '1',
          '-port', str(environment.reserve_ports(1))]
  args.extend(environment.topo_server().flags())
  args.extend(protocols_flavor().tablet_manager_protocol_flags())

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
# path is either: keyspace/shard for vttablet* or zk path for vtdb*
def vtclient2(uid, path, query, bindvars=None, user=None, password=None, driver=None,
              verbose=False, raise_on_error=True):
  if (user is None) != (password is None):
    raise TypeError("you should provide either both or none of user and password")

  # for ZK paths to not have // in the path, that confuses things
  if path.startswith('/'):
    path = path[1:]
  server = "localhost:%u/%s" % (uid, path)

  cmdline = environment.binary_args('vtclient2') + ['-server', server]
  cmdline += environment.topo_server().flags()
  cmdline += protocols_flavor().tabletconn_protocol_flags()
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

def check_srv_keyspace(cell, keyspace, expected, keyspace_id_type='uint64'):
  ks = run_vtctl_json(['GetSrvKeyspace', cell, keyspace])
  result = ""
  for tablet_type in sorted(ks['TabletTypes']):
    result += "Partitions(%s):" % tablet_type
    partition = ks['Partitions'][tablet_type]
    for shard in partition['Shards']:
      result = result + " %s-%s" % (shard['KeyRange']['Start'],
                                    shard['KeyRange']['End'])
    result += "\n"
  result += "TabletTypes: " + ",".join(sorted(ks['TabletTypes']))
  logging.debug("Cell %s keyspace %s has data:\n%s", cell, keyspace, result)
  if expected != result:
    raise Exception("Mismatch in srv keyspace for cell %s keyspace %s, expected:\n%s\ngot:\n%s" % (
                   cell, keyspace, expected, result))
  if 'keyspace_id' != ks.get('ShardingColumnName'):
    raise Exception("Got wrong ShardingColumnName in SrvKeyspace: %s" %
                   str(ks))
  if keyspace_id_type != ks.get('ShardingColumnType'):
    raise Exception("Got wrong ShardingColumnType in SrvKeyspace: %s" %
                   str(ks))

def get_status(port):
  return urllib2.urlopen('http://localhost:%u%s' % (port, environment.status_url)).read()

def curl(url, request=None, data=None, background=False, retry_timeout=0, **kwargs):
  args = [environment.curl_bin, '--silent', '--no-buffer', '--location']
  if not background:
    args.append('--show-error')
  if request:
    args.extend(['--request', request])
  if data:
    args.extend(['--data', data])
  args.append(url)

  if background:
    return run_bg(args, **kwargs)

  if retry_timeout > 0:
    while True:
      try:
        return run(args, trap_output=True, **kwargs)
      except TestError as e:
        retry_timeout = wait_step('cmd: %s, error: %s' % (str(args), str(e)), retry_timeout)

  return run(args, trap_output=True, **kwargs)

class VtctldError(Exception): pass

# save the first running instance, and an RPC connection to it,
# so we can use it to run remote vtctl commands
vtctld = None
vtctld_connection = None

class Vtctld(object):

  def __init__(self):
    self.port = environment.reserve_ports(1)

  def dbtopo(self):
    data = json.load(urllib2.urlopen('http://localhost:%u/dbtopo?format=json' %
                                     self.port))
    if data["Error"]:
      raise VtctldError(data)
    return data["Topology"]

  def serving_graph(self):
    data = json.load(urllib2.urlopen('http://localhost:%u/serving_graph/test_nj?format=json' % self.port))
    if data['Errors']:
      raise VtctldError(data['Errors'])
    return data["Keyspaces"]

  def start(self):
    args = environment.binary_args('vtctld') + [
            '-debug',
            '-templates', environment.vttop + '/go/cmd/vtctld/templates',
            '-log_dir', environment.vtlogroot,
            '-port', str(self.port),
            ] + \
            environment.topo_server().flags() + \
            protocols_flavor().tablet_manager_protocol_flags()
    stderr_fd = open(os.path.join(environment.tmproot, "vtctld.stderr"), "w")
    self.proc = run_bg(args, stderr=stderr_fd)

    # wait for the process to listen to RPC
    timeout = 30
    while True:
      v = get_vars(self.port)
      if v:
        break
      timeout = wait_step('waiting for vtctld to start', timeout,
                          sleep_time=0.2)

    # save the running instance so vtctl commands can be remote executed now
    global vtctld, vtctld_connection
    if not vtctld:
      vtctld = self
      vtctld_connection = vtctl_client.connect(
          protocols_flavor().vtctl_client_protocol(), 'localhost:%u' % self.port, 30)

    return self.proc

  def process_args(self):
    return ['-vtctld_addr', 'http://localhost:%u/' % self.port]

  def vtctl_client(self, args):
    if options.verbose == 2:
      log_level='INFO'
    elif options.verbose == 1:
      log_level='WARNING'
    else:
      log_level='ERROR'

    out, err = run(environment.binary_args('vtctlclient') +
                   ['-vtctl_client_protocol',
                    protocols_flavor().vtctl_client_protocol(),
                    '-server', 'localhost:%u' % self.port,
                    '-stderrthreshold', log_level] + args,
                   trap_output=True)
    return out
