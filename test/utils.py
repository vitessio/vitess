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

import tablet

options = None
devnull = open('/dev/null', 'w')
vttop = os.environ['VTTOP']
vtroot = os.environ['VTROOT']
vtdataroot = os.environ.get('VTDATAROOT', '/vt')
hostname = socket.gethostname()
vtportstart = int(os.environ.get('VTPORTSTART', '6700'))

class TestError(Exception):
  pass

class Break(Exception):
  pass

# tmp files management: all under /vt/tmp
tmp_root = os.path.join(vtdataroot, 'tmp')
def setup():
  try:
    os.makedirs(tmp_root)
  except OSError:
    # directory already exists
    pass
setup()

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
  parser.add_option("--no-build", action="store_true")

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
    shutil.rmtree(tmp_root)
  except OSError as e:
    logging.debug("remove_tmp_files: %s", str(e))

def pause(prompt):
  if options.debug:
    raw_input(prompt)

# port management: reserve count consecutive ports, returns the first one
def reserve_ports(count):
  global vtportstart
  result = vtportstart
  vtportstart += count
  return result

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
          if pid not in already_killed:
            os.kill(pid, signal.SIGTERM)
      except OSError as e:
        logging.debug("kill_sub_processes: %s", str(e))

def kill_sub_process(proc):
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

# compile command line programs
compiled_progs = []
def prog_compile(names):
  for name in names:
    if name in compiled_progs:
      continue
    compiled_progs.append(name)
    if options.no_build:
      logging.debug('Skipping build of %s', name)
    else:
      run('go build', cwd=vttop+'/go/cmd/'+name)

# background zk process
# (note the zkocc addresses will only work with an extra zkocc process)
zk_port_base = reserve_ports(3)
zkocc_port_base = reserve_ports(3)
def zk_setup(add_bad_host=False):
  global zk_port_base
  global zkocc_port_base
  zk_ports = ":".join([str(zk_port_base), str(zk_port_base+1), str(zk_port_base+2)])
  prog_compile(['zkctl', 'zk'])
  run('%s/bin/zkctl -log_dir %s -zk.cfg 1@%s:%s init' % (vtroot, tmp_root, hostname, zk_ports))
  config = tmp_root+'/test-zk-client-conf.json'
  with open(config, 'w') as f:
    ca_server = 'localhost:%u' % (zk_port_base+2)
    if add_bad_host:
      ca_server += ',does.not.exists:1234'
    zk_cell_mapping = {'test_nj': 'localhost:%u'%(zk_port_base+2),
                       'test_ny': 'localhost:%u'%(zk_port_base+2),
                       'test_ca': ca_server,
                       'global': 'localhost:%u'%(zk_port_base+2),
                       'test_nj:_zkocc': 'localhost:%u,localhost:%u,localhost:%u'%(zkocc_port_base,zkocc_port_base+1,zkocc_port_base+2),
                       'test_ny:_zkocc': 'localhost:%u'%(zkocc_port_base),
                       'test_ca:_zkocc': 'localhost:%u'%(zkocc_port_base),
                       'global:_zkocc': 'localhost:%u'%(zkocc_port_base),}
    json.dump(zk_cell_mapping, f)
  os.putenv('ZK_CLIENT_CONFIG', config)
  run(vtroot+'/bin/zk touch -p /zk/test_nj/vt')
  run(vtroot+'/bin/zk touch -p /zk/test_ny/vt')
  run(vtroot+'/bin/zk touch -p /zk/test_ca/vt')

def zk_teardown():
  global zk_port_base
  zk_ports = ":".join([str(zk_port_base), str(zk_port_base+1), str(zk_port_base+2)])
  run('%s/bin/zkctl -log_dir %s -zk.cfg 1@%s:%s teardown' % (vtroot, tmp_root, hostname, zk_ports), raise_on_error=False)

def zk_wipe():
  # Work around safety check on recursive delete.
  run(vtroot+'/bin/zk rm -rf /zk/test_nj/vt/*')
  run(vtroot+'/bin/zk rm -rf /zk/test_ny/vt/*')
  run(vtroot+'/bin/zk rm -rf /zk/global/vt/*')

  run(vtroot+'/bin/zk rm -f /zk/test_nj/vt')
  run(vtroot+'/bin/zk rm -f /zk/test_ny/vt')
  run(vtroot+'/bin/zk rm -f /zk/global/vt')

def validate_topology(ping_tablets=False):
  if ping_tablets:
    run_vtctl('Validate -ping-tablets')
  else:
    run_vtctl('Validate')

def zk_ls(path):
  out, err = run(vtroot+'/bin/zk ls '+path, trap_output=True)
  return sorted(out.splitlines())

def zk_cat(path):
  out, err = run(vtroot+'/bin/zk cat '+path, trap_output=True)
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
  global zkocc_port_base
  prog_compile(['zkocc'])
  args = [vtroot+'/bin/zkocc',
          '-port', str(zkocc_port_base),
          '-stderrthreshold=ERROR',
          ] + extra_params + cells
  sp = run_bg(args)
  wait_for_vars("zkocc", zkocc_port_base)
  return sp

def zkocc_kill(sp):
  kill_sub_process(sp)
  sp.wait()

# vtgate helpers
def vtgate_start(cell='test_nj', retry_delay=1, retry_count=1, topo_impl="zookeeper", tablet_bson_encrypted=False):
  port = reserve_ports(1)
  prog_compile(['vtgate'])
  args = [vtroot+'/bin/vtgate',
          '-port', str(port),
          '-cell', cell,
          '-topo_implementation', topo_impl,
          '-retry-delay', '%ss' % (str(retry_delay)),
          '-retry-count', str(retry_count),
          '-log_dir', tmp_root,
          ]
  if tablet_bson_encrypted:
    args.append('-tablet-bson-encrypted')
  sp = run_bg(args)
  wait_for_vars("vtgate", port)
  return sp, port

def vtgate_kill(sp):
  kill_sub_process(sp)
  sp.wait()

# vtctl helpers
def run_vtctl(clargs, log_level='', auto_log=False, expect_fail=False, **kwargs):
  prog_compile(['vtctl'])
  args = [vtroot+'/bin/vtctl', '-log_dir', tmp_root]

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

# vtclient2 helpers
# driver is one of:
# - vttablet (default), vttablet-streaming
# - vtdb, vtdb-streaming
# - vtdb-zkocc, vtdb-streaming-zkocc
# path is either: keyspace/shard for vttablet* or zk path for vtdb*
def vtclient2(uid, path, query, bindvars=None, user=None, password=None, driver=None,
              verbose=False, raise_on_error=True):
  prog_compile(['vtclient2'])
  if (user is None) != (password is None):
    raise TypeError("you should provide either both or none of user and password")

  # for ZK paths to not have // in the path, that confuses things
  if path.startswith('/'):
    path = path[1:]
  server = "localhost:%u/%s" % (uid, path)
  if user is not None:
    server = "%s:%s@%s" % (user, password, server)

  cmdline = [vtroot+'/bin/vtclient2', '-server', server]
  if bindvars:
    cmdline.extend(['-bindvars', bindvars])
  if driver:
    cmdline.extend(['-driver', driver])
  if verbose:
    cmdline.append('-verbose')
  cmdline.append(query)

  return run(cmdline, raise_on_error=raise_on_error, trap_output=True)

# mysql helpers
def mysql_query(uid, dbname, query):
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket='%s/vt_%010d/mysql.sock' % (vtdataroot, uid),
                         db=dbname)
  cursor = conn.cursor()
  cursor.execute(query)
  try:
    return cursor.fetchall()
  finally:
    conn.close()

def mysql_write_query(uid, dbname, query):
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket='%s/vt_%010d/mysql.sock' % (vtdataroot, uid),
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
                         unix_socket='%s/vt_%010d/mysql.sock' % (vtdataroot, uid))
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
