#!/usr/bin/env python

# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Common import for all tests."""

import base64
import contextlib
import json
import logging
import optparse
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import unittest
import urllib2

from vtdb import prefer_vtroot_imports  # pylint: disable=unused-import
from vtdb import vtgate_client

import environment
from mysql_flavor import mysql_flavor
from mysql_flavor import set_mysql_flavor
import MySQLdb
from protocols_flavor import protocols_flavor
from topo_flavor.server import set_topo_server_flavor
from vtctl import vtctl_client
from vtdb import keyrange_constants
from vtgate_gateway_flavor.gateway import set_vtgate_gateway_flavor
from vtgate_gateway_flavor.gateway import vtgate_gateway_flavor
from vtproto import topodata_pb2


options = None
devnull = open('/dev/null', 'w')
try:
  hostname = socket.getaddrinfo(
      socket.getfqdn(), None, 0, 0, 0, socket.AI_CANONNAME)[0][3]
except socket.gaierror:
  # Fallback to 'localhost' if getfqdn() returns this value for "::1" and
  # getaddrinfo() cannot resolve it and throws an exception.
  # This error scenario was observed on mberlin@'s corp Macbook in 2018.
  if socket.getfqdn() == '1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa':  # pylint: disable=line-too-long
    hostname = 'localhost'
  else:
    raise


class TestError(Exception):
  pass


class Break(Exception):
  pass

environment.setup()


class LoggingStream(object):

  def __init__(self):
    self.line = ''

  def write(self, value):
    if value == '\n':
      # we already printed it
      self.line = ''
      return
    self.line += value
    logging.info('===== ' + self.line)
    if value.endswith('\n'):
      self.line = ''

  def writeln(self, value):
    self.write(value)
    self.line = ''

  def flush(self):
    pass


def add_options(parser):
  environment.add_options(parser)
  parser.add_option('-d', '--debug', action='store_true',
                    help='utils.pause() statements will wait for user input')
  parser.add_option('-k', '--keep-logs', action='store_true',
                    help='Do not delete log files on teardown.')
  parser.add_option(
      '-q', '--quiet', action='store_const', const=0, dest='verbose', default=1)
  parser.add_option(
      '-v', '--verbose', action='store_const', const=2, dest='verbose',
      default=1)
  parser.add_option('--skip-build', action='store_true',
                    help='Do not build the go binaries when running the test.')
  parser.add_option(
      '--skip-teardown', action='store_true',
      help='Leave the global processes running after the test is done.')
  parser.add_option('--mysql-flavor')
  parser.add_option('--protocols-flavor', default='grpc')
  parser.add_option('--topo-server-flavor', default='zk2')
  parser.add_option('--vtgate-gateway-flavor', default='discoverygateway')


def set_options(opts):
  global options
  options = opts

  set_mysql_flavor(options.mysql_flavor)
  environment.setup_protocol_flavor(options.protocols_flavor)
  set_topo_server_flavor(options.topo_server_flavor)
  set_vtgate_gateway_flavor(options.vtgate_gateway_flavor)
  environment.skip_build = options.skip_build


# main executes the test classes contained in the passed module, or
# __main__ if empty.
def main(mod=None, test_options=None):
  """The replacement main method, which parses args and runs tests.

  Args:
    mod: module that contains the test methods.
    test_options: a function which adds OptionParser options that are specific
      to a test file.
  """
  if mod is None:
    mod = sys.modules['__main__']

  global options

  parser = optparse.OptionParser(usage='usage: %prog [options] [test_names]')
  add_options(parser)
  if test_options:
    test_options(parser)
  (options, args) = parser.parse_args()

  environment.set_log_level(options.verbose)
  logging.basicConfig(
      format='-- %(asctime)s %(module)s:%(lineno)d %(levelname)s %(message)s')

  set_options(options)

  run_tests(mod, args)


def run_tests(mod, args):
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
      result = unittest.TextTestRunner(
          stream=logger, verbosity=options.verbose, failfast=True).run(suite)
      if not result.wasSuccessful():
        sys.exit(-1)
  except KeyboardInterrupt:
    logging.warning('======== Tests interrupted, cleaning up ========')
    mod.tearDownModule()
    # If you interrupt a test, you probably want to stop evaluating the rest.
    sys.exit(1)
  finally:
    if options.keep_logs:
      logging.warning('Leaving temporary files behind (--keep-logs), please '
                      'clean up before next run: ' + os.environ['VTDATAROOT'])


def remove_tmp_files():
  if options.keep_logs:
    return
  try:
    shutil.rmtree(environment.tmproot)
  except OSError as e:
    logging.debug('remove_tmp_files: %s', str(e))


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


def required_teardown():
  """Required cleanup steps that can't be skipped with --skip-teardown."""
  # We can't skip closing of gRPC connections, because the Python interpreter
  # won't let us die if any connections are left open.
  global vtctld_connection, vtctld
  if vtctld_connection:
    vtctld_connection.close()
    vtctld_connection = None
    vtctld = None


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
        logging.debug('kill_sub_processes: %s', str(e))


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
    kargs['stdout'] = subprocess.PIPE
    kargs['stderr'] = subprocess.PIPE
  logging.debug(
      'run: %s %s', str(cmd),
      ', '.join('%s=%s' % x for x in kargs.iteritems()))
  proc = subprocess.Popen(args, **kargs)
  proc.args = args
  stdout, stderr = proc.communicate()
  if proc.returncode:
    if raise_on_error:
      pause('cmd fail: %s, pausing...' % (args))
      raise TestError('cmd fail:', args, proc.returncode, stdout, stderr)
    else:
      logging.debug('cmd fail: %s %d %s %s',
                    str(args), proc.returncode, stdout, stderr)
  return stdout, stderr


# run sub-process, expects failure
def run_fail(cmd, **kargs):
  if isinstance(cmd, str):
    args = shlex.split(cmd)
  else:
    args = cmd
  kargs['stdout'] = subprocess.PIPE
  kargs['stderr'] = subprocess.PIPE
  if options.verbose == 2:
    logging.debug(
        'run: (expect fail) %s %s', cmd,
        ', '.join('%s=%s' % x for x in kargs.iteritems()))
  proc = subprocess.Popen(args, **kargs)
  proc.args = args
  stdout, stderr = proc.communicate()
  if proc.returncode == 0 or options.verbose == 2:
    logging.info('stdout:\n%sstderr:\n%s', stdout, stderr)
  if proc.returncode == 0:
    raise TestError('expected fail:', args, stdout, stderr)
  return stdout, stderr


# run a daemon - kill when this script exits
def run_bg(cmd, **kargs):
  if options.verbose == 2:
    logging.debug(
        'run: %s %s', cmd, ', '.join('%s=%s' % x for x in kargs.iteritems()))
  if 'extra_env' in kargs:
    kargs['env'] = os.environ.copy()
    if kargs['extra_env']:
      kargs['env'].update(kargs['extra_env'])
    del kargs['extra_env']
  if isinstance(cmd, str):
    args = shlex.split(cmd)
  else:
    args = cmd
  proc = subprocess.Popen(args=args, **kargs)
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
        sys.stderr.write('proc failed: %s %s\n' % (proc.returncode, proc.args))
      if raise_on_error:
        raise subprocess.CalledProcessError(proc.returncode,
                                            ' '.join(proc.args))


def validate_topology(ping_tablets=False):
  if ping_tablets:
    run_vtctl(['Validate', '-ping-tablets'])
  else:
    run_vtctl(['Validate'])


# wait_step is a helper for looping until a condition is true.
# use as follow:
#    timeout = 10
#    while True:
#      <step>
#      if <done>:
#        break
#      timeout = utils.wait_step('description of condition', timeout)
def wait_step(msg, timeout, sleep_time=0.1):
  timeout -= sleep_time
  if timeout <= 0:
    raise TestError('timeout waiting for condition "%s"' % msg)
  logging.debug('Sleeping for %f seconds waiting for condition "%s"',
                sleep_time, msg)
  time.sleep(sleep_time)
  return timeout


# vars helpers
def get_vars(port):
  """Returns the dict for vars from a vtxxx process. None if not available."""
  try:
    url = 'http://localhost:%d/debug/vars' % int(port)
    f = urllib2.urlopen(url)
    data = f.read()
    f.close()
  except urllib2.URLError:
    return None
  try:
    return json.loads(data)
  except ValueError:
    print data
    raise


def wait_for_vars(name, port, var=None, key=None, value=None, timeout=10.0):
  """Waits for the vars of a process, and optional values.

  Args:
    name: nickname for the process.
    port: process port to look at.
    var: if specified, waits for var in vars.
    key: if specified, waits for vars[var][key]==value.
    value: if key if specified, waits for vars[var][key]==value.
    timeout: how long to wait.
  """
  text = 'waiting for http://localhost:%d/debug/vars of %s' % (port, name)
  if var:
    text += ' value %s' % var
    if key:
      text += ' key %s:%s' % (key, value)
  while True:
    display_text = text
    v = get_vars(port)
    if v:
      if var is None:
        break
      if var in v:
        if key is None:
          break
        if key in v[var]:
          if v[var][key] == value:
            break
          else:
            display_text += ' (current value:%s)' % v[var][key]
        else:
          display_text += ' (no current value)'
      else:
        display_text += ' (%s not in vars)' % var
    else:
      display_text += ' (no vars yet)'
    timeout = wait_step(display_text, timeout)


def poll_for_vars(
    name, port, condition_msg, timeout=60.0, condition_fn=None,
    require_vars=False):
  """Polls for debug variables to exist or match specific conditions.

  This function polls in a tight loop, with no sleeps. This is useful for
  variables that are expected to be short-lived (e.g., a 'Done' state
  immediately before a process exits).

  Args:
    name: the name of the process that we're trying to poll vars from.
    port: the port number that we should poll for variables.
    condition_msg: string describing the conditions that we're polling for,
      used for error messaging.
    timeout: number of seconds that we should attempt to poll for.
    condition_fn: a function that takes the debug vars dict as input, and
      returns a truthy value if it matches the success conditions.
    require_vars: True iff we expect the vars to always exist. If
      True, and the vars don't exist, we'll raise a TestError. This
      can be used to differentiate between a timeout waiting for a
      particular condition vs if the process that you're polling has
      already exited.

  Raises:
    TestError: if the conditions aren't met within the given timeout, or
               if vars are required and don't exist.

  Returns:
    dict of debug variables

  """
  start_time = time.time()
  while True:
    if (time.time() - start_time) >= timeout:
      raise TestError(
          'Timed out polling for vars from %s; condition "%s" not met' %
          (name, condition_msg))
    v = get_vars(port)
    if v is None:
      if require_vars:
        raise TestError(
            'Expected vars to exist on %s, but they do not; '
            'process probably exited earlier than expected.' % (name,))
      continue
    if condition_fn is None:
      return v
    elif condition_fn(v):
      return v


def apply_vschema(vschema):
  for k, v in vschema.iteritems():
    fname = os.path.join(environment.tmproot, 'vschema.json')
    with open(fname, 'w') as f:
      f.write(v)
    run_vtctl(['ApplyVSchema', '-vschema_file', fname, k])


def wait_for_tablet_type(tablet_alias, expected_type, timeout=10):
  """Waits for a given tablet's SlaveType to become the expected value.

  Args:
    tablet_alias: Alias of the tablet.
    expected_type: Type of the tablet e.g. "replica".
    timeout: Timeout in seconds.

  Raises:
    TestError: SlaveType did not become expected_type within timeout seconds.
  """
  type_as_int = topodata_pb2.TabletType.Value(expected_type.upper())
  while True:
    if run_vtctl_json(['GetTablet', tablet_alias])['type'] == type_as_int:
      logging.debug('tablet %s went to expected type: %s',
                    tablet_alias, expected_type)
      break
    timeout = wait_step(
        "%s's SlaveType to be %s" % (tablet_alias, expected_type),
        timeout)


def wait_for_replication_pos(tablet_a, tablet_b, timeout=60.0):
  """Waits for tablet B to catch up to the replication position of tablet A.

  Args:
    tablet_a: tablet Object for tablet A.
    tablet_b: tablet Object for tablet B.
    timeout: Timeout in seconds.

  Raises:
    TestError: replication position did not catch up within timeout seconds.
  """
  replication_pos_a = mysql_flavor().master_position(tablet_a)
  while True:
    replication_pos_b = mysql_flavor().master_position(tablet_b)
    if mysql_flavor().position_at_least(replication_pos_b, replication_pos_a):
      break
    timeout = wait_step(
        "%s's replication position to catch up %s's; "
        'currently at: %s, waiting to catch up to: %s' % (
            tablet_b.tablet_alias, tablet_a.tablet_alias, replication_pos_b,
            replication_pos_a),
        timeout, sleep_time=0.1)

# Save the first running instance of vtgate. It is saved when 'start'
# is called, and cleared when kill is called.
vtgate = None


class VtGate(object):
  """VtGate object represents a vtgate process."""

  def __init__(self, port=None, mysql_server=False):
    """Creates the Vtgate instance and reserve the ports if necessary."""
    self.port = port or environment.reserve_ports(1)
    if protocols_flavor().vtgate_protocol() == 'grpc':
      self.grpc_port = environment.reserve_ports(1)
    self.proc = None
    self.mysql_port = None
    if mysql_server:
      self.mysql_port = environment.reserve_ports(1)

  def start(self, cell='test_nj', retry_count=2,
            topo_impl=None, cache_ttl='1s',
            extra_args=None, tablets=None,
            tablet_types_to_wait='MASTER,REPLICA',
            l2vtgates=None):
    """Start vtgate. Saves it into the global vtgate variable if not set yet."""

    args = environment.binary_args('vtgate') + [
        '-port', str(self.port),
        '-cell', cell,
        '-retry-count', str(retry_count),
        '-log_dir', environment.vtlogroot,
        '-srv_topo_cache_ttl', cache_ttl,
        '-srv_topo_cache_refresh', cache_ttl,
        '-tablet_protocol', protocols_flavor().tabletconn_protocol(),
        '-stderrthreshold', get_log_level(),
        '-normalize_queries',
        '-gateway_implementation', vtgate_gateway_flavor().flavor(),
    ]
    args.extend(vtgate_gateway_flavor().flags(cell=cell, tablets=tablets))
    if l2vtgates:
      args.extend(['-l2vtgate_addrs', ','.join(l2vtgates)])
    if tablet_types_to_wait:
      args.extend(['-tablet_types_to_wait', tablet_types_to_wait])

    if protocols_flavor().vtgate_protocol() == 'grpc':
      args.extend(['-grpc_port', str(self.grpc_port)])
      args.extend(['-grpc_max_message_size',
                   str(environment.grpc_max_message_size)])
    if protocols_flavor().service_map():
      args.extend(['-service_map', ','.join(protocols_flavor().service_map())])
    if topo_impl:
      args.extend(['-topo_implementation', topo_impl])
    else:
      args.extend(environment.topo_server().flags())
    if extra_args:
      args.extend(extra_args)
    if self.mysql_port:
      args.extend(['-mysql_server_port', str(self.mysql_port)])

    self.proc = run_bg(args)
    # We use a longer timeout here, as we may be waiting for the initial
    # state of a few tablets.
    wait_for_vars('vtgate', self.port, timeout=20.0)

    global vtgate
    if not vtgate:
      vtgate = self

  def kill(self):
    """Terminates the vtgate process, and waits for it to exit.

    If this process is the one saved in the global vtgate variable,
    clears it.

    Note if the test is using just one global vtgate process, and
    starting it with the test, and killing it at the end of the test,
    there is no need to call this kill() method,
    utils.kill_sub_processes() will do a good enough job.

    """
    if self.proc is None:
      return
    kill_sub_process(self.proc, soft=True)
    self.proc.wait()
    self.proc = None

    global vtgate
    if vtgate == self:
      vtgate = None

  def addr(self):
    """Returns the address of the vtgate process, for web access."""
    return 'localhost:%d' % self.port

  def rpc_endpoint(self, python=False):
    """Returns the protocol and endpoint to use for RPCs."""
    if python:
      protocol = protocols_flavor().vtgate_python_protocol()
    else:
      protocol = protocols_flavor().vtgate_protocol()
    if protocol == 'grpc':
      return protocol, 'localhost:%d' % self.grpc_port
    return protocol, self.addr()

  def get_status(self):
    """Returns the status page for this process."""
    return get_status(self.port)

  def get_vars(self):
    """Returns the vars for this process."""
    return get_vars(self.port)

  def get_vschema(self):
    """Returns the used vschema for this process."""
    return urllib2.urlopen('http://localhost:%d/debug/vschema' %
                           self.port).read()

  @contextlib.contextmanager
  def create_connection(self):
    """Connects to vtgate and allows to create a cursor to execute queries.

    This method is preferred over the two other methods ("vtclient", "execute")
    to execute a query in tests.

    Yields:
      A vtgate connection object.

    Example:
      with self.vtgate.create_connection() as conn:
        c = conn.cursor(keyspace=KEYSPACE, shards=[SHARD], tablet_type='master',
                        writable=self.writable)
        c.execute('SELECT * FROM buffer WHERE id = :id', {'id': 1})
    """
    protocol, endpoint = self.rpc_endpoint(python=True)
    # Use a very long timeout to account for slow tests.
    conn = vtgate_client.connect(protocol, endpoint, 600.0)
    yield conn
    conn.close()

  @contextlib.contextmanager
  def write_transaction(self, **kwargs):
    """Begins a write transaction and commits automatically.

    Note that each transaction contextmanager will create a new connection.

    Args:
      **kwargs: vtgate cursor args. See vtgate_cursor.VTGateCursor.

    Yields:
      A writable vtgate cursor.

    Example:
      with utils.vtgate.write_transaction(keyspace=KEYSPACE, shards=[SHARD],
                                          tablet_type='master') as tx:
        tx.execute('INSERT INTO table1 (id, msg) VALUES (:id, :msg)',
                   {'id': 1, 'msg': 'msg1'})
    """
    with self.create_connection() as conn:
      cursor = conn.cursor(writable=True, **kwargs)
      cursor.begin()
      yield cursor
      cursor.commit()

  def vtclient(self, sql, keyspace=None, tablet_type='master',
               bindvars=None, streaming=False,
               verbose=False, raise_on_error=True, json_output=False):
    """Uses the vtclient binary to send a query to vtgate."""
    protocol, addr = self.rpc_endpoint()
    args = environment.binary_args('vtclient') + [
        '-server', addr,
        '-vtgate_protocol', protocol]
    if json_output:
      args.append('-json')
    if bindvars:
      args.extend(['-bind_variables', json.dumps(bindvars)])
    if streaming:
      args.append('-streaming')
    if keyspace:
      args.extend(['-target', '%s@%s' % (keyspace, tablet_type)])
    else:
      args.extend(['-target', '@'+tablet_type])
    if verbose:
      args.append('-alsologtostderr')
    args.append(sql)

    out, err = run(args, raise_on_error=raise_on_error, trap_output=True)
    if json_output:
      return json.loads(out), err
    return out, err

  def execute(self, sql, tablet_type='master', bindvars=None,
              execute_options=None):
    """Uses 'vtctl VtGateExecute' to execute a command.

    Args:
      sql: the command to execute.
      tablet_type: the tablet_type to use.
      bindvars: a dict of bind variables.
      execute_options: proto-encoded ExecuteOptions object.

    Returns:
      the result of running vtctl command.
    """
    _, addr = self.rpc_endpoint()
    args = ['VtGateExecute', '-json',
            '-server', addr,
            '-target', '@'+tablet_type]
    if bindvars:
      args.extend(['-bind_variables', json.dumps(bindvars)])
    if execute_options:
      args.extend(['-options', execute_options])
    args.append(sql)
    return run_vtctl_json(args)

  def execute_shards(self, sql, keyspace, shards, tablet_type='master',
                     bindvars=None):
    """Uses 'vtctl VtGateExecuteShards' to execute a command."""
    _, addr = self.rpc_endpoint()
    args = ['VtGateExecuteShards', '-json',
            '-server', addr,
            '-keyspace', keyspace,
            '-shards', shards,
            '-tablet_type', tablet_type]
    if bindvars:
      args.extend(['-bind_variables', json.dumps(bindvars)])
    args.append(sql)
    return run_vtctl_json(args)

  def split_query(self, sql, keyspace, split_count, bindvars=None):
    """Uses 'vtctl VtGateSplitQuery' to cut a query up in chunks."""
    _, addr = self.rpc_endpoint()
    args = ['VtGateSplitQuery',
            '-server', addr,
            '-keyspace', keyspace,
            '-split_count', str(split_count)]
    if bindvars:
      args.extend(['-bind_variables', json.dumps(bindvars)])
    args.append(sql)
    return run_vtctl_json(args)

  def wait_for_endpoints(self, name, count, timeout=20.0, var=None):
    """waits until vtgate gets endpoints.

    Args:
      name: name of the endpoint, in the form: 'keyspace.shard.type'.
      count: how many endpoints to wait for.
      timeout: how long to wait.
      var: name of the variable to use. if None, defaults to the gateway's.
    """
    wait_for_vars('vtgate', self.port,
                  var=var or vtgate_gateway_flavor().connection_count_vars(),
                  key=name, value=count, timeout=timeout)

  def verify_no_endpoint(self, name):
    """verifies the vtgate doesn't have any enpoint of the given name.

    Args:
      name: name of the endpoint, in the form: 'keyspace.shard.type'.
    """
    def condition(v):
      return (v.get(vtgate_gateway_flavor().connection_count_vars())
              .get(name, None)) is None

    poll_for_vars('l2vtgate', self.port,
                  'no endpoint named ' + name,
                  timeout=5.0,
                  condition_fn=condition)


# vtctl helpers
# The modes are not all equivalent, and we don't really thrive for it.
# If a client needs to rely on vtctl's command line behavior, make
# sure to use mode=utils.VTCTL_VTCTL
VTCTL_AUTO = 0
VTCTL_VTCTL = 1
VTCTL_VTCTLCLIENT = 2
VTCTL_RPC = 3


def run_vtctl(clargs, auto_log=False, expect_fail=False,
              mode=VTCTL_AUTO, **kwargs):
  if mode == VTCTL_AUTO:
    if not expect_fail and vtctld:
      mode = VTCTL_RPC
    else:
      mode = VTCTL_VTCTL

  if mode == VTCTL_VTCTL:
    return run_vtctl_vtctl(clargs, auto_log=auto_log,
                           expect_fail=expect_fail, **kwargs)
  elif mode == VTCTL_VTCTLCLIENT:
    result = vtctld.vtctl_client(clargs)
    return result, ''
  elif mode == VTCTL_RPC:
    if auto_log:
      logging.debug('vtctl: %s', ' '.join(clargs))
    result = vtctl_client.execute_vtctl_command(vtctld_connection, clargs,
                                                info_to_debug=True,
                                                action_timeout=120)
    return result, ''

  raise Exception('Unknown mode: %s', mode)


def run_vtctl_vtctl(clargs, auto_log=False, expect_fail=False,
                    **kwargs):
  args = environment.binary_args('vtctl') + [
      '-log_dir', environment.vtlogroot,
      '-enable_queries',
  ]
  args.extend(environment.topo_server().flags())
  args.extend(['-tablet_manager_protocol',
               protocols_flavor().tablet_manager_protocol()])
  args.extend(['-tablet_protocol', protocols_flavor().tabletconn_protocol()])
  args.extend(['-throttler_client_protocol',
               protocols_flavor().throttler_client_protocol()])
  args.extend(['-vtgate_protocol', protocols_flavor().vtgate_protocol()])
  # TODO(b/26388813): Remove the next two lines once vtctl WaitForDrain is
  #                   integrated in the vtctl MigrateServed* commands.
  args.extend(['--wait_for_drain_sleep_rdonly', '0s'])
  args.extend(['--wait_for_drain_sleep_replica', '0s'])

  if auto_log:
    args.append('--stderrthreshold=%s' % get_log_level())

  if isinstance(clargs, str):
    cmd = ' '.join(args) + ' ' + clargs
  else:
    cmd = args + clargs

  if expect_fail:
    return run_fail(cmd, **kwargs)
  return run(cmd, **kwargs)


# run_vtctl_json runs the provided vtctl command and returns the result
# parsed as json
def run_vtctl_json(clargs, auto_log=True):
  stdout, _ = run_vtctl(clargs, trap_output=True, auto_log=auto_log)
  return json.loads(stdout)


def get_log_level():
  if options.verbose == 2:
    return '0'
  elif options.verbose == 1:
    return '1'
  else:
    return '2'


# vtworker helpers
def run_vtworker(clargs, auto_log=False, expect_fail=False, **kwargs):
  """Runs a vtworker process, returning the stdout and stderr."""
  cmd, _, _ = _get_vtworker_cmd(clargs, auto_log)
  if expect_fail:
    return run_fail(cmd, **kwargs)
  return run(cmd, **kwargs)


def run_vtworker_bg(clargs, auto_log=False, **kwargs):
  """Starts a background vtworker process."""
  cmd, port, rpc_port = _get_vtworker_cmd(clargs, auto_log)
  proc = run_bg(cmd, **kwargs), port, rpc_port
  wait_for_vars('vtworker', port)
  return proc


def _get_vtworker_cmd(clargs, auto_log=False):
  """Assembles the command that is needed to run a vtworker.

  Args:
    clargs: Command line arguments passed to vtworker.
    auto_log: If true, set --stderrthreshold according to the test log level.

  Returns:
    cmd - list of cmd arguments, can be passed to any `run`-like functions
    port - int with the port number that the vtworker is running with
    rpc_port - int with the port number of the RPC interface
  """
  port = environment.reserve_ports(1)
  rpc_port = port
  args = environment.binary_args('vtworker') + [
      '-log_dir', environment.vtlogroot,
      '-port', str(port),
      '-executefetch_retry_time', '1s',
      '-tablet_manager_protocol',
      protocols_flavor().tablet_manager_protocol(),
      '-tablet_protocol', protocols_flavor().tabletconn_protocol(),
  ]
  args.extend(environment.topo_server().flags())
  if protocols_flavor().service_map():
    args.extend(['-service_map',
                 ','.join(protocols_flavor().service_map())])
  if protocols_flavor().vtworker_client_protocol() == 'grpc':
    rpc_port = environment.reserve_ports(1)
    args.extend(['-grpc_port', str(rpc_port)])

  if auto_log:
    args.append('--stderrthreshold=%s' % get_log_level())

  cmd = args + clargs
  return cmd, port, rpc_port


# vtworker client helpers
def run_vtworker_client_bg(args, rpc_port):
  """Runs vtworkerclient to execute a command on a remote vtworker.

  Args:
    args: Full vtworker command.
    rpc_port: Port number.

  Returns:
    proc: process returned by subprocess.Popen
  """
  return run_bg(
      environment.binary_args('vtworkerclient') + [
          '-log_dir', environment.vtlogroot,
          '-vtworker_client_protocol',
          protocols_flavor().vtworker_client_protocol(),
          '-server', 'localhost:%d' % rpc_port,
          '-stderrthreshold', get_log_level(),
      ] + args)


def run_automation_server(auto_log=False):
  """Starts a background automation_server process.

  Args:
    auto_log: True to log.

  Returns:
    rpc_port - int with the port number of the RPC interface
  """
  rpc_port = environment.reserve_ports(1)
  args = environment.binary_args('automation_server') + [
      '-log_dir', environment.vtlogroot,
      '-port', str(rpc_port),
      '-vtctl_client_protocol',
      protocols_flavor().vtctl_client_protocol(),
      '-vtworker_client_protocol',
      protocols_flavor().vtworker_client_protocol(),
  ]
  if auto_log:
    args.append('--stderrthreshold=%s' % get_log_level())

  return run_bg(args), rpc_port


# mysql helpers
def mysql_query(uid, dbname, query):
  conn = MySQLdb.Connect(
      user='vt_dba',
      unix_socket='%s/vt_%010d/mysql.sock' % (environment.vtdataroot, uid),
      db=dbname)
  cursor = conn.cursor()
  cursor.execute(query)
  try:
    return cursor.fetchall()
  finally:
    conn.close()


def mysql_write_query(uid, dbname, query):
  conn = MySQLdb.Connect(
      user='vt_dba',
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
  conn = MySQLdb.Connect(
      user='vt_dba',
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
  for _ in xrange(3):
    try:
      check_db_read_only(uid)
      return
    except TestError as e:
      logging.warning('wait_db_read_only: %s', str(e))
      time.sleep(1.0)
  raise e


def check_srv_keyspace(cell, keyspace, expected, keyspace_id_type='uint64',
                       sharding_column_name='keyspace_id'):
  ks = run_vtctl_json(['GetSrvKeyspace', cell, keyspace])
  result = ''
  pmap = {}
  for partition in ks['partitions']:
    tablet_type = topodata_pb2.TabletType.Name(partition['served_type']).lower()
    if tablet_type == 'batch':
      tablet_type = 'rdonly'
    r = 'Partitions(%s):' % tablet_type
    for shard in partition['shard_references']:
      s = ''
      e = ''
      if 'key_range' in shard and shard['key_range']:
        if 'start' in shard['key_range']:
          s = shard['key_range']['start']
          s = base64.b64decode(s).encode('hex') if s else ''
        if 'end' in shard['key_range']:
          e = shard['key_range']['end']
          e = base64.b64decode(e).encode('hex') if e else ''
      r += ' %s-%s' % (s, e)
    pmap[tablet_type] = r + '\n'
  for tablet_type in sorted(pmap):
    result += pmap[tablet_type]
  logging.debug('Cell %s keyspace %s has data:\n%s', cell, keyspace, result)
  if expected != result:
    raise Exception(
        'Mismatch in srv keyspace for cell %s keyspace %s, expected:\n%'
        's\ngot:\n%s' % (
            cell, keyspace, expected, result))
  if sharding_column_name != ks.get('sharding_column_name'):
    raise Exception('Got wrong sharding_column_name in SrvKeyspace: %s' %
                    str(ks))
  if keyspace_id_type != keyrange_constants.PROTO3_KIT_TO_STRING[
      ks.get('sharding_column_type')]:
    raise Exception('Got wrong sharding_column_type in SrvKeyspace: %s' %
                    str(ks))


def check_shard_query_service(
    testcase, shard_name, tablet_type, expected_state):
  """Checks DisableQueryService in the shard record's TabletControlMap."""
  # We assume that query service should be enabled unless
  # DisableQueryService is explicitly True
  query_service_enabled = True
  tablet_controls = run_vtctl_json(
      ['GetShard', shard_name]).get('tablet_controls')
  if tablet_controls:
    for tc in tablet_controls:
      if tc['tablet_type'] == tablet_type:
        if tc.get('disable_query_service', False):
          query_service_enabled = False

  testcase.assertEqual(
      query_service_enabled,
      expected_state,
      'shard %s does not have the correct query service state: '
      'got %s but expected %s' %
      (shard_name, query_service_enabled, expected_state)
  )


def check_shard_query_services(
    testcase, shard_names, tablet_type, expected_state):
  for shard_name in shard_names:
    check_shard_query_service(
        testcase, shard_name, tablet_type, expected_state)


def check_tablet_query_service(
    testcase, tablet, serving, tablet_control_disabled):
  """Check that the query service is enabled or disabled on the tablet."""
  tablet_vars = get_vars(tablet.port)
  if serving:
    expected_state = 'SERVING'
  else:
    expected_state = 'NOT_SERVING'
  testcase.assertEqual(
      tablet_vars['TabletStateName'], expected_state,
      'tablet %s (%s/%s, %s) is not in the right serving state: got %s'
      ' expected %s' % (tablet.tablet_alias, tablet.keyspace, tablet.shard,
                        tablet.tablet_type,
                        tablet_vars['TabletStateName'], expected_state))

  status = tablet.get_status()
  tc_dqs = 'Query Service disabled: TabletControl.DisableQueryService set'
  if tablet_control_disabled:
    testcase.assertIn(tc_dqs, status)
  else:
    testcase.assertNotIn(tc_dqs, status)

  if tablet.tablet_type == 'rdonly':
    # Run RunHealthCheck to be sure the tablet doesn't change its serving state.
    run_vtctl(['RunHealthCheck', tablet.tablet_alias],
              auto_log=True)

    tablet_vars = get_vars(tablet.port)
    testcase.assertEqual(
        tablet_vars['TabletStateName'], expected_state,
        'tablet %s is not in the right serving state after health check: '
        'got %s expected %s' %
        (tablet.tablet_alias, tablet_vars['TabletStateName'], expected_state))


def check_tablet_query_services(
    testcase, tablets, serving, tablet_control_disabled):
  for tablet in tablets:
    check_tablet_query_service(
        testcase, tablet, serving, tablet_control_disabled)


def get_status(port):
  return urllib2.urlopen(
      'http://localhost:%d%s' % (port, environment.status_url)).read()


def curl(url, request=None, data=None, background=False, retry_timeout=0,
         **kwargs):
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
        retry_timeout = wait_step(
            'cmd: %s, error: %s' % (str(args), str(e)), retry_timeout)

  return run(args, trap_output=True, **kwargs)


class VtctldError(Exception):
  pass

# save the first running instance, and an RPC connection to it,
# so we can use it to run remote vtctl commands
vtctld = None
vtctld_connection = None


class Vtctld(object):

  def __init__(self):
    self.port = environment.reserve_ports(1)
    self.schema_change_dir = os.path.join(
        environment.tmproot, 'schema_change_test')
    if protocols_flavor().vtctl_client_protocol() == 'grpc':
      self.grpc_port = environment.reserve_ports(1)

  def start(self, enable_schema_change_dir=False, extra_flags=None):
    # Note the vtctld2 web dir is set to 'dist', which is populated
    # when a toplevel 'make build_web' is run. This is meant to test
    # the development version of the UI. The real checked-in app is in
    # app/.
    args = environment.binary_args('vtctld') + [
        '-enable_queries',
        '-cell', 'test_nj',
        '-web_dir', environment.vttop + '/web/vtctld',
        '-web_dir2', environment.vttop + '/web/vtctld2',
        '--log_dir', environment.vtlogroot,
        '--port', str(self.port),
        '-tablet_manager_protocol',
        protocols_flavor().tablet_manager_protocol(),
        '-tablet_protocol', protocols_flavor().tabletconn_protocol(),
        '-throttler_client_protocol',
        protocols_flavor().throttler_client_protocol(),
        '-vtgate_protocol', protocols_flavor().vtgate_protocol(),
        '-workflow_manager_init',
        '-workflow_manager_use_election',
        '-schema_swap_delay_between_errors', '1s',
        '-wait_for_drain_sleep_rdonly', '1s',
        '-wait_for_drain_sleep_replica', '1s',
    ] + environment.topo_server().flags()
    if extra_flags:
      args += extra_flags
    # TODO(b/26388813): Remove the next two lines once vtctl WaitForDrain is
    #                   integrated in the vtctl MigrateServed* commands.
    args.extend(['--wait_for_drain_sleep_rdonly', '0s'])
    args.extend(['--wait_for_drain_sleep_replica', '0s'])
    if enable_schema_change_dir:
      args += [
          '--schema_change_dir', self.schema_change_dir,
          '--schema_change_controller', 'local',
          '--schema_change_check_interval', '1',
      ]
    if protocols_flavor().service_map():
      args.extend(['-service_map', ','.join(protocols_flavor().service_map())])
    if protocols_flavor().vtctl_client_protocol() == 'grpc':
      args.extend(['-grpc_port', str(self.grpc_port)])
    stdout_fd = open(os.path.join(environment.tmproot, 'vtctld.stdout'), 'w')
    stderr_fd = open(os.path.join(environment.tmproot, 'vtctld.stderr'), 'w')
    self.proc = run_bg(args, stdout=stdout_fd, stderr=stderr_fd)

    # wait for the process to listen to RPC
    timeout = 30
    while True:
      v = get_vars(self.port)
      if v:
        break
      if self.proc.poll() is not None:
        raise TestError('vtctld died while starting')
      timeout = wait_step('waiting for vtctld to start', timeout,
                          sleep_time=0.2)

    # save the running instance so vtctl commands can be remote executed now
    global vtctld, vtctld_connection
    if not vtctld:
      vtctld = self
      protocol, endpoint = self.rpc_endpoint(python=True)
      vtctld_connection = vtctl_client.connect(protocol, endpoint, 30)

    return self.proc

  def rpc_endpoint(self, python=False):
    """RPC endpoint to vtctld.

    The RPC endpoint may differ from the webinterface URL e.g. because gRPC
    requires a dedicated port.

    Args:
      python: boolean, True iff this is for access with Python (as opposed to
              Go).

    Returns:
      protocol - string e.g. 'grpc'
      endpoint - string e.g. 'localhost:15001'
    """
    if python:
      protocol = protocols_flavor().vtctl_python_client_protocol()
    else:
      protocol = protocols_flavor().vtctl_client_protocol()
    rpc_port = self.port
    if protocol == 'grpc':
      rpc_port = self.grpc_port
    return (protocol, '%s:%d' % (socket.getfqdn(), rpc_port))

  def process_args(self):
    return ['-vtctld_addr', 'http://localhost:%d/' % self.port]

  def vtctl_client(self, args):
    if options.verbose == 2:
      log_level = 'INFO'
    elif options.verbose == 1:
      log_level = 'WARNING'
    else:
      log_level = 'ERROR'

    protocol, endpoint = self.rpc_endpoint()
    out, _ = run(
        environment.binary_args('vtctlclient') +
        ['-vtctl_client_protocol', protocol,
         '-server', endpoint,
         '-stderrthreshold', log_level] + args,
        trap_output=True)
    return out


def uint64_to_hex(integer):
  """Returns the hex representation of an int treated as a 64-bit unsigned int.

  The result is padded by zeros if necessary to fill a 16 character string.
  Useful for converting keyspace ids integers.

  Example:
  uint64_to_hex(1) == "0000000000000001"
  uint64_to_hex(0xDEADBEAF) == "00000000DEADBEEF"
  uint64_to_hex(0xDEADBEAFDEADBEAFDEADBEAF) raises an out of range exception.

  Args:
    integer: the value to print.

  Returns:
    String with the hex representation.

  Raises:
    ValueError: if the integer is out of range.
  """
  if integer > (1<<64)-1 or integer < 0:
    raise ValueError('Integer out of range: %d' % integer)
  return '%016X' % integer
