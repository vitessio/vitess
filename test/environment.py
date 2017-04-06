#!/usr/bin/env python
"""Initialize the test environment."""

import logging
import os
import subprocess
import sys

import protocols_flavor

# Import the topo implementations that you want registered as options for the
# --topo-server-flavor flag.
# pylint: disable=unused-import
import topo_flavor.zookeeper
import topo_flavor.zk2
import topo_flavor.etcd
import topo_flavor.etcd2
import topo_flavor.consul

# This imports topo_server into this module, so clients can write
# environment.topo_server().
# pylint: disable=unused-import
from topo_flavor.server import topo_server

# Import the VTGate gateway flavors that you want registered as options for the
# --gateway_implementation flag.
# pylint: disable=unused-import
import vtgate_gateway_flavor.discoverygateway

from selenium import webdriver

from vttest import mysql_flavor


# sanity check the environment
if os.environ['USER'] == 'root':
  sys.stderr.write(
      'ERROR: Vitess and mysqld '
      'should not be run as root.\n')
  sys.exit(1)
if 'VTTOP' not in os.environ:
  sys.stderr.write(
      'ERROR: Vitess environment not set up. '
      'Please run "source dev.env" first.\n')
  sys.exit(1)

# vttop is the toplevel of the vitess source tree
vttop = os.environ['VTTOP']

# vtroot is where everything gets installed
vtroot = os.environ['VTROOT']

# vtdataroot is where to put all the data files
vtdataroot = os.environ.get('VTDATAROOT', '/vt')

# vt_mysql_root is where MySQL is installed
vt_mysql_root = os.environ.get(
    'VT_MYSQL_ROOT', os.path.join(vtroot, 'dist', 'mysql'))

# tmproot is the temporary place to put all test files
tmproot = os.path.join(vtdataroot, 'tmp')

# vtlogroot is where to put all the log files
vtlogroot = tmproot

# where to start allocating ports from
vtportstart = int(os.environ.get('VTPORTSTART', '6700'))

# url in which binaries export their status.
status_url = '/debug/status'

# location of the curl binary, used for some tests.
curl_bin = '/usr/bin/curl'

# if set, we will not build the binaries
skip_build = False

# location of the run_local_database.py file
run_local_database = os.path.join(vtroot, 'py-vtdb', 'vttest',
                                  'run_local_database.py')

# url to hit to force the logs to flush.
flush_logs_url = '/debug/flushlogs'


def setup():
  try:
    os.makedirs(tmproot)
  except OSError:
    # directory already exists
    pass


# port management: reserve count consecutive ports, returns the first one
def reserve_ports(count):
  global vtportstart
  result = vtportstart
  vtportstart += count
  return result


def run(args, raise_on_error=True, **kargs):
  """simple run command, cannot use utils.run to avoid circular dependencies.

  Args:
    args: Variable length argument list.
    raise_on_error: if exception should be raised when seeing error.
    **kargs: Arbitrary keyword arguments.

  Returns:
    None

  Raises:
    Exception: when it cannot start subprocess.
  """
  try:
    logging.debug(
        'run: %s %s', str(args),
        ', '.join('%s=%s' % x for x in kargs.iteritems()))
    proc = subprocess.Popen(args,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            **kargs)
    stdout, stderr = proc.communicate()
  except Exception as e:
    raise Exception('Command failed', e, args)

  if proc.returncode:
    if raise_on_error:
      raise Exception('Command failed: ' + ' '.join(args) + ':\n' + stdout +
                      stderr)
    else:
      logging.error('Command failed: %s:\n%s%s', ' '.join(args), stdout, stderr)
  return stdout, stderr


# compile command line programs, only once
compiled_progs = []


def prog_compile(name):
  if skip_build or name in compiled_progs:
    return
  compiled_progs.append(name)
  logging.debug('Compiling %s', name)
  run(['go', 'install'], cwd=os.path.join(vttop, 'go', 'cmd', name))


# binary management: returns the full path for a binary this should
# typically not be used outside this file, unless you want to bypass
# global flag injection (see binary_args)
def binary_path(name):
  prog_compile(name)
  return os.path.join(vtroot, 'bin', name)


# returns flags specific to a given binary
# use this to globally inject flags any time a given command runs
# e.g. - if name == 'vtctl': return ['-extra_arg', 'value']
# pylint: disable=unused-argument
def binary_flags(name):
  return []


# returns binary_path + binary_flags as a list
# this should be used instead of binary_path whenever possible
def binary_args(name):
  return [binary_path(name)] + binary_flags(name)


# returns binary_path + binary_flags as a string
# this should be used instead of binary_path whenever possible
def binary_argstr(name):
  return ' '.join(binary_args(name))


# binary management for the MySQL distribution.
def mysql_binary_path(name):
  return os.path.join(vt_mysql_root, 'bin', name)


def lameduck_flag(lameduck_period):
  return ['-lameduck-period', lameduck_period]


# pylint: disable=unused-argument
def add_options(parser):
  """Add environment-specific command-line options."""
  pass


def setup_protocol_flavor(flavor):
  """Imports the right protocols flavor implementation.

  This is a separate method that does dynamic import of the module so the
  tests only depend and import the code they will use.
  Each protocols flavor implementation will import the modules it needs.

  Args:
    flavor: the flavor name to use.
  """
  if flavor == 'grpc':
    import grpc_protocols_flavor  # pylint: disable=g-import-not-at-top
    protocols_flavor.set_protocols_flavor(
        grpc_protocols_flavor.GRpcProtocolsFlavor())

  else:
    logging.error('Unknown protocols flavor %s', flavor)
    exit(1)

  logging.debug('Using protocols flavor \'%s\'', flavor)


def reset_mysql_flavor():
  mysql_flavor.set_mysql_flavor(None)


def create_webdriver():
  """Creates a webdriver object (local or remote for Travis)."""
  if os.environ.get('CI') == 'true' and os.environ.get('TRAVIS') == 'true':
    username = os.environ['SAUCE_USERNAME']
    access_key = os.environ['SAUCE_ACCESS_KEY']
    capabilities = {}
    capabilities['tunnel-identifier'] = os.environ['TRAVIS_JOB_NUMBER']
    capabilities['build'] = os.environ['TRAVIS_BUILD_NUMBER']
    capabilities['platform'] = 'Linux'
    capabilities['browserName'] = 'chrome'
    hub_url = '%s:%s@localhost:4445' % (username, access_key)
    driver = webdriver.Remote(
        desired_capabilities=capabilities,
        command_executor='http://%s/wd/hub' % hub_url)
  else:
    os.environ['webdriver.chrome.driver'] = os.path.join(vtroot, 'dist')
    # Only testing against Chrome for now
    driver = webdriver.Chrome()
    driver.set_window_position(0, 0)
    driver.set_window_size(1280, 1024)
  return driver
