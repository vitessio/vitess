#!/usr/bin/python

import json
import logging
import os
import socket
import subprocess

# vttop is the toplevel of the vitess source tree
vttop = os.environ['VTTOP']

# vtroot is where everything gets installed
vtroot = os.environ['VTROOT']

# vtdataroot is where to put all the data files
vtdataroot = os.environ.get('VTDATAROOT', '/vt')

# vt_mysql_root is where MySQL is installed
vt_mysql_root = os.environ.get('VT_MYSQL_ROOT', os.path.join(vtroot, 'dist', 'mysql'))

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

# when an RPC times out, this message will be in the logs
rpc_timeout_message = 'Timeout waiting for'

def memcached_bin():
  in_vt = os.path.join(vtroot, 'bin', 'memcached')
  if os.path.exists(in_vt):
    return in_vt
  return 'memcached'

# url to hit to force the logs to flush.
flush_logs_url = '/debug/flushlogs'

def setup():
  global tmproot
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

# simple run command, cannot use utils.run to avoid circular dependencies
def run(args, raise_on_error=True, **kargs):
  try:
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
  if name in compiled_progs:
    return
  compiled_progs.append(name)
  logging.debug('Compiling %s', name)
  run(['go', 'install'], cwd=os.path.join(vttop, 'go', 'cmd', name))

# binary management: returns the full path for a binary
# this should typically not be used outside this file, unless you want to bypass
# global flag injection (see binary_args)
def binary_path(name):
  prog_compile(name)
  return os.path.join(vtroot, 'bin', name)

# returns flags specific to a given binary
# use this to globally inject flags any time a given command runs
# e.g. - if name == 'vtctl': return ['-extra_arg', 'value']
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

# topology server management: we use zookeeper in all the tests
topo_server_implementation = 'zookeeper'
hostname = socket.gethostname()
zk_port_base = reserve_ports(3)
zkocc_port_base = reserve_ports(3)
def topo_server_setup(add_bad_host=False):
  global zk_port_base
  global zkocc_port_base
  zk_ports = ":".join([str(zk_port_base), str(zk_port_base+1), str(zk_port_base+2)])
  run(binary_args('zkctl') + [
       '-log_dir', vtlogroot,
       '-zk.cfg', '1@%s:%s' % (hostname, zk_ports),
       'init'])
  config = tmproot+'/test-zk-client-conf.json'
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
  os.environ['ZK_CLIENT_CONFIG'] = config
  run(binary_args('zk') + ['touch', '-p', '/zk/test_nj/vt'])
  run(binary_args('zk') + ['touch', '-p', '/zk/test_ny/vt'])
  run(binary_args('zk') + ['touch', '-p', '/zk/test_ca/vt'])

def topo_server_teardown():
  global zk_port_base
  zk_ports = ":".join([str(zk_port_base), str(zk_port_base+1), str(zk_port_base+2)])
  run(binary_args('zkctl') + [
       '-log_dir', vtlogroot,
       '-zk.cfg', '1@%s:%s' % (hostname, zk_ports),
       'teardown'], raise_on_error=False)

def topo_server_wipe():
  # Work around safety check on recursive delete.
  run(binary_args('zk') + ['rm', '-rf', '/zk/test_nj/vt/*'])
  run(binary_args('zk') + ['rm', '-rf', '/zk/test_ny/vt/*'])
  run(binary_args('zk') + ['rm', '-rf', '/zk/global/vt/*'])

  run(binary_args('zk') + ['rm', '-f', '/zk/test_nj/vt'])
  run(binary_args('zk') + ['rm', '-f', '/zk/test_ny/vt'])
  run(binary_args('zk') + ['rm', '-f', '/zk/global/vt'])

def topo_server_flags():
  return ['-topo_implementation', 'zookeeper']

def tablet_manager_protocol_flags():
  return ['-tablet_manager_protocol', 'bson']

def tabletconn_protocol_flags():
  return ['-tablet_protocol', 'gorpc']

def vtctl_client_protocol():
  return 'gorpc'
