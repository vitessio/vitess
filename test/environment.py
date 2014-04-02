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

# tmproot is the temporary place to put all test files
tmproot = os.path.join(vtdataroot, 'tmp')

# vtlogroot is where to put all the log files
vtlogroot = tmproot

# where to start allocating ports from
vtportstart = int(os.environ.get('VTPORTSTART', '6700'))

# url in which binaries export their status.
status_url = '/debug/status'

# which flavor of mysql to use in tests
mysql_flavor = os.environ.get('MYSQL_FLAVOR', 'GoogleMysql')

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
  proc = subprocess.Popen(args,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          **kargs)
  stdout, stderr = proc.communicate()
  if proc.returncode:
    if raise_on_error:
      raise Exception('Command failed: ' + ' '.join(args) + ':\n' + stdout +
                      stderr)
    else:
      logging.error('Command failed: %s:\n%s%s', ' '.join(args), stdout, stderr)

# compile command line programs, only once
compiled_progs = []
def prog_compile(name):
  if name in compiled_progs:
    return
  compiled_progs.append(name)
  logging.debug('Compiling %s', name)
  run(['go', 'install'], cwd=os.path.join(vttop, 'go', 'cmd', name))

# binary management: returns the full path for a binary
def binary_path(name):
  prog_compile(name)
  return os.path.join(vtroot, 'bin', name)

# topology server management: we use zookeeper in all the tests
topo_server_implementation = 'zookeeper'
hostname = socket.gethostname()
zk_port_base = reserve_ports(3)
zkocc_port_base = reserve_ports(3)
def topo_server_setup(add_bad_host=False):
  global zk_port_base
  global zkocc_port_base
  zk_ports = ":".join([str(zk_port_base), str(zk_port_base+1), str(zk_port_base+2)])
  run([binary_path('zkctl'),
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
  os.putenv('ZK_CLIENT_CONFIG', config)
  run([binary_path('zk'), 'touch', '-p', '/zk/test_nj/vt'])
  run([binary_path('zk'), 'touch', '-p', '/zk/test_ny/vt'])
  run([binary_path('zk'), 'touch', '-p', '/zk/test_ca/vt'])

def topo_server_teardown():
  global zk_port_base
  zk_ports = ":".join([str(zk_port_base), str(zk_port_base+1), str(zk_port_base+2)])
  run([binary_path('zkctl'),
       '-log_dir', vtlogroot,
       '-zk.cfg', '1@%s:%s' % (hostname, zk_ports),
       'teardown'], raise_on_error=False)

def topo_server_wipe():
  # Work around safety check on recursive delete.
  run([binary_path('zk'), 'rm', '-rf', '/zk/test_nj/vt/*'])
  run([binary_path('zk'), 'rm', '-rf', '/zk/test_ny/vt/*'])
  run([binary_path('zk'), 'rm', '-rf', '/zk/global/vt/*'])

  run([binary_path('zk'), 'rm', '-f', '/zk/test_nj/vt'])
  run([binary_path('zk'), 'rm', '-f', '/zk/test_ny/vt'])
  run([binary_path('zk'), 'rm', '-f', '/zk/global/vt'])

def topo_server_flags():
  return ['-topo_implementation', 'zookeeper']

def tablet_manager_protocol_flags():
  return ['-tablet_manager_protocol', 'bson']

def tabletconn_protocol_flags():
  return ['-tablet_protocol', 'gorpc']
