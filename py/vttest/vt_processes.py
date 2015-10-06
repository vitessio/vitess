# Copyright 2015 Google Inc. All Rights Reserved.

"""Starts the vtcombo or (vtgate and vtocc) processes."""

import json
import logging
import os
import socket
import subprocess
import time
import urllib

from vttest import environment
from vttest import fakezk_config


class ShardInfo(object):
  """Contains the description for setting up a test shard.

  Every shard should have a unique db_name, since they're all stored in a single
  MySQL instance for the purpose of this test.
  """

  def __init__(self, keyspace, shard_name, db_name):
    self.keyspace = keyspace
    self.name = shard_name
    self.db_name = db_name


class VtProcess(object):
  """Base class for a vt process, vtcombo, vtgate or vtocc."""

  START_RETRIES = 5

  def __init__(self, name, directory, binary, port_name, port_instance=0):
    self.name = name
    self.directory = directory
    self.binary = binary
    self.extraparams = []
    self.port_name = port_name
    self.port_instance = port_instance
    self.process = None

  def wait_start(self):
    """Start the process and wait for it to respond on HTTP."""

    for _ in xrange(0, self.START_RETRIES):
      self.port = environment.get_port(self.port_name,
                                       instance=self.port_instance)
      if environment.get_protocol() == 'grpc':
        self.grpc_port = environment.get_port(self.port_name,
                                              instance=self.port_instance,
                                              protocol='grpc')
      else:
        self.grpc_port = None
      logs_subdirectory = environment.get_logs_directory(self.directory)
      cmd = [
          self.binary,
          '-port', '%u' % self.port,
          '-log_dir', logs_subdirectory,
          ]
      if environment.get_protocol() == 'grpc':
        cmd.extend(['-grpc_port', '%u' % self.grpc_port])
      cmd.extend(self.extraparams)
      logging.info('Starting process: %s', cmd)
      stdout = os.path.join(logs_subdirectory, '%s.%d.log' %
                            (self.name, self.port))
      self.stdout = open(stdout, 'w')
      self.process = subprocess.Popen(cmd,
                                      stdout=self.stdout,
                                      stderr=subprocess.STDOUT)
      timeout = time.time() + 20.0
      while time.time() < timeout:
        if environment.process_is_healthy(
            self.name, self.addr()) and self.get_vars():
          logging.info('%s started.', self.name)
          return
        elif self.process.poll() is not None:
          logging.error('%s process exited prematurely.', self.name)
          break
        time.sleep(0.3)

      logging.error('cannot start %s process on time: %s ',
                    self.name, socket.getfqdn())
      self.kill()

    raise Exception('Failed %d times to run %s' % (
        self.START_RETRIES,
        self.name))

  def addr(self):
    """Return the host:port of the process."""
    return '%s:%u' % (socket.getfqdn(), self.port)

  def grpc_addr(self):
    """Return the grpc host:port of the process.

    Only call this is environment.get_protocol() == 'grpc'."""
    return '%s:%u' % (socket.getfqdn(), self.grpc_port)

  def get_vars(self):
    """Return the debug vars."""
    data = None
    try:
      url = 'http://%s/debug/vars' % self.addr()
      f = urllib.urlopen(url)
      data = f.read()
      f.close()
    except IOError:
      return None
    try:
      return json.loads(data)
    except ValueError:
      logging.error('%s' % data)
      raise

  def kill(self):
    """Kill the process."""
    # These will proceed without error even if the process is already gone.
    self.process.terminate()

  def wait(self):
    """Wait for the process to end."""
    self.process.wait()


class VtoccProcess(VtProcess):
  """Represents a vtocc subprocess."""

  def __init__(self, directory, mysql_db, port_instance, db_name,
               keyspace, shard, charset='utf8'):
    VtProcess.__init__(self, 'vtocc-%s-%s-%s' % (os.environ['USER'], keyspace,
                                                 shard),
                       directory, environment.vtocc_binary,
                       port_name='vtocc', port_instance=port_instance)
    self.extraparams = [
        '-db-config-app-dbname', db_name,
        '-db-config-app-keyspace', keyspace,
        '-db-config-app-shard', shard,
        '-db-config-app-charset', charset,
        '-db-config-app-host', mysql_db.hostname(),
        '-db-config-app-port', str(mysql_db.port()),
        '-db-config-app-uname', mysql_db.username(),
        '-db-config-app-pass', mysql_db.password(),
        '-db-config-app-unixsocket', mysql_db.unix_socket(),
        '-queryserver-config-transaction-timeout', '300',
        '-queryserver-config-schema-reload-time', '60',
    ] + environment.extra_vtocc_parameters()
    self.keyspace = keyspace
    self.shard = shard
    self.mysql_db = mysql_db

  def add_shard(self, config):
    """Add the shard of this process to the given config."""
    config.add_shard(self.keyspace, self.shard, self.port, self.grpc_port)

  def wait_for_state(self, state_to_wait='SERVING', timeout=60.0):
    if not state_to_wait:
      return
    while True:
      v = self.get_vars()
      if v is None:
        logging.debug(
            'vtocc %s not answering at /debug/vars, waiting...',
            self.name)
      elif 'TabletStateName' not in v:
        logging.debug(
            'vtocc %s not exporting TabletStateName, waiting...',
            self.name)
      else:
        s = v['TabletStateName']
        if s != state_to_wait:
          logging.debug('vtocc %s in state %s != %s',
                        self.name, s, state_to_wait)
        else:
          break
      timeout = wait_step('waiting for state %s' % state_to_wait,
                          timeout, sleep_time=0.1)


class VtgateProcess(VtProcess):
  """Represents a vtgate subprocess."""

  def __init__(self, directory, cell):
    VtProcess.__init__(self, 'vtgate-%s' % os.environ['USER'], directory,
                       environment.vtgate_binary, port_name='vtgate')
    self.config_file = os.path.join(directory, self.name + '.json')
    self.extraparams = [
        '-fakezk-config', self.config_file,
        '-topo_implementation', 'fakezk',
        '-cell', cell,
    ] + environment.extra_vtgate_parameters()


class AllVtoccProcesses(object):
  """Manage a list of all VtoccProcess objects."""

  def __init__(self, directory, shards, mysql_db, charset):
    self.vtoccs = []
    instance = 0
    for shard in shards:
      self.vtoccs.append(VtoccProcess(directory, mysql_db, instance,
                                      shard.db_name, shard.keyspace,
                                      shard.name, charset))
      instance += 1

  def wait_start(self):
    # we want to start the processes first,
    # and then wait for state update,
    # as the state change could take some time.
    for vtocc in self.vtoccs:
      vtocc.wait_start()
    for vtocc in self.vtoccs:
      vtocc.wait_for_state()

  def add_shards(self, config):
    for vtocc in self.vtoccs:
      vtocc.add_shard(config)

  def kill(self):
    for vtocc in self.vtoccs:
      vtocc.kill()

  def wait(self):
    for vtocc in self.vtoccs:
      vtocc.wait()


class VtcomboProcess(VtProcess):
  """Represents a vtcombo subprocess."""

  def __init__(self, directory, shards, mysql_db, charset):
    VtProcess.__init__(self, 'vtcombo-%s' % os.environ['USER'], directory,
                       environment.vtcombo_binary, port_name='vtcombo')
    topology = ",".join(["%s/%s:%s" % (shard.keyspace, shard.name, shard.db_name) for shard in shards])
    self.extraparams = [
        '-db-config-app-charset', charset,
        '-db-config-app-host', mysql_db.hostname(),
        '-db-config-app-port', str(mysql_db.port()),
        '-db-config-app-uname', mysql_db.username(),
        '-db-config-app-pass', mysql_db.password(),
        '-db-config-app-unixsocket', mysql_db.unix_socket(),
        '-queryserver-config-pool-size', '4',
        '-queryserver-config-transaction-cap', '4',
        '-queryserver-config-transaction-timeout', '300',
        '-queryserver-config-schema-reload-time', '60',
        '-queryserver-config-stream-pool-size', '4',
        '-topology', topology,
        '-mycnf_server_id', '1',
        '-mycnf_socket_file', mysql_db.unix_socket(),
    ] + environment.extra_vtcombo_parameters()


all_vtocc_processes = None
vtgate_process = None
vtcombo_process = None


def start_vt_processes(directory, shards, mysql_db,
                       cell='test_cell',
                       charset='utf8',
                       use_vtcombo=False):
  """Start the vt processes.

  Parameters:
    directory: the toplevel directory for the processes (logs, ...)
    shards: an array of ShardInfo objects.
    mysql_db: an instance of the mysql_db.MySqlDB class.
    cell: the cell name to use (unused if use_vtcombo).
    charset: the character set for the database connections.
    use_vtcombo: if set, launch a single vtcombo, instead of vtgate+vttablet.
  """
  global all_vtocc_processes
  global vtgate_process
  global vtcombo_process

  if use_vtcombo:
    # eventually, this will be the default
    logging.info('start_vtocc_processes(directory=%s,vtcombo_binary=%s)',
               directory, environment.vtcombo_binary)
    vtcombo_process = VtcomboProcess(directory, shards, mysql_db, charset)
    vtcombo_process.wait_start()
    return

  # display the binary paths
  logging.info('start_vtocc_processes(directory=%s,vtocc_binary=%s'
               ',vtgate_binary=%s)',
               directory, environment.vtocc_binary, environment.vtgate_binary)

  # first start the vtocc processes
  all_vtocc_processes = AllVtoccProcesses(directory, shards, mysql_db,
                                          charset)
  all_vtocc_processes.wait_start()

  # generate the fakezk config to use with vtgate
  config = fakezk_config.FakeZkConfig(mysql_port=mysql_db.port(), cell=cell)
  all_vtocc_processes.add_shards(config)
  json_data = config.as_json()
  logging.info('json config for vtgate: %s', json_data)

  # start a vtgate with that config file
  vtgate_process = VtgateProcess(directory, cell)
  with open(vtgate_process.config_file, 'w') as f:
    f.write(json_data)
  vtgate_process.wait_start()


def kill_vt_processes():
  """Call kill() on all processes."""
  if all_vtocc_processes:
    all_vtocc_processes.kill()
  if vtgate_process:
    vtgate_process.kill()
  if vtcombo_process:
    vtcombo_process.kill()


def wait_vt_processes():
  """Call wait() on all processes."""
  if all_vtocc_processes:
    all_vtocc_processes.wait()
  if vtgate_process:
    vtgate_process.wait()
  if vtcombo_process:
    vtcombo_process.wait()


def kill_and_wait_vt_processes():
  """Call kill() and then wait() on all processes."""
  kill_vt_processes()
  wait_vt_processes()


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
    raise Exception("timeout waiting for condition '%s'" % msg)
  logging.debug("Sleeping for %f seconds waiting for condition '%s'",
                sleep_time, msg)
  time.sleep(sleep_time)
  return timeout
