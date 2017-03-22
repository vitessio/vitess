"""Manage VTTablet during test."""

import json
import logging
import os
import re
import shutil
import sys
import time
import urllib2
import warnings

import MySQLdb

import environment
from mysql_flavor import mysql_flavor
from protocols_flavor import protocols_flavor
from topo_flavor.server import topo_server
import utils

# Dropping a table inexplicably produces a warning despite
# the 'IF EXISTS' clause. Squelch these warnings.
warnings.simplefilter('ignore')

tablet_cell_map = {
    62344: 'nj',
    62044: 'nj',
    41983: 'nj',
    31981: 'ny',
}


def get_backup_storage_flags():
  return ['-backup_storage_implementation', 'file',
          '-file_backup_storage_root',
          os.path.join(environment.tmproot, 'backupstorage')]


def get_all_extra_my_cnf(extra_my_cnf):
  all_extra_my_cnf = [environment.vttop + '/config/mycnf/default-fast.cnf']
  flavor_my_cnf = mysql_flavor().extra_my_cnf()
  if flavor_my_cnf:
    all_extra_my_cnf.append(flavor_my_cnf)
  if extra_my_cnf:
    all_extra_my_cnf.append(extra_my_cnf)
  return all_extra_my_cnf


class Tablet(object):
  """This class helps manage a vttablet instance.

  To use it for vttablet, you need to use init_tablet and/or
  start_vttablet.
  """
  default_uid = 62344
  seq = 0
  tablets_running = 0
  default_db_dba_config = {
      'dba': {
          'uname': 'vt_dba',
          'charset': 'utf8'
      },
  }
  default_db_config = {
      'app': {
          'uname': 'vt_app',
          'charset': 'utf8'
      },
      'allprivs': {
          'uname': 'vt_allprivs',
          'charset': 'utf8'
      },
      'dba': {
          'uname': 'vt_dba',
          'charset': 'utf8'
      },
      'filtered': {
          'uname': 'vt_filtered',
          'charset': 'utf8'
      },
      'repl': {
          'uname': 'vt_repl',
          'charset': 'utf8'
      }
  }

  def __init__(self, tablet_uid=None, port=None, mysql_port=None, cell=None,
               use_mysqlctld=False, vt_dba_passwd=None):
    self.tablet_uid = tablet_uid or (Tablet.default_uid + Tablet.seq)
    self.port = port or (environment.reserve_ports(1))
    self.mysql_port = mysql_port or (environment.reserve_ports(1))
    self.grpc_port = environment.reserve_ports(1)
    self.use_mysqlctld = use_mysqlctld
    self.vt_dba_passwd = vt_dba_passwd
    Tablet.seq += 1

    if cell:
      self.cell = cell
    else:
      self.cell = tablet_cell_map.get(tablet_uid, 'nj')
    self.proc = None

    # filled in during init_mysql
    self.mysqlctld_process = None

    # filled in during init_tablet
    self.keyspace = None
    self.shard = None
    self.index = None
    self.tablet_index = None

    # utility variables
    self.tablet_alias = 'test_%s-%010d' % (self.cell, self.tablet_uid)
    self.zk_tablet_path = (
        '/zk/test_%s/vt/tablets/%010d' % (self.cell, self.tablet_uid))

  def __str__(self):
    return 'tablet: uid: %d web: http://localhost:%d/ rpc port: %d' % (
        self.tablet_uid, self.port, self.grpc_port)

  def mysqlctl(self, cmd, extra_my_cnf=None, with_ports=False, verbose=False,
               extra_args=None):
    """Runs a mysqlctl command.

    Args:
      cmd: the command to run.
      extra_my_cnf: list of extra mycnf files to use
      with_ports: if set, sends the tablet and mysql ports to mysqlctl.
      verbose: passed to mysqlctld.
      extra_args: passed to mysqlctl.
    Returns:
      the result of run_bg.
    """
    extra_env = {}
    all_extra_my_cnf = get_all_extra_my_cnf(extra_my_cnf)
    if all_extra_my_cnf:
      extra_env['EXTRA_MY_CNF'] = ':'.join(all_extra_my_cnf)
    args = environment.binary_args('mysqlctl') + [
        '-log_dir', environment.vtlogroot,
        '-tablet_uid', str(self.tablet_uid)]
    if self.use_mysqlctld:
      args.extend(
          ['-mysqlctl_socket', os.path.join(self.tablet_dir, 'mysqlctl.sock')])
    if with_ports:
      args.extend(['-port', str(self.port),
                   '-mysql_port', str(self.mysql_port)])
    self._add_dbconfigs(self.default_db_dba_config, args)
    if verbose:
      args.append('-alsologtostderr')
    if extra_args:
      args.extend(extra_args)
    args.extend(cmd)
    return utils.run_bg(args, extra_env=extra_env)

  def mysqlctld(self, cmd, extra_my_cnf=None, verbose=False, extra_args=None):
    """Runs a mysqlctld command.

    Args:
      cmd: the command to run.
      extra_my_cnf: list of extra mycnf files to use
      verbose: passed to mysqlctld.
      extra_args: passed to mysqlctld.
    Returns:
      the result of run_bg.
    """
    extra_env = {}
    all_extra_my_cnf = get_all_extra_my_cnf(extra_my_cnf)
    if all_extra_my_cnf:
      extra_env['EXTRA_MY_CNF'] = ':'.join(all_extra_my_cnf)
    args = environment.binary_args('mysqlctld') + [
        '-log_dir', environment.vtlogroot,
        '-tablet_uid', str(self.tablet_uid),
        '-mysql_port', str(self.mysql_port),
        '-socket_file', os.path.join(self.tablet_dir, 'mysqlctl.sock')]
    self._add_dbconfigs(self.default_db_dba_config, args)
    if verbose:
      args.append('-alsologtostderr')
    if extra_args:
      args.extend(extra_args)
    args.extend(cmd)
    return utils.run_bg(args, extra_env=extra_env)

  def init_mysql(self, extra_my_cnf=None, init_db=None, extra_args=None,
                 use_rbr=False):
    """Init the mysql tablet directory, starts mysqld.

    Either runs 'mysqlctl init', or starts a mysqlctld process.

    Args:
      extra_my_cnf: to pass to mysqlctl.
      init_db: if set, use this init_db script instead of the default.
      extra_args: passed to mysqlctld / mysqlctl.
      use_rbr: configure the MySQL daemon to use RBR.

    Returns:
      The forked process.
    """
    if use_rbr:
      if extra_my_cnf:
        extra_my_cnf += ':' + environment.vttop + '/config/mycnf/rbr.cnf'
      else:
        extra_my_cnf = environment.vttop + '/config/mycnf/rbr.cnf'

    if not init_db:
      init_db = environment.vttop + '/config/init_db.sql'

    if self.use_mysqlctld:
      self.mysqlctld_process = self.mysqlctld(['-init_db_sql_file', init_db],
                                              extra_my_cnf=extra_my_cnf,
                                              extra_args=extra_args)
      return self.mysqlctld_process
    else:
      return self.mysqlctl(['init', '-init_db_sql_file', init_db],
                           extra_my_cnf=extra_my_cnf, with_ports=True,
                           extra_args=extra_args)

  def start_mysql(self):
    return self.mysqlctl(['start'], with_ports=True)

  def shutdown_mysql(self, extra_args=None):
    return self.mysqlctl(['shutdown'], with_ports=True, extra_args=extra_args)

  def teardown_mysql(self, extra_args=None):
    if self.use_mysqlctld and self.mysqlctld_process:
      # if we use mysqlctld, we just terminate it gracefully, so it kills
      # its mysqld. And we return it, so we can wait for it.
      utils.kill_sub_process(self.mysqlctld_process, soft=True)
      return self.mysqlctld_process
    if utils.options.keep_logs:
      return self.shutdown_mysql(extra_args=extra_args)
    return self.mysqlctl(['teardown', '-force'], extra_args=extra_args)

  def remove_tree(self, ignore_options=False):
    if not ignore_options and utils.options.keep_logs:
      return
    try:
      shutil.rmtree(self.tablet_dir)
    except OSError as e:
      if utils.options.verbose == 2:
        print >> sys.stderr, e, self.tablet_dir

  def mysql_connection_parameters(self, dbname, user='vt_dba'):
    result = dict(user=user,
                  unix_socket=self.tablet_dir + '/mysql.sock',
                  db=dbname)
    if user == 'vt_dba' and self.vt_dba_passwd:
      result['passwd'] = self.vt_dba_passwd
    return result

  def connect(self, dbname='', user='vt_dba', **params):
    params.update(self.mysql_connection_parameters(dbname, user))
    conn = MySQLdb.Connect(**params)
    return conn, conn.cursor()

  # Query the MySQL instance directly
  def mquery(self, dbname, query, write=False, user='vt_dba', conn_params=None,
             log_query=False):
    """Runs a query to MySQL directly, using python's SQL driver.

    Args:
      dbname: if set, the dbname to use.
      query: the SQL query (or queries) to run.
      write: if set, wraps the query in a transaction.
      user: the db user to use.
      conn_params: extra mysql connection parameters.
      log_query: if true, the query will be logged.
    Returns:
      the rows.
    """
    if conn_params is None:
      conn_params = {}
    conn, cursor = self.connect(dbname, user=user, **conn_params)
    if write:
      conn.begin()
    if isinstance(query, basestring):
      query = [query]

    for q in query:
      if log_query:
        logging.debug('mysql(%s,%s): %s', self.tablet_uid, dbname, q)
      cursor.execute(q)

    if write:
      conn.commit()

    try:
      return cursor.fetchall()
    finally:
      cursor.close()
      conn.close()

  def assert_table_count(self, dbname, table, n, where=''):
    result = self.mquery(dbname, 'select count(*) from ' + table + ' ' + where)
    if result[0][0] != n:
      raise utils.TestError('expected %d rows in %s' % (n, table), result)

  def reset_replication(self):
    self.mquery('', mysql_flavor().reset_replication_commands())

  def set_semi_sync_enabled(self, master=None, slave=None):
    logging.debug('mysql(%s): setting semi-sync mode: master=%s, slave=%s',
                  self.tablet_uid, master, slave)
    self.mquery('',
                mysql_flavor().set_semi_sync_enabled_commands(master, slave))

  def populate(self, dbname, create_sql, insert_sqls=None):
    self.create_db(dbname)
    if isinstance(create_sql, basestring):
      create_sql = [create_sql]
    for q in create_sql:
      self.mquery(dbname, q)
    if insert_sqls:
      for q in insert_sqls:
        self.mquery(dbname, q, write=True)

  def has_db(self, name):
    rows = self.mquery('', 'show databases')
    for row in rows:
      dbname = row[0]
      if dbname == name:
        return True
    return False

  def drop_db(self, name):
    self.mquery('', 'drop database if exists %s' % name)
    while self.has_db(name):
      logging.debug('%s sleeping while waiting for database drop: %s',
                    self.tablet_alias, name)
      time.sleep(0.3)
      self.mquery('', 'drop database if exists %s' % name)

  def create_db(self, name):
    self.drop_db(name)
    self.mquery('', 'create database %s' % name)

  def clean_dbs(self, include_vt=False):
    logging.debug('mysql(%s): removing all databases', self.tablet_uid)
    rows = self.mquery('', 'show databases')
    for row in rows:
      dbname = row[0]
      if dbname in ['information_schema', 'performance_schema', 'mysql', 'sys']:
        continue
      if dbname == '_vt' and not include_vt:
        continue
      self.drop_db(dbname)

  def wait_check_db_var(self, name, value):
    for _ in range(3):
      try:
        return self.check_db_var(name, value)
      except utils.TestError as e:
        print >> sys.stderr, 'WARNING: ', e
      time.sleep(1.0)
    raise e

  def check_db_var(self, name, value):
    row = self.get_db_var(name)
    if row != (name, value):
      raise utils.TestError('variable not set correctly', name, row)

  def get_db_var(self, name):
    conn, cursor = self.connect()
    try:
      cursor.execute("show variables like '%s'" % name)
      return cursor.fetchone()
    finally:
      conn.close()

  def check_db_status(self, name, value):
    row = self.get_db_status(name)
    if row[1] != value:
      raise utils.TestError('status not correct', name, row)

  def get_db_status(self, name):
    conn, cursor = self.connect()
    try:
      cursor.execute("show status like '%s'" % name)
      return cursor.fetchone()
    finally:
      conn.close()

  def update_addrs(self):
    args = [
        'UpdateTabletAddrs',
        '-hostname', 'localhost',
        '-ip-addr', '127.0.0.1',
        '-mysql-port', '%d' % self.mysql_port,
        '-vt-port', '%d' % self.port,
        self.tablet_alias
    ]
    return utils.run_vtctl(args)

  def init_tablet(self, tablet_type, keyspace, shard,
                  tablet_index=None,
                  start=False, dbname=None, parent=True, wait_for_start=True,
                  include_mysql_port=True, **kwargs):
    """Initialize a tablet's record in topology."""

    self.tablet_type = tablet_type
    self.keyspace = keyspace
    self.shard = shard
    self.tablet_index = tablet_index

    self.dbname = dbname or ('vt_' + self.keyspace)

    args = ['InitTablet',
            '-hostname', 'localhost',
            '-port', str(self.port),
            '-allow_update']
    if include_mysql_port:
      args.extend(['-mysql_port', str(self.mysql_port)])
    if parent:
      args.append('-parent')
    if dbname:
      args.extend(['-db_name_override', dbname])
    if keyspace:
      args.extend(['-keyspace', keyspace])
    if shard:
      args.extend(['-shard', shard])
    args.extend([self.tablet_alias, tablet_type])
    utils.run_vtctl(args)
    if start:
      if not wait_for_start:
        expected_state = None
      elif tablet_type == 'master':
        expected_state = 'SERVING'
      else:
        expected_state = 'NOT_SERVING'
      self.start_vttablet(wait_for_state=expected_state, **kwargs)

  @property
  def tablet_dir(self):
    return '%s/vt_%010d' % (environment.vtdataroot, self.tablet_uid)

  def grpc_enabled(self):
    return (
        protocols_flavor().tabletconn_protocol() == 'grpc' or
        protocols_flavor().tablet_manager_protocol() == 'grpc' or
        protocols_flavor().binlog_player_protocol() == 'grpc')

  def semi_sync_enabled(self):
    return self.enable_semi_sync

  def flush(self):
    utils.curl('http://localhost:%s%s' %
               (self.port, environment.flush_logs_url),
               stderr=utils.devnull, stdout=utils.devnull)

  def start_vttablet(
      self, port=None,
      wait_for_state='SERVING', filecustomrules=None, zkcustomrules=None,
      schema_override=None,
      repl_extra_flags=None, table_acl_config=None,
      lameduck_period=None, security_policy=None,
      full_mycnf_args=False,
      extra_args=None, extra_env=None, include_mysql_port=True,
      init_tablet_type=None, init_keyspace=None, init_shard=None,
      # TODO(mberlin): Assign the index automatically and remove this parameter.
      tablet_index=None,
      init_db_name_override=None,
      supports_backups=True, grace_period='1s', enable_semi_sync=True):
    # pylint: disable=g-doc-args
    """Starts a vttablet process, and returns it.

    The process is also saved in self.proc, so it's easy to kill as well.

    Returns:
      the process that was started.
    """
    # pylint: enable=g-doc-args

    args = environment.binary_args('vttablet')
    # Use 'localhost' as hostname because Travis CI worker hostnames
    # are too long for MySQL replication.
    args.extend(['-tablet_hostname', 'localhost'])
    args.extend(['-tablet-path', self.tablet_alias])
    args.extend(environment.topo_server().flags())
    args.extend(['-binlog_player_protocol',
                 protocols_flavor().binlog_player_protocol()])
    args.extend(['-tablet_manager_protocol',
                 protocols_flavor().tablet_manager_protocol()])
    args.extend(['-tablet_protocol', protocols_flavor().tabletconn_protocol()])
    args.extend(['-binlog_player_healthcheck_topology_refresh', '1s'])
    args.extend(['-binlog_player_healthcheck_retry_delay', '1s'])
    args.extend(['-binlog_player_retry_delay', '1s'])
    args.extend(['-pid_file', os.path.join(self.tablet_dir, 'vttablet.pid')])
    # always enable_replication_reporter with somewhat short values for tests
    args.extend(['-health_check_interval', '2s'])
    args.extend(['-enable_replication_reporter'])
    args.extend(['-degraded_threshold', '5s'])
    args.extend(['-watch_replication_stream'])
    if enable_semi_sync:
      args.append('-enable_semi_sync')
    # Remember the setting in case a test wants to know it.
    self.enable_semi_sync = enable_semi_sync
    if self.use_mysqlctld:
      args.extend(
          ['-mysqlctl_socket', os.path.join(self.tablet_dir, 'mysqlctl.sock')])

    if full_mycnf_args:
      # this flag is used to specify all the mycnf_ flags, to make
      # sure that code works.
      relay_log_path = os.path.join(self.tablet_dir, 'relay-logs',
                                    'vt-%010d-relay-bin' % self.tablet_uid)
      args.extend([
          '-mycnf_server_id', str(self.tablet_uid),
          '-mycnf_data_dir', os.path.join(self.tablet_dir, 'data'),
          '-mycnf_innodb_data_home_dir', os.path.join(self.tablet_dir,
                                                      'innodb', 'data'),
          '-mycnf_innodb_log_group_home_dir', os.path.join(self.tablet_dir,
                                                           'innodb', 'logs'),
          '-mycnf_socket_file', os.path.join(self.tablet_dir, 'mysql.sock'),
          '-mycnf_error_log_path', os.path.join(self.tablet_dir, 'error.log'),
          '-mycnf_slow_log_path', os.path.join(self.tablet_dir,
                                               'slow-query.log'),
          '-mycnf_relay_log_path', relay_log_path,
          '-mycnf_relay_log_index_path', relay_log_path + '.index',
          '-mycnf_relay_log_info_path', os.path.join(self.tablet_dir,
                                                     'relay-logs',
                                                     'relay-log.info'),
          '-mycnf_bin_log_path', os.path.join(
              self.tablet_dir, 'bin-logs', 'vt-%010d-bin' % self.tablet_uid),
          '-mycnf_master_info_file', os.path.join(self.tablet_dir,
                                                  'master.info'),
          '-mycnf_pid_file', os.path.join(self.tablet_dir, 'mysql.pid'),
          '-mycnf_tmp_dir', os.path.join(self.tablet_dir, 'tmp'),
          '-mycnf_slave_load_tmp_dir', os.path.join(self.tablet_dir, 'tmp'),
      ])
      if include_mysql_port:
        args.extend(['-mycnf_mysql_port', str(self.mysql_port)])

    # this is used to run InitTablet as part of the vttablet startup
    if init_tablet_type:
      self.tablet_type = init_tablet_type
      args.extend(['-init_tablet_type', init_tablet_type])
    if init_keyspace:
      self.keyspace = init_keyspace
      self.shard = init_shard
      # tablet_index is required for the update_addr call below.
      if self.tablet_index is None:
        self.tablet_index = tablet_index
      args.extend(['-init_keyspace', init_keyspace,
                   '-init_shard', init_shard])
      if init_db_name_override:
        self.dbname = init_db_name_override
        args.extend(['-init_db_name_override', init_db_name_override])
      else:
        self.dbname = 'vt_' + init_keyspace

    if supports_backups:
      args.extend(['-restore_from_backup'] + get_backup_storage_flags())

      # When vttablet restores from backup, and if not using
      # mysqlctld, it will re-generate the .cnf file.  So we need to
      # have EXTRA_MY_CNF set properly.
      if not self.use_mysqlctld:
        all_extra_my_cnf = get_all_extra_my_cnf(None)
        if all_extra_my_cnf:
          if not extra_env:
            extra_env = {}
          extra_env['EXTRA_MY_CNF'] = ':'.join(all_extra_my_cnf)

    if extra_args:
      args.extend(extra_args)

    args.extend(['-port', '%s' % (port or self.port),
                 '-log_dir', environment.vtlogroot])

    self._add_dbconfigs(self.default_db_config, args, repl_extra_flags)

    if filecustomrules:
      args.extend(['-filecustomrules', filecustomrules])
    if zkcustomrules:
      args.extend(['-zkcustomrules', zkcustomrules])

    if schema_override:
      args.extend(['-schema-override', schema_override])

    if table_acl_config:
      args.extend(['-table-acl-config', table_acl_config])
      args.extend(['-queryserver-config-strict-table-acl'])

    if protocols_flavor().service_map():
      args.extend(['-service_map', ','.join(protocols_flavor().service_map())])
    if self.grpc_enabled():
      args.extend(['-grpc_port', str(self.grpc_port)])
    if lameduck_period:
      args.extend(environment.lameduck_flag(lameduck_period))
    if grace_period:
      args.extend(['-serving_state_grace_period', grace_period])
    if security_policy:
      args.extend(['-security_policy', security_policy])

    args.extend(['-enable-autocommit'])
    stderr_fd = open(
        os.path.join(environment.vtlogroot, 'vttablet-%d.stderr' %
                     self.tablet_uid), 'w')
    # increment count only the first time
    if not self.proc:
      Tablet.tablets_running += 1
    self.proc = utils.run_bg(args, stderr=stderr_fd, extra_env=extra_env)

    log_message = (
        'Started vttablet: %s (%s) with pid: %s - Log files: '
        '%s/vttablet.*.{INFO,WARNING,ERROR,FATAL}.*.%s' %
        (self.tablet_uid, self.tablet_alias, self.proc.pid,
         environment.vtlogroot, self.proc.pid))
    # This may race with the stderr output from the process (though
    # that's usually empty).
    stderr_fd.write(log_message + '\n')
    stderr_fd.close()
    logging.debug(log_message)

    # wait for query service to be in the right state
    if wait_for_state:
      self.wait_for_vttablet_state(wait_for_state, port=port)

    if self.tablet_index is not None:
      topo_server().update_addr(
          'test_'+self.cell, self.keyspace, self.shard,
          self.tablet_index, (port or self.port))

    return self.proc

  def wait_for_vttablet_state(self, expected, timeout=60.0, port=None):
    expr = re.compile('^' + expected + '$')
    while True:
      v = utils.get_vars(port or self.port)
      last_seen_state = '?'
      if v is None:
        if self.proc.poll() is not None:
          raise utils.TestError(
              'vttablet died while test waiting for state %s' % expected)
        logging.debug(
            '  vttablet %s not answering at /debug/vars, waiting...',
            self.tablet_alias)
      else:
        if 'TabletStateName' not in v:
          logging.debug(
              '  vttablet %s not exporting TabletStateName, waiting...',
              self.tablet_alias)
        else:
          s = v['TabletStateName']
          last_seen_state = s
          if expr.match(s):
            break
          else:
            logging.debug(
                '  vttablet %s in state %s != %s', self.tablet_alias, s,
                expected)
      timeout = utils.wait_step(
          'waiting for %s state %s (last seen state: %s)' %
          (self.tablet_alias, expected, last_seen_state),
          timeout, sleep_time=0.1)

  def wait_for_mysqlctl_socket(self, timeout=60.0):
    mysql_sock = os.path.join(self.tablet_dir, 'mysql.sock')
    mysqlctl_sock = os.path.join(self.tablet_dir, 'mysqlctl.sock')
    while True:
      wait_for = []
      if not os.path.exists(mysql_sock):
        wait_for.append(mysql_sock)
      if not os.path.exists(mysqlctl_sock):
        wait_for.append(mysqlctl_sock)
      if not wait_for:
        return
      timeout = utils.wait_step('waiting for socket files: %s' % str(wait_for),
                                timeout, sleep_time=2.0)

  def _add_dbconfigs(self, cfg, args, repl_extra_flags=None):
    """Helper method to generate and add --db-config-* flags to 'args'."""
    if repl_extra_flags is None:
      repl_extra_flags = {}
    config = dict(cfg)
    if self.keyspace:
      if 'app' in config:
        config['app']['dbname'] = self.dbname
      if 'repl' in config:
        config['repl']['dbname'] = self.dbname
    if 'repl' in config:
      config['repl'].update(repl_extra_flags)
    for key1 in config:
      for key2 in config[key1]:
        args.extend(['-db-config-' + key1 + '-' + key2, config[key1][key2]])

  def get_status(self):
    return utils.get_status(self.port)

  def get_healthz(self):
    return urllib2.urlopen('http://localhost:%d/healthz' % self.port).read()

  def kill_vttablet(self, wait=True):
    """Sends a SIG_TERM to the tablet.

    Args:
      wait: will wait for the process to exit.
    Returns:
      the subprocess object (use it to get exit code).
    """
    logging.debug('killing vttablet: %s, wait: %s', self.tablet_alias,
                  str(wait))
    proc = self.proc
    if proc is not None:
      Tablet.tablets_running -= 1
      if proc.poll() is None:
        proc.terminate()
        if wait:
          proc.wait()
      self.proc = None
    return proc

  def hard_kill_vttablet(self):
    logging.debug('hard killing vttablet: %s', self.tablet_alias)
    if self.proc is not None:
      Tablet.tablets_running -= 1
      if self.proc.poll() is None:
        self.proc.kill()
        self.proc.wait()
      self.proc = None

  def wait_for_binlog_server_state(self, expected, timeout=30.0):
    """Wait for the tablet's binlog server to be in the provided state.

    Args:
      expected: the state to wait for.
      timeout: how long to wait before error.
    """
    while True:
      v = utils.get_vars(self.port)
      if v is None:
        if self.proc.poll() is not None:
          raise utils.TestError(
              'vttablet died while test waiting for binlog state %s' %
              expected)
        logging.debug('  vttablet not answering at /debug/vars, waiting...')
      else:
        if 'UpdateStreamState' not in v:
          logging.debug(
              '  vttablet not exporting BinlogServerState, waiting...')
        else:
          s = v['UpdateStreamState']
          if s != expected:
            logging.debug("  vttablet's binlog server in state %s != %s", s,
                          expected)
          else:
            break
      timeout = utils.wait_step(
          'waiting for binlog server state %s' % expected,
          timeout, sleep_time=0.5)
    logging.debug('tablet %s binlog service is in state %s',
                  self.tablet_alias, expected)

  def wait_for_binlog_player_count(self, expected, timeout=30.0):
    """Wait for a tablet to have binlog players.

    Args:
      expected: number of expected binlog players to wait for.
      timeout: how long to wait.
    """
    while True:
      v = utils.get_vars(self.port)
      if v is None:
        if self.proc.poll() is not None:
          raise utils.TestError(
              'vttablet died while test waiting for binlog count %s' %
              expected)
        logging.debug('  vttablet not answering at /debug/vars, waiting...')
      else:
        if 'BinlogPlayerMapSize' not in v:
          logging.debug(
              '  vttablet not exporting BinlogPlayerMapSize, waiting...')
        else:
          s = v['BinlogPlayerMapSize']
          if s != expected:
            logging.debug("  vttablet's binlog player map has count %d != %d",
                          s, expected)
          else:
            break
      timeout = utils.wait_step(
          'waiting for binlog player count %d' % expected,
          timeout, sleep_time=0.5)
    logging.debug('tablet %s binlog player has %d players',
                  self.tablet_alias, expected)

  @classmethod
  def check_vttablet_count(cls):
    if Tablet.tablets_running > 0:
      raise utils.TestError('This test is not killing all its vttablets')

  def execute(self, sql, bindvars=None, transaction_id=None,
              execute_options=None, auto_log=True):
    """execute uses 'vtctl VtTabletExecute' to execute a command.

    Args:
      sql: the command to execute.
      bindvars: a dict of bind variables.
      transaction_id: the id of the transaction to use if necessary.
      execute_options: proto-encoded ExecuteOptions object.
      auto_log: passed to run_vtctl.

    Returns:
      the result of running vtctl command.
    """
    args = [
        'VtTabletExecute', '-json',
    ]
    if bindvars:
      args.extend(['-bind_variables', json.dumps(bindvars)])
    if transaction_id:
      args.extend(['-transaction_id', str(transaction_id)])
    if execute_options:
      args.extend(['-options', execute_options])
    args.extend([self.tablet_alias, sql])
    return utils.run_vtctl_json(args, auto_log=auto_log)

  def begin(self, auto_log=True):
    """begin uses 'vtctl VtTabletBegin' to start a transaction.

    Args:
      auto_log: passed to run_vtctl.

    Returns:
      the transaction id.
    """
    args = [
        'VtTabletBegin',
        self.tablet_alias,
    ]
    result = utils.run_vtctl_json(args, auto_log=auto_log)
    return result['transaction_id']

  def commit(self, transaction_id, auto_log=True):
    """commit uses 'vtctl VtTabletCommit' to commit a transaction.

    Args:
      transaction_id: id of the transaction to roll back.
      auto_log: passed to run_vtctl.

    Returns:
      the return code for run_vtctl.
    """
    args = [
        'VtTabletCommit',
        self.tablet_alias,
        str(transaction_id),
    ]
    return utils.run_vtctl(args, auto_log=auto_log)

  def rollback(self, transaction_id, auto_log=True):
    """rollback uses 'vtctl VtTabletRollback' to rollback a transaction.

    Args:
      transaction_id: id of the transaction to roll back.
      auto_log: passed to run_vtctl.

    Returns:
      the return code for run_vtctl.
    """
    args = [
        'VtTabletRollback',
        self.tablet_alias,
        str(transaction_id),
    ]
    return utils.run_vtctl(args, auto_log=auto_log)

  def rpc_endpoint(self):
    """Returns the protocol and endpoint to use for RPCs."""
    if self.grpc_enabled():
      return 'localhost:%d' % self.grpc_port
    return 'localhost:%d' % self.port


def kill_tablets(tablets):
  for t in tablets:
    logging.debug('killing vttablet: %s', t.tablet_alias)
    if t.proc is not None:
      Tablet.tablets_running -= 1
      t.proc.terminate()

  for t in tablets:
    if t.proc is not None:
      t.proc.wait()
      t.proc = None
