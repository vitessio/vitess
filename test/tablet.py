import json
import logging
import os
import shutil
import sys
import time
import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter('ignore')

import MySQLdb

import environment
import utils
from mysql_flavor import mysql_flavor

from vtdb import tablet

tablet_cell_map = {
    62344: 'nj',
    62044: 'nj',
    41983: 'nj',
    31981: 'ny',
}


class Tablet(object):
  """This class helps manage a vttablet or vtocc instance.

  To use it for vttablet, you need to use init_tablet and/or
  start_vttablet. For vtocc, you can just call start_vtocc.
  If you use it to start as vtocc, many of the support functions
  that are meant for vttablet will not work."""
  default_uid = 62344
  seq = 0
  tablets_running = 0
  default_db_config = {
      'app': {
          'uname': 'vt_app',
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

  def __init__(self, tablet_uid=None, port=None, mysql_port=None, cell=None):
    self.tablet_uid = tablet_uid or (Tablet.default_uid + Tablet.seq)
    self.port = port or (environment.reserve_ports(1))
    self.mysql_port = mysql_port or (environment.reserve_ports(1))
    Tablet.seq += 1

    if cell:
      self.cell = cell
    else:
      self.cell = tablet_cell_map.get(tablet_uid, 'nj')
    self.proc = None

    # filled in during init_tablet
    self.keyspace = None
    self.shard = None

    # utility variables
    self.tablet_alias = 'test_%s-%010d' % (self.cell, self.tablet_uid)
    self.zk_tablet_path = (
        '/zk/test_%s/vt/tablets/%010d' % (self.cell, self.tablet_uid))
    self.zk_pid = self.zk_tablet_path + '/pid'
    self.checked_zk_pid = False

  def mysqlctl(self, cmd, extra_my_cnf=None, with_ports=False, verbose=False):
    all_extra_my_cnf = []
    flavor_my_cnf = mysql_flavor.extra_my_cnf()
    if flavor_my_cnf:
      all_extra_my_cnf.append(flavor_my_cnf)
    if extra_my_cnf:
      all_extra_my_cnf.append(extra_my_cnf)
    extra_env = None
    if all_extra_my_cnf:
      extra_env = {
          'EXTRA_MY_CNF': ':'.join(all_extra_my_cnf),
      }
    args = environment.binary_args('mysqlctl') + [
        '-log_dir', environment.vtlogroot,
        '-tablet_uid', str(self.tablet_uid)]
    if with_ports:
      args.extend(['-port', str(self.port),
                   '-mysql_port', str(self.mysql_port)])
    if verbose:
      args.append('-alsologtostderr')
    args.extend(cmd)
    return utils.run_bg(args, extra_env=extra_env)

  def init_mysql(self, extra_my_cnf=None):
    return self.mysqlctl(
        ['init', '-bootstrap_archive', mysql_flavor.bootstrap_archive()],
        extra_my_cnf=extra_my_cnf, with_ports=True)

  def start_mysql(self):
    return self.mysqlctl(['start'], with_ports=True)

  def shutdown_mysql(self):
    return self.mysqlctl(['shutdown'], with_ports=True)

  def teardown_mysql(self):
    return self.mysqlctl(['teardown', '-force'])

  def remove_tree(self):
    try:
      shutil.rmtree(self.tablet_dir)
    except OSError as e:
      if utils.options.verbose == 2:
        print >> sys.stderr, e, self.tablet_dir

  def mysql_connection_parameters(self, dbname, user='vt_dba'):
    return dict(user=user,
                unix_socket=self.tablet_dir + '/mysql.sock',
                db=dbname)

  def connect(self, dbname='', user='vt_dba'):
    conn = MySQLdb.Connect(
        **self.mysql_connection_parameters(dbname, user))
    return conn, conn.cursor()

  # Query the MySQL instance directly
  def mquery(self, dbname, query, write=False, user='vt_dba'):
    conn, cursor = self.connect(dbname, user=user)
    if write:
      conn.begin()
    if isinstance(query, basestring):
      query = [query]

    for q in query:
      # logging.debug("mysql(%s,%s): %s", self.tablet_uid, dbname, q)
      cursor.execute(q)

    if write:
      conn.commit()

    try:
      return cursor.fetchall()
    finally:
      conn.close()

  # path is either:
  # - keyspace/shard for vttablet and vttablet-streaming
  # - zk path for vtdb, vtdb-streaming
  def vquery(self, query, path='', user=None, password=None, driver=None,
             verbose=False, raise_on_error=True):
    return utils.vtclient2(self.port, path, query, user=user,
                           password=password, driver=driver,
                           verbose=verbose, raise_on_error=raise_on_error)

  def assert_table_count(self, dbname, table, n, where=''):
    result = self.mquery(dbname, 'select count(*) from ' + table + ' ' + where)
    if result[0][0] != n:
      raise utils.TestError('expected %u rows in %s' % (n, table), result)

  def reset_replication(self):
    self.mquery('', mysql_flavor.reset_replication_commands())

  def populate(self, dbname, create_sql, insert_sqls=[]):
    self.create_db(dbname)
    if isinstance(create_sql, basestring):
      create_sql = [create_sql]
    for q in create_sql:
      self.mquery(dbname, q)
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

  def clean_dbs(self):
    logging.debug('mysql(%s): removing all databases', self.tablet_uid)
    rows = self.mquery('', 'show databases')
    for row in rows:
      dbname = row[0]
      if dbname in ['information_schema', '_vt', 'mysql']:
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

  def update_addrs(self):
    args = [
        'UpdateTabletAddrs',
        '-hostname', 'localhost',
        '-ip-addr', '127.0.0.1',
        '-mysql-port', '%u' % self.mysql_port,
        '-vt-port', '%u' % self.port,
        '-vts-port', '%u' % (self.port + 500),
        self.tablet_alias
    ]
    return utils.run_vtctl(args)

  def scrap(self, force=False, skip_rebuild=False):
    args = ['ScrapTablet']
    if force:
      args.append('-force')
    if skip_rebuild:
      args.append('-skip-rebuild')
    args.append(self.tablet_alias)
    utils.run_vtctl(args, auto_log=True)

  def init_tablet(
      self, tablet_type, keyspace=None, shard=None, force=True,
      start=False, dbname=None, parent=True, wait_for_start=True,
      **kwargs):
    self.tablet_type = tablet_type
    self.keyspace = keyspace
    self.shard = shard

    if dbname is None:
      self.dbname = 'vt_' + (self.keyspace or 'database')
    else:
      self.dbname = dbname

    args = ['InitTablet',
            '-hostname', 'localhost',
            '-port', str(self.port),
            '-mysql_port', str(self.mysql_port)]
    if force:
      args.append('-force')
    if parent:
      args.append('-parent')
    if dbname:
      args.extend(['-db-name-override', dbname])
    if keyspace:
      args.extend(['-keyspace', keyspace])
    if shard:
      args.extend(['-shard', shard])
    args.extend([self.tablet_alias, tablet_type])
    utils.run_vtctl(args)
    if start:
      if not wait_for_start:
        expected_state = None
      elif tablet_type == 'master' or tablet_type == 'replica' or tablet_type == 'rdonly' or tablet_type == 'batch':
        expected_state = 'SERVING'
      else:
        expected_state = 'NOT_SERVING'
      self.start_vttablet(wait_for_state=expected_state, **kwargs)

  def conn(self):
    conn = tablet.TabletConnection(
        'localhost:%d' % self.port, self.tablet_type, self.keyspace,
        self.shard, 30)
    conn.dial()
    return conn

  @property
  def tablet_dir(self):
    return '%s/vt_%010d' % (environment.vtdataroot, self.tablet_uid)

  def flush(self):
    utils.curl('http://localhost:%s%s' %
               (self.port, environment.flush_logs_url),
               stderr=utils.devnull, stdout=utils.devnull)

  def _start_prog(self, binary, port=None, auth=False, memcache=False,
                  wait_for_state='SERVING', customrules=None,
                  schema_override=None, cert=None, key=None, ca_cert=None,
                  repl_extra_flags={}, table_acl_config=None,
                  lameduck_period=None, security_policy=None,
                  extra_args=None):
    environment.prog_compile(binary)
    args = environment.binary_args(binary)
    args.extend(['-port', '%s' % (port or self.port),
                 '-log_dir', environment.vtlogroot])

    dbconfigs = self._get_db_configs_file(repl_extra_flags)
    for key1 in dbconfigs:
      for key2 in dbconfigs[key1]:
        args.extend(['-db-config-' + key1 + '-' + key2, dbconfigs[key1][key2]])

    if memcache:
      args.extend(['-rowcache-bin', environment.memcached_bin()])
      memcache_socket = os.path.join(self.tablet_dir, 'memcache.sock')
      args.extend(['-rowcache-socket', memcache_socket])
      args.extend(['-enable-rowcache'])

    if auth:
      args.extend(
          ['-auth-credentials',
           os.path.join(
               environment.vttop, 'test', 'test_data',
               'authcredentials_test.json')])

    if customrules:
      args.extend(['-customrules', customrules])

    if schema_override:
      args.extend(['-schema-override', schema_override])

    if table_acl_config:
      args.extend(['-table-acl-config', table_acl_config])
      args.extend(['-queryserver-config-strict-table-acl'])

    if cert:
      self.secure_port = environment.reserve_ports(1)
      args.extend(['-secure-port', '%s' % self.secure_port,
                   '-cert', cert,
                   '-key', key])
      if ca_cert:
        args.extend(['-ca_cert', ca_cert])
    if lameduck_period:
      args.extend(['-lameduck-period', lameduck_period])
    if security_policy:
      args.extend(['-security_policy', security_policy])
    if extra_args:
      args.extend(extra_args)

    stderr_fd = open(os.path.join(self.tablet_dir, '%s.stderr' % binary), 'w')
    # increment count only the first time
    if not self.proc:
      Tablet.tablets_running += 1
    self.proc = utils.run_bg(args, stderr=stderr_fd)
    stderr_fd.close()

    # wait for query service to be in the right state
    if wait_for_state:
      if binary == 'vttablet':
        self.wait_for_vttablet_state(wait_for_state, port=port)
      else:
        self.wait_for_vtocc_state(wait_for_state, port=port)

    return self.proc

  def start_vttablet(self, port=None, auth=False, memcache=False,
                     wait_for_state='SERVING', customrules=None,
                     schema_override=None, cert=None, key=None, ca_cert=None,
                     repl_extra_flags={}, table_acl_config=None,
                     lameduck_period=None, security_policy=None,
                     target_tablet_type=None, full_mycnf_args=False,
                     extra_args=None):
    """Starts a vttablet process, and returns it.

    The process is also saved in self.proc, so it's easy to kill as well.
    """
    environment.prog_compile('vtaction')
    args = []
    args.extend(['-tablet-path', self.tablet_alias])
    args.extend(environment.topo_server_flags())
    args.extend(utils.binlog_player_protocol_flags)
    args.extend(environment.tablet_manager_protocol_flags())

    if full_mycnf_args:
      # this flag is used to specify all the mycnf_ flags, to make
      # sure that code works and can fork actions.
      relay_log_path = os.path.join(self.tablet_dir, 'relay-logs',
                                    'vt-%010d-relay-bin' % self.tablet_uid)
      args.extend([
          '-mycnf_server_id', str(self.tablet_uid),
          '-mycnf_mysql_port', str(self.mysql_port),
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
          '-mycnf_bin_log_path', os.path.join(self.tablet_dir, 'bin-logs',
                                              'vt-%010d-bin' % self.tablet_uid),
          '-mycnf_master_info_file', os.path.join(self.tablet_dir,
                                                  'master.info'),
          '-mycnf_pid_file', os.path.join(self.tablet_dir, 'mysql.pid'),
          '-mycnf_tmp_dir', os.path.join(self.tablet_dir, 'tmp'),
          '-mycnf_slave_load_tmp_dir', os.path.join(self.tablet_dir, 'tmp'),
      ])
    if target_tablet_type:
      args.extend(['-target_tablet_type', target_tablet_type,
                   '-health_check_interval', '2s',
                   '-allowed_replication_lag', '30'])

    if extra_args:
      args.extend(extra_args)

    return self._start_prog(binary='vttablet', port=port, auth=auth,
                            memcache=memcache, wait_for_state=wait_for_state,
                            customrules=customrules,
                            schema_override=schema_override,
                            cert=cert, key=key, ca_cert=ca_cert,
                            repl_extra_flags=repl_extra_flags,
                            table_acl_config=table_acl_config,
                            lameduck_period=lameduck_period, extra_args=args,
                            security_policy=security_policy)

  def start_vtocc(self, port=None, auth=False, memcache=False,
                  wait_for_state='SERVING', customrules=None,
                  schema_override=None, cert=None, key=None, ca_cert=None,
                  repl_extra_flags={}, table_acl_config=None,
                  lameduck_period=None, security_policy=None,
                  keyspace=None, shard=False,
                  extra_args=None):
    """Starts a vtocc process, and returns it.

    The process is also saved in self.proc, so it's easy to kill as well.
    """
    self.keyspace = keyspace
    self.shard = shard
    self.dbname = 'vt_' + (self.keyspace or 'database')
    args = []
    args.extend(["-db-config-app-unixsocket", self.tablet_dir + '/mysql.sock'])
    args.extend(["-db-config-dba-unixsocket", self.tablet_dir + '/mysql.sock'])
    args.extend(["-db-config-app-keyspace", keyspace])
    args.extend(["-db-config-app-shard", shard])
    args.extend(["-binlog-path", "foo"])

    if extra_args:
      args.extend(extra_args)

    return self._start_prog(binary='vtocc', port=port, auth=auth,
                            memcache=memcache, wait_for_state=wait_for_state,
                            customrules=customrules,
                            schema_override=schema_override,
                            cert=cert, key=key, ca_cert=ca_cert,
                            repl_extra_flags=repl_extra_flags,
                            table_acl_config=table_acl_config,
                            lameduck_period=lameduck_period, extra_args=args,
                            security_policy=security_policy)


  def wait_for_vttablet_state(self, expected, timeout=60.0, port=None):
    # wait for zookeeper PID just to be sure we have it
    if environment.topo_server_implementation == 'zookeeper':
      if not self.checked_zk_pid:
        utils.run(environment.binary_args('zk') + ['wait', '-e', self.zk_pid],
                  stdout=utils.devnull)
        self.checked_zk_pid = True
    self.wait_for_vtocc_state(expected, timeout=timeout, port=port)

  def wait_for_vtocc_state(self, expected, timeout=60.0, port=None):
    while True:
      v = utils.get_vars(port or self.port)
      if v == None:
        logging.debug(
            '  vttablet %s not answering at /debug/vars, waiting...',
            self.tablet_alias)
      else:
        if 'Voltron' not in v:
          logging.debug(
              '  vttablet %s not exporting Voltron, waiting...',
              self.tablet_alias)
        else:
          s = v['TabletStateName']
          if s != expected:
            logging.debug(
                '  vttablet %s in state %s != %s', self.tablet_alias, s,
                expected)
          else:
            break
      timeout = utils.wait_step('waiting for state %s' % expected, timeout,
                                sleep_time=0.1)

  def _get_db_configs_file(self, repl_extra_flags={}):
    config = dict(self.default_db_config)
    if self.keyspace:
      config['app']['dbname'] = self.dbname
      config['dba']['dbname'] = self.dbname
      config['repl']['dbname'] = self.dbname
    config['repl'].update(repl_extra_flags)
    return config

  def get_status(self):
    return utils.get_status(self.port)

  def kill_vttablet(self):
    logging.debug('killing vttablet: %s', self.tablet_alias)
    if self.proc is not None:
      Tablet.tablets_running -= 1
      self.proc.terminate()
      self.proc.wait()
      self.proc = None

  def wait_for_binlog_server_state(self, expected, timeout=30.0):
    while True:
      v = utils.get_vars(self.port)
      if v == None:
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
      timeout = utils.wait_step('waiting for binlog server state %s' % expected,
                                timeout, sleep_time=0.5)
    logging.debug('tablet %s binlog service is in state %s',
                  self.tablet_alias, expected)

  def wait_for_binlog_player_count(self, expected, timeout=30.0):
    while True:
      v = utils.get_vars(self.port)
      if v == None:
        logging.debug('  vttablet not answering at /debug/vars, waiting...')
      else:
        if 'BinlogPlayerMapSize' not in v:
          logging.debug(
              '  vttablet not exporting BinlogPlayerMapSize, waiting...')
        else:
          s = v['BinlogPlayerMapSize']
          if s != expected:
            logging.debug("  vttablet's binlog player map has count %u != %u",
                          s, expected)
          else:
            break
      timeout = utils.wait_step('waiting for binlog player count %d' % expected,
                                timeout, sleep_time=0.5)
    logging.debug('tablet %s binlog player has %d players',
                  self.tablet_alias, expected)

  @classmethod
  def check_vttablet_count(klass):
    if Tablet.tablets_running > 0:
      raise utils.TestError('This test is not killing all its vttablets')


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
