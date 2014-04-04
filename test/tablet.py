import json
import logging
import os
import shutil
import sys
import time
import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import MySQLdb

import environment
import utils

tablet_cell_map = {
    62344: 'nj',
    62044: 'nj',
    41983: 'nj',
    31981: 'ny',
}

class Tablet(object):
  default_uid = 62344
  seq = 0
  tablets_running = 0
  default_db_config = {
    "app": {
      "uname": "vt_app",
      "charset": "utf8"
      },
    "dba": {
      "uname": "vt_dba",
      "charset": "utf8"
      },
    "repl": {
      "uname": "vt_repl",
      "charset": "utf8"
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
    self.zk_tablet_path = '/zk/test_%s/vt/tablets/%010d' % (self.cell, self.tablet_uid)
    self.zk_pid = self.zk_tablet_path + '/pid'

  def mysqlctl(self, cmd, extra_my_cnf=None, with_ports=False):
    all_extra_my_cnf = []
    if environment.mysql_flavor == "GoogleMysql":
      # we have to manually enable hierarchical replication to support groupid
      all_extra_my_cnf.append(environment.vttop + "/config/mycnf/master_google.cnf")
    if extra_my_cnf:
       all_extra_my_cnf.append(extra_my_cnf)
    env = None
    if all_extra_my_cnf:
      env = os.environ.copy()
      env['EXTRA_MY_CNF'] = ":".join(all_extra_my_cnf)
    ports_params = []
    if with_ports:
      ports_params = ['-port', str(self.port),
                      '-mysql-port', str(self.mysql_port)]

    return utils.run_bg([environment.binary_path('mysqlctl'),
                         '-log_dir', environment.vtlogroot,
                         '-tablet-uid', str(self.tablet_uid)] +
                        ports_params + cmd, env=env)

  def init_mysql(self, extra_my_cnf=None):
    return self.mysqlctl(['init'], extra_my_cnf=extra_my_cnf,
                         with_ports=True)

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
                unix_socket=self.tablet_dir+'/mysql.sock',
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
      raise utils.TestError("expected %u rows in %s" % (n, table), result)

  def reset_replication(self):
    commands = [
        'RESET MASTER',
        'STOP SLAVE',
        'RESET SLAVE',
        ]
    if environment.mysql_flavor == "GoogleMysql":
      commands.append('CHANGE MASTER TO MASTER_HOST = ""')
    self.mquery('', commands)

  def populate(self, dbname, create_sql, insert_sqls=[]):
      self.create_db(dbname)
      if isinstance(create_sql, basestring):
        create_sql= [create_sql]
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
      logging.debug("%s sleeping while waiting for database drop: %s",
                    self.tablet_alias, name)
      time.sleep(0.3)
      self.mquery('', 'drop database if exists %s' % name)

  def create_db(self, name):
    self.drop_db(name)
    self.mquery('', 'create database %s' % name)

  def clean_dbs(self):
    logging.debug("mysql(%s): removing all databases", self.tablet_uid)
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
      args.append("-force")
    if skip_rebuild:
      args.append("-skip-rebuild")
    args.append(self.tablet_alias)
    utils.run_vtctl(args, auto_log=True)

  def init_tablet(self, tablet_type, keyspace=None, shard=None, force=True, start=False, dbname=None, parent=True, wait_for_start=True, **kwargs):
    self.keyspace = keyspace
    self.shard = shard

    if dbname is None:
      self.dbname = "vt_" + (self.keyspace or "database")
    else:
      self.dbname = dbname

    args = ['InitTablet']
    if force:
      args.append('-force')
    if parent:
      args.append('-parent')
    if dbname:
      args.append('-db-name-override='+dbname)
    args.extend([self.tablet_alias,
                 'localhost',
                 str(self.mysql_port),
                 str(self.port)])
    if keyspace:
      args.append(keyspace)
    else:
      args.append('')
    if shard:
      args.append(shard)
    else:
      args.append('')
    args.append(tablet_type)
    utils.run_vtctl(args)
    if start:
      if not wait_for_start:
        expected_state = None
      elif tablet_type == 'master' or tablet_type == 'replica' or tablet_type == 'rdonly' or tablet_type == 'batch':
        expected_state = "SERVING"
      else:
        expected_state = "NOT_SERVING"
      self.start_vttablet(wait_for_state=expected_state, **kwargs)

  @property
  def tablet_dir(self):
    return "%s/vt_%010d" % (environment.vtdataroot, self.tablet_uid)

  def flush(self):
    utils.curl('http://localhost:%s%s' % (self.port, environment.flush_logs_url), stderr=utils.devnull, stdout=utils.devnull)

  def start_vttablet(self, port=None, auth=False, memcache=False, wait_for_state="SERVING", customrules=None, schema_override=None, cert=None, key=None, ca_cert=None, repl_extra_flags={}, sensitive_mode=False, target_tablet_type=None):
    """
    Starts a vttablet process, and returns it.
    The process is also saved in self.proc, so it's easy to kill as well.
    """
    environment.prog_compile('vtaction')
    args = [environment.binary_path('vttablet'),
            '-port', '%s' % (port or self.port),
            '-tablet-path', self.tablet_alias,
            '-log_dir', environment.vtlogroot]
    args.extend(environment.topo_server_flags())
    args.extend(utils.binlog_player_protocol_flags)

    dbconfigs = self._get_db_configs_file(repl_extra_flags)
    for key1 in dbconfigs:
      for key2 in dbconfigs[key1]:
        args.extend(["-db-config-"+key1+"-"+key2, dbconfigs[key1][key2]])

    if memcache:
      if os.path.exists(environment.vtroot + "/bin/memcached"):
        args.extend(["-rowcache-bin", environment.vtroot + "/bin/memcached"])
      else:
        args.extend(["-rowcache-bin", "memcached"])
      memcache_socket = os.path.join(self.tablet_dir, "memcache.sock")
      args.extend(["-rowcache-socket", memcache_socket])
      args.extend(["-enable-rowcache"])

    if auth:
      args.extend(['-auth-credentials', os.path.join(environment.vttop, 'test', 'test_data', 'authcredentials_test.json')])

    if customrules:
      args.extend(['-customrules', customrules])

    if schema_override:
      args.extend(['-schema-override', schema_override])

    if cert:
      self.secure_port = environment.reserve_ports(1)
      args.extend(['-secure-port', '%s' % self.secure_port,
                   '-cert', cert,
                   '-key', key])
      if ca_cert:
        args.extend(['-ca_cert', ca_cert])
    if sensitive_mode:
      args.extend(['-queryserver-config-sensitive-mode'])
    if target_tablet_type:
      args.extend(['-target_tablet_type', target_tablet_type,
                   '-health_check_interval', '2s',
                   '-allowed_replication_lag', '30'])

    stderr_fd = open(os.path.join(self.tablet_dir, "vttablet.stderr"), "w")
    # increment count only the first time
    if not self.proc:
      Tablet.tablets_running += 1
    self.proc = utils.run_bg(args, stderr=stderr_fd)
    stderr_fd.close()

    # wait for zookeeper PID just to be sure we have it
    if environment.topo_server_implementation == 'zookeeper':
      utils.run(environment.binary_path('zk')+' wait -e ' + self.zk_pid, stdout=utils.devnull)

    # wait for query service to be in the right state
    if wait_for_state:
      self.wait_for_vttablet_state(wait_for_state, port=port)

    return self.proc

  def wait_for_vttablet_state(self, expected, timeout=60.0, port=None):
    while True:
      v = utils.get_vars(port or self.port)
      if v == None:
        logging.debug("  vttablet %s not answering at /debug/vars, waiting...", self.tablet_alias)
      else:
        if 'Voltron' not in v:
          logging.debug("  vttablet %s not exporting Voltron, waiting...", self.tablet_alias)
        else:
          s = v["TabletStateName"]
          if s != expected:
            logging.debug("  vttablet %s in state %s != %s", self.tablet_alias, s, expected)
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

  def kill_vttablet(self):
    logging.debug("killing vttablet: %s", self.tablet_alias)
    if self.proc is not None:
      Tablet.tablets_running -= 1
      self.proc.terminate()
      self.proc.wait()
      self.proc = None

  def wait_for_binlog_server_state(self, expected, timeout=30.0):
    while True:
      v = utils.get_vars(self.port)
      if v == None:
        logging.debug("  vttablet not answering at /debug/vars, waiting...")
      else:
        if 'UpdateStreamState' not in v:
          logging.debug("  vttablet not exporting BinlogServerState, waiting...")
        else:
          s = v['UpdateStreamState']
          if s != expected:
            logging.debug("  vttablet's binlog server in state %s != %s", s,
                          expected)
          else:
            break
      timeout = utils.wait_step('waiting for binlog server state %s' % expected,
                                timeout, sleep_time=0.5)
    logging.debug("tablet %s binlog service is in state %s",
                  self.tablet_alias, expected)

  def wait_for_binlog_player_count(self, expected, timeout=30.0):
    while True:
      v = utils.get_vars(self.port)
      if v == None:
        logging.debug("  vttablet not answering at /debug/vars, waiting...")
      else:
        if 'BinlogPlayerMapSize' not in v:
          logging.debug("  vttablet not exporting BinlogPlayerMapSize, waiting...")
        else:
          s = v['BinlogPlayerMapSize']
          if s != expected:
            logging.debug("  vttablet's binlog player map has count %u != %u",
                          s, expected)
          else:
            break
      timeout = utils.wait_step('waiting for binlog player count %d' % expected,
                                timeout, sleep_time=0.5)
    logging.debug("tablet %s binlog player has %d players",
                  self.tablet_alias, expected)

  @classmethod
  def check_vttablet_count(klass):
    if Tablet.tablets_running > 0:
      raise utils.TestError("This test is not killing all its vttablets")

def kill_tablets(tablets):
  for t in tablets:
    logging.debug("killing vttablet: %s", t.tablet_alias)
    if t.proc is not None:
      Tablet.tablets_running -= 1
      t.proc.terminate()

  for t in tablets:
    if t.proc is not None:
      t.proc.wait()
      t.proc = None
