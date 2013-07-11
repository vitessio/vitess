import json
import os
import shutil
import sys
import time
import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import MySQLdb

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
  default_db_config = {
    "app": {
      "uname": "vt_dba", # it's vt_dba so that the tests can create
                         # and drop tables.
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
    self.port = port or (utils.reserve_ports(1))
    self.mysql_port = mysql_port or (utils.reserve_ports(1))
    Tablet.seq += 1

    if cell:
      self.cell = cell
    else:
      self.cell = tablet_cell_map.get(tablet_uid, 'nj')
    self.proc = None
    self.memcached = None
    self.memcache_path = None

    # filled in during init_tablet
    self.keyspace = None
    self.shard = None

    # utility variables
    self.tablet_alias = 'test_%s-%010d' % (self.cell, self.tablet_uid)
    self.zk_tablet_path = '/zk/test_%s/vt/tablets/%010d' % (self.cell, self.tablet_uid)
    self.zk_pid = self.zk_tablet_path + '/pid'

  def mysqlctl(self, cmd, quiet=False, extra_my_cnf=None):
    utils.prog_compile(['mysqlctl'])

    logLevel = ''
    if utils.options.verbose and not quiet:
      logLevel = ' -log.level=INFO'

    env = None
    if extra_my_cnf:
      env = os.environ.copy()
      env['EXTRA_MY_CNF'] = extra_my_cnf

    return utils.run_bg(os.path.join(utils.vtroot, 'bin', 'mysqlctl') +
                        logLevel + ' -tablet-uid %u ' % self.tablet_uid + cmd,
                        env=env)

  def init_mysql(self, extra_my_cnf=None):
    return self.mysqlctl('-port %u -mysql-port %u init' % (self.port, self.mysql_port), quiet=True, extra_my_cnf=extra_my_cnf)

  def start_mysql(self):
    return self.mysqlctl('-port %u -mysql-port %u start' % (self.port, self.mysql_port), quiet=True)

  def shutdown_mysql(self):
    return self.mysqlctl('-port %u -mysql-port %u shutdown' % (self.port, self.mysql_port), quiet=True)

  def teardown_mysql(self):
    return self.mysqlctl('teardown -force', quiet=True)

  def remove_tree(self):
    path = '%s/vt_%010d' % (utils.vtdataroot, self.tablet_uid)
    try:
      shutil.rmtree(path)
    except OSError as e:
      if utils.options.verbose:
        print >> sys.stderr, e, path

  def mysql_connection_parameters(self, dbname):
    return dict(user='vt_dba',
                unix_socket='%s/vt_%010d/mysql.sock' % (utils.vtdataroot, self.tablet_uid),
                db=dbname)

  def connect(self, dbname=''):
    conn = MySQLdb.Connect(
      **self.mysql_connection_parameters(dbname))
    return conn, conn.cursor()

  # Query the MySQL instance directly
  def mquery(self, dbname, query, write=False):
    conn, cursor = self.connect(dbname)
    if write:
      conn.begin()
    if isinstance(query, basestring):
      query = [query]

    for q in query:
      # utils.debug("mysql(%s,%s): %s" % (self.tablet_uid, dbname, q))
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

  def create_db(self, name):
    self.mquery('', 'drop database if exists %s' % name)
    while self.has_db(name):
      utils.debug("%s sleeping while waiting for database drop: %s" % (self.tablet_alias, name))
      time.sleep(0.3)
      self.mquery('', 'drop database if exists %s' % name)
    self.mquery('', 'create database %s' % name)

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

  def scrap(self, force=False, skip_rebuild=False):
    args = ['ScrapTablet']
    if force:
      args.append("-force")
    if skip_rebuild:
      args.append("-skip-rebuild")
    args.append(self.tablet_alias)
    utils.run_vtctl(args, auto_log=True)

  def init_tablet(self, tablet_type, keyspace=None, shard=None, force=True, start=False, auth=False, dbname=None, memcache=False):
    self.keyspace = keyspace
    self.shard = shard

    if dbname is None:
      self.dbname = "vt_" + (self.keyspace or "database")
    else:
      self.dbname = dbname

    args = ['InitTablet']
    if force:
      args.append('-force')
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
      if tablet_type == 'master' or tablet_type == 'replica' or tablet_type == 'rdonly' or tablet_type == 'batch':
        expected_state = "OPEN"
      else:
        expected_state = "NOT_SERVING"
      self.start_vttablet(wait_for_state=expected_state, auth=auth, memcache=True)

  @property
  def tablet_dir(self):
    return "%s/vt_%010d" % (utils.vtdataroot, self.tablet_uid)

  @property
  def querylog_file(self):
    return os.path.join(self.tablet_dir, "vttablet.querylog")

  @property
  def logfile(self):
    return os.path.join(self.tablet_dir, "vttablet.log")

  def start_vttablet(self, port=None, auth=False, memcache=False, wait_for_state="OPEN", customrules=None, schema_override=None, cert=None, key=None, ca_cert=None, repl_extra_flags={}):
    """
    Starts a vttablet process, and returns it.
    The process is also saved in self.proc, so it's easy to kill as well.
    """
    utils.prog_compile(['vtaction',
                        'vttablet',
                        ])

    args = [os.path.join(utils.vtroot, 'bin', 'vttablet'),
            '-port', '%s' % (port or self.port),
            '-tablet-path', self.tablet_alias,
            '-logfile', self.logfile,
            '-log.level', 'INFO',
            '-db-configs-file', self._write_db_configs_file(repl_extra_flags),
            '-debug-querylog-file', self.querylog_file]

    if memcache:
      self.start_memcache()
      args.extend(['-rowcache', self.memcache_path])

    if auth:
      args.extend(['-auth-credentials', os.path.join(utils.vttop, 'test', 'test_data', 'authcredentials_test.json')])

    if customrules:
      args.extend(['-customrules', customrules])

    if schema_override:
      args.extend(['-schema-override', schema_override])

    if cert:
      self.secure_port = utils.reserve_ports(1)
      args.extend(['-secure-port', '%s' % self.secure_port,
                   '-cert', cert,
                   '-key', key])
      if ca_cert:
        args.extend(['-ca-cert', ca_cert])

    stderr_fd = open(os.path.join(self.tablet_dir, "vttablet.stderr"), "w")
    self.proc = utils.run_bg(args, stderr=stderr_fd)
    stderr_fd.close()

    # wait for zookeeper PID just to be sure we have it
    utils.run(utils.vtroot+'/bin/zk wait -e ' + self.zk_pid, stdout=utils.devnull)

    # wait for query service to be in the right state
    self.wait_for_vttablet_state(wait_for_state, port=port)

    return self.proc

  def wait_for_vttablet_state(self, expected, timeout=5.0, port=None):
    while True:
      v = utils.get_vars(port or self.port)
      if v == None:
        utils.debug("  vttablet not answering at /debug/vars, waiting...")
      else:
        if 'Voltron' not in v:
          utils.debug("  vttablet not exporting Voltron, waiting...")
        else:
          s = v['Voltron']['States']['Current']
          if s != expected:
            utils.debug("  vttablet in state %s != %s" % (s, expected))
          else:
            break

      utils.debug("sleeping a bit while we wait")
      time.sleep(0.1)
      timeout -= 0.1
      if timeout <= 0:
        raise utils.TestError("timeout waiting for state %s" % expected)

  def _write_db_configs_file(self, repl_extra_flags={}):
    config = dict(self.default_db_config)
    if self.keyspace:
      config['app']['dbname'] = self.dbname
      config['dba']['dbname'] = self.dbname
      config['repl']['dbname'] = self.dbname
    config['repl'].update(repl_extra_flags)
    path = os.path.join(self.tablet_dir, 'db-configs.json')

    with open(path, 'w') as fi:
      json.dump(config, fi)

    return path

  def kill_vttablet(self):
    utils.debug("killing vttablet: " + self.tablet_alias)
    if self.proc is not None:
      utils.kill_sub_process(self.proc)
    if self.memcached:
      self.kill_memcache()

  def start_memcache(self):
      self.memcache_path = os.path.join(self.tablet_dir, "memcache.sock")
      try:
        self.memcached = utils.run_bg(' '.join(["memcached", "-s", self.memcache_path]), stdout=utils.devnull)
      except Exception as e:
        print "Error: memcached couldn't start"
        raise

  def kill_memcache(self):
    utils.kill_sub_process(self.memcached)
