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

vttop = os.environ['VTTOP']
vtroot = os.environ['VTROOT']
vtdataroot = os.environ.get('VTDATAROOT', '/vt')

tablet_cell_map = {
    62344: 'nj',
    62044: 'nj',
    41983: 'nj',
    31981: 'ny',
}

class Tablet(object):
  default_uid = 62344
  default_port = 6700
  default_mysql_port = 3700
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
    self.port = port or (Tablet.default_port + Tablet.seq)
    self.mysql_port = mysql_port or (Tablet.default_mysql_port + Tablet.seq)
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
    self.zk_tablet_alias = None

    # utility variables
    self.zk_tablet_path = '/zk/test_%s/vt/tablets/%010d' % (self.cell, self.tablet_uid)
    self.zk_pid = self.zk_tablet_path + '/pid'

  def mysqlctl(self, cmd, quiet=False):
    utils.prog_compile(['mysqlctl'])

    logLevel = ''
    if utils.options.verbose and not quiet:
      logLevel = ' -log.level=INFO'

    return utils.run_bg(os.path.join(vtroot, 'bin', 'mysqlctl') +
                        logLevel + ' -tablet-uid %u ' % self.tablet_uid + cmd)

  def start_mysql(self):
    return self.mysqlctl('-port %u -mysql-port %u init' % (self.port, self.mysql_port), quiet=True)

  def teardown_mysql(self):
    return self.mysqlctl('teardown -force', quiet=True)

  def remove_tree(self):
    path = '%s/vt_%010d' % (vtdataroot, self.tablet_uid)
    try:
      shutil.rmtree(path)
    except OSError as e:
      if utils.options.verbose:
        print >> sys.stderr, e, path

  def connect(self, dbname=''):
    conn = MySQLdb.Connect(
        user='vt_dba',
        unix_socket='%s/vt_%010d/mysql.sock' % (vtdataroot, self.tablet_uid),
        db=dbname)
    return conn, conn.cursor()

  # Query the MySQL instance directly
  def mquery(self, dbname, query, write=False):
    conn, cursor = self.connect(dbname)
    if write:
      conn.begin()
    if isinstance(query, basestring):
      query = [query]

    for q in query:
      if utils.options.verbose:
        print "mysql(%s,%s): %s" % (self.tablet_uid, dbname, q)
      cursor.execute(q)

    if write:
      conn.commit()

    try:
      return cursor.fetchall()
    finally:
      conn.close()

  def vquery(self, query, dbname='', user=None, password=None, driver=None,
             verbose=False, raise_on_error=True):
    return utils.vtclient2(self.port, dbname, query, user=user,
                           password=password, driver=driver,
                           verbose=verbose, raise_on_error=raise_on_error)

  def assert_table_count(self, dbname, table, n, where=''):
    result = self.mquery(dbname, 'select count(*) from ' + table + ' ' + where)
    if result[0][0] != n:
      raise utils.TestError("expected %u rows in %s" % (n, table), result)

  def populate(self, dbname, create_sql, insert_sqls=[]):
      self.create_db(dbname)
      self.mquery(dbname, create_sql)
      for q in insert_sqls:
        self.mquery(dbname, q, write=True)

  def create_db(self, name):
    self.mquery('', 'drop database if exists %s' % name)
    rows = self.mquery('', 'show databases')
    for row in rows:
      dbname = row[0]
      if dbname == name:
        raise utils.TestError("drop database didn't work???")
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

  def init_tablet(self, tablet_type, keyspace=None, shard=None, force=True, zk_parent_alias=None, key_start=None, key_end=None, start=False):
    self.keyspace = keyspace
    self.shard = shard
    if keyspace:
      self.zk_tablet_alias = "/zk/global/vt/keyspaces/%s/shards/%s/test_%s-%010d" % (self.keyspace, self.shard, self.cell, self.tablet_uid)
    else:
      self.zk_tablet_alias = ""

    args = ['InitTablet']
    if force:
      args.append('-force')
    if key_start:
      args.append('-key-start='+key_start)
    if key_end:
      args.append('-key-end='+key_end)
    args.extend([self.zk_tablet_path,
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
    if zk_parent_alias:
      args.append(zk_parent_alias)

    utils.run_vtctl(args)
    if start:
      if tablet_type == 'master' or tablet_type == 'replica' or tablet_type == 'rdonly' or tablet_type == 'batch':
        expected_state = "OPEN"
      else:
        expected_state = "NOT_SERVING"
      self.start_vttablet(wait_for_state=expected_state)

  @property
  def tablet_dir(self):
    return "%s/vt_%010d" % (vtdataroot, self.tablet_uid)

  @property
  def querylog_file(self):
    return os.path.join(self.tablet_dir, "vttablet.querylog")

  @property
  def logfile(self):
    return os.path.join(self.tablet_dir, "vttablet.log")

  def start_vttablet(self, port=None, auth=False, memcache=False, wait_for_state="OPEN"):
    """
    Starts a vttablet process, and returns it.
    The process is also saved in self.proc, so it's easy to kill as well.
    """
    utils.prog_compile(['vtaction',
                        'vttablet',
                        ])
    if memcache:
      self.start_memcache()

    args = [os.path.join(vtroot, 'bin', 'vttablet'),
            '-port', '%s' % (port or self.port),
            '-tablet-path', self.zk_tablet_path,
            '-logfile', self.logfile,
            '-log.level', 'INFO',
            '-db-configs-file', self._write_db_configs_file(),
            '-debug-querylog-file', self.querylog_file]
    if auth:
      args.extend(['-auth-credentials', os.path.join(vttop, 'test', 'test_data', 'authcredentials_test.json')])

    stderr_fd = open(os.path.join(self.tablet_dir, "vttablet.stderr"), "w")
    self.proc = utils.run_bg(args, stderr=stderr_fd)
    stderr_fd.close()

    # wait for zookeeper PID just to be sure we have it
    utils.run(vtroot+'/bin/zk wait -e ' + self.zk_pid, stdout=utils.devnull)

    # wait for query service to be in the right state
    self.wait_for_vttablet_state(wait_for_state, port=port)

    return self.proc

  def wait_for_vttablet_state(self, expected, timeout=5.0, port=None):
    while True:
      v = utils.get_vars(port or self.port)
      if v == None:
        utils.debug("  vttablet not answering at /debug/vars, waiting...")
      else:
        if not 'Voltron' in v:
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

  def _write_db_configs_file(self):
    config = dict(self.default_db_config)
    if self.keyspace:
      config['app']['dbname'] = "vt_" + self.keyspace
      config['dba']['dbname'] = "vt_" + self.keyspace
      config['repl']['dbname'] = "vt_" + self.keyspace
    path = os.path.join(self.tablet_dir, 'db-configs.json')

    if self.memcached:
      for d in config.values():
        d['memcache'] = self.memcache_path

      config['memcache'] = self.memcache_path

    with open(path, 'w') as fi:
      json.dump(config, fi)

    return path

  def kill_vttablet(self):
    utils.debug("killing vttablet: " + self.zk_tablet_path)
    utils.kill_sub_process(self.proc)
    if self.memcached:
      self.kill_memcache()

  def start_memcache(self):
      self.memcache_path = os.path.join(self.tablet_dir, "memcache.sock")
      self.memcached = utils.run_bg(' '.join(["memcached", "-s", self.memcache_path]), stdout=utils.devnull)

  def kill_memcache(self):
    utils.kill_sub_process(self.memcached)
