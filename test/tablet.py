import os
import shutil
from subprocess import check_call, Popen, CalledProcessError, PIPE
import sys
import time

import MySQLdb

import utils

vttop = os.environ['VTTOP']
vtroot = os.environ['VTROOT']

class Tablet(object):
  default_uid = 62344
  default_port = 6700
  default_mysql_port = 3700
  seq = 0

  def __init__(self, tablet_uid=None, port=None, mysql_port=None, datacenter='nj'):
    self.tablet_uid = tablet_uid or (Tablet.default_uid + Tablet.seq)
    self.port = port or (Tablet.default_port + Tablet.seq)
    self.mysql_port = mysql_port or (Tablet.default_mysql_port + Tablet.seq)
    Tablet.seq += 1

    self.datacenter = datacenter
    self.proc = None

    # filled in during init_tablet
    self.keyspace = None
    self.shard = None
    self.zk_tablet_alias = None

    # utility variables
    self.zk_tablet_path = '/zk/test_%s/vt/tablets/%010d' % (self.datacenter, self.tablet_uid)
    self.zk_pid = self.zk_tablet_path + '/pid'

  def json_vtns_addr(self):
    return {
        "uid": self.tablet_uid,
        "host": "localhost",
        "port": 0,
        "named_port_map": {
            "_mysql": self.mysql_port,
            "_vtocc": self.port,
            }
        }

  def mysqlctl(self, cmd):
    return utils.run_bg(os.path.join(vtroot, 'bin', 'mysqlctl') +
                        ' -tablet-uid %u ' % self.tablet_uid + cmd)

  def start_mysql(self):
    return self.mysqlctl('-port %u -mysql-port %u init' % (self.port, self.mysql_port))

  def teardown_mysql(self):
    return self.mysqlctl('-force teardown')

  def remove_tree(self):
    path = '/vt/vt_%010d' % self.tablet_uid
    try:
      shutil.rmtree(path)
    except OSError as e:
      if utils.options.verbose:
        print >> sys.stderr, e, path

  def connect(self, dbname=''):
    conn = MySQLdb.Connect(
        user='vt_dba',
        unix_socket='/vt/vt_%010d/mysql.sock' % self.tablet_uid,
        db=dbname)
    return conn, conn.cursor()

  # Query the MySQL instance directly
  def mquery(self, query, dbname='', write=False):
    conn, cursor = self.connect(dbname)
    if write:
      conn.begin()

    cursor.execute(query)

    if write:
      conn.commit()

    try:
      return cursor.fetchall()
    finally:
      conn.close()

  def vquery(self, query, dbname='', user=None, password=None, driver=None,
                     verbose=False):
    utils.prog_compile(['vtclient2'])
    if (user is None) != (password is None):
      raise TypeError("you should provide either both or none of user and password")

    # for ZK paths to not have // in the path, that confuses things
    if dbname.startswith('/'):
      dbname = dbname[1:]
    server = "localhost:%u/%s" % (self.port, dbname)
    if user is not None:
      server = "%s:%s@%s" % (user, password, server)

    cmdline = [vtroot+'/bin/vtclient2', '-server', server]
    if driver:
      cmdline.extend(["-driver", driver])
    if verbose:
      cmdline.append("-verbose")
    cmdline.append('"%s"' % query)

    return utils.run(' '.join(cmdline), trap_output=True)


  def assertResultCount(self, query, n, dbname=''):
    result = self.mquery(query, dbname=dbname, write=False)
    if result[0][0] != n:
      raise utils.TestError("expected %s rows in vt_insert_test" % n, result)

  def populate(self, dbname, create_sql, insert_sqls):
      self.create_db(dbname)
      self.mquery(create_sql, dbname=dbname)
      for q in insert_sqls:
        self.mquery(q, write=True, dbname=dbname)

  def create_db(self, name='vt_test_keyspace'):
    self.mquery('drop database if exists %s' % name)
    self.mquery('create database %s' % name)

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

  def init_tablet(self, keyspace='test_keyspace', shard='0', db_type='master', force=True, parent_path=None, start=False):
    self.keyspace = keyspace
    self.shard = shard
    self.zk_tablet_alias = "/zk/global/vt/keyspaces/%s/shards/%s/test_%s-%010d" % (self.keyspace, self.shard, self.datacenter, self.tablet_uid)

    if force:
      args = ['-force']
    else:
      args = []

    args.extend(['InitTablet', self.zk_tablet_path, 'localhost', str(self.mysql_port), str(self.port), keyspace, shard, db_type])

    if parent_path != None:
      args.append(parent_path)

    utils.run_vtctl(' '.join(args))
    if start:
      self.start_vttablet()

  def start_vttablet(self, port=None, auth=False):
    """
    Starts a vttablet process, and returns it.
    The process is also saved in self.proc, so it's easy to kill as well.
    """
    args = [os.path.join(vtroot, 'bin', 'vttablet'),
            '-port %s' % (port or self.port),
            '-tablet-path %s' % self.zk_tablet_path,
            '-logfile /vt/vt_%010d/vttablet.log' % self.tablet_uid]
    if auth:
      args.extend(['-auth-credentials', os.path.join(vttop, 'test', 'authcredentials_test.json')])

    self.proc = utils.run_bg(' '.join(args))
    utils.run(vtroot+'/bin/zk wait -e ' + self.zk_pid, stdout=utils.devnull)
    return self.proc

  def kill_vttablet(self):
    utils.kill_sub_process(self.proc)
