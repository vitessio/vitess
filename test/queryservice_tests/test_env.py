#!/usr/bin/env python

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import json
import os
import shutil
import subprocess
import time
import urllib2
import MySQLdb as mysql

from vtdb import tablet as tablet_conn
from vtdb import cursor
from vtdb import dbexceptions

import framework
import cases_framework
import tablet
import utils


class EnvironmentError(Exception):
  pass


class TestEnv(object):

  def connect(self):
    c = tablet_conn.connect("localhost:%s" % self.tablet.port, 'test_keyspace', '0', 2)
    c.max_attempts = 1
    return c

  def execute(self, query, binds=None, cursorclass=None):
    if binds is None:
      binds = {}
    curs = cursor.TabletCursor(self.conn)
    try:
      curs.execute(query, binds)
    except dbexceptions.OperationalError:
      self.conn = self.connect()
      raise
    return curs

  def debug_vars(self):
    return framework.MultiDict(json.load(urllib2.urlopen("http://localhost:9461/debug/vars")))

  def table_stats(self):
    return framework.MultiDict(json.load(urllib2.urlopen("http://localhost:9461/debug/table_stats")))

  def query_stats(self):
    return json.load(urllib2.urlopen("http://localhost:9461/debug/query_stats"))

  def health(self):
    return urllib2.urlopen("http://localhost:9461/debug/health").read()

  def check_streamlog(self, cases, log):
    error_count = 0

    for case, line in zip(cases_framework.cases_iterator(cases), log):
      log = cases_framework.Log(line)
      failures = log.check(case)
      error_count += len(failures)
      for fail in failures:
        print "FAIL:", case, fail
    return error_count

  def check_full_streamlog(self, fi):
    # FIXME(szopa): better test?
    for line in fi:
      if '"name":"bytes 4"' in line:
        print "FAIL: full streamlog doesn't contain all bind variables."
        return 1
    return 0

  def create_customrules(self, filename):
    with open(filename, "w") as f:
      f.write("""[{
        "Name": "r1",
        "Description": "disallow bindvar 'asdfg'",
        "BindVarConds":[{
          "Name": "asdfg",
          "OnAbsent": false,
          "Operator": "NOOP"
        }]
      }]""")

  def create_schema_override(self, filename):
    with open(filename, "w") as f:
      f.write("""[{
        "Name": "vtocc_view",
        "PKColumns": ["key2"],
        "Cache": {
          "Type": "RW",
          "Prefix": "view1"
        }
      }, {
        "Name": "vtocc_part1",
        "PKColumns": ["key2"],
        "Cache": {
          "Type": "W",
          "Table": "vtocc_view"
        }
      }, {
        "Name": "vtocc_part2",
        "PKColumns": ["key3"],
        "Cache": {
          "Type": "W",
          "Table": "vtocc_view"
        }
      }]""")

  def run_cases(self, cases):
    curs = cursor.TabletCursor(self.conn)
    error_count = 0
    curl = subprocess.Popen(['curl', '-s', '-N', 'http://localhost:9461/debug/querylog'], stdout=open('/tmp/vtocc_streamlog.log', 'w'))
    curl_full = subprocess.Popen(['curl', '-s', '-N', 'http://localhost:9461/debug/querylog?full=true'], stdout=open('/tmp/vtocc_streamlog_full.log', 'w'))
    time.sleep(1)
    for case in cases:
      if isinstance(case, basestring):
        curs.execute(case)
        continue
      try:
        failures = case.run(curs, self.querylog)
      except Exception:
        print "Exception in", case
        raise
      error_count += len(failures)
      for fail in failures:
        print "FAIL:", case, fail
    curl.terminate()
    curl_full.terminate()
    error_count += self.check_streamlog(cases, open('/tmp/vtocc_streamlog.log', 'r'))
    error_count += self.check_full_streamlog(open('/tmp/vtocc_streamlog_full.log', 'r'))
    return error_count


class VttabletTestEnv(TestEnv):
  tablet = tablet.Tablet(62344, 9461, 9460)
  vttop = os.getenv("VTTOP")
  vtroot = os.getenv("VTROOT")

  def setUp(self):
    utils.zk_setup()
    utils.setup()
    if self.vttop is None:
      raise EnvironmentError("VTTOP not defined")
    if self.vtroot is None:
      raise EnvironmentError("VTROOT not defined")

    framework.execute('go install', verbose=utils.options.verbose, cwd=self.vttop+'/go/cmd/mysqlctl')

    utils.wait_procs([self.tablet.init_mysql()])
    self.tablet.mquery("", ["create database vt_test_keyspace", "set global read_only = off"])

    self.mysql_conn, mcu = self.tablet.connect('vt_test_keyspace')
    self.clean_sqls = []
    self.init_sqls = []
    clean_mode = False
    with open(os.path.join(self.vttop, "test", "test_data", "test_schema.sql")) as f:
      for line in f:
        line = line.rstrip()
        if line == "# clean":
          clean_mode = True
        if line=='' or line.startswith("#"):
          continue
        if clean_mode:
          self.clean_sqls.append(line)
        else:
          self.init_sqls.append(line)
    try:
      for line in self.init_sqls:
        mcu.execute(line, {})
    finally:
      mcu.close()

    utils.run_vtctl('CreateKeyspace -force /zk/global/vt/keyspaces/test_keyspace')
    self.tablet.init_tablet('master', 'test_keyspace', '0')

    customrules = '/tmp/customrules.json'
    self.create_customrules(customrules)
    schema_override = '/tmp/schema_override.json'
    self.create_schema_override(schema_override)
    if utils.options.memcache:
      self.tablet.start_vttablet(memcache=True, customrules=customrules, schema_override=schema_override)
    else:
      self.tablet.start_vttablet(customrules=customrules, schema_override=schema_override)

    # FIXME(szopa): This is necessary here only because of a bug that
    # makes the qs reload its config only after an action.
    utils.run_vtctl('Ping ' + self.tablet.zk_tablet_path)

    for i in range(30):
      try:
        self.conn = self.connect()
        self.txlogger = subprocess.Popen(['curl', '-s', '-N', 'http://localhost:9461/debug/txlog'], stdout=open('/tmp/vtocc_txlog.log', 'w'))
        self.txlog = framework.Tailer(open('/tmp/vtocc_txlog.log'), flush=self.tablet.flush)
        self.log = framework.Tailer(open(os.path.join(self.tablet.tablet_dir, 'vttablet.INFO')), flush=self.tablet.flush)
        querylog_file = '/tmp/vtocc_streamlog_%s.log' % self.tablet.port
        utils.run_bg(['curl', '-s', '-N', 'http://localhost:9461/debug/querylog?full=true'], stdout=open(querylog_file, 'w'))
        time.sleep(1)
        self.querylog = framework.Tailer(open(querylog_file), sleep=0.1)

        return
      except dbexceptions.OperationalError:
        if i == 29:
          raise
        time.sleep(1)

  def tearDown(self):
    self.tablet.kill_vttablet()
    try:
      utils.wait_procs([self.tablet.teardown_mysql()])
    except:
      # FIXME: remove
      pass
    if getattr(self, "txlogger", None):
      self.txlogger.terminate()
    utils.zk_teardown()
    utils.kill_sub_processes()
    utils.remove_tmp_files()
    # self.tablet.remove_tree()

  def mysql_connect(self, dbname=''):
    return self.tablet.connect()

class VtoccTestEnv(TestEnv):
  tabletuid = "9460"
  vtoccport = 9461
  mysqlport = 9460
  vttop = os.getenv("VTTOP")
  vtroot = os.getenv("VTROOT")
  vtdataroot = os.getenv("VTDATAROOT") or "/vt"
  mysqldir = os.path.join(vtdataroot, "vt_0000009460")

  def setUp(self):
    if self.vttop is None:
      raise EnvironmentError("VTTOP not defined")
    if self.vtroot is None:
      raise EnvironmentError("VTROOT not defined")
    try:
      os.makedirs(utils.tmp_root)
    except OSError:
      pass

    utils.setup()

    framework.execute('go install', verbose=utils.options.verbose, cwd=self.vttop+'/go/cmd/vtocc')
    framework.execute('go install', verbose=utils.options.verbose, cwd=self.vttop+'/go/cmd/mysqlctl')

    # start mysql
    res = subprocess.call([
        self.vtroot+"/bin/mysqlctl",
        "-tablet-uid",  self.tabletuid,
        "-port", str(self.vtoccport),
        "-mysql-port", str(self.mysqlport),
        "init"
        ])
    if res != 0:
      raise EnvironmentError("Cannot start mysql")
    res = subprocess.call([
        "mysql",
        "-S",  self.mysqldir+"/mysql.sock",
        "-u", "vt_dba",
        "-e", "create database vt_test_keyspace ; set global read_only = off"])
    if res != 0:
      raise Exception("Cannot create vt_test_keyspace database")

    self.mysql_conn = self.mysql_connect()
    mcu = self.mysql_conn.cursor()
    self.clean_sqls = []
    self.init_sqls = []
    clean_mode = False
    with open(os.path.join(self.vttop, "test", "test_data", "test_schema.sql")) as f:
      for line in f:
        line = line.rstrip()
        if line == "# clean":
          clean_mode = True
        if line=='' or line.startswith("#"):
          continue
        if clean_mode:
          self.clean_sqls.append(line)
        else:
          self.init_sqls.append(line)
    try:
      for line in self.init_sqls:
        mcu.execute(line, {})
    finally:
      mcu.close()

    customrules = '/tmp/customrules.json'
    self.create_customrules(customrules)
    schema_override = '/tmp/schema_override.json'
    self.create_schema_override(schema_override)

    occ_args = [
      self.vtroot+"/bin/vtocc",
      "-port", "9461",
      "-customrules", customrules,
      "-schema-override", schema_override,
      "-db-config-app-charset", "utf8",
      "-db-config-app-dbname", "vt_test_keyspace",
      "-db-config-app-host", "localhost",
      "-db-config-app-unixsocket", self.mysqldir+"/mysql.sock",
      "-db-config-app-uname", 'vt_dba',   # use vt_dba as some tests depend on 'drop'
      "-db-config-app-keyspace", "test_keyspace",
      "-db-config-app-shard", "0"
    ]
    if utils.options.memcache:
      memcache = self.mysqldir+"/memcache.sock"
      occ_args.extend(["-rowcache-bin", "memcached"])
      occ_args.extend(["-rowcache-socket", memcache])

    self.vtstderr = open("/tmp/vtocc_stderr.log", "a+")
    self.vtstdout = open("/tmp/vtocc_stdout.log", "a+")
    self.vtocc = subprocess.Popen(occ_args, stdout=self.vtstdout, stderr=self.vtstderr)
    for i in range(30):
      try:
        self.conn = self.connect()
        self.txlogger = subprocess.Popen(['curl', '-s', '-N', 'http://localhost:9461/debug/txlog'], stdout=open('/tmp/vtocc_txlog.log', 'w'))
        self.txlog = framework.Tailer(open('/tmp/vtocc_txlog.log', 'r'))

        def flush():
          utils.run(['curl', '-s', '-N', 'http://localhost:9461/debug/flushlogs'], trap_output=True)

        self.log = framework.Tailer(open('/tmp/vtocc.INFO'), flush=flush)
        utils.run_bg(['curl', '-s', '-N', 'http://localhost:9461/debug/querylog?full=true'], stdout=open('/tmp/vtocc_streamlog_9461.log', 'w'))
        self.querylog = framework.Tailer(open('/tmp/vtocc_streamlog_9461.log'), sleep=0.1)
        return
      except dbexceptions.OperationalError:
        if i == 29:
          raise
        time.sleep(1)

  def tearDown(self):
    try:
      mcu = self.mysql_conn.cursor()
      for line in self.clean_sqls:
        try:
          mcu.execute(line, {})
        except:
          pass
      mcu.close()
    except:
      pass
    if getattr(self, "txlogger", None):
      self.txlogger.terminate()
    if getattr(self, "vtocc", None):
      self.vtocc.terminate()
    if getattr(self, "vtstderr", None):
      self.vtstderr.close()
    if getattr(self, "vtstdout", None):
      self.vtstdout.close()

    # stop mysql, delete directory
    subprocess.call([
        self.vtroot+"/bin/mysqlctl",
        "-tablet-uid",  self.tabletuid,
        "teardown", "-force"
        ])
    shutil.rmtree(self.mysqldir)

  def connect(self):
    c = tablet_conn.connect("localhost:9461", 'test_keyspace', '0', 2)
    c.max_attempts = 1
    return c

  def mysql_connect(self):
    return mysql.connect(
      host='localhost',
      user='vt_dba',
      port=self.mysqlport,
      db='vt_test_keyspace',
      unix_socket=self.mysqldir+"/mysql.sock",
      charset='utf8')
