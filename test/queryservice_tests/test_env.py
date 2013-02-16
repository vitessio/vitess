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

from vtdb import vt_occ2 as db
from vtdb import dbexceptions

import framework
import cases_framework
import tablet
import utils


class EnvironmentError(Exception): 
  pass


class TestEnv(object):

  def connect(self):
    c = db.connect("localhost:%s" % self.tablet.port, 2, dbname='vt_test_keyspace')
    c.max_attempts = 1
    return c

  def execute(self, query, binds=None, cursorclass=None):
    if binds is None:
      binds = {}
    curs = self.conn.cursor(cursorclass=cursorclass)
    try:
      curs.execute(query, binds)
    except mysql.OperationalError:
      self.conn = self.connect()
      raise
    return curs

  def debug_vars(self):
    return framework.MultiDict(json.load(urllib2.urlopen("http://localhost:9461/debug/vars")))

  def table_stats(self):
    return framework.MultiDict(json.load(urllib2.urlopen("http://localhost:9461/debug/schema/tables")))

  def query_stats(self):
    return json.load(urllib2.urlopen("http://localhost:9461/debug/schema/query_stats"))

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

  def run_cases(self, cases):
    cursor = self.conn.cursor()
    error_count = 0
    curl = subprocess.Popen(['curl', '-s', '-N', 'http://localhost:9461/debug/vt/querylog'], stdout=open('/tmp/vtocc_streamlog.log', 'w'))
    curl_full = subprocess.Popen(['curl', '-s', '-N', 'http://localhost:9461/debug/vt/querylog?full=true'], stdout=open('/tmp/vtocc_streamlog_full.log', 'w'))
    time.sleep(1)
    for case in cases:
      if isinstance(case, basestring):
        cursor.execute(case)
        continue
      try:
        failures = case.run(cursor, self.querylog)
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
    if self.vttop is None:
      raise EnvironmentError("VTTOP not defined")
    if self.vtroot is None:
      raise EnvironmentError("VTROOT not defined")

    framework.execute('go build', verbose=utils.options.verbose, cwd=self.vttop+'/go/cmd/mysqlctl')

    utils.wait_procs([self.tablet.start_mysql()])
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

    if utils.options.memcache:
      self.tablet.start_vttablet(memcache=True)
    else:
      self.tablet.start_vttablet()

    # FIXME(szopa): This is necessary here only because of a bug that
    # makes the qs reload its config only after an action.
    utils.run_vtctl('Ping ' + self.tablet.zk_tablet_path)

    for i in range(30):
      try:
        self.conn = self.connect()
        self.querylog = framework.Tailer(open(self.tablet.querylog_file, "r"))
        self.log = framework.Tailer(open(self.tablet.logfile, "r"))
        return
      except dbexceptions.OperationalError:
        if i == 29:
          raise
        time.sleep(1)

  def tearDown(self):
    try:
      self.tablet.kill_vttablet()
    except AttributeError:
      print "Not killing vttablet - it wasn't running."
    try:
      utils.wait_procs([self.tablet.teardown_mysql()])
    except:
      # FIXME: remove
      pass
    utils.zk_teardown()
    utils.kill_sub_processes()
    utils.remove_tmp_files()
    # self.tablet.remove_tree()
    
    if getattr(self, "memcached", None):
      self.memcached.terminate()

  def mysql_connect(self, dbname=''):
    return self.tablet.connect()

class VtoccTestEnv(TestEnv):
  logfile = "/tmp/vtocc.log"
  querylogfile = "/tmp/vtocc_queries.log"
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

    framework.execute('go build', verbose=utils.options.verbose, cwd=self.vttop+'/go/cmd/vtocc')
    framework.execute('go build', verbose=utils.options.verbose, cwd=self.vttop+'/go/cmd/mysqlctl')

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
    dbconfig = self.mysqldir+"/dbconf.json"
    if utils.options.memcache:
      memcache = self.mysqldir+"/memcache.sock"
    with open(dbconfig, 'w') as f:
      conf = {
          'charset': 'utf8',
          'dbname': 'vt_test_keyspace',
          'host': 'localhost',
          'unix_socket': self.mysqldir+"/mysql.sock",
          'uname': 'vt_dba',   # use vt_dba as some tests depend on 'drop'
          }
      if utils.options.memcache:
        conf['memcache'] = memcache
      json.dump(conf, f)

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

    if utils.options.memcache:
      self.memcached = subprocess.Popen(["memcached", "-s", memcache])
    occ_args = [
      self.vtroot+"/bin/vtocc",
      "-port", "9461",
      "-dbconfig", dbconfig,
      "-logfile", self.logfile,
      "-querylog", self.querylogfile,
    ]
    self.vtstderr = open("/tmp/vtocc_stderr.log", "a+")
    self.vtocc = subprocess.Popen(occ_args, stderr=self.vtstderr)
    for i in range(30):
      try:
        self.conn = self.connect()
        self.querylog = framework.Tailer(open(self.querylogfile, "r"))
        self.log = framework.Tailer(open(self.logfile, "r"))
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
    if getattr(self, "vtocc", None):
      self.vtocc.kill()
    if getattr(self, "vtstderr", None):
      self.vtstderr.close()
    if getattr(self, "memcached", None):
      self.memcached.terminate()

    # stop mysql, delete directory
    subprocess.call([
        self.vtroot+"/bin/mysqlctl",
        "-tablet-uid",  self.tabletuid,
        "teardown", "-force"
        ])
    shutil.rmtree(self.mysqldir)

  def connect(self):
    c = db.connect("localhost:9461", 2, dbname='vt_test_keyspace')
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
