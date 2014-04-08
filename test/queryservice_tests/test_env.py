#!/usr/bin/env python

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import contextlib
import json
import MySQLdb as mysql
import os
import shutil
import subprocess
import time
import urllib2
import uuid

from vtdb import tablet as tablet_conn
from vtdb import cursor
from vtdb import dbexceptions

import framework
import cases_framework
import environment
import tablet
import utils


class EnvironmentError(Exception):
  pass

class Querylog(object):

  def __init__(self, env):
    self.env = env
    self.id = str(uuid.uuid4())

  @property
  def path(self):
    return os.path.join(environment.vtlogroot, 'querylog' + self.id)

  @property
  def path_full(self):
    return os.path.join(environment.vtlogroot, 'querylog_full' + self.id)

  def __enter__(self):
    self.curl = utils.curl(self.env.url('/debug/querylog'), background=True, stdout=open(self.path, 'w'))
    self.curl_full = utils.curl(self.env.url('/debug/querylog?full=true'), background=True, stdout=open(self.path_full, 'w'))
    time.sleep(0.3)
    self.tailer = framework.Tailer(open(self.path), sleep=0.1)
    return self

  def __exit__(self, *args, **kwargs):
    self.curl.terminate()
    self.curl_full.terminate()
    return


class TestEnv(object):
  memcache = False
  sensitive_mode = False
  port = 0

  txlog_file = os.path.join(environment.vtlogroot, "txlog")

  @property
  def address(self):
    return "localhost:%s" % self.port

  def connect(self):
    c = tablet_conn.connect(self.address, '', 'test_keyspace', '0', 2)
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

  def url(self, path):
    return "http://localhost:%s/" % (self.port) + path

  def http_get(self, path, use_json=True):
    data = urllib2.urlopen(self.url(path)).read()
    if use_json:
      return json.loads(data)
    return data

  def debug_vars(self):
    return framework.MultiDict(self.http_get("/debug/vars"))

  def table_stats(self):
    return framework.MultiDict(self.http_get("/debug/table_stats"))

  def query_stats(self):
    return self.http_get("/debug/query_stats")

  def health(self):
    return self.http_get("/debug/health", use_json=False)

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

    with Querylog(self) as querylog:
      for case in cases:
        if isinstance(case, basestring):
          curs.execute(case)
          continue
        try:
          failures = case.run(curs, self)
        except Exception:
          print "Exception in", case
          raise
        error_count += len(failures)
        for fail in failures:
          print "FAIL:", case, fail
      error_count += self.check_streamlog(cases, open(querylog.path, 'r'))
      error_count += self.check_full_streamlog(open(querylog.path_full, 'r'))
    return error_count


class VttabletTestEnv(TestEnv):
  tablet = tablet.Tablet(62344)
  vttop = environment.vttop
  vtroot = environment.vtroot

  @property
  def port(self):
    return self.tablet.port

  def setUp(self):
    environment.topo_server_setup()

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

    utils.run_vtctl('CreateKeyspace -force test_keyspace')
    self.tablet.init_tablet('master', 'test_keyspace', '0')

    customrules = os.path.join(environment.tmproot, 'customrules.json')
    self.create_customrules(customrules)
    schema_override = os.path.join(environment.tmproot, 'schema_override.json')
    self.create_schema_override(schema_override)
    self.tablet.start_vttablet(memcache=self.memcache, customrules=customrules, schema_override=schema_override, sensitive_mode=self.sensitive_mode)

    # FIXME(szopa): This is necessary here only because of a bug that
    # makes the qs reload its config only after an action.
    utils.run_vtctl('Ping ' + self.tablet.tablet_alias)

    for i in range(30):
      try:
        self.conn = self.connect()
        self.txlogger = utils.curl(self.url('/debug/txlog'), background=True, stdout=open(self.txlog_file, 'w'))
        self.txlog = framework.Tailer(open(self.txlog_file), flush=self.tablet.flush)
        self.log = framework.Tailer(open(os.path.join(environment.vtlogroot, 'vttablet.INFO')), flush=self.tablet.flush)
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
    environment.topo_server_teardown()
    utils.kill_sub_processes()
    utils.remove_tmp_files()
    # self.tablet.remove_tree()

  def mysql_connect(self, dbname=''):
    return self.tablet.connect()

class VtoccTestEnv(TestEnv):
  tabletuid = "9460"
  port = environment.reserve_ports(1)
  mysqlport = environment.reserve_ports(1)
  mysqldir = os.path.join(environment.vtdataroot, "vt_0000009460")

  def setUp(self):
    # start mysql
    res = subprocess.call([
        environment.binary_path("mysqlctl"),
        "-tablet-uid",  self.tabletuid,
        "-port", str(self.port),
        "-mysql-port", str(self.mysqlport),
        "init"
        ])
    if res != 0:
      raise EnvironmentError("Cannot start mysql")
    res = subprocess.call([
        environment.mysql_binary_path('mysql'),
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
    with open(os.path.join(environment.vttop, "test", "test_data", "test_schema.sql")) as f:
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

    customrules = os.path.join(environment.tmproot, 'customrules.json')
    self.create_customrules(customrules)
    schema_override = os.path.join(environment.tmproot, 'schema_override.json')
    self.create_schema_override(schema_override)

    occ_args = [
      environment.binary_path('vtocc'),
      "-port", str(self.port),
      "-customrules", customrules,
      "-log_dir", environment.vtlogroot,
      "-schema-override", schema_override,
      "-db-config-app-charset", "utf8",
      "-db-config-app-dbname", "vt_test_keyspace",
      "-db-config-app-host", "localhost",
      "-db-config-app-unixsocket", self.mysqldir+"/mysql.sock",
      "-db-config-app-uname", 'vt_dba',   # use vt_dba as some tests depend on 'drop'
      "-db-config-app-keyspace", "test_keyspace",
      "-db-config-app-shard", "0",
    ]
    if self.memcache:
      memcache = self.mysqldir+"/memcache.sock"
      occ_args.extend(["-rowcache-bin", environment.memcached_bin()])
      occ_args.extend(["-rowcache-socket", memcache])
      occ_args.extend(["-enable-rowcache"])

    if self.sensitive_mode:
      occ_args.extend(['-queryserver-config-sensitive-mode'])
    self.vtocc = subprocess.Popen(occ_args, stdout=utils.devnull, stderr=utils.devnull)
    for i in range(30):
      try:
        self.conn = self.connect()
        self.txlogger = utils.curl(self.url('/debug/txlog'), background=True, stdout=open(self.txlog_file, 'w'))
        self.txlog = framework.Tailer(open(self.txlog_file, 'r'))

        def flush():
          utils.curl(self.url(environment.flush_logs_url), trap_output=True)

        self.log = framework.Tailer(open(os.path.join(environment.vtlogroot, 'vtocc.INFO')), flush=flush)
        return
      except (dbexceptions.OperationalError, dbexceptions.RetryError):
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

    # stop mysql, delete directory
    subprocess.call([
        environment.binary_path('mysqlctl'),
        "-tablet-uid",  self.tabletuid,
        "teardown", "-force"
        ])
    try:
      shutil.rmtree(self.mysqldir)
    except:
      pass

  def connect(self):
    c = tablet_conn.connect("localhost:%s" % self.port, '', 'test_keyspace', '0', 2)
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
