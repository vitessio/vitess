#!/usr/bin/env python

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import json
import optparse
import os
import shutil
import subprocess
import sys
import time
import urllib2
import MySQLdb as mysql

from vtdb import vt_occ2 as db
from vtdb import dbexceptions

import framework
import cases_framework
import cache_tests
import nocache_tests
import stream_tests

parser = optparse.OptionParser(usage="usage: %prog [options]")
parser.add_option("-m", "--memcache", action="store_true", default=False,
                  help="starts a memcached, and tests rowcache")
parser.add_option("-v", "--verbose", action="store_true", default=False)
parser.add_option("-t", "--testcase", action="store", default=None,
                  help="Run a single named test")
(options, args) = parser.parse_args()

LOGFILE = "/tmp/vtocc.log"
QUERYLOGFILE = "/tmp/vtocc_queries.log"

TABLETUID = "9460"
MYSQLPORT = 9460
VTOCCPORT = 9461

class TestEnv(object):
  def setUp(self):
    vttop = os.getenv("VTTOP")
    if vttop is None:
      raise Exception("VTTOP not defined")
    self.vtroot = os.getenv("VTROOT")
    if self.vtroot is None:
      raise Exception("VTROOT not defined")
    framework.execute('go build', verbose=options.verbose, cwd=vttop+'/go/cmd/vtocc')
    framework.execute('go build', verbose=options.verbose, cwd=vttop+'/go/cmd/mysqlctl')

    # start mysql
    res = subprocess.call([
        self.vtroot+"/bin/mysqlctl",
        "-tablet-uid",  TABLETUID,
        "-port", str(VTOCCPORT),
        "-mysql-port", str(MYSQLPORT),
        "init"
        ])
    if res != 0:
      raise Exception("Cannot start mysql")
    self.mysqldir = "/vt/vt_0000009460"
    res = subprocess.call([
        "mysql",
        "-S",  self.mysqldir+"/mysql.sock",
        "-u", "vt_dba",
        "-e", "create database vt_test ; set global read_only = off"])
    if res != 0:
      raise Exception("Cannot create vt_test database")
    dbconfig = self.mysqldir+"/dbconf.json"
    if options.memcache:
      memcache = self.mysqldir+"/memcache.sock"
    with open(dbconfig, 'w') as f:
      conf = {
          'charset': 'utf8',
          'dbname': 'vt_test',
          'host': 'localhost',
          'unix_socket': self.mysqldir+"/mysql.sock",
          'uname': 'vt_dba',   # use vt_dba as some tests depend on 'drop'
          }
      if options.memcache:
        conf['memcache'] = memcache
      json.dump(conf, f)

    self.mysql_conn = self.mysql_connect()
    mcu = self.mysql_conn.cursor()
    self.clean_sqls = []
    self.init_sqls = []
    clean_mode = False
    with open("test_schema.sql") as f:
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

    if options.memcache:
      self.memcached = subprocess.Popen(["memcached", "-s", memcache])
    occ_args = [
      self.vtroot+"/bin/vtocc",
      "-port", "9461",
      "-dbconfig", dbconfig,
      "-logfile", LOGFILE,
      "-querylog", QUERYLOGFILE,
    ]
    self.vtstderr = open("/tmp/vtocc_stderr.log", "a+")
    self.vtocc = subprocess.Popen(occ_args, stderr=self.vtstderr)
    for i in range(30):
      try:
        self.conn = self.connect()
        self.querylog = framework.Tailer(open(QUERYLOGFILE, "r"))
        self.log = framework.Tailer(open(LOGFILE, "r"))
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
      self.vtocc.terminate()
    if getattr(self, "vtstderr", None):
      self.vtstderr.close()
    if getattr(self, "memcached", None):
      self.memcached.terminate()

    # stop mysql, delete directory
    subprocess.call([
        self.vtroot+"/bin/mysqlctl",
        "-tablet-uid",  TABLETUID,
        "-force", "teardown"
        ])
    shutil.rmtree(self.mysqldir)

  def mysql_connect(self):
    return mysql.connect(
      host='localhost',
      user='vt_dba',
      port=MYSQLPORT,
      db='vt_test',
      unix_socket=self.mysqldir+"/mysql.sock",
      charset='utf8')

  def connect(self):
    return db.connect("localhost:9461", 2, dbname='vt_test')

  def execute(self, query, binds=None, cursorclass=None):
    if binds is None:
      binds = {}
    curs = self.conn.cursor(cursorclass=cursorclass)
    curs.execute(query, binds)
    return curs

  def debug_vars(self):
    return framework.MultiDict(json.load(urllib2.urlopen("http://localhost:9461/debug/vars")))

  def table_stats(self):
    return framework.MultiDict(json.load(urllib2.urlopen("http://localhost:9461/debug/schema/tables")))

  def check_streamlog(self, cases, log):
    error_count = 0

    for case, line in zip(cases_framework.cases_iterator(cases), log):
      log = cases_framework.Log(line)
      failures = log.check(case)
      error_count += len(failures)
      for fail in failures:
        print "FAIL:", case, fail
    return error_count

  def run_cases(self, cases):
    cursor = self.conn.cursor()
    error_count = 0
    curl = subprocess.Popen(['curl', '-s', '-N', 'http://localhost:9461/debug/vt/querylog'], stdout=open('/tmp/vtocc_streamlog.log', 'w'))
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
    error_count += self.check_streamlog(cases, open('/tmp/vtocc_streamlog.log', 'r'))
    return error_count


env = TestEnv()
try:
  env.setUp()
  try:
    t = nocache_tests.TestNocache(options.testcase, options.verbose)
    t.set_env(env)
    t.run()
  except KeyError:
    pass
  try:
    t = stream_tests.TestStream(options.testcase, options.verbose)
    t.set_env(env)
    t.run()
  except KeyError:
    pass
  if getattr(env, "memcached", None):
    print "Testing row cache"
    t = cache_tests.TestCache(options.testcase, options.verbose)
    t.set_env(env)
    t.run()
finally:
  env.tearDown()
