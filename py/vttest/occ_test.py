#!/usr/bin/env python

# Copyright 2012, Google Inc.
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:

#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import json
import optparse
import os
import subprocess
import sys
import time
import urllib2
import MySQLdb as mysql

from vtdb import vt_occ2 as db
from vtdb import dbexceptions

from vttest import framework
from vttest import cache_tests
from vttest import nocache_tests

parser = optparse.OptionParser(usage="usage: %prog [options]")
parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False)
parser.add_option("-t", "--testcase", action="store", dest="testcase", default=None,
    help="Run a single named test")
parser.add_option("-c", "--dbconfig", action="store", dest="dbconfig", default="dbtest.json",
    help="json db config file")
(options, args) = parser.parse_args()

LOGFILE = "/tmp/vtocc.log"
QUERYLOGFILE = "/tmp/vtocc_queries.log"

class TestEnv(object):
  def setUp(self):
    vttop = os.getenv("VTTOP")
    if vttop is None:
      raise Exception("VTTOP not defined")
    occpath = vttop+"/go/cmd/vtocc/"
    with open(options.dbconfig) as f:
      self.cfg = json.load(f)

    self.mysql_conn = self.mysql_connect(self.cfg)
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

    if self.cfg.get("memcache"):
      self.memcached = subprocess.Popen(["memcached", "-s", self.cfg["memcache"]])
    occ_args = [
      vttop+"/go/cmd/vtocc/vtocc",
      "-config", "occ.json",
      "-dbconfig", options.dbconfig,
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
    self.vtstderr.close()
    if getattr(self, "memcached", None):
      self.memcached.terminate()

  def mysql_connect(self, cfg):
    return mysql.connect(
      host=cfg.get('host', ''),
      user=cfg.get('uname', ''),
      passwd=cfg.get('pass', ''),
      port=cfg.get('port', 0),
      db=cfg.get('dbname'),
      unix_socket=cfg.get('unix_socket', ''),
      charset=cfg.get('charset', ''))

  def connect(self):
    return db.connect("localhost:9461", 2, dbname=self.cfg.get('dbname', None))

  def execute(self, query, binds=None):
    if binds is None:
      binds = {}
    curs = self.conn.cursor()
    curs.execute(query, binds)
    return curs

  def debug_vars(self):
    return framework.MultiDict(json.load(urllib2.urlopen("http://localhost:9461/debug/vars")))

  def table_stats(self):
    return framework.MultiDict(json.load(urllib2.urlopen("http://localhost:9461/debug/schema/tables")))

  def run_cases(self, cases):
    curs = self.conn.cursor()
    error_count = 0
    count = 0
    for case in cases:
      if options.verbose:
        print case[0]
      count += 1

      if len(case) == 5:
        tstart = self.table_stats()[case[4][0]]

      if len(case) == 1:
        curs.execute(case[0])
        continue
      self.querylog.reset()
      curs.execute(case[0], case[1])

      if len(case) == 2:
        continue
      if case[2] is not None:
        results = []
        for row in curs:
          results.append(row)
        if results != case[2]:
          print "Function: run_cases(%d): FAIL: %s:\n%s\n%s"%(count, case[0], case[2], results)
          error_count += 1

      if len(case) == 3:
        continue
      if case[3] is not None:
        querylog = normalizelog(self.querylog.read())
        if querylog != case[3]:
          print "Function: run_cases(%d): FAIL: %s:\n%s\n%s"%(count, case[0], case[3], querylog)
          error_count += 1

      if len(case) == 4:
        continue
      tend = self.table_stats()[case[4][0]]
      if tstart["Hits"]+case[4][1] != tend["Hits"]:
        print "Function: run_cases(%d): FAIL: %s:\nHits: %s!=%s"%(count, case[0], tstart["Hits"]+case[4][1], tend["Hits"])
        error_count += 1
      if tstart["Absent"]+case[4][2] != tend["Absent"]:
        print "Function: run_cases(%d): FAIL: %s:\nAbsent: %s!=%s"%(count, case[0], tstart["Absent"]+case[4][2], tend["Absent"])
        error_count += 1
      if tstart["Misses"]+case[4][3] != tend["Misses"]:
        print "Function: run_cases(%d): FAIL: %s:\nMisses: %s!=%s"%(count, case[0], tstart["Misses"]+case[4][3], tend["Misses"])
        error_count += 1
      if tstart["Invalidations"]+case[4][4] != tend["Invalidations"]:
        print "Function: run_cases(%d): FAIL: %s:\nInvalidations: %s!=%s"%(count, case[0], tstart["Invalidations"]+case[4][4], tend["Invalidations"])
        error_count += 1
    return error_count

def normalizelog(data):
  lines = data.split("\n")
  newlines = []
  for line in lines:
    pos = line.find("INFO: ")
    if pos >= 0:
      newlines.append(line[pos+6:])
  return newlines

env = TestEnv()
try:
  env.setUp()
  try:
    t = nocache_tests.TestNocache(options.testcase, options.verbose)
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
