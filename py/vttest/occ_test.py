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
from vttest import framework
from vttest import exec_cases
from vtdb import vt_occ2 as db
from vtdb import dbexceptions

parser = optparse.OptionParser(usage="usage: %prog [options]")
parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False)
parser.add_option("-t", "--testcase", action="store", dest="testcase", default=None,
    help="Run a single named test")
parser.add_option("-c", "--dbconfig", action="store", dest="dbconfig", default="dbtest.json",
    help="json db config file")
(options, args) = parser.parse_args()

QUERYLOGFILE = "/tmp/vtocc_queries.log"

class TestVtocc(framework.TestCase):
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

    occ_args = [
      vttop+"/go/cmd/vtocc/vtocc",
      "-config", "occ.json",
      "-dbconfig", options.dbconfig,
      "-logfile", "/tmp/vtocc.log",
      "-querylog", QUERYLOGFILE,
    ]
    with open("/tmp/vtocc_stderr.log", "a+") as vtstderr:
      self.vtocc = subprocess.Popen(occ_args, stderr=vtstderr)
    for i in range(30):
      try:
        self.conn = self.connect()
        self.querylog = framework.Tailer(open(QUERYLOGFILE, "r"))
        return
      except dbexceptions.OperationalError:
        time.sleep(1)
    self.conn = self.connect()
    self.querylog = framework.Tailer(open(QUERYLOGFILE, "r"))

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

  def test_data(self):
    cu = self.execute("select * from vtocc_test where intval=1")
    self.assertEqual(cu.description, [('intval', 3), ('floatval', 4), ('charval', 253), ('binval', 253)])
    self.assertEqual(cu.rowcount, 1)
    self.assertEqual(cu.fetchone(), (1, 1.12345, "\xc2\xa2", "\x00\xff"))
    cu = self.execute("select * from vtocc_test where intval=2")
    self.assertEqual(cu.fetchone(), (2, None, '', None))

  def test_binary(self):
    self.execute("begin")
    binary_data = '\x00\'\"\b\n\r\t\x1a\\\x00\x0f\xf0\xff'
    self.execute("insert into vtocc_test values(4, null, null, '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\\x00\x0f\xf0\xff')")
    bvar = {}
    bvar['bindata'] = binary_data
    self.execute("insert into vtocc_test values(5, null, null, %(bindata)s)", bvar)
    self.execute("commit")
    cu = self.execute("select * from vtocc_test where intval=4")
    self.assertEqual(cu.fetchone()[3], binary_data)
    cu = self.execute("select * from vtocc_test where intval=5")
    self.assertEqual(cu.fetchone()[3], binary_data)
    self.execute("begin")
    self.execute("delete from vtocc_test where intval in (4,5)")
    self.execute("commit")

  def test_simple_read(self):
    vstart = self.debug_vars()
    cu = self.execute("select * from vtocc_test limit 2")
    vend = self.debug_vars()
    self.assertEqual(cu.rowcount, 2)
    self.assertEqual(vstart.mget("Queries.TotalCount", 0)+1, vend.Queries.TotalCount)
    self.assertEqual(vstart.mget("Queries.Histograms.PASS_SELECT.Count", 0)+1, vend.Queries.Histograms.PASS_SELECT.Count)
    self.assertNotEqual(vend.Voltron.ConnPool.Size, 0)

  def test_commit(self):
    vstart = self.debug_vars()
    self.execute("begin")
    self.assertNotEqual(self.conn.transaction_id, 0)
    self.execute("insert into vtocc_test (intval, floatval, charval, binval) values(4, null, null, null)")
    self.execute("commit")
    cu = self.execute("select * from vtocc_test")
    self.assertEqual(cu.rowcount, 4)
    self.execute("begin")
    self.execute("delete from vtocc_test where intval=4")
    self.execute("commit")
    cu = self.execute("select * from vtocc_test")
    self.assertEqual(cu.rowcount, 3)
    vend = self.debug_vars()
    # We should have at least one connection
    self.assertNotEqual(vend.Voltron.TxPool.Size, 0)
    self.assertEqual(vstart.mget("Transactions.TotalCount", 0)+2, vend.Transactions.TotalCount)
    self.assertEqual(vstart.mget("Transactions.Histograms.Completed.Count", 0)+2, vend.Transactions.Histograms.Completed.Count)
    self.assertEqual(vstart.mget("Queries.TotalCount", 0)+4, vend.Queries.TotalCount)
    self.assertEqual(vstart.mget("Queries.Histograms.PLAN_INSERT_PK.Count", 0)+1, vend.Queries.Histograms.PLAN_INSERT_PK.Count)
    self.assertEqual(vstart.mget("Queries.Histograms.DML_PK.Count", 0)+1, vend.Queries.Histograms.DML_PK.Count)
    self.assertEqual(vstart.mget("Queries.Histograms.PASS_SELECT.Count", 0)+2, vend.Queries.Histograms.PASS_SELECT.Count)

  def test_integrity_error(self):
    vstart = self.debug_vars()
    self.execute("begin")
    try:
      self.execute("insert into vtocc_test values(1, null, null, null)")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertEqual(e[0], 1062)
      self.assertContains(e[1], "error: Duplicate")
    else:
      self.assertFail("Did not receive exception")
    finally:
      self.execute("rollback")
    vend = self.debug_vars()
    self.assertEqual(vstart.mget("Errors.DupKey", 0)+1, vend.Errors.DupKey)

  def test_rollback(self):
    vstart = self.debug_vars()
    self.execute("begin")
    self.assertNotEqual(self.conn.transaction_id, 0)
    self.execute("insert into vtocc_test values(4, null, null, null)")
    self.execute("rollback")
    cu = self.execute("select * from vtocc_test")
    self.assertEqual(cu.rowcount, 3)
    vend = self.debug_vars()
    self.assertNotEqual(vend.Voltron.TxPool.Size, 0)
    self.assertEqual(vstart.mget("Transactions.TotalCount", 0)+1, vend.Transactions.TotalCount)
    self.assertEqual(vstart.mget("Transactions.Histograms.Aborted.Count", 0)+1, vend.Transactions.Histograms.Aborted.Count)

  def test_nontx_dml(self):
    vstart = self.debug_vars()
    try:
      self.execute("insert into vtocc_test values(4, null, null, null)")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: DMLs")
    else:
      self.assertFail("Did not receive exception")
    vend = self.debug_vars()
    self.assertEqual(vstart.mget("Errors.Fail", 0)+1, vend.Errors.Fail)

  def test_trailing_comment(self):
    vstart = self.debug_vars()
    bv={}
    bv["ival"] = 1
    self.execute("select * from vtocc_test where intval=%(ival)s", bv)
    vend = self.debug_vars()
    self.assertEqual(vstart.mget("Voltron.QueryCache.Length", 0)+1, vend.Voltron.QueryCache.Length)
    # This should not increase the query cache size
    self.execute("select * from vtocc_test where intval=%(ival)s /* trailing comment */", bv)
    vend = self.debug_vars()
    self.assertEqual(vstart.mget("Voltron.QueryCache.Length", 0)+1, vend.Voltron.QueryCache.Length)

  def test_for_update(self):
    try:
      self.execute("select * from vtocc_test where intval=2 for update")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: Disallowed")
    else:
      self.assertFail("Did not receive exception")

    # If these throw no exceptions, we're good
    self.execute("begin")
    self.execute("select * from vtocc_test where intval=2 for update")
    self.execute("commit")
    # Make sure the row is not locked for read
    self.execute("select * from vtocc_test where intval=2")

  def test_pool_size(self):
    vstart = self.debug_vars()
    self.execute("set vt_pool_size=1")
    try:
      self.execute("select sleep(3) from dual")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError):
      pass
    else:
      self.assertFail("Did not receive exception")
    self.execute("select 1 from dual")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.ConnPool.Capacity, 1)
    self.assertEqual(vstart.Voltron.ConnPool.WaitCount+1, vend.Voltron.ConnPool.WaitCount)
    self.execute("set vt_pool_size=16")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.ConnPool.Capacity, 16)

  def test_transaction_cap(self):
    vstart = self.debug_vars()
    self.execute("set vt_transaction_cap=1")
    co2 = self.connect()
    self.execute("begin")
    try:
      cu2 = co2.cursor()
      cu2.execute("begin", {})
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: Transaction")
    else:
      self.assertFail("Did not receive exception")
    finally:
      cu2.close()
      co2.close()
    self.execute("commit")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.TxPool.Capacity, 1)
    self.execute("set vt_transaction_cap=20")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.TxPool.Capacity, 20)

  def test_transaction_timeout(self):
    vstart = self.debug_vars()
    self.execute("set vt_transaction_timeout=1")
    self.execute("begin")
    time.sleep(2)
    try:
      self.execute("commit")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: Transaction")
    else:
      self.assertFail("Did not receive exception")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.ActiveTxPool.Timeout, 1)
    self.assertEqual(vstart.mget("Kills.Transactions", 0)+1, vend.Kills.Transactions)
    self.execute("set vt_transaction_timeout=30")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.ActiveTxPool.Timeout, 30)

  def test_query_cache(self):
    vstart = self.debug_vars()
    self.execute("set vt_query_cache_size=1")
    bv={}
    bv["ival1"] = 1
    self.execute("select * from vtocc_test where intval=%(ival1)s", bv)
    bv["ival2"] = 1
    self.execute("select * from vtocc_test where intval=%(ival2)s", bv)
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.QueryCache.Length, 1)
    self.assertEqual(vend.Voltron.QueryCache.Size, 1)
    self.assertEqual(vend.Voltron.QueryCache.Capacity, 1)
    self.execute("set vt_query_cache_size=5000")
    self.execute("select * from vtocc_test where intval=%(ival1)s", bv)
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.QueryCache.Length, 2)
    self.assertEqual(vend.Voltron.QueryCache.Size, 2)
    self.assertEqual(vend.Voltron.QueryCache.Capacity, 5000)

  def test_schema_reload_time(self):
    mcu = self.mysql_conn.cursor()
    mcu.execute("create table vtocc_temp(intval int)")
    self.execute("set vt_schema_reload_time=1")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.SchemaReloadTime, 1)
    # This should not throw an exception
    try:
      for i in range(10):
        try:
          self.execute("select * from vtocc_temp")
          self.execute("set vt_schema_reload_time=600")
          vend = self.debug_vars()
          self.assertEqual(vend.Voltron.SchemaReloadTime, 600)
          break
        except db.MySQLErrors.DatabaseError, e:
          self.assertContains(e[1], "not found in schema")
          time.sleep(1)
    finally:
      mcu.execute("drop table vtocc_temp")
      mcu.close()

  def test_max_result_size(self):
    self.execute("set vt_max_result_size=2")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.MaxResultSize, 2)
    try:
      self.execute("select * from vtocc_test")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: Row")
    else:
      self.assertFail("Did not receive exception")
    self.execute("set vt_max_result_size=10000")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.MaxResultSize, 10000)

  def test_query_timeout(self):
    vstart = self.debug_vars()
    conn = db.connect("localhost:9461", 5, dbname=self.cfg['dbname'])
    cu = conn.cursor()
    self.execute("set vt_query_timeout=1")
    try:
      cu.execute("begin", {})
      cu.execute("select sleep(2) from vtocc_test", {})
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: Query")
    else:
      self.assertFail("Did not receive exception")

    try:
      cu.execute("select 1 from dual", {})
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: Transaction")
    else:
      self.assertFail("Did not receive exception")

    try:
      cu.close()
      conn.close()
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(str(e), "error: Transaction")
    else:
      self.assertFail("Did not receive exception")

    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.ActivePool.Timeout, 1)
    self.assertEqual(vstart.mget("Kills.Queries", 0)+1, vend.Kills.Queries)
    self.execute("set vt_query_timeout=30")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.ActivePool.Timeout, 30)

  def test_idle_timeout(self):
    vstart = self.debug_vars()
    self.execute("set vt_idle_timeout=1")
    time.sleep(2)
    self.execute("select 1 from dual")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.ConnPool.IdleTimeout, 1)
    self.assertEqual(vend.Voltron.TxPool.IdleTimeout, 1)
    self.execute("set vt_idle_timeout=1800")
    vend = self.debug_vars()
    self.assertEqual(vend.Voltron.ConnPool.IdleTimeout, 1800)
    self.assertEqual(vend.Voltron.TxPool.IdleTimeout, 1800)

  def test_consolidation(self):
    vstart = self.debug_vars()
    for i in range(2):
      try:
        self.execute("select sleep(3) from dual")
      except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError):
        pass
    vend = self.debug_vars()
    self.assertEqual(vstart.mget("Waits.TotalCount", 0)+1, vend.Waits.TotalCount)
    self.assertEqual(vstart.mget("Waits.Histograms.Consolidations.Count", 0)+1, vend.Waits.Histograms.Consolidations.Count)

  def test_execution(self):
    curs = self.conn.cursor()
    error_count = 0
    for case in exec_cases.exec_cases:
      if len(case) == 1:
        curs.execute(case[0])
        continue
      self.querylog.reset()
      curs.execute(case[0], case[1])
      if len(case) == 2:
        continue
      results = []
      for row in curs:
        results.append(row)
      if results != case[2]:
        print "Function: test_execution: FAIL: %s:\n%s\n%s"%(case[0], case[2], results)
        error_count += 1
      if len(case) == 3:
        continue
      querylog = normalizelog(self.querylog.read())
      if querylog != case[3]:
        print "Function: test_execution: FAIL: %s:\n%s\n%s"%(case[0], case[3], querylog)
        error_count += 1
    if error_count != 0:
      self.assertFail("test_execution errors: %d"%(error_count))

def normalizelog(data):
  lines = data.split("\n")
  newlines = []
  for line in lines:
    pos = line.find("INFO: ")
    if pos >= 0:
      newlines.append(line[pos+6:])
  return newlines

t = TestVtocc(options.testcase, options.verbose)
t.run()
