import time

from vtdb import vt_occ2 as db

import framework
import nocache_cases

class TestNocache(framework.TestCase):
  def test_data(self):
    cu = self.env.execute("select * from vtocc_test where intval=1")
    self.assertEqual(cu.description, [('intval', 3), ('floatval', 4), ('charval', 253), ('binval', 253)])
    self.assertEqual(cu.rowcount, 1)
    self.assertEqual(cu.fetchone(), (1, 1.12345, "\xc2\xa2", "\x00\xff"))
    cu = self.env.execute("select * from vtocc_test where intval=2")
    self.assertEqual(cu.fetchone(), (2, None, '', None))

  def test_binary(self):
    self.env.execute("begin")
    binary_data = '\x00\'\"\b\n\r\t\x1a\\\x00\x0f\xf0\xff'
    self.env.execute("insert into vtocc_test values(4, null, null, '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\\x00\x0f\xf0\xff')")
    bvar = {}
    bvar['bindata'] = binary_data
    self.env.execute("insert into vtocc_test values(5, null, null, %(bindata)s)", bvar)
    self.env.execute("commit")
    cu = self.env.execute("select * from vtocc_test where intval=4")
    self.assertEqual(cu.fetchone()[3], binary_data)
    cu = self.env.execute("select * from vtocc_test where intval=5")
    self.assertEqual(cu.fetchone()[3], binary_data)
    self.env.execute("begin")
    self.env.execute("delete from vtocc_test where intval in (4,5)")
    self.env.execute("commit")

  def test_simple_read(self):
    vstart = self.env.debug_vars()
    cu = self.env.execute("select * from vtocc_test limit 2")
    vend = self.env.debug_vars()
    self.assertEqual(cu.rowcount, 2)
    self.assertEqual(vstart.mget("Queries.TotalCount", 0)+1, vend.Queries.TotalCount)
    self.assertEqual(vstart.mget("Queries.Histograms.PASS_SELECT.Count", 0)+1, vend.Queries.Histograms.PASS_SELECT.Count)
    self.assertNotEqual(vend.Voltron.ConnPool.Size, 0)

  def test_commit(self):
    vstart = self.env.debug_vars()
    self.env.execute("begin")
    self.assertNotEqual(self.env.conn.transaction_id, 0)
    self.env.execute("insert into vtocc_test (intval, floatval, charval, binval) values(4, null, null, null)")
    self.env.execute("commit")
    cu = self.env.execute("select * from vtocc_test")
    self.assertEqual(cu.rowcount, 4)
    self.env.execute("begin")
    self.env.execute("delete from vtocc_test where intval=4")
    self.env.execute("commit")
    cu = self.env.execute("select * from vtocc_test")
    self.assertEqual(cu.rowcount, 3)
    vend = self.env.debug_vars()
    # We should have at least one connection
    self.assertNotEqual(vend.Voltron.TxPool.Size, 0)
    self.assertEqual(vstart.mget("Transactions.TotalCount", 0)+2, vend.Transactions.TotalCount)
    self.assertEqual(vstart.mget("Transactions.Histograms.Completed.Count", 0)+2, vend.Transactions.Histograms.Completed.Count)
    self.assertEqual(vstart.mget("Queries.TotalCount", 0)+4, vend.Queries.TotalCount)
    self.assertEqual(vstart.mget("Queries.Histograms.INSERT_PK.Count", 0)+1, vend.Queries.Histograms.INSERT_PK.Count)
    self.assertEqual(vstart.mget("Queries.Histograms.DML_PK.Count", 0)+1, vend.Queries.Histograms.DML_PK.Count)
    self.assertEqual(vstart.mget("Queries.Histograms.PASS_SELECT.Count", 0)+2, vend.Queries.Histograms.PASS_SELECT.Count)

  def test_integrity_error(self):
    vstart = self.env.debug_vars()
    self.env.execute("begin")
    try:
      self.env.execute("insert into vtocc_test values(1, null, null, null)")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertEqual(e[0], 1062)
      self.assertContains(e[1], "error: Duplicate")
    else:
      self.fail("Did not receive exception")
    finally:
      self.env.execute("rollback")
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("Errors.DupKey", 0)+1, vend.Errors.DupKey)

  def test_rollback(self):
    vstart = self.env.debug_vars()
    self.env.execute("begin")
    self.assertNotEqual(self.env.conn.transaction_id, 0)
    self.env.execute("insert into vtocc_test values(4, null, null, null)")
    self.env.execute("rollback")
    cu = self.env.execute("select * from vtocc_test")
    self.assertEqual(cu.rowcount, 3)
    vend = self.env.debug_vars()
    self.assertNotEqual(vend.Voltron.TxPool.Size, 0)
    self.assertEqual(vstart.mget("Transactions.TotalCount", 0)+1, vend.Transactions.TotalCount)
    self.assertEqual(vstart.mget("Transactions.Histograms.Aborted.Count", 0)+1, vend.Transactions.Histograms.Aborted.Count)

  def test_nontx_dml(self):
    vstart = self.env.debug_vars()
    try:
      self.env.execute("insert into vtocc_test values(4, null, null, null)")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: DMLs")
    else:
      self.fail("Did not receive exception")
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("Errors.Fail", 0)+1, vend.Errors.Fail)

  def test_trailing_comment(self):
    vstart = self.env.debug_vars()
    bv={}
    bv["ival"] = 1
    self.env.execute("select * from vtocc_test where intval=%(ival)s", bv)
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("Voltron.QueryCache.Length", 0)+1, vend.Voltron.QueryCache.Length)
    # This should not increase the query cache size
    self.env.execute("select * from vtocc_test where intval=%(ival)s /* trailing comment */", bv)
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("Voltron.QueryCache.Length", 0)+1, vend.Voltron.QueryCache.Length)
    # This should also not increase the query cache size
    self.env.execute("select * from vtocc_test where intval=%(ival)s /* trailing comment1 */ /* comment2 */", bv)
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("Voltron.QueryCache.Length", 0)+1, vend.Voltron.QueryCache.Length)

  def test_for_update(self):
    try:
      self.env.execute("select * from vtocc_test where intval=2 for update")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: Disallowed")
    else:
      self.fail("Did not receive exception")

    # If these throw no exceptions, we're good
    self.env.execute("begin")
    self.env.execute("select * from vtocc_test where intval=2 for update")
    self.env.execute("commit")
    # Make sure the row is not locked for read
    self.env.execute("select * from vtocc_test where intval=2")

  def test_pool_size(self):
    vstart = self.env.debug_vars()
    self.env.execute("set vt_pool_size=1")
    self.assertRaises(db.MySQLErrors.DatabaseError, self.env.execute, "select sleep(3) from dual")
    self.env.execute("select 1 from dual")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.ConnPool.Capacity, 1)
    self.assertEqual(vstart.Voltron.ConnPool.WaitCount+1, vend.Voltron.ConnPool.WaitCount)
    self.env.execute("set vt_pool_size=16")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.ConnPool.Capacity, 16)

  def test_transaction_cap(self):
    self.env.execute("set vt_transaction_cap=1")
    co2 = self.env.connect()
    self.env.execute("begin")
    try:
      cu2 = co2.cursor()
      cu2.execute("begin", {})
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "timeout")
    else:
      self.fail("Did not receive exception")
    finally:
      cu2.close()
      co2.close()
    self.env.execute("commit")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.TxPool.Capacity, 1)
    self.env.execute("set vt_transaction_cap=20")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.TxPool.Capacity, 20)

  def test_transaction_timeout(self):
    self.env.execute("set vt_transaction_timeout=0.25")
    # wait for any pending transactions to timeout
    time.sleep(0.3)
    vstart = self.env.debug_vars()
    self.env.execute("begin")
    time.sleep(0.3)
    try:
      self.env.execute("commit")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: Transaction")
    else:
      self.fail("Did not receive exception")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.ActiveTxPool.Timeout, 250000000)
    self.assertEqual(vstart.mget("Kills.Transactions", 0)+1, vend.Kills.Transactions)
    self.env.execute("set vt_transaction_timeout=30")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.ActiveTxPool.Timeout, 30000000000)

  def test_query_cache(self):
    self.env.execute("set vt_query_cache_size=1")
    bv={}
    bv["ival1"] = 1
    self.env.execute("select * from vtocc_test where intval=%(ival1)s", bv)
    bv["ival2"] = 1
    self.env.execute("select * from vtocc_test where intval=%(ival2)s", bv)
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.QueryCache.Length, 1)
    self.assertEqual(vend.Voltron.QueryCache.Size, 1)
    self.assertEqual(vend.Voltron.QueryCache.Capacity, 1)
    self.env.execute("set vt_query_cache_size=5000")
    self.env.execute("select * from vtocc_test where intval=%(ival1)s", bv)
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.QueryCache.Length, 2)
    self.assertEqual(vend.Voltron.QueryCache.Size, 2)
    self.assertEqual(vend.Voltron.QueryCache.Capacity, 5000)
    self.env.execute("select * from vtocc_test where intval=1")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.QueryCache.Length, 3)
    self.assertEqual(vend.Voltron.QueryCache.Size, 3)
    self.assertEqual(vend.Voltron.QueryCache.Capacity, 5000)

  def test_schema_reload_time(self):
    mcu = self.env.mysql_conn.cursor()
    mcu.execute("create table vtocc_temp(intval int)")
    # This should cause a reload
    self.env.execute("set vt_schema_reload_time=600")
    try:
      for i in range(10):
        try:
          self.env.execute("select * from vtocc_temp")
        except db.MySQLErrors.DatabaseError, e:
          self.assertContains(e[1], "not found in schema")
          time.sleep(1)
        else:
          break
      # Should not throw an exception
      self.env.execute("select * from vtocc_temp")
    finally:
      mcu.execute("drop table vtocc_temp")
      mcu.close()

  def test_max_result_size(self):
    self.env.execute("set vt_max_result_size=2")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.MaxResultSize, 2)
    try:
      self.env.execute("select * from vtocc_test")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: Row")
    else:
      self.fail("Did not receive exception")
    self.env.execute("set vt_max_result_size=10000")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.MaxResultSize, 10000)

  def test_query_timeout(self):
    vstart = self.env.debug_vars()
    conn = db.connect("localhost:9461", 5, dbname="vt_test_keyspace")
    cu = conn.cursor()
    self.env.execute("set vt_query_timeout=0.25")
    try:
      cu.execute("begin", {})
      cu.execute("select sleep(0.5) from vtocc_test", {})
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      if "error: Query" not in e[1] and "error: Lost connection" not in e[1]:
        self.fail("Query not killed as expected")
    else:
      self.fail("Did not receive exception")

    try:
      cu.execute("select 1 from dual", {})
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: Transaction")
    else:
      self.fail("Did not receive exception")

    cu.close()
    conn.close()

    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.ActivePool.Timeout, 250000000)
    self.assertEqual(vstart.mget("Kills.Queries", 0)+1, vend.Kills.Queries)
    self.env.execute("set vt_query_timeout=30")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.ActivePool.Timeout, 30000000000)

  def test_idle_timeout(self):
    self.env.execute("set vt_idle_timeout=1")
    time.sleep(2)
    self.env.execute("select 1 from dual")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.ConnPool.IdleTimeout, 1000000000)
    self.assertEqual(vend.Voltron.TxPool.IdleTimeout, 1000000000)
    self.env.execute("set vt_idle_timeout=1800")
    vend = self.env.debug_vars()
    self.assertEqual(vend.Voltron.ConnPool.IdleTimeout, 1800000000000)
    self.assertEqual(vend.Voltron.TxPool.IdleTimeout, 1800000000000)

  def test_consolidation(self):
    vstart = self.env.debug_vars()
    # The first call always does a full fetch for field info
    self.assertRaises(db.MySQLErrors.DatabaseError, self.env.execute, "select sleep(3) from dual")
    time.sleep(2)
    for i in range(2):
      try:
        self.env.execute("select sleep(3) from dual")
      except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError):
        pass
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("Waits.TotalCount", 0)+1, vend.Waits.TotalCount)
    self.assertEqual(vstart.mget("Waits.Histograms.Consolidations.Count", 0)+1, vend.Waits.Histograms.Consolidations.Count)

  def test_batch(self):
    queries = ["select * from vtocc_a where id = %(a)s", "select * from vtocc_b where id = %(b)s"]
    bvars = [{"a":2}, {"b":2}]
    results = self.env.conn._execute_batch(queries, bvars)
    self.assertEqual(results, [([(1L, 2L, 'bcde', 'fghi')], 1, 0, [('eid', 8), ('id', 3), ('name', 253), ('foo', 253)]), ([(1L, 2L)], 1, 0, [('eid', 8), ('id', 3)])])

  def test_bind_in_select(self):
    bv = {}
    bv['bv'] = 1
    cu = self.env.execute('select %(bv)s from vtocc_test', bv)
    self.assertEqual(cu.description, [('1', 8)])
    bv['bv'] = 'abcd'
    cu = self.env.execute('select %(bv)s from vtocc_test', bv)
    self.assertEqual(cu.description, [('abcd', 253)])

  def test_types(self):
    self._verify_mismatch("insert into vtocc_ints(tiny) values('str')")
    self._verify_mismatch("insert into vtocc_ints(tiny) values(%(str)s)", {"str": "str"})
    self._verify_mismatch("insert into vtocc_ints(tiny) values(1.2)")
    self._verify_mismatch("insert into vtocc_ints(tiny) values(%(fl)s)", {"fl": 1.2})
    self._verify_mismatch("insert into vtocc_strings(vb) values(1)")
    self._verify_mismatch("insert into vtocc_strings(vb) values(%(id)s)", {"id": 1})
    self._verify_error("insert into vtocc_strings(vb) values('12345678901234567')", None, "error: Data too long")
    self._verify_error("insert into vtocc_ints(tiny) values(-129)", None, "error: Out of range")

    try:
      self.env.execute("begin")
      self.env.execute("insert into vtocc_ints(tiny, medium) values(1, -129)")
      self.env.execute("insert into vtocc_fracts(id, num) values(1, 1)")
      self.env.execute("insert into vtocc_strings(vb) values('a')")
      self.env.execute("commit")
      self._verify_mismatch("insert into vtocc_strings(vb) select tiny from vtocc_ints")
      self._verify_mismatch("insert into vtocc_ints(tiny) select num from vtocc_fracts")
      self._verify_mismatch("insert into vtocc_ints(tiny) select vb from vtocc_strings")
      self._verify_error("insert into vtocc_ints(tiny) select medium from vtocc_ints", None, "error: Out of range")
    finally:
      self.env.execute("begin")
      self.env.execute("delete from vtocc_ints")
      self.env.execute("delete from vtocc_fracts")
      self.env.execute("delete from vtocc_strings")
      self.env.execute("commit")

  def _verify_mismatch(self, query, bindvars=None):
    self._verify_error(query, bindvars, "error: Type mismatch")

  def _verify_error(self, query, bindvars, err):
    self.env.execute("begin")
    try:
      self.env.execute(query, bindvars)
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], err)
    else:
      self.fail("Did not receive exception: " + query)
    finally:
      self.env.execute("rollback")

  def test_sqls(self):
    error_count = self.env.run_cases(nocache_cases.cases)
    if error_count != 0:
      self.fail("test_execution errors: %d"%(error_count))
