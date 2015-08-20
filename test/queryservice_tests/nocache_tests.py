import time

from vtdb import field_types
from vtdb import dbexceptions
from vtdb import tablet as tablet_conn
from vtdb import cursor

import framework
import nocache_cases
import environment
import utils

class TestNocache(framework.TestCase):
  def test_data(self):
    cu = self.env.execute("select * from vtocc_test where intval=1")
    self.assertEqual(cu.description, [('intval', 3), ('floatval', 4), ('charval', 253), ('binval', 253)])
    self.assertEqual(cu.rowcount, 1)
    self.assertEqual(cu.fetchone(), (1, 1.12345, "\xc2\xa2", "\x00\xff"))
    cu = self.env.execute("select * from vtocc_test where intval=2")
    self.assertEqual(cu.fetchone(), (2, None, '', None))

  def test_binary(self):
    self.env.conn.begin()
    binary_data = '\x00\'\"\b\n\r\t\x1a\\\x00\x0f\xf0\xff'
    self.env.execute("insert into vtocc_test values(4, null, null, '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\\x00\x0f\xf0\xff')")
    bvar = {'bindata': binary_data}
    self.env.execute("insert into vtocc_test values(5, null, null, :bindata)", bvar)
    self.env.conn.commit()
    cu = self.env.execute("select * from vtocc_test where intval=4")
    self.assertEqual(cu.fetchone()[3], binary_data)
    cu = self.env.execute("select * from vtocc_test where intval=5")
    self.assertEqual(cu.fetchone()[3], binary_data)
    self.env.conn.begin()
    self.env.execute("delete from vtocc_test where intval in (4,5)")
    self.env.conn.commit()

  def test_simple_read(self):
    vstart = self.env.debug_vars()
    cu = self.env.execute("select * from vtocc_test limit 2")
    vend = self.env.debug_vars()
    self.assertEqual(cu.rowcount, 2)
    self.assertEqual(vstart.mget("Queries.TotalCount", 0)+1, vend.Queries.TotalCount)
    self.assertEqual(vstart.mget("Queries.Histograms.PASS_SELECT.Count", 0)+1, vend.Queries.Histograms.PASS_SELECT.Count)

  def test_nocache_list_arg(self):
    cu = self.env.execute("select * from vtocc_test where intval in ::list", {"list": field_types.List([2, 3, 4])})
    self.assertEqual(cu.rowcount, 2)
    cu = self.env.execute("select * from vtocc_test where intval in ::list", {"list": field_types.List([3, 4])})
    self.assertEqual(cu.rowcount, 1)
    cu = self.env.execute("select * from vtocc_test where intval in ::list", {"list": field_types.List([3])})
    self.assertEqual(cu.rowcount, 1)
    with self.assertRaises(dbexceptions.DatabaseError):
      cu = self.env.execute("select * from vtocc_test where intval in ::list", {"list": field_types.List()})

  def test_commit(self):
    vstart = self.env.debug_vars()
    self.env.txlog.reset()
    self.env.conn.begin()
    self.assertNotEqual(self.env.conn.transaction_id, 0)
    self.env.execute("insert into vtocc_test (intval, floatval, charval, binval) values(4, null, null, null)")
    self.env.conn.commit()
    time.sleep(0.1)
    txlog = self.env.txlog.read().split('\t')
    self.assertEqual(txlog[4], "commit")
    self.assertEqual(txlog[5], "insert into vtocc_test (intval, floatval, charval, binval) values(4, null, null, null)")
    cu = self.env.execute("select * from vtocc_test")
    self.assertEqual(cu.rowcount, 4)
    self.env.conn.begin()
    self.env.execute("delete from vtocc_test where intval=4")
    self.env.conn.commit()
    cu = self.env.execute("select * from vtocc_test")
    self.assertEqual(cu.rowcount, 3)
    vend = self.env.debug_vars()
    # We should have at least one connection
    self.assertEqual(vstart.mget("Transactions.TotalCount", 0)+2, vend.Transactions.TotalCount)
    self.assertEqual(vstart.mget("Transactions.Histograms.Completed.Count", 0)+2, vend.Transactions.Histograms.Completed.Count)
    self.assertEqual(vstart.mget("Queries.TotalCount", 0)+8, vend.Queries.TotalCount)
    self.assertEqual(vstart.mget("Queries.Histograms.BEGIN.Count", 0)+2, vend.Queries.Histograms.BEGIN.Count)
    self.assertEqual(vstart.mget("Queries.Histograms.COMMIT.Count", 0)+2, vend.Queries.Histograms.COMMIT.Count)
    self.assertEqual(vstart.mget("Queries.Histograms.INSERT_PK.Count", 0)+1, vend.Queries.Histograms.INSERT_PK.Count)
    self.assertEqual(vstart.mget("Queries.Histograms.DML_PK.Count", 0)+1, vend.Queries.Histograms.DML_PK.Count)
    self.assertEqual(vstart.mget("Queries.Histograms.PASS_SELECT.Count", 0)+2, vend.Queries.Histograms.PASS_SELECT.Count)

  def test_integrity_error(self):
    vstart = self.env.debug_vars()
    self.env.conn.begin()
    try:
      self.env.execute("insert into vtocc_test values(1, null, null, null)")
    except dbexceptions.IntegrityError as e:
      self.assertContains(str(e), "error: duplicate")
    else:
      self.fail("Did not receive exception")
    finally:
      self.env.conn.rollback()
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("InfoErrors.DupKey", 0)+1, vend.InfoErrors.DupKey)

  def test_rollback(self):
    vstart = self.env.debug_vars()
    self.env.txlog.reset()
    self.env.conn.begin()
    self.assertNotEqual(self.env.conn.transaction_id, 0)
    self.env.execute("insert into vtocc_test values(4, null, null, null)")
    self.env.conn.rollback()
    time.sleep(0.1)
    txlog = self.env.txlog.read().split('\t')
    self.assertEqual(txlog[4], "rollback")
    self.assertEqual(txlog[5], "insert into vtocc_test values(4, null, null, null)")
    cu = self.env.execute("select * from vtocc_test")
    self.assertEqual(cu.rowcount, 4)
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("Transactions.TotalCount", 0)+1, vend.Transactions.TotalCount)
    self.assertEqual(vstart.mget("Transactions.Histograms.Aborted.Count", 0)+1, vend.Transactions.Histograms.Aborted.Count)
    self.assertEqual(vstart.mget("Queries.Histograms.BEGIN.Count", 0)+1, vend.Queries.Histograms.BEGIN.Count)
    self.assertEqual(vstart.mget("Queries.Histograms.ROLLBACK.Count", 0)+1, vend.Queries.Histograms.ROLLBACK.Count)

  def test_nontx_dml(self):
    vstart = self.env.debug_vars()
    results = self.env.execute("insert into vtocc_test values(444, null, null, null)")
    vend = self.env.debug_vars()
    self.assertEqual(results.description, [])

  def test_trailing_comment(self):
    vstart = self.env.debug_vars()
    bv={'ival': 1}
    self.env.execute("select * from vtocc_test where intval=:ival", bv)
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("QueryCacheLength", 0)+1, vend.QueryCacheLength)
    # This should not increase the query cache size
    self.env.execute("select * from vtocc_test where intval=:ival /* trailing comment */", bv)
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("QueryCacheLength", 0)+1, vend.QueryCacheLength)
    # This should also not increase the query cache size
    self.env.execute("select * from vtocc_test where intval=:ival /* trailing comment1 */ /* comment2 */", bv)
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("QueryCacheLength", 0)+1, vend.QueryCacheLength)

  def test_complex_dmls(self):
    self.env.conn.begin()
    try:
      with self.assertRaises(dbexceptions.DatabaseError):
        self.env.execute("insert into vtocc_a(eid, id, name, foo) values (7, 1+1, '', '')")
      with self.assertRaises(dbexceptions.DatabaseError):
        self.env.execute("insert into vtocc_d(eid, id) values (1, 1)")
      with self.assertRaises(dbexceptions.DatabaseError):
        self.env.execute("update vtocc_a set eid = 1+1 where eid = 1 and id = 1")
      with self.assertRaises(dbexceptions.DatabaseError):
        self.env.execute("insert into vtocc_d(eid, id) values (1, 1)")

      self.env.execute("delete from upsert_test")
      with self.assertRaises(dbexceptions.DatabaseError):
        self.env.execute("insert into upsert_test(id1, id2) values (1, 1), (2, 2) on duplicate key update id1 = 1")

      self.env.execute("delete from upsert_test")
      with self.assertRaises(dbexceptions.DatabaseError):
        self.env.execute("insert into upsert_test(id1, id2) select eid, id from vtocc_a limit 1 on duplicate key update id2 = id1")

      self.env.execute("delete from upsert_test")
      with self.assertRaises(dbexceptions.DatabaseError):
        self.env.execute("insert into upsert_test(id1, id2) values (1, 1) on duplicate key update id1 = 2+1")

      self.env.execute("delete from upsert_test")
      with self.assertRaises(dbexceptions.DatabaseError):
        self.env.execute("insert into upsert_test(id1, id2) values (1, 1)")
        self.env.execute("insert into upsert_test(id1, id2) values (2, 1) on duplicate key update id2 = 2")
    finally:
      self.env.conn.rollback()

  def test_pass_dml(self):
    self.env.execute("set vt_strict_mode=0")
    self.env.conn.begin()
    try:
      self.env.execute("insert into vtocc_a(eid, id, name, foo) values (7, 1+1, '', '')")
      self.env.execute("insert into vtocc_d(eid, id) values (1, 1)")
      self.env.execute("insert into vtocc_a(eid, id, name, foo) values (8, 2, '', '') on duplicate key update id = 2+1")
      self.env.execute("update vtocc_a set eid = 1+1 where eid = 1 and id = 1")
      self.env.execute("insert into vtocc_d(eid, id) values (1, 1)")
      self.env.execute("update vtocc_a set eid = :eid where eid = 1 and id = 1", {"eid": 3})
    finally:
      self.env.conn.rollback()
      self.env.execute("set vt_strict_mode=1")

  def test_select_lock(self):
    for lock_mode in ['for update', 'lock in share mode']:
      try:
        self.env.execute("select * from vtocc_test where intval=2 %s" % lock_mode)
      except dbexceptions.DatabaseError as e:
        self.assertContains(str(e), "error: Disallowed")
      else:
        self.fail("Did not receive exception")

      # If these throw no exceptions, we're good
      self.env.conn.begin()
      self.env.execute("select * from vtocc_test where intval=2 %s" % lock_mode)
      self.env.conn.commit()
      # Make sure the row is not locked for read
      self.env.execute("select * from vtocc_test where intval=2")

  def test_pool_size(self):
    vstart = self.env.debug_vars()
    self.env.execute("set vt_pool_size=1")
    self.assertRaises(dbexceptions.DatabaseError, self.env.execute, "select sleep(3) from dual")
    self.env.execute("select 1 from dual")
    vend = self.env.debug_vars()
    self.assertEqual(vend.ConnPoolCapacity, 1)
    self.assertEqual(vstart.ConnPoolWaitCount+1, vend.ConnPoolWaitCount)
    self.env.execute("set vt_pool_size=16")
    vend = self.env.debug_vars()
    self.assertEqual(vend.ConnPoolCapacity, 16)

  def test_transaction_cap(self):
    self.env.execute("set vt_transaction_cap=1")
    self.env.execute("set vt_txpool_timeout=0.5")
    vstart = self.env.debug_vars()
    self.assertEqual(vstart.TransactionPoolPoolTimeout, 5e8)
    co2 = self.env.connect()
    self.env.conn.begin()
    try:
      cu2 = cursor.TabletCursor(co2)
      start = time.time()
      co2.begin()
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), "tx_pool_full")
      self.assertTrue(time.time()-start >= 0.4)
    else:
      self.fail("Did not receive exception")
    finally:
      cu2.close()
      co2.close()
    self.env.conn.commit()
    vend = self.env.debug_vars()
    self.assertEqual(vend.TransactionPoolCapacity, 1)
    self.env.execute("set vt_transaction_cap=20")
    self.env.execute("set vt_txpool_timeout=1")
    vend = self.env.debug_vars()
    self.assertEqual(vend.TransactionPoolCapacity, 20)
    self.assertEqual(vend.TransactionPoolPoolTimeout, 1e9)
    self.assertEqual(vstart.mget("Errors.TxPoolFull", 0) + 1, vend.Errors.TxPoolFull)

  def test_transaction_timeout(self):
    self.env.execute("set vt_transaction_timeout=0.25")
    # wait for any pending transactions to timeout
    time.sleep(0.3)
    vstart = self.env.debug_vars()
    self.env.txlog.reset()
    self.env.conn.begin()
    vmid = self.env.debug_vars()
    self.assertEqual(vstart.TransactionPoolAvailable, vmid.TransactionPoolAvailable+1)
    time.sleep(0.3)
    try:
      self.env.conn.commit()
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), "not_in_tx: Transaction")
    else:
      self.fail("Did not receive exception")
    time.sleep(0.1)
    txlog = self.env.txlog.read().split('\t')
    self.assertEqual(txlog[4], "kill")
    vend = self.env.debug_vars()
    self.assertEqual(vstart.TransactionPoolAvailable, vend.TransactionPoolAvailable)
    self.assertEqual(vend.TransactionPoolTimeout, 250000000)
    self.assertEqual(vstart.mget("Kills.Transactions", 0)+1, vend.Kills.Transactions)
    self.env.execute("set vt_transaction_timeout=30")
    vend = self.env.debug_vars()
    self.assertEqual(vend.TransactionPoolTimeout, 30000000000)

  def test_query_cache(self):
    self.env.execute("set vt_query_cache_size=1")
    bv={'ival1': 1, 'ival2': 1}
    self.env.execute("select * from vtocc_test where intval=:ival1", bv)
    self.env.execute("select * from vtocc_test where intval=:ival2", bv)
    vend = self.env.debug_vars()
    self.assertEqual(vend.QueryCacheLength, 1)
    self.assertEqual(vend.QueryCacheSize, 1)
    self.assertEqual(vend.QueryCacheCapacity, 1)
    self.env.execute("set vt_query_cache_size=5000")
    self.env.execute("select * from vtocc_test where intval=:ival1", bv)
    vend = self.env.debug_vars()
    self.assertEqual(vend.QueryCacheLength, 2)
    self.assertEqual(vend.QueryCacheSize, 2)
    self.assertEqual(vend.QueryCacheCapacity, 5000)
    self.env.execute("select * from vtocc_test where intval=1")
    vend = self.env.debug_vars()
    self.assertEqual(vend.QueryCacheLength, 3)
    self.assertEqual(vend.QueryCacheSize, 3)
    self.assertEqual(vend.QueryCacheCapacity, 5000)

  def test_schema_reload_time(self):
    vend = self.env.debug_vars()
    self.assertEqual(vend.SchemaReloadTime, 1800 * 1e9)
    mcu = self.env.mysql_conn.cursor()
    mcu.execute("create table vtocc_temp(intval int)")
    # This should cause a reload
    self.env.execute("set vt_schema_reload_time=600")
    vend = self.env.debug_vars()
    self.assertEqual(vend.SchemaReloadTime, 600 * 1e9)
    try:
      for i in range(10):
        try:
          self.env.execute("select * from vtocc_temp")
        except dbexceptions.DatabaseError as e:
          self.assertContains(str(e), "not found in schema")
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
    self.assertEqual(vend.MaxResultSize, 2)
    try:
      self.env.execute("select * from vtocc_test")
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), "error: Row")
    else:
      self.fail("Did not receive exception")
    self.env.execute("set vt_max_result_size=10000")
    vend = self.env.debug_vars()
    self.assertEqual(vend.MaxResultSize, 10000)

  def test_max_dml_rows(self):
    self.env.conn.begin()
    self.env.execute("insert into vtocc_a(eid, id, name, foo) values (3, 1, '', ''), (3, 2, '', ''), (3, 3, '', '')")
    self.env.conn.commit()

    # Verify all three rows are updated in a single DML.
    self.env.querylog.reset()
    self.env.conn.begin()
    self.env.execute("update vtocc_a set foo='fghi' where eid = 3")
    self.env.conn.commit()
    log = self.env.querylog.tailer.read()
    self.assertContains(log, "update vtocc_a set foo = 'fghi' where (eid = 3 and id = 1) or (eid = 3 and id = 2) or (eid = 3 and id = 3) /* _stream vtocc_a (eid id ) (3 1 ) (3 2 ) (3 3 )")

    # Verify that rows get split, and if pk changes, those values are also
    # split correctly.
    self.env.execute("set vt_max_dml_rows=2")
    self.env.querylog.reset()
    self.env.conn.begin()
    self.env.execute("update vtocc_a set eid=2 where eid = 3")
    self.env.conn.commit()
    log = self.env.querylog.tailer.read()
    self.assertContains(log, "update vtocc_a set eid = 2 where (eid = 3 and id = 1) or (eid = 3 and id = 2) /* _stream vtocc_a (eid id ) (3 1 ) (3 2 ) (2 1 ) (2 2 )")
    self.assertContains(log, "update vtocc_a set eid = 2 where (eid = 3 and id = 3) /* _stream vtocc_a (eid id ) (3 3 ) (2 3 )")

    # Verify that a normal update get split correctly.
    self.env.querylog.reset()
    self.env.conn.begin()
    self.env.execute("update vtocc_a set foo='fghi' where eid = 2")
    self.env.conn.commit()
    log = self.env.querylog.tailer.read()
    self.assertContains(log, "update vtocc_a set foo = 'fghi' where (eid = 2 and id = 1) or (eid = 2 and id = 2) /* _stream vtocc_a (eid id ) (2 1 ) (2 2 )")
    self.assertContains(log, "update vtocc_a set foo = 'fghi' where (eid = 2 and id = 3) /* _stream vtocc_a (eid id ) (2 3 )")

    # Verify that a delete get split correctly.
    self.env.querylog.reset()
    self.env.conn.begin()
    self.env.execute("delete from vtocc_a where eid = 2")
    self.env.conn.commit()
    log = self.env.querylog.tailer.read()
    self.assertContains(log, "delete from vtocc_a where (eid = 2 and id = 1) or (eid = 2 and id = 2) /* _stream vtocc_a (eid id ) (2 1 ) (2 2 )")
    self.assertContains(log, "delete from vtocc_a where (eid = 2 and id = 3) /* _stream vtocc_a (eid id ) (2 3 )")

    # Reset vt_max_dml_rows
    self.env.execute("set vt_max_dml_rows=500")

  def test_query_timeout(self):
    vstart = self.env.debug_vars()
    conn = tablet_conn.connect(self.env.address, '', 'test_keyspace', '0', 5, user='youtube-dev-dedicated', password='vtpass')
    cu = cursor.TabletCursor(conn)
    self.env.execute("set vt_query_timeout=0.25")
    try:
      conn.begin()
      cu.execute("select sleep(0.5) from vtocc_test", {})
    except dbexceptions.DatabaseError as e:
      if "error: Query" not in str(e) and "error: the query was killed" not in str(e):
        self.fail("Query not killed as expected")
    else:
      self.fail("Did not receive exception")

    try:
      cu.execute("select 1 from dual", {})
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), "not_in_tx: Transaction")
    else:
      self.fail("Did not receive exception")

    cu.close()
    conn.close()

    vend = self.env.debug_vars()
    self.assertEqual(vend.QueryTimeout, 250000000)
    self.assertEqual(vstart.mget("Kills.Queries", 0)+1, vend.Kills.Queries)
    self.env.execute("set vt_query_timeout=30")
    vend = self.env.debug_vars()
    self.assertEqual(vend.QueryTimeout, 30000000000)

  def test_idle_timeout(self):
    self.env.execute("set vt_idle_timeout=1")
    time.sleep(2)
    self.env.execute("select 1 from dual")
    vend = self.env.debug_vars()
    self.assertEqual(vend.ConnPoolIdleTimeout, 1000000000)
    self.assertEqual(vend.TransactionPoolIdleTimeout, 1000000000)
    self.env.execute("set vt_idle_timeout=1800")
    vend = self.env.debug_vars()
    self.assertEqual(vend.ConnPoolIdleTimeout, 1800000000000)
    self.assertEqual(vend.TransactionPoolIdleTimeout, 1800000000000)

  def test_consolidation(self):
    vstart = self.env.debug_vars()
    # The first call always does a full fetch for field info
    self.assertRaises(dbexceptions.DatabaseError, self.env.execute, "select sleep(3) from dual")
    time.sleep(2)
    for i in range(2):
      try:
        self.env.execute("select sleep(3) from dual")
      except dbexceptions.OperationalError:
        pass
    vend = self.env.debug_vars()
    self.assertEqual(vstart.mget("Waits.TotalCount", 0)+1, vend.Waits.TotalCount)
    self.assertEqual(vstart.mget("Waits.Histograms.Consolidations.Count", 0)+1, vend.Waits.Histograms.Consolidations.Count)

  def test_batch(self):
    queries = ["select * from vtocc_a where id = :a", "select * from vtocc_b where id = :b"]
    bvars = [{"a":2}, {"b":2}]
    results = self.env.conn._execute_batch(queries, bvars, False)
    self.assertEqual(results, [([(1L, 2L, 'bcde', 'fghi')], 1, 0, [('eid', 8), ('id', 3), ('name', 253), ('foo', 253)]), ([(1L, 2L)], 1, 0, [('eid', 8), ('id', 3)])])

    # Not in transaction, as_transaction false
    queries = [
        "insert into vtocc_test (intval, floatval, charval, binval) values(4, null, null, null)",
        "select * from vtocc_test where intval = 4",
        "delete from vtocc_test where intval = 4",
        ]
    results = self.env.conn._execute_batch(queries, [{}, {}, {}], False)
    self.assertEqual(results[1][0], [(4L, None, None, None)])

    # In transaction, as_transaction false
    self.env.conn.begin()
    results = self.env.conn._execute_batch(queries, [{}, {}, {}], False)
    self.assertEqual(results[1][0], [(4L, None, None, None)])
    self.env.conn.commit()

    # In transaction, as_transaction true
    self.env.conn.begin()
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*cannot start a new transaction.*'):
      self.env.conn._execute_batch(queries, [{}, {}, {}], True)
    self.env.conn.rollback()

    # Not in transaction, as_transaction true
    results = self.env.conn._execute_batch(queries, [{}, {}, {}], True)
    self.assertEqual(results[1][0], [(4L, None, None, None)])

  def test_bind_in_select(self):
    bv = {'bv': 1}
    cu = self.env.execute('select :bv from vtocc_test', bv)
    self.assertEqual(cu.description, [('1', 8)])
    bv = {'bv': 'abcd'}
    cu = self.env.execute('select :bv from vtocc_test', bv)
    self.assertEqual(cu.description, [('abcd', 253)])

  def test_types(self):
    self._verify_mismatch("insert into vtocc_ints(tiny) values('str')")
    self._verify_mismatch("insert into vtocc_ints(tiny) values(:str)", {"str": "str"})
    self._verify_mismatch("insert into vtocc_ints(tiny) values(1.2)")
    self._verify_mismatch("insert into vtocc_ints(tiny) values(:fl)", {"fl": 1.2})
    self._verify_mismatch("insert into vtocc_strings(vb) values(1)")
    self._verify_mismatch("insert into vtocc_strings(vb) values(:id)", {"id": 1})
    self._verify_error("insert into vtocc_strings(vb) values('12345678901234567')", None, "error: Data too long")
    self._verify_error("insert into vtocc_ints(tiny) values(-129)", None, "error: Out of range")

    try:
      self.env.conn.begin()
      self.env.execute("insert into vtocc_ints(tiny, medium) values(1, -129)")
      self.env.execute("insert into vtocc_fracts(id, num) values(1, 1)")
      self.env.execute("insert into vtocc_strings(vb) values('a')")
      self.env.conn.commit()
      self._verify_mismatch("insert into vtocc_strings(vb) select tiny from vtocc_ints")
      self._verify_mismatch("insert into vtocc_ints(tiny) select num from vtocc_fracts")
      self._verify_mismatch("insert into vtocc_ints(tiny) select vb from vtocc_strings")
      self._verify_error("insert into vtocc_ints(tiny) select medium from vtocc_ints", None, "error: Out of range")
    finally:
      self.env.conn.begin()
      self.env.execute("delete from vtocc_ints")
      self.env.execute("delete from vtocc_fracts")
      self.env.execute("delete from vtocc_strings")
      self.env.conn.commit()

  def test_customrules(self):
    bv = {'asdfg': 1}
    try:
      self.env.execute("select * from vtocc_test where intval=:asdfg", bv)
      self.fail("Bindvar asdfg should not be allowed by custom rule")
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), "error: Query disallowed")
    # Test dynamic custom rule for vttablet
    if self.env.env == "vttablet":
      if environment.topo_server().flavor() == 'zookeeper':
        # Make a change to the rule
        self.env.change_customrules()
        time.sleep(3)
        try:
          self.env.execute("select * from vtocc_test where intval=:asdfg", bv)
        except dbexceptions.DatabaseError as e:
          self.fail("Bindvar asdfg should be allowed after a change of custom rule, Err=" + str(e))
        # Restore the rule
        self.env.restore_customrules()
        time.sleep(3)
        try:
          self.env.execute("select * from vtocc_test where intval=:asdfg", bv)
          self.fail("Bindvar asdfg should not be allowed by custom rule")
        except dbexceptions.DatabaseError as e:
          self.assertContains(str(e), "error: Query disallowed")

  def test_health(self):
    self.assertEqual(self.env.health(), "ok")

  def test_query_stats(self):
    bv = {'eid': 1}
    self.env.execute("select eid as query_stats from vtocc_a where eid = :eid", bv)
    self._verify_query_stats(self.env.query_stats(), "select eid as query_stats from vtocc_a where eid = :eid", "vtocc_a", "PASS_SELECT", 1, 2, 0)
    tstartQueryCounts = self._get_vars_query_stats(self.env.debug_vars()["QueryCounts"], "vtocc_a", "PASS_SELECT")
    tstartRowCounts = self._get_vars_query_stats(self.env.debug_vars()["QueryRowCounts"], "vtocc_a", "PASS_SELECT")
    tstartErrorCounts = self._get_vars_query_stats(self.env.debug_vars()["QueryErrorCounts"], "vtocc_a", "PASS_SELECT")
    tstartTimesNs = self._get_vars_query_stats(self.env.debug_vars()["QueryTimesNs"], "vtocc_a", "PASS_SELECT")
    self.assertEqual(tstartQueryCounts, 1)
    self.assertEqual(tstartRowCounts, 2)
    self.assertEqual(tstartErrorCounts, 0)
    self.assertTrue(tstartTimesNs > 0)

    try:
      self.env.execute("select eid as query_stats from vtocc_a where dontexist(eid) = :eid", bv)
    except dbexceptions.DatabaseError:
      pass
    else:
      self.fail("Did not receive exception: " + query)
    self._verify_query_stats(self.env.query_stats(), "select eid as query_stats from vtocc_a where dontexist(eid) = :eid", "vtocc_a", "PASS_SELECT", 1, 0, 1)
    tendQueryCounts = self._get_vars_query_stats(self.env.debug_vars()["QueryCounts"], "vtocc_a", "PASS_SELECT")
    tendRowCounts = self._get_vars_query_stats(self.env.debug_vars()["QueryRowCounts"], "vtocc_a", "PASS_SELECT")
    tendErrorCounts = self._get_vars_query_stats(self.env.debug_vars()["QueryErrorCounts"], "vtocc_a", "PASS_SELECT")
    tendTimesNs = self._get_vars_query_stats(self.env.debug_vars()["QueryTimesNs"], "vtocc_a", "PASS_SELECT")
    self.assertEqual(tstartQueryCounts+1, tendQueryCounts)
    self.assertEqual(tstartRowCounts, tendRowCounts)
    self.assertEqual(tstartErrorCounts+1, tendErrorCounts)
    self.assertTrue((tendTimesNs - tstartTimesNs) > 0)

  def test_other(self):
    cu = self.env.execute("show variables like 'version'")
    for v in cu:
      self.assertEqual(v[0], 'version')
    cu = self.env.execute("describe vtocc_a")
    self.assertEqual(cu.rowcount, 4)
    cu = self.env.execute("explain vtocc_a")
    self.assertEqual(cu.rowcount, 4)

  def _verify_mismatch(self, query, bindvars=None):
    self._verify_error(query, bindvars, "error: type mismatch")

  def _verify_error(self, query, bindvars, err):
    self.env.conn.begin()
    try:
      self.env.execute(query, bindvars)
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), err)
    else:
      self.fail("Did not receive exception: " + query)
    finally:
      self.env.conn.rollback()

  def _get_vars_query_stats(self, query_stats, table, plan):
    return query_stats[table + "." + plan]

  def _verify_query_stats(self, query_stats, query, table, plan, count, rows, errors):
    for stat in query_stats:
      if stat["Query"] != query:
        continue
      self.assertEqual(stat["Table"], table)
      self.assertEqual(stat["Plan"], plan)
      self.assertEqual(stat["QueryCount"], count)
      self.assertEqual(stat["RowCount"], rows)
      self.assertEqual(stat["ErrorCount"], errors)
      self.assertTrue(stat["Time"] > 0)
      return
    self.fail("query %s not found" % query)

  def test_sqls(self):
    error_count = self.env.run_cases(nocache_cases.cases)
    if error_count != 0:
      self.fail("test_execution errors: %d"%(error_count))

  def test_table_acl_no_access(self):
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*table acl error.*'):
      self.env.execute("select * from vtocc_acl_no_access where key1=1")
    self.env.conn.begin()
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*table acl error.*'):
      self.env.execute("delete from vtocc_acl_no_access where key1=1")
    self.env.conn.commit()
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*table acl error.*'):
      self.env.execute("alter table vtocc_acl_no_access comment 'comment'")
    cu = cursor.StreamCursor(self.env.conn)
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*table acl error.*'):
      cu.execute("select * from vtocc_acl_no_access where key1=1", {})
    cu.close()

  def test_table_acl_read_only(self):
    self.env.execute("select * from vtocc_acl_read_only where key1=1")
    self.env.conn.begin()
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*table acl error.*'):
      self.env.execute("delete from vtocc_acl_read_only where key1=1")
    self.env.conn.commit()
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*table acl error.*'):
      self.env.execute("alter table vtocc_acl_read_only comment 'comment'")
    cu = cursor.StreamCursor(self.env.conn)
    cu.execute("select * from vtocc_acl_read_only where key1=1", {})
    cu.fetchall()
    cu.close()

  def test_table_acl_read_write(self):
    self.env.execute("select * from vtocc_acl_read_write where key1=1")
    self.env.conn.begin()
    self.env.execute("delete from vtocc_acl_read_write where key1=1")
    self.env.conn.commit()
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*table acl error.*'):
      self.env.execute("alter table vtocc_acl_read_write comment 'comment'")
    cu = cursor.StreamCursor(self.env.conn)
    cu.execute("select * from vtocc_acl_read_write where key1=1", {})
    cu.fetchall()
    cu.close()

  def test_table_acl_admin(self):
    self.env.execute("select * from vtocc_acl_admin where key1=1")
    self.env.conn.begin()
    self.env.execute("delete from vtocc_acl_admin where key1=1")
    self.env.conn.commit()
    self.env.execute("alter table vtocc_acl_admin comment 'comment'")
    cu = cursor.StreamCursor(self.env.conn)
    cu.execute("select * from vtocc_acl_admin where key1=1", {})
    cu.fetchall()
    cu.close()

  def test_table_acl_unmatched(self):
    self.env.execute("select * from vtocc_acl_unmatched where key1=1")
    self.env.conn.begin()
    self.env.execute("delete from vtocc_acl_unmatched where key1=1")
    self.env.conn.commit()
    self.env.execute("alter table vtocc_acl_unmatched comment 'comment'")
    cu = cursor.StreamCursor(self.env.conn)
    cu.execute("select * from vtocc_acl_unmatched where key1=1", {})
    cu.fetchall()
    cu.close()

  def test_table_acl_all_user_read_only(self):
    self.env.execute("select * from vtocc_acl_all_user_read_only where key1=1")
    self.env.conn.begin()
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*table acl error.*'):
      self.env.execute("delete from vtocc_acl_all_user_read_only where key1=1")
    self.env.conn.commit()
    with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*table acl error.*'):
      self.env.execute("alter table vtocc_acl_all_user_read_only comment 'comment'")
    cu = cursor.StreamCursor(self.env.conn)
    cu.execute("select * from vtocc_acl_all_user_read_only where key1=1", {})
    cu.fetchall()
    cu.close()

  # This is a super-slow test. Uncomment and test if you change
  # the server-side reconnect logic.
  #def test_server_reconnect(self):
  #  self.env.execute("set vt_pool_size=1")
  #  self.env.execute("select * from vtocc_test limit :l", {"l": 1})
  #  self.env.tablet.shutdown_mysql()
  #  time.sleep(5)
  #  self.env.tablet.start_mysql()
  #  time.sleep(5)
  #  self.env.execute("select * from vtocc_test limit :l", {"l": 1})
  #  self.env.conn.begin()
  #  self.env.tablet.shutdown_mysql()
  #  time.sleep(5)
  #  self.env.tablet.start_mysql()
  #  time.sleep(5)
  #  with self.assertRaisesRegexp(dbexceptions.DatabaseError, ".*server has gone away.*"):
  #    self.env.execute("select * from vtocc_test limit :l", {"l": 1})
  #  self.env.conn.rollback()
  #  self.env.execute("set vt_pool_size=16")

  # Super-slow test.
  #def test_mysql_shutdown(self):
  #  self.env.execute("select * from vtocc_test limit :l", {"l": 1})
  #  self.env.tablet.shutdown_mysql()
  #  time.sleep(5)
  #  with self.assertRaisesRegexp(dbexceptions.DatabaseError, '.*state NOT_SERVING.*'):
  #    self.env.execute("select * from vtocc_test limit :l", {"l": 1})
  #  self.env.tablet.start_mysql()
  #  time.sleep(5)
