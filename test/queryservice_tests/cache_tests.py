from vtdb import dbexceptions
from vtdb import field_types

import framework
import cache_cases1
import cache_cases2

import cases_framework

class TestWillNotBeCached(framework.TestCase):

  def setUp(self):
    self.env.log.reset()

  def tearDown(self):
    self.env.execute("drop table vtocc_nocache")

  def test_nocache(self):
    self.env.execute("create table vtocc_nocache(eid int, primary key (eid)) comment 'vtocc_nocache'")
    self.assertContains(self.env.log.read(), "Will not be cached")

  def test_nopk(self):
    self.env.execute("create table vtocc_nocache(eid int)")
    self.assertContains(self.env.log.read(), "Will not be cached")

  def test_charcol(self):
    self.env.execute("create table vtocc_nocache(eid varchar(10), primary key (eid))")
    self.assertContains(self.env.log.read(), "Will not be cached")


class TestCache(framework.TestCase):
  def test_num_str(self):
    try:
      self.env.execute("select bid, eid from vtocc_cached2 where eid = 1 and bid = 1")
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), "error: type mismatch")
    else:
      self.fail("Did not receive exception")

  def test_cache_list_arg(self):
    cu = self.env.execute("select * from vtocc_cached1 where eid in ::list", {"list": (3, 4, 32768)})
    self.assertEqual(cu.rowcount, 2)
    cu = self.env.execute("select * from vtocc_cached1 where eid in ::list", {"list": set([3, 4, 32768])})
    self.assertEqual(cu.rowcount, 2)
    cu = self.env.execute("select * from vtocc_cached1 where eid in ::list", {"list": [3, 4, 32768]})
    self.assertEqual(cu.rowcount, 2)
    cu = self.env.execute("select * from vtocc_cached1 where eid in ::list", {"list": [3, 4]})
    self.assertEqual(cu.rowcount, 2)
    cu = self.env.execute("select * from vtocc_cached1 where eid in ::list", {"list": [3]})
    self.assertEqual(cu.rowcount, 1)
    with self.assertRaises(dbexceptions.DatabaseError):
      cu = self.env.execute("select * from vtocc_cached1 where eid in ::list", {"list": list()})

  def test_uncache(self):
    try:
      # Verify row cache is working
      self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
      tstart = self.env.table_stats()["vtocc_cached2"]
      self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
      tend = self.env.table_stats()["vtocc_cached2"]
      self.assertEqual(tstart["Hits"]+1, tend["Hits"])
      # disable
      self.env.execute("alter table vtocc_cached2 comment 'vtocc_nocache'")
      self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
      try:
        tstart = self.env.table_stats()["vtocc_cached2"]
      except KeyError:
        pass
      else:
        self.fail("Did not receive exception")
    finally:
      self.env.execute("alter table vtocc_cached2 comment ''")

    # Verify row cache is working again
    self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
    tstart = self.env.table_stats()["vtocc_cached2"]
    self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
    tend = self.env.table_stats()["vtocc_cached2"]
    self.assertEqual(tstart["Hits"]+1, tend["Hits"])

  def test_bad_limit(self):
    try:
      with self.assertRaises(dbexceptions.DatabaseError):
        self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo' limit :a", {"a": -1})
    finally:
      self.env.execute("alter table vtocc_cached2 comment ''")

  def test_rename(self):
    try:
      # Verify row cache is working
      self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
      tstart = self.env.table_stats()["vtocc_cached2"]
      self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
      tend = self.env.table_stats()["vtocc_cached2"]
      self.assertEqual(tstart["Hits"]+1, tend["Hits"])
      # rename
      self.env.execute("alter table vtocc_cached2 rename to vtocc_renamed")
      try:
        tstart = self.env.table_stats()["vtocc_cached2"]
      except KeyError:
        pass
      else:
        self.fail("Did not receive exception")
      # Verify row cache is working
      self.env.execute("select * from vtocc_renamed where eid = 2 and bid = 'foo'")
      tstart = self.env.table_stats()["vtocc_renamed"]
      self.env.execute("select * from vtocc_renamed where eid = 2 and bid = 'foo'")
      tend = self.env.table_stats()["vtocc_renamed"]
      self.assertEqual(tstart["Hits"]+1, tend["Hits"])
    finally:
      # alter table so there's no hash collision when renamed
      self.env.execute("alter table vtocc_renamed comment 'renamed'")
      self.env.execute("rename table vtocc_renamed to vtocc_cached2")

    # Verify row cache is working again
    self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
    tstart = self.env.table_stats()["vtocc_cached2"]
    self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
    tend = self.env.table_stats()["vtocc_cached2"]
    self.assertEqual(tstart["Hits"]+1, tend["Hits"])

  def test_overrides(self):
    tstart = self.env.table_stats()["vtocc_view"]
    self.env.querylog.reset()
    cu = self.env.execute("select * from vtocc_view where key2 = 1")
    self.assertEqual(cu.fetchone(), (1L, 10L, 1L, 3L))
    tend = self.env.table_stats()["vtocc_view"]
    self.assertEqual(tstart["Misses"]+1, tend["Misses"])
    log = self.env.querylog.tailer.read()

    self.assertContains(log, "select * from vtocc_view where 1 != 1")
    self.assertContains(log, "select key2, key1, data1, data2 from vtocc_view where key2 in (1)")

    tstart = self.env.table_stats()["vtocc_view"]
    cu = self.env.execute("select * from vtocc_view where key2 = 1")
    self.assertEqual(cu.fetchone(), (1L, 10L, 1L, 3L))
    tend = self.env.table_stats()["vtocc_view"]
    self.assertEqual(tstart["Hits"]+1, tend["Hits"])

    tstart = self.env.table_stats()["vtocc_view"]
    self.env.conn.begin()
    self.env.querylog.reset()
    self.env.execute("update vtocc_part1 set data1 = 2 where key2 = 1")
    log = self.env.querylog.tailer.read()
    self.env.conn.commit()
    self.assertContains(log, "update vtocc_part1 set data1 = 2 where key2 in (1) /* _stream vtocc_part1 (key2 ) (1 ); */")


    self.env.querylog.reset()
    cu = self.env.execute("select * from vtocc_view where key2 = 1")
    self.assertEqual(cu.fetchone(), (1L, 10L, 2L, 3L))
    tend = self.env.table_stats()["vtocc_view"]
    self.assertEqual(tstart["Misses"]+1, tend["Misses"])
    log = self.env.querylog.tailer.read()
    self.assertContains(log, "select key2, key1, data1, data2 from vtocc_view where key2 in (1)")

    tstart = self.env.table_stats()["vtocc_view"]
    cu = self.env.execute("select * from vtocc_view where key2 = 1")
    self.assertEqual(cu.fetchone(), (1L, 10L, 2L, 3L))
    tend = self.env.table_stats()["vtocc_view"]
    self.assertEqual(tstart["Hits"]+1, tend["Hits"])

    tstart = self.env.table_stats()["vtocc_view"]
    self.env.conn.begin()
    self.env.execute("update vtocc_part2 set data2 = 2 where key3 = 1")
    self.env.conn.commit()


    self.env.querylog.reset()
    cu = self.env.execute("select * from vtocc_view where key2 = 1")
    self.assertEqual(cu.fetchone(), (1L, 10L, 2L, 2L))
    tend = self.env.table_stats()["vtocc_view"]
    self.assertEqual(tstart["Misses"]+1, tend["Misses"])
    log = self.env.querylog.tailer.read()
    self.assertContains(log, "select key2, key1, data1, data2 from vtocc_view where key2 in (1)")

    tstart = self.env.table_stats()["vtocc_view"]
    cu = self.env.execute("select * from vtocc_view where key2 = 1")
    self.assertEqual(cu.fetchone(), (1L, 10L, 2L, 2L))
    tend = self.env.table_stats()["vtocc_view"]
    self.assertEqual(tstart["Hits"]+1, tend["Hits"])

  def test_nodata(self):
    # This should not fail
    cu = self.env.execute("select * from vtocc_cached2 where eid = 6 and name = 'bar'")
    self.assertEqual(cu.rowcount, 0)

  def test_types(self):
    self._verify_mismatch("select * from vtocc_cached2 where eid = 'str' and bid = 'str'")
    self._verify_mismatch("select * from vtocc_cached2 where eid = :str and bid = :str", {"str": "str"})
    self._verify_mismatch("select * from vtocc_cached2 where eid = 1 and bid = 1")
    self._verify_mismatch("select * from vtocc_cached2 where eid = :id and bid = :id", {"id": 1})
    self._verify_mismatch("select * from vtocc_cached2 where eid = 1.2 and bid = 1.2")
    self._verify_mismatch("select * from vtocc_cached2 where eid = :fl and bid = :fl", {"fl": 1.2})

  def test_stats(self):
    self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
    tstartHits = self._get_vars_table_stats(self.env.debug_vars()["TableStats"], "vtocc_cached2", "Hits")
    self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
    tendHits = self._get_vars_table_stats(self.env.debug_vars()["TableStats"], "vtocc_cached2", "Hits")
    self.assertEqual(tstartHits+1, tendHits)

    tstartMisses = self._get_vars_table_stats(self.env.debug_vars()["TableStats"], "vtocc_view", "Misses")
    self.env.conn.begin()
    self.env.execute("update vtocc_part2 set data2 = 2 where key3 = 1")
    self.env.conn.commit()
    self.env.execute("select * from vtocc_view where key2 = 1")
    tendMisses = self._get_vars_table_stats(self.env.debug_vars()["TableStats"], "vtocc_view", "Misses")
    self.assertEqual(tstartMisses+1, tendMisses)

  def test_spot_check(self):
    vstart = self.env.debug_vars()
    self.assertEqual(vstart["RowcacheSpotCheckRatio"], 0)
    self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
    self.assertEqual(vstart["RowcacheSpotCheckCount"], self.env.debug_vars()["RowcacheSpotCheckCount"])
    self.env.execute("set vt_spot_check_ratio=1")
    self.assertEqual(self.env.debug_vars()["RowcacheSpotCheckRatio"], 1)
    self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
    self.assertEqual(vstart["RowcacheSpotCheckCount"]+1, self.env.debug_vars()["RowcacheSpotCheckCount"])

    vstart = self.env.debug_vars()
    self.env.execute("select * from vtocc_cached1 where eid in (9)")
    self.assertEqual(vstart["RowcacheSpotCheckCount"], self.env.debug_vars()["RowcacheSpotCheckCount"])
    self.env.execute("select * from vtocc_cached1 where eid in (9)")
    self.assertEqual(vstart["RowcacheSpotCheckCount"]+1, self.env.debug_vars()["RowcacheSpotCheckCount"])

    self.env.execute("set vt_spot_check_ratio=0")
    self.assertEqual(self.env.debug_vars()["RowcacheSpotCheckRatio"], 0)

  def _verify_mismatch(self, query, bindvars=None):
    try:
      self.env.execute(query, bindvars)
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), "error: type mismatch")
    else:
      self.fail("Did not receive exception")

  def test_cache1_sqls(self):
    error_count = self.env.run_cases(cache_cases1.cases)
    if error_count != 0:
      self.fail("test_cache1_sqls errors: %d" % error_count)

  def test_cache2_sqls(self):
    error_count = self.env.run_cases(cache_cases2.cases)
    if error_count != 0:
      self.fail("test_cache2_sqls errors: %d" % error_count)

  def _get_vars_table_stats(self, table_stats, table, stats):
    return table_stats[table + "." + stats]
