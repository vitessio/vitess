from vtdb import dbexceptions
from vtdb import field_types

import framework
import cache_cases1
import cache_cases2

import cases_framework


class TestCache(framework.TestCase):
  def test_overrides(self):
    tstart = self.env.table_stats()["vtocc_view"]
    self.env.querylog.reset()
    cu = self.env.execute("select * from vtocc_view where key2 = 1")
    self.assertEqual(cu.fetchone(), (1L, 10L, 1L, 3L))
    tend = self.env.table_stats()["vtocc_view"]
    self.assertEqual(tstart["Misses"]+1, tend["Misses"])
    log = self.env.querylog.tailer.read()

    self.assertContains(log, "select * from vtocc_view where 1 != 1")
    self.assertContains(
        log,
        "select key2, key1, data1, data2 from vtocc_view where key2 in (1)")

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
    self.assertContains(
        log,
        "update vtocc_part1 set data1 = 2 where key2 in (1) "
        "/* _stream vtocc_part1 (key2 ) (1 ); */")


    self.env.querylog.reset()
    cu = self.env.execute("select * from vtocc_view where key2 = 1")
    self.assertEqual(cu.fetchone(), (1L, 10L, 2L, 3L))
    tend = self.env.table_stats()["vtocc_view"]
    self.assertEqual(tstart["Misses"]+1, tend["Misses"])
    log = self.env.querylog.tailer.read()
    self.assertContains(
        log,
        "select key2, key1, data1, data2 from vtocc_view where key2 in (1)")

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
    self.assertContains(
        log,
        "select key2, key1, data1, data2 from vtocc_view where key2 in (1)")

    tstart = self.env.table_stats()["vtocc_view"]
    cu = self.env.execute("select * from vtocc_view where key2 = 1")
    self.assertEqual(cu.fetchone(), (1L, 10L, 2L, 2L))
    tend = self.env.table_stats()["vtocc_view"]
    self.assertEqual(tstart["Hits"]+1, tend["Hits"])

  def test_cache1_sqls(self):
    error_count = self.env.run_cases(cache_cases1.cases)
    if error_count != 0:
      self.fail("test_cache1_sqls errors: %d" % error_count)

  def test_cache2_sqls(self):
    error_count = self.env.run_cases(cache_cases2.cases)
    if error_count != 0:
      self.fail("test_cache2_sqls errors: %d" % error_count)
