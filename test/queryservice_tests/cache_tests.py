from vtdb import dbexceptions
from vtdb import vt_occ2

import framework
import cache_cases

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
      self.env.execute("select bid, eid from vtocc_cached where eid = 1 and bid = 1")
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), "error: Type")
    else:
      self.fail("Did not receive exception")

  def test_uncache(self):
    try:
      # Verify row cache is working
      self.env.execute("select * from vtocc_cached where eid = 2 and bid = 'foo'")
      tstart = self.env.table_stats()["vtocc_cached"]
      self.env.execute("select * from vtocc_cached where eid = 2 and bid = 'foo'")
      tend = self.env.table_stats()["vtocc_cached"]
      self.assertEqual(tstart["Hits"]+1, tend["Hits"])
      # disable
      self.env.execute("alter table vtocc_cached comment 'vtocc_nocache'")
      self.env.execute("select * from vtocc_cached where eid = 2 and bid = 'foo'")
      try:
        tstart = self.env.table_stats()["vtocc_cached"]
      except KeyError:
        pass
      else:
        self.fail("Did not receive exception")
    finally:
      self.env.execute("alter table vtocc_cached comment ''")

    # Verify row cache is working again
    self.env.execute("select * from vtocc_cached where eid = 2 and bid = 'foo'")
    tstart = self.env.table_stats()["vtocc_cached"]
    self.env.execute("select * from vtocc_cached where eid = 2 and bid = 'foo'")
    tend = self.env.table_stats()["vtocc_cached"]
    self.assertEqual(tstart["Hits"]+1, tend["Hits"])

  def test_rename(self):
    try:
      # Verify row cache is working
      self.env.execute("select * from vtocc_cached where eid = 2 and bid = 'foo'")
      tstart = self.env.table_stats()["vtocc_cached"]
      self.env.execute("select * from vtocc_cached where eid = 2 and bid = 'foo'")
      tend = self.env.table_stats()["vtocc_cached"]
      self.assertEqual(tstart["Hits"]+1, tend["Hits"])
      # rename
      self.env.execute("alter table vtocc_cached rename to vtocc_cached2")
      try:
        tstart = self.env.table_stats()["vtocc_cached"]
      except KeyError:
        pass
      else:
        self.fail("Did not receive exception")
      # Verify row cache is working
      self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
      tstart = self.env.table_stats()["vtocc_cached2"]
      self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
      tend = self.env.table_stats()["vtocc_cached2"]
      self.assertEqual(tstart["Hits"]+1, tend["Hits"])
    finally:
      # alter table so there's no hash collision when renamed
      self.env.execute("alter table vtocc_cached2 comment 'renmaed'")
      self.env.execute("rename table vtocc_cached2 to vtocc_cached")

    # Verify row cache is working again
    self.env.execute("select * from vtocc_cached where eid = 2 and bid = 'foo'")
    tstart = self.env.table_stats()["vtocc_cached"]
    self.env.execute("select * from vtocc_cached where eid = 2 and bid = 'foo'")
    tend = self.env.table_stats()["vtocc_cached"]
    self.assertEqual(tstart["Hits"]+1, tend["Hits"])

  def test_nopass(self):
    try:
      self.env.conn.begin()
      self.env.execute("insert into vtocc_cached(eid, bid, name, foo) values(unix_time(), 'foo', 'bar', 'bar')")
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), "error: DML too complex")
    else:
      self.fail("Did not receive exception")
    finally:
      self.env.conn.rollback()

  def test_types(self):
    self._verify_mismatch("select * from vtocc_cached where eid = 'str' and bid = 'str'")
    self._verify_mismatch("select * from vtocc_cached where eid = %(str)s and bid = %(str)s", {"str": "str"})
    self._verify_mismatch("select * from vtocc_cached where eid = 1 and bid = 1")
    self._verify_mismatch("select * from vtocc_cached where eid = %(id)s and bid = %(id)s", {"id": 1})
    self._verify_mismatch("select * from vtocc_cached where eid = 1.2 and bid = 1.2")
    self._verify_mismatch("select * from vtocc_cached where eid = %(fl)s and bid = %(fl)s", {"fl": 1.2})

  def _verify_mismatch(self, query, bindvars=None):
    try:
      self.env.execute(query, bindvars)
    except dbexceptions.DatabaseError as e:
      self.assertContains(str(e), "error: Type mismatch")
    else:
      self.fail("Did not receive exception")

  def test_cache_sqls(self):
    error_count = self.env.run_cases(cache_cases.cases)
    if error_count != 0:
      self.fail("test_execution errors: %d" % error_count)
