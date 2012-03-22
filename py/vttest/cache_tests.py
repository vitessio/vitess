from vtdb import vt_occ2 as db

from vttest import framework
from vttest import cache_cases

class TestCache(framework.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def set_env(self, env):
    self.env = env

  def test_num_str(self):
    try:
      self.env.execute("select bid, eid from vtocc_cached where eid = 1 and bid = 1")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: Type")

  def test_nocache(self):
    try:
      self.env.log.reset()
      self.env.execute("create table vtocc_nocache(eid int, primary key (eid)) comment 'vtocc_nocache'")
      self.assertContains(self.env.log.read(), "Will not be cached")
    finally:
      self.env.execute("drop table vtocc_nocache")

  def test_nopk(self):
    try:
      self.env.log.reset()
      self.env.execute("create table vtocc_nocache(eid int)")
      self.assertContains(self.env.log.read(), "Will not be cached")
    finally:
      self.env.execute("drop table vtocc_nocache")

  def test_charcol(self):
    try:
      self.env.log.reset()
      self.env.execute("create table vtocc_nocache(eid varchar(10), primary key (eid))")
      self.assertContains(self.env.log.read(), "Will not be cached")
    finally:
      self.env.execute("drop table vtocc_nocache")

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
        assertFail("KeyError expected")
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
        assertFail("KeyError expected")
      # Verify row cache is working
      self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
      tstart = self.env.table_stats()["vtocc_cached2"]
      self.env.execute("select * from vtocc_cached2 where eid = 2 and bid = 'foo'")
      tend = self.env.table_stats()["vtocc_cached2"]
      self.assertEqual(tstart["Hits"]+1, tend["Hits"])
    finally:
      self.env.execute("rename table vtocc_cached2 to vtocc_cached")

    # Verify row cache is working again
    self.env.execute("select * from vtocc_cached where eid = 2 and bid = 'foo'")
    tstart = self.env.table_stats()["vtocc_cached"]
    self.env.execute("select * from vtocc_cached where eid = 2 and bid = 'foo'")
    tend = self.env.table_stats()["vtocc_cached"]
    self.assertEqual(tstart["Hits"]+1, tend["Hits"])

  def test_nopass(self):
    try:
      self.env.execute("begin")
      self.env.execute("insert into vtocc_cached(eid, bid, name, foo) values(unix_time(), 'foo', 'bar', 'bar')")
    except (db.MySQLErrors.DatabaseError, db.dbexceptions.OperationalError), e:
      self.assertContains(e[1], "error: DML too complex")
    else:
      self.assertFail("Did not receive exception")
    finally:
      self.env.execute("rollback")

  def test_sqls(self):
    error_count = self.env.run_cases(cache_cases.cache_cases)
    if error_count != 0:
      self.assertFail("test_execution errors: %d"%(error_count))
