from cases_framework import Case, MultiCase

# Covers cases for vtocc_cached2

class Case2(Case):
  def __init__(self, **kwargs):
    Case.__init__(self, cache_table='vtocc_cached2', **kwargs)


cases = [
  "alter table vtocc_cached2 comment 'new'",

  Case2(doc="PK_EQUAL (null key)",
       query_plan="PK_EQUAL",
       sql="select * from vtocc_cached2 where eid = 2 and bid = :bid",
       bindings={"bid": None},
       result=[],
       rewritten=[
         "select * from vtocc_cached2 where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached2 where (eid, bid) = (2, null)"],
       cache_absent=1),

  Case2(doc="PK_EQUAL (empty cache)",
       query_plan="PK_EQUAL",
       sql="select * from vtocc_cached2 where eid = 2 and bid = 'foo'",
       result=[(2, 'foo', 'abcd2', 'efgh')],
       rewritten=[
         "select * from vtocc_cached2 where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached2 where (eid, bid) = (2, 'foo')"],
       cache_misses=1),
  # (2.foo) is in cache

  Case2(doc="PK_EQUAL, use cache",
       query_plan="PK_EQUAL",
       sql="select bid, eid, name, foo from vtocc_cached2 where eid = 2 and bid = 'foo'",
       result=[('foo', 2, 'abcd2', 'efgh')],
       rewritten=["select bid, eid, name, foo from vtocc_cached2 where 1 != 1"],
       cache_hits=1),
  # (2.foo) is in cache

  Case2(doc="PK_EQUAL, absent",
       query_plan="PK_EQUAL",
       sql="select bid, eid, name, foo from vtocc_cached2 where eid = 3 and bid = 'foo'",
       result=[],
       rewritten=[
         "select bid, eid, name, foo from vtocc_cached2 where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached2 where (eid, bid) = (3, 'foo')"],
       cache_absent=1),
  # (2.foo)

  Case2(doc="out of order columns list",
       sql="select bid, eid from vtocc_cached2 where eid = 1 and bid = 'foo'",
       result=[('foo', 1)],
       rewritten=[
         "select bid, eid from vtocc_cached2 where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached2 where (eid, bid) = (1, 'foo')"],
       cache_misses=1),
  # (1.foo, 2.foo)

  Case2(doc="out of order columns list, use cache",
       sql="select bid, eid from vtocc_cached2 where eid = 1 and bid = 'foo'",
       result=[('foo', 1)],
       rewritten=[],
       cache_hits=1),
  # (1.foo, 2.foo)

  Case2(doc="pk_in for composite pk table, two fetches from db (absent)",
       query_plan="PK_IN",
       sql="select eid, bid, name, foo from vtocc_cached2 where eid = 1 and bid in('absent1', 'absent2')",
       result=[],
       rewritten=[
         "select eid, bid, name, foo from vtocc_cached2 where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached2 where (eid, bid) = (1, 'absent1') or (eid, bid) = (1, 'absent2')"],
       cache_hits=0,
       cache_misses=0,
       cache_absent=2,
       cache_invalidations=0),
  # (1.foo, 1.bar, 2.foo)

  Case2(doc="pk_in for composite pk table, 1 fetch from db",
       query_plan="PK_IN",
       sql="select eid, bid, name, foo from vtocc_cached2 where eid = 1 and bid in('foo', 'bar')",
       result=[(1L, 'foo', 'abcd1', 'efgh'), (1L, 'bar', 'abcd1', 'efgh')],
       rewritten=[
         "select eid, bid, name, foo from vtocc_cached2 where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached2 where (eid, bid) = (1, 'bar')"],
       cache_hits=1,
       cache_misses=1,
       cache_absent=0,
       cache_invalidations=0),
  # (1.foo, 1.bar, 2.foo)

  Case2(doc="pk_in for composite pk table, 0 fetch from db",
       query_plan="PK_IN",
       sql="select eid, bid, name, foo from vtocc_cached2 where eid = 1 and bid in('foo', 'bar')",
       result=[(1L, 'foo', 'abcd1', 'efgh'), (1L, 'bar', 'abcd1', 'efgh')],
       rewritten=[],
       cache_hits=2,
       cache_misses=0,
       cache_absent=0,
       cache_invalidations=0),
  # (1.foo, 1.bar, 2.foo)

  Case2(doc="select_subquery for composite pk table, 1 fetch from db",
       query_plan="SELECT_SUBQUERY",
       sql="select eid, bid, name, foo from vtocc_cached2 where eid = 2 and name='abcd2'",
       result=[(2L, 'foo', 'abcd2', 'efgh'), (2L, 'bar', 'abcd2', 'efgh')],
       rewritten=[
         "select eid, bid, name, foo from vtocc_cached2 where 1 != 1",
         "select eid, bid from vtocc_cached2 use index (aname2) where eid = 2 and name = 'abcd2' limit 10001",
         "select eid, bid, name, foo from vtocc_cached2 where (eid, bid) = (2, 'bar')"],
       cache_hits=1,
       cache_misses=1,
       cache_absent=0,
       cache_invalidations=0),
  # (1.foo, 1.bar, 2.foo, 2.bar)

  Case2(doc="verify 1.bar is in cache",
       sql="select bid, eid from vtocc_cached2 where eid = 1 and bid = 'bar'",
       result=[('bar', 1)],
       rewritten=[
         "select bid, eid from vtocc_cached2 where 1 != 1"],
       cache_hits=1),
  # (1.foo, 1.bar, 2.foo, 2.bar)

  MultiCase(
      "update",
      ['begin',
       "update vtocc_cached2 set foo='fghi' where bid = 'bar'",
       Case2(sql="commit",
            cache_invalidations=2),
       Case2(sql="select * from vtocc_cached2 where eid = 1 and bid = 'bar'",
            result=[(1L, 'bar', 'abcd1', 'fghi')],
            rewritten=[
                "select * from vtocc_cached2 where 1 != 1",
                "select eid, bid, name, foo from vtocc_cached2 where (eid, bid) = (1, 'bar')"],
            cache_misses=1)]),
  # (1.foo, 1.bar, 2.foo, 2.bar)

  MultiCase(
      "this will not invalidate the cache",
      ['begin',
       "update vtocc_cached2 set foo='fghi' where bid = 'bar'",
       'rollback',
       Case2(sql="select * from vtocc_cached2 where eid = 1 and bid = 'bar'",
            result=[(1L, 'bar', 'abcd1', 'fghi')],
            rewritten=[],
            cache_hits=1)]),
  # (1.foo, 1.bar, 2.foo, 2.bar)

  MultiCase(
      "delete",
      ['begin',
       "delete from vtocc_cached2 where eid = 1 and bid = 'bar'",
       Case2(sql="commit",
            cache_invalidations=1),
       Case2(sql="select * from vtocc_cached2 where eid = 1 and bid = 'bar'",
            result=[],
            rewritten="select eid, bid, name, foo from vtocc_cached2 where (eid, bid) = (1, 'bar')",
            cache_absent=1),
       "begin",
       "insert into vtocc_cached2(eid, bid, name, foo) values (1, 'bar', 'abcd1', 'efgh')",
       Case2(sql="commit",
            cache_invalidations=0)]),
  # (1.foo, 2.foo, 2.bar)

  Case2(doc="Verify 1.foo is in cache",
       sql="select * from vtocc_cached2 where eid = 1 and bid = 'foo'",
       result=[(1, 'foo', 'abcd1', 'efgh')],
       rewritten=["select * from vtocc_cached2 where 1 != 1"],
       cache_hits=1),
  # (1.foo, 2.foo, 2.bar)

  # DDL
  "alter table vtocc_cached2 comment 'test'",

  Case2(doc="Verify cache is empty after DDL",
       sql="select * from vtocc_cached2 where eid = 1 and bid = 'foo'",
       result=[(1, 'foo', 'abcd1', 'efgh')],
       rewritten=[
         "select * from vtocc_cached2 where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached2 where (eid, bid) = (1, 'foo')"],
       cache_misses=1),

  # (1.foo)
  Case2(doc="Verify row is cached",
       sql="select * from vtocc_cached2 where eid = 1 and bid = 'foo'",
       result=[(1, 'foo', 'abcd1', 'efgh')],
       rewritten=[],
       cache_hits=1),
  # (1.foo)
]
