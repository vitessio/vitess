from cases_framework import Case, MultiCase

# Covers cases for vtocc_cached1

class Case1(Case):
  def __init__(self, **kwargs):
    Case.__init__(self, cache_table='vtocc_cached1', **kwargs)

cases = [
  "alter table vtocc_cached1 comment 'new'",

  Case1(doc="PK_EQUAL (empty cache)",
       query_plan="PK_EQUAL",
       sql="select * from vtocc_cached1 where eid = 1",
       result=[(1L, 'a', 'abcd')],
       rewritten=[
         "select * from vtocc_cached1 where 1 != 1",
         "select eid, name, foo from vtocc_cached1 where eid in (1)"],
       cache_misses=1),
  # (1) is in cache

  Case1(doc="PK_EQUAL, use cache",
       query_plan="PK_EQUAL",
       sql="select * from vtocc_cached1 where eid = 1",
       result=[(1L, 'a', 'abcd')],
       rewritten=[],
       cache_hits=1),
  # (1)

  Case1(doc="PK_IN (empty cache)",
       query_plan="PK_IN",
       sql="select * from vtocc_cached1 where eid in (1, 3, 6)",
       result=[(1L, 'a', 'abcd'), (3L, 'c', 'abcd')],
       rewritten=[
         "select * from vtocc_cached1 where 1 != 1",
         "select eid, name, foo from vtocc_cached1 where eid in (3, 6)"],
       cache_hits=1,
       cache_misses=1,
       cache_absent=1),
  # (1, 3)

  Case1(doc="SELECT_SUBQUERY (1, 2)",
       sql="select * from vtocc_cached1 where name = 'a'",
       result=[(1L, 'a', 'abcd'), (2L, 'a', 'abcd')],
       rewritten=[
         "select * from vtocc_cached1 where 1 != 1",
         "select eid from vtocc_cached1 use index (aname1) where name = 'a' limit 10001",
         "select eid, name, foo from vtocc_cached1 where eid in (2)"],
       cache_hits=1,
       cache_misses=1),
  # (1, 2, 3)

  Case1(doc="covering index",
       query_plan="PASS_SELECT",
       sql="select eid, name from vtocc_cached1 where name = 'a'",
       result=[(1L, 'a'), (2L, 'a')],
       rewritten=[
         "select eid, name from vtocc_cached1 where 1 != 1",
         "select eid, name from vtocc_cached1 where name = 'a' limit 10001"]),
  # (1, 2, 3)

  Case1(doc="SELECT_SUBQUERY (1, 2)",
       sql="select * from vtocc_cached1 where name = 'a'",
       result=[(1L, 'a', 'abcd'), (2L, 'a', 'abcd')],
       rewritten=["select eid from vtocc_cached1 use index (aname1) where name = 'a' limit 10001"],
       cache_hits=2),
  # (1, 2, 3)

  Case1(doc="SELECT_SUBQUERY (4, 5)",
       sql="select * from vtocc_cached1 where name between 'd' and 'e'",
       result=[(4L, 'd', 'abcd'), (5L, 'e', 'efgh')],
       rewritten=[
         "select * from vtocc_cached1 where 1 != 1",
         "select eid from vtocc_cached1 use index (aname1) where name between 'd' and 'e' limit 10001",
         "select eid, name, foo from vtocc_cached1 where eid in (4, 5)"],
       cache_hits=0,
       cache_misses=2),
  # (1, 2, 3, 4, 5)

  Case1(doc="PASS_SELECT",
      sql="select * from vtocc_cached1 where foo='abcd'",
      result=[(1L, 'a', 'abcd'), (2L, 'a', 'abcd'), (3L, 'c', 'abcd'), (4L, 'd', 'abcd')],
      rewritten=[
        "select * from vtocc_cached1 where 1 != 1",
        "select * from vtocc_cached1 where foo = 'abcd' limit 10001"],
      cache_hits=0, cache_misses=0, cache_absent=0),
  # (1, 2, 3)
]
