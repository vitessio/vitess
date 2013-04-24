from cases_framework import Case, MultiCase

cases = [
  "alter table vtocc_cached comment 'new'",
  Case(doc="SELECT_PK (null key)",
       query_plan="SELECT_PK",
       sql="select * from vtocc_cached where eid = 2 and bid = %(bid)s",
       bindings={"bid": None},
       result=[],
       rewritten=[
         "select * from vtocc_cached where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached where eid = 2 and bid = null"],
       cache_absent=1),

  Case(doc="SELECT_PK (empty cache)",
       query_plan="SELECT_PK",
       sql="select * from vtocc_cached where eid = 2 and bid = 'foo'",
       result=[(2, 'foo', 'abcd2', 'efgh')],
       rewritten=[
         "select * from vtocc_cached where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached where eid = 2 and bid = 'foo'"],
       cache_misses=1),
  # (2.foo) is in cache

  Case(doc="SELECT_PK, use cache",
       query_plan="SELECT_PK",
       sql="select bid, eid, name, foo from vtocc_cached where eid = 2 and bid = 'foo'",
       result=[('foo', 2, 'abcd2', 'efgh')],
       rewritten=["select bid, eid, name, foo from vtocc_cached where 1 != 1"],
       cache_hits=1),
  # (2.foo) is in cache

  Case(doc="SELECT_PK, absent",
       query_plan="SELECT_PK",
       sql="select bid, eid, name, foo from vtocc_cached where eid = 3 and bid = 'foo'",
       result=[],
       rewritten=[
         "select bid, eid, name, foo from vtocc_cached where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached where eid = 3 and bid = 'foo'"],
       cache_absent=1),
  # (2.foo)

  Case(doc="SELECT_SUBQUERY (2.foo)",
       sql="select * from vtocc_cached where eid = 2 and name = 'abcd2'",
       result=[(2L, 'bar', 'abcd2', 'efgh'), (2L, 'foo', 'abcd2', 'efgh')],
       rewritten=[
         "select * from vtocc_cached where 1 != 1",
         "select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001",
         "select eid, bid, name, foo from vtocc_cached where eid = 2 and bid = 'bar'"],
       cache_hits=1,
       cache_misses=1),
  # (2.bar, 2.foo)

  Case(doc="SELECT_SUBQUERY (2.foo, 2.bar)",
       sql="select * from vtocc_cached where eid = 2 and name = 'abcd2'",
       result=[(2L, 'bar', 'abcd2', 'efgh'), (2L, 'foo', 'abcd2', 'efgh')],
       rewritten="select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001",
       cache_hits=2),
  # (2.bar, 2.foo)

  Case(doc="out of order columns list",
       sql="select bid, eid from vtocc_cached where eid = 1 and bid = 'foo'",
       result=[('foo', 1)],
       rewritten=[
         "select bid, eid from vtocc_cached where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached where eid = 1 and bid = 'foo'"],
       cache_misses=1),
  # (1.foo, 2.bar, 2.foo)

  Case(doc="out of order columns list, use cache",
       sql="select bid, eid from vtocc_cached where eid = 1 and bid = 'foo'",
       result=[('foo', 1)],
       rewritten=[],
       cache_hits=1),
  # (1.foo, 2.bar, 2.foo)

  MultiCase(
      "PASS_SELECT", #  it currently doesn't cache
      ['select * from vtocc_cached',
       Case(query_plan="PASS_SELECT",
            sql="select eid, bid, name, foo from vtocc_cached",
            rewritten=[
              "select eid, bid, name, foo from vtocc_cached where 1 != 1",
              "select eid, bid, name, foo from vtocc_cached limit 10001"],
            cache_hits=0,
            cache_misses=0,
            cache_absent=0,
            cache_invalidations=0)]),
      # (1.foo, 2.bar, 2.foo)


  Case(doc="verify 1.bar is not cached",
       sql="select bid, eid from vtocc_cached where eid = 1 and bid = 'bar'",
       result=[('bar', 1)],
       rewritten=[
         "select bid, eid from vtocc_cached where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached where eid = 1 and bid = 'bar'"],
       cache_misses=1),
  # (1.foo, 1.bar, 2.foo, 2.bar)

  MultiCase(
      "update",
      ['begin',
       "update vtocc_cached set foo='fghi' where bid = 'bar'",
       Case(sql="commit",
            cache_invalidations=2),
       Case(sql="select * from vtocc_cached where eid = 2 and name = 'abcd2'",
            result=[(2L, 'bar', 'abcd2', 'fghi'), (2L, 'foo', 'abcd2', 'efgh')],
            rewritten=[
                "select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001",
                "select eid, bid, name, foo from vtocc_cached where eid = 2 and bid = 'bar'"],
            cache_hits=1,
            cache_misses=1)]),
  # (1.foo, 2.foo, 2.bar)

  MultiCase(
      "Verify cache",
      ["select sleep(0.2) from dual",
       Case(sql="select * from vtocc_cached where eid = 2 and name = 'abcd2'",
            result=[(2L, 'bar', 'abcd2', 'fghi'), (2L, 'foo', 'abcd2', 'efgh')],
            rewritten="select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001",
            cache_hits=2)]),

  Case(doc="this will use the cache",
       sql="select * from vtocc_cached where eid = 2 and name = 'abcd2'",
       result=[(2L, 'bar', 'abcd2', 'fghi'), (2L, 'foo', 'abcd2', 'efgh')],
       rewritten="select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001",
       cache_hits=2),
      # (1.foo, 2.bar, 2.foo)

  MultiCase(
      "this will not invalidate the cache",
      ['begin',
       "update vtocc_cached set foo='fghi' where bid = 'bar'",
       'rollback',
       Case(sql="select * from vtocc_cached where eid = 2 and name = 'abcd2'",
            result=[(2L, 'bar', 'abcd2', 'fghi'), (2L, 'foo', 'abcd2', 'efgh')],
            rewritten="select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001",
            cache_hits=2)]),
  # (1.foo, 2.bar, 2.foo)

  MultiCase(
      "delete",
      ['begin',
       "delete from vtocc_cached where eid = 2 and bid = 'bar'",
       Case(sql="commit",
            cache_invalidations=1),
       Case(sql="select * from vtocc_cached where eid = 2 and name = 'abcd2'",
            result=[(2L, 'foo', 'abcd2', 'efgh')],
            rewritten="select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001",
            cache_hits=1),
       "begin",
       "insert into vtocc_cached(eid, bid, name, foo) values (2, 'bar', 'abcd2', 'efgh')",
       Case(sql="commit",
            cache_invalidations=1)]),
  # (1.foo, 2.foo)

  MultiCase(
      "insert on dup key",
      ['begin',
       "insert into vtocc_cached(eid, bid, name, foo) values (2, 'foo', 'abcd2', 'efgh') on duplicate key update foo='fghi'",
       Case(sql="commit",
            cache_invalidations=1),
       Case(sql="select * from vtocc_cached where eid = 2 and name = 'abcd2'",
            result=[(2L, 'bar', 'abcd2', 'efgh'), (2L, 'foo', 'abcd2', 'fghi')],
            rewritten=[
                "select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001",
                "select eid, bid, name, foo from vtocc_cached where eid = 2 and bid = 'bar'",
                "select eid, bid, name, foo from vtocc_cached where eid = 2 and bid = 'foo'"],
            cache_misses=2)]),
  # (1.foo)

  Case(doc="Verify 1.foo is in cache",
       sql="select * from vtocc_cached where eid = 1 and bid = 'foo'",
       result=[(1, 'foo', 'abcd1', 'efgh')],
       rewritten=["select * from vtocc_cached where 1 != 1"],
       cache_hits=1),
  # (1.foo) is in cache

  # DDL
  "alter table vtocc_cached comment 'test'",

  Case(doc="Verify cache is empty after DDL",
       sql="select * from vtocc_cached where eid = 1 and bid = 'foo'",
       result=[(1, 'foo', 'abcd1', 'efgh')],
       rewritten=[
         "select * from vtocc_cached where 1 != 1",
         "select eid, bid, name, foo from vtocc_cached where eid = 1 and bid = 'foo'"],
       cache_misses=1),

  # (1.foo)
  Case(doc="Verify row is cached",
       sql="select * from vtocc_cached where eid = 1 and bid = 'foo'",
       result=[(1, 'foo', 'abcd1', 'efgh')],
       rewritten=[],
       cache_hits=1),
  # (1.foo)
]
