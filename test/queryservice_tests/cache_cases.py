import datetime
from decimal import Decimal

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
      "SELECT_CACHE_RESULT", #  it currently doesn't cache
      ['select * from vtocc_cached',
       Case(query_plan="SELECT_CACHE_RESULT",
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

  # data types test
  MultiCase(
      'integer data types',
      ['begin',
       Case(sql="insert into vtocc_ints values(%(tiny)s, %(tinyu)s, %(small)s, %(smallu)s, %(medium)s, %(mediumu)s, %(normal)s, %(normalu)s, %(big)s, %(bigu)s, %(year)s)",
         bindings={"tiny": -128, "tinyu": 255, "small": -32768, "smallu": 65535, "medium": -8388608, "mediumu": 16777215, "normal": -2147483648, "normalu": 4294967295, "big": -9223372036854775808, "bigu": 18446744073709551615, "year": 2012},
         rewritten='insert into vtocc_ints values (-128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 2012) /* _stream vtocc_ints (tiny ) (-128 ); */'),
       'commit',
       Case(sql="select * from vtocc_ints where tiny = -128",
         result=[(-128, 255, -32768, 65535, -8388608, 16777215, -2147483648L, 4294967295L, -9223372036854775808L, 18446744073709551615L, 2012)],
         rewritten=[
           "select * from vtocc_ints where 1 != 1",
           "select tiny, tinyu, small, smallu, medium, mediumu, normal, normalu, big, bigu, y from vtocc_ints where tiny = -128"]),
       Case(sql="select * from vtocc_ints where tiny = -128",
         result=[(-128, 255, -32768, 65535, -8388608, 16777215, -2147483648L, 4294967295L, -9223372036854775808L, 18446744073709551615L, 2012)],
         rewritten=[]),
       'begin',
       Case(sql="insert into vtocc_ints select 2, tinyu, small, smallu, medium, mediumu, normal, normalu, big, bigu, y from vtocc_ints",
         rewritten=[
           "select 2, tinyu, small, smallu, medium, mediumu, normal, normalu, big, bigu, y from vtocc_ints limit 10001",
           "insert into vtocc_ints values (2, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 2012) /* _stream vtocc_ints (tiny ) (2 ); */"]),
       'commit',
       'begin',
       'delete from vtocc_ints',
       'commit']),

  MultiCase(
      'fractional data types',
      ['begin',
       Case(sql="insert into vtocc_fracts values(%(id)s, %(deci)s, %(num)s, %(f)s, %(d)s)",
         bindings={"id": 1, "deci": Decimal('1.99'), "num": Decimal('2.99'), "f": 3.99, "d": 4.99},
         rewritten="insert into vtocc_fracts values (1, '1.99', '2.99', 3.99, 4.99) /* _stream vtocc_fracts (id ) (1 ); */"),
       'commit',
       Case(sql="select * from vtocc_fracts where id = 1",
         result=[(1L, Decimal('1.99'), Decimal('2.99'), 3.9900000000000002, 4.9900000000000002)],
         rewritten=[
           "select * from vtocc_fracts where 1 != 1",
           "select id, deci, num, f, d from vtocc_fracts where id = 1"]),
       Case(sql="select * from vtocc_fracts where id = 1",
         result=[(1L, Decimal('1.99'), Decimal('2.99'), 3.9900000000000002, 4.9900000000000002)],
         rewritten=[]),
       'begin',
       Case(sql="insert into vtocc_fracts select 2, deci, num, f, d from vtocc_fracts",
         rewritten=[
           "select 2, deci, num, f, d from vtocc_fracts limit 10001",
           "insert into vtocc_fracts values (2, 1.99, 2.99, 3.99, 4.99) /* _stream vtocc_fracts (id ) (2 ); */"]),
       'commit',
       'begin',
       'delete from vtocc_fracts',
       'commit']),

  MultiCase(
      'string data types',
      ['begin',
       Case(sql="insert into vtocc_strings values(%(vb)s, %(c)s, %(vc)s, %(b)s, %(tb)s, %(bl)s, %(ttx)s, %(tx)s, %(en)s, %(s)s)",
         bindings={"vb": "a", "c": "b", "vc": "c", "b": "d", "tb": "e", "bl": "f", "ttx": "g", "tx": "h", "en": "a", "s": "a,b"},
         rewritten="insert into vtocc_strings values ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'a', 'a,b') /* _stream vtocc_strings (vb ) ('YQ==' ); */"),
       'commit',
       Case(sql="select * from vtocc_strings where vb = 'a'",
         result=[('a', 'b', 'c', 'd\x00\x00\x00', 'e', 'f', 'g', 'h', 'a', 'a,b')],
         rewritten=[
           "select * from vtocc_strings where 1 != 1",
           "select vb, c, vc, b, tb, bl, ttx, tx, en, s from vtocc_strings where vb = 'a'"]),
       Case(sql="select * from vtocc_strings where vb = 'a'",
         result=[('a', 'b', 'c', 'd\x00\x00\x00', 'e', 'f', 'g', 'h', 'a', 'a,b')],
         rewritten=[]),
       'begin',
       Case(sql="insert into vtocc_strings select 'b', c, vc, b, tb, bl, ttx, tx, en, s from vtocc_strings",
         rewritten=[
           "select 'b', c, vc, b, tb, bl, ttx, tx, en, s from vtocc_strings limit 10001",
           "insert into vtocc_strings values ('b', 'b', 'c', 'd\\0\\0\\0', 'e', 'f', 'g', 'h', 'a', 'a,b') /* _stream vtocc_strings (vb ) ('Yg==' ); */"]),
       'commit',
       'begin',
       'delete from vtocc_strings',
       'commit']),

  MultiCase(
      'misc data types',
      ['begin',
       Case(sql="insert into vtocc_misc values(%(id)s, %(b)s, %(d)s, %(dt)s, %(t)s)",
         bindings={"id": 1, "b": "\x01", "d": "2012-01-01", "dt": "2012-01-01 15:45:45", "t": "15:45:45"},
         rewritten="insert into vtocc_misc values (1, '\1', '2012-01-01', '2012-01-01 15:45:45', '15:45:45') /* _stream vtocc_misc (id ) (1 ); */"),
       'commit',
       Case(sql="select * from vtocc_misc where id = 1",
         result=[(1L, '\x01', datetime.date(2012, 1, 1), datetime.datetime(2012, 1, 1, 15, 45, 45), datetime.timedelta(0, 56745))],
         rewritten=[
           "select * from vtocc_misc where 1 != 1",
           "select id, b, d, dt, t from vtocc_misc where id = 1"]),
       Case(sql="select * from vtocc_misc where id = 1",
         result=[(1L, '\x01', datetime.date(2012, 1, 1), datetime.datetime(2012, 1, 1, 15, 45, 45), datetime.timedelta(0, 56745))],
         rewritten=[]),
       'begin',
       Case(sql="insert into vtocc_misc select 2, b, d, dt, t from vtocc_misc",
         rewritten=[
           "select 2, b, d, dt, t from vtocc_misc limit 10001",
           "insert into vtocc_misc values (2, '\x01', '2012-01-01', '2012-01-01 15:45:45', '15:45:45') /* _stream vtocc_misc (id ) (2 ); */"]),
       'commit',
       'begin',
       'delete from vtocc_misc',
       'commit']),
]
