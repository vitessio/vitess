# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

cache_cases = [
  ["alter table vtocc_cached comment 'new'"],
  # SELECT_PK (empty cache)
  [
    "select * from vtocc_cached where eid = 2 and bid = 'foo'", {},
    [(2, 'foo', 'abcd2', 'efgh')],
    ["select eid, bid, name, foo from vtocc_cached where eid = 2 and bid = 'foo'"],
    ['vtocc_cached', 0, 0, 1, 0],
  ], # (2.foo) is in cache

  # SELECT_PK, use cache
  [
    "select bid, eid, name, foo from vtocc_cached where eid = 2 and bid = 'foo'", {},
    [('foo', 2, 'abcd2', 'efgh')],
    [],
    ['vtocc_cached', 1, 0, 0, 0],
  ], # (2.foo)

  # SELECT_PK, absent
  [
    "select bid, eid, name, foo from vtocc_cached where eid = 3 and bid = 'foo'", {},
    [],
    ["select eid, bid, name, foo from vtocc_cached where eid = 3 and bid = 'foo'"],
    ['vtocc_cached', 0, 1, 0, 0],
  ], # (2.foo)

  # SELECT_PK, number as string
  [
    "select bid, eid, name, foo from vtocc_cached where eid = '0x2' and bid = 'foo'", {},
    [('foo', 2, 'abcd2', 'efgh')],
    [],
    ['vtocc_cached', 1, 0, 0, 0],
  ], # (2.foo)

  # SELECT_SUBQUERY (2.foo)
  [
    "select * from vtocc_cached where eid = 2 and name = 'abcd2'", {},
    [(2L, 'bar', 'abcd2', 'efgh'), (2L, 'foo', 'abcd2', 'efgh')],
    [
      "select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001",
      "select eid, bid, name, foo from vtocc_cached where eid = 2 and bid = 'bar'"
    ],
    ['vtocc_cached', 1, 0, 1, 0],
  ], # (2.bar, 2.foo)

  # SELECT_SUBQUERY (2.foo, 2.bar)
  [
    "select * from vtocc_cached where eid = 2 and name = 'abcd2'", {},
    [(2L, 'bar', 'abcd2', 'efgh'), (2L, 'foo', 'abcd2', 'efgh')],
    ["select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001"],
    ['vtocc_cached', 2, 0, 0, 0],
  ], # (2.bar, 2.foo)

  # out of order columns list
  [
    "select bid, eid from vtocc_cached where eid = 1 and bid = 'foo'", {},
    [('foo', 1)],
    ["select eid, bid, name, foo from vtocc_cached where eid = 1 and bid = 'foo'"],
    ['vtocc_cached', 0, 0, 1, 0],
  ], # (1.foo, 2.bar, 2.foo)

  # out of order columns list, use cache
  [
    "select bid, eid from vtocc_cached where eid = 1 and bid = 'foo'", {},
    [('foo', 1)],
    [],
    ['vtocc_cached', 1, 0, 0, 0],
  ], # (1.foo, 2.bar, 2.foo)

  # SELECT_CACHE_RESULT (it currently doesn't cache)
  ['select * from vtocc_cached'],
  [
    "select eid, bid, name, foo from vtocc_cached", {},
    None,
    ["select eid, bid, name, foo from vtocc_cached limit 10001"],
    ['vtocc_cached', 0, 0, 0, 0],
  ], # (1.foo, 2.bar, 2.foo)

  # verify 1.bar is not cached
  [
    "select bid, eid from vtocc_cached where eid = 1 and bid = 'bar'", {},
    [('bar', 1)],
    ["select eid, bid, name, foo from vtocc_cached where eid = 1 and bid = 'bar'"],
    ['vtocc_cached', 0, 0, 1, 0],
  ], # (1.foo, 1.bar, 2.foo, 2.bar)

  # update
  ['begin'],
  ["update vtocc_cached set foo='fghi' where bid = 'bar'"],
  [
    "commit", {}, None, None,
    ['vtocc_cached', 0, 0, 0, 2],
  ],
  [
    "select * from vtocc_cached where eid = 2 and name = 'abcd2'", {},
    [(2L, 'bar', 'abcd2', 'fghi'), (2L, 'foo', 'abcd2', 'efgh')],
    [
      "select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001",
      "select eid, bid, name, foo from vtocc_cached where eid = 2 and bid = 'bar'"
    ],
    ['vtocc_cached', 1, 0, 1, 0],
  ], # (1.foo, 2.foo, 2.bar)

  # Verify cache
  ["select sleep(0.2) from dual"],
  [
    "select * from vtocc_cached where eid = 2 and name = 'abcd2'", {},
    [(2L, 'bar', 'abcd2', 'fghi'), (2L, 'foo', 'abcd2', 'efgh')],
    ["select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001"],
    ['vtocc_cached', 2, 0, 0, 0],
  ], # (1.foo, 2.bar, 2.foo)

  # this will use the cache
  [
    "select * from vtocc_cached where eid = 2 and name = 'abcd2'", {},
    [(2L, 'bar', 'abcd2', 'fghi'), (2L, 'foo', 'abcd2', 'efgh')],
    ["select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001"],
    ['vtocc_cached', 2, 0, 0, 0],
  ], # (1.foo, 2.bar, 2.foo)

  # this will not invalidate the cache
  ['begin'],
  ["update vtocc_cached set foo='fghi' where bid = 'bar'"],
  ["rollback"],
  [
    "select * from vtocc_cached where eid = 2 and name = 'abcd2'", {},
    [(2L, 'bar', 'abcd2', 'fghi'), (2L, 'foo', 'abcd2', 'efgh')],
    ["select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001"],
    ['vtocc_cached', 2, 0, 0, 0],
  ], # (1.foo, 2.bar, 2.foo)

  # delete
  ['begin'],
  ["delete from vtocc_cached where eid = 2 and bid = 'bar'"],
  [
    "commit", {}, None, None,
    ['vtocc_cached', 0, 0, 0, 1],
  ],
  [
    "select * from vtocc_cached where eid = 2 and name = 'abcd2'", {},
    [(2L, 'foo', 'abcd2', 'efgh')],
    ["select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001"],
    ['vtocc_cached', 1, 0, 0, 0],
  ],
  ["begin"],
  ["insert into vtocc_cached(eid, bid, name, foo) values (2, 'bar', 'abcd2', 'efgh')"],
  [
    "commit", {}, None, None,
    ['vtocc_cached', 0, 0, 0, 1],
  ], # (1.foo, 2.foo)

  # insert on dup key
  ['begin'],
  ["insert into vtocc_cached(eid, bid, name, foo) values (2, 'foo', 'abcd2', 'efgh') on duplicate key update foo='fghi'"],
  [
    "commit", {}, None, None,
    ['vtocc_cached', 0, 0, 0, 1],
  ],
  [
    "select * from vtocc_cached where eid = 2 and name = 'abcd2'", {},
    [(2L, 'bar', 'abcd2', 'efgh'), (2L, 'foo', 'abcd2', 'fghi')],
    [
      "select eid, bid from vtocc_cached use index (aname) where eid = 2 and name = 'abcd2' limit 10001",
      "select eid, bid, name, foo from vtocc_cached where eid = 2 and bid = 'bar'",
      "select eid, bid, name, foo from vtocc_cached where eid = 2 and bid = 'foo'"
    ],
    ['vtocc_cached', 0, 0, 2, 0],
  ], # (1.foo)

  # Verify 1.foo is in cache
  [
    "select * from vtocc_cached where eid = 1 and bid = 'foo'", {},
    [(1, 'foo', 'abcd1', 'efgh')],
    [],
    ['vtocc_cached', 1, 0, 0, 0],
  ], # (1.foo) is in cache

  # DDL
  ["alter table vtocc_cached comment 'test'"],
  # Verify cache is empty
  [
    "select * from vtocc_cached where eid = 1 and bid = 'foo'", {},
    [(1, 'foo', 'abcd1', 'efgh')],
    ["select eid, bid, name, foo from vtocc_cached where eid = 1 and bid = 'foo'"],
    ['vtocc_cached', 0, 0, 1, 0],
  ], # (1.foo)
  # Verify row is cached
  [
    "select * from vtocc_cached where eid = 1 and bid = 'foo'", {},
    [(1, 'foo', 'abcd1', 'efgh')],
    [],
    ['vtocc_cached', 1, 0, 0, 0],
  ], # (1.foo)
]
