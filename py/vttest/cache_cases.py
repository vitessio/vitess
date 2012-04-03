# Copyright 2012, Google Inc.
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:

#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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
