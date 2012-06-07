# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

nocache_cases = [
  # union
  [
    'select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b', {},
    [(1L, 1L), (1L, 2L)],
    ['select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b'],
  ],

  # distinct
  [
    'select /* distinct */ distinct * from vtocc_a', {},
    [(1L, 1L, 'abcd', 'efgh'), (1L, 2L, 'bcde', 'fghi')],
    ['select /* distinct */ distinct * from vtocc_a limit 10001'],
  ],

  # group by
  [
    'select /* group by */ eid, sum(id) from vtocc_a group by eid', {},
    [(1L, 3L)],
    ['select /* group by */ eid, sum(id) from vtocc_a group by eid limit 10001'],
  ],

  # having
  [
    'select /* having */ sum(id) from vtocc_a having sum(id) = 3', {},
    [(3L,)],
    ['select /* having */ sum(id) from vtocc_a having sum(id) = 3 limit 10001'],
  ],

  # limit
  [
    'select /* limit */ eid, id from vtocc_a limit %(a)s', {"a": 1},
    [(1L, 1L)],
    ['select /* limit */ eid, id from vtocc_a limit 1'],
  ],

  # multi-table
  [
    'select /* multi-table */ a.eid, a.id, b.eid, b.id  from vtocc_a as a, vtocc_b as b', {},
    [(1L, 1L, 1L, 1L), (1L, 2L, 1L, 1L), (1L, 1L, 1L, 2L), (1L, 2L, 1L, 2L)],
    ['select /* multi-table */ a.eid, a.id, b.eid, b.id from vtocc_a as a, vtocc_b as b limit 10001'],
  ],

  # multi-table join
  [
    'select /* multi-table join */ a.eid, a.id, b.eid, b.id from vtocc_a as a join vtocc_b as b on a.eid = b.eid and a.id = b.id', {},
    [(1L, 1L, 1L, 1L), (1L, 2L, 1L, 2L)],
    ['select /* multi-table join */ a.eid, a.id, b.eid, b.id from vtocc_a as a join vtocc_b as b on a.eid = b.eid and a.id = b.id limit 10001'],
  ],

  # complex select list
  [
    'select /* complex select list */ eid+1, id from vtocc_a', {},
    [(2L, 1L), (2L, 2L)],
    ['select /* complex select list */ eid+1, id from vtocc_a limit 10001'],
  ],

  # *
  [
    'select /* * */ * from vtocc_a', {},
    [(1L, 1L, 'abcd', 'efgh'), (1L, 2L, 'bcde', 'fghi')],
    ['select /* * */ * from vtocc_a limit 10001'],
  ],

  # table alias
  [
    'select /* table alias */ a.eid from vtocc_a as a where a.eid=1', {},
    [(1L,), (1L,)],
    ['select /* table alias */ a.eid from vtocc_a as a where a.eid = 1 limit 10001'],
  ],

  # parenthesised col
  [
    'select /* parenthesised col */ (eid) from vtocc_a where eid = 1 and id = 1', {},
    [(1L,)],
    ['select /* parenthesised col */ eid from vtocc_a where eid = 1 and id = 1 limit 10001'],
  ],

  # for update
  ['begin'],
  [
    'select /* for update */ eid from vtocc_a where eid = 1 and id = 1 for update', {},
    [(1L,)],
    ['select /* for update */ eid from vtocc_a where eid = 1 and id = 1 limit 10001 for update'],
  ],
  ['commit'],

  # complex where
  [
    'select /* complex where */ id from vtocc_a where id+1 = 2', {},
    [(1L,)],
    ['select /* complex where */ id from vtocc_a where id+1 = 2 limit 10001'],
  ],

  # complex where (non-value operand)
  [
    'select /* complex where (non-value operand) */ eid, id from vtocc_a where eid = id', {},
    [(1L, 1L)],
    ['select /* complex where (non-value operand) */ eid, id from vtocc_a where eid = id limit 10001'],
  ],

  # (condition)
  [
    'select /* (condition) */ * from vtocc_a where (eid = 1)', {},
    [(1L, 1L, 'abcd', 'efgh'), (1L, 2L, 'bcde', 'fghi')],
    ['select /* (condition) */ * from vtocc_a where (eid = 1) limit 10001'],
  ],

  # inequality
  [
    'select /* inequality */ * from vtocc_a where id > 1', {},
    [(1L, 2L, 'bcde', 'fghi')],
    ['select /* inequality */ * from vtocc_a where id > 1 limit 10001'],
  ],

  # in
  [
    'select /* in */ * from vtocc_a where id in (1, 2)', {},
    [(1L, 1L, 'abcd', 'efgh'), (1L, 2L, 'bcde', 'fghi')],
    ['select /* in */ * from vtocc_a where id in (1, 2) limit 10001'],
  ],

  # between
  [
    'select /* between */ * from vtocc_a where id between 1 and 2', {},
    [(1L, 1L, 'abcd', 'efgh'), (1L, 2L, 'bcde', 'fghi')],
    ['select /* between */ * from vtocc_a where id between 1 and 2 limit 10001'],
  ],

  # order
  [
    'select /* order */ * from vtocc_a order by id desc', {},
    [(1L, 2L, 'bcde', 'fghi'), (1L, 1L, 'abcd', 'efgh')],
    ['select /* order */ * from vtocc_a order by id desc limit 10001'],
  ],

  # simple insert
  ['begin'],
  [
    "insert /* simple */ into vtocc_a values (2, 1, 'aaaa', 'bbbb')", {},
    [],
    ["insert /* simple */ into vtocc_a values (2, 1, 'aaaa', 'bbbb') /* _stream vtocc_a (eid id ) (2 1 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid = 2 and id = 1', {}, [(2L, 1L, 'aaaa', 'bbbb')]],
  ['begin'], ['delete from vtocc_a where eid>1'], ['commit'],

  # qualified insert
  ['begin'],
  [
    "insert /* qualified */ into vtocc_a(eid, id, name, foo) values (3, 1, 'aaaa', 'cccc')", {},
    [],
    ["insert /* qualified */ into vtocc_a(eid, id, name, foo) values (3, 1, 'aaaa', 'cccc') /* _stream vtocc_a (eid id ) (3 1 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid = 3 and id = 1', {}, [(3L, 1L, 'aaaa', 'cccc')]],
  ['begin'], ['delete from vtocc_a where eid>1'], ['commit'],

  # bind values
  ['begin'],
  [
    "insert /* bind values */ into vtocc_a(eid, id, name, foo) values (%(eid)s, %(id)s, %(name)s, %(foo)s)",
    {"eid": 4, "id": 1, "name": "aaaa", "foo": "cccc"},
    [],
    ["insert /* bind values */ into vtocc_a(eid, id, name, foo) values (4, 1, 'aaaa', 'cccc') /* _stream vtocc_a (eid id ) (4 1 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid = 4 and id = 1', {}, [(4L, 1L, 'aaaa', 'cccc')]],
  ['begin'], ['delete from vtocc_a where eid>1'], ['commit'],

  # out of sequence columns
  ['begin'],
  [
    "insert into vtocc_a(id, eid, foo, name) values (-1, 5, 'aaa', 'bbb')", {},
    [],
    ["insert into vtocc_a(id, eid, foo, name) values (-1, 5, 'aaa', 'bbb') /* _stream vtocc_a (eid id ) (5 -1 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid = 5 and id = -1', {}, [(5L, -1L, 'bbb', 'aaa')]],
  ['begin'], ['delete from vtocc_a where eid>1'], ['commit'],

  # numbers as strings
  ['begin'],
  [
    "insert into vtocc_a(id, eid, foo, name) values (%(id)s, '6', 111, 222)", { "id": "1"},
    [],
    ["insert into vtocc_a(id, eid, foo, name) values ('1', '6', 111, 222) /* _stream vtocc_a (eid id ) (6 1 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid = 6 and id = 1', {}, [(6L, 1L, '222', '111')]],
  ['begin'], ['delete from vtocc_a where eid>1'], ['commit'],

  # strings as numbers
  ['begin'],
  [
    "insert into vtocc_c(name, eid, foo) values (%(name)s, '9', 'aaa')", { "name": "bbb"},
    [],
    ["insert into vtocc_c(name, eid, foo) values ('bbb', '9', 'aaa') /* _stream vtocc_c (eid name ) (9 'YmJi' ); */"],
  ],
  ['commit'],
  ['select * from vtocc_c where eid = 9', {}, [(9, 'bbb', 'aaa')]],
  ['begin'], ['delete from vtocc_c where eid<10'], ['commit'],

  # expressions
  ['begin'],
  [
    "insert into vtocc_a(eid, id, name, foo) values (7, 1+1, '', '')", {},
    [],
    ["insert into vtocc_a(eid, id, name, foo) values (7, 1+1, '', '')"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid = 7', {}, [(7L, 2L, '', '')]],
  ['begin'], ['delete from vtocc_a where eid>1'], ['commit'],

  # no index
  ['begin'],
  [
    "insert into vtocc_d(eid, id) values (1, 1)", {},
    [],
    ["insert into vtocc_d(eid, id) values (1, 1)"],
  ],
  ['commit'],
  ['select * from vtocc_d', {}, [(1L, 1L)]],
  ['begin'], ['delete from vtocc_d'], ['commit'],

  # on duplicate key
  ['begin'],
  [
    "insert into vtocc_a(eid, id, name, foo) values (8, 1, '', '') on duplicate key update name = 'foo'", {},
    [],
    ["insert into vtocc_a(eid, id, name, foo) values (8, 1, '', '') on duplicate key update name = 'foo' /* _stream vtocc_a (eid id ) (8 1 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid = 8', {}, [(8L, 1L, '', '')]],
  ['begin'],
  [
    "insert into vtocc_a(eid, id, name, foo) values (8, 1, '', '') on duplicate key update name = 'foo'", {},
    [],
    ["insert into vtocc_a(eid, id, name, foo) values (8, 1, '', '') on duplicate key update name = 'foo' /* _stream vtocc_a (eid id ) (8 1 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid = 8', {}, [(8L, 1L, 'foo', '')]],
  ['begin'],
  [
    "insert into vtocc_a(eid, id, name, foo) values (8, 1, '', '') on duplicate key update id = 2", {},
    [],
    ["insert into vtocc_a(eid, id, name, foo) values (8, 1, '', '') on duplicate key update id = 2 /* _stream vtocc_a (eid id ) (8 1 ) (8 2 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid = 8', {}, [(8L, 2L, 'foo', '')]],
  ['begin'],
  [
    "insert into vtocc_a(eid, id, name, foo) values (8, 2, '', '') on duplicate key update id = 2+1", {},
    [],
    ["insert into vtocc_a(eid, id, name, foo) values (8, 2, '', '') on duplicate key update id = 2+1"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid = 8', {}, [(8L, 3L, 'foo', '')]],
  ['begin'], ['delete from vtocc_a where eid>1'], ['commit'],

  # subquery
  ['begin'],
  [
    "insert /* subquery */ into vtocc_a(eid, id, name, foo) select eid, foo, name, foo from vtocc_c", {},
    [],
    [
      'select eid, foo, name, foo from vtocc_c limit 10001',
      "insert /* subquery */ into vtocc_a(eid, id, name, foo) values (10, 20, 'abcd', '20'), (11, 30, 'bcde', '30') /* _stream vtocc_a (eid id ) (10 20 ) (11 30 ); */",
    ],
  ],
  ['commit'],
  ['select * from vtocc_a where eid in (10, 11)', {}, [(10L, 20L, 'abcd', '20'), (11L, 30L, 'bcde', '30')]],
  ['begin'],
  [
    "insert into vtocc_a(eid, id, name, foo) select eid, foo, name, foo from vtocc_c on duplicate key update foo='bar'", {},
    [],
    [
      'select eid, foo, name, foo from vtocc_c limit 10001',
      "insert into vtocc_a(eid, id, name, foo) values (10, 20, 'abcd', '20'), (11, 30, 'bcde', '30') on duplicate key update foo = 'bar' /* _stream vtocc_a (eid id ) (10 20 ) (11 30 ); */",
    ],
  ],
  ['commit'],
  ['select * from vtocc_a where eid in (10, 11)', {}, [(10L, 20L, 'abcd', 'bar'), (11L, 30L, 'bcde', 'bar')]],
  ['begin'],
  [
    "insert into vtocc_a(eid, id, name, foo) select eid, foo, name, foo from vtocc_c limit 1 on duplicate key update id = 21", {},
    [],
    [
      'select eid, foo, name, foo from vtocc_c limit 1',
      "insert into vtocc_a(eid, id, name, foo) values (10, 20, 'abcd', '20') on duplicate key update id = 21 /* _stream vtocc_a (eid id ) (10 20 ) (10 21 ); */",
    ],
  ],
  ['commit'],
  ['select * from vtocc_a where eid in (10, 11)', {}, [(10L, 21L, 'abcd', 'bar'), (11L, 30, 'bcde', 'bar')]],
  ['begin'], ['delete from vtocc_a where eid>1'], ['delete from vtocc_c where eid<10'], ['commit'],

  # multi-value
  ['begin'],
  [
    "insert into vtocc_a(eid, id, name, foo) values (5, 1, '', ''), (7, 1, '', '')", {},
    [],
    ["insert into vtocc_a(eid, id, name, foo) values (5, 1, '', ''), (7, 1, '', '') /* _stream vtocc_a (eid id ) (5 1 ) (7 1 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid>1', {}, [(5L, 1L, '', ''), (7L, 1L, '', '')]],
  ['begin'], ['delete from vtocc_a where eid>1'], ['commit'],

  # update
  ['begin'],
  [
    "update /* pk */ vtocc_a set foo='bar' where eid = 1 and id = 1", {},
    [],
    ["update /* pk */ vtocc_a set foo = 'bar' where eid = 1 and id = 1 /* _stream vtocc_a (eid id ) (1 1 ); */"]
  ],
  ['commit'],
  ['select foo from vtocc_a where id = 1', {}, [('bar',)]],
  ['begin'], ["update vtocc_a set foo='efgh' where id=1"], ['commit'],

  # single in
  ['begin'],
  [
    "update /* pk */ vtocc_a set foo='bar' where eid = 1 and id in (1, 2)", {},
    [],
    ["update /* pk */ vtocc_a set foo = 'bar' where eid = 1 and id in (1, 2) /* _stream vtocc_a (eid id ) (1 1 ) (1 2 ); */"],
  ],
  ['commit'],
  ['select foo from vtocc_a where id = 1', {}, [('bar',)]],
  ['begin'], ["update vtocc_a set foo='efgh' where id=1"], ["update vtocc_a set foo='fghi' where id=2"], ['commit'],

  # double in
  ['begin'],
  [
    "update /* pk */ vtocc_a set foo='bar' where eid in (1) and id in (1, 2)", {},
    [],
    [
      'select eid, id from vtocc_a where eid in (1) and id in (1, 2) limit 10001 for update',
      "update /* pk */ vtocc_a set foo = 'bar' where eid = 1 and id = 1 /* _stream vtocc_a (eid id ) (1 1 ); */",
      "update /* pk */ vtocc_a set foo = 'bar' where eid = 1 and id = 2 /* _stream vtocc_a (eid id ) (1 2 ); */",
    ],
  ],
  ['commit'],
  ['select foo from vtocc_a where id = 1', {}, [('bar',)]],
  ['begin'], ["update vtocc_a set foo='efgh' where id=1"], ["update vtocc_a set foo='fghi' where id=2"], ['commit'],

  # double in 2
  ['begin'],
  [
    "update /* pk */ vtocc_a set foo='bar' where eid in (1, 2) and id in (1, 2)", {},
    [],
    [
      'select eid, id from vtocc_a where eid in (1, 2) and id in (1, 2) limit 10001 for update',
      "update /* pk */ vtocc_a set foo = 'bar' where eid = 1 and id = 1 /* _stream vtocc_a (eid id ) (1 1 ); */",
      "update /* pk */ vtocc_a set foo = 'bar' where eid = 1 and id = 2 /* _stream vtocc_a (eid id ) (1 2 ); */",
    ],
  ],
  ['commit'],
  ['select foo from vtocc_a where id = 1', {}, [('bar',)]],
  ['begin'], ["update vtocc_a set foo='efgh' where id=1"], ["update vtocc_a set foo='fghi' where id=2"], ['commit'],

  # tuple in
  ['begin'],
  [
    "update /* pk */ vtocc_a set foo='bar' where (eid, id) in ((1, 1), (1, 2))", {},
    [],
    ["update /* pk */ vtocc_a set foo = 'bar' where (eid, id) in ((1, 1), (1, 2)) /* _stream vtocc_a (eid id ) (1 1 ) (1 2 ); */"],
  ],
  ['commit'],
  ['select foo from vtocc_a where id = 1', {}, [('bar',)]],
  ['begin'], ["update vtocc_a set foo='efgh' where id=1"], ["update vtocc_a set foo='fghi' where id=2"], ['commit'],

  # pk change
  ['begin'],
  [
    "update vtocc_a set eid = 2 where eid = 1 and id = 1", {},
    [],
    ["update vtocc_a set eid = 2 where eid = 1 and id = 1 /* _stream vtocc_a (eid id ) (1 1 ) (2 1 ); */"],
  ],
  ['commit'],
  ['select eid from vtocc_a where id = 1', {}, [(2L,)]],
  ['begin'], ["update vtocc_a set eid=1 where id=1"], ['commit'],

  # complex pk change
  ['begin'],
  [
    "update vtocc_a set eid = 1+1 where eid = 1 and id = 1", {},
    [],
    ["update vtocc_a set eid = 1+1 where eid = 1 and id = 1"],
  ],
  ['commit'],
  ['select eid from vtocc_a where id = 1', {}, [(2L,)]],
  ['begin'], ["update vtocc_a set eid=1 where id=1"], ['commit'],

  # complex where
  ['begin'],
  [
    "update vtocc_a set eid = 1+1 where eid = 1 and id = 1+0", {},
    [],
    ["update vtocc_a set eid = 1+1 where eid = 1 and id = 1+0"],
  ],
  ['commit'],
  ['select eid from vtocc_a where id = 1', {}, [(2L,)]],
  ['begin'], ["update vtocc_a set eid=1 where id=1"], ['commit'],

  # partial pk
  ['begin'],
  [
    "update /* pk */ vtocc_a set foo='bar' where id = 1", {},
    [],
    [
      "select eid, id from vtocc_a where id = 1 limit 10001 for update",
      "update /* pk */ vtocc_a set foo = 'bar' where eid = 1 and id = 1 /* _stream vtocc_a (eid id ) (1 1 ); */",
    ],
  ],
  ['commit'],
  ['select foo from vtocc_a where id = 1', {}, [('bar',)]],
  ['begin'], ["update vtocc_a set foo='efgh' where id=1"], ['commit'],

  # limit
  ['begin'],
  [
    "update /* pk */ vtocc_a set foo='bar' where eid = 1 limit 1", {},
    [],
    [
      "select eid, id from vtocc_a where eid = 1 limit 1 for update",
      "update /* pk */ vtocc_a set foo = 'bar' where eid = 1 and id = 1 /* _stream vtocc_a (eid id ) (1 1 ); */",
    ],
  ],
  ['commit'],
  ['select foo from vtocc_a where id = 1', {}, [('bar',)]],
  ['begin'], ["update vtocc_a set foo='efgh' where id=1"], ['commit'],

  # order by
  ['begin'],
  [
    "update /* pk */ vtocc_a set foo='bar' where eid = 1 order by id desc limit 1", {},
    [],
    [
      "select eid, id from vtocc_a where eid = 1 order by id desc limit 1 for update",
      "update /* pk */ vtocc_a set foo = 'bar' where eid = 1 and id = 2 /* _stream vtocc_a (eid id ) (1 2 ); */",
    ],
  ],
  ['commit'],
  ['select foo from vtocc_a where id = 2', {}, [('bar',)]],
  ['begin'], ["update vtocc_a set foo='fghi' where id=2"], ['commit'],

  # missing where
  ['begin'],
  [
    "update vtocc_a set foo='bar'", {},
    [],
    [
      "select eid, id from vtocc_a limit 10001 for update",
      "update vtocc_a set foo = 'bar' where eid = 1 and id = 1 /* _stream vtocc_a (eid id ) (1 1 ); */",
      "update vtocc_a set foo = 'bar' where eid = 1 and id = 2 /* _stream vtocc_a (eid id ) (1 2 ); */",
    ],
  ],
  ['commit'],
  ['select * from vtocc_a', {}, [(1L, 1L, 'abcd', 'bar'), (1L, 2L, 'bcde', 'bar')]],
  ['begin'], ["update vtocc_a set foo='efgh' where id=1"], ["update vtocc_a set foo='fghi' where id=2"], ['commit'],

  # no index
  ['begin'],
  ["insert into vtocc_d(eid, id) values (1, 1)"],
  [
    "update vtocc_d set id = 2 where eid = 1", {},
    [],
    ["update vtocc_d set id = 2 where eid = 1"],
  ],
  ['commit'],
  ['select * from vtocc_d', {}, [(1L, 2L)]],
  ['begin'], ['delete from vtocc_d'], ['commit'],

  # delete
  ['begin'],
  ["insert into vtocc_a(eid, id, name, foo) values (2, 1, '', '')"],
  [
    "delete /* pk */ from vtocc_a where eid = 2 and id = 1", {},
    [],
    ["delete /* pk */ from vtocc_a where eid = 2 and id = 1 /* _stream vtocc_a (eid id ) (2 1 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid=2', {}, []],

  # single in
  ['begin'],
  ["insert into vtocc_a(eid, id, name, foo) values (2, 1, '', '')"],
  [
    "delete /* pk */ from vtocc_a where eid = 2 and id in (1, 2)", {},
    [],
    ["delete /* pk */ from vtocc_a where eid = 2 and id in (1, 2) /* _stream vtocc_a (eid id ) (2 1 ) (2 2 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid=2', {}, []],

  # double in
  ['begin'],
  ["insert into vtocc_a(eid, id, name, foo) values (2, 1, '', '')"],
  [
    "delete /* pk */ from vtocc_a where eid in (2) and id in (1, 2)", {},
    [],
    [
      'select eid, id from vtocc_a where eid in (2) and id in (1, 2) limit 10001 for update',
      'delete /* pk */ from vtocc_a where eid = 2 and id = 1 /* _stream vtocc_a (eid id ) (2 1 ); */',
    ],
  ],
  ['commit'],
  ['select * from vtocc_a where eid=2', {}, []],

  # double in 2
  ['begin'],
  ["insert into vtocc_a(eid, id, name, foo) values (2, 1, '', '')"],
  [
    "delete /* pk */ from vtocc_a where eid in (2, 3) and id in (1, 2)", {},
    [],
    [
      'select eid, id from vtocc_a where eid in (2, 3) and id in (1, 2) limit 10001 for update',
      'delete /* pk */ from vtocc_a where eid = 2 and id = 1 /* _stream vtocc_a (eid id ) (2 1 ); */'
    ],
  ],
  ['commit'],
  ['select * from vtocc_a where eid=2', {}, []],

  # tuple in
  ['begin'],
  ["insert into vtocc_a(eid, id, name, foo) values (2, 1, '', '')"],
  [
    "delete /* pk */ from vtocc_a where (eid, id) in ((2, 1), (3, 2))", {},
    [],
    ["delete /* pk */ from vtocc_a where (eid, id) in ((2, 1), (3, 2)) /* _stream vtocc_a (eid id ) (2 1 ) (3 2 ); */"],
  ],
  ['commit'],
  ['select * from vtocc_a where eid=2', {}, []],

  # complex where
  ['begin'],
  ["insert into vtocc_a(eid, id, name, foo) values (2, 1, '', '')"],
  [
    "delete from vtocc_a where eid = 1+1 and id = 1", {},
    [],
    [
      'select eid, id from vtocc_a where eid = 1+1 and id = 1 limit 10001 for update',
      "delete from vtocc_a where eid = 2 and id = 1 /* _stream vtocc_a (eid id ) (2 1 ); */",
    ],
  ],
  ['commit'],
  ['select * from vtocc_a where eid=2', {}, []],

  # partial pk
  ['begin'],
  ["insert into vtocc_a(eid, id, name, foo) values (2, 1, '', '')"],
  [
    "delete from vtocc_a where eid = 2", {},
    [],
    [
      'select eid, id from vtocc_a where eid = 2 limit 10001 for update',
      "delete from vtocc_a where eid = 2 and id = 1 /* _stream vtocc_a (eid id ) (2 1 ); */",
    ],
  ],
  ['commit'],
  ['select * from vtocc_a where eid=2', {}, []],

  # limit
  ['begin'],
  ["insert into vtocc_a(eid, id, name, foo) values (2, 1, '', '')"],
  [
    "delete from vtocc_a where eid = 2 limit 1", {},
    [],
    [
      'select eid, id from vtocc_a where eid = 2 limit 1 for update',
      "delete from vtocc_a where eid = 2 and id = 1 /* _stream vtocc_a (eid id ) (2 1 ); */",
    ],
  ],
  ['commit'],
  ['select * from vtocc_a where eid=2', {}, []],

  # order by
  ['begin'],
  ["insert into vtocc_a(eid, id, name, foo) values (2, 1, '', '')"],
  [
    "delete from vtocc_a order by eid desc limit 1", {},
    [],
    [
      'select eid, id from vtocc_a order by eid desc limit 1 for update',
      "delete from vtocc_a where eid = 2 and id = 1 /* _stream vtocc_a (eid id ) (2 1 ); */",
    ],
  ],
  ['commit'],
  ['select * from vtocc_a where eid=2', {}, []],

  # missing where
  ['begin'],
  [
    "delete from vtocc_a", {},
    [],
    [
      'select eid, id from vtocc_a limit 10001 for update',
      "delete from vtocc_a where eid = 1 and id = 1 /* _stream vtocc_a (eid id ) (1 1 ); */",
      "delete from vtocc_a where eid = 1 and id = 2 /* _stream vtocc_a (eid id ) (1 2 ); */",
    ],
  ],
  ['rollback'],
  ['select * from vtocc_a', {}, [(1L, 1L, 'abcd', 'efgh'), (1L, 2L, 'bcde', 'fghi')]],

  # no index
  ['begin'],
  ['insert into vtocc_d values (1, 1)'],
  [
    'delete from vtocc_d where eid =1 and id =1', {},
    [],
    ['delete from vtocc_d where eid = 1 and id = 1'],
  ],
  ['commit'],
  ['select * from vtocc_d', {}, []],

  # missing values
  ['begin'],
  [
    "insert into vtocc_e(foo) values ('foo')", {},
    [],
    ["insert into vtocc_e(foo) values ('foo') /* _stream vtocc_e (eid id name ) (null 1 'bmFtZQ==' ); */"],
  ],
  [
    "insert into vtocc_e(foo) select foo from vtocc_a", {},
    [],
    ["select foo from vtocc_a limit 10001", "insert into vtocc_e(foo) values ('efgh'), ('fghi') /* _stream vtocc_e (eid id name ) (null 1 'bmFtZQ==' ) (null 1 'bmFtZQ==' ); */"],
  ],
  ['delete from vtocc_e'],
  ['commit'],
]
