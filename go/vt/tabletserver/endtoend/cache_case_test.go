// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"testing"

	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
)

// TestCacheCases1 covers cases for vitess_cached1.
func TestCacheCases1(t *testing.T) {
	client := framework.NewClient()

	testCases := []framework.Testable{
		framework.TestQuery("alter table vitess_cached1 comment 'new'"),
		// (1) will be in cache after this.
		&framework.TestCase{
			Name:  "PK_IN (empty cache)",
			Query: "select * from vitess_cached1 where eid = 1",
			Result: [][]string{
				{"1", "a", "abcd"},
			},
			Rewritten: []string{
				"select * from vitess_cached1 where 1 != 1",
				"select eid, name, foo from vitess_cached1 where eid in (1)",
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_cached1",
			Misses:       1,
		},
		// (1)
		&framework.TestCase{
			Name:  "PK_IN, use cache",
			Query: "select * from vitess_cached1 where eid = 1",
			Result: [][]string{
				{"1", "a", "abcd"},
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_cached1",
			Hits:         1,
		},
		// (1, 3)
		&framework.TestCase{
			Name:  "PK_IN (empty cache)",
			Query: "select * from vitess_cached1 where eid in (1, 3, 6)",
			Result: [][]string{
				{"1", "a", "abcd"},
				{"3", "c", "abcd"},
			},
			Rewritten: []string{
				"select * from vitess_cached1 where 1 != 1",
				"select eid, name, foo from vitess_cached1 where eid in (3, 6)",
			},
			RowsAffected: 2,
			Plan:         "PK_IN",
			Table:        "vitess_cached1",
			Hits:         1,
			Misses:       1,
			Absent:       1,
		},
		// (1, 3)
		&framework.TestCase{
			Name:  "PK_IN limit 0",
			Query: "select * from vitess_cached1 where eid in (1, 3, 6) limit 0",
			Rewritten: []string{
				"select * from vitess_cached1 where 1 != 1",
			},
			Plan:  "PK_IN",
			Table: "vitess_cached1",
		},
		// (1, 3)
		&framework.TestCase{
			Name:  "PK_IN limit 1",
			Query: "select * from vitess_cached1 where eid in (1, 3, 6) limit 1",
			Result: [][]string{
				{"1", "a", "abcd"},
			},
			Rewritten: []string{
				"select * from vitess_cached1 where 1 != 1",
				"select eid, name, foo from vitess_cached1 where eid in (6)",
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_cached1",
			Hits:         2,
			Absent:       1,
		},
		// (1, 3)
		&framework.TestCase{
			Name:  "PK_IN limit :a",
			Query: "select * from vitess_cached1 where eid in (1, 3, 6) limit :a",
			BindVars: map[string]interface{}{
				"a": 1,
			},
			Result: [][]string{
				{"1", "a", "abcd"},
			},
			Rewritten: []string{
				"select * from vitess_cached1 where 1 != 1",
				"select eid, name, foo from vitess_cached1 where eid in (6)",
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_cached1",
			Hits:         2,
			Absent:       1,
		},
		// (1, 2, 3)
		&framework.TestCase{
			Name:  "SELECT_SUBQUERY (1, 2)",
			Query: "select * from vitess_cached1 where name = 'a'",
			Result: [][]string{
				{"1", "a", "abcd"},
				{"2", "a", "abcd"},
			},
			Rewritten: []string{
				"select * from vitess_cached1 where 1 != 1",
				"select eid from vitess_cached1 use index (aname1) where name = 'a' limit 10001",
				"select eid, name, foo from vitess_cached1 where eid in (2)",
			},
			RowsAffected: 2,
			Table:        "vitess_cached1",
			Hits:         1,
			Misses:       1,
		},
		// (1, 2, 3)
		&framework.TestCase{
			Name:  "covering index",
			Query: "select eid, name from vitess_cached1 where name = 'a'",
			Result: [][]string{
				{"1", "a"},
				{"2", "a"},
			},
			Rewritten: []string{
				"select eid, name from vitess_cached1 where 1 != 1",
				"select eid, name from vitess_cached1 where name = 'a' limit 10001",
			},
			RowsAffected: 2,
			Plan:         "PASS_SELECT",
			Table:        "vitess_cached1",
		},
		// (1, 2, 3)
		&framework.TestCase{
			Name:  "SELECT_SUBQUERY (1, 2)",
			Query: "select * from vitess_cached1 where name = 'a'",
			Result: [][]string{
				{"1", "a", "abcd"},
				{"2", "a", "abcd"},
			},
			Rewritten: []string{
				"select eid from vitess_cached1 use index (aname1) where name = 'a' limit 10001",
			},
			RowsAffected: 2,
			Table:        "vitess_cached1",
			Hits:         2,
		},
		// (1, 2, 3, 4, 5)
		&framework.TestCase{
			Name:  "SELECT_SUBQUERY (4, 5)",
			Query: "select * from vitess_cached1 where name between 'd' and 'e'",
			Result: [][]string{
				{"4", "d", "abcd"},
				{"5", "e", "efgh"},
			},
			Rewritten: []string{
				"select * from vitess_cached1 where 1 != 1",
				"select eid from vitess_cached1 use index (aname1) where name between 'd' and 'e' limit 10001",
				"select eid, name, foo from vitess_cached1 where eid in (4, 5)",
			},
			RowsAffected: 2,
			Plan:         "SELECT_SUBQUERY",
			Table:        "vitess_cached1",
			Misses:       2,
		},
		// (1, 2, 3, 4, 5)
		&framework.TestCase{
			Name:  "PASS_SELECT",
			Query: "select * from vitess_cached1 where foo='abcd'",
			Result: [][]string{
				{"1", "a", "abcd"},
				{"2", "a", "abcd"},
				{"3", "c", "abcd"},
				{"4", "d", "abcd"},
			},
			Rewritten: []string{
				"select * from vitess_cached1 where 1 != 1",
				"select * from vitess_cached1 where foo = 'abcd' limit 10001",
			},
			RowsAffected: 4,
			Plan:         "PASS_SELECT",
			Table:        "vitess_cached1",
		},
	}
	for _, tcase := range testCases {
		if err := tcase.Test("", client); err != nil {
			t.Error(err)
		}
	}
}

// TestCacheCases2 covers cases for vitess_cached2.
func TestCacheCases2(t *testing.T) {
	client := framework.NewClient()

	testCases := []framework.Testable{
		framework.TestQuery("alter table vitess_cached2 comment 'new'"),
		&framework.TestCase{
			Name:  "PK_IN (null key)",
			Query: "select * from vitess_cached2 where eid = 2 and bid = :bid",
			BindVars: map[string]interface{}{
				"bid": nil,
			},
			Rewritten: []string{
				"select * from vitess_cached2 where 1 != 1",
				"select eid, bid, name, foo from vitess_cached2 where (eid = 2 and bid = null)",
			},
			Plan:   "PK_IN",
			Table:  "vitess_cached2",
			Absent: 1,
		},
		// (2.foo) is in cache
		&framework.TestCase{
			Name:  "PK_IN (empty cache)",
			Query: "select * from vitess_cached2 where eid = 2 and bid = 'foo'",
			Result: [][]string{
				{"2", "foo", "abcd2", "efgh"},
			},
			Rewritten: []string{
				"select * from vitess_cached2 where 1 != 1",
				"select eid, bid, name, foo from vitess_cached2 where (eid = 2 and bid = 'foo')",
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_cached2",
			Misses:       1,
		},
		// (2.foo)
		&framework.TestCase{
			Name:  "PK_IN, use cache",
			Query: "select bid, eid, name, foo from vitess_cached2 where eid = 2 and bid = 'foo'",
			Result: [][]string{
				{"foo", "2", "abcd2", "efgh"},
			},
			Rewritten: []string{
				"select bid, eid, name, foo from vitess_cached2 where 1 != 1",
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_cached2",
			Hits:         1,
		},
		// (2.foo)
		&framework.TestCase{
			Name:  "PK_IN, absent",
			Query: "select bid, eid, name, foo from vitess_cached2 where eid = 3 and bid = 'foo'",
			Rewritten: []string{
				"select bid, eid, name, foo from vitess_cached2 where 1 != 1",
				"select eid, bid, name, foo from vitess_cached2 where (eid = 3 and bid = 'foo')",
			},
			Plan:   "PK_IN",
			Table:  "vitess_cached2",
			Absent: 1,
		},
		// (1.foo, 2.foo)
		&framework.TestCase{
			Name:  "out of order columns list",
			Query: "select bid, eid from vitess_cached2 where eid = 1 and bid = 'foo'",
			Result: [][]string{
				{"foo", "1"},
			},
			Rewritten: []string{
				"select bid, eid from vitess_cached2 where 1 != 1",
				"select eid, bid, name, foo from vitess_cached2 where (eid = 1 and bid = 'foo')",
			},
			RowsAffected: 1,
			Table:        "vitess_cached2",
			Misses:       1,
		},
		// (1.foo, 2.foo)
		&framework.TestCase{
			Name:  "out of order columns list, use cache",
			Query: "select bid, eid from vitess_cached2 where eid = 1 and bid = 'foo'",
			Result: [][]string{
				{"foo", "1"},
			},
			RowsAffected: 1,
			Table:        "vitess_cached2",
			Hits:         1,
		},
		// (1.foo, 1.bar, 2.foo)
		&framework.TestCase{
			Name:  "pk_in for composite pk table, two fetches from db (absent)",
			Query: "select eid, bid, name, foo from vitess_cached2 where eid = 1 and bid in('absent1', 'absent2')",
			Rewritten: []string{
				"select eid, bid, name, foo from vitess_cached2 where 1 != 1",
				"select eid, bid, name, foo from vitess_cached2 where (eid = 1 and bid = 'absent1') or (eid = 1 and bid = 'absent2')",
			},
			Plan:   "PK_IN",
			Table:  "vitess_cached2",
			Absent: 2,
		},
		// (1.foo, 1.bar, 2.foo)
		&framework.TestCase{
			Name:  "pk_in for composite pk table, 1 fetch from db",
			Query: "select eid, bid, name, foo from vitess_cached2 where eid = 1 and bid in('foo', 'bar')",
			Result: [][]string{
				{"1", "foo", "abcd1", "efgh"},
				{"1", "bar", "abcd1", "efgh"},
			},
			Rewritten: []string{
				"select eid, bid, name, foo from vitess_cached2 where 1 != 1",
				"select eid, bid, name, foo from vitess_cached2 where (eid = 1 and bid = 'bar')",
			},
			RowsAffected: 2,
			Plan:         "PK_IN",
			Table:        "vitess_cached2",
			Hits:         1,
			Misses:       1,
		},
		// (1.foo, 1.bar, 2.foo)
		&framework.TestCase{
			Name:  "pk_in for composite pk table, 0 fetch from db",
			Query: "select eid, bid, name, foo from vitess_cached2 where eid = 1 and bid in('foo', 'bar')",
			Result: [][]string{
				{"1", "foo", "abcd1", "efgh"},
				{"1", "bar", "abcd1", "efgh"},
			},
			RowsAffected: 2,
			Plan:         "PK_IN",
			Table:        "vitess_cached2",
			Hits:         2,
		},
		// (1.foo, 1.bar, 2.foo, 2.bar)
		&framework.TestCase{
			Name:  "select_subquery for composite pk table, 1 fetch from db",
			Query: "select eid, bid, name, foo from vitess_cached2 where eid = 2 and name='abcd2'",
			Result: [][]string{
				{"2", "foo", "abcd2", "efgh"},
				{"2", "bar", "abcd2", "efgh"},
			},
			Rewritten: []string{
				"select eid, bid, name, foo from vitess_cached2 where 1 != 1",
				"select eid, bid from vitess_cached2 use index (aname2) where eid = 2 and name = 'abcd2' limit 10001",
				"select eid, bid, name, foo from vitess_cached2 where (eid = 2 and bid = 'bar')",
			},
			RowsAffected: 2,
			Plan:         "SELECT_SUBQUERY",
			Table:        "vitess_cached2",
			Hits:         1,
			Misses:       1,
		},
		// (1.foo, 1.bar, 2.foo, 2.bar)
		&framework.TestCase{
			Name:  "verify 1.bar is in cache",
			Query: "select bid, eid from vitess_cached2 where eid = 1 and bid = 'bar'",
			Result: [][]string{
				{"bar", "1"},
			},
			Rewritten: []string{
				"select bid, eid from vitess_cached2 where 1 != 1",
			},
			RowsAffected: 1,
			Table:        "vitess_cached2",
			Hits:         1,
		},
		// (1.foo, 1.bar, 2.foo, 2.bar)
		&framework.MultiCase{
			Name: "update",
			Cases: []framework.Testable{
				framework.TestQuery("begin"),
				framework.TestQuery("update vitess_cached2 set foo='fghi' where bid = 'bar'"),
				&framework.TestCase{
					Query:         "commit",
					Table:         "vitess_cached2",
					Invalidations: 2,
				},
				&framework.TestCase{
					Query: "select * from vitess_cached2 where eid = 1 and bid = 'bar'",
					Result: [][]string{
						{"1", "bar", "abcd1", "fghi"},
					},
					Rewritten: []string{
						"select * from vitess_cached2 where 1 != 1",
						"select eid, bid, name, foo from vitess_cached2 where (eid = 1 and bid = 'bar')",
					},
					RowsAffected: 1,
					Table:        "vitess_cached2",
					Misses:       1,
				},
			},
		},
		// (1.foo, 1.bar, 2.foo, 2.bar)
		&framework.MultiCase{
			Name: "this will not invalidate the cache",
			Cases: []framework.Testable{
				framework.TestQuery("begin"),
				framework.TestQuery("update vitess_cached2 set foo='fghi' where bid = 'bar'"),
				framework.TestQuery("rollback"),
				&framework.TestCase{
					Query: "select * from vitess_cached2 where eid = 1 and bid = 'bar'",
					Result: [][]string{
						{"1", "bar", "abcd1", "fghi"},
					},
					RowsAffected: 1,
					Table:        "vitess_cached2",
					Hits:         1,
				},
			},
		},
		// (1.foo, 1.bar, 2.foo, 2.bar)
		&framework.MultiCase{
			Name: "upsert should invalidate rowcache",
			Cases: []framework.Testable{
				&framework.TestCase{
					Query: "select * from vitess_cached2 where eid = 1 and bid = 'bar'",
					Result: [][]string{
						{"1", "bar", "abcd1", "fghi"},
					},
					RowsAffected: 1,
					Table:        "vitess_cached2",
					Hits:         1,
				},
				framework.TestQuery("begin"),
				&framework.TestCase{
					Query: "insert into vitess_cached2 values(1, 'bar', 'abcd1', 'fghi') on duplicate key update foo='fghi'",
					Rewritten: []string{
						"insert into vitess_cached2 values (1, 'bar', 'abcd1', 'fghi') /* _stream vitess_cached2 (eid bid ) (1 'YmFy' )",
						"update vitess_cached2 set foo = 'fghi' where (eid = 1 and bid = 'bar') /* _stream vitess_cached2 (eid bid ) (1 'YmFy' )",
					},
					Table: "vitess_cached2",
				},
				framework.TestQuery("commit"),
				&framework.TestCase{
					Query: "select * from vitess_cached2 where eid = 1 and bid = 'bar'",
					Result: [][]string{
						{"1", "bar", "abcd1", "fghi"},
					},
					Rewritten: []string{
						"select eid, bid, name, foo from vitess_cached2 where (eid = 1 and bid = 'bar')",
					},
					RowsAffected: 1,
					Table:        "vitess_cached2",
					Misses:       1,
				},
			},
		},
		// (1.foo, 2.foo, 2.bar)
		&framework.MultiCase{
			Name: "delete",
			Cases: []framework.Testable{
				framework.TestQuery("begin"),
				framework.TestQuery("delete from vitess_cached2 where eid = 1 and bid = 'bar'"),
				&framework.TestCase{
					Query:         "commit",
					Table:         "vitess_cached2",
					Invalidations: 1,
				},
				&framework.TestCase{
					Query: "select * from vitess_cached2 where eid = 1 and bid = 'bar'",
					Rewritten: []string{
						"select eid, bid, name, foo from vitess_cached2 where (eid = 1 and bid = 'bar')",
					},
					Table:  "vitess_cached2",
					Absent: 1,
				},
				framework.TestQuery("begin"),
				framework.TestQuery("insert into vitess_cached2(eid, bid, name, foo) values (1, 'bar', 'abcd1', 'efgh')"),
				&framework.TestCase{
					Query: "commit",
					Table: "vitess_cached2",
				},
			},
		},
		// (1.foo, 2.foo, 2.bar)
		&framework.TestCase{
			Name:  "Verify 1.foo is in cache",
			Query: "select * from vitess_cached2 where eid = 1 and bid = 'foo'",
			Result: [][]string{
				{"1", "foo", "abcd1", "efgh"},
			},
			Rewritten: []string{
				"select * from vitess_cached2 where 1 != 1",
			},
			RowsAffected: 1,
			Table:        "vitess_cached2",
			Hits:         1,
		},
		// DDL
		framework.TestQuery("alter table vitess_cached2 comment 'test'"),
		// (1.foo)
		&framework.TestCase{
			Name:  "Verify cache is empty after DDL",
			Query: "select * from vitess_cached2 where eid = 1 and bid = 'foo'",
			Result: [][]string{
				{"1", "foo", "abcd1", "efgh"},
			},
			Rewritten: []string{
				"select * from vitess_cached2 where 1 != 1",
				"select eid, bid, name, foo from vitess_cached2 where (eid = 1 and bid = 'foo')",
			},
			RowsAffected: 1,
			Table:        "vitess_cached2",
			Misses:       1,
		},
		// (1.foo)
		&framework.TestCase{
			Name:  "Verify row is cached",
			Query: "select * from vitess_cached2 where eid = 1 and bid = 'foo'",
			Result: [][]string{
				{"1", "foo", "abcd1", "efgh"},
			},
			RowsAffected: 1,
			Table:        "vitess_cached2",
			Hits:         1,
		},
	}
	for _, tcase := range testCases {
		if err := tcase.Test("", client); err != nil {
			t.Error(err)
		}
	}
}

func TestCacheCasesOverrides(t *testing.T) {
	client := framework.NewClient()

	testCases := []framework.Testable{
		&framework.TestCase{
			Name:  "select from view (cache miss)",
			Query: "select * from vitess_view where key2 = 1",
			Result: [][]string{
				{"1", "10", "1", "3"},
			},
			Rewritten: []string{
				"select * from vitess_view where 1 != 1",
				"select key2, key1, data1, data2 from vitess_view where key2 in (1)",
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_view",
			Misses:       1,
		},
		&framework.TestCase{
			Name:  "select from view (cache hit)",
			Query: "select * from vitess_view where key2 = 1",
			Result: [][]string{
				{"1", "10", "1", "3"},
			},
			Rewritten:    []string{},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_view",
			Hits:         1,
		},
		&framework.TestCase{
			Name:  "update part1 table of view",
			Query: "update vitess_part1 set data1 = 2 where key2 = 1",
			Rewritten: []string{
				"begin",
				"update vitess_part1 set data1 = 2 where key2 in (1) /* _stream vitess_part1 (key2 ) (1 )",
				"commit",
			},
			RowsAffected: 1,
			Plan:         "DML_PK",
		},
		&framework.TestCase{
			Name:  "verify cache got invalidated",
			Query: "select * from vitess_view where key2 = 1",
			Result: [][]string{
				{"1", "10", "2", "3"},
			},
			Rewritten: []string{
				"select key2, key1, data1, data2 from vitess_view where key2 in (1)",
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_view",
			Misses:       1,
		},
		&framework.TestCase{
			Name:  "verify cache got reloaded",
			Query: "select * from vitess_view where key2 = 1",
			Result: [][]string{
				{"1", "10", "2", "3"},
			},
			Rewritten:    []string{},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_view",
			Hits:         1,
		},
		&framework.TestCase{
			Name:  "update part2 table of view",
			Query: "update vitess_part2 set data2 = 2 where key3 = 1",
			Rewritten: []string{
				"begin",
				"update vitess_part2 set data2 = 2 where key3 in (1) /* _stream vitess_part2 (key3 ) (1 )",
				"commit",
			},
			RowsAffected: 1,
			Plan:         "DML_PK",
		},
		&framework.TestCase{
			Name:  "re-verify cache got invalidated",
			Query: "select * from vitess_view where key2 = 1",
			Result: [][]string{
				{"1", "10", "2", "2"},
			},
			Rewritten: []string{
				"select key2, key1, data1, data2 from vitess_view where key2 in (1)",
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_view",
			Misses:       1,
		},
		&framework.TestCase{
			Name:  "re-verify cache got reloaded",
			Query: "select * from vitess_view where key2 = 1",
			Result: [][]string{
				{"1", "10", "2", "2"},
			},
			Rewritten:    []string{},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vitess_view",
			Hits:         1,
		},
	}
	for _, tcase := range testCases {
		if err := tcase.Test("", client); err != nil {
			t.Error(err)
		}
	}
}
