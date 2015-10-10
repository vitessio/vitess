// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"testing"

	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
)

// TestCacheCases1 covers cases for vtocc_cached1.
func TestCacheCases1(t *testing.T) {
	client := framework.NewDefaultClient()

	testCases := []framework.Testable{
		framework.TestQuery("alter table vtocc_cached1 comment 'new'"),
		// (1) will be in cache after this.
		&framework.TestCase{
			Name:  "PK_IN (empty cache)",
			Query: "select * from vtocc_cached1 where eid = 1",
			Result: [][]string{
				{"1", "a", "abcd"},
			},
			Rewritten: []string{
				"select * from vtocc_cached1 where 1 != 1",
				"select eid, name, foo from vtocc_cached1 where eid in (1)",
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vtocc_cached1",
			Misses:       1,
		},
		// (1)
		&framework.TestCase{
			Name:  "PK_IN, use cache",
			Query: "select * from vtocc_cached1 where eid = 1",
			Result: [][]string{
				{"1", "a", "abcd"},
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vtocc_cached1",
			Hits:         1,
		},
		// (1, 3)
		&framework.TestCase{
			Name:  "PK_IN (empty cache)",
			Query: "select * from vtocc_cached1 where eid in (1, 3, 6)",
			Result: [][]string{
				{"1", "a", "abcd"},
				{"3", "c", "abcd"},
			},
			Rewritten: []string{
				"select * from vtocc_cached1 where 1 != 1",
				"select eid, name, foo from vtocc_cached1 where eid in (3, 6)",
			},
			RowsAffected: 2,
			Plan:         "PK_IN",
			Table:        "vtocc_cached1",
			Hits:         1,
			Misses:       1,
			Absent:       1,
		},
		// (1, 3)
		&framework.TestCase{
			Name:  "PK_IN limit 0",
			Query: "select * from vtocc_cached1 where eid in (1, 3, 6) limit 0",
			Rewritten: []string{
				"select * from vtocc_cached1 where 1 != 1",
			},
			Plan:  "PK_IN",
			Table: "vtocc_cached1",
		},
		// (1, 3)
		&framework.TestCase{
			Name:  "PK_IN limit 1",
			Query: "select * from vtocc_cached1 where eid in (1, 3, 6) limit 1",
			Result: [][]string{
				{"1", "a", "abcd"},
			},
			Rewritten: []string{
				"select * from vtocc_cached1 where 1 != 1",
				"select eid, name, foo from vtocc_cached1 where eid in (6)",
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vtocc_cached1",
			Hits:         2,
			Absent:       1,
		},
		// (1, 3)
		&framework.TestCase{
			Name:  "PK_IN limit :a",
			Query: "select * from vtocc_cached1 where eid in (1, 3, 6) limit :a",
			BindVars: map[string]interface{}{
				"a": 1,
			},
			Result: [][]string{
				{"1", "a", "abcd"},
			},
			Rewritten: []string{
				"select * from vtocc_cached1 where 1 != 1",
				"select eid, name, foo from vtocc_cached1 where eid in (6)",
			},
			RowsAffected: 1,
			Plan:         "PK_IN",
			Table:        "vtocc_cached1",
			Hits:         2,
			Absent:       1,
		},
		// (1, 2, 3)
		&framework.TestCase{
			Name:  "SELECT_SUBQUERY (1, 2)",
			Query: "select * from vtocc_cached1 where name = 'a'",
			Result: [][]string{
				{"1", "a", "abcd"},
				{"2", "a", "abcd"},
			},
			Rewritten: []string{
				"select * from vtocc_cached1 where 1 != 1",
				"select eid from vtocc_cached1 use index (aname1) where name = 'a' limit 10001",
				"select eid, name, foo from vtocc_cached1 where eid in (2)",
			},
			RowsAffected: 2,
			Table:        "vtocc_cached1",
			Hits:         1,
			Misses:       1,
		},
		// (1, 2, 3)
		&framework.TestCase{
			Name:  "covering index",
			Query: "select eid, name from vtocc_cached1 where name = 'a'",
			Result: [][]string{
				{"1", "a"},
				{"2", "a"},
			},
			Rewritten: []string{
				"select eid, name from vtocc_cached1 where 1 != 1",
				"select eid, name from vtocc_cached1 where name = 'a' limit 10001",
			},
			RowsAffected: 2,
			Plan:         "PASS_SELECT",
			Table:        "vtocc_cached1",
		},
		// (1, 2, 3)
		&framework.TestCase{
			Name:  "SELECT_SUBQUERY (1, 2)",
			Query: "select * from vtocc_cached1 where name = 'a'",
			Result: [][]string{
				{"1", "a", "abcd"},
				{"2", "a", "abcd"},
			},
			Rewritten: []string{
				"select eid from vtocc_cached1 use index (aname1) where name = 'a' limit 10001",
			},
			RowsAffected: 2,
			Table:        "vtocc_cached1",
			Hits:         2,
		},
		// (1, 2, 3, 4, 5)
		&framework.TestCase{
			Name:  "SELECT_SUBQUERY (4, 5)",
			Query: "select * from vtocc_cached1 where name between 'd' and 'e'",
			Result: [][]string{
				{"4", "d", "abcd"},
				{"5", "e", "efgh"},
			},
			Rewritten: []string{
				"select * from vtocc_cached1 where 1 != 1",
				"select eid from vtocc_cached1 use index (aname1) where name between 'd' and 'e' limit 10001",
				"select eid, name, foo from vtocc_cached1 where eid in (4, 5)",
			},
			RowsAffected: 2,
			Plan:         "SELECT_SUBQUERY",
			Table:        "vtocc_cached1",
			Misses:       2,
		},
		// (1, 2, 3, 4, 5)
		&framework.TestCase{
			Name:  "PASS_SELECT",
			Query: "select * from vtocc_cached1 where foo='abcd'",
			Result: [][]string{
				{"1", "a", "abcd"},
				{"2", "a", "abcd"},
				{"3", "c", "abcd"},
				{"4", "d", "abcd"},
			},
			Rewritten: []string{
				"select * from vtocc_cached1 where 1 != 1",
				"select * from vtocc_cached1 where foo = 'abcd' limit 10001",
			},
			RowsAffected: 4,
			Plan:         "PASS_SELECT",
			Table:        "vtocc_cached1",
		},
	}
	for _, tcase := range testCases {
		if err := tcase.Test("", client); err != nil {
			t.Error(err)
		}
	}
}
