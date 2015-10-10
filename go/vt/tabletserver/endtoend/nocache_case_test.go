// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"testing"

	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
)

var frameworkErrors = `union failed:
Result mismatch:
'[[1 1] [1 2]]' does not match
'[[2 1] [1 2]]'
RowsAffected mismatch: 2, want 1
Rewritten mismatch:
'[select eid, id from vtocc_a where 1 != 1 union select eid, id from vtocc_b where 1 != 1 select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b]' does not match
'[select eid id from vtocc_a where 1 != 1 union select eid, id from vtocc_b where 1 != 1 select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b]'
Plan mismatch: PASS_SELECT, want aa
Hits mismatch on table stats: 0, want 1
Hits mismatch on query info: 0, want 1
Misses mismatch on table stats: 0, want 2
Misses mismatch on query info: 0, want 2
Absent mismatch on table stats: 0, want 3
Absent mismatch on query info: 0, want 3`

func TestTheFramework(t *testing.T) {
	client := framework.NewDefaultClient()

	expectFail := framework.TestCase{
		Name:  "union",
		Query: "select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b",
		Result: [][]string{
			[]string{"2", "1"},
			[]string{"1", "2"},
		},
		RowsAffected: 1,
		Rewritten: []string{
			"select eid id from vtocc_a where 1 != 1 union select eid, id from vtocc_b where 1 != 1",
			"select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b",
		},
		Plan:   "aa",
		Table:  "bb",
		Hits:   1,
		Misses: 2,
		Absent: 3,
	}
	err := expectFail.Test("", client)
	if err == nil || err.Error() != frameworkErrors {
		t.Errorf("Framework result: \n%q\nexpecting\n%q", err.Error(), frameworkErrors)
	}
}

func TestNocacheCases(t *testing.T) {
	client := framework.NewDefaultClient()

	testCases := []framework.Testable{
		&framework.TestCase{
			Name:  "union",
			Query: "select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b",
			Result: [][]string{
				[]string{"1", "1"},
				[]string{"1", "2"},
			},
			RowsAffected: 2,
			Rewritten: []string{
				"select eid, id from vtocc_a where 1 != 1 union select eid, id from vtocc_b where 1 != 1",
				"select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name: "double union",
			Query: "select /* double union */ eid, id " +
				"from vtocc_a union select eid, id " +
				"from vtocc_b union select eid, id from vtocc_d",
			Result: [][]string{
				[]string{"1", "1"},
				[]string{"1", "2"},
			},
			RowsAffected: 2,
			Rewritten: []string{
				"select eid, id from vtocc_a where 1 != 1 " +
					"union select eid, id from vtocc_b where 1 != 1 " +
					"union select eid, id from vtocc_d where 1 != 1",
				"select /* double union */ eid, id from vtocc_a " +
					"union select eid, id from vtocc_b " +
					"union select eid, id from vtocc_d",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name:  "distinct",
			Query: "select /* distinct */ distinct * from vtocc_a",
			Result: [][]string{
				[]string{"1", "1", "abcd", "efgh"},
				[]string{"1", "2", "bcde", "fghi"},
			},
			Rewritten: []string{
				"select * from vtocc_a where 1 != 1",
				"select /* distinct */ distinct * from vtocc_a limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name:  "group by",
			Query: "select /* group by */ eid, sum(id) from vtocc_a group by eid",
			Result: [][]string{
				[]string{"1", "3"},
			},
			RowsAffected: 1,
			Rewritten: []string{
				"select eid, sum(id) from vtocc_a where 1 != 1",
				"select /* group by */ eid, sum(id) " +
					"from vtocc_a group by eid limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name:  "having",
			Query: "select /* having */ sum(id) from vtocc_a having sum(id) = 3",
			Result: [][]string{
				[]string{"3"},
			},
			RowsAffected: 1,
			Rewritten: []string{
				"select sum(id) from vtocc_a where 1 != 1",
				"select /* having */ sum(id) " +
					"from vtocc_a having sum(id) = 3 limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name:     "limit",
			Query:    "select /* limit */ eid, id from vtocc_a limit :a",
			BindVars: map[string]interface{}{"a": 1},
			Result: [][]string{
				[]string{"1", "1"},
			},
			RowsAffected: 1,
			Rewritten: []string{
				"select eid, id from vtocc_a where 1 != 1",
				"select /* limit */ eid, id from vtocc_a limit 1",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name: "multi-table",
			Query: "select /* multi-table */ a.eid, a.id, b.eid, b.id  " +
				"from vtocc_a as a, vtocc_b as b order by a.eid, a.id, b.eid, b.id",
			Result: [][]string{
				[]string{"1", "1", "1", "1"},
				[]string{"1", "1", "1", "2"},
				[]string{"1", "2", "1", "1"},
				[]string{"1", "2", "1", "2"},
			},
			RowsAffected: 4,
			Rewritten: []string{
				"select a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a, vtocc_b as b where 1 != 1",
				"select /* multi-table */ a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a, vtocc_b as b " +
					"order by a.eid asc, a.id asc, b.eid asc, b.id asc limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name: "join",
			Query: "select /* join */ a.eid, a.id, b.eid, b.id " +
				"from vtocc_a as a join vtocc_b as b on a.eid = b.eid and a.id = b.id",
			Result: [][]string{
				[]string{"1", "1", "1", "1"},
				[]string{"1", "2", "1", "2"},
			},
			RowsAffected: 2,
			Rewritten: []string{
				"select a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a join vtocc_b as b where 1 != 1",
				"select /* join */ a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a join vtocc_b as b " +
					"on a.eid = b.eid and a.id = b.id limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name: "straight_join",
			Query: "select /* straight_join */ a.eid, a.id, b.eid, b.id " +
				"from vtocc_a as a straight_join vtocc_b as b " +
				"on a.eid = b.eid and a.id = b.id",
			Result: [][]string{
				[]string{"1", "1", "1", "1"},
				[]string{"1", "2", "1", "2"},
			},
			RowsAffected: 2,
			Rewritten: []string{
				"select a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a straight_join vtocc_b as b where 1 != 1",
				"select /* straight_join */ a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a straight_join vtocc_b as b " +
					"on a.eid = b.eid and a.id = b.id limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name: "cross join",
			Query: "select /* cross join */ a.eid, a.id, b.eid, b.id " +
				"from vtocc_a as a cross join vtocc_b as b " +
				"on a.eid = b.eid and a.id = b.id",
			Result: [][]string{
				[]string{"1", "1", "1", "1"},
				[]string{"1", "2", "1", "2"},
			},
			RowsAffected: 2,
			Rewritten: []string{
				"select a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a cross join vtocc_b as b where 1 != 1",
				"select /* cross join */ a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a cross join vtocc_b as b " +
					"on a.eid = b.eid and a.id = b.id limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name: "natural join",
			Query: "select /* natural join */ a.eid, a.id, b.eid, b.id " +
				"from vtocc_a as a natural join vtocc_b as b",
			Result: [][]string{
				[]string{"1", "1", "1", "1"},
				[]string{"1", "2", "1", "2"},
			},
			RowsAffected: 2,
			Rewritten: []string{
				"select a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a natural join vtocc_b as b where 1 != 1",
				"select /* natural join */ a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a natural join vtocc_b as b limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name: "left join",
			Query: "select /* left join */ a.eid, a.id, b.eid, b.id " +
				"from vtocc_a as a left join vtocc_b as b " +
				"on a.eid = b.eid and a.id = b.id",
			Result: [][]string{
				[]string{"1", "1", "1", "1"},
				[]string{"1", "2", "1", "2"},
			},
			RowsAffected: 2,
			Rewritten: []string{
				"select a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a left join vtocc_b as b on 1 != 1 where 1 != 1",
				"select /* left join */ a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a left join vtocc_b as b " +
					"on a.eid = b.eid and a.id = b.id limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name: "right join",
			Query: "select /* right join */ a.eid, a.id, b.eid, b.id " +
				"from vtocc_a as a right join vtocc_b as b " +
				"on a.eid = b.eid and a.id = b.id",
			Result: [][]string{
				[]string{"1", "1", "1", "1"},
				[]string{"1", "2", "1", "2"},
			},
			RowsAffected: 2,
			Rewritten: []string{
				"select a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a right join vtocc_b as b " +
					"on 1 != 1 where 1 != 1",
				"select /* right join */ a.eid, a.id, b.eid, b.id " +
					"from vtocc_a as a right join vtocc_b as b " +
					"on a.eid = b.eid and a.id = b.id limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name:  "complex select list",
			Query: "select /* complex select list */ eid+1, id from vtocc_a",
			Result: [][]string{
				[]string{"2", "1"},
				[]string{"2", "2"},
			},
			RowsAffected: 2,
			Rewritten: []string{
				"select eid + 1, id from vtocc_a where 1 != 1",
				"select /* complex select list */ eid + 1, id " +
					"from vtocc_a limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name:  "*",
			Query: "select /* * */ * from vtocc_a",
			Result: [][]string{
				[]string{"1", "1", "abcd", "efgh"},
				[]string{"1", "2", "bcde", "fghi"},
			},
			RowsAffected: 2,
			Rewritten: []string{
				"select * from vtocc_a where 1 != 1",
				"select /* * */ * from vtocc_a limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name:  "table alias",
			Query: "select /* table alias */ a.eid from vtocc_a as a where a.eid=1",
			Result: [][]string{
				[]string{"1"},
				[]string{"1"},
			},
			RowsAffected: 2,
			Rewritten: []string{
				"select a.eid from vtocc_a as a where 1 != 1",
				"select /* table alias */ a.eid from vtocc_a as a " +
					"where a.eid = 1 limit 10001",
			},
			Plan: "PASS_SELECT",
		},
		&framework.TestCase{
			Name: "parenthesised col",
			Query: "select /* parenthesised col */ (eid) from vtocc_a " +
				"where eid = 1 and id = 1",
			Result: [][]string{
				[]string{"1"},
			},
			RowsAffected: 1,
			Rewritten: []string{
				"select (eid) from vtocc_a where 1 != 1",
				"select /* parenthesised col */ (eid) from vtocc_a " +
					"where eid = 1 and id = 1 limit 10001",
			},
			Plan: "PASS_SELECT",
		},
	}
	for _, tcase := range testCases {
		if err := tcase.Test("", client); err != nil {
			t.Error(err)
		}
	}
}
