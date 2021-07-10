/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package endtoend

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var frameworkErrors = `fail failed:
Result mismatch:
'[[1 1] [1 2]]' does not match
'[[2 1] [1 2]]'
RowsReturned mismatch: 2, want 1
Rewritten mismatch:
'["select eid, id from vitess_a where 1 != 1 union select eid, id from vitess_b where 1 != 1" "select /* fail */ eid, id from vitess_a union select eid, id from vitess_b limit 10001"]' does not match
'["select eid id from vitess_a where 1 != 1 union select eid, id from vitess_b where 1 != 1" "select /* fail */ eid, id from vitess_a union select eid, id from vitess_b"]'
Plan mismatch: Select, want aa`

func TestTheFramework(t *testing.T) {
	client := framework.NewClient()

	expectFail := framework.TestCase{
		Name:  "fail",
		Query: "select /* fail */ eid, id from vitess_a union select eid, id from vitess_b",
		Result: [][]string{
			{"2", "1"},
			{"1", "2"},
		},
		RowsReturned: 1,
		Rewritten: []string{
			"select eid id from vitess_a where 1 != 1 union select eid, id from vitess_b where 1 != 1",
			"select /* fail */ eid, id from vitess_a union select eid, id from vitess_b",
		},
		Plan:  "aa",
		Table: "bb",
	}
	err := expectFail.Test("", client)
	require.Error(t, err)
	utils.MustMatch(t, frameworkErrors, err.Error())
}

var TestQueryCases = []framework.Testable{
	&framework.TestCase{
		Name:  "union",
		Query: "select /* union */ eid, id from vitess_a union select eid, id from vitess_b",
		Result: [][]string{
			{"1", "1"},
			{"1", "2"},
		},
		Rewritten: []string{
			"select eid, id from vitess_a where 1 != 1 union select eid, id from vitess_b where 1 != 1",
			"select /* union */ eid, id from vitess_a union select eid, id from vitess_b limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "double union",
		Query: "select /* double union */ eid, id from vitess_a union select eid, id from vitess_b union select eid, id from vitess_d",
		Result: [][]string{
			{"1", "1"},
			{"1", "2"},
		},
		Rewritten: []string{
			"select eid, id from vitess_a where 1 != 1 union select eid, id from vitess_b where 1 != 1 union select eid, id from vitess_d where 1 != 1",
			"select /* double union */ eid, id from vitess_a union select eid, id from vitess_b union select eid, id from vitess_d limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "distinct",
		Query: "select /* distinct */ distinct * from vitess_a",
		Result: [][]string{
			{"1", "1", "abcd", "efgh"},
			{"1", "2", "bcde", "fghi"},
		},
		Rewritten: []string{
			"select * from vitess_a where 1 != 1",
			"select /* distinct */ distinct * from vitess_a limit 10001",
		},
	},
	&framework.TestCase{
		Name:  "group by",
		Query: "select /* group by */ eid, sum(id) from vitess_a group by eid",
		Result: [][]string{
			{"1", "3"},
		},
		Rewritten: []string{
			"select eid, sum(id) from vitess_a where 1 != 1 group by eid",
			"select /* group by */ eid, sum(id) from vitess_a group by eid limit 10001",
		},
		RowsReturned: 1,
	},
	&framework.TestCase{
		Name:  "having",
		Query: "select /* having */ sum(id) from vitess_a having sum(id) = 3",
		Result: [][]string{
			{"3"},
		},
		Rewritten: []string{
			"select sum(id) from vitess_a where 1 != 1",
			"select /* having */ sum(id) from vitess_a having sum(id) = 3 limit 10001",
		},
		RowsReturned: 1,
	},
	&framework.TestCase{
		Name:  "limit",
		Query: "select /* limit */ eid, id from vitess_a limit :a",
		BindVars: map[string]*querypb.BindVariable{
			"a": sqltypes.Int64BindVariable(1),
		},
		Result: [][]string{
			{"1", "1"},
		},
		Rewritten: []string{
			"select eid, id from vitess_a where 1 != 1",
			"select /* limit */ eid, id from vitess_a limit 1",
		},
		RowsReturned: 1,
	},
	&framework.TestCase{
		Name:  "multi-table",
		Query: "select /* multi-table */ a.eid, a.id, b.eid, b.id  from vitess_a as a, vitess_b as b order by a.eid, a.id, b.eid, b.id",
		Result: [][]string{
			{"1", "1", "1", "1"},
			{"1", "1", "1", "2"},
			{"1", "2", "1", "1"},
			{"1", "2", "1", "2"},
		},
		Rewritten: []string{
			"select a.eid, a.id, b.eid, b.id from vitess_a as a, vitess_b as b where 1 != 1",
			"select /* multi-table */ a.eid, a.id, b.eid, b.id from vitess_a as a, vitess_b as b order by a.eid asc, a.id asc, b.eid asc, b.id asc limit 10001",
		},
		RowsReturned: 4,
	},
	&framework.TestCase{
		Name:  "join",
		Query: "select /* join */ a.eid, a.id, b.eid, b.id from vitess_a as a join vitess_b as b on a.eid = b.eid and a.id = b.id",
		Result: [][]string{
			{"1", "1", "1", "1"},
			{"1", "2", "1", "2"},
		},
		Rewritten: []string{
			"select a.eid, a.id, b.eid, b.id from vitess_a as a join vitess_b as b on a.eid = b.eid and a.id = b.id where 1 != 1",
			"select /* join */ a.eid, a.id, b.eid, b.id from vitess_a as a join vitess_b as b on a.eid = b.eid and a.id = b.id limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "straight_join",
		Query: "select /* straight_join */ a.eid, a.id, b.eid, b.id from vitess_a as a straight_join vitess_b as b on a.eid = b.eid and a.id = b.id",
		Result: [][]string{
			{"1", "1", "1", "1"},
			{"1", "2", "1", "2"},
		},
		Rewritten: []string{
			"select a.eid, a.id, b.eid, b.id from vitess_a as a straight_join vitess_b as b on a.eid = b.eid and a.id = b.id where 1 != 1",
			"select /* straight_join */ a.eid, a.id, b.eid, b.id from vitess_a as a straight_join vitess_b as b on a.eid = b.eid and a.id = b.id limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "cross join",
		Query: "select /* cross join */ a.eid, a.id, b.eid, b.id from vitess_a as a cross join vitess_b as b on a.eid = b.eid and a.id = b.id",
		Result: [][]string{
			{"1", "1", "1", "1"},
			{"1", "2", "1", "2"},
		},
		Rewritten: []string{
			"select a.eid, a.id, b.eid, b.id from vitess_a as a join vitess_b as b on a.eid = b.eid and a.id = b.id where 1 != 1",
			"select /* cross join */ a.eid, a.id, b.eid, b.id from vitess_a as a join vitess_b as b on a.eid = b.eid and a.id = b.id limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "natural join",
		Query: "select /* natural join */ a.eid, a.id, b.eid, b.id from vitess_a as a natural join vitess_b as b",
		Result: [][]string{
			{"1", "1", "1", "1"},
			{"1", "2", "1", "2"},
		},
		Rewritten: []string{
			"select a.eid, a.id, b.eid, b.id from vitess_a as a natural join vitess_b as b where 1 != 1",
			"select /* natural join */ a.eid, a.id, b.eid, b.id from vitess_a as a natural join vitess_b as b limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "left join",
		Query: "select /* left join */ a.eid, a.id, b.eid, b.id from vitess_a as a left join vitess_b as b on a.eid = b.eid and a.id = b.id",
		Result: [][]string{
			{"1", "1", "1", "1"},
			{"1", "2", "1", "2"},
		},
		Rewritten: []string{
			"select a.eid, a.id, b.eid, b.id from vitess_a as a left join vitess_b as b on a.eid = b.eid and a.id = b.id where 1 != 1",
			"select /* left join */ a.eid, a.id, b.eid, b.id from vitess_a as a left join vitess_b as b on a.eid = b.eid and a.id = b.id limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "right join",
		Query: "select /* right join */ a.eid, a.id, b.eid, b.id from vitess_a as a right join vitess_b as b on a.eid = b.eid and a.id = b.id",
		Result: [][]string{
			{"1", "1", "1", "1"},
			{"1", "2", "1", "2"},
		},
		Rewritten: []string{
			"select a.eid, a.id, b.eid, b.id from vitess_a as a right join vitess_b as b on a.eid = b.eid and a.id = b.id where 1 != 1",
			"select /* right join */ a.eid, a.id, b.eid, b.id from vitess_a as a right join vitess_b as b on a.eid = b.eid and a.id = b.id limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "complex select list",
		Query: "select /* complex select list */ eid+1, id from vitess_a",
		Result: [][]string{
			{"2", "1"},
			{"2", "2"},
		},
		Rewritten: []string{
			"select eid + 1, id from vitess_a where 1 != 1",
			"select /* complex select list */ eid + 1, id from vitess_a limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "*",
		Query: "select /* * */ * from vitess_a",
		Result: [][]string{
			{"1", "1", "abcd", "efgh"},
			{"1", "2", "bcde", "fghi"},
		},
		Rewritten: []string{
			"select * from vitess_a where 1 != 1",
			"select /* * */ * from vitess_a limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "table alias",
		Query: "select /* table alias */ a.eid from vitess_a as a where a.eid=1",
		Result: [][]string{
			{"1"},
			{"1"},
		},
		Rewritten: []string{
			"select a.eid from vitess_a as a where 1 != 1",
			"select /* table alias */ a.eid from vitess_a as a where a.eid = 1 limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "parenthesised col",
		Query: "select /* parenthesised col */ (eid) from vitess_a where eid = 1 and id = 1",
		Result: [][]string{
			{"1"},
		},
		Rewritten: []string{
			"select eid from vitess_a where 1 != 1",
			"select /* parenthesised col */ eid from vitess_a where eid = 1 and id = 1 limit 10001",
		},
		RowsReturned: 1,
	},
	&framework.MultiCase{
		Name: "for update",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "select /* for update */ eid from vitess_a where eid = 1 and id = 1 for update",
				Result: [][]string{
					{"1"},
				},
				Rewritten: []string{
					"select eid from vitess_a where 1 != 1",
					"select /* for update */ eid from vitess_a where eid = 1 and id = 1 limit 10001 for update",
				},
				RowsReturned: 1,
			},
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "lock in share mode",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "select /* for update */ eid from vitess_a where eid = 1 and id = 1 lock in share mode",
				Result: [][]string{
					{"1"},
				},
				Rewritten: []string{
					"select eid from vitess_a where 1 != 1",
					"select /* for update */ eid from vitess_a where eid = 1 and id = 1 limit 10001 lock in share mode",
				},
				RowsReturned: 1,
			},
			framework.TestQuery("commit"),
		},
	},
	&framework.TestCase{
		Name:  "complex where",
		Query: "select /* complex where */ id from vitess_a where id+1 = 2",
		Result: [][]string{
			{"1"},
		},
		Rewritten: []string{
			"select id from vitess_a where 1 != 1",
			"select /* complex where */ id from vitess_a where id + 1 = 2 limit 10001",
		},
		RowsReturned: 1,
	},
	&framework.TestCase{
		Name:  "complex where (non-value operand)",
		Query: "select /* complex where (non-value operand) */ eid, id from vitess_a where eid = id",
		Result: [][]string{
			{"1", "1"},
		},
		Rewritten: []string{
			"select eid, id from vitess_a where 1 != 1",
			"select /* complex where (non-value operand) */ eid, id from vitess_a where eid = id limit 10001",
		},
		RowsReturned: 1,
	},
	&framework.TestCase{
		Name:  "(condition)",
		Query: "select /* (condition) */ * from vitess_a where (eid = 1)",
		Result: [][]string{
			{"1", "1", "abcd", "efgh"},
			{"1", "2", "bcde", "fghi"},
		},
		Rewritten: []string{
			"select * from vitess_a where 1 != 1",
			"select /* (condition) */ * from vitess_a where eid = 1 limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "inequality",
		Query: "select /* inequality */ * from vitess_a where id > 1",
		Result: [][]string{
			{"1", "2", "bcde", "fghi"},
		},
		Rewritten: []string{
			"select * from vitess_a where 1 != 1",
			"select /* inequality */ * from vitess_a where id > 1 limit 10001",
		},
		RowsReturned: 1,
	},
	&framework.TestCase{
		Name:  "in",
		Query: "select /* in */ * from vitess_a where id in (1, 2)",
		Result: [][]string{
			{"1", "1", "abcd", "efgh"},
			{"1", "2", "bcde", "fghi"},
		},
		Rewritten: []string{
			"select * from vitess_a where 1 != 1",
			"select /* in */ * from vitess_a where id in (1, 2) limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "between",
		Query: "select /* between */ * from vitess_a where id between 1 and 2",
		Result: [][]string{
			{"1", "1", "abcd", "efgh"},
			{"1", "2", "bcde", "fghi"},
		},
		Rewritten: []string{
			"select * from vitess_a where 1 != 1",
			"select /* between */ * from vitess_a where id between 1 and 2 limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "order",
		Query: "select /* order */ * from vitess_a order by id desc",
		Result: [][]string{
			{"1", "2", "bcde", "fghi"},
			{"1", "1", "abcd", "efgh"},
		},
		Rewritten: []string{
			"select * from vitess_a where 1 != 1",
			"select /* order */ * from vitess_a order by id desc limit 10001",
		},
		RowsReturned: 2,
	},
	&framework.TestCase{
		Name:  "select in select list",
		Query: "select (select eid from vitess_a where id = 1), eid from vitess_a where id = 2",
		Result: [][]string{
			{"1", "1"},
		},
		Rewritten: []string{
			"select (select eid from vitess_a where 1 != 1), eid from vitess_a where 1 != 1",
			"select (select eid from vitess_a where id = 1), eid from vitess_a where id = 2 limit 10001",
		},
		RowsReturned: 1,
	},
	&framework.TestCase{
		Name:  "select in from clause",
		Query: "select eid from (select eid from vitess_a where id=2) as a",
		Result: [][]string{
			{"1"},
		},
		Rewritten: []string{
			"select eid from (select eid from vitess_a where 1 != 1) as a where 1 != 1",
			"select eid from (select eid from vitess_a where id = 2) as a limit 10001",
		},
		RowsReturned: 1,
	},
	&framework.MultiCase{
		Name: "select in transaction",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid = 2 and id = 1",
				Rewritten: []string{
					"select * from vitess_a where 1 != 1",
					"select * from vitess_a where eid = 2 and id = 1 limit 10001",
				},
			},
			&framework.TestCase{
				Query: "select * from vitess_a where eid = 2 and id = 1",
				Rewritten: []string{
					"select * from vitess_a where eid = 2 and id = 1 limit 10001",
				},
			},
			&framework.TestCase{
				Query: "select :bv from vitess_a where eid = 2 and id = 1",
				BindVars: map[string]*querypb.BindVariable{
					"bv": sqltypes.Int64BindVariable(1),
				},
				Rewritten: []string{
					"select 1 from vitess_a where eid = 2 and id = 1 limit 10001",
				},
			},
			&framework.TestCase{
				Query: "select :bv from vitess_a where eid = 2 and id = 1",
				BindVars: map[string]*querypb.BindVariable{
					"bv": sqltypes.StringBindVariable("abcd"),
				},
				Rewritten: []string{
					"select 'abcd' from vitess_a where eid = 2 and id = 1 limit 10001",
				},
			},
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "simple insert",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert /* simple */ into vitess_a values (2, 1, 'aaaa', 'bbbb')",
				Rewritten: []string{
					"insert /* simple */ into vitess_a values (2, 1, 'aaaa', 'bbbb')",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid = 2 and id = 1",
				Result: [][]string{
					{"2", "1", "aaaa", "bbbb"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_a where eid>1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "insert ignore",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert /* simple */ ignore into vitess_a values (2, 1, 'aaaa', 'bbbb')",
				Rewritten: []string{
					"insert /* simple */ ignore into vitess_a values (2, 1, 'aaaa', 'bbbb')",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid = 2 and id = 1",
				Result: [][]string{
					{"2", "1", "aaaa", "bbbb"},
				},
			},
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert /* simple */ ignore into vitess_a values (2, 1, 'cccc', 'cccc')",
				Rewritten: []string{
					"insert /* simple */ ignore into vitess_a values (2, 1, 'cccc', 'cccc')",
				},
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid = 2 and id = 1",
				Result: [][]string{
					{"2", "1", "aaaa", "bbbb"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_a where eid>1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "qualified insert",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert /* qualified */ into vitess_a(eid, id, name, foo) values (3, 1, 'aaaa', 'cccc')",
				Rewritten: []string{
					"insert /* qualified */ into vitess_a(eid, id, `name`, foo) values (3, 1, 'aaaa', 'cccc')",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid = 3 and id = 1",
				Result: [][]string{
					{"3", "1", "aaaa", "cccc"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_a where eid>1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "insert with mixed case column names",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_mixed_case(col1, col2) values(1, 2)",
				Rewritten: []string{
					"insert into vitess_mixed_case(col1, col2) values (1, 2)",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select COL1, COL2 from vitess_mixed_case",
				Result: [][]string{
					{"1", "2"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_mixed_case"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "insert auto_increment",
		Cases: []framework.Testable{
			framework.TestQuery("alter table vitess_e auto_increment = 1"),
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert /* auto_increment */ into vitess_e(name, foo) values ('aaaa', 'cccc')",
				Rewritten: []string{
					"insert /* auto_increment */ into vitess_e(`name`, foo) values ('aaaa', 'cccc')",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_e",
				Result: [][]string{
					{"1", "1", "aaaa", "cccc"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_e"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "insert with null auto_increment",
		Cases: []framework.Testable{
			framework.TestQuery("alter table vitess_e auto_increment = 1"),
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert /* auto_increment */ into vitess_e(eid, name, foo) values (NULL, 'aaaa', 'cccc')",
				Rewritten: []string{
					"insert /* auto_increment */ into vitess_e(eid, `name`, foo) values (null, 'aaaa', 'cccc')",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_e",
				Result: [][]string{
					{"1", "1", "aaaa", "cccc"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_e"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "insert with number default value",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert /* num default */ into vitess_a(eid, name, foo) values (3, 'aaaa', 'cccc')",
				Rewritten: []string{
					"insert /* num default */ into vitess_a(eid, `name`, foo) values (3, 'aaaa', 'cccc')",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid = 3 and id = 1",
				Result: [][]string{
					{"3", "1", "aaaa", "cccc"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_a where eid>1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "insert with string default value",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert /* string default */ into vitess_f(id) values (1)",
				Rewritten: []string{
					"insert /* string default */ into vitess_f(id) values (1)",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_f",
				Result: [][]string{
					{"ab", "1"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_f"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "bind values",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert /* bind values */ into vitess_a(eid, id, name, foo) values (:eid, :id, :name, :foo)",
				BindVars: map[string]*querypb.BindVariable{
					"foo":  sqltypes.StringBindVariable("cccc"),
					"eid":  sqltypes.Int64BindVariable(4),
					"name": sqltypes.StringBindVariable("aaaa"),
					"id":   sqltypes.Int64BindVariable(1),
				},
				Rewritten: []string{
					"insert /* bind values */ into vitess_a(eid, id, `name`, foo) values (4, 1, 'aaaa', 'cccc')",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid = 4 and id = 1",
				Result: [][]string{
					{"4", "1", "aaaa", "cccc"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_a where eid>1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "positional values",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert /* positional values */ into vitess_a(eid, id, name, foo) values (?, ?, ?, ?)",
				BindVars: map[string]*querypb.BindVariable{
					"v1": sqltypes.Int64BindVariable(4),
					"v2": sqltypes.Int64BindVariable(1),
					"v3": sqltypes.StringBindVariable("aaaa"),
					"v4": sqltypes.StringBindVariable("cccc"),
				},
				Rewritten: []string{
					"insert /* positional values */ into vitess_a(eid, id, `name`, foo) values (4, 1, 'aaaa', 'cccc')",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid = 4 and id = 1",
				Result: [][]string{
					{"4", "1", "aaaa", "cccc"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_a where eid>1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "out of sequence columns",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_a(id, eid, foo, name) values (-1, 5, 'aaa', 'bbb')",
				Rewritten: []string{
					"insert into vitess_a(id, eid, foo, `name`) values (-1, 5, 'aaa', 'bbb')",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid = 5 and id = -1",
				Result: [][]string{
					{"5", "-1", "bbb", "aaa"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_a where eid>1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "subquery",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert /* subquery */ into vitess_a(eid, name, foo) select eid, name, foo from vitess_c",
				Rewritten: []string{
					"insert /* subquery */ into vitess_a(eid, `name`, foo) select eid, `name`, foo from vitess_c",
				},
				RowsAffected: 2,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid in (10, 11)",
				Result: [][]string{
					{"10", "1", "abcd", "20"},
					{"11", "1", "bcde", "30"},
				},
			},
			framework.TestQuery("alter table vitess_e auto_increment = 20"),
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_e(id, name, foo) select eid, name, foo from vitess_c",
				Rewritten: []string{
					"insert into vitess_e(id, `name`, foo) select eid, `name`, foo from vitess_c",
				},
				RowsAffected: 2,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select eid, id, name, foo from vitess_e",
				Result: [][]string{
					{"20", "10", "abcd", "20"},
					{"21", "11", "bcde", "30"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_a where eid>1"),
			framework.TestQuery("delete from vitess_c where eid<10"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "multi-value",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_a(eid, id, name, foo) values (5, 1, '', ''), (7, 1, '', '')",
				Rewritten: []string{
					"insert into vitess_a(eid, id, `name`, foo) values (5, 1, '', ''), (7, 1, '', '')",
				},
				RowsAffected: 2,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid>1",
				Result: [][]string{
					{"5", "1", "", ""},
					{"7", "1", "", ""},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_a where eid>1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "upsert single row present/absent",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into upsert_test(id1, id2) values (1, 1) on duplicate key update id2 = 1",
				Rewritten: []string{
					"insert into upsert_test(id1, id2) values (1, 1) on duplicate key update id2 = 1",
				},
				RowsAffected: 1,
			},
			&framework.TestCase{
				Query: "select * from upsert_test",
				Result: [][]string{
					{"1", "1"},
				},
			},
			&framework.TestCase{
				Query: "insert into upsert_test(id1, id2) values (1, 2) on duplicate key update id2 = 2",
				Rewritten: []string{
					"insert into upsert_test(id1, id2) values (1, 2) on duplicate key update id2 = 2",
				},
				RowsAffected: 2,
			},
			&framework.TestCase{
				Query: "select * from upsert_test",
				Result: [][]string{
					{"1", "2"},
				},
			},
			&framework.TestCase{
				Query: "insert into upsert_test(id1, id2) values (1, 2) on duplicate key update id2 = 2",
				Rewritten: []string{
					"insert into upsert_test(id1, id2) values (1, 2) on duplicate key update id2 = 2",
				},
			},
			&framework.TestCase{
				Query: "insert ignore into upsert_test(id1, id2) values (1, 3) on duplicate key update id2 = 3",
				Rewritten: []string{
					"insert ignore into upsert_test(id1, id2) values (1, 3) on duplicate key update id2 = 3",
				},
				RowsAffected: 2,
			},
			&framework.TestCase{
				Query: "select * from upsert_test",
				Result: [][]string{
					{"1", "3"},
				},
			},
			framework.TestQuery("commit"),
			framework.TestQuery("begin"),
			framework.TestQuery("delete from upsert_test"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "upsert changes pk",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into upsert_test(id1, id2) values (1, 1) on duplicate key update id1 = 1",
				Rewritten: []string{
					"insert into upsert_test(id1, id2) values (1, 1) on duplicate key update id1 = 1",
				},
				RowsAffected: 1,
			},
			&framework.TestCase{
				Query: "select * from upsert_test",
				Result: [][]string{
					{"1", "1"},
				},
			},
			&framework.TestCase{
				Query: "insert into upsert_test(id1, id2) values (1, 2) on duplicate key update id1 = 2",
				Rewritten: []string{
					"insert into upsert_test(id1, id2) values (1, 2) on duplicate key update id1 = 2",
				},
				RowsAffected: 2,
			},
			&framework.TestCase{
				Query: "select * from upsert_test",
				Result: [][]string{
					{"2", "1"},
				},
			},
			framework.TestQuery("commit"),
			framework.TestQuery("begin"),
			framework.TestQuery("delete from upsert_test"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "upsert single row with values()",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into upsert_test(id1, id2) values (1, 1) on duplicate key update id2 = values(id2) + 1",
				Rewritten: []string{
					"insert into upsert_test(id1, id2) values (1, 1) on duplicate key update id2 = values(id2) + 1",
				},
				RowsAffected: 1,
			},
			&framework.TestCase{
				Query: "select * from upsert_test",
				Result: [][]string{
					{"1", "1"},
				},
			},
			&framework.TestCase{
				Query: "insert into upsert_test(id1, id2) values (1, 2) on duplicate key update id2 = values(id2) + 1",
				Rewritten: []string{
					"insert into upsert_test(id1, id2) values (1, 2) on duplicate key update id2 = values(id2) + 1",
				},
				RowsAffected: 2,
			},
			&framework.TestCase{
				Query: "select * from upsert_test",
				Result: [][]string{
					{"1", "3"},
				},
			},
			&framework.TestCase{
				Query: "insert into upsert_test(id1, id2) values (1, 2) on duplicate key update id2 = values(id1)",
				Rewritten: []string{
					"insert into upsert_test(id1, id2) values (1, 2) on duplicate key update id2 = values(id1)",
				},
			},
			&framework.TestCase{
				Query: "select * from upsert_test",
				Result: [][]string{
					{"1", "1"},
				},
			},
			&framework.TestCase{
				Query: "insert ignore into upsert_test(id1, id2) values (1, 3) on duplicate key update id2 = greatest(values(id1), values(id2))",
				Rewritten: []string{
					"insert ignore into upsert_test(id1, id2) values (1, 3) on duplicate key update id2 = greatest(values(id1), values(id2))",
				},
				RowsAffected: 2,
			},
			&framework.TestCase{
				Query: "select * from upsert_test",
				Result: [][]string{
					{"1", "3"},
				},
			},
			framework.TestQuery("commit"),
			framework.TestQuery("begin"),
			framework.TestQuery("delete from upsert_test"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "update",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update /* pk */ vitess_a set foo='bar' where eid = 1 and id = 1",
				Rewritten: []string{
					"update /* pk */ vitess_a set foo = 'bar' where eid = 1 and id = 1 limit 10001",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select foo from vitess_a where id = 1",
				Result: [][]string{
					{"bar"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("update vitess_a set foo='efgh' where id=1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "single in update",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update /* pk */ vitess_a set foo='bar' where eid = 1 and id in (1, 2)",
				Rewritten: []string{
					"update /* pk */ vitess_a set foo = 'bar' where eid = 1 and id in (1, 2) limit 10001",
				},
				RowsAffected: 2,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select foo from vitess_a where id = 1",
				Result: [][]string{
					{"bar"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("update vitess_a set foo='efgh' where id=1"),
			framework.TestQuery("update vitess_a set foo='fghi' where id=2"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "double in update",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update /* pk */ vitess_a set foo='bar' where eid in (1) and id in (1, 2)",
				Rewritten: []string{
					"update /* pk */ vitess_a set foo = 'bar' where eid in (1) and id in (1, 2) limit 10001",
				},
				RowsAffected: 2,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select foo from vitess_a where id = 1",
				Result: [][]string{
					{"bar"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("update vitess_a set foo='efgh' where id=1"),
			framework.TestQuery("update vitess_a set foo='fghi' where id=2"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "double in 2 update",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update /* pk */ vitess_a set foo='bar' where eid in (1, 2) and id in (1, 2)",
				Rewritten: []string{
					"update /* pk */ vitess_a set foo = 'bar' where eid in (1, 2) and id in (1, 2) limit 10001",
				},
				RowsAffected: 2,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select foo from vitess_a where id = 1",
				Result: [][]string{
					{"bar"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("update vitess_a set foo='efgh' where id=1"),
			framework.TestQuery("update vitess_a set foo='fghi' where id=2"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "pk change update",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update vitess_a set eid = 2 where eid = 1 and id = 1",
				Rewritten: []string{
					"update vitess_a set eid = 2 where eid = 1 and id = 1 limit 10001",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select eid from vitess_a where id = 1",
				Result: [][]string{
					{"2"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("update vitess_a set eid=1 where id=1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "partial pk update",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update /* pk */ vitess_a set foo='bar' where id = 1",
				Rewritten: []string{
					"update /* pk */ vitess_a set foo = 'bar' where id = 1 limit 10001",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select foo from vitess_a where id = 1",
				Result: [][]string{
					{"bar"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("update vitess_a set foo='efgh' where id=1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "limit update",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update /* pk */ vitess_a set foo='bar' where eid = 1 limit 1",
				Rewritten: []string{
					"update /* pk */ vitess_a set foo = 'bar' where eid = 1 limit 1",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select foo from vitess_a where id = 1",
				Result: [][]string{
					{"bar"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("update vitess_a set foo='efgh' where id=1"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "order by update",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update /* pk */ vitess_a set foo='bar' where eid = 1 order by id desc limit 1",
				Rewritten: []string{
					"update /* pk */ vitess_a set foo = 'bar' where eid = 1 order by id desc limit 1",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select foo from vitess_a where id = 2",
				Result: [][]string{
					{"bar"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("update vitess_a set foo='fghi' where id=2"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "missing where update",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update vitess_a set foo='bar'",
				Rewritten: []string{
					"update vitess_a set foo = 'bar' limit 10001",
				},
				RowsAffected: 2,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a",
				Result: [][]string{
					{"1", "1", "abcd", "bar"},
					{"1", "2", "bcde", "bar"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("update vitess_a set foo='efgh' where id=1"),
			framework.TestQuery("update vitess_a set foo='fghi' where id=2"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "single pk update one row update",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_f(vb,id) values ('a', 1), ('b', 2)"),
			framework.TestQuery("commit"),
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update vitess_f set id=2 where vb='a'",
				Rewritten: []string{
					"update vitess_f set id = 2 where vb = 'a' limit 10001",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_f",
				Result: [][]string{
					{"a", "2"},
					{"b", "2"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_f"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "single pk update two rows",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_f(vb,id) values ('a', 1), ('b', 2)"),
			framework.TestQuery("commit"),
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update vitess_f set id=3 where vb in ('a', 'b')",
				Rewritten: []string{
					"update vitess_f set id = 3 where vb in ('a', 'b') limit 10001",
				},
				RowsAffected: 2,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_f",
				Result: [][]string{
					{"a", "3"},
					{"b", "3"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_f"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "single pk update subquery",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_f(vb,id) values ('a', 1), ('b', 2)"),
			framework.TestQuery("commit"),
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update vitess_f set id=4 where id >= 0",
				Rewritten: []string{
					"update vitess_f set id = 4 where id >= 0 limit 10001",
				},
				RowsAffected: 2,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_f",
				Result: [][]string{
					{"a", "4"},
					{"b", "4"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_f"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "single pk update subquery no rows",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_f(vb,id) values ('a', 1), ('b', 2)"),
			framework.TestQuery("commit"),
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update vitess_f set id=4 where id < 0",
				Rewritten: []string{
					"update vitess_f set id = 4 where id < 0 limit 10001",
				},
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_f",
				Result: [][]string{
					{"a", "1"},
					{"b", "2"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_f"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "delete",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_a(eid, id, name, foo) values (2, 1, '', '')"),
			&framework.TestCase{
				Query: "delete /* pk */ from vitess_a where eid = 2 and id = 1",
				Rewritten: []string{
					"delete /* pk */ from vitess_a where eid = 2 and id = 1 limit 10001",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid=2",
			},
		},
	},
	&framework.MultiCase{
		Name: "single in delete",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_a(eid, id, name, foo) values (2, 1, '', '')"),
			&framework.TestCase{
				Query: "delete /* pk */ from vitess_a where eid = 2 and id in (1, 2)",
				Rewritten: []string{
					"delete /* pk */ from vitess_a where eid = 2 and id in (1, 2) limit 10001",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid=2",
			},
		},
	},
	&framework.MultiCase{
		Name: "double in delete",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_a(eid, id, name, foo) values (2, 1, '', '')"),
			&framework.TestCase{
				Query: "delete /* pk */ from vitess_a where eid in (2) and id in (1, 2)",
				Rewritten: []string{
					"delete /* pk */ from vitess_a where eid in (2) and id in (1, 2) limit 10001",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid=2",
			},
		},
	},
	&framework.MultiCase{
		Name: "double in 2 delete",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_a(eid, id, name, foo) values (2, 1, '', '')"),
			&framework.TestCase{
				Query: "delete /* pk */ from vitess_a where eid in (2, 3) and id in (1, 2)",
				Rewritten: []string{
					"delete /* pk */ from vitess_a where eid in (2, 3) and id in (1, 2) limit 10001",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid=2",
			},
		},
	},
	&framework.MultiCase{
		Name: "complex where delete",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_a(eid, id, name, foo) values (2, 1, '', '')"),
			&framework.TestCase{
				Query: "delete from vitess_a where eid = 1+1 and id = 1",
				Rewritten: []string{
					"delete from vitess_a where eid = 1 + 1 and id = 1 limit 10001",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid=2",
			},
		},
	},
	&framework.MultiCase{
		Name: "partial pk delete",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_a(eid, id, name, foo) values (2, 1, '', '')"),
			&framework.TestCase{
				Query: "delete from vitess_a where eid = 2",
				Rewritten: []string{
					"delete from vitess_a where eid = 2 limit 10001",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid=2",
			},
		},
	},
	&framework.MultiCase{
		Name: "limit delete",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_a(eid, id, name, foo) values (2, 1, '', '')"),
			&framework.TestCase{
				Query: "delete from vitess_a where eid = 2 limit 1",
				Rewritten: []string{
					"delete from vitess_a where eid = 2 limit 1",
				},
				RowsAffected: 1,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid=2",
			},
		},
	},
	&framework.MultiCase{
		Name: "order by delete",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_a(eid, id, name, foo) values (2, 1, '', '')"),
			framework.TestQuery("insert into vitess_a(eid, id, name, foo) values (2, 2, '', '')"),
			&framework.TestCase{
				Query: "delete from vitess_a where eid = 2 order by id desc",
				Rewritten: []string{
					"delete from vitess_a where eid = 2 order by id desc limit 10001",
				},
				RowsAffected: 2,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_a where eid=2",
			},
		},
	},
	&framework.MultiCase{
		Name: "integer data types",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_ints values(:tiny, :tinyu, :small, :smallu, :medium, :mediumu, :normal, :normalu, :big, :bigu, :year)",
				BindVars: map[string]*querypb.BindVariable{
					"medium":  sqltypes.Int64BindVariable(-8388608),
					"smallu":  sqltypes.Int64BindVariable(65535),
					"normal":  sqltypes.Int64BindVariable(-2147483648),
					"big":     sqltypes.Int64BindVariable(-9223372036854775808),
					"tinyu":   sqltypes.Int64BindVariable(255),
					"year":    sqltypes.Int64BindVariable(2012),
					"tiny":    sqltypes.Int64BindVariable(-128),
					"bigu":    sqltypes.Uint64BindVariable(18446744073709551615),
					"normalu": sqltypes.Int64BindVariable(4294967295),
					"small":   sqltypes.Int64BindVariable(-32768),
					"mediumu": sqltypes.Int64BindVariable(16777215),
				},
				Rewritten: []string{
					"insert into vitess_ints values (-128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 2012)",
				},
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_ints where tiny = -128",
				Result: [][]string{
					{"-128", "255", "-32768", "65535", "-8388608", "16777215", "-2147483648", "4294967295", "-9223372036854775808", "18446744073709551615", "2012"},
				},
				Rewritten: []string{
					"select * from vitess_ints where 1 != 1",
					"select * from vitess_ints where tiny = -128 limit 10001",
				},
			},
			&framework.TestCase{
				Query: "select * from vitess_ints where tiny = -128",
				Result: [][]string{
					{"-128", "255", "-32768", "65535", "-8388608", "16777215", "-2147483648", "4294967295", "-9223372036854775808", "18446744073709551615", "2012"},
				},
				Rewritten: []string{
					"select * from vitess_ints where tiny = -128 limit 10001",
				},
			},
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_ints select 2, tinyu, small, smallu, medium, mediumu, normal, normalu, big, bigu, y from vitess_ints",
				Rewritten: []string{
					"insert into vitess_ints select 2, tinyu, small, smallu, medium, mediumu, normal, normalu, big, bigu, y from vitess_ints",
				},
			},
			framework.TestQuery("commit"),
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_ints"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "fractional data types",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_fracts values(:id, :deci, :num, :f, :d)",
				BindVars: map[string]*querypb.BindVariable{
					"d":    sqltypes.Float64BindVariable(4.99),
					"num":  sqltypes.StringBindVariable("2.99"),
					"id":   sqltypes.Int64BindVariable(1),
					"f":    sqltypes.Float64BindVariable(3.99),
					"deci": sqltypes.StringBindVariable("1.99"),
				},
				Rewritten: []string{
					"insert into vitess_fracts values (1, '1.99', '2.99', 3.99, 4.99)",
				},
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_fracts where id = 1",
				Result: [][]string{
					{"1", "1.99", "2.99", "3.99", "4.99"},
				},
				Rewritten: []string{
					"select * from vitess_fracts where 1 != 1",
					"select * from vitess_fracts where id = 1 limit 10001",
				},
			},
			&framework.TestCase{
				Query: "select * from vitess_fracts where id = 1",
				Result: [][]string{
					{"1", "1.99", "2.99", "3.99", "4.99"},
				},
				Rewritten: []string{
					"select * from vitess_fracts where id = 1 limit 10001",
				},
			},
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_fracts select 2, deci, num, f, d from vitess_fracts",
				Rewritten: []string{
					"insert into vitess_fracts select 2, deci, num, f, d from vitess_fracts",
				},
			},
			framework.TestQuery("commit"),
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_fracts"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "string data types",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_strings values (:vb, :c, :vc, :b, :tb, :bl, :ttx, :tx, :en, :s)",
				BindVars: map[string]*querypb.BindVariable{
					"ttx": sqltypes.StringBindVariable("g"),
					"vb":  sqltypes.StringBindVariable("a"),
					"vc":  sqltypes.StringBindVariable("c"),
					"en":  sqltypes.StringBindVariable("a"),
					"tx":  sqltypes.StringBindVariable("h"),
					"bl":  sqltypes.StringBindVariable("f"),
					"s":   sqltypes.StringBindVariable("a,b"),
					"b":   sqltypes.StringBindVariable("d"),
					"tb":  sqltypes.StringBindVariable("e"),
					"c":   sqltypes.StringBindVariable("b"),
				},
				Rewritten: []string{
					"insert into vitess_strings values ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'a', 'a,b')",
				},
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_strings where vb = 'a'",
				Result: [][]string{
					{"a", "b", "c", "d\x00\x00\x00", "e", "f", "g", "h", "a", "a,b"},
				},
				Rewritten: []string{
					"select * from vitess_strings where 1 != 1",
					"select * from vitess_strings where vb = 'a' limit 10001",
				},
			},
			&framework.TestCase{
				Query: "select * from vitess_strings where vb = 'a'",
				Result: [][]string{
					{"a", "b", "c", "d\x00\x00\x00", "e", "f", "g", "h", "a", "a,b"},
				},
				Rewritten: []string{
					"select * from vitess_strings where vb = 'a' limit 10001",
				},
			},
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_strings select 'b', c, vc, b, tb, bl, ttx, tx, en, s from vitess_strings",
				Rewritten: []string{
					"insert into vitess_strings select 'b', c, vc, b, tb, bl, ttx, tx, en, s from vitess_strings",
				},
			},
			framework.TestQuery("commit"),
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_strings"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "misc data types",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_misc values(:id, :b, :d, :dt, :t, point(1, 2))",
				BindVars: map[string]*querypb.BindVariable{
					"t":  sqltypes.StringBindVariable("15:45:45"),
					"dt": sqltypes.StringBindVariable("2012-01-01 15:45:45"),
					"b":  sqltypes.StringBindVariable("\x01"),
					"id": sqltypes.Int64BindVariable(1),
					"d":  sqltypes.StringBindVariable("2012-01-01"),
				},
				Rewritten: []string{
					"insert into vitess_misc values (1, '\x01', '2012-01-01', '2012-01-01 15:45:45', '15:45:45', point(1, 2))",
				},
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_misc where id = 1",
				Result: [][]string{
					{"1", "\x01", "2012-01-01", "2012-01-01 15:45:45", "15:45:45", point12},
				},
				Rewritten: []string{
					"select * from vitess_misc where 1 != 1",
					"select * from vitess_misc where id = 1 limit 10001",
				},
			},
			&framework.TestCase{
				Query: "select * from vitess_misc where id = 1",
				Result: [][]string{
					{"1", "\x01", "2012-01-01", "2012-01-01 15:45:45", "15:45:45", point12},
				},
				Rewritten: []string{
					"select * from vitess_misc where id = 1 limit 10001",
				},
			},
			framework.TestQuery("begin"),
			&framework.TestCase{
				// Skip geometry test. The binary representation is non-trivial to represent as go string.
				Query: "insert into vitess_misc(id, b, d, dt, t) select 2, b, d, dt, t from vitess_misc",
				Rewritten: []string{
					"insert into vitess_misc(id, b, d, dt, t) select 2, b, d, dt, t from vitess_misc",
				},
			},
			framework.TestQuery("commit"),
			framework.TestQuery("begin"),
			framework.TestQuery("delete from vitess_misc"),
			framework.TestQuery("commit"),
		},
	},
	&framework.MultiCase{
		Name: "boolean expressions",
		Cases: []framework.Testable{
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_bool(bval, sval, ival) values (true, 'foo', false)"),
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_bool",
				Result: [][]string{
					{"1", "1", "foo", "0"},
				},
			},
			framework.TestQuery("begin"),
			framework.TestQuery("insert into vitess_bool(bval, sval, ival) values (true, 'bar', 23)"),
			framework.TestQuery("insert into vitess_bool(bval, sval, ival) values (true, 'baz', 2342)"),
			framework.TestQuery("insert into vitess_bool(bval, sval, ival) values (true, 'test', 123)"),
			framework.TestQuery("insert into vitess_bool(bval, sval, ival) values (true, 'aa', 384)"),
			framework.TestQuery("insert into vitess_bool(bval, sval, ival) values (false, 'bbb', 213)"),
			framework.TestQuery("insert into vitess_bool(bval, sval, ival) values (false, 'cc', 24342)"),
			framework.TestQuery("insert into vitess_bool(bval, sval, ival) values (false, 'd', 1231)"),
			framework.TestQuery("insert into vitess_bool(bval, sval, ival) values (false, 'ee', 3894)"),
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_bool where bval",
				Result: [][]string{
					{"1", "1", "foo", "0"},
					{"2", "1", "bar", "23"},
					{"3", "1", "baz", "2342"},
					{"4", "1", "test", "123"},
					{"5", "1", "aa", "384"},
				},
			},
			&framework.TestCase{
				Query: "select * from vitess_bool where case sval when 'foo' then true when 'test' then true else false end",
				Result: [][]string{
					{"1", "1", "foo", "0"},
					{"4", "1", "test", "123"},
				},
			},
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "insert into vitess_bool(auto, bval, sval, ival) values (1, false, 'test2', 191) on duplicate key update bval = false",
				Rewritten: []string{
					"insert into vitess_bool(auto, bval, sval, ival) values (1, false, 'test2', 191) on duplicate key update bval = false",
				},
				RowsAffected: 2,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_bool where bval",
				Result: [][]string{
					{"2", "1", "bar", "23"},
					{"3", "1", "baz", "2342"},
					{"4", "1", "test", "123"},
					{"5", "1", "aa", "384"},
				},
			},
			&framework.TestCase{
				Query: "select * from vitess_bool where not bval",
				Result: [][]string{
					{"1", "0", "foo", "0"},
					{"6", "0", "bbb", "213"},
					{"7", "0", "cc", "24342"},
					{"8", "0", "d", "1231"},
					{"9", "0", "ee", "3894"},
				},
			},
			framework.TestQuery("begin"),
			&framework.TestCase{
				Query: "update vitess_bool set sval = 'test' where bval is false or ival = 23",
				Rewritten: []string{
					"update vitess_bool set sval = 'test' where bval is false or ival = 23 limit 10001",
				},
				RowsAffected: 6,
			},
			framework.TestQuery("commit"),
			&framework.TestCase{
				Query: "select * from vitess_bool where not bval",
				Result: [][]string{
					{"1", "0", "test", "0"},
					{"6", "0", "test", "213"},
					{"7", "0", "test", "24342"},
					{"8", "0", "test", "1231"},
					{"9", "0", "test", "3894"},
				},
			},
			&framework.TestCase{
				Query: "select (bval or ival) from vitess_bool where ival = 213",
				Result: [][]string{
					{"1"},
				},
			},
			&framework.TestCase{
				Query: "select bval from vitess_bool where ival = 213",
				Result: [][]string{
					{"0"},
				},
			},
		},
	},
	&framework.MultiCase{
		Name: "impossible queries",
		Cases: []framework.Testable{
			&framework.TestCase{
				Name:  "specific column",
				Query: "select eid from vitess_a where 1 != 1",
				Rewritten: []string{
					"select eid from vitess_a where 1 != 1",
				},
				RowsAffected: 0,
			},
			&framework.TestCase{
				Name:  "all columns",
				Query: "select * from vitess_a where 1 != 1",
				Rewritten: []string{
					"select * from vitess_a where 1 != 1",
				},
				RowsAffected: 0,
			},
			&framework.TestCase{
				Name:  "bind vars",
				Query: "select :bv from vitess_a where 1 != 1",
				BindVars: map[string]*querypb.BindVariable{
					"bv": sqltypes.Int64BindVariable(1),
				},
				Rewritten: []string{
					"select 1 from vitess_a where 1 != 1 limit 10001",
				},
				RowsAffected: 0,
			},
		},
	},
}

// Most of these tests are not really needed because the queries are mostly pass-through.
// They're left as is because they still demonstrate the variety of constructs being supported.
func TestQueries(t *testing.T) {
	client := framework.NewClient()

	for _, tcase := range TestQueryCases {
		if err := tcase.Test("", client); err != nil {
			t.Error(err)
		}
	}
}

func BenchmarkTabletQueries(b *testing.B) {
	client := framework.NewClient()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tcase := TestQueryCases[rand.Intn(len(TestQueryCases))]
		if err := tcase.Benchmark(client); err != nil {
			b.Error(err)
		}
	}
}
