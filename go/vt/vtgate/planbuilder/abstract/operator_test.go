/*
Copyright 2021 The Vitess Authors.

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

package abstract

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestOperator(t *testing.T) {
	type tcase struct {
		input, output string
	}
	tcases := []tcase{{
		input: "select * from t",
		output: `QueryGraph: {
Tables:
	1:t
}`,
	}, {
		input: "select t.c from t,y,z where t.c = y.c and (t.a = z.a or t.a = y.a) and 1 < 2",
		output: `QueryGraph: {
Tables:
	1:t
	2:y
	4:z
JoinPredicates:
	1:2 - t.c = y.c
	1:2:4 - t.a = z.a or t.a = y.a
ForAll: 1 < 2
}`,
	}, {
		input: "select t.c from t join y on t.id = y.t_id join z on t.id = z.t_id where t.name = 'foo' and y.col = 42 and z.baz = 101",
		output: `QueryGraph: {
Tables:
	1:t where t.` + "`name`" + ` = 'foo'
	2:y where y.col = 42
	4:z where z.baz = 101
JoinPredicates:
	1:2 - t.id = y.t_id
	1:4 - t.id = z.t_id
}`,
	}, {
		input: "select t.c from t,y,z where t.name = 'foo' and y.col = 42 and z.baz = 101 and t.id = y.t_id and t.id = z.t_id",
		output: `QueryGraph: {
Tables:
	1:t where t.` + "`name`" + ` = 'foo'
	2:y where y.col = 42
	4:z where z.baz = 101
JoinPredicates:
	1:2 - t.id = y.t_id
	1:4 - t.id = z.t_id
}`,
	}, {
		input: "select 1 from t where '1' = 1 and 12 = '12'",
		output: `QueryGraph: {
Tables:
	1:t
ForAll: '1' = 1 and 12 = '12'
}`,
	}, {
		input: "select 1 from t left join s on t.id = s.id",
		output: `OuterJoin: {
	Inner: 	QueryGraph: {
	Tables:
		1:t
	}
	Outer: 	QueryGraph: {
	Tables:
		2:s
	}
	Predicate: t.id = s.id
}`,
	}, {
		input: "select 1 from t join s on t.id = s.id and t.name = s.name",
		output: `QueryGraph: {
Tables:
	1:t
	2:s
JoinPredicates:
	1:2 - t.id = s.id and t.` + "`name`" + ` = s.` + "`name`" + `
}`,
	}, {
		input: "select 1 from t left join s on t.id = s.id where t.name = 'Mister'",
		output: `OuterJoin: {
	Inner: 	QueryGraph: {
	Tables:
		1:t where t.` + "`name`" + ` = 'Mister'
	}
	Outer: 	QueryGraph: {
	Tables:
		2:s
	}
	Predicate: t.id = s.id
}`,
	}, {
		input: "select 1 from t right join s on t.id = s.id",
		output: `OuterJoin: {
	Inner: 	QueryGraph: {
	Tables:
		2:s
	}
	Outer: 	QueryGraph: {
	Tables:
		1:t
	}
	Predicate: t.id = s.id
}`,
	}, {
		input: "select 1 from (a left join b on a.id = b.id) join (c left join d on c.id = d.id) on a.id = c.id",
		output: `Join: {
	LHS: 	OuterJoin: {
		Inner: 	QueryGraph: {
		Tables:
			1:a
		}
		Outer: 	QueryGraph: {
		Tables:
			2:b
		}
		Predicate: a.id = b.id
	}
	RHS: 	OuterJoin: {
		Inner: 	QueryGraph: {
		Tables:
			4:c
		}
		Outer: 	QueryGraph: {
		Tables:
			8:d
		}
		Predicate: c.id = d.id
	}
	Predicate: a.id = c.id
}`,
	}}

	for i, tc := range tcases {
		sql := tc.input
		t.Run(fmt.Sprintf("%d %s", i, sql), func(t *testing.T) {
			tree, err := sqlparser.Parse(sql)
			require.NoError(t, err)
			semTable, err := semantics.Analyze(tree, "", &semantics.FakeSI{})
			require.NoError(t, err)
			optree, err := CreateOperatorFromSelect(tree.(*sqlparser.Select), semTable)
			require.NoError(t, err)
			assert.Equal(t, tc.output, testString(optree))
			if t.Failed() {
				fmt.Println(testString(optree))
			}
		})
	}
}

func testString(op Operator) string {
	switch op := op.(type) {
	case *QueryGraph:
		return fmt.Sprintf("QueryGraph: %s", op.testString())
	case *Join:
		leftStr := indent(testString(op.LHS))
		rightStr := indent(testString(op.RHS))
		return fmt.Sprintf("Join: {\n\tLHS: %s\n\tRHS: %s\n\tPredicate: %s\n}", leftStr, rightStr, sqlparser.String(op.Exp))
	case *LeftJoin:
		leftStr := indent(testString(op.Left))
		rightStr := indent(testString(op.Right))
		return fmt.Sprintf("OuterJoin: {\n\tInner: %s\n\tOuter: %s\n\tPredicate: %s\n}", leftStr, rightStr, sqlparser.String(op.Predicate))
	}
	return "implement me"
}

func indent(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = "\t" + line
	}
	return strings.Join(lines, "\n")
}

func (qt *QueryTable) testString() string {
	var alias string
	if !qt.Alias.As.IsEmpty() {
		alias = " AS " + sqlparser.String(qt.Alias.As)
	}
	var preds []string
	for _, predicate := range qt.Predicates {
		preds = append(preds, sqlparser.String(predicate))
	}
	var where string
	if len(preds) > 0 {
		where = " where " + strings.Join(preds, " and ")
	}

	return fmt.Sprintf("\t%d:%s%s%s", qt.TableID, sqlparser.String(qt.Table), alias, where)
}

func (qg *QueryGraph) testString() string {
	return fmt.Sprintf(`{
Tables:
%s%s%s
}`, strings.Join(qg.tableNames(), "\n"), qg.crossPredicateString(), qg.noDepsString())
}

func (qg *QueryGraph) crossPredicateString() string {
	if len(qg.innerJoins) == 0 {
		return ""
	}
	var joinPreds []string
	for deps, predicates := range qg.innerJoins {
		var tables []string
		for _, id := range deps.Constituents() {
			tables = append(tables, fmt.Sprintf("%d", id))
		}
		var expressions []string
		for _, expr := range predicates {
			expressions = append(expressions, sqlparser.String(expr))
		}
		tableConcat := strings.Join(tables, ":")
		exprConcat := strings.Join(expressions, " and ")
		joinPreds = append(joinPreds, fmt.Sprintf("\t%s - %s", tableConcat, exprConcat))
	}
	sort.Strings(joinPreds)
	return fmt.Sprintf("\nJoinPredicates:\n%s", strings.Join(joinPreds, "\n"))
}

func (qg *QueryGraph) tableNames() []string {
	var tables []string
	for _, t := range qg.Tables {
		tables = append(tables, t.testString())
	}
	return tables
}

func (qg *QueryGraph) noDepsString() string {
	if qg.NoDeps == nil {
		return ""
	}
	return fmt.Sprintf("\nForAll: %s", sqlparser.String(qg.NoDeps))
}
