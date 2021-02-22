/*
Copyright 2020 The Vitess Authors.

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

package planbuilder

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type tcase struct {
	input, output string
}

var tcases = []tcase{{
	input: "select * from t",
	output: `{
Tables:
	1:t
}`,
}, {
	input: "select t.c from t,y,z where t.c = y.c and (t.a = z.a or t.a = y.a) and 1 < 2",
	output: `{
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
	output: `{
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
	output: `{
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
	output: `{
Tables:
	1:t
ForAll: '1' = 1 and 12 = '12'
}`,
}, {
	input: "select 1 from t where exists (select 1)",
	output: `{
Tables:
	1:t
ForAll: exists (select 1 from dual)
SubQueries:
(select 1 from dual) - 	{
	Tables:
		2:dual
	}
}`,
}}

func equals(left, right sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     left,
		Right:    right,
	}
}

func colName(table, column string) *sqlparser.ColName {
	return &sqlparser.ColName{Name: sqlparser.NewColIdent(column), Qualifier: tableName(table)}
}

func tableName(name string) sqlparser.TableName {
	return sqlparser.TableName{Name: sqlparser.NewTableIdent(name)}
}

func TestQueryGraph(t *testing.T) {
	for i, tc := range tcases {
		sql := tc.input
		t.Run(fmt.Sprintf("%d %s", i, sql), func(t *testing.T) {
			tree, err := sqlparser.Parse(sql)
			require.NoError(t, err)
			semTable, err := semantics.Analyse(tree)
			require.NoError(t, err)
			qgraph, err := createQGFromSelect(tree.(*sqlparser.Select), semTable)
			require.NoError(t, err)
			assert.Equal(t, tc.output, qgraph.testString())
			utils.MustMatch(t, tc.output, qgraph.testString(), "incorrect query graph")
		})
	}
}

func TestString(t *testing.T) {
	tree, err := sqlparser.Parse("select * from a,b join c on b.id = c.id where a.id = b.id and b.col IN (select 42) and func() = 'foo'")
	require.NoError(t, err)
	semTable, err := semantics.Analyse(tree)
	require.NoError(t, err)
	qgraph, err := createQGFromSelect(tree.(*sqlparser.Select), semTable)
	require.NoError(t, err)
	utils.MustMatch(t, `{
Tables:
	1:a
	2:b where b.col in (select 42 from dual)
	4:c
JoinPredicates:
	1:2 - a.id = b.id
	2:4 - b.id = c.id
ForAll: func() = 'foo'
SubQueries:
(select 42 from dual) - 	{
	Tables:
		8:dual
	}
}`, qgraph.testString())
}

func (qt *queryTable) testString() string {
	var alias string
	if !qt.alias.As.IsEmpty() {
		alias = " AS " + sqlparser.String(qt.alias.As)
	}
	var preds []string
	for _, predicate := range qt.predicates {
		preds = append(preds, sqlparser.String(predicate))
	}
	var where string
	if len(preds) > 0 {
		where = " where " + strings.Join(preds, " and ")
	}

	return fmt.Sprintf("\t%d:%s%s%s", qt.tableID, sqlparser.String(qt.table), alias, where)
}

func (qg *queryGraph) testString() string {
	return fmt.Sprintf(`{
Tables:
%s%s%s%s
}`, strings.Join(qg.tableNames(), "\n"), qg.crossPredicateString(), qg.noDepsString(), qg.subqueriesString())
}

func (qg *queryGraph) crossPredicateString() string {
	if len(qg.crossTable) == 0 {
		return ""
	}
	var joinPreds []string
	for deps, predicates := range qg.crossTable {
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

func (qg *queryGraph) tableNames() []string {
	var tables []string
	for _, t := range qg.tables {
		tables = append(tables, t.testString())
	}
	return tables
}

func (qg *queryGraph) subqueriesString() string {
	if len(qg.subqueries) == 0 {
		return ""
	}
	var graphs []string
	for sq, qgraphs := range qg.subqueries {
		key := sqlparser.String(sq)
		for _, inner := range qgraphs {
			str := inner.testString()
			splitInner := strings.Split(str, "\n")
			for i, s := range splitInner {
				splitInner[i] = "\t" + s
			}
			graphs = append(graphs, fmt.Sprintf("%s - %s", key, strings.Join(splitInner, "\n")))
		}
	}
	return fmt.Sprintf("\nSubQueries:\n%s", strings.Join(graphs, "\n"))
}

func (qg *queryGraph) noDepsString() string {
	if qg.noDeps == nil {
		return ""
	}
	return fmt.Sprintf("\nForAll: %s", sqlparser.String(qg.noDeps))
}
