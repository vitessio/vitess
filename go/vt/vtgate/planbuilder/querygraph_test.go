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
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type tcase struct {
	input  string
	output *queryGraph
}

var threeWayJoin = &queryGraph{
	tables: []*queryTable{{
		tableID:    1,
		alias:      tableAlias("t"),
		table:      tableName("t"),
		predicates: []sqlparser.Expr{equals(colName("t", "name"), literalString("foo"))},
	}, {
		tableID:    2,
		alias:      tableAlias("y"),
		table:      tableName("y"),
		predicates: []sqlparser.Expr{equals(colName("y", "col"), literalInt(42))},
	}, {
		tableID:    4,
		alias:      tableAlias("z"),
		table:      tableName("z"),
		predicates: []sqlparser.Expr{equals(colName("z", "baz"), literalInt(101))},
	}},
	crossTable: map[semantics.TableSet][]sqlparser.Expr{
		1 | 2: {
			equals(
				colName("t", "id"),
				colName("y", "t_id"))},
		1 | 4: {
			equals(
				colName("t", "id"),
				colName("z", "t_id"))}}}

var tcases = []tcase{{
	input: "select * from t",
	output: &queryGraph{
		tables: []*queryTable{{
			tableID: 1,
			alias:   tableAlias("t"),
			table:   tableName("t"),
		}},
		crossTable: map[semantics.TableSet][]sqlparser.Expr{},
	},
}, {
	input: "select t.c from t,y,z where t.c = y.c and (t.a = z.a or t.a = y.a) and 1 < 2",
	output: &queryGraph{
		tables: []*queryTable{{
			tableID: 1,
			alias:   tableAlias("t"),
			table:   tableName("t"),
		}, {
			tableID: 2,
			alias:   tableAlias("y"),
			table:   tableName("y"),
		}, {
			tableID: 4,
			alias:   tableAlias("z"),
			table:   tableName("z"),
		}},
		crossTable: map[semantics.TableSet][]sqlparser.Expr{
			1 | 2: {
				equals(
					colName("t", "c"),
					colName("y", "c"))},
			1 | 2 | 4: {
				or(
					equals(
						colName("t", "a"),
						colName("z", "a")),
					equals(
						colName("t", "a"),
						colName("y", "a")))},
		},
		noDeps: &sqlparser.ComparisonExpr{
			Operator: sqlparser.LessThanOp,
			Left:     literalInt(1),
			Right:    literalInt(2)},
	},
}, {
	input:  "select t.c from t join y on t.id = y.t_id join z on t.id = z.t_id where t.name = 'foo' and y.col = 42 and z.baz = 101",
	output: threeWayJoin,
}, {
	input:  "select t.c from t,y,z where t.name = 'foo' and y.col = 42 and z.baz = 101 and t.id = y.t_id and t.id = z.t_id",
	output: threeWayJoin,
}, {
	input: "select 1 from t where '1' = 1 and 12 = '12'",
	output: &queryGraph{
		tables: []*queryTable{{
			tableID: 1,
			alias:   tableAlias("t"),
			table:   tableName("t"),
		}},
		crossTable: map[semantics.TableSet][]sqlparser.Expr{},
		noDeps: &sqlparser.AndExpr{
			Left:  equals(literalString("1"), literalInt(1)),
			Right: equals(literalInt(12), literalString("12")),
		},
	},
}}

func literalInt(i int) *sqlparser.Literal {
	return &sqlparser.Literal{Type: sqlparser.IntVal, Val: []byte(fmt.Sprintf("%d", i))}
}

func literalString(s string) *sqlparser.Literal {
	return &sqlparser.Literal{Type: sqlparser.StrVal, Val: []byte(s)}
}

func or(left, right sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.OrExpr{
		Left:  left,
		Right: right,
	}
}

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

func tableAlias(name string) *sqlparser.AliasedTableExpr {
	return &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent(name)}}
}

func tableName(name string) sqlparser.TableName {
	return sqlparser.TableName{Name: sqlparser.NewTableIdent(name)}
}

type schemaInf struct{}

func (node *schemaInf) FindTable(tablename sqlparser.TableName) (*vindexes.Table, error) {
	return nil, nil
}

func TestQueryGraph(t *testing.T) {
	for _, tc := range tcases {
		sql := tc.input
		t.Run(sql, func(t *testing.T) {
			tree, err := sqlparser.Parse(sql)
			require.NoError(t, err)
			semTable, err := semantics.Analyse(tree, &schemaInf{})
			require.NoError(t, err)
			qgraph, err := createQGFromSelect(tree.(*sqlparser.Select), semTable)
			require.NoError(t, err)
			mustMatch(t, tc.output, qgraph, "incorrect query graph")
		})
	}
}

var mustMatch = utils.MustMatchFn(
	[]interface{}{ // types with unexported fields
		queryGraph{},
		queryTable{},
		sqlparser.TableIdent{},
	},
	[]string{}, // ignored fields
)
