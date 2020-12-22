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

var tcases = []tcase{
	{
		input: "select * from t",
		output: &queryGraph{
			tables: []*queryTable{
				{
					tableIdentifier: 1,
					alias: &sqlparser.AliasedTableExpr{
						Expr: sqlparser.TableName{
							Name: sqlparser.NewTableIdent("t"),
						},
					},
					table:      sqlparser.TableName{Name: sqlparser.NewTableIdent("t")},
					predicates: nil,
				},
			},
			crossTable: map[semantics.TableSet][]sqlparser.Expr{},
		},
	}, {
		input: "select t.c from t,y,z where t.c = y.c and (t.a = z.a or t.a = y.a) and 1 < 2",
		output: &queryGraph{
			tables: []*queryTable{
				{tableIdentifier: 1, alias: &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t")}}, table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t")}},
				{
					tableIdentifier: 2,
					alias:           &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("y")}},
					table:           sqlparser.TableName{Name: sqlparser.NewTableIdent("y")},
				},
				{
					tableIdentifier: 4,
					alias:           &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("z")}},
					table:           sqlparser.TableName{Name: sqlparser.NewTableIdent("z")},
				},
			},
			crossTable: map[semantics.TableSet][]sqlparser.Expr{
				3: {
					&sqlparser.ComparisonExpr{
						Left:  &sqlparser.ColName{Name: sqlparser.NewColIdent("c"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t")}},
						Right: &sqlparser.ColName{Name: sqlparser.NewColIdent("c"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("y")}},
					},
				},
				7: {
					&sqlparser.OrExpr{
						Left: &sqlparser.ComparisonExpr{
							Left:  &sqlparser.ColName{Name: sqlparser.NewColIdent("a"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t")}},
							Right: &sqlparser.ColName{Name: sqlparser.NewColIdent("a"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("z")}},
						},
						Right: &sqlparser.ComparisonExpr{
							Left:  &sqlparser.ColName{Name: sqlparser.NewColIdent("a"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t")}},
							Right: &sqlparser.ColName{Name: sqlparser.NewColIdent("a"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("y")}},
						},
					},
				},
			},
			noDeps: &sqlparser.ComparisonExpr{
				Operator: 1,
				Left:     &sqlparser.Literal{Type: 1, Val: []uint8{0x31}},
				Right:    &sqlparser.Literal{Type: 1, Val: []uint8{0x32}},
			},
		},
	},
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
