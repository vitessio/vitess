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

package operators

import (
	"testing"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestQP(t *testing.T) {
	tcases := []struct {
		sql string

		expErr   string
		expOrder []ops.OrderBy
	}{
		{
			sql: "select * from user",
		},
		{
			sql: "select 1, count(1) from user",
		},
		{
			sql: "select max(id) from user",
		},
		{
			sql: "select 1, count(1) from user order by 1",
			expOrder: []ops.OrderBy{
				{Inner: &sqlparser.Order{Expr: sqlparser.NewIntLiteral("1")}, SimplifiedExpr: sqlparser.NewIntLiteral("1")},
			},
		},
		{
			sql: "select id from user order by col, id, 1",
			expOrder: []ops.OrderBy{
				{Inner: &sqlparser.Order{Expr: sqlparser.NewColName("col")}, SimplifiedExpr: sqlparser.NewColName("col")},
				{Inner: &sqlparser.Order{Expr: sqlparser.NewColName("id")}, SimplifiedExpr: sqlparser.NewColName("id")},
				{Inner: &sqlparser.Order{Expr: sqlparser.NewColName("id")}, SimplifiedExpr: sqlparser.NewColName("id")},
			},
		},
		{
			sql: "SELECT CONCAT(last_name,', ',first_name) AS full_name FROM mytable ORDER BY full_name", // alias in order not supported
			expOrder: []ops.OrderBy{
				{
					Inner: &sqlparser.Order{Expr: sqlparser.NewColName("full_name")},
					SimplifiedExpr: &sqlparser.FuncExpr{
						Name: sqlparser.NewIdentifierCI("CONCAT"),
						Exprs: sqlparser.SelectExprs{
							&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("last_name")},
							&sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral(", ")},
							&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("first_name")},
						},
					},
				},
			},
		}, {
			sql:    "select count(*) b from user group by b",
			expErr: "cannot group on 'count(*)'",
		},
	}
	ctx := &plancontext.PlanningContext{SemTable: semantics.EmptySemTable()}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tcase.sql)
			require.NoError(t, err)

			sel := stmt.(*sqlparser.Select)
			_, err = semantics.Analyze(sel, "", &semantics.FakeSI{})
			require.NoError(t, err)

			qp, err := CreateQPFromSelect(ctx, sel)
			if tcase.expErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tcase.expErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, len(sel.SelectExprs), len(qp.SelectExprs))
				require.Equal(t, len(tcase.expOrder), len(qp.OrderExprs), "not enough order expressions in QP")
				for index, expOrder := range tcase.expOrder {
					assert.True(t, sqlparser.Equals.SQLNode(expOrder.Inner, qp.OrderExprs[index].Inner), "want: %+v, got %+v", sqlparser.String(expOrder.Inner), sqlparser.String(qp.OrderExprs[index].Inner))
					assert.True(t, sqlparser.Equals.SQLNode(expOrder.SimplifiedExpr, qp.OrderExprs[index].SimplifiedExpr), "want: %v, got %v", sqlparser.String(expOrder.SimplifiedExpr), sqlparser.String(qp.OrderExprs[index].SimplifiedExpr))
				}
			}
		})
	}
}

func TestQPSimplifiedExpr(t *testing.T) {
	testCases := []struct {
		query, expected string
	}{
		{
			query: "select intcol, count(*) from user group by 1",
			expected: `
{
  "Select": [
    "intcol",
    "aggr: count(*)"
  ],
  "Grouping": [
    "intcol"
  ],
  "OrderBy": [],
  "Distinct": false
}`,
		},
		{
			query: "select intcol, textcol from user order by 1, textcol",
			expected: `
{
  "Select": [
    "intcol",
    "textcol"
  ],
  "Grouping": [],
  "OrderBy": [
    "intcol asc",
    "textcol asc"
  ],
  "Distinct": false
}`,
		},
		{
			query: "select intcol, textcol, count(id) from user group by intcol, textcol, extracol order by 2 desc",
			expected: `
{
  "Select": [
    "intcol",
    "textcol",
    "aggr: count(id)"
  ],
  "Grouping": [
    "intcol",
    "textcol",
    "extracol"
  ],
  "OrderBy": [
    "textcol desc"
  ],
  "Distinct": false
}`,
		},
		{
			query: "select distinct col1, col2 from user group by col1, col2",
			expected: `
{
  "Select": [
    "col1",
    "col2"
  ],
  "Grouping": [],
  "OrderBy": [],
  "Distinct": true
}`,
		},
		{
			query: "select distinct count(*) from user",
			expected: `
{
  "Select": [
    "aggr: count(*)"
  ],
  "Grouping": [],
  "OrderBy": [],
  "Distinct": false
}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.query, func(t *testing.T) {
			ast, err := sqlparser.Parse(tc.query)
			require.NoError(t, err)
			sel := ast.(*sqlparser.Select)
			_, err = semantics.Analyze(sel, "", &semantics.FakeSI{})
			require.NoError(t, err)
			ctx := &plancontext.PlanningContext{SemTable: semantics.EmptySemTable()}
			qp, err := CreateQPFromSelect(ctx, sel)
			require.NoError(t, err)
			require.Equal(t, tc.expected[1:], qp.toString())
		})
	}
}

func TestCompareRefInt(t *testing.T) {
	one := 1
	two := 2
	tests := []struct {
		name string
		a    *int
		b    *int
		want bool
	}{
		{
			name: "1<2",
			a:    &one,
			b:    &two,
			want: true,
		}, {
			name: "2<1",
			a:    &two,
			b:    &one,
			want: false,
		}, {
			name: "2<nil",
			a:    &two,
			b:    nil,
			want: true,
		}, {
			name: "nil<1",
			a:    nil,
			b:    &one,
			want: false,
		}, {
			name: "nil<nil",
			a:    nil,
			b:    nil,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, CompareRefInt(tt.a, tt.b))
		})
	}
}
