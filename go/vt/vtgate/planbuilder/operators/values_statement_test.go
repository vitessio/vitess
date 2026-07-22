/*
Copyright 2026 The Vitess Authors.

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestValuesStatementOperatorPlanningHelpers(t *testing.T) {
	values := mustParseValuesStatement(t, "values row(1, 2)")
	ctx := &plancontext.PlanningContext{SemTable: semantics.EmptySemTable()}

	op := translateQueryToOp(ctx, values)
	horizon, ok := op.(*Horizon)
	require.True(t, ok)
	assert.Equal(t, values, horizon.Query)

	route, ok := horizon.Source.(*Route)
	require.True(t, ok)
	_, ok = route.Routing.(*DualRouting)
	require.True(t, ok)

	qp := CreateQPFromSelectStatement(ctx, values)
	require.Len(t, qp.SelectExprs, 2)
	assert.Equal(t, "column_0", sqlparser.String(qp.SelectExprs[0].Col))
	assert.Equal(t, "column_1", sqlparser.String(qp.SelectExprs[1].Col))

	selectExprs := horizon.GetSelectExprs(ctx)
	require.Len(t, selectExprs, 2)
	assert.Equal(t, "column_0", sqlparser.String(selectExprs[0]))
	assert.Equal(t, "column_1", sqlparser.String(selectExprs[1]))
	assert.Equal(t, 1, horizon.FindCol(ctx, sqlparser.NewColName("column_1"), false))
	assert.Equal(t, -1, horizon.FindCol(ctx, sqlparser.NewColName("missing"), false))

	pred := &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     sqlparser.NewColName("column_0"),
		Right:    sqlparser.NewIntLiteral("1"),
	}
	tableID := semantics.SingleTableSet(0)
	horizon.TableId = &tableID
	filter, ok := horizon.AddPredicate(ctx, pred).(*Filter)
	require.True(t, ok)
	require.Len(t, filter.Predicates, 1)
	assert.Same(t, pred, filter.Predicates[0])
	assert.Same(t, horizon, filter.Source)

	preds, joinColumns := (&SubQueryBuilder{}).inspectStatement(ctx, values)
	assert.Empty(t, preds)
	assert.Empty(t, joinColumns)

	assert.True(t, isMergeable(ctx, values, horizon))
}

func TestValuesStatementListArgPlanningHelpers(t *testing.T) {
	values := mustParseValuesStatement(t, "values ::vals")
	ctx := &plancontext.PlanningContext{SemTable: semantics.EmptySemTable()}

	op := translateQueryToOp(ctx, values)
	horizon, ok := op.(*Horizon)
	require.True(t, ok)
	assert.Equal(t, values, horizon.Query)

	qp := CreateQPFromSelectStatement(ctx, values)
	assert.Empty(t, qp.SelectExprs)

	assert.Empty(t, horizon.GetSelectExprs(ctx))
	assert.Equal(t, -1, horizon.FindCol(ctx, sqlparser.NewColName("column_0"), false))
	assert.True(t, isMergeable(ctx, values, horizon))
}

func TestToSQLValuesStatementHorizon(t *testing.T) {
	values := mustParseValuesStatement(t, "values row(1, 2)")
	horizon := newHorizon(&Table{}, values)

	stmt, dmlOp, err := ToSQL(&plancontext.PlanningContext{}, horizon)
	require.NoError(t, err)
	require.Nil(t, dmlOp)
	assert.Equal(t, "values row(1, 2)", sqlparser.String(stmt))
}

func TestToSQLValuesStatementListArgHorizon(t *testing.T) {
	values := mustParseValuesStatement(t, "values ::vals")
	horizon := newHorizon(&Table{}, values)

	stmt, dmlOp, err := ToSQL(&plancontext.PlanningContext{}, horizon)
	require.NoError(t, err)
	require.Nil(t, dmlOp)
	assert.Equal(t, "values ::vals", sqlparser.String(stmt))
}

func TestToSQLDerivedValuesStatementProjectsColumns(t *testing.T) {
	values := mustParseValuesStatement(t, "values row(1, 2)")
	tableID := semantics.SingleTableSet(0)
	horizon := newHorizon(&Table{}, values)
	horizon.TableId = &tableID
	horizon.Alias = "sub"
	horizon.Columns = []*sqlparser.ColName{
		sqlparser.NewColName("column_0"),
		sqlparser.NewColName("column_1"),
	}

	stmt, dmlOp, err := ToSQL(&plancontext.PlanningContext{}, horizon)
	require.NoError(t, err)
	require.Nil(t, dmlOp)
	assert.Equal(t, "select column_0, column_1 from (values row(1, 2)) as sub", sqlparser.String(stmt))
}

func TestValuesStatementHorizonAddsHelperColumns(t *testing.T) {
	values := mustParseValuesStatement(t, "values row(1)")
	ctx := &plancontext.PlanningContext{SemTable: semantics.EmptySemTable()}
	horizon := newHorizon(&Table{}, values)

	wsOffset := horizon.AddWSColumn(ctx, 0, false)
	assert.Equal(t, 1, wsOffset)
	assert.Equal(t, "values row(1, weight_string(1))", sqlparser.String(values))

	constOffset := horizon.AddColumn(ctx, true, false, aeWrap(sqlparser.NewIntLiteral("0")))
	assert.Equal(t, 2, constOffset)
	assert.Equal(t, "values row(1, weight_string(1), 0)", sqlparser.String(values))
}

func TestStripDownValuesStatement(t *testing.T) {
	tests := []struct {
		name string
		from *sqlparser.ValuesStatement
	}{
		{
			name: "rows",
			from: &sqlparser.ValuesStatement{
				Rows: sqlparser.Values{sqlparser.ValTuple{
					sqlparser.NewIntLiteral("1"),
				}},
				Order: sqlparser.OrderBy{&sqlparser.Order{
					Expr:      sqlparser.NewColName("column_0"),
					Direction: sqlparser.DescOrder,
				}},
				Limit: &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral("1")},
			},
		},
		{
			name: "list arg",
			from: &sqlparser.ValuesStatement{
				ListArg: sqlparser.ListArg("vals"),
				Limit:   &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral("1")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			to := &sqlparser.ValuesStatement{}

			stripDownQuery(tt.from, to)

			assert.Equal(t, tt.from.Rows, to.Rows)
			assert.Equal(t, tt.from.ListArg, to.ListArg)
			assert.Equal(t, tt.from.Order, to.Order)
			assert.Equal(t, tt.from.Limit, to.Limit)
		})
	}
}

func mustParseValuesStatement(t *testing.T, query string) *sqlparser.ValuesStatement {
	t.Helper()

	stmt, err := sqlparser.NewTestParser().Parse(query)
	require.NoError(t, err)
	values, ok := stmt.(*sqlparser.ValuesStatement)
	require.True(t, ok)
	return values
}
