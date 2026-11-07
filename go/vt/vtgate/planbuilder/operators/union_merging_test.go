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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestHasUnresolvedUnionStarOnlyAtOutput(t *testing.T) {
	union := &Union{unionColumns: []sqlparser.SelectExpr{&sqlparser.StarExpr{}}}
	require.True(t, hasUnresolvedUnionStar(union))
	require.True(t, hasUnresolvedUnionStar(&Ordering{unaryOperator: newUnaryOp(union)}))

	selectExprs := &sqlparser.SelectExprs{Exprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{
		Expr: sqlparser.NewColName("marker"),
		As:   sqlparser.NewIdentifierCI("x"),
	}}}
	horizon := &Horizon{
		unaryOperator: newUnaryOp(union),
		Query:         &sqlparser.Select{SelectExprs: selectExprs},
	}
	require.False(t, hasUnresolvedUnionStar(horizon))
}

func TestAddRuntimeWeightStringColumnKeepsUnionProjection(t *testing.T) {
	makeSource := func() *sqlparser.Select {
		selectExprs := &sqlparser.SelectExprs{Exprs: []sqlparser.SelectExpr{&sqlparser.StarExpr{}}}
		return &sqlparser.Select{SelectExprs: selectExprs}
	}
	left, right := makeSource(), makeSource()
	union := &Union{
		Sources: []Operator{
			&Horizon{Query: left},
			&Horizon{Query: right},
		},
		unionColumns: []sqlparser.SelectExpr{&sqlparser.StarExpr{}},
	}

	name, ok := union.addRuntimeWeightStringColumn(sqlparser.NewColName("name"))
	require.True(t, ok)
	require.NotEmpty(t, name)
	require.Len(t, union.unionColumns, 1)
	require.Len(t, left.SelectExprs.Exprs, 2)
	require.Len(t, right.SelectExprs.Exprs, 2)
	added, ok := left.SelectExprs.Exprs[1].(*sqlparser.AliasedExpr)
	require.True(t, ok)
	require.Equal(t, name, added.As.String())
}

func TestKnownOutputOffsetStopsAtExplicitProjection(t *testing.T) {
	expr := sqlparser.NewColNameWithQualifier("marker", sqlparser.NewTableName("t"))
	projection := &Projection{
		unaryOperator: newUnaryOp(&Union{unionColumns: []sqlparser.SelectExpr{&sqlparser.StarExpr{}}}),
		Columns: AliasedProjections{newProjExpr(&sqlparser.AliasedExpr{
			Expr: expr,
			As:   sqlparser.NewIdentifierCI("x"),
		})},
	}

	offset, ok := knownOutputOffset(&plancontext.PlanningContext{SemTable: semantics.EmptySemTable()}, projection, sqlparser.Clone(expr))
	require.True(t, ok)
	require.Zero(t, offset)
	require.False(t, hasUnresolvedUnionStar(projection))
}
