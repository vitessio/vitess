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

	qualifiedAlias := sqlparser.NewColNameWithQualifier("x", sqlparser.NewTableName("t"))
	_, ok = knownOutputOffset(&plancontext.PlanningContext{SemTable: semantics.EmptySemTable()}, projection, qualifiedAlias)
	require.False(t, ok)
}

// Before an arm's shard-local limit is planned, its VTGate LIMIT alone must
// stop an outer upper limit from changing the route beneath it.
func TestUpperLimitStopsAtArmLimit(t *testing.T) {
	source := &Table{}
	route := &Route{unaryOperator: newUnaryOp(source)}
	arm := newLimit(route, &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral("2")}, false)
	outer := newLimit(arm, &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral("3")}, false)
	ctx := &plancontext.PlanningContext{SemTable: semantics.EmptySemTable()}
	setUpperLimit(ctx, outer)
	require.Same(t, source, route.Source, "an outer LIMIT must not cross the arm's own LIMIT")
}
