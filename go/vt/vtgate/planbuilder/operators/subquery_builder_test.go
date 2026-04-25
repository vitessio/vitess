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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// TestReplaceSubqueryNode_ExistsPlaceholderUntyped verifies that
// replaceSubqueryNode mints an untyped *Argument for PulloutExists pullouts,
// regardless of the inner subquery's first SELECT column type.
//
// EXISTS evaluates to a 0/1 flag at runtime; inheriting the inner column's
// type would be wrong — for non-INT types it leads to incorrect planning
// decisions (NeedsWeightString misfiring) and Argument.Format emitting
// invalid CASTs (CAST(:arg AS DECIMAL(...)) etc. for what's a 0/1 integer).
//
// Plantest doesn't currently surface this because rewriteSubqueryArgsForPullout
// later replaces the placeholder with NewArgument(HasValuesName) before SQL
// is emitted, so Argument.Format never sees the buggy type. This unit test
// asserts the property directly at the mint site.
//
// Caught by GitHub Copilot in PR review (#19963).
func TestReplaceSubqueryNode_ExistsPlaceholderUntyped(t *testing.T) {
	cases := []struct {
		name      string
		innerType sqltypes.Type
	}{
		{"Decimal inner", sqltypes.Decimal},
		{"VarChar inner", sqltypes.VarChar},
		{"Datetime inner", sqltypes.Datetime},
		{"Time inner", sqltypes.Time},
		{"Float64 inner", sqltypes.Float64},
		{"Uint64 inner", sqltypes.Uint64},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			arg := mintExistsPlaceholder(t, c.innerType)
			assert.Equal(t, sqltypes.Unknown, arg.Type,
				"PulloutExists placeholder must be untyped, but got %v from inner %v",
				arg.Type, c.innerType)
			assert.Equal(t, "__sq1", arg.Name)
		})
	}
}

// TestReplaceSubqueryNode_ValuePlaceholderTyped verifies that the typed-Argument
// path still kicks in for PulloutValue (scalar subqueries). This guards against
// an over-eager fix accidentally widening the EXISTS untyped path to other
// pullout kinds.
func TestReplaceSubqueryNode_ValuePlaceholderTyped(t *testing.T) {
	parser := sqlparser.NewTestParser()
	colExpr, err := parser.ParseExpr("col")
	require.NoError(t, err)

	// Inner subquery: SELECT col FROM dual, with col typed as Decimal.
	inner := &sqlparser.Select{
		SelectExprs: &sqlparser.SelectExprs{
			Exprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: colExpr}},
		},
		From: []sqlparser.TableExpr{
			&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewIdentifierCS("dual")}},
		},
	}
	sq := &sqlparser.Subquery{Select: inner}

	st := semantics.EmptySemTable()
	st.ExprTypes[colExpr] = evalengine.NewType(sqltypes.Decimal, collations.Unknown)
	ctx := &plancontext.PlanningContext{SemTable: st}

	// Run Rewrite with sq as the root so cursor.Replace lands somewhere
	// observable. We use a wrapper Cmp so the Subquery has a parent slot
	// the cursor can write into.
	wrapper := &sqlparser.ComparisonExpr{Operator: sqlparser.EqualOp, Left: sqlparser.NewIntLiteral("1"), Right: sq}
	sqb := &SubQueryBuilder{}
	result := sqlparser.Rewrite(wrapper, func(c *sqlparser.Cursor) bool {
		if _, ok := c.Node().(*sqlparser.Subquery); ok {
			sqb.replaceSubqueryNode(ctx, c, sq, "__sq1", opcode.PulloutValue, false)
			return false
		}
		return true
	}, nil)

	cmp := result.(*sqlparser.ComparisonExpr)
	arg, ok := cmp.Right.(*sqlparser.Argument)
	require.True(t, ok, "expected *Argument replacement for PulloutValue, got %T", cmp.Right)
	assert.Equal(t, sqltypes.Decimal, arg.Type,
		"PulloutValue placeholder should inherit the inner column's type")
}

// mintExistsPlaceholder constructs a minimal Subquery whose first SELECT
// column has the given type, runs replaceSubqueryNode with PulloutExists,
// and returns the resulting Argument.
//
// The scaffolding mirrors what pullOutValueSubqueries does in production:
// the cursor sits on the ExistsExpr (not the inner Subquery), and
// cursor.Replace substitutes the ExistsExpr with the *Argument. We wrap
// the ExistsExpr in a ComparisonExpr so the cursor's parent slot is an
// Expr (which can hold either *ExistsExpr or *Argument).
func mintExistsPlaceholder(t *testing.T, innerType sqltypes.Type) *sqlparser.Argument {
	t.Helper()
	parser := sqlparser.NewTestParser()
	colExpr, err := parser.ParseExpr("col")
	require.NoError(t, err)

	inner := &sqlparser.Select{
		SelectExprs: &sqlparser.SelectExprs{
			Exprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: colExpr}},
		},
		From: []sqlparser.TableExpr{
			&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewIdentifierCS("dual")}},
		},
	}
	sq := &sqlparser.Subquery{Select: inner}

	st := semantics.EmptySemTable()
	st.ExprTypes[colExpr] = evalengine.NewType(innerType, collations.Unknown)
	ctx := &plancontext.PlanningContext{SemTable: st}

	// Wrap ExistsExpr as the right side of a comparison so it sits in an
	// Expr slot the cursor can rewrite into an *Argument.
	wrapper := &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     sqlparser.NewIntLiteral("1"),
		Right:    &sqlparser.ExistsExpr{Subquery: sq},
	}
	sqb := &SubQueryBuilder{}
	result := sqlparser.Rewrite(wrapper, func(c *sqlparser.Cursor) bool {
		if exists, ok := c.Node().(*sqlparser.ExistsExpr); ok {
			sqb.replaceSubqueryNode(ctx, c, exists.Subquery, "__sq1", opcode.PulloutExists, false)
			return false
		}
		return true
	}, nil)

	cmp, ok := result.(*sqlparser.ComparisonExpr)
	require.True(t, ok)
	arg, ok := cmp.Right.(*sqlparser.Argument)
	require.True(t, ok, "expected *Argument replacement, got %T", cmp.Right)
	return arg
}
