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

// TestDeleteRoutingPredicatesScopedToSubquery exercises the two-part drop
// condition that today lives as an open-coded slice.Filter in
// subqueryRouteMerger.mergeShardedRouting (subquery_planning.go:603-637 prior
// to this PR). The condition is: drop a SeenPredicates entry IFF
//
//	(1) RecursiveDeps(expr) overlaps SubqueryTables, AND
//	(2) expr contains an *Argument whose name is in JoinColumnArgs.
//
// Either condition false → keep.
func TestDeleteRoutingPredicatesScopedToSubquery(t *testing.T) {
	parser := sqlparser.NewTestParser()
	subqueryTables := semantics.SingleTableSet(0)
	outerTables := semantics.SingleTableSet(1)

	parseExpr := func(s string) sqlparser.Expr {
		t.Helper()
		expr, err := parser.ParseExpr(s)
		require.NoError(t, err)
		return expr
	}

	type tc struct {
		name    string
		expr    string
		argName string // pretend the predicate references this *Argument
		// deps is which TableSet the predicate's RecursiveDeps should report
		deps     semantics.TableSet
		argSet   map[string]struct{}
		wantDrop bool
	}

	matchingArgs := map[string]struct{}{"u_id": {}}

	cases := []tc{
		{
			name:     "depends on subquery AND has matching arg → DROP",
			expr:     "user.id = :u_id",
			argName:  "u_id",
			deps:     subqueryTables,
			argSet:   matchingArgs,
			wantDrop: true,
		},
		{
			name:     "depends on subquery, has arg but name does NOT match → KEEP",
			expr:     "user.id = :other_arg",
			argName:  "other_arg",
			deps:     subqueryTables,
			argSet:   matchingArgs,
			wantDrop: false,
		},
		{
			name:     "depends on subquery, no Argument at all → KEEP",
			expr:     "user.id = 5",
			argName:  "",
			deps:     subqueryTables,
			argSet:   matchingArgs,
			wantDrop: false,
		},
		{
			name:     "does NOT depend on subquery, has matching arg → KEEP",
			expr:     "o.col = :u_id",
			argName:  "u_id",
			deps:     outerTables,
			argSet:   matchingArgs,
			wantDrop: false,
		},
		{
			name:     "does NOT depend on subquery, no arg → KEEP",
			expr:     "o.col = 5",
			argName:  "",
			deps:     outerTables,
			argSet:   matchingArgs,
			wantDrop: false,
		},
		{
			name:     "empty arg set → KEEP regardless of subquery deps",
			expr:     "user.id = :u_id",
			argName:  "u_id",
			deps:     subqueryTables,
			argSet:   map[string]struct{}{},
			wantDrop: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expr := parseExpr(c.expr)

			st := semantics.EmptySemTable()
			// register the predicate's recursive dependency
			st.Recursive[expr] = c.deps

			ctx := &plancontext.PlanningContext{SemTable: st}

			rule := &deleteRoutingPredicatesScopedToSubquery{
				SubqueryTables: subqueryTables,
				JoinColumnArgs: c.argSet,
			}

			got := rule.apply(ctx, exprRoutingPredicate, expr)
			if c.wantDrop {
				assert.Nil(t, got, "expected predicate to be dropped")
			} else {
				assert.NotNil(t, got, "expected predicate to be kept")
				assert.Equal(t, sqlparser.String(expr), sqlparser.String(got))
			}
		})
	}
}

// TestDeleteRoutingPredicatesScopedToSubquery_NonRoutingKindIgnored verifies
// the rule short-circuits for kinds it doesn't own. Future PRs will run the
// same rule list against other carriers; the rule must pass-through any kind
// it doesn't recognise.
func TestDeleteRoutingPredicatesScopedToSubquery_NonRoutingKindIgnored(t *testing.T) {
	parser := sqlparser.NewTestParser()
	expr, err := parser.ParseExpr("user.id = :u_id")
	require.NoError(t, err)

	st := semantics.EmptySemTable()
	st.Recursive[expr] = semantics.SingleTableSet(0)
	ctx := &plancontext.PlanningContext{SemTable: st}

	rule := &deleteRoutingPredicatesScopedToSubquery{
		SubqueryTables: semantics.SingleTableSet(0),
		JoinColumnArgs: map[string]struct{}{"u_id": {}},
	}

	// kind != exprRoutingPredicate → pass through, do not drop.
	got := rule.apply(ctx, exprPredicate, expr)
	assert.NotNil(t, got)
	got = rule.apply(ctx, exprProjection, expr)
	assert.NotNil(t, got)
}

// TestExprMentionsAnyArg covers the small helper directly. The Walk
// short-circuits via io.EOF on the first match.
func TestExprMentionsAnyArg(t *testing.T) {
	parser := sqlparser.NewTestParser()
	cases := []struct {
		expr string
		args []string
		want bool
	}{
		{"user.id = :u_id", []string{"u_id"}, true},
		{"user.id = :u_id", []string{"other"}, false},
		{"user.id = 5", []string{"u_id"}, false},
		{"x = :a OR y = :b", []string{"b"}, true},
		{"x = :a OR y = :b", []string{}, false},
	}
	for _, c := range cases {
		t.Run(c.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(c.expr)
			require.NoError(t, err)
			set := map[string]struct{}{}
			for _, a := range c.args {
				set[a] = struct{}{}
			}
			assert.Equal(t, c.want, exprMentionsAnyArg(expr, set))
		})
	}
}
