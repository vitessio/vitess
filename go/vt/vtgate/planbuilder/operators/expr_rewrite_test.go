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

// runMergeRewriteForTest runs applyMergeRewrite and surfaces its
// leftoverReport. Production callers don't need the report — they pass
// assertFatal and panic on a leftover, or assertOff and skip collection
// entirely. Tests use this helper to assert on which slots a rule reached
// or missed.
func runMergeRewriteForTest(
	ctx *plancontext.PlanningContext,
	root Operator,
	program *mergeRewriteProgram,
) *leftoverReport {
	if program == nil || len(program.Rules) == 0 || root == nil {
		return newLeftoverReport(&mergeRewriteProgram{})
	}
	visited := visitTreeForRewrite(ctx, root, program)
	report := newLeftoverReport(program)
	for _, op := range visited {
		collectLeftoversFromCarrier(ctx, op, report)
		if route, ok := op.(*Route); ok {
			collectLeftoversFromRouting(ctx, route.Routing, report)
		}
	}
	return report
}

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
			expr:     "t.id = :u_id",
			argName:  "u_id",
			deps:     subqueryTables,
			argSet:   matchingArgs,
			wantDrop: true,
		},
		{
			name:     "depends on subquery, has arg but name does NOT match → KEEP",
			expr:     "t.id = :other_arg",
			argName:  "other_arg",
			deps:     subqueryTables,
			argSet:   matchingArgs,
			wantDrop: false,
		},
		{
			name:     "depends on subquery, no Argument at all → KEEP",
			expr:     "t.id = 5",
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
			expr:     "t.id = :u_id",
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
	expr, err := parser.ParseExpr("t.id = :u_id")
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

// TestReplaceArgByExpr covers the rule's substitution semantics in isolation.
// Used at merge time to swap an Argument placeholder for the inner expression
// it referenced.
func TestReplaceArgByExpr(t *testing.T) {
	parser := sqlparser.NewTestParser()
	parseExpr := func(s string) sqlparser.Expr {
		t.Helper()
		expr, err := parser.ParseExpr(s)
		require.NoError(t, err)
		return expr
	}

	t.Run("replaces matching Argument", func(t *testing.T) {
		expr := parseExpr("t.id = :u_id")
		repl := parseExpr("42")
		rule := &replaceArgByExpr{ByName: map[string]sqlparser.Expr{"u_id": repl}}
		out := rule.apply(&plancontext.PlanningContext{}, exprPredicate, expr)
		assert.Equal(t, "t.id = 42", sqlparser.String(out))
	})

	t.Run("non-matching Argument is left alone", func(t *testing.T) {
		expr := parseExpr("t.id = :other")
		repl := parseExpr("42")
		rule := &replaceArgByExpr{ByName: map[string]sqlparser.Expr{"u_id": repl}}
		out := rule.apply(&plancontext.PlanningContext{}, exprPredicate, expr)
		assert.Equal(t, "t.id = :other", sqlparser.String(out))
	})

	t.Run("nil ByName is a no-op", func(t *testing.T) {
		expr := parseExpr("t.id = :u_id")
		rule := &replaceArgByExpr{}
		out := rule.apply(&plancontext.PlanningContext{}, exprPredicate, expr)
		assert.Equal(t, "t.id = :u_id", sqlparser.String(out))
	})

	t.Run("affectedArgNames returns the set of ByName keys", func(t *testing.T) {
		rule := &replaceArgByExpr{ByName: map[string]sqlparser.Expr{
			"a": parseExpr("1"),
			"b": parseExpr("2"),
		}}
		got := rule.affectedArgNames()
		assert.Len(t, got, 2)
		_, hasA := got["a"]
		_, hasB := got["b"]
		assert.True(t, hasA)
		assert.True(t, hasB)
	})

	t.Run("matches unqualified ColName by Name", func(t *testing.T) {
		// Subquery placeholders historically appear as either Argument
		// or ColName; the rule matches both. See subquery_planning.go's
		// rewriteMergedSubqueryExpr for the legacy code path that does
		// the same.
		expr := parseExpr("__sq1 + 1")
		rule := &replaceArgByExpr{ByName: map[string]sqlparser.Expr{
			"__sq1": parseExpr("42"),
		}}
		out := rule.apply(&plancontext.PlanningContext{}, exprPredicate, expr)
		assert.Equal(t, "42 + 1", sqlparser.String(out))
	})

	t.Run("ignores qualified ColName even if Name matches", func(t *testing.T) {
		// `__sq1` as an unqualified bare name matches; `t.__sq1` (a real
		// table-qualified column reference) does not. This protects
		// against unlikely-but-possible name collisions with real column
		// names.
		expr := parseExpr("t.__sq1 + 1")
		rule := &replaceArgByExpr{ByName: map[string]sqlparser.Expr{
			"__sq1": parseExpr("42"),
		}}
		out := rule.apply(&plancontext.PlanningContext{}, exprPredicate, expr)
		assert.Equal(t, "t.__sq1 + 1", sqlparser.String(out))
	})

	t.Run("nested-subquery loop substitutes args introduced by an earlier substitution", func(t *testing.T) {
		// :outer → (1 + :inner), then :inner → 99. The fixed-point loop
		// keeps applying until nothing changes, so the second-tier
		// argument is reached. This mirrors rewriteMergedSubqueryExpr's
		// `for merged` loop.
		expr := parseExpr("col = :outer")
		rule := &replaceArgByExpr{ByName: map[string]sqlparser.Expr{
			"outer": parseExpr("1 + :inner"),
			"inner": parseExpr("99"),
		}}
		out := rule.apply(&plancontext.PlanningContext{}, exprPredicate, expr)
		assert.Equal(t, "col = 1 + 99", sqlparser.String(out))
	})
}

// stubLeafOp is a test-only leaf Operator for building tiny operator trees
// without going through the full planner. Implements just enough of the
// Operator interface to satisfy the tree walker.
type stubLeafOp struct {
	nullaryOperator
	noColumns
	noPredicates
}

func (s *stubLeafOp) Clone(_ []Operator) Operator                          { return s }
func (s *stubLeafOp) SetInputs(_ []Operator)                               {}
func (s *stubLeafOp) ShortDescription() string                             { return "stubLeaf" }
func (s *stubLeafOp) GetOrdering(_ *plancontext.PlanningContext) []OrderBy { return nil }

// TestApplyMergeRewrite_OnFilter verifies the tree-walking applier reaches a
// Filter in the subtree and substitutes Argument refs in its predicate slots.
func TestApplyMergeRewrite_OnFilter(t *testing.T) {
	parser := sqlparser.NewTestParser()
	expr, err := parser.ParseExpr("col = :sq_arg")
	require.NoError(t, err)
	repl, err := parser.ParseExpr("99")
	require.NoError(t, err)

	st := semantics.EmptySemTable()
	st.Recursive[expr] = semantics.SingleTableSet(0)
	ctx := &plancontext.PlanningContext{SemTable: st}

	root := &Filter{
		unaryOperator: newUnaryOp(&stubLeafOp{}),
		Predicates:    []sqlparser.Expr{expr},
	}

	report := runMergeRewriteForTest(ctx, root, &mergeRewriteProgram{
		Rules: []exprRule{
			&replaceArgByExpr{ByName: map[string]sqlparser.Expr{"sq_arg": repl}},
		},
		AssertMode: assertDiagnostic,
	})

	require.Len(t, root.Predicates, 1)
	assert.Equal(t, "col = 99", sqlparser.String(root.Predicates[0]))
	// Argument was substituted, so no leftover.
	assert.Empty(t, report.LeftoverArgs["sq_arg"])
}

// TestApplyMergeRewrite_FatalLeakPanics verifies that when fatal mode is set
// and a carrier holds an Argument matching the program's affected set after
// the rewrite walk, applyMergeRewrite panics via vterrors.VT13001 with a
// message naming the leftover Argument and the operator/kind site. This is
// the assertion that runs under every production call site after PR 5's
// flip from diagnostic to fatal.
func TestApplyMergeRewrite_FatalLeakPanics(t *testing.T) {
	parser := sqlparser.NewTestParser()
	expr, err := parser.ParseExpr("col = :sq_arg")
	require.NoError(t, err)

	st := semantics.EmptySemTable()
	st.Recursive[expr] = semantics.SingleTableSet(0)
	ctx := &plancontext.PlanningContext{SemTable: st}

	root := &Filter{
		unaryOperator: newUnaryOp(&stubLeafOp{}),
		Predicates:    []sqlparser.Expr{expr},
	}

	// A rule whose affectedArgNames includes "sq_arg" but whose apply never
	// substitutes (deleteRoutingPredicatesScopedToSubquery with empty
	// SubqueryTables fails the table-overlap check on every entry).
	rule := &deleteRoutingPredicatesScopedToSubquery{
		SubqueryTables: semantics.EmptyTableSet(),
		JoinColumnArgs: map[string]struct{}{"sq_arg": {}},
	}

	defer func() {
		r := recover()
		require.NotNil(t, r, "expected fatal-mode panic")
		err, ok := r.(error)
		require.True(t, ok, "expected panic value to be an error, got %T", r)
		assert.ErrorContains(t, err, "VT13001")
		assert.ErrorContains(t, err, "leftover affected entries")
		assert.ErrorContains(t, err, `Argument "sq_arg"`)
		assert.ErrorContains(t, err, "exprPredicate")
	}()

	applyMergeRewrite(ctx, root, &mergeRewriteProgram{
		Rules:      []exprRule{rule},
		AssertMode: assertFatal,
	})
	t.Fatal("applyMergeRewrite should have panicked")
}

// TestApplyMergeRewrite_LeakDetected verifies that when a carrier holds an
// Argument that's in the program's affected set but no rule substitutes it,
// the diagnostic-mode report records the leak (no panic). The diagnostic
// surface stays available for tests/implementors that want to inspect
// rewrite gaps without halting planning.
func TestApplyMergeRewrite_LeakDetected(t *testing.T) {
	parser := sqlparser.NewTestParser()
	expr, err := parser.ParseExpr("col = :sq_arg")
	require.NoError(t, err)

	st := semantics.EmptySemTable()
	st.Recursive[expr] = semantics.SingleTableSet(0)
	ctx := &plancontext.PlanningContext{SemTable: st}

	// A Filter holding an Argument no rule will substitute. The rule's
	// affectedArgNames mentions "sq_arg" so the report should surface the
	// leak. Using a deleteRoutingPredicatesScopedToSubquery with empty
	// SubqueryTables — its table-overlap check fails on every entry, so it
	// never drops, but its affectedArgNames is still {"sq_arg"}.
	root := &Filter{
		unaryOperator: newUnaryOp(&stubLeafOp{}),
		Predicates:    []sqlparser.Expr{expr},
	}

	rule := &deleteRoutingPredicatesScopedToSubquery{
		SubqueryTables: semantics.EmptyTableSet(),
		JoinColumnArgs: map[string]struct{}{"sq_arg": {}},
	}

	report := runMergeRewriteForTest(ctx, root, &mergeRewriteProgram{
		Rules:      []exprRule{rule},
		AssertMode: assertDiagnostic,
	})

	// The Argument survived in the Filter — the report should show it as
	// a leftover.
	assert.NotEmpty(t, report.LeftoverArgs["sq_arg"], "expected leak to be recorded")
}

// TestOrderingVisitExpressions verifies the Ordering carrier exposes each
// Order[i].SimplifiedExpr slot under exprOrderBy and propagates the rule's
// rewritten expression back into the slice. Inner.Expr is intentionally not
// exposed (matches what the deleted settleOrderingExpressions did); PR 5's
// strict assertion will surface any gap.
func TestOrderingVisitExpressions(t *testing.T) {
	parser := sqlparser.NewTestParser()
	expr1, err := parser.ParseExpr("col + :sq_arg")
	require.NoError(t, err)
	expr2, err := parser.ParseExpr("col2")
	require.NoError(t, err)
	repl, err := parser.ParseExpr("99")
	require.NoError(t, err)

	innerOrder := &sqlparser.Order{Expr: expr1, Direction: sqlparser.AscOrder}
	o := &Ordering{
		unaryOperator: newUnaryOp(&stubLeafOp{}),
		Order: []OrderBy{
			{Inner: innerOrder, SimplifiedExpr: expr1},
			{Inner: &sqlparser.Order{Expr: expr2}, SimplifiedExpr: expr2},
		},
	}

	visited := 0
	o.VisitExpressions(func(kind exprKind, e sqlparser.Expr) sqlparser.Expr {
		assert.Equal(t, exprOrderBy, kind)
		visited++
		// substitute :sq_arg in the first slot, leave the second alone
		if visited == 1 {
			return repl
		}
		return e
	})
	assert.Equal(t, 2, visited, "every Order entry should be visited")
	assert.Same(t, repl, o.Order[0].SimplifiedExpr,
		"rule's return value should land in SimplifiedExpr")
	// Inner.Expr is intentionally not updated by the carrier.
	assert.Equal(t, "col + :sq_arg", sqlparser.String(o.Order[0].Inner.Expr))
}

// TestOrderingVisitExpressions_DropPanics verifies drop semantics aren't
// allowed on ORDER BY slots — every entry must keep an expression.
func TestOrderingVisitExpressions_DropPanics(t *testing.T) {
	parser := sqlparser.NewTestParser()
	expr, err := parser.ParseExpr("col")
	require.NoError(t, err)

	o := &Ordering{
		unaryOperator: newUnaryOp(&stubLeafOp{}),
		Order: []OrderBy{
			{Inner: &sqlparser.Order{Expr: expr}, SimplifiedExpr: expr},
		},
	}

	assert.Panics(t, func() {
		o.VisitExpressions(func(_ exprKind, _ sqlparser.Expr) sqlparser.Expr {
			return nil
		})
	})
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
		{"t.id = :u_id", []string{"u_id"}, true},
		{"t.id = :u_id", []string{"other"}, false},
		{"t.id = 5", []string{"u_id"}, false},
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
