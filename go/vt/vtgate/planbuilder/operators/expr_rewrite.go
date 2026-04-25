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
	"io"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/predicates"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// exprKind tags the kind of expression slot an exprCarrier is exposing.
	// The kind is used by rules to filter slots they care about, and by the
	// nonDeletableSlots registry to reject drops on slots that can't be
	// removed structurally.
	exprKind uint8

	// routingExprCarrier is implemented by Routing impls that own predicate
	// slots (today: ShardedRouting.SeenPredicates). ResetAfterRewrite returns
	// the replacement Routing because resetRoutingLogic can return a
	// different concrete type than the input — for instance UpdateRoutingLogic
	// returns *NoneRouting when a predicate evaluates to false.
	routingExprCarrier interface {
		VisitRoutingExpressions(fn func(exprKind, sqlparser.Expr) sqlparser.Expr)
		ResetAfterRewrite(ctx *plancontext.PlanningContext) Routing
	}

	// exprRule is the closed rule vocabulary applied by the merge-rewrite
	// engine. Every rule contributes:
	//   - apply, the per-slot transformation
	//   - affectedIDs / affectedArgNames, telling the assertion which
	//     predicates.IDs and *sqlparser.Argument names should not survive
	//     anywhere in the rewritten subtree
	// apply takes ctx because rules need ctx.SemTable.RecursiveDeps for
	// table-scope checks, and future rules will use ctx for error formatting
	// and semantic queries.
	exprRule interface {
		apply(ctx *plancontext.PlanningContext, kind exprKind, expr sqlparser.Expr) sqlparser.Expr
		affectedIDs() map[predicates.ID]struct{}
		affectedArgNames() map[string]struct{}
	}

	// assertMode controls how affected-set leftovers are reported.
	// Zero value is assertOff, always. There is no package-level "global
	// default" — every call site declares its mode explicitly.
	assertMode uint8

	// mergeRewriteProgram is the input to applyRoutingRewrite. AssertMode's
	// zero value (assertOff) means no end-of-build check is performed. PR 2
	// extends this with a leftoverReport surface and the tree-walking
	// applyMergeRewrite entry point.
	mergeRewriteProgram struct {
		Rules      []exprRule
		AssertMode assertMode
	}
)

const (
	exprPredicate exprKind = iota
	exprProjection
	exprOrderBy
	exprGroupBy
	exprHaving
	exprLimitCount
	exprLimitOffset
	exprAggregate
	exprSetExpr
	exprJoinPredicate
	exprRoutingPredicate
)

const (
	// assertOff: no end-of-build check, no telemetry. Zero value. Programs
	// that don't opt in pay no walk-cost.
	assertOff assertMode = iota
	// assertDiagnostic: collect leftovers into a leftoverReport on the
	// PlanningContext. Used during PRs 2-4 while ctx.MergedSubqueries is
	// still acting as the safety net for un-migrated operators. No slog
	// output, to avoid flake risk in tests that assert on log streams.
	assertDiagnostic
	// assertFatal: panic via vterrors.VT13001 on any leftover. PR 5
	// replaces every assertDiagnostic with assertFatal in one sweep.
	assertFatal
)

// nonDeletableSlots lists the kinds whose slot cannot be structurally removed
// — fn returning nil for these kinds is a programmer error and the engine
// will panic. Predicate slots can be dropped (Filter with no predicates is
// trivially removable); projection / aggregate / group-by / set-expr slots
// cannot — the operator's shape depends on them being present.
var nonDeletableSlots = map[exprKind]bool{
	exprProjection: true,
	exprAggregate:  true,
	exprGroupBy:    true,
	exprSetExpr:    true,
}

// applyRoutingRewrite is the routing-level applier. Used at merge sites where
// the operator tree isn't yet finalised but the routing's SeenPredicates need
// rewriting before resetRoutingLogic is called. Today's only call site is the
// SeenPredicates filter inside subqueryRouteMerger.mergeShardedRouting; future
// PRs introduce applyMergeRewrite for tree-wide rewriting.
//
// Returns the replacement Routing, since ResetAfterRewrite (and underneath it
// resetRoutingLogic) may produce a Routing of a different concrete type than
// the input carrier — for example *NoneRouting when a predicate is known
// false.
func applyRoutingRewrite(
	ctx *plancontext.PlanningContext,
	carrier routingExprCarrier,
	program *mergeRewriteProgram,
) Routing {
	if program == nil || len(program.Rules) == 0 {
		return carrier.ResetAfterRewrite(ctx)
	}

	carrier.VisitRoutingExpressions(func(kind exprKind, expr sqlparser.Expr) sqlparser.Expr {
		out := expr
		for _, rule := range program.Rules {
			out = rule.apply(ctx, kind, out)
			if out == nil {
				if nonDeletableSlots[kind] {
					panic(programmerErrorDropOnNonDeletable(kind))
				}
				return nil
			}
		}
		return out
	})

	routing := carrier.ResetAfterRewrite(ctx)
	runAssertions(ctx, []Operator{}, routing, program)
	return routing
}

// runAssertions implements the AssertMode contract. PR 1 only carries
// routing-level rules so the operator-tree walk is empty; PR 2 extends it.
// In assertOff mode this is a no-op; in assertDiagnostic mode it populates
// ctx.RewriteReport; in assertFatal mode it panics on any leftover.
func runAssertions(
	ctx *plancontext.PlanningContext,
	tree []Operator,
	routing Routing,
	program *mergeRewriteProgram,
) {
	if program.AssertMode == assertOff {
		return
	}
	// PR 1: with no exprCarrier walk, the only post-rewrite surface to
	// inspect is the routing carrier itself. PR 2 extends this to walk every
	// reachable carrier in tree and collect leftovers across operator slots.
	_ = ctx
	_ = tree
	_ = routing
}

// programmerErrorDropOnNonDeletable formats the panic message for fn returning
// nil on a non-deletable slot. Kept minimal pending PR 5's full assertion
// message format.
func programmerErrorDropOnNonDeletable(kind exprKind) string {
	return "expr rewrite engine: rule returned nil for non-deletable slot kind " + kindName(kind)
}

func kindName(kind exprKind) string {
	switch kind {
	case exprPredicate:
		return "exprPredicate"
	case exprProjection:
		return "exprProjection"
	case exprOrderBy:
		return "exprOrderBy"
	case exprGroupBy:
		return "exprGroupBy"
	case exprHaving:
		return "exprHaving"
	case exprLimitCount:
		return "exprLimitCount"
	case exprLimitOffset:
		return "exprLimitOffset"
	case exprAggregate:
		return "exprAggregate"
	case exprSetExpr:
		return "exprSetExpr"
	case exprJoinPredicate:
		return "exprJoinPredicate"
	case exprRoutingPredicate:
		return "exprRoutingPredicate"
	}
	return "exprKind(unknown)"
}

// deleteRoutingPredicatesScopedToSubquery is the engine-side equivalent of the
// open-coded slice.Filter that today's subqueryRouteMerger.mergeShardedRouting
// uses to drop SeenPredicates whose argument refs are no longer produced after
// the inner subquery merges into the outer route.
//
// The drop condition is two-part. A SeenPredicates entry is dropped IFF:
//
//  1. ctx.SemTable.RecursiveDeps(expr).IsOverlapping(SubqueryTables) — the
//     predicate depends on at least one of the inner subquery's tables, AND
//  2. expr contains an *sqlparser.Argument whose Name is in JoinColumnArgs —
//     it references a bind variable produced by one of the join columns that
//     no longer has a producer after the merge.
//
// Either condition false → keep the predicate. This matches today's behaviour
// exactly (predicates that don't depend on subquery tables stay regardless of
// args; predicates that depend on subquery tables but contain no matching arg
// also stay — e.g. "user.id = 5" inside an inner subquery).
type deleteRoutingPredicatesScopedToSubquery struct {
	SubqueryTables semantics.TableSet
	JoinColumnArgs map[string]struct{}
}

func (r *deleteRoutingPredicatesScopedToSubquery) apply(
	ctx *plancontext.PlanningContext,
	kind exprKind,
	expr sqlparser.Expr,
) sqlparser.Expr {
	if kind != exprRoutingPredicate {
		return expr
	}
	if expr == nil {
		return nil
	}
	if !ctx.SemTable.RecursiveDeps(expr).IsOverlapping(r.SubqueryTables) {
		return expr
	}
	if !exprMentionsAnyArg(expr, r.JoinColumnArgs) {
		return expr
	}
	return nil
}

func (r *deleteRoutingPredicatesScopedToSubquery) affectedIDs() map[predicates.ID]struct{} {
	return nil
}

func (r *deleteRoutingPredicatesScopedToSubquery) affectedArgNames() map[string]struct{} {
	return r.JoinColumnArgs
}

// exprMentionsAnyArg reports whether expr contains any *sqlparser.Argument
// whose Name is a key in args. Linear in the size of expr; bails out on the
// first match.
func exprMentionsAnyArg(expr sqlparser.Expr, args map[string]struct{}) bool {
	if len(args) == 0 {
		return false
	}
	var found bool
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		arg, ok := node.(*sqlparser.Argument)
		if !ok {
			return true, nil
		}
		if _, hit := args[arg.Name]; hit {
			found = true
			// io.EOF is the sentinel today's open-coded walker uses
			// (subquery_planning.go:630) to short-circuit after a hit.
			return false, io.EOF
		}
		return true, nil
	}, expr)
	return found
}
