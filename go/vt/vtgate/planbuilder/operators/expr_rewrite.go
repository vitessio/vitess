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
	"fmt"
	"io"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
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

	// exprCarrier is implemented by every Operator that owns expressions
	// which may need to be rewritten at merge time. fn is invoked once per
	// slot; the slot is replaced with whatever fn returns. Returning nil
	// means "drop this slot"; the engine panics via vterrors.VT13001 if fn
	// returns nil for a kind in nonDeletableSlots.
	exprCarrier interface {
		Operator
		VisitExpressions(fn func(exprKind, sqlparser.Expr) sqlparser.Expr)
	}

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

	// mergeRewriteProgram is the input to applyRoutingRewrite /
	// applyMergeRewrite. AssertMode's zero value (assertOff) means no
	// end-of-build check is performed.
	mergeRewriteProgram struct {
		Rules      []exprRule
		AssertMode assertMode
	}

	// leftoverReport is populated when AssertMode == assertDiagnostic. It
	// describes which Argument names and predicate IDs were declared in a
	// program's affected set yet still appear somewhere in the rewritten
	// subtree. Tests and future PR migration work read this to discover
	// which carriers still owe a VisitExpressions implementation.
	leftoverReport struct {
		// LeftoverArgs maps an Argument.Name to short string descriptions of
		// the operator+kind combinations that still hold it.
		LeftoverArgs map[string][]string
		// LeftoverIDs is reserved for replaceByID rules — populated when an
		// affected predicates.ID still resolves to a non-rewritten Current()
		// in some carrier.
		LeftoverIDs map[predicates.ID][]string
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
	// assertDiagnostic: collect leftovers into a leftoverReport (no slog
	// output, to avoid flake risk in tests that assert on log streams).
	// No production call site uses this mode after PR 5; it stays
	// available for tests and ad-hoc telemetry when developing new rules
	// or carriers.
	assertDiagnostic
	// assertFatal: panic via vterrors.VT13001 on any leftover. The mode
	// every production call site declares.
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
// rewriting before resetRoutingLogic is called. The original call site is the
// SeenPredicates filter inside subqueryRouteMerger.mergeShardedRouting.
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

	applyRulesToRoutingCarrier(ctx, carrier, program)

	routing := carrier.ResetAfterRewrite(ctx)

	// runAssertions on a routing-only call walks no operator tree; the
	// affected sets are checked against the rewritten routing carrier and
	// any post-rewrite Routing returned above (in case ResetAfterRewrite
	// swapped to a different impl that also implements the carrier).
	if program.AssertMode != assertOff {
		report := newLeftoverReport(program)
		collectLeftoversFromRouting(ctx, routing, report)
		finalizeAssertions(program, report)
	}
	return routing
}

// applyMergeRewrite is the tree-walking applier. Walks every reachable
// operator under root (using the existing Visit infrastructure), applies all
// rules to every exprCarrier slot, and applies routing rules to any Route's
// embedded Routing impl that is also a routingExprCarrier — capturing each
// ResetAfterRewrite's potential Routing-impl swap back into the Route.
//
// At end-of-walk the program's AssertMode is honoured (see assertMode).
//
// Conventionally, callers pass the just-merged subtree as root, not the
// global plan root — the engine only needs to reach operators that may
// contain the rule's substitution targets.
func applyMergeRewrite(
	ctx *plancontext.PlanningContext,
	root Operator,
	program *mergeRewriteProgram,
) {
	if program == nil || len(program.Rules) == 0 || root == nil {
		return
	}

	visited := visitTreeForRewrite(ctx, root, program)

	if program.AssertMode == assertOff {
		return
	}
	report := newLeftoverReport(program)
	for _, op := range visited {
		collectLeftoversFromCarrier(ctx, op, report)
		if route, ok := op.(*Route); ok {
			collectLeftoversFromRouting(ctx, route.Routing, report)
		}
	}
	finalizeAssertions(program, report)
}

// visitTreeForRewrite walks every operator reachable from root (Visit, not
// BottomUp — we don't return a transformed tree, we mutate slots in place).
// For each carrier, runs the program's rules on each slot. For each Route
// whose Routing is also a routingExprCarrier, runs the rules on routing
// slots and captures the Routing returned by ResetAfterRewrite back into
// route.Routing — that is how a Routing-impl swap (e.g. ShardedRouting →
// NoneRouting) survives the rewrite.
func visitTreeForRewrite(
	ctx *plancontext.PlanningContext,
	root Operator,
	program *mergeRewriteProgram,
) []Operator {
	visited := make([]Operator, 0, 8)
	if err := Visit(root, func(op Operator) error {
		visited = append(visited, op)
		if carrier, ok := op.(exprCarrier); ok {
			applyRulesToCarrier(ctx, carrier, program)
		}
		if route, ok := op.(*Route); ok {
			if rc, ok := route.Routing.(routingExprCarrier); ok {
				applyRulesToRoutingCarrier(ctx, rc, program)
				route.Routing = rc.ResetAfterRewrite(ctx)
			}
		}
		return nil
	}); err != nil {
		// Visit only returns errors when the visitor returns one; ours
		// never does. If this fires it indicates a programming error.
		panic(vterrors.VT13001(fmt.Sprintf("expr rewrite engine: tree walk failed: %v", err)))
	}
	return visited
}

// applyRulesToCarrier dispatches every rule to every slot of an exprCarrier.
// Rules return the replacement expression; nil means "drop this slot" and
// only succeeds for kinds outside nonDeletableSlots — otherwise we panic.
func applyRulesToCarrier(
	ctx *plancontext.PlanningContext,
	carrier exprCarrier,
	program *mergeRewriteProgram,
) {
	carrier.VisitExpressions(func(kind exprKind, expr sqlparser.Expr) sqlparser.Expr {
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
}

// applyRulesToRoutingCarrier mirrors applyRulesToCarrier for routing-level
// slots. Kept distinct so callers can apply rules to a routing carrier
// before its ResetAfterRewrite — applyRoutingRewrite uses this without
// walking any operator tree.
func applyRulesToRoutingCarrier(
	ctx *plancontext.PlanningContext,
	carrier routingExprCarrier,
	program *mergeRewriteProgram,
) {
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
}

// newLeftoverReport prepares an empty report shaped to the program's affected
// sets. Only argument names from rules with non-empty affectedArgNames are
// pre-keyed, so a report stays small even for large programs.
func newLeftoverReport(program *mergeRewriteProgram) *leftoverReport {
	r := &leftoverReport{
		LeftoverArgs: map[string][]string{},
		LeftoverIDs:  map[predicates.ID][]string{},
	}
	for _, rule := range program.Rules {
		for name := range rule.affectedArgNames() {
			if _, exists := r.LeftoverArgs[name]; !exists {
				r.LeftoverArgs[name] = nil
			}
		}
		for id := range rule.affectedIDs() {
			if _, exists := r.LeftoverIDs[id]; !exists {
				r.LeftoverIDs[id] = nil
			}
		}
	}
	return r
}

// collectLeftoversFromCarrier walks every slot of a carrier and records any
// Argument whose name is in the report's affected set as still present.
// Future PRs add predicates.ID checks for replaceByID rules.
func collectLeftoversFromCarrier(
	_ *plancontext.PlanningContext,
	op Operator,
	report *leftoverReport,
) {
	carrier, ok := op.(exprCarrier)
	if !ok {
		return
	}
	desc := op.ShortDescription()
	carrier.VisitExpressions(func(kind exprKind, expr sqlparser.Expr) sqlparser.Expr {
		recordArgsInExpr(expr, kind, desc, report)
		return expr
	})
}

func collectLeftoversFromRouting(
	_ *plancontext.PlanningContext,
	routing Routing,
	report *leftoverReport,
) {
	rc, ok := routing.(routingExprCarrier)
	if !ok {
		return
	}
	desc := fmt.Sprintf("Routing(%T)", routing)
	rc.VisitRoutingExpressions(func(kind exprKind, expr sqlparser.Expr) sqlparser.Expr {
		recordArgsInExpr(expr, kind, desc, report)
		return expr
	})
}

func recordArgsInExpr(expr sqlparser.Expr, kind exprKind, desc string, report *leftoverReport) {
	if expr == nil || len(report.LeftoverArgs) == 0 {
		return
	}
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		arg, ok := node.(*sqlparser.Argument)
		if !ok {
			return true, nil
		}
		if _, hit := report.LeftoverArgs[arg.Name]; hit {
			report.LeftoverArgs[arg.Name] = append(
				report.LeftoverArgs[arg.Name],
				fmt.Sprintf("%s/%s", desc, kindName(kind)),
			)
		}
		return true, nil
	}, expr)
}

// finalizeAssertions interprets the report under the program's AssertMode.
// In assertDiagnostic the report is dropped on the floor (no slog noise).
// In assertFatal any non-empty leftover panics with a vterrors.VT13001
// naming the leftover Argument(s) and the operator/kind site(s) that still
// hold them.
//
// Scope note: the report only contains leaks found in carrier slots
// (operators implementing exprCarrier or routingExprCarrier). An operator
// that holds an Argument matching the affected set but does not implement a
// carrier is not surfaced here. The assertion's role is to verify that
// every carrier the engine reached was processed correctly, not to
// guarantee the absence of un-substituted Arguments anywhere in the tree.
// The latter guarantee is provided by plantest goldens passing — the
// emitted Query/FieldQuery would diverge if any operator on the path to
// SQL emission held a leftover Argument.
func finalizeAssertions(program *mergeRewriteProgram, report *leftoverReport) {
	switch program.AssertMode {
	case assertOff, assertDiagnostic:
		// Diagnostic-mode reports are inspected by callers that want them
		// (today: tests via the helper added below). Production callers
		// pass assertDiagnostic during PRs 2-4 to exercise the plumbing
		// without enforcement.
		return
	case assertFatal:
		var leftovers []string
		for name, sites := range report.LeftoverArgs {
			if len(sites) == 0 {
				continue
			}
			leftovers = append(leftovers, fmt.Sprintf("Argument %q at %v", name, sites))
		}
		for id, sites := range report.LeftoverIDs {
			if len(sites) == 0 {
				continue
			}
			leftovers = append(leftovers, fmt.Sprintf("predicates.ID %d at %v", id, sites))
		}
		if len(leftovers) > 0 {
			panic(vterrors.VT13001("expr rewrite engine: leftover affected entries: " + fmt.Sprint(leftovers)))
		}
	}
}

// programmerErrorDropOnNonDeletable formats the panic message for fn returning
// nil on a non-deletable slot.
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

// replaceArgByExpr substitutes subquery-placeholder references with the inner
// expression they refer to, once the inner subquery has been merged into the
// outer route. Matches both *sqlparser.Argument{Name} and *sqlparser.ColName
// whose unqualified Name matches a key in ByName — historical reality is that
// subquery placeholders appear in either form depending on what planning
// phase produced them. The ColName match is a hack inherited from the
// pre-engine rewriteMergedSubqueryExpr path (deleted in PR 3) and is
// preserved verbatim here to not regress goldens; revisiting it is out of
// scope.
//
// The replacement is applied in a fixed-point loop because substituting one
// argument can introduce another (nested subqueries: replacing :__sq1 yields
// a Subquery whose body contains :__sq2).
type replaceArgByExpr struct {
	ByName map[string]sqlparser.Expr
}

func (r *replaceArgByExpr) apply(
	_ *plancontext.PlanningContext,
	_ exprKind,
	expr sqlparser.Expr,
) sqlparser.Expr {
	if expr == nil || len(r.ByName) == 0 {
		return expr
	}
	// We use Rewrite (not CopyOnRewrite) because slot-replace semantics
	// don't require preserving the original AST; the caller already accepts
	// our return value as the new slot contents.
	out := expr
	for {
		changed := false
		out = sqlparser.Rewrite(out, nil, func(cursor *sqlparser.Cursor) bool {
			switch n := cursor.Node().(type) {
			case *sqlparser.Argument:
				if repl, hit := r.ByName[n.Name]; hit {
					cursor.Replace(repl)
					changed = true
					return false
				}
			case *sqlparser.ColName:
				if !n.Qualifier.IsEmpty() {
					return true
				}
				if repl, hit := r.ByName[n.Name.String()]; hit {
					cursor.Replace(repl)
					changed = true
					return false
				}
			}
			return true
		}).(sqlparser.Expr)
		if !changed {
			break
		}
	}
	return out
}

func (r *replaceArgByExpr) affectedIDs() map[predicates.ID]struct{} {
	return nil
}

func (r *replaceArgByExpr) affectedArgNames() map[string]struct{} {
	if len(r.ByName) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(r.ByName))
	for name := range r.ByName {
		out[name] = struct{}{}
	}
	return out
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
