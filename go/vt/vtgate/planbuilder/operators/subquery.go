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
	"fmt"
	"maps"
	"slices"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// SubQuery represents a subquery used for filtering rows in an
// outer query through a join.
type SubQuery struct {
	// Fields filled in at the time of construction:
	Outer             Operator             // Outer query operator.
	Subquery          Operator             // Subquery operator.
	FilterType        opcode.PulloutOpcode // Type of subquery filter.
	Original          sqlparser.Expr       // This is the expression we should use if we can merge the inner to the outer
	originalSubquery  *sqlparser.Subquery  // Subquery representation, e.g., (SELECT foo from user LIMIT 1).
	Predicates        []sqlparser.Expr     // Predicates joining outer and inner queries. Empty for uncorrelated subqueries.
	OuterPredicate    sqlparser.Expr       // This is the predicate that is using the subquery expression. It will not be empty for projections
	ArgName           string               // This is the name of the ColName or Argument used to replace the subquery
	TopLevel          bool                 // will be false if the subquery is deeply nested
	JoinColumns       []applyJoinColumn    // Broken up join predicates.
	SubqueryValueName string               // Value name returned by the subquery (uncorrelated queries).
	HasValuesName     string               // Argument name passed to the subquery (uncorrelated queries).

	// Fields related to correlated subqueries:
	Vars    map[string]int // Arguments copied from outer to inner, set during offset planning.
	outerID semantics.TableSet
	// correlated stores whether this subquery is correlated or not.
	// We use this information to fail the planning if we are unable to merge the subquery with a route.
	correlated bool

	// IsArgument is set to true if the subquery puts the
	IsArgument bool

	// path locates this subquery's owning *sqlparser.Subquery node within the
	// shared predicate tree. Used by group-aware merge / settle to identify
	// which Subquery node within group.Original belongs to this member.
	path sqlparser.ASTPath

	// group, when non-nil, links this SubQuery with sibling SubQueries that
	// were extracted from the same predicate (e.g. multiple EXISTS / IN
	// subqueries inside one OR predicate). All members in a group coordinate
	// through the shared subqueryGroup so the predicate is emitted exactly
	// once at settle and merges don't bake duplicates onto outer.Source.
	group *subqueryGroup
}

// subqueryGroup owns the shared, mutable state for a set of SubQuery operators
// extracted from the same WHERE/ON predicate. Members read and write the
// predicate through this struct so that:
//
//   - merges can inline a member's subquery Select into the shared predicate
//     without baking duplicate filters onto outer.Source, and
//   - settle emits the predicate exactly once, with bind-var substitutions
//     applied for any members that did not merge.
type subqueryGroup struct {
	// Original is the shared, mutable predicate. Merges replace the inner
	// Select of an owning Subquery node within Original via path lookup.
	Original sqlparser.Expr

	// Members lists all SubQuery operators extracted from this predicate, in
	// DFS discovery order. Used at settle time to enumerate which Subquery
	// nodes need substitution and to elect a leader.
	Members []*SubQuery

	// merged tracks which members have been merged into the outer Route. The
	// merged member's subquery Select has been inlined into Original and its
	// SubQuery operator has been removed from the SubQueryContainer; settle
	// must skip these when applying bind-var substitutions.
	merged map[*SubQuery]bool

	// emitted is set once the group's predicate has been added as a filter on
	// the outer source (either by pushOrMergeSubQueryContainer when the whole
	// group merged, or by settleFilterInGroup's leader). Subsequent attempts
	// to emit are no-ops.
	emitted bool
}

func (sq *SubQuery) planOffsets(ctx *plancontext.PlanningContext) Operator {
	sq.Vars = make(map[string]int)
	columns, err := sq.GetJoinColumns(ctx, sq.Outer)
	if err != nil {
		panic(err)
	}
	for _, jc := range columns {
		for _, lhsExpr := range jc.LHSExprs {
			offset := sq.Outer.AddColumn(ctx, true, false, aeWrap(lhsExpr.Expr))
			sq.Vars[lhsExpr.Name] = offset
		}
	}
	return nil
}

func (sq *SubQuery) OuterExpressionsNeeded(ctx *plancontext.PlanningContext, outer Operator) (result []*sqlparser.ColName) {
	joinColumns, err := sq.GetJoinColumns(ctx, outer)
	if err != nil {
		return nil
	}
	for _, jc := range joinColumns {
		for _, lhsExpr := range jc.LHSExprs {
			col, ok := lhsExpr.Expr.(*sqlparser.ColName)
			if !ok {
				panic(vterrors.VT13001("joins can only compare columns: %s", sqlparser.String(lhsExpr.Expr)))
			}
			result = append(result, col)
		}
	}
	return result
}

func (sq *SubQuery) GetJoinColumns(ctx *plancontext.PlanningContext, outer Operator) ([]applyJoinColumn, error) {
	if outer == nil {
		return nil, vterrors.VT13001("outer operator cannot be nil")
	}
	outerID := TableID(outer)
	if sq.JoinColumns != nil {
		if sq.outerID == outerID {
			return sq.JoinColumns, nil
		}
	}
	sq.outerID = outerID
	mapper := func(in sqlparser.Expr) (applyJoinColumn, error) {
		return breakExpressionInLHSandRHS(ctx, in, outerID), nil
	}
	joinPredicates, err := slice.MapWithError(sq.Predicates, mapper)
	if err != nil {
		return nil, err
	}
	sq.JoinColumns = joinPredicates
	return sq.JoinColumns, nil
}

// Clone implements the Operator interface
func (sq *SubQuery) Clone(inputs []Operator) Operator {
	klone := *sq
	switch len(inputs) {
	case 1:
		klone.Subquery = inputs[0]
	case 2:
		klone.Outer = inputs[0]
		klone.Subquery = inputs[1]
	default:
		panic("wrong number of inputs")
	}
	klone.JoinColumns = slices.Clone(sq.JoinColumns)
	klone.Vars = maps.Clone(sq.Vars)
	klone.Predicates = slices.Clone(sq.Predicates)
	return &klone
}

func (sq *SubQuery) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return sq.Outer.GetOrdering(ctx)
}

// Inputs implements the Operator interface
func (sq *SubQuery) Inputs() []Operator {
	if sq.Outer == nil {
		return []Operator{sq.Subquery}
	}

	return []Operator{sq.Outer, sq.Subquery}
}

// SetInputs implements the Operator interface
func (sq *SubQuery) SetInputs(inputs []Operator) {
	switch len(inputs) {
	case 1:
		sq.Subquery = inputs[0]
	case 2:
		sq.Outer = inputs[0]
		sq.Subquery = inputs[1]
	default:
		panic("wrong number of inputs")
	}
}

func (sq *SubQuery) ShortDescription() string {
	var typ string
	if sq.IsArgument {
		typ = "ARGUMENT"
	} else {
		typ = "FILTER"
	}
	var pred string

	if len(sq.Predicates) > 0 || sq.OuterPredicate != nil {
		preds := append(sq.Predicates, sq.OuterPredicate)
		pred = " MERGE ON " + sqlparser.String(sqlparser.AndExpressions(preds...))
	}
	return fmt.Sprintf(":%s %s %v%s", sq.ArgName, typ, sq.FilterType.String(), pred)
}

func (sq *SubQuery) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	sq.Outer = sq.Outer.AddPredicate(ctx, expr)
	return sq
}

func (sq *SubQuery) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, ae *sqlparser.AliasedExpr) int {
	ae = sqlparser.Clone(ae)
	// we need to rewrite the column name to an argument if it's the same as the subquery column name
	ae.Expr = rewriteColNameToArgument(ctx, ae.Expr, []*SubQuery{sq}, sq)
	return sq.Outer.AddColumn(ctx, reuseExisting, addToGroupBy, ae)
}

func (sq *SubQuery) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return sq.Outer.AddWSColumn(ctx, offset, underRoute)
}

func (sq *SubQuery) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return sq.Outer.FindCol(ctx, expr, underRoute)
}

func (sq *SubQuery) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return sq.Outer.GetColumns(ctx)
}

func (sq *SubQuery) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return sq.Outer.GetSelectExprs(ctx)
}

// GetMergePredicates returns the predicates that we can use to try to merge this subquery with the outer query.
func (sq *SubQuery) GetMergePredicates() []sqlparser.Expr {
	if sq.OuterPredicate != nil {
		return append(sq.Predicates, sq.OuterPredicate)
	}
	return sq.Predicates
}

func (sq *SubQuery) settle(ctx *plancontext.PlanningContext, outer Operator) Operator {
	// We can allow uncorrelated queries even when subquery isn't the top level construct,
	// like if its underneath an Aggregator, because they will be pulled out and run separately.
	if !sq.TopLevel && sq.correlated {
		panic(subqueryNotAtTopErr)
	}
	if sq.correlated && sq.FilterType != opcode.PulloutExists {
		panic(correlatedSubqueryErr)
	}
	if sq.IsArgument {
		if len(sq.GetMergePredicates()) > 0 {
			// this means that we have a correlated subquery on our hands
			panic(correlatedSubqueryErr)
		}
		sq.SubqueryValueName = sq.ArgName
		return outer
	}
	return sq.settleFilter(ctx, outer)
}

var (
	correlatedSubqueryErr = vterrors.VT12001("correlated subquery is only supported for EXISTS")
	subqueryNotAtTopErr   = vterrors.VT12001("unmergable subquery can not be inside complex expression")
)

func (sq *SubQuery) addLimit() {
	// for a correlated subquery, we can add a limit 1 to the subquery
	sq.Subquery = newLimit(sq.Subquery, &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral("1")}, true)
}

func (sq *SubQuery) settleFilter(ctx *plancontext.PlanningContext, outer Operator) Operator {
	if len(sq.Predicates) > 0 {
		if sq.FilterType != opcode.PulloutExists {
			panic(correlatedSubqueryErr)
		}
		sq.addLimit()
		return outer
	}

	if sq.group != nil {
		return sq.settleFilterInGroup(ctx, outer)
	}
	return sq.settleFilterSingle(ctx, outer)
}

// settleFilterSingle handles the common case of a single SubQuery owning its
// entire WHERE predicate: walk Original, substitute the owning Subquery /
// ExistsExpr / ComparisonExpr with bind-var placeholders, and emit a Filter.
func (sq *SubQuery) settleFilterSingle(ctx *plancontext.PlanningContext, outer Operator) Operator {
	hasValuesArg := func() string {
		s := ctx.ReservedVars.ReserveVariable(string(sqlparser.HasValueSubQueryBaseName))
		sq.HasValuesName = s
		return s
	}
	post := func(cursor *sqlparser.CopyOnWriteCursor) {
		node := cursor.Node()
		// For IN and NOT IN type filters, we have to add a Expression that checks if we got any rows back or not
		// for correctness. That expression should be ANDed with the expression that has the IN/NOT IN comparison.
		if compExpr, isCompExpr := node.(*sqlparser.ComparisonExpr); sq.FilterType.NeedsListArg() && isCompExpr {
			if listArg, isListArg := compExpr.Right.(sqlparser.ListArg); isListArg && listArg.String() == sq.ArgName {
				if sq.FilterType == opcode.PulloutIn {
					cursor.Replace(sqlparser.AndExpressions(sqlparser.NewArgument(hasValuesArg()), compExpr))
				} else {
					cursor.Replace(&sqlparser.OrExpr{
						Left:  sqlparser.NewNotExpr(sqlparser.NewArgument(hasValuesArg())),
						Right: compExpr,
					})
				}
			}
		}
		// For EXISTS / NOT EXISTS, the meaningful substitution is replacing the whole
		// ExistsExpr with the has-values argument. This preserves any surrounding
		// expression context (OR, AND, etc.) in rhsPred.
		if _, isExists := node.(*sqlparser.ExistsExpr); isExists &&
			(sq.FilterType == opcode.PulloutExists || sq.FilterType == opcode.PulloutNotExists) {
			cursor.Replace(sqlparser.NewArgument(hasValuesArg()))
			return
		}
		if _, ok := node.(*sqlparser.Subquery); !ok {
			return
		}

		var arg sqlparser.Expr
		if sq.FilterType.NeedsListArg() {
			arg = sqlparser.NewListArg(sq.ArgName)
		} else {
			arg = sqlparser.NewArgument(sq.ArgName)
		}
		cursor.Replace(arg)
	}
	rhsPred := sqlparser.CopyOnRewrite(sq.Original, dontEnterSubqueries, post, ctx.SemTable.CopySemanticInfo).(sqlparser.Expr)

	var predicates []sqlparser.Expr
	switch sq.FilterType {
	case opcode.PulloutExists:
		sq.addLimit()
		predicates = append(predicates, rhsPred)
	case opcode.PulloutNotExists:
		sq.addLimit()
		sq.FilterType = opcode.PulloutExists // it's the same pullout as EXISTS, just with a NOT in front of the predicate
		predicates = append(predicates, rhsPred)
	case opcode.PulloutIn:
		// Because we replace the comparison expression with an AND expression, it might be the top level construct there.
		// In this case, it is better to send the two sides of the AND expression separately in the predicates because it can
		// lead to better routing. This however might not always be true for example we can have the rhsPred to be something like
		// `user.id = 2 OR (:__sq_has_values AND user.id IN ::sql1)`
		if andExpr, isAndExpr := rhsPred.(*sqlparser.AndExpr); isAndExpr {
			predicates = append(predicates, andExpr.Left, andExpr.Right)
		} else {
			predicates = append(predicates, rhsPred)
		}
		sq.SubqueryValueName = sq.ArgName
	case opcode.PulloutNotIn:
		predicates = append(predicates, rhsPred)
		sq.SubqueryValueName = sq.ArgName
	case opcode.PulloutValue:
		predicates = append(predicates, rhsPred)
		sq.SubqueryValueName = sq.ArgName
	}
	return newFilter(outer, predicates...)
}

// settleFilterInGroup handles SubQueries that share a predicate with siblings
// (e.g. multiple subqueries in one OR). Non-leader members add their inner
// limit and return outer unchanged; the leader (last unmerged member in DFS
// order) walks the shared predicate once, substitutes every unmerged member's
// owning node with the right bind-var placeholder, and emits a single Filter.
//
// Coordination is via group.merged (which siblings the merge phase absorbed
// into the outer Route, and so should be kept inline rather than substituted)
// and group.emitted (set once the predicate has been emitted, either here or
// by pushOrMergeSubQueryContainer when the entire group merged).
func (sq *SubQuery) settleFilterInGroup(ctx *plancontext.PlanningContext, outer Operator) Operator {
	g := sq.group

	// Always attach a limit-1 to our inner subquery — applies to both leader
	// and non-leader members. The engine reads it independently per SubQuery.
	if sq.FilterType == opcode.PulloutExists || sq.FilterType == opcode.PulloutNotExists {
		sq.addLimit()
	}
	if sq.FilterType == opcode.PulloutNotExists {
		// Engine treats NOT EXISTS pullout the same as EXISTS pullout — the
		// negation lives in the predicate (we keep the wrapping NotExpr).
		sq.FilterType = opcode.PulloutExists
	}
	if sq.FilterType.NeedsListArg() || sq.FilterType == opcode.PulloutValue {
		sq.SubqueryValueName = sq.ArgName
	}

	if g.emitted || !sq.isGroupLeader() {
		return outer
	}

	// Leader path. Reserve HasValuesName for every still-alive (unmerged)
	// member up front so that the single rewrite below can emit the correct
	// bind var per owning subquery node, regardless of which member's settle
	// runs first.
	for _, m := range g.Members {
		if g.merged[m] {
			continue
		}
		if m.HasValuesName == "" &&
			(m.FilterType == opcode.PulloutExists ||
				m.FilterType == opcode.PulloutIn ||
				m.FilterType == opcode.PulloutNotIn) {
			m.HasValuesName = ctx.ReservedVars.ReserveVariable(string(sqlparser.HasValueSubQueryBaseName))
		}
	}

	// Build a Subquery* -> owning member map by resolving each unmerged
	// member's path against the shared predicate. We use this map (rather
	// than a counter) so the post callback can look up ownership directly,
	// even when the rewrite produces new ExistsExpr instances.
	ownerByNode := make(map[*sqlparser.Subquery]*SubQuery, len(g.Members))
	for _, m := range g.Members {
		if g.merged[m] {
			continue
		}
		if subqNode, ok := sqlparser.GetNodeFromPath(g.Original, m.path).(*sqlparser.Subquery); ok {
			ownerByNode[subqNode] = m
		}
	}

	post := func(cursor *sqlparser.CopyOnWriteCursor) {
		switch node := cursor.Node().(type) {
		case *sqlparser.ComparisonExpr:
			// IN / NOT IN: by post-order, the inner Subquery has already been
			// replaced with a ListArg in the *Subquery branch below; we now
			// wrap the whole comparison with the has-values guard.
			listArg, ok := node.Right.(sqlparser.ListArg)
			if !ok {
				return
			}
			owner := findGroupMemberByArgName(g, listArg.String())
			if owner == nil {
				return
			}
			switch owner.FilterType {
			case opcode.PulloutIn:
				cursor.Replace(sqlparser.AndExpressions(sqlparser.NewArgument(owner.HasValuesName), node))
			case opcode.PulloutNotIn:
				cursor.Replace(&sqlparser.OrExpr{
					Left:  sqlparser.NewNotExpr(sqlparser.NewArgument(owner.HasValuesName)),
					Right: node,
				})
			}

		case *sqlparser.ExistsExpr:
			// For EXISTS / NOT EXISTS, replace the whole ExistsExpr with the
			// has-values arg of the owning member. We deliberately skip
			// replacing the inner Subquery (see the *Subquery branch) so
			// node.Subquery still keys into ownerByNode here.
			owner := ownerByNode[node.Subquery]
			if owner == nil || owner.FilterType != opcode.PulloutExists {
				return
			}
			cursor.Replace(sqlparser.NewArgument(owner.HasValuesName))

		case *sqlparser.Subquery:
			owner := ownerByNode[node]
			if owner == nil {
				return
			}
			if owner.FilterType == opcode.PulloutExists {
				// Defer to the wrapping ExistsExpr post above.
				return
			}
			if owner.FilterType.NeedsListArg() {
				cursor.Replace(sqlparser.NewListArg(owner.ArgName))
			} else {
				cursor.Replace(sqlparser.NewArgument(owner.ArgName))
			}
		}
	}

	rhsPred := sqlparser.CopyOnRewrite(g.Original, dontEnterSubqueries, post, ctx.SemTable.CopySemanticInfo).(sqlparser.Expr)
	g.emitted = true
	return newFilter(outer, rhsPred)
}

// isGroupLeader reports whether this SubQuery is the last unmerged member of
// its group in DFS extraction order. The leader emits the group's predicate
// as a single Filter at settle time. Choosing the LAST in DFS order means
// that by the time the leader settles, all earlier siblings have already
// settled (settleSubqueries iterates op.Inner in append order, which is DFS),
// so their bind values are bound by the operator chain wrapping our Filter.
func (sq *SubQuery) isGroupLeader() bool {
	g := sq.group
	if g == nil {
		return false
	}
	for i := len(g.Members) - 1; i >= 0; i-- {
		m := g.Members[i]
		if g.merged[m] {
			continue
		}
		return m == sq
	}
	return false
}

// findGroupMemberByArgName returns the still-unmerged group member whose
// reserved subquery argument name matches the given ListArg name, or nil if
// no such member exists.
func findGroupMemberByArgName(g *subqueryGroup, name string) *SubQuery {
	for _, m := range g.Members {
		if g.merged[m] {
			continue
		}
		if m.ArgName == name {
			return m
		}
	}
	return nil
}

func dontEnterSubqueries(node, _ sqlparser.SQLNode) bool {
	if _, ok := node.(*sqlparser.Subquery); ok {
		return false
	}
	return true
}

func (sq *SubQuery) isMerged(ctx *plancontext.PlanningContext) bool {
	_, ok := ctx.MergedSubqueries[sq.ArgName]
	return ok
}

// predicate returns the predicate this SubQuery operates on. For grouped
// SubQueries (multiple subqueries extracted from the same expression), this
// is the shared, mutable predicate held on the group; for singletons it is
// the SubQuery's own Original.
func (sq *SubQuery) predicate() sqlparser.Expr {
	if sq.group != nil {
		return sq.group.Original
	}
	return sq.Original
}

// mapExpr rewrites all expressions according to the provided function
func (sq *SubQuery) mapExpr(f func(expr sqlparser.Expr) sqlparser.Expr) {
	sq.Predicates = slice.Map(sq.Predicates, f)
	sq.Original = f(sq.Original)
	sq.originalSubquery = f(sq.originalSubquery).(*sqlparser.Subquery)
	if sq.group != nil {
		// Keep the shared group predicate in lockstep with this member's view
		// of it. All members run mapExpr through the same join-rewrite path;
		// the first one updates the group, later members observe an already-
		// rewritten Original (f is the same function, so reapplying it is a no-op).
		sq.group.Original = sq.Original
	}
}
