/*
Copyright 2022 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func optimizeSubQuery(ctx *plancontext.PlanningContext, op *SubQuery) (ops.Operator, rewrite.TreeIdentity, error) {
	var unmerged []*SubQueryOp

	// first loop over the subqueries and try to merge them into the outer plan
	outer := op.Outer
	for _, inner := range op.Inner {
		innerOp := inner.Inner

		var preds []sqlparser.Expr
		preds, innerOp = unresolvedAndSource(ctx, innerOp)
		merger := func(a, b *Route) (*Route, error) {
			return mergeSubQueryOp(ctx, a, b, inner)
		}

		newInner := &SubQueryInner{
			Inner:             inner.Inner,
			ExtractedSubquery: inner.ExtractedSubquery,
		}
		merged, err := tryMergeSubQueryOp(ctx, outer, innerOp, newInner, preds, merger)
		if err != nil {
			return nil, rewrite.SameTree, err
		}

		if merged != nil {
			outer = merged
			continue
		}

		if len(preds) == 0 {
			// uncorrelated queries
			sq := &SubQueryOp{
				Extracted: inner.ExtractedSubquery,
				Inner:     innerOp,
			}
			unmerged = append(unmerged, sq)
			continue
		}

		if inner.ExtractedSubquery.OpCode == int(engine.PulloutExists) {
			correlatedTree, err := createCorrelatedSubqueryOp(ctx, innerOp, outer, preds, inner.ExtractedSubquery)
			if err != nil {
				return nil, rewrite.SameTree, err
			}
			outer = correlatedTree
			continue
		}

		return nil, rewrite.SameTree, vterrors.VT12001("cross-shard correlated subquery")
	}

	for _, tree := range unmerged {
		tree.Outer = outer
		outer = tree
	}
	return outer, rewrite.NewTree, nil
}

func unresolvedAndSource(ctx *plancontext.PlanningContext, op ops.Operator) ([]sqlparser.Expr, ops.Operator) {
	preds := UnresolvedPredicates(op, ctx.SemTable)
	if filter, ok := op.(*Filter); ok {
		if ctx.SemTable.ASTEquals().Exprs(preds, filter.Predicates) {
			// if we are seeing a single filter with only these predicates,
			// we can throw away the filter and just use the source
			return preds, filter.Source
		}
	}

	return preds, op
}

func mergeSubQueryOp(ctx *plancontext.PlanningContext, outer *Route, inner *Route, subq *SubQueryInner) (*Route, error) {
	subq.ExtractedSubquery.NeedsRewrite = true
	outer.SysTableTableSchema = append(outer.SysTableTableSchema, inner.SysTableTableSchema...)
	for k, v := range inner.SysTableTableName {
		if outer.SysTableTableName == nil {
			outer.SysTableTableName = map[string]evalengine.Expr{}
		}
		outer.SysTableTableName[k] = v
	}

	// When merging an inner query with its outer query, we can remove the
	// inner query from the list of predicates that can influence routing of
	// the outer query.
	//
	// Note that not all inner queries necessarily are part of the routing
	// predicates list, so this might be a no-op.
	subQueryWasPredicate := false
	for i, predicate := range outer.SeenPredicates {
		if ctx.SemTable.EqualsExpr(predicate, subq.ExtractedSubquery) {
			outer.SeenPredicates = append(outer.SeenPredicates[:i], outer.SeenPredicates[i+1:]...)

			subQueryWasPredicate = true

			// The `ExtractedSubquery` of an inner query is unique (due to the uniqueness of bind variable names)
			// so we can stop after the first match.
			break
		}
	}

	err := outer.resetRoutingSelections(ctx)
	if err != nil {
		return nil, err
	}

	if subQueryWasPredicate {
		// Copy Vindex predicates from the inner route to the upper route.
		// If we can route based on some of these predicates, the routing can improve
		outer.VindexPreds = append(outer.VindexPreds, inner.VindexPreds...)

		if inner.RouteOpCode == engine.None {
			outer.setSelectNoneOpcode()
		}
	}

	outer.MergedWith = append(outer.MergedWith, inner)

	return outer, nil
}

func isMergeable(ctx *plancontext.PlanningContext, query sqlparser.SelectStatement, op ops.Operator) bool {
	validVindex := func(expr sqlparser.Expr) bool {
		sc := findColumnVindex(ctx, op, expr)
		return sc != nil && sc.IsUnique()
	}

	if query.GetLimit() != nil {
		return false
	}

	sel, ok := query.(*sqlparser.Select)
	if !ok {
		return false
	}

	if len(sel.GroupBy) > 0 {
		// iff we are grouping, we need to check that we can perform the grouping inside a single shard, and we check that
		// by checking that one of the grouping expressions used is a unique single column vindex.
		// TODO: we could also support the case where all the columns of a multi-column vindex are used in the grouping
		for _, gb := range sel.GroupBy {
			if validVindex(gb) {
				return true
			}
		}
		return false
	}

	// if we have grouping, we have already checked that it's safe, and don't need to check for aggregations
	// but if we don't have groupings, we need to check if there are aggregations that will mess with us
	if sqlparser.ContainsAggregation(sel.SelectExprs) {
		return false
	}

	if sqlparser.ContainsAggregation(sel.Having) {
		return false
	}

	return true
}

func tryMergeSubQueryOp(
	ctx *plancontext.PlanningContext,
	outer, subq ops.Operator,
	subQueryInner *SubQueryInner,
	joinPredicates []sqlparser.Expr,
	merger mergeFunc,
) (ops.Operator, error) {
	switch outerOp := outer.(type) {
	case *Filter:
		op, err := tryMergeSubQueryOp(ctx, outerOp.Source, subq, subQueryInner, joinPredicates, merger)
		if err != nil || op == nil {
			return nil, err
		}
		outerOp.Source = op
		return outerOp, nil
	case *Route:
		return tryMergeSubqueryWithRoute(ctx, subq, outerOp, joinPredicates, merger, subQueryInner)
	case *ApplyJoin:
		return tryMergeSubqueryWithJoin(ctx, subq, outerOp, joinPredicates, merger, subQueryInner)
	default:
		return nil, nil
	}
}

func tryMergeSubqueryWithRoute(
	ctx *plancontext.PlanningContext,
	subq ops.Operator,
	outerOp *Route,
	joinPredicates []sqlparser.Expr,
	merger mergeFunc,
	subQueryInner *SubQueryInner,
) (ops.Operator, error) {
	subqueryRoute, isRoute := subq.(*Route)
	if !isRoute {
		return nil, nil
	}

	if outerOp.RouteOpCode == engine.Reference && !subqueryRoute.IsSingleShard() {
		return nil, nil
	}

	merged, err := tryMerge(ctx, outerOp, subq, joinPredicates, merger)
	if err != nil {
		return nil, err
	}

	// If the subqueries could be merged here, we're done
	if merged != nil {
		return merged, err
	}

	if !isMergeable(ctx, subQueryInner.ExtractedSubquery.Subquery.Select, subq) {
		return nil, nil
	}

	// Special case: Inner query won't return any results / is not routable.
	if subqueryRoute.RouteOpCode == engine.None {
		merged, err := merger(outerOp, subqueryRoute)
		if err != nil {
			return nil, err
		}
		return merged, err
	}

	// Inner subqueries can be merged with the outer subquery as long as
	// the inner query is a single column selection, and that single column has a matching
	// vindex on the outer query's operand.
	if canMergeSubqueryOnColumnSelection(ctx, outerOp, subqueryRoute, subQueryInner.ExtractedSubquery) {
		merged, err := merger(outerOp, subqueryRoute)

		if err != nil {
			return nil, err
		}

		if merged != nil {
			// since we inlined the subquery into the outer query, new vindex options might have been enabled,
			// so we go over our current options to check if anything better has come up.
			merged.PickBestAvailableVindex()
			return merged, err
		}
	}
	return nil, nil
}

func tryMergeSubqueryWithJoin(
	ctx *plancontext.PlanningContext,
	subq ops.Operator,
	outerOp *ApplyJoin,
	joinPredicates []sqlparser.Expr,
	merger mergeFunc,
	subQueryInner *SubQueryInner,
) (ops.PhysicalOperator, error) {
	// Trying to merge the subquery with the left-hand or right-hand side of the join

	if outerOp.LeftJoin {
		return nil, nil
	}
	newMergefunc := func(a, b *Route) (*Route, error) {
		rt, err := merger(a, b)
		if err != nil {
			return nil, err
		}
		outerOp.RHS, err = rewriteColumnsInSubqueryOpForJoin(ctx, outerOp.RHS, outerOp, subQueryInner)
		return rt, err
	}
	merged, err := tryMergeSubQueryOp(ctx, outerOp.LHS, subq, subQueryInner, joinPredicates, newMergefunc)
	if err != nil {
		return nil, err
	}
	if merged != nil {
		outerOp.LHS = merged
		return outerOp, nil
	}

	newMergefunc = func(a, b *Route) (*Route, error) {
		rt, err := merger(a, b)
		if err != nil {
			return nil, err
		}
		outerOp.LHS, err = rewriteColumnsInSubqueryOpForJoin(ctx, outerOp.LHS, outerOp, subQueryInner)
		return rt, err
	}
	merged, err = tryMergeSubQueryOp(ctx, outerOp.RHS, subq, subQueryInner, joinPredicates, newMergefunc)
	if err != nil {
		return nil, err
	}
	if merged != nil {
		outerOp.RHS = merged
		return outerOp, nil
	}
	return nil, nil
}

// rewriteColumnsInSubqueryOpForJoin rewrites the columns that appear from the other side
// of the join. For example, let's say we merged a subquery on the right side of a join tree
// If it was using any columns from the left side then they need to be replaced by bind variables supplied
// from that side.
// outerTree is the joinTree within whose children the subquery lives in
// the child of joinTree which does not contain the subquery is the otherTree
func rewriteColumnsInSubqueryOpForJoin(
	ctx *plancontext.PlanningContext,
	innerOp ops.Operator,
	outerTree *ApplyJoin,
	subQueryInner *SubQueryInner,
) (ops.Operator, error) {
	resultInnerOp := innerOp
	var rewriteError error
	// go over the entire expression in the subquery
	sqlparser.SafeRewrite(subQueryInner.ExtractedSubquery.Original, nil, func(cursor *sqlparser.Cursor) bool {
		node, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return true
		}

		// check whether the column name belongs to the other side of the join tree
		if !ctx.SemTable.RecursiveDeps(node).IsSolvedBy(TableID(resultInnerOp)) {
			return true
		}

		// get the bindVariable for that column name and replace it in the subquery
		bindVar := ctx.ReservedVars.ReserveColName(node)
		cursor.Replace(sqlparser.NewArgument(bindVar))
		// check whether the bindVariable already exists in the joinVars of the other tree
		_, alreadyExists := outerTree.Vars[bindVar]
		if alreadyExists {
			return true
		}
		// if it does not exist, then push this as an output column there and add it to the joinVars
		offset, err := resultInnerOp.AddColumn(ctx, node)
		if err != nil {
			rewriteError = err
			return false
		}
		outerTree.Vars[bindVar] = offset
		return true
	})

	// update the dependencies for the subquery by removing the dependencies from the innerOp
	tableSet := ctx.SemTable.Direct[subQueryInner.ExtractedSubquery.Subquery]
	ctx.SemTable.Direct[subQueryInner.ExtractedSubquery.Subquery] = tableSet.Remove(TableID(resultInnerOp))
	tableSet = ctx.SemTable.Recursive[subQueryInner.ExtractedSubquery.Subquery]
	ctx.SemTable.Recursive[subQueryInner.ExtractedSubquery.Subquery] = tableSet.Remove(TableID(resultInnerOp))

	// return any error while rewriting
	return resultInnerOp, rewriteError
}

func createCorrelatedSubqueryOp(
	ctx *plancontext.PlanningContext,
	innerOp, outerOp ops.Operator,
	preds []sqlparser.Expr,
	extractedSubquery *sqlparser.ExtractedSubquery,
) (*CorrelatedSubQueryOp, error) {
	newOuter, err := RemovePredicate(ctx, extractedSubquery, outerOp)
	if err != nil {
		return nil, vterrors.VT12001("EXISTS sub-queries are only supported with AND clause")
	}

	resultOuterOp := newOuter
	vars := map[string]int{}
	bindVars := map[*sqlparser.ColName]string{}
	var lhsCols []*sqlparser.ColName
	for _, pred := range preds {
		var rewriteError error
		sqlparser.SafeRewrite(pred, nil, func(cursor *sqlparser.Cursor) bool {
			node, ok := cursor.Node().(*sqlparser.ColName)
			if !ok {
				return true
			}

			nodeDeps := ctx.SemTable.RecursiveDeps(node)
			if !nodeDeps.IsSolvedBy(TableID(resultOuterOp)) {
				return true
			}

			// check whether the bindVariable already exists in the map
			// we do so by checking that the column names are the same and their recursive dependencies are the same
			// so the column names `user.a` and `a` would be considered equal as long as both are bound to the same table
			for colName, bindVar := range bindVars {
				if ctx.SemTable.EqualsExpr(node, colName) {
					cursor.Replace(sqlparser.NewArgument(bindVar))
					return true
				}
			}

			// get the bindVariable for that column name and replace it in the predicate
			bindVar := ctx.ReservedVars.ReserveColName(node)
			cursor.Replace(sqlparser.NewArgument(bindVar))
			// store it in the map for future comparisons
			bindVars[node] = bindVar

			// if it does not exist, then push this as an output column in the outerOp and add it to the joinVars
			offset, err := resultOuterOp.AddColumn(ctx, node)
			if err != nil {
				rewriteError = err
				return true
			}
			lhsCols = append(lhsCols, node)
			vars[bindVar] = offset
			return true
		})
		if rewriteError != nil {
			return nil, rewriteError
		}
		var err error
		innerOp, err = innerOp.AddPredicate(ctx, pred)
		if err != nil {
			return nil, err
		}
	}
	return &CorrelatedSubQueryOp{
		Outer:      resultOuterOp,
		Inner:      innerOp,
		Extracted:  extractedSubquery,
		Vars:       vars,
		LHSColumns: lhsCols,
	}, nil
}

// canMergeSubqueryOnColumnSelection will return true if the predicate used allows us to merge the two subqueries
// into a single Route. This can be done if we are comparing two columns that contain data that is guaranteed
// to exist on the same shard.
func canMergeSubqueryOnColumnSelection(ctx *plancontext.PlanningContext, a, b *Route, predicate *sqlparser.ExtractedSubquery) bool {
	left := predicate.OtherSide
	opCode := predicate.OpCode
	if opCode != int(engine.PulloutValue) && opCode != int(engine.PulloutIn) {
		return false
	}

	lVindex := findColumnVindex(ctx, a, left)
	if lVindex == nil || !lVindex.IsUnique() {
		return false
	}

	rightSelection := extractSingleColumnSubquerySelection(predicate.Subquery)
	if rightSelection == nil {
		return false
	}

	rVindex := findColumnVindex(ctx, b, rightSelection)
	if rVindex == nil {
		return false
	}
	return rVindex == lVindex
}

// Searches for the single column returned from a subquery, like the `col` in `(SELECT col FROM tbl)`
func extractSingleColumnSubquerySelection(subquery *sqlparser.Subquery) *sqlparser.ColName {
	if subquery.Select.GetColumnCount() != 1 {
		return nil
	}

	columnExpr := subquery.Select.GetColumns()[0]

	aliasedExpr, ok := columnExpr.(*sqlparser.AliasedExpr)
	if !ok {
		return nil
	}

	return getColName(aliasedExpr.Expr)
}
