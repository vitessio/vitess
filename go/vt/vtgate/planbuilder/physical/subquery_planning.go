package physical

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func optimizeSubQuery(ctx *plancontext.PlanningContext, op *abstract.SubQuery) (abstract.PhysicalOperator, error) {
	outerOp, err := CreatePhysicalOperator(ctx, op.Outer)
	if err != nil {
		return nil, err
	}
	var unmerged []*SubQueryOp

	// first loop over the subqueries and try to merge them into the outer plan
	for _, inner := range op.Inner {
		innerOp, err := CreatePhysicalOperator(ctx, inner.Inner)
		if err != nil {
			return nil, err
		}

		preds := inner.Inner.UnsolvedPredicates(ctx.SemTable)
		merger := func(a, b *Route) (*Route, error) {
			return mergeSubQueryOp(ctx, a, b, inner)
		}

		newInner := &SubQueryInner{
			Inner:             inner.Inner,
			ExtractedSubquery: inner.ExtractedSubquery,
		}
		merged, err := tryMergeSubQueryOp(ctx, outerOp, innerOp, newInner, preds, merger)
		if err != nil {
			return nil, err
		}

		if merged != nil {
			outerOp = merged
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
			correlatedTree, err := createCorrelatedSubqueryOp(ctx, innerOp, outerOp, preds, inner.ExtractedSubquery)
			if err != nil {
				return nil, err
			}
			outerOp = correlatedTree
			continue
		}

		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard correlated subquery")
	}

	/*
		build a tree of the unmerged subqueries
		rt: route, sqt: subqueryTree


		            sqt
		         sqt   rt
		        rt rt
	*/
	for _, tree := range unmerged {
		tree.Outer = outerOp
		outerOp = tree
	}
	return outerOp, nil
}

func mergeSubQueryOp(ctx *plancontext.PlanningContext, outer *Route, inner *Route, subq *abstract.SubQueryInner) (*Route, error) {
	subq.ExtractedSubquery.NeedsRewrite = true

	// go over the subquery and add its tables to the one's solved by the route it is merged with
	// this is needed to so that later when we try to push projections, we get the correct
	// solved tableID from the route, since it also includes the tables from the subquery after merging
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch n := node.(type) {
		case *sqlparser.AliasedTableExpr:
			ts := outer.TableID()
			ts.MergeInPlace(ctx.SemTable.TableSetFor(n))
		}
		return true, nil
	}, subq.ExtractedSubquery.Subquery)
	if err != nil {
		return nil, err
	}
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
	for i, predicate := range outer.SeenPredicates {
		if sqlparser.EqualsExpr(predicate, subq.ExtractedSubquery) {
			outer.SeenPredicates = append(outer.SeenPredicates[:i], outer.SeenPredicates[i+1:]...)

			// The `ExtractedSubquery` of an inner query is unique (due to the uniqueness of bind variable names)
			// so we can stop after the first match.
			break
		}
	}

	err = outer.resetRoutingSelections(ctx)
	if err != nil {
		return nil, err
	}
	return outer, nil
}

func tryMergeSubQueryOp(
	ctx *plancontext.PlanningContext,
	outer, subq abstract.PhysicalOperator,
	subQueryInner *SubQueryInner,
	joinPredicates []sqlparser.Expr,
	merger mergeFunc,
) (abstract.PhysicalOperator, error) {
	var merged abstract.PhysicalOperator
	var err error
	switch outerOp := outer.(type) {
	case *Route:
		if shouldTryMergingSubquery(outerOp, subq) {
			merged, err = tryMerge(ctx, outerOp, subq, joinPredicates, merger)
			if err != nil {
				return nil, err
			}
			return merged, err
		}
		return nil, nil
	case *ApplyJoin:
		// Trying to merge the subquery with the left-hand or right-hand side of the join

		if outerOp.LeftJoin {
			return nil, nil
		}
		newMergefunc := func(a, b *Route) (*Route, error) {
			rt, err := merger(a, b)
			if err != nil {
				return nil, err
			}
			outerOp.RHS, err = rewriteColumnsInSubqueryOpForApplyJoin(ctx, outerOp.RHS, outerOp, subQueryInner)
			return rt, err
		}
		merged, err = tryMergeSubQueryOp(ctx, outerOp.LHS, subq, subQueryInner, joinPredicates, newMergefunc)
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
			outerOp.LHS, err = rewriteColumnsInSubqueryOpForApplyJoin(ctx, outerOp.LHS, outerOp, subQueryInner)
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
	default:
		return nil, nil
	}
}

// shouldTryMergingSubquery returns whether there is a possibility of merging the subquery with the outer route
// For some cases like Reference, we shouldn't try to merge them, since the common logic of tryMerge will allow them to
// be merged, even though they shouldn't
func shouldTryMergingSubquery(outerOp *Route, subq abstract.PhysicalOperator) bool {
	if outerOp.RouteOpCode == engine.Reference {
		subqRoute, isRoute := subq.(*Route)
		if !isRoute {
			return false
		}
		return subqRoute.RouteOpCode.IsSingleShard()
	}
	return true
}

// rewriteColumnsInSubqueryOpForApplyJoin rewrites the columns that appear from the other side
// of the join. For example, let's say we merged a subquery on the right side of a join tree
// If it was using any columns from the left side then they need to be replaced by bind variables supplied
// from that side.
// outerTree is the joinTree within whose children the subquery lives in
// the child of joinTree which does not contain the subquery is the otherTree
func rewriteColumnsInSubqueryOpForApplyJoin(
	ctx *plancontext.PlanningContext,
	innerOp abstract.PhysicalOperator,
	outerTree *ApplyJoin,
	subQueryInner *SubQueryInner,
) (abstract.PhysicalOperator, error) {
	resultInnerOp := innerOp
	var rewriteError error
	// go over the entire expression in the subquery
	sqlparser.Rewrite(subQueryInner.ExtractedSubquery.Original, func(cursor *sqlparser.Cursor) bool {
		sqlNode := cursor.Node()
		switch node := sqlNode.(type) {
		case *sqlparser.ColName:
			// check whether the column name belongs to the other side of the join tree
			if ctx.SemTable.RecursiveDeps(node).IsSolvedBy(resultInnerOp.TableID()) {
				// get the bindVariable for that column name and replace it in the subquery
				bindVar := ctx.ReservedVars.ReserveColName(node)
				cursor.Replace(sqlparser.NewArgument(bindVar))
				// check whether the bindVariable already exists in the joinVars of the other tree
				_, alreadyExists := outerTree.Vars[bindVar]
				if alreadyExists {
					return false
				}
				// if it does not exist, then push this as an output column there and add it to the joinVars
				newInnerOp, columnIndexes, err := PushOutputColumns(ctx, resultInnerOp, node)
				if err != nil {
					rewriteError = err
					return false
				}
				columnIndex := columnIndexes[0]
				outerTree.Vars[bindVar] = columnIndex
				resultInnerOp = newInnerOp
				return false
			}
		}
		return true
	}, nil)

	// update the dependencies for the subquery by removing the dependencies from the innerOp
	tableSet := ctx.SemTable.Direct[subQueryInner.ExtractedSubquery.Subquery]
	tableSet.RemoveInPlace(resultInnerOp.TableID())
	ctx.SemTable.Direct[subQueryInner.ExtractedSubquery.Subquery] = tableSet
	tableSet = ctx.SemTable.Recursive[subQueryInner.ExtractedSubquery.Subquery]
	tableSet.RemoveInPlace(resultInnerOp.TableID())
	ctx.SemTable.Recursive[subQueryInner.ExtractedSubquery.Subquery] = tableSet

	// return any error while rewriting
	return resultInnerOp, rewriteError
}

func createCorrelatedSubqueryOp(
	ctx *plancontext.PlanningContext,
	innerOp, outerOp abstract.PhysicalOperator,
	preds []sqlparser.Expr,
	extractedSubquery *sqlparser.ExtractedSubquery,
) (*CorrelatedSubQueryOp, error) {
	newOuter, err := RemovePredicate(ctx, extractedSubquery, outerOp)
	if err != nil {
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "exists sub-queries are only supported with AND clause")
	}

	resultOuterOp := newOuter
	vars := map[string]int{}
	bindVars := map[*sqlparser.ColName]string{}
	var lhsCols []*sqlparser.ColName
	for _, pred := range preds {
		var rewriteError error
		sqlparser.Rewrite(pred, func(cursor *sqlparser.Cursor) bool {
			switch node := cursor.Node().(type) {
			case *sqlparser.ColName:
				if ctx.SemTable.RecursiveDeps(node).IsSolvedBy(resultOuterOp.TableID()) {
					// check whether the bindVariable already exists in the map
					// we do so by checking that the column names are the same and their recursive dependencies are the same
					// so if the column names user.a and a would also be equal if the latter is also referencing the user table
					for colName, bindVar := range bindVars {
						if node.Name.Equal(colName.Name) && ctx.SemTable.RecursiveDeps(node).Equals(ctx.SemTable.RecursiveDeps(colName)) {
							cursor.Replace(sqlparser.NewArgument(bindVar))
							return false
						}
					}

					// get the bindVariable for that column name and replace it in the predicate
					bindVar := ctx.ReservedVars.ReserveColName(node)
					cursor.Replace(sqlparser.NewArgument(bindVar))
					// store it in the map for future comparisons
					bindVars[node] = bindVar

					// if it does not exist, then push this as an output column in the outerOp and add it to the joinVars
					newOuterOp, columnIndexes, err := PushOutputColumns(ctx, resultOuterOp, node)
					if err != nil {
						rewriteError = err
						return false
					}
					lhsCols = append(lhsCols, node)
					columnIndex := columnIndexes[0]
					vars[bindVar] = columnIndex
					resultOuterOp = newOuterOp
					return false
				}
			}
			return true
		}, nil)
		if rewriteError != nil {
			return nil, rewriteError
		}
		var err error
		innerOp, err = PushPredicate(ctx, pred, innerOp)
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
