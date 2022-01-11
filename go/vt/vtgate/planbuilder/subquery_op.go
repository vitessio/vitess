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

package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/context"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/physical"
)

func optimizeSubQueryOp(ctx *context.PlanningContext, op *abstract.SubQuery) (abstract.PhysicalOperator, error) {
	outerOp, err := createPhysicalOperator(ctx, op.Outer)
	if err != nil {
		return nil, err
	}
	var unmerged []*physical.SubQueryOp

	// first loop over the subqueries and try to merge them into the outer plan
	for _, inner := range op.Inner {
		innerOp, err := createPhysicalOperator(ctx, inner.Inner)
		if err != nil {
			return nil, err
		}

		preds := inner.Inner.UnsolvedPredicates(ctx.SemTable)
		merger := func(a, b *physical.RouteOp) (*physical.RouteOp, error) {
			return mergeSubQueryOp(ctx, a, b, inner)
		}

		newInner := &physical.SubQueryInner{
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
			sq := &physical.SubQueryOp{
				Extracted: inner.ExtractedSubquery,
				Inner:     innerOp,
			}
			unmerged = append(unmerged, sq)
			continue
		}

		if inner.ExtractedSubquery.OpCode == int(engine.PulloutExists) {
			_, correlatedTree, err := createCorrelatedSubqueryOp(ctx, innerOp, outerOp, preds, inner.ExtractedSubquery)
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

func mergeSubQueryOp(ctx *context.PlanningContext, outer *physical.RouteOp, inner *physical.RouteOp, subq *abstract.SubQueryInner) (*physical.RouteOp, error) {
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
		outer.SysTableTableName[k] = v
	}

	// TODO: implement reset routing selections
	// err = outer.resetRoutingSelections(ctx)
	// if err != nil {
	// 	return nil, err
	// }
	return outer, nil
}

func tryMergeSubQueryOp(
	ctx *context.PlanningContext,
	outer, subq abstract.PhysicalOperator,
	subQueryInner *physical.SubQueryInner,
	joinPredicates []sqlparser.Expr,
	merger mergeOpFunc,
) (abstract.PhysicalOperator, error) {
	var merged abstract.PhysicalOperator
	var err error
	switch outerOp := outer.(type) {
	case *physical.RouteOp:
		merged, err = tryMergeOp(ctx, outerOp, subq, joinPredicates, merger)
		if err != nil {
			return nil, err
		}
		return merged, err
	case *physical.ApplyJoin:
		// Trying to merge the subquery with the left-hand or right-hand side of the join

		if outerOp.LeftJoin {
			return nil, nil
		}
		newMergefunc := func(a, b *physical.RouteOp) (*physical.RouteOp, error) {
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

		newMergefunc = func(a, b *physical.RouteOp) (*physical.RouteOp, error) {
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

// rewriteColumnsInSubqueryOpForApplyJoin rewrites the columns that appear from the other side
// of the join. For example, let's say we merged a subquery on the right side of a join tree
// If it was using any columns from the left side then they need to be replaced by bind variables supplied
// from that side.
// outerTree is the joinTree within whose children the subquery lives in
// the child of joinTree which does not contain the subquery is the otherTree
func rewriteColumnsInSubqueryOpForApplyJoin(
	ctx *context.PlanningContext,
	innerOp abstract.PhysicalOperator,
	outerTree *physical.ApplyJoin,
	subQueryInner *physical.SubQueryInner,
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
	ctx *context.PlanningContext,
	innerOp, outerOp abstract.PhysicalOperator,
	preds []sqlparser.Expr,
	extractedSubquery *sqlparser.ExtractedSubquery,
) (
	abstract.PhysicalOperator,
	*physical.CorrelatedSubQueryOp,
	error,
) {
	// TODO: add remove predicate to the physical operator
	// err := outerOp.removePredicate(ctx, extractedSubquery)
	// if err != nil {
	// 	return nil, nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "exists sub-queries are only supported with AND clause")
	// }

	resultOuterOp := outerOp
	vars := map[string]int{}
	bindVars := map[*sqlparser.ColName]string{}
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
					columnIndex := columnIndexes[0]
					vars[bindVar] = columnIndex
					resultOuterOp = newOuterOp
					return false
				}
			}
			return true
		}, nil)
		if rewriteError != nil {
			return nil, nil, rewriteError
		}
		var err error
		innerOp, err = PushPredicate(ctx, pred, innerOp)
		if err != nil {
			return nil, nil, err
		}
	}
	return resultOuterOp, &physical.CorrelatedSubQueryOp{
		Outer:     outerOp,
		Inner:     innerOp,
		Extracted: extractedSubquery,
		Vars:      vars,
	}, nil
}

func transformSubQueryOpPlan(ctx *context.PlanningContext, op *physical.SubQueryOp) (logicalPlan, error) {
	innerPlan, err := transformOpToLogicalPlan(ctx, op.Inner)
	if err != nil {
		return nil, err
	}
	innerPlan, err = planHorizon(ctx, innerPlan, op.Extracted.Subquery.Select)
	if err != nil {
		return nil, err
	}

	argName := op.Extracted.GetArgName()
	hasValuesArg := op.Extracted.GetHasValuesArg()
	outerPlan, err := transformOpToLogicalPlan(ctx, op.Outer)

	merged := mergeSubQueryOpPlan(ctx, innerPlan, outerPlan, op)
	if merged != nil {
		return merged, nil
	}
	plan := newPulloutSubquery(engine.PulloutOpcode(op.Extracted.OpCode), argName, hasValuesArg, innerPlan)
	if err != nil {
		return nil, err
	}
	plan.underlying = outerPlan
	return plan, err
}

func transformCorrelatedSubQueryOpPlan(ctx *context.PlanningContext, op *physical.CorrelatedSubQueryOp) (logicalPlan, error) {
	outer, err := transformOpToLogicalPlan(ctx, op.Outer)
	if err != nil {
		return nil, err
	}
	inner, err := transformOpToLogicalPlan(ctx, op.Inner)
	if err != nil {
		return nil, err
	}
	return newSemiJoin(outer, inner, op.Vars), nil
}

func mergeSubQueryOpPlan(ctx *context.PlanningContext, inner, outer logicalPlan, n *physical.SubQueryOp) logicalPlan {
	iroute, ok := inner.(*routeGen4)
	if !ok {
		return nil
	}
	oroute, ok := outer.(*routeGen4)
	if !ok {
		return nil
	}

	if canMergeSubqueryPlans(ctx, iroute, oroute) {
		// n.extracted is an expression that lives in oroute.Select.
		// Instead of looking for it in the AST, we have a copy in the subquery tree that we can update
		n.Extracted.NeedsRewrite = true
		replaceSubQuery(ctx, oroute.Select)
		return mergeSystemTableInformation(oroute, iroute)
	}
	return nil
}
