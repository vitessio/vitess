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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// pushProjection pushes a projection to the plan.
func pushProjection(
	ctx *plancontext.PlanningContext,
	expr *sqlparser.AliasedExpr,
	plan logicalPlan,
	inner, reuseCol, hasAggregation bool,
) (offset int, added bool, err error) {
	switch node := plan.(type) {
	case *limit, *projection, *pulloutSubquery, *distinct, *filter:
		// All of these either push to the single source, or push to the LHS
		src := node.Inputs()[0]
		return pushProjection(ctx, expr, src, inner, reuseCol, hasAggregation)
	case *routeGen4:
		return addExpressionToRoute(ctx, node, expr, reuseCol)
	case *hashJoin:
		return pushProjectionIntoHashJoin(ctx, expr, node, reuseCol, inner, hasAggregation)
	case *joinGen4:
		return pushProjectionIntoJoin(ctx, expr, node, reuseCol, inner, hasAggregation)
	case *simpleProjection:
		return pushProjectionIntoSimpleProj(ctx, expr, node, inner, hasAggregation, reuseCol)
	case *orderedAggregate:
		return pushProjectionIntoOA(ctx, expr, node, inner, hasAggregation)
	case *vindexFunc:
		return pushProjectionIntoVindexFunc(node, expr, reuseCol)
	case *semiJoin:
		return pushProjectionIntoSemiJoin(ctx, expr, reuseCol, node, inner, hasAggregation)
	case *concatenateGen4:
		return pushProjectionIntoConcatenate(ctx, expr, hasAggregation, node, inner, reuseCol)
	default:
		return 0, false, vterrors.VT13001(fmt.Sprintf("push projection does not yet support: %T", node))
	}
}

func pushProjectionIntoVindexFunc(node *vindexFunc, expr *sqlparser.AliasedExpr, reuseCol bool) (int, bool, error) {
	colsBefore := len(node.eVindexFunc.Cols)
	i, err := node.SupplyProjection(expr, reuseCol)
	if err != nil {
		return 0, false, err
	}
	return i /* col added */, len(node.eVindexFunc.Cols) > colsBefore, nil
}

func pushProjectionIntoConcatenate(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, hasAggregation bool, node *concatenateGen4, inner bool, reuseCol bool) (int, bool, error) {
	if hasAggregation {
		return 0, false, vterrors.VT12001("aggregation on UNIONs")
	}
	offset, added, err := pushProjection(ctx, expr, node.sources[0], inner, reuseCol, hasAggregation)
	if err != nil {
		return 0, false, err
	}
	if added && ctx.SemTable.DirectDeps(expr.Expr).NonEmpty() {
		return 0, false, vterrors.VT13001(fmt.Sprintf("pushing projection %v on concatenate should reference an existing column", sqlparser.String(expr)))
	}
	if added {
		for _, source := range node.sources[1:] {
			_, _, err := pushProjection(ctx, expr, source, inner, reuseCol, hasAggregation)
			if err != nil {
				return 0, false, err
			}
		}
	}
	return offset, added, nil
}

func pushProjectionIntoSemiJoin(
	ctx *plancontext.PlanningContext,
	expr *sqlparser.AliasedExpr,
	reuseCol bool,
	node *semiJoin,
	inner, hasAggregation bool,
) (int, bool, error) {
	passDownReuseCol := reuseCol
	if !reuseCol {
		passDownReuseCol = expr.As.IsEmpty()
	}
	offset, added, err := pushProjection(ctx, expr, node.lhs, inner, passDownReuseCol, hasAggregation)
	if err != nil {
		return 0, false, err
	}
	column := -(offset + 1)
	if reuseCol && !added {
		for idx, col := range node.cols {
			if column == col {
				return idx, false, nil
			}
		}
	}
	node.cols = append(node.cols, column)
	return len(node.cols) - 1, true, nil
}

func pushProjectionIntoOA(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, node *orderedAggregate, inner, hasAggregation bool) (int, bool, error) {
	colName, isColName := expr.Expr.(*sqlparser.ColName)
	for _, aggregate := range node.aggregates {
		if ctx.SemTable.EqualsExpr(aggregate.Expr, expr.Expr) {
			return aggregate.Col, false, nil
		}
		if isColName && colName.Name.EqualString(aggregate.Alias) {
			return aggregate.Col, false, nil
		}
	}
	for _, key := range node.groupByKeys {
		if ctx.SemTable.EqualsExpr(key.Expr, expr.Expr) {
			return key.KeyCol, false, nil
		}
	}
	offset, _, err := pushProjection(ctx, expr, node.input, inner, true, hasAggregation)
	if err != nil {
		return 0, false, err
	}
	node.aggregates = append(node.aggregates, &engine.AggregateParams{
		Opcode:   engine.AggregateRandom,
		Col:      offset,
		Alias:    expr.ColumnName(),
		Expr:     expr.Expr,
		Original: expr,
	})
	return offset, true, nil
}

func pushProjectionIntoSimpleProj(
	ctx *plancontext.PlanningContext,
	expr *sqlparser.AliasedExpr,
	node *simpleProjection,
	inner, hasAggregation, reuseCol bool,
) (int, bool, error) {
	offset, _, err := pushProjection(ctx, expr, node.input, inner, true, hasAggregation)
	if err != nil {
		return 0, false, err
	}
	for i, value := range node.eSimpleProj.Cols {
		// we return early if we already have the column in the simple projection's
		// output list so we do not add it again.
		if reuseCol && value == offset {
			return i, false, nil
		}
	}
	node.eSimpleProj.Cols = append(node.eSimpleProj.Cols, offset)
	return len(node.eSimpleProj.Cols) - 1, true, nil
}

func pushProjectionIntoJoin(
	ctx *plancontext.PlanningContext,
	expr *sqlparser.AliasedExpr,
	node *joinGen4,
	reuseCol, inner, hasAggregation bool,
) (int, bool, error) {
	lhsSolves := node.Left.ContainsTables()
	rhsSolves := node.Right.ContainsTables()
	deps := ctx.SemTable.RecursiveDeps(expr.Expr)
	var column int
	var appended bool
	passDownReuseCol := reuseCol
	if !reuseCol {
		passDownReuseCol = expr.As.IsEmpty()
	}
	switch {
	case deps.IsSolvedBy(lhsSolves):
		offset, added, err := pushProjection(ctx, expr, node.Left, inner, passDownReuseCol, hasAggregation)
		if err != nil {
			return 0, false, err
		}
		column = -(offset + 1)
		appended = added
	case deps.IsSolvedBy(rhsSolves):
		offset, added, err := pushProjection(ctx, expr, node.Right, inner && node.Opcode != engine.LeftJoin, passDownReuseCol, hasAggregation)
		if err != nil {
			return 0, false, err
		}
		column = offset + 1
		appended = added
	default:
		// if an expression has aggregation, then it should not be split up and pushed to both sides,
		// for example an expression like count(*) will have dependencies on both sides, but we should not push it
		// instead we should return an error
		if hasAggregation {
			return 0, false, vterrors.VT12001("cross-shard query with aggregates")
		}
		// now we break the expression into left and right side dependencies and rewrite the left ones to bind variables
		bvName, cols, rewrittenExpr, err := operators.BreakExpressionInLHSandRHS(ctx, expr.Expr, lhsSolves)
		if err != nil {
			return 0, false, err
		}
		// go over all the columns coming from the left side of the tree and push them down. While at it, also update the bind variable map.
		// It is okay to reuse the columns on the left side since
		// the final expression which will be selected will be pushed into the right side.
		for i, col := range cols {
			colOffset, _, err := pushProjection(ctx, &sqlparser.AliasedExpr{Expr: col}, node.Left, inner, true, false)
			if err != nil {
				return 0, false, err
			}
			node.Vars[bvName[i]] = colOffset
		}
		// push the rewritten expression on the right side of the tree. Here we should take care whether we want to reuse the expression or not.
		expr.Expr = rewrittenExpr
		offset, added, err := pushProjection(ctx, expr, node.Right, inner && node.Opcode != engine.LeftJoin, passDownReuseCol, false)
		if err != nil {
			return 0, false, err
		}
		column = offset + 1
		appended = added
	}
	if reuseCol && !appended {
		for idx, col := range node.Cols {
			if column == col {
				return idx, false, nil
			}
		}
		// the column was not appended to either child, but we could not find it in out cols list,
		// so we'll still add it
	}
	node.Cols = append(node.Cols, column)
	return len(node.Cols) - 1, true, nil
}

func pushProjectionIntoHashJoin(
	ctx *plancontext.PlanningContext,
	expr *sqlparser.AliasedExpr,
	node *hashJoin,
	reuseCol, inner, hasAggregation bool,
) (int, bool, error) {
	lhsSolves := node.Left.ContainsTables()
	rhsSolves := node.Right.ContainsTables()
	deps := ctx.SemTable.RecursiveDeps(expr.Expr)
	var column int
	var appended bool
	passDownReuseCol := reuseCol
	if !reuseCol {
		passDownReuseCol = expr.As.IsEmpty()
	}
	switch {
	case deps.IsSolvedBy(lhsSolves):
		offset, added, err := pushProjection(ctx, expr, node.Left, inner, passDownReuseCol, hasAggregation)
		if err != nil {
			return 0, false, err
		}
		column = -(offset + 1)
		appended = added
	case deps.IsSolvedBy(rhsSolves):
		offset, added, err := pushProjection(ctx, expr, node.Right, inner && node.Opcode != engine.LeftJoin, passDownReuseCol, hasAggregation)
		if err != nil {
			return 0, false, err
		}
		column = offset + 1
		appended = added
	default:
		// if an expression has aggregation, then it should not be split up and pushed to both sides,
		// for example an expression like count(*) will have dependencies on both sides, but we should not push it
		// instead we should return an error
		if hasAggregation {
			return 0, false, vterrors.VT12001("cross-shard query with aggregates")
		}
		return 0, false, vterrors.VT12001("hash join with projection from both sides of the join")
	}
	if reuseCol && !appended {
		for idx, col := range node.Cols {
			if column == col {
				return idx, false, nil
			}
		}
		// the column was not appended to either child, but we could not find it in out cols list,
		// so we'll still add it
	}
	node.Cols = append(node.Cols, column)
	return len(node.Cols) - 1, true, nil
}

func addExpressionToRoute(ctx *plancontext.PlanningContext, rb *routeGen4, expr *sqlparser.AliasedExpr, reuseCol bool) (int, bool, error) {
	if reuseCol {
		if i := checkIfAlreadyExists(expr, rb.Select, ctx.SemTable); i != -1 {
			return i, false, nil
		}
	}
	sqlparser.RemoveKeyspaceFromColName(expr.Expr)
	sel, isSel := rb.Select.(*sqlparser.Select)
	if !isSel {
		return 0, false, vterrors.VT12001(fmt.Sprintf("pushing projection '%s' on %T", sqlparser.String(expr), rb.Select))
	}

	if ctx.RewriteDerivedExpr {
		// if we are trying to push a projection that belongs to a DerivedTable
		// we rewrite that expression, so it matches the column name used inside
		// that derived table.
		err := rewriteProjectionOfDerivedTable(expr, ctx.SemTable)
		if err != nil {
			return 0, false, err
		}
	}

	offset := len(sel.SelectExprs)
	sel.SelectExprs = append(sel.SelectExprs, expr)
	return offset, true, nil
}

func rewriteProjectionOfDerivedTable(expr *sqlparser.AliasedExpr, semTable *semantics.SemTable) error {
	ti, err := semTable.TableInfoForExpr(expr.Expr)
	if err != nil && err != semantics.ErrNotSingleTable {
		return err
	}
	_, isDerivedTable := ti.(*semantics.DerivedTable)
	if isDerivedTable {
		expr.Expr, err = semantics.RewriteDerivedTableExpression(expr.Expr, ti)
		if err != nil {
			return err
		}
	}
	return nil
}
