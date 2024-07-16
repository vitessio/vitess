/*
Copyright 2023 The Vitess Authors.

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
	"strings"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func expandHorizon(ctx *plancontext.PlanningContext, horizon *Horizon) (Operator, *ApplyResult) {
	statement := horizon.selectStatement()
	switch sel := statement.(type) {
	case *sqlparser.Select:
		return expandSelectHorizon(ctx, horizon, sel)
	case *sqlparser.Union:
		return expandUnionHorizon(ctx, horizon, sel)
	}
	panic(vterrors.VT13001(fmt.Sprintf("unexpected statement type %T", statement)))
}

func expandUnionHorizon(ctx *plancontext.PlanningContext, horizon *Horizon, union *sqlparser.Union) (Operator, *ApplyResult) {
	op := horizon.Source

	qp := horizon.getQP(ctx)

	if len(qp.OrderExprs) > 0 {
		op = &Ordering{
			Source: op,
			Order:  qp.OrderExprs,
		}
	}

	if union.Limit != nil {
		op = &Limit{
			Source: op,
			AST:    union.Limit,
		}
	}

	if horizon.TableId != nil {
		proj := newAliasedProjection(op)
		proj.DT = &DerivedTable{
			TableID: *horizon.TableId,
			Alias:   horizon.Alias,
			Columns: horizon.ColumnAliases,
		}
		op = proj
	}

	if op == horizon.Source {
		return op, Rewrote("removed UNION horizon not used")
	}

	return op, Rewrote("expand UNION horizon into smaller components")
}

func expandSelectHorizon(ctx *plancontext.PlanningContext, horizon *Horizon, sel *sqlparser.Select) (Operator, *ApplyResult) {
	qp := horizon.getQP(ctx)
	var extracted []string
	if horizon.IsDerived() {
		// if we are dealing with a derived table, we need to make sure that the ordering columns
		// are available outside the derived table
		for _, order := range horizon.Query.GetOrderBy() {
			qp.addDerivedColumn(ctx, order.Expr)
		}
	}
	op := createProjectionFromSelect(ctx, horizon)
	if qp.HasAggr {
		extracted = append(extracted, "Aggregation")
	} else {
		extracted = append(extracted, "Projection")
	}

	if qp.NeedsDistinct() {
		op = &Distinct{
			Required: true,
			Source:   op,
			QP:       qp,
		}
		extracted = append(extracted, "Distinct")
	}

	if sel.Having != nil {
		op = addWherePredicates(ctx, sel.Having.Expr, op)
		extracted = append(extracted, "Filter")
	}

	if len(qp.OrderExprs) > 0 {
		op = expandOrderBy(ctx, op, qp, horizon.Alias)
		extracted = append(extracted, "Ordering")
	}

	if sel.Limit != nil {
		op = &Limit{
			Source: op,
			AST:    sel.Limit,
		}
		extracted = append(extracted, "Limit")
	}

	return op, Rewrote(fmt.Sprintf("expand SELECT horizon into (%s)", strings.Join(extracted, ", ")))
}

func expandOrderBy(ctx *plancontext.PlanningContext, op Operator, qp *QueryProjection, derived string) Operator {
	var newOrder []OrderBy
	sqc := &SubQueryBuilder{}
	proj, ok := op.(*Projection)

	for _, expr := range qp.OrderExprs {
		// Attempt to extract any subqueries within the expression
		newExpr, subqs := sqc.pullOutValueSubqueries(ctx, expr.SimplifiedExpr, TableID(op), false)
		if newExpr == nil {
			// If no subqueries are found, retain the original order expression
			if derived != "" {
				expr = exposeOrderingColumn(ctx, qp, expr, derived)
			}
			newOrder = append(newOrder, expr)
			continue
		}

		// If the operator is not a projection, we cannot handle subqueries with aggregation if we are unable to push everything into a single route.
		if !ok {
			ctx.SemTable.NotSingleRouteErr = vterrors.VT12001("subquery with aggregation in order by")
			return &Ordering{
				Source: op,
				Order:  qp.OrderExprs,
			}
		} else {
			// Add the new subquery expression to the projection
			proj.addSubqueryExpr(ctx, aeWrap(newExpr), newExpr, subqs...)
		}

		// Replace the original order expression with the new expression containing subqueries
		newOrder = append(newOrder, OrderBy{
			Inner: &sqlparser.Order{
				Expr:      newExpr,
				Direction: expr.Inner.Direction,
			},
			SimplifiedExpr: newExpr,
		})
	}

	// Update the source of the projection if we have it
	if proj != nil {
		proj.Source = sqc.getRootOperator(proj.Source, nil)
	}

	// Return the updated operator with the new order by expressions
	return &Ordering{
		Source: op,
		Order:  newOrder,
	}
}

// exposeOrderingColumn will expose the ordering column to the outer query
func exposeOrderingColumn(ctx *plancontext.PlanningContext, qp *QueryProjection, orderBy OrderBy, derived string) OrderBy {
	for _, se := range qp.SelectExprs {
		aliasedExpr, err := se.GetAliasedExpr()
		if err != nil {
			panic(vterrors.VT13001("unexpected expression in select"))
		}
		if ctx.SemTable.EqualsExprWithDeps(aliasedExpr.Expr, orderBy.SimplifiedExpr) {
			newExpr := sqlparser.NewColNameWithQualifier(aliasedExpr.ColumnName(), sqlparser.NewTableName(derived))
			ctx.SemTable.CopySemanticInfo(orderBy.SimplifiedExpr, newExpr)
			orderBy.SimplifiedExpr = newExpr
			orderBy.Inner = &sqlparser.Order{Expr: newExpr, Direction: orderBy.Inner.Direction}
			break
		}
	}

	return orderBy
}

func createProjectionFromSelect(ctx *plancontext.PlanningContext, horizon *Horizon) Operator {
	qp := horizon.getQP(ctx)

	var dt *DerivedTable
	if horizon.TableId != nil {
		// if we are dealing with a derived table, we need to create a derived table object
		dt = &DerivedTable{
			TableID: *horizon.TableId,
			Alias:   horizon.Alias,
			Columns: horizon.ColumnAliases,
		}
	}

	if qp.NeedsAggregation() {
		return createProjectionWithAggr(ctx, qp, dt, horizon.src())
	}

	projX := createProjectionWithoutAggr(ctx, qp, horizon.src())
	projX.DT = dt
	return projX
}

func createProjectionWithAggr(ctx *plancontext.PlanningContext, qp *QueryProjection, dt *DerivedTable, src Operator) Operator {
	aggregations, complexAggr := qp.AggregationExpressions(ctx, true)
	aggrOp := &Aggregator{
		Source:       src,
		Original:     true,
		QP:           qp,
		Grouping:     qp.GetGrouping(),
		Aggregations: aggregations,
		DT:           dt,
	}

	// Go through all aggregations and check for any subquery.
	sqc := &SubQueryBuilder{}
	for idx, aggr := range aggregations {
		aggregations[idx] = pullOutValueSubqueries(ctx, aggr, sqc, TableID(src))
	}
	aggrOp.Source = sqc.getRootOperator(src, nil)

	// create the projection columns from aggregator.
	if complexAggr {
		return createProjectionForComplexAggregation(aggrOp, qp)
	}
	return createProjectionForSimpleAggregation(ctx, aggrOp, qp)
}

func pullOutValueSubqueries(ctx *plancontext.PlanningContext, aggr Aggr, sqc *SubQueryBuilder, outerID semantics.TableSet) Aggr {
	exprs := aggr.getPushColumnExprs()
	var newExprs sqlparser.Exprs
	for _, expr := range exprs {
		newExpr, subqs := sqc.pullOutValueSubqueries(ctx, expr, outerID, false)
		if newExpr != nil {
			newExprs = append(newExprs, newExpr)
			aggr.SubQueryExpression = append(aggr.SubQueryExpression, subqs...)
		} else {
			newExprs = append(newExprs, expr)
		}
	}
	if len(aggr.SubQueryExpression) > 0 {
		aggr.setPushColumn(newExprs)
	}

	return aggr
}

func createProjectionForSimpleAggregation(ctx *plancontext.PlanningContext, a *Aggregator, qp *QueryProjection) Operator {
outer:
	for colIdx, expr := range qp.SelectExprs {
		ae, err := expr.GetAliasedExpr()
		if err != nil {
			panic(err)
		}
		addedToCol := false
		for idx, groupBy := range a.Grouping {
			if ctx.SemTable.EqualsExprWithDeps(groupBy.Inner, ae.Expr) {
				if !addedToCol {
					a.Columns = append(a.Columns, ae)
					addedToCol = true
				}
				if groupBy.ColOffset < 0 {
					a.Grouping[idx].ColOffset = colIdx
				}
			}
		}
		if addedToCol {
			continue
		}
		for idx, aggr := range a.Aggregations {
			if ctx.SemTable.EqualsExprWithDeps(aggr.Original.Expr, ae.Expr) && aggr.ColOffset < 0 {
				a.Columns = append(a.Columns, ae)
				a.Aggregations[idx].ColOffset = colIdx
				continue outer
			}
		}
		panic(vterrors.VT13001(fmt.Sprintf("Could not find the %s in aggregation in the original query", sqlparser.String(ae))))
	}
	return a
}

func createProjectionForComplexAggregation(a *Aggregator, qp *QueryProjection) Operator {
	p := newAliasedProjection(a)
	p.DT = a.DT
	for _, expr := range qp.SelectExprs {
		ae, err := expr.GetAliasedExpr()
		if err != nil {
			panic(err)
		}

		p.addProjExpr(newProjExpr(ae))
	}
	for i, by := range a.Grouping {
		a.Grouping[i].ColOffset = len(a.Columns)
		a.Columns = append(a.Columns, aeWrap(by.Inner))
	}
	for i, aggregation := range a.Aggregations {
		a.Aggregations[i].ColOffset = len(a.Columns)
		a.Columns = append(a.Columns, aggregation.Original)
	}
	return p
}

func createProjectionWithoutAggr(ctx *plancontext.PlanningContext, qp *QueryProjection, src Operator) *Projection {
	// first we need to check if we have all columns or there are still unexpanded stars
	aes, err := slice.MapWithError(qp.SelectExprs, func(from SelectExpr) (*sqlparser.AliasedExpr, error) {
		ae, ok := from.Col.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("star found")
		}
		return ae, nil
	})

	if err != nil {
		// if we have unexpanded expressions, we take this shortcut and hope we don't need any offsets from this plan
		return newStarProjection(src, qp)
	}

	proj := newAliasedProjection(nil)
	sqc := &SubQueryBuilder{}
	outerID := TableID(src)
	for _, ae := range aes {
		org := ctx.SemTable.Clone(ae).(*sqlparser.AliasedExpr)
		expr := ae.Expr
		newExpr, subqs := sqc.pullOutValueSubqueries(ctx, expr, outerID, false)
		if newExpr == nil {
			// there was no subquery in this expression
			proj.addUnexploredExpr(org, expr)
		} else {
			proj.addSubqueryExpr(ctx, org, newExpr, subqs...)
		}
	}
	proj.Source = sqc.getRootOperator(src, nil)
	return proj
}

func newStarProjection(src Operator, qp *QueryProjection) *Projection {
	cols := sqlparser.SelectExprs{}

	for _, expr := range qp.SelectExprs {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			_, isSubQ := node.(*sqlparser.Subquery)
			if !isSubQ {
				return true, nil
			}
			panic(vterrors.VT09015())
		}, expr.Col)
		cols = append(cols, expr.Col)
	}

	return &Projection{
		Source:  src,
		Columns: StarProjections(cols),
	}
}
