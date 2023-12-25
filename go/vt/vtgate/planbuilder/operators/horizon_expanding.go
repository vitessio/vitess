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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func expandHorizon(ctx *plancontext.PlanningContext, horizon *Horizon) (ops.Operator, *rewrite.ApplyResult, error) {
	statement := horizon.selectStatement()
	switch sel := statement.(type) {
	case *sqlparser.Select:
		return expandSelectHorizon(ctx, horizon, sel)
	case *sqlparser.Union:
		return expandUnionHorizon(ctx, horizon, sel)
	}
	return nil, nil, vterrors.VT13001(fmt.Sprintf("unexpected statement type %T", statement))
}

func expandUnionHorizon(ctx *plancontext.PlanningContext, horizon *Horizon, union *sqlparser.Union) (ops.Operator, *rewrite.ApplyResult, error) {
	op := horizon.Source

	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

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
		return op, rewrite.NewTree("removed UNION horizon not used", op), nil
	}

	return op, rewrite.NewTree("expand UNION horizon into smaller components", op), nil
}

func expandSelectHorizon(ctx *plancontext.PlanningContext, horizon *Horizon, sel *sqlparser.Select) (ops.Operator, *rewrite.ApplyResult, error) {
	op, err := createProjectionFromSelect(ctx, horizon)
	if err != nil {
		return nil, nil, err
	}

	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

	var extracted []string
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
		op, err = addWherePredicates(ctx, sel.Having.Expr, op)
		if err != nil {
			return nil, nil, err
		}
		extracted = append(extracted, "Filter")
	}

	if len(qp.OrderExprs) > 0 {
		op = &Ordering{
			Source: op,
			Order:  qp.OrderExprs,
		}
		extracted = append(extracted, "Ordering")
	}

	if sel.Limit != nil {
		op = &Limit{
			Source: op,
			AST:    sel.Limit,
		}
		extracted = append(extracted, "Limit")
	}

	return op, rewrite.NewTree(fmt.Sprintf("expand SELECT horizon into (%s)", strings.Join(extracted, ", ")), op), nil
}

func createProjectionFromSelect(ctx *plancontext.PlanningContext, horizon *Horizon) (out ops.Operator, err error) {
	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, err
	}

	var dt *DerivedTable
	if horizon.TableId != nil {
		dt = &DerivedTable{
			TableID: *horizon.TableId,
			Alias:   horizon.Alias,
			Columns: horizon.ColumnAliases,
		}
	}

	if !qp.NeedsAggregation() {
		projX, err := createProjectionWithoutAggr(ctx, qp, horizon.src())
		if err != nil {
			return nil, err
		}
		projX.DT = dt
		out = projX

		return out, nil
	}

	aggregations, complexAggr, err := qp.AggregationExpressions(ctx, true)
	if err != nil {
		return nil, err
	}

	src := horizon.src()
	a := &Aggregator{
		Source:       src,
		Original:     true,
		QP:           qp,
		Grouping:     qp.GetGrouping(),
		Aggregations: aggregations,
		DT:           dt,
	}

	sqc := &SubQueryBuilder{}
	outerID := TableID(src)
	for idx, aggr := range aggregations {
		expr := aggr.Original.Expr
		newExpr, subqs, err := sqc.pullOutValueSubqueries(ctx, expr, outerID, false)
		if err != nil {
			return nil, err
		}
		if newExpr != nil {
			aggregations[idx].SubQueryExpression = subqs
		}
	}
	a.Source = sqc.getRootOperator(src)

	if complexAggr {
		return createProjectionForComplexAggregation(a, qp)
	}
	return createProjectionForSimpleAggregation(ctx, a, qp)
}

func createProjectionForSimpleAggregation(ctx *plancontext.PlanningContext, a *Aggregator, qp *QueryProjection) (ops.Operator, error) {
outer:
	for colIdx, expr := range qp.SelectExprs {
		ae, err := expr.GetAliasedExpr()
		if err != nil {
			return nil, err
		}
		addedToCol := false
		for idx, groupBy := range a.Grouping {
			if ctx.SemTable.EqualsExprWithDeps(groupBy.SimplifiedExpr, ae.Expr) {
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
		return nil, vterrors.VT13001(fmt.Sprintf("Could not find the %s in aggregation in the original query", sqlparser.String(ae)))
	}
	return a, nil
}

func createProjectionForComplexAggregation(a *Aggregator, qp *QueryProjection) (ops.Operator, error) {
	p := newAliasedProjection(a)
	p.DT = a.DT
	for _, expr := range qp.SelectExprs {
		ae, err := expr.GetAliasedExpr()
		if err != nil {
			return nil, err
		}

		_, err = p.addProjExpr(newProjExpr(ae))
		if err != nil {
			return nil, err
		}
	}
	for i, by := range a.Grouping {
		a.Grouping[i].ColOffset = len(a.Columns)
		a.Columns = append(a.Columns, aeWrap(by.SimplifiedExpr))
	}
	for i, aggregation := range a.Aggregations {
		a.Aggregations[i].ColOffset = len(a.Columns)
		a.Columns = append(a.Columns, aggregation.Original)
	}
	return p, nil
}

func createProjectionWithoutAggr(ctx *plancontext.PlanningContext, qp *QueryProjection, src ops.Operator) (*Projection, error) {
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
		org := sqlparser.CloneRefOfAliasedExpr(ae)
		expr := ae.Expr
		newExpr, subqs, err := sqc.pullOutValueSubqueries(ctx, expr, outerID, false)
		if err != nil {
			return nil, err
		}
		if newExpr == nil {
			// there was no subquery in this expression
			_, err := proj.addUnexploredExpr(org, expr)
			if err != nil {
				return nil, err
			}
		} else {
			err := proj.addSubqueryExpr(org, newExpr, subqs...)
			if err != nil {
				return nil, err
			}
		}
	}
	proj.Source = sqc.getRootOperator(src)
	return proj, nil
}

func newStarProjection(src ops.Operator, qp *QueryProjection) (*Projection, error) {
	cols := sqlparser.SelectExprs{}

	for _, expr := range qp.SelectExprs {
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			_, isSubQ := node.(*sqlparser.Subquery)
			if !isSubQ {
				return true, nil
			}
			return false, vterrors.VT09015()
		}, expr.Col)
		if err != nil {
			return nil, err
		}
		cols = append(cols, expr.Col)
	}

	return &Projection{
		Source:  src,
		Columns: StarProjections(cols),
	}, nil
}
