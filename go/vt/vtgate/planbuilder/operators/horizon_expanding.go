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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func expandHorizon(ctx *plancontext.PlanningContext, horizon horizonLike) (ops.Operator, *rewrite.ApplyResult, error) {
	sel, isSel := horizon.selectStatement().(*sqlparser.Select)
	if !isSel {
		return nil, nil, errHorizonNotPlanned()
	}

	op, err := createProjectionFromSelect(ctx, horizon)
	if err != nil {
		return nil, nil, err
	}

	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

	if qp.NeedsDistinct() {
		op = &Distinct{
			Source: op,
			QP:     qp,
		}
	}

	if sel.Having != nil {
		op = &Filter{
			Source:         op,
			Predicates:     sqlparser.SplitAndExpression(nil, sel.Having.Expr),
			FinalPredicate: nil,
		}
	}

	if len(qp.OrderExprs) > 0 {
		op = &Ordering{
			Source: op,
			Order:  qp.OrderExprs,
		}
	}

	if sel.Limit != nil {
		op = &Limit{
			Source: op,
			AST:    sel.Limit,
		}
	}

	return op, rewrite.NewTree("expand horizon into smaller components", op), nil
}

func checkInvalid(aggregations []Aggr, horizon horizonLike) error {
	for _, aggregation := range aggregations {
		if aggregation.Distinct {
			return errHorizonNotPlanned()
		}
	}
	if _, isDerived := horizon.(*Derived); isDerived {
		return errHorizonNotPlanned()
	}
	return nil
}

func createProjectionFromSelect(ctx *plancontext.PlanningContext, horizon horizonLike) (out ops.Operator, err error) {
	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, err
	}

	if !qp.NeedsAggregation() {
		projX, err := createProjectionWithoutAggr(qp, horizon.src())
		if err != nil {
			return nil, err
		}
		if derived, isDerived := horizon.(*Derived); isDerived {
			id := derived.TableId
			projX.TableID = &id
			projX.Alias = derived.Alias
		}
		out = projX

		return out, nil
	}

	err = checkAggregationSupported(horizon)
	if err != nil {
		return nil, err
	}

	aggregations, complexAggr, err := qp.AggregationExpressions(ctx, true)
	if err != nil {
		return nil, err
	}

	if err := checkInvalid(aggregations, horizon); err != nil {
		return nil, err
	}

	a := &Aggregator{
		Source:       horizon.src(),
		Original:     true,
		QP:           qp,
		Grouping:     qp.GetGrouping(),
		Aggregations: aggregations,
	}

	if derived, isDerived := horizon.(*Derived); isDerived {
		id := derived.TableId
		a.TableID = &id
		a.Alias = derived.Alias
	}

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
	p := &Projection{
		Source:  a,
		Alias:   a.Alias,
		TableID: a.TableID,
	}

	for _, expr := range qp.SelectExprs {
		ae, err := expr.GetAliasedExpr()
		if err != nil {
			return nil, err
		}
		p.Columns = append(p.Columns, ae)
		p.Projections = append(p.Projections, UnexploredExpression{E: ae.Expr})
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

func createProjectionWithoutAggr(qp *QueryProjection, src ops.Operator) (*Projection, error) {
	proj := &Projection{
		Source: src,
	}

	for _, e := range qp.SelectExprs {
		if _, isStar := e.Col.(*sqlparser.StarExpr); isStar {
			return nil, errHorizonNotPlanned()
		}
		ae, err := e.GetAliasedExpr()

		if err != nil {
			return nil, err
		}
		expr := ae.Expr
		if sqlparser.ContainsAggregation(expr) {
			aggr, ok := expr.(sqlparser.AggrFunc)
			if !ok {
				// need to add logic to extract aggregations and pushed them to the top level
				return nil, errHorizonNotPlanned()
			}
			expr = aggr.GetArg()
			if expr == nil {
				expr = sqlparser.NewIntLiteral("1")
			}
		}

		proj.addUnexploredExpr(ae, expr)
	}
	return proj, nil
}
