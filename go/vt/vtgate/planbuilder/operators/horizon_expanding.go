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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
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
		op = &Projection{
			Source:  op,
			TableID: horizon.TableId,
			Alias:   horizon.Alias,
		}
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

	extracted := []string{"Projection"}

	if qp.NeedsDistinct() {
		op = &Distinct{
			Required: true,
			Source:   op,
			QP:       qp,
		}
		extracted = append(extracted, "Distinct")
	}

	if sel.Having != nil {
		op = &Filter{
			Source:               op,
			Predicates:           sqlparser.SplitAndExpression(nil, sel.Having.Expr),
			PredicateWithOffsets: nil,
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

	if !qp.NeedsAggregation() {
		projX, err := createProjectionWithoutAggr(ctx, qp, horizon.src())
		if err != nil {
			return nil, err
		}
		projX.TableID = horizon.TableId
		projX.Alias = horizon.Alias
		out = projX

		return out, nil
	}

	aggregations, complexAggr, err := qp.AggregationExpressions(ctx, true)
	if err != nil {
		return nil, err
	}

	a := &Aggregator{
		Source:       horizon.src(),
		Original:     true,
		QP:           qp,
		Grouping:     qp.GetGrouping(),
		Aggregations: aggregations,
		TableID:      horizon.TableId,
		Alias:        horizon.Alias,
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

func createProjectionWithoutAggr(ctx *plancontext.PlanningContext, qp *QueryProjection, src ops.Operator) (*Projection, error) {
	proj := &Projection{}
	sqc := &SubQueryContainer{}
	outerID := TableID(src)

	for _, e := range qp.SelectExprs {
		if _, isStar := e.Col.(*sqlparser.StarExpr); isStar {
			return nil, errHorizonNotPlanned()
		}
		ae, err := e.GetAliasedExpr()
		if err != nil {
			return nil, err
		}

		expr := ae.Expr
		newExpr, subqs, err := sqc.handleSubqueries(ctx, expr, outerID)
		if err != nil {
			return nil, err
		}
		if newExpr == nil {
			// there was no subquery in this expression
			proj.addUnexploredExpr(ae, expr)
		} else {
			proj.addSubqueryExpr(ae, newExpr, subqs...)
		}
	}
	proj.Source = sqc.getRootOperator(src)
	return proj, nil
}

type subqueryExtraction struct {
	new  sqlparser.Expr
	subq []*sqlparser.Subquery
	cols []*sqlparser.ColName
}

func (sq *SubQueryContainer) handleSubqueries(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	outerID semantics.TableSet,
) (sqlparser.Expr, []*SubQuery, error) {
	original := sqlparser.CloneExpr(expr)
	sqe := extractSubQueries(ctx, expr)
	if sqe == nil {
		return nil, nil, nil
	}
	var newSubqs []*SubQuery

	for idx, subq := range sqe.subq {
		sqInner, err := createSubquery(ctx, original, subq, outerID, nil, sqe.cols[idx], opcode.PulloutValue)
		if err != nil {
			return nil, nil, err
		}
		newSubqs = append(newSubqs, sqInner)
	}

	sq.Inner = append(sq.Inner, newSubqs...)

	return sqe.new, newSubqs, nil
}

func extractSubQueries(ctx *plancontext.PlanningContext, expr sqlparser.Expr) *subqueryExtraction {
	sqe := &subqueryExtraction{}
	sqlparser.Rewrite(expr, nil, func(cursor *sqlparser.Cursor) bool {
		if subq, ok := cursor.Node().(*sqlparser.Subquery); ok {
			reseveSq := ctx.ReservedVars.ReserveSubQuery()
			reserveSqColName := sqlparser.NewColName(reseveSq)
			cursor.Replace(reserveSqColName)
			sqe.subq = append(sqe.subq, subq)
			sqe.cols = append(sqe.cols, reserveSqColName)
		}
		return true
	})
	if len(sqe.subq) == 0 {
		return nil
	}
	sqe.new = expr
	return sqe
}
