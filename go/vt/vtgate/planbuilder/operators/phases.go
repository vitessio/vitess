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
	"io"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	Phase int
)

const (
	physicalTransform Phase = iota
	initialPlanning
	pullDistinctFromUnion
	delegateAggregation
	recursiveCTEHorizons
	addAggrOrdering
	cleanOutPerfDistinct
	dmlWithInput
	subquerySettling
	DONE
)

func (p Phase) String() string {
	switch p {
	case physicalTransform:
		return "physicalTransform"
	case initialPlanning:
		return "initial horizon planning optimization"
	case pullDistinctFromUnion:
		return "pull distinct from UNION"
	case delegateAggregation:
		return "split aggregation between vtgate and mysql"
	case recursiveCTEHorizons:
		return "expand recursive CTE horizons"
	case addAggrOrdering:
		return "optimize aggregations with ORDER BY"
	case cleanOutPerfDistinct:
		return "optimize Distinct operations"
	case subquerySettling:
		return "settle subqueries"
	case dmlWithInput:
		return "expand update/delete to dml with input"
	default:
		panic(vterrors.VT13001("unhandled default case"))
	}
}

func (p Phase) shouldRun(s semantics.QuerySignature) bool {
	switch p {
	case pullDistinctFromUnion:
		return s.Union
	case delegateAggregation:
		return s.Aggregation
	case recursiveCTEHorizons:
		return s.RecursiveCTE
	case addAggrOrdering:
		return s.Aggregation
	case cleanOutPerfDistinct:
		return s.Distinct
	case subquerySettling:
		return s.SubQueries
	case dmlWithInput:
		return s.DML
	default:
		return true
	}
}

func (p Phase) act(ctx *plancontext.PlanningContext, op Operator) Operator {
	switch p {
	case pullDistinctFromUnion:
		return isolateDistinctFromUnion(ctx, op)
	case delegateAggregation:
		return enableDelegateAggregation(ctx, op)
	case addAggrOrdering:
		return addOrderingForAllAggregations(ctx, op)
	case recursiveCTEHorizons:
		return planRecursiveCTEHorizons(ctx, op)
	case cleanOutPerfDistinct:
		return removePerformanceDistinctAboveRoute(ctx, op)
	case subquerySettling:
		return settleSubqueries(ctx, op)
	case dmlWithInput:
		return findDMLAboveRoute(ctx, op)
	default:
		return op
	}
}

type phaser struct {
	current Phase
}

func (p *phaser) next(ctx *plancontext.PlanningContext) Phase {
	for {
		curr := p.current
		if curr == DONE {
			return DONE
		}

		p.current++

		if curr.shouldRun(ctx.SemTable.QuerySignature) {
			return curr
		}
	}
}

func findDMLAboveRoute(ctx *plancontext.PlanningContext, root Operator) Operator {
	visitor := func(in Operator, _ semantics.TableSet, isRoot bool) (Operator, *ApplyResult) {
		switch op := in.(type) {
		case *Delete:
			return createDMLWithInput(ctx, op, op.Source, op.DMLCommon)
		case *Update:
			return createDMLWithInput(ctx, op, op.Source, op.DMLCommon)
		}
		return in, NoRewrite
	}

	return BottomUp(root, TableID, visitor, stopAtRoute)
}

func createDMLWithInput(ctx *plancontext.PlanningContext, op, src Operator, in *DMLCommon) (Operator, *ApplyResult) {
	if len(in.Target.VTable.PrimaryKey) == 0 {
		panic(vterrors.VT09015())
	}
	dm := &DMLWithInput{}
	var leftComp sqlparser.ValTuple
	proj := newAliasedProjection(src)
	dm.cols = make([][]*sqlparser.ColName, 1)
	for _, col := range in.Target.VTable.PrimaryKey {
		colName := sqlparser.NewColNameWithQualifier(col.String(), in.Target.Name)
		ctx.SemTable.Recursive[colName] = in.Target.ID
		proj.AddColumn(ctx, true, false, aeWrap(colName))
		dm.cols[0] = append(dm.cols[0], colName)
		leftComp = append(leftComp, colName)
	}

	dm.Source = proj

	var targetTable *Table
	_ = Visit(src, func(operator Operator) error {
		if tbl, ok := operator.(*Table); ok && tbl.QTable.ID == in.Target.ID {
			targetTable = tbl
			return io.EOF
		}
		return nil
	})
	if targetTable == nil {
		panic(vterrors.VT13001("target DELETE table not found"))
	}

	// optimize for case when there is only single column on left hand side.
	var lhs sqlparser.Expr = leftComp
	if len(leftComp) == 1 {
		lhs = leftComp[0]
	}
	compExpr := sqlparser.NewComparisonExpr(sqlparser.InOp, lhs, sqlparser.ListArg(engine.DmlVals), nil)
	targetQT := targetTable.QTable
	qt := &QueryTable{
		ID:         targetQT.ID,
		Alias:      sqlparser.Clone(targetQT.Alias),
		Table:      sqlparser.Clone(targetQT.Table),
		Predicates: []sqlparser.Expr{compExpr},
	}

	qg := &QueryGraph{Tables: []*QueryTable{qt}}
	in.Source = qg

	if in.OwnedVindexQuery != nil {
		in.OwnedVindexQuery.From = sqlparser.TableExprs{targetQT.Alias}
		in.OwnedVindexQuery.Where = sqlparser.NewWhere(sqlparser.WhereClause, compExpr)
		in.OwnedVindexQuery.OrderBy = nil
		in.OwnedVindexQuery.Limit = nil
	}
	dm.DML = append(dm.DML, op)

	return dm, Rewrote("changed Delete to DMLWithInput")
}

func removePerformanceDistinctAboveRoute(_ *plancontext.PlanningContext, op Operator) Operator {
	return BottomUp(op, TableID, func(innerOp Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		d, ok := innerOp.(*Distinct)
		if !ok || d.Required {
			return innerOp, NoRewrite
		}

		return d.Source, Rewrote("removed distinct not required that was not pushed under route")
	}, stopAtRoute)
}

func enableDelegateAggregation(ctx *plancontext.PlanningContext, op Operator) Operator {
	return prepareForAggregationPushing(ctx, op)
}

// addOrderingForAllAggregations is run we have pushed down Aggregators as far down as possible.
func addOrderingForAllAggregations(ctx *plancontext.PlanningContext, root Operator) Operator {
	visitor := func(in Operator, _ semantics.TableSet, isRoot bool) (Operator, *ApplyResult) {
		aggrOp, ok := in.(*Aggregator)
		if !ok {
			return in, NoRewrite
		}

		requireOrdering := needsOrdering(ctx, aggrOp)
		var res *ApplyResult
		if requireOrdering {
			addOrderingFor(aggrOp)
			res = Rewrote("added ordering before aggregation")
		}
		return in, res
	}

	return BottomUp(root, TableID, visitor, stopAtRoute)
}

func addOrderingFor(aggrOp *Aggregator) {
	orderBys := slice.Map(aggrOp.Grouping, func(from GroupBy) OrderBy {
		return from.AsOrderBy()
	})
	if aggrOp.DistinctExpr != nil {
		orderBys = append(orderBys, OrderBy{
			Inner: &sqlparser.Order{
				Expr: aggrOp.DistinctExpr,
			},
			SimplifiedExpr: aggrOp.DistinctExpr,
		})
	}
	aggrOp.Source = &Ordering{
		Source: aggrOp.Source,
		Order:  orderBys,
	}
}

func needsOrdering(ctx *plancontext.PlanningContext, in *Aggregator) bool {
	requiredOrder := slice.Map(in.Grouping, func(from GroupBy) sqlparser.Expr {
		return from.Inner
	})
	if in.DistinctExpr != nil {
		requiredOrder = append(requiredOrder, in.DistinctExpr)
	}
	if len(requiredOrder) == 0 {
		return false
	}
	srcOrdering := in.Source.GetOrdering(ctx)
	if len(srcOrdering) < len(requiredOrder) {
		return true
	}
	for idx, gb := range requiredOrder {
		if !ctx.SemTable.EqualsExprWithDeps(srcOrdering[idx].SimplifiedExpr, gb) {
			return true
		}
	}
	return false
}

func addGroupByOnRHSOfJoin(root Operator) Operator {
	visitor := func(in Operator, _ semantics.TableSet, isRoot bool) (Operator, *ApplyResult) {
		join, ok := in.(*ApplyJoin)
		if !ok {
			return in, NoRewrite
		}

		return addLiteralGroupingToRHS(join)
	}

	return TopDown(root, TableID, visitor, stopAtRoute)
}

func addLiteralGroupingToRHS(in *ApplyJoin) (Operator, *ApplyResult) {
	_ = Visit(in.RHS, func(op Operator) error {
		aggr, isAggr := op.(*Aggregator)
		if !isAggr {
			return nil
		}
		if len(aggr.Grouping) == 0 {
			gb := sqlparser.NewFloatLiteral(".0")
			aggr.Grouping = append(aggr.Grouping, NewGroupBy(gb))
		}
		return nil
	})
	return in, NoRewrite
}

// prepareForAggregationPushing adds columns needed by an operator to its input.
// This happens only when the filter expression can be retrieved as an offset from the underlying mysql.
func prepareForAggregationPushing(ctx *plancontext.PlanningContext, root Operator) Operator {
	return TopDown(root, TableID, func(in Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		filter, ok := in.(*Filter)
		if !ok {
			return in, NoRewrite
		}

		var neededAggrs []sqlparser.Expr
		extractAggrs := func(cursor *sqlparser.CopyOnWriteCursor) {
			node := cursor.Node()
			if ctx.IsAggr(node) {
				neededAggrs = append(neededAggrs, node.(sqlparser.Expr))
			}
		}

		for _, expr := range filter.Predicates {
			_ = sqlparser.CopyOnRewrite(expr, dontEnterSubqueries, extractAggrs, nil)
		}

		if neededAggrs == nil {
			return in, NoRewrite
		}

		addedCols := false
		aggregator := findAggregatorInSource(filter.Source)
		for _, aggr := range neededAggrs {
			if aggregator.FindCol(ctx, aggr, false) == -1 {
				aggregator.addColumnWithoutPushing(ctx, aeWrap(aggr), false)
				addedCols = true
			}
		}

		if addedCols {
			return in, Rewrote("added columns because filter needs it")
		}
		return in, NoRewrite
	}, stopAtRoute)
}

// prepareForAggregationPushing adds columns needed by an operator to its input.
// This happens only when the filter expression can be retrieved as an offset from the underlying mysql.
func planRecursiveCTEHorizons(ctx *plancontext.PlanningContext, root Operator) Operator {
	return TopDown(root, TableID, func(in Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		// These recursive CTEs have not been pushed under a route, so we will have to evaluate it one the vtgate
		// That means that we need to turn anything that is coming from the recursion into arguments
		rcte, ok := in.(*RecurseCTE)
		if !ok {
			return in, NoRewrite
		}
		hz := rcte.Horizon
		hz.Source = rcte.Term
		newTerm, _ := expandHorizon(ctx, hz)
		pr := findProjection(newTerm)
		ap, err := pr.GetAliasedProjections()
		if err != nil {
			panic(vterrors.VT09015())
		}

		// We need to break the expressions into LHS and RHS, and store them in the CTE for later use
		projections := slice.Map(ap, func(p *ProjExpr) *plancontext.RecurseExpression {
			recurseExpression := breakCTEExpressionInLhsAndRhs(ctx, p.EvalExpr, rcte.LeftID)
			p.EvalExpr = recurseExpression.RightExpr
			return recurseExpression
		})
		rcte.Projections = projections
		rcte.Term = newTerm
		return rcte, Rewrote("expanded horizon on term side of recursive CTE")
	}, stopAtRoute)
}

func findProjection(op Operator) *Projection {
	for {
		proj, ok := op.(*Projection)
		if ok {
			return proj
		}
		inputs := op.Inputs()
		if len(inputs) != 1 {
			panic(vterrors.VT13001("unexpected multiple inputs"))
		}
		src := inputs[0]
		_, isRoute := src.(*Route)
		if isRoute {
			panic(vterrors.VT13001("failed to find the projection"))
		}
		op = src
	}
}
