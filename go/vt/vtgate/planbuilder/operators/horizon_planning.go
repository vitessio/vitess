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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
)

var errHorizonNotPlanned = vterrors.VT12001("query cannot be fully operator planned")

// planHorizons is the process of figuring out how to perform the operations in the Horizon
// If we can push it under a route - done.
// If we can't, we will instead expand the Horizon into
// smaller operators and try to push these down as far as possible
func planHorizons(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, rewrite.ApplyResult, error) {
		switch in := in.(type) {
		case *Horizon:
			op, err := pushOrExpandHorizon(ctx, in)
			if err != nil {
				return nil, false, err
			}
			return op, rewrite.NewTree, nil
		case *Derived:
			op, err := planDerived(ctx, in)
			if err != nil {
				return nil, false, err
			}
			return op, rewrite.NewTree, err
		case *Projection:
			return tryPushingDownProjection(ctx, in)
		default:
			return in, rewrite.SameTree, nil
		}
	}

	newOp, err := rewrite.FixedPointBottomUp(root, TableID, visitor, stopAtRoute)
	if err != nil {
		if vterr, ok := err.(*vterrors.VitessError); ok && vterr.ID == "VT13001" {
			// we encountered a bug. let's try to back out
			return nil, errHorizonNotPlanned
		}
		return nil, err
	}

	return newOp, nil
}

func tryPushingDownProjection(
	ctx *plancontext.PlanningContext,
	p *Projection,
) (ops.Operator, rewrite.ApplyResult, error) {
	switch src := p.Source.(type) {
	case *Route:
		return pushDownProjectionIntoRoute(p, src)
	case *ApplyJoin:
		return pushDownProjectionInApplyJoin(ctx, p, src)
	case *Vindex:
		return pushDownProjectionInVindex(ctx, p, src)
	default:
		return p, rewrite.SameTree, nil
	}
}

func pushDownProjectionIntoRoute(p *Projection, src ops.Operator) (ops.Operator, rewrite.ApplyResult, error) {
	op, err := rewrite.Swap(p, src)
	if err != nil {
		return nil, false, err
	}
	return op, rewrite.NewTree, nil
}

func pushDownProjectionInVindex(
	ctx *plancontext.PlanningContext,
	p *Projection,
	src *Vindex,
) (ops.Operator, rewrite.ApplyResult, error) {
	for _, column := range p.Columns {
		expr := column.GetExpr()
		_, _, err := src.AddColumn(ctx, aeWrap(expr))
		if err != nil {
			return nil, false, err
		}
	}
	return src, rewrite.NewTree, nil
}

// pushDownProjectionInApplyJoin pushes down a projection operation into an ApplyJoin operation.
// It processes each input column and creates new JoinColumns for the ApplyJoin operation based on
// the input column's expression. It also creates new Projection operators for the left and right
// children of the ApplyJoin operation, if needed.
func pushDownProjectionInApplyJoin(ctx *plancontext.PlanningContext, p *Projection, src *ApplyJoin) (ops.Operator, rewrite.ApplyResult, error) {
	var (
		// idx is the current column index in the Projection's columns.
		// in is the current column's ProjExpr.
		idx int
		in  ProjExpr

		// lhsCols and rhsCols store the columns to be added to the left and right child projections.
		// lhsNames and rhsNames store the corresponding column names.
		lhsCols, rhsCols   []ProjExpr
		lhsNames, rhsNames []string
	)

	// Iterate through each column in the Projection's columns.
	for idx = 0; idx < len(p.Columns); idx++ {
		in = p.Columns[idx]
		expr := in.GetExpr()

		// Check if the current expression can reuse an existing column in the ApplyJoin.
		if _, found := canReuseColumn(ctx, src.ColumnsAST, expr, jcToExpr); found {
			continue
		}

		// Get a JoinColumn for the current expression.
		col, err := src.getJoinColumnFor(ctx, expr)
		if err != nil {
			return nil, false, err
		}

		// Update the left and right child columns and names based on the JoinColumn type.
		switch {
		case col.IsPureLeft():
			lhsCols = append(lhsCols, in)
			lhsNames = append(lhsNames, p.ColumnNames[idx])
		case col.IsPureRight():
			rhsCols = append(rhsCols, in)
			rhsNames = append(rhsNames, p.ColumnNames[idx])
		case col.IsMixedLeftAndRight():
			for _, lhsExpr := range col.LHSExprs {
				lhsCols = append(lhsCols, &Expr{E: lhsExpr})
				lhsNames = append(lhsNames, sqlparser.String(lhsExpr))
			}

			rhsCols = append(rhsCols, &Expr{E: col.RHSExpr})
			rhsNames = append(rhsNames, p.ColumnNames[idx])
		}

		// Add the new JoinColumn to the ApplyJoin's ColumnsAST.
		src.ColumnsAST = append(src.ColumnsAST, col)
	}

	var err error

	// Create and update the Projection operators for the left and right children, if needed.
	src.LHS, err = createProjectionWithTheseColumns(src.LHS, lhsNames, lhsCols)
	if err != nil {
		return nil, false, err
	}

	src.RHS, err = createProjectionWithTheseColumns(src.RHS, rhsNames, rhsCols)
	if err != nil {
		return nil, false, err
	}

	return src, rewrite.NewTree, nil
}

func createProjectionWithTheseColumns(src ops.Operator, colNames []string, columns []ProjExpr) (ops.Operator, error) {
	if len(columns) == 0 {
		return src, nil
	}
	proj, err := createProjection(src)
	if err != nil {
		return nil, err
	}
	proj.ColumnNames = colNames
	proj.Columns = columns
	return proj, nil
}

func stopAtRoute(operator ops.Operator) rewrite.VisitRule {
	_, isRoute := operator.(*Route)
	return rewrite.VisitRule(!isRoute)
}

func pushOrExpandHorizon(ctx *plancontext.PlanningContext, in horizonLike) (ops.Operator, error) {
	rb, isRoute := in.src().(*Route)
	if isRoute && rb.IsSingleShard() && in.selectStatement().GetLimit() == nil {
		return rewrite.Swap(in, rb)
	}

	sel, isSel := in.selectStatement().(*sqlparser.Select)
	if !isSel {
		return nil, errHorizonNotPlanned
	}

	qp, err := CreateQPFromSelect(ctx, sel)
	if err != nil {
		return nil, err
	}

	needsOrdering := len(qp.OrderExprs) > 0
	canPushDown := isRoute && sel.Having == nil && !needsOrdering && !qp.NeedsAggregation() && !sel.Distinct && sel.Limit == nil

	if canPushDown {
		return rewrite.Swap(in, rb)
	}

	return expandHorizon(qp, in)
}

// horizonLike should be removed. we should use Horizon for both these cases
type horizonLike interface {
	ops.Operator
	selectStatement() sqlparser.SelectStatement
	src() ops.Operator
}

func planDerived(ctx *plancontext.PlanningContext, in *Derived) (ops.Operator, error) {
	return pushOrExpandHorizon(ctx, in)
}

func expandHorizon(qp *QueryProjection, horizon horizonLike) (ops.Operator, error) {
	sel, isSel := horizon.selectStatement().(*sqlparser.Select)
	if !isSel {
		return nil, errHorizonNotPlanned
	}

	needsOrdering := len(qp.OrderExprs) > 0
	src := horizon.src()
	_, isDerived := src.(*Derived)

	if qp.NeedsAggregation() || sel.Having != nil || sel.Limit != nil || isDerived || needsOrdering || qp.NeedsDistinct() || sel.Distinct {
		return nil, errHorizonNotPlanned
	}

	proj := &Projection{
		Source: src,
	}

	for _, e := range qp.SelectExprs {
		expr, err := e.GetAliasedExpr()
		if err != nil {
			return nil, err
		}
		proj.Columns = append(proj.Columns, Expr{E: expr.Expr})
		colName := ""
		if !expr.As.IsEmpty() {
			colName = expr.ColumnName()
		}
		proj.ColumnNames = append(proj.ColumnNames, colName)
	}

	return proj, nil
}

func aeWrap(e sqlparser.Expr) *sqlparser.AliasedExpr {
	return &sqlparser.AliasedExpr{Expr: e}
}
