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

func errHorizonNotPlanned() error {
	return _errHorizonNotPlanned
}

var _errHorizonNotPlanned = vterrors.VT12001("query cannot be fully operator planned")

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
			op, err := pushOrExpandHorizon(ctx, in)
			if err != nil {
				return nil, false, err
			}
			return op, rewrite.NewTree, err
		case *Projection:
			return tryPushingDownProjection(ctx, in)
		case *Limit:
			return tryPushingDownLimit(in)
		case *Ordering:
			return tryPushingDownOrdering(ctx, in)
		default:
			return in, rewrite.SameTree, nil
		}
	}

	newOp, err := rewrite.FixedPointBottomUp(root, TableID, visitor, stopAtRoute)
	if err != nil {
		if vterr, ok := err.(*vterrors.VitessError); ok && vterr.ID == "VT13001" {
			// we encountered a bug. let's try to back out
			return nil, errHorizonNotPlanned()
		}
		return nil, err
	}

	return newOp, nil
}

func tryPushingDownOrdering(ctx *plancontext.PlanningContext, in *Ordering) (ops.Operator, rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *Route:
		return swap(in, src)
	case *ApplyJoin:
		if canPushLeft(ctx, src, in.Order) {
			// ApplyJoin is stable in regard to the columns coming from the LHS,
			// so if all the ordering columns come from the LHS, we can push down the Ordering there
			src.LHS, in.Source = in, src.LHS
			return src, rewrite.NewTree, nil
		}
	}
	return in, rewrite.SameTree, nil
}

func canPushLeft(ctx *plancontext.PlanningContext, aj *ApplyJoin, order []ops.OrderBy) bool {
	lhs := TableID(aj.LHS)
	for _, order := range order {
		deps := ctx.SemTable.DirectDeps(order.Inner.Expr)
		if !deps.IsSolvedBy(lhs) {
			return false
		}
	}
	return true
}

func tryPushingDownProjection(
	ctx *plancontext.PlanningContext,
	p *Projection,
) (ops.Operator, rewrite.ApplyResult, error) {
	switch src := p.Source.(type) {
	case *Route:
		return swap(p, src)
	case *ApplyJoin:
		return pushDownProjectionInApplyJoin(ctx, p, src)
	case *Vindex:
		return pushDownProjectionInVindex(ctx, p, src)
	default:
		return p, rewrite.SameTree, nil
	}
}

func swap(a, b ops.Operator) (ops.Operator, rewrite.ApplyResult, error) {
	op, err := rewrite.Swap(a, b)
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

type projector struct {
	cols  []ProjExpr
	names []string
}

func (p *projector) add(e ProjExpr, alias string) {
	p.cols = append(p.cols, e)
	p.names = append(p.names, alias)
}

// pushDownProjectionInApplyJoin pushes down a projection operation into an ApplyJoin operation.
// It processes each input column and creates new JoinColumns for the ApplyJoin operation based on
// the input column's expression. It also creates new Projection operators for the left and right
// children of the ApplyJoin operation, if needed.
func pushDownProjectionInApplyJoin(
	ctx *plancontext.PlanningContext,
	p *Projection,
	src *ApplyJoin,
) (ops.Operator, rewrite.ApplyResult, error) {
	lhs, rhs := &projector{}, &projector{}

	src.ColumnsAST = nil
	// Iterate through each column in the Projection's columns.
	for idx := 0; idx < len(p.Columns); idx++ {
		in := p.Columns[idx]
		expr := in.GetExpr()

		// Check if the current expression can reuse an existing column in the ApplyJoin.
		if _, found := canReuseColumn(ctx, src.ColumnsAST, expr, jcToAliasedExpr); found {
			continue
		}

		// Get a JoinColumn for the current expression.
		colName := p.ColumnNames[idx]
		col, err := src.getJoinColumnFor(ctx, &sqlparser.AliasedExpr{Expr: expr, As: sqlparser.NewIdentifierCI(colName)})
		if err != nil {
			return nil, false, err
		}

		// Update the left and right child columns and names based on the JoinColumn type.
		switch {
		case col.IsPureLeft():
			lhs.add(in, colName)
		case col.IsPureRight():
			rhs.add(in, colName)
		case col.IsMixedLeftAndRight():
			for _, lhsExpr := range col.LHSExprs {
				lhs.add(&Expr{E: lhsExpr}, "")
			}
			rhs.add(&Expr{E: col.RHSExpr}, colName)
		}

		// Add the new JoinColumn to the ApplyJoin's ColumnsAST.
		src.ColumnsAST = append(src.ColumnsAST, col)
	}

	var err error

	// Create and update the Projection operators for the left and right children, if needed.
	src.LHS, err = createProjectionWithTheseColumns(src.LHS, lhs, p.TableID, p.Alias)
	if err != nil {
		return nil, false, err
	}

	src.RHS, err = createProjectionWithTheseColumns(src.RHS, rhs, p.TableID, p.Alias)
	if err != nil {
		return nil, false, err
	}

	return src, rewrite.NewTree, nil
}

func createProjectionWithTheseColumns(
	src ops.Operator,
	p *projector,
	tableID *semantics.TableSet,
	alias string,
) (ops.Operator, error) {
	if len(p.cols) == 0 {
		return src, nil
	}
	proj, err := createProjection(src)
	if err != nil {
		return nil, err
	}
	proj.ColumnNames = p.names
	proj.Columns = p.cols
	proj.TableID = tableID
	proj.Alias = alias
	return proj, nil
}

func stopAtRoute(operator ops.Operator) rewrite.VisitRule {
	_, isRoute := operator.(*Route)
	return rewrite.VisitRule(!isRoute)
}

func tryPushingDownLimit(in *Limit) (ops.Operator, rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *Route:
		return tryPushingDownLimitInRoute(in, src)
	case *Projection:
		return swap(in, src)
	default:
		if in.Pushed {
			return in, rewrite.SameTree, nil
		}
		return setUpperLimit(in)
	}
}

func setUpperLimit(in *Limit) (ops.Operator, rewrite.ApplyResult, error) {
	visitor := func(op ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, rewrite.ApplyResult, error) {
		return op, rewrite.SameTree, nil
	}
	shouldVisit := func(op ops.Operator) rewrite.VisitRule {
		switch op := op.(type) {
		case *Join, *ApplyJoin:
			// we can't push limits down on either side
			return rewrite.SkipChildren
		case *Route:
			newSrc := &Limit{
				Source: op.Source,
				AST:    &sqlparser.Limit{Rowcount: sqlparser.NewArgument("__upper_limit")},
				Pushed: false,
			}
			op.Source = newSrc
			return rewrite.SkipChildren
		default:
			return rewrite.VisitChildren
		}
	}

	_, err := rewrite.TopDown(in.Source, TableID, visitor, shouldVisit)
	if err != nil {
		return nil, false, err
	}
	return in, rewrite.SameTree, nil
}

func tryPushingDownLimitInRoute(in *Limit, src *Route) (ops.Operator, rewrite.ApplyResult, error) {
	if src.IsSingleShard() {
		return swap(in, src)
	}

	return setUpperLimit(in)
}

func pushOrExpandHorizon(ctx *plancontext.PlanningContext, in horizonLike) (ops.Operator, error) {
	if derived, ok := in.(*Derived); ok {
		if len(derived.ColumnAliases) > 0 {
			return nil, errHorizonNotPlanned()
		}
	}
	rb, isRoute := in.src().(*Route)
	if isRoute && rb.IsSingleShard() {
		return rewrite.Swap(in, rb)
	}

	sel, isSel := in.selectStatement().(*sqlparser.Select)
	if !isSel {
		return nil, errHorizonNotPlanned()
	}

	qp, err := in.getQP(ctx)
	if err != nil {
		return nil, err
	}

	needsOrdering := len(qp.OrderExprs) > 0
	canPushDown := isRoute && sel.Having == nil && !needsOrdering && !qp.NeedsAggregation() && !sel.Distinct && sel.Limit == nil

	if canPushDown {
		return rewrite.Swap(in, rb)
	}

	if _, ok := in.(*Derived); ok {
		// we're still not ready if we have to expand the derived table horizon
		return nil, errHorizonNotPlanned()
	}

	return expandHorizon(ctx, in)
}

// horizonLike should be removed. we should use Horizon for both these cases
type horizonLike interface {
	ops.Operator
	selectStatement() sqlparser.SelectStatement
	src() ops.Operator
	getQP(ctx *plancontext.PlanningContext) (*QueryProjection, error)
}

func expandHorizon(ctx *plancontext.PlanningContext, horizon horizonLike) (ops.Operator, error) {
	sel, isSel := horizon.selectStatement().(*sqlparser.Select)
	if !isSel {
		return nil, errHorizonNotPlanned()
	}
	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, err
	}

	src := horizon.src()

	if qp.NeedsAggregation() || sel.Having != nil || qp.NeedsDistinct() || sel.Distinct {
		return nil, errHorizonNotPlanned()
	}

	var op ops.Operator
	proj, err := createProjectionFromSelect(src, qp.SelectExprs)
	if err != nil {
		return nil, err
	}
	if derived, isDerived := horizon.(*Derived); isDerived {
		id := derived.TableId
		proj.TableID = &id
	}
	op = proj

	if qp.OrderExprs != nil {
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

	return op, nil
}

func createProjectionFromSelect(src ops.Operator, selectExprs []SelectExpr) (*Projection, error) {
	proj := &Projection{
		Source: src,
	}

	for _, e := range selectExprs {
		if _, isStar := e.Col.(*sqlparser.StarExpr); isStar {
			return nil, errHorizonNotPlanned()
		}
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
