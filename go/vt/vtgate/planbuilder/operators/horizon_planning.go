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
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
)

var errNotHorizonPlanned = vterrors.VT12001("query cannot be fully operator planned")

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
			op, err := planDerived(ctx, in, isRoot)
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
			return nil, errNotHorizonPlanned
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
		op, err := rewrite.Swap(p, src)
		if err != nil {
			return nil, false, err
		}
		return op, rewrite.NewTree, nil
	case *ApplyJoin:
		var (
			// by setting idx, in and expr, and then running src.Push(),
			// we'll get the return data in the remaining variables
			idx int
			in  ProjExpr

			// this is the output of running src.Push
			lhsCols, rhsColumns []ProjExpr
			lhsNames, rhsNames  []string
			out                 JoinColumn
			err                 error
		)
		chooser := chooseSide{
			left: func() error {
				lhsCols = append(lhsCols, in)
				lhsNames = append(lhsNames, p.ColumnNames[idx])
				out = JoinColumn{
					Original: in.GetExpr(),
					LHSExprs: []sqlparser.Expr{in.GetExpr()},
				}
				return nil
			},
			right: func() error {
				rhsColumns = append(rhsColumns, in)
				rhsNames = append(rhsNames, p.ColumnNames[idx])
				out = JoinColumn{
					Original: in.GetExpr(),
					RHSExpr:  in.GetExpr(),
				}
				return nil
			},
			both: func() error {
				out, err = BreakExpressionInLHSandRHS(ctx, in.GetExpr(), TableID(src.LHS))
				if err != nil {
					return err
				}

				for _, lhsExpr := range out.LHSExprs {
					lhsCols = append(lhsCols, &Expr{E: lhsExpr})
					lhsNames = append(lhsNames, sqlparser.String(lhsExpr))
				}

				rhsColumns = append(rhsColumns, &Expr{E: out.RHSExpr})
				rhsNames = append(rhsNames, p.ColumnNames[idx])

				return nil
			},
		}

		for idx = 0; idx < len(p.Columns); idx++ {
			in = p.Columns[idx]
			chooser.Expr = in.GetExpr()
			err := src.Push(ctx, chooser)
			if err != nil {
				return nil, false, err
			}
			src.ColumnsAST = append(src.ColumnsAST, out)
		}

		if len(lhsCols) > 0 {
			lhsProj := NewProjection(src.LHS)
			lhsProj.ColumnNames = lhsNames
			lhsProj.Columns = lhsCols
			src.LHS = lhsProj
		}
		if len(rhsColumns) > 0 {
			rhsProj := NewProjection(src.RHS)
			rhsProj.ColumnNames = rhsNames
			rhsProj.Columns = rhsColumns
			src.RHS = rhsProj
		}
		return src, rewrite.NewTree, nil

	default:
		return p, rewrite.SameTree, nil
	}
}

func planOffsets(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, rewrite.ApplyResult, error) {
		var err error
		switch op := in.(type) {
		case *Horizon:
			return nil, false, vterrors.VT13001("should not see Horizons here")
		case *Derived:
			return nil, false, vterrors.VT13001("should not see Derived here")
		case *Filter:
			err = planFilter(ctx, op)
		case *Projection:
			return planOffsetsForProjection(ctx, op)
		case *ApplyJoin:
			err = op.planOffsets(ctx)
		}
		if err != nil {
			return nil, false, err
		}
		return in, rewrite.SameTree, nil
	}

	op, err := rewrite.BottomUp(root, TableID, visitor, stopAtRoute)
	if err != nil {
		if vterr, ok := err.(*vterrors.VitessError); ok && vterr.ID == "VT13001" {
			// we encountered a bug. let's try to back out
			return nil, errNotHorizonPlanned
		}
		return nil, err
	}

	return op, nil
}

func (a *ApplyJoin) planOffsets(ctx *plancontext.PlanningContext) error {
	for _, col := range a.ColumnsAST {
		for i, lhsExpr := range col.LHSExprs {
			offset, err := a.pushColLeft(ctx, aeWrap(lhsExpr))
			if err != nil {
				return err
			}
			if col.RHSExpr == nil {
				// if we don't have a RHS expr, it means that this is a pure LHS expression
				a.Columns = append(a.Columns, -offset-1)
			} else {
				a.Vars[col.BvNames[i]] = offset
			}
		}
		if col.RHSExpr != nil {
			offset, err := a.pushColRight(ctx, aeWrap(col.RHSExpr))
			if err != nil {
				return err
			}
			a.Columns = append(a.Columns, offset+1)
		}
	}
	return nil
}

func stopAtRoute(operator ops.Operator) rewrite.VisitRule {
	_, isRoute := operator.(*Route)
	return rewrite.VisitRule(!isRoute)
}

func planOffsetsForProjection(ctx *plancontext.PlanningContext, op *Projection) (ops.Operator, rewrite.ApplyResult, error) {
	var err error
	isSimple := true
	for i, col := range op.Columns {
		rewritten := sqlparser.CopyOnRewrite(col.GetExpr(), nil, func(cursor *sqlparser.CopyOnWriteCursor) {
			col, ok := cursor.Node().(*sqlparser.ColName)
			if !ok {
				return
			}
			newSrc, offset, terr := op.Source.AddColumn(ctx, aeWrap(col))
			if terr != nil {
				err = terr
			}
			op.Source = newSrc
			cursor.Replace(sqlparser.NewOffset(offset, col))
		}, nil).(sqlparser.Expr)
		if err != nil {
			return nil, false, err
		}

		offset, ok := rewritten.(*sqlparser.Offset)
		if ok {
			// we got a pure offset back. No need to do anything else
			op.Columns[i] = Offset{
				Expr:   col.GetExpr(),
				Offset: offset.V,
			}
			continue
		}
		isSimple = false

		eexpr, err := evalengine.Translate(rewritten, nil)
		if err != nil {
			return nil, false, err
		}

		op.Columns[i] = Eval{
			Expr:  rewritten,
			EExpr: eexpr,
		}
	}
	if !isSimple {
		return op, rewrite.SameTree, nil
	}

	// is we were able to turn all the columns into offsets, we can use the SimpleProjection instead
	sp := &SimpleProjection{
		Source: op.Source,
	}
	for i, column := range op.Columns {
		offset := column.(Offset)
		sp.Columns = append(sp.Columns, offset.Offset)
		sp.ASTColumns = append(sp.ASTColumns, &sqlparser.AliasedExpr{Expr: offset.Expr, As: sqlparser.NewIdentifierCI(op.ColumnNames[i])})
	}
	return sp, rewrite.NewTree, nil
}

func pushOrExpandHorizon(ctx *plancontext.PlanningContext, in *Horizon) (ops.Operator, error) {
	rb, isRoute := in.Source.(*Route)
	if isRoute && rb.IsSingleShard() && in.Select.GetLimit() == nil {
		return rewrite.Swap(in, rb)
	}

	sel, isSel := in.selectStatement().(*sqlparser.Select)
	if !isSel {
		return nil, errNotHorizonPlanned
	}

	qp, err := CreateQPFromSelect(ctx, sel)
	if err != nil {
		return nil, err
	}

	needsOrdering := len(qp.OrderExprs) > 0
	canPushDown := isRoute && sel.Having == nil && !needsOrdering

	if canPushDown {
		return rewrite.Swap(in, rb)
	}

	return expandHorizon(ctx, qp, in)
}

// horizonLike should be removed. we should use Horizon for both these cases
type horizonLike interface {
	ops.Operator
	selectStatement() sqlparser.SelectStatement
	src() ops.Operator
}

func planSelectExpressions(ctx *plancontext.PlanningContext, in horizonLike) (ops.Operator, error) {
	sel, isSel := in.selectStatement().(*sqlparser.Select)
	if !isSel {
		return nil, errNotHorizonPlanned
	}

	qp, err := CreateQPFromSelect(ctx, sel)
	if err != nil {
		return nil, err
	}

	src := in.src()
	rb, isRoute := src.(*Route)

	needsOrdering := len(qp.OrderExprs) > 0
	canPushDown := isRoute && sel.Having == nil && !needsOrdering

	if canPushDown {
		return rewrite.Swap(in, rb)
	}

	return expandHorizon(ctx, qp, in)
}

func planDerived(ctx *plancontext.PlanningContext, in *Derived, isRoot bool) (ops.Operator, error) {
	return planSelectExpressions(ctx, in)
}

func expandHorizon(ctx *plancontext.PlanningContext, qp *QueryProjection, horizon horizonLike) (ops.Operator, error) {
	sel, isSel := horizon.selectStatement().(*sqlparser.Select)
	if !isSel {
		return nil, errNotHorizonPlanned
	}

	needsOrdering := len(qp.OrderExprs) > 0
	src := horizon.src()
	_, isDerived := src.(*Derived)

	if qp.NeedsAggregation() || sel.Having != nil || sel.Limit != nil || isDerived || needsOrdering || qp.NeedsDistinct() {
		return nil, errNotHorizonPlanned
	}

	proj := &Projection{
		Source: src,
	}

	for _, e := range qp.SelectExprs {
		expr, err := e.GetAliasedExpr()
		if err != nil {
			return nil, err
		}
		if !expr.As.IsEmpty() {
			// we are not handling column names correct yet, so let's fail here for now
			return nil, errNotHorizonPlanned
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

func pushProjections(ctx *plancontext.PlanningContext, qp *QueryProjection, src ops.Operator, isRoot bool) (ops.Operator, error) {
	// if we are at the root, we have to return the columns the user asked for. in all other levels, we reuse as much as possible
	var proj *SimpleProjection
	if isRoot {
		proj = newSimpleProjection(src)
	}
	for _, e := range qp.SelectExprs {
		expr, err := e.GetAliasedExpr()
		if err != nil {
			return nil, err
		}
		if !expr.As.IsEmpty() {
			// we are not handling column names correct yet, so let's fail here for now
			return nil, errNotHorizonPlanned
		}
		var offset int
		src, offset, err = src.AddColumn(ctx, expr)
		if err != nil {
			return nil, err
		}
		if isRoot {
			proj.ASTColumns = append(proj.ASTColumns, expr)
			proj.Columns = append(proj.Columns, offset)
		}
	}

	if !isRoot {
		return src, nil
	}

	return proj, nil
}

func aeWrap(e sqlparser.Expr) *sqlparser.AliasedExpr {
	return &sqlparser.AliasedExpr{Expr: e}
}

func planFilter(ctx *plancontext.PlanningContext, in *Filter) error {
	resolveColumn := func(col *sqlparser.ColName) (int, error) {
		newSrc, offset, err := in.Source.AddColumn(ctx, aeWrap(col))
		if err != nil {
			return 0, err
		}
		in.Source = newSrc
		return offset, nil
	}
	cfg := &evalengine.Config{
		ResolveType:   ctx.SemTable.TypeForExpr,
		Collation:     ctx.SemTable.Collation,
		ResolveColumn: resolveColumn,
	}

	eexpr, err := evalengine.Translate(sqlparser.AndExpressions(in.Predicates...), cfg)
	if err != nil {
		return err
	}

	in.FinalPredicate = eexpr
	return nil
}
