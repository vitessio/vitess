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
		return pushDownProjectionInApplyJoin(ctx, p, src)
	default:
		return p, rewrite.SameTree, nil
	}
}

func pushDownProjectionInApplyJoin(ctx *plancontext.PlanningContext, p *Projection, src *ApplyJoin) (ops.Operator, rewrite.ApplyResult, error) {
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
		if _, found := canReuseColumn(ctx, src.ColumnsAST, chooser.Expr, jcToExpr); found {
			continue
		}
		err := src.Push(ctx, chooser)
		if err != nil {
			return nil, false, err
		}
		src.ColumnsAST = append(src.ColumnsAST, out)
	}

	if len(lhsCols) > 0 {
		lhsProj, err := createProjection(src.LHS)
		if err != nil {
			return nil, false, err
		}
		lhsProj.ColumnNames = lhsNames
		lhsProj.Columns = lhsCols
		src.LHS = lhsProj
	}
	if len(rhsColumns) > 0 {
		rhsProj, err := createProjection(src.RHS)
		if err != nil {
			return nil, false, err
		}
		rhsProj.ColumnNames = rhsNames
		rhsProj.Columns = rhsColumns
		src.RHS = rhsProj
	}
	return src, rewrite.NewTree, nil
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
		return nil, errNotHorizonPlanned
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
		return nil, errNotHorizonPlanned
	}

	needsOrdering := len(qp.OrderExprs) > 0
	src := horizon.src()
	_, isDerived := src.(*Derived)

	if qp.NeedsAggregation() || sel.Having != nil || sel.Limit != nil || isDerived || needsOrdering || qp.NeedsDistinct() || sel.Distinct {
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

func aeWrap(e sqlparser.Expr) *sqlparser.AliasedExpr {
	return &sqlparser.AliasedExpr{Expr: e}
}

func (f *Filter) planOffsets(ctx *plancontext.PlanningContext) error {
	resolveColumn := func(col *sqlparser.ColName) (int, error) {
		newSrc, offset, err := f.Source.AddColumn(ctx, aeWrap(col))
		if err != nil {
			return 0, err
		}
		f.Source = newSrc
		return offset, nil
	}
	cfg := &evalengine.Config{
		ResolveType:   ctx.SemTable.TypeForExpr,
		Collation:     ctx.SemTable.Collation,
		ResolveColumn: resolveColumn,
	}

	eexpr, err := evalengine.Translate(sqlparser.AndExpressions(f.Predicates...), cfg)
	if err != nil {
		return err
	}

	f.FinalPredicate = eexpr
	return nil
}
