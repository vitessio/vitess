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

// planColumns is the process of figuring out all necessary columns.
// They can be needed because the user wants to return the value of a column,
// or because we need a column for filtering, grouping or ordering
func planColumns(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	// We only need to do column planning to the point we hit a Route.
	// Everything underneath the route is handled by the mysql planner
	stopAtRoute := func(operator ops.Operator) rewrite.VisitRule {
		_, isRoute := operator.(*Route)
		return rewrite.VisitRule(!isRoute)
	}
	visitor := func(in ops.Operator, _ semantics.TableSet) (ops.Operator, rewrite.TreeIdentity, error) {
		switch in := in.(type) {
		case *Horizon:
			op, err := planHorizon(ctx, in, in == root)
			if err != nil {
				return nil, false, err
			}
			return op, rewrite.NewTree, nil
		case *Derived:
			op, err := planDerived(ctx, in, in == root)
			if err != nil {
				return nil, false, err
			}
			return op, rewrite.NewTree, err
		case *Filter:
			err := planFilter(ctx, in)
			if err != nil {
				return nil, false, err
			}
			return in, rewrite.SameTree, nil
		default:
			return in, rewrite.SameTree, nil
		}
	}

	newOp, err := rewrite.BottomUp(root, TableID, visitor, stopAtRoute)
	if err != nil {
		if vterr, ok := err.(*vterrors.VitessError); ok && vterr.ID == "VT13001" {
			// we encountered a bug. let's try to back out
			return nil, errNotHorizonPlanned
		}
		return nil, err
	}

	return newOp, nil
}

func planHorizon(ctx *plancontext.PlanningContext, in *Horizon, isRoot bool) (ops.Operator, error) {
	rb, isRoute := in.Source.(*Route)
	if isRoute && rb.IsSingleShard() && in.Select.GetLimit() == nil {
		return rewrite.Swap(in, rb)
	}

	return planSelectExpressions(ctx, in, isRoot)
}

// horizonLike should be removed. we should use Horizon for both these cases
type horizonLike interface {
	ops.Operator
	selectStatement() sqlparser.SelectStatement
	src() ops.Operator
}

func planSelectExpressions(ctx *plancontext.PlanningContext, in horizonLike, isRoot bool) (ops.Operator, error) {
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
	_, isDerived := src.(*Derived)

	switch {
	case qp.NeedsAggregation() || sel.Having != nil || sel.Limit != nil || isDerived || needsOrdering || qp.NeedsDistinct():
		return nil, errNotHorizonPlanned
	case canPushDown:
		return rewrite.Swap(in, rb)
	default:
		return pushProjections(ctx, qp, src, isRoot)
	}
}

func planDerived(ctx *plancontext.PlanningContext, in *Derived, isRoot bool) (ops.Operator, error) {
	return planSelectExpressions(ctx, in, isRoot)
}

func pushProjections(ctx *plancontext.PlanningContext, qp *QueryProjection, src ops.Operator, isRoot bool) (ops.Operator, error) {
	// if we are at the root, we have to return the columns the user asked for. in all other levels, we reuse as much as possible
	canReuseCols := !isRoot
	proj := newSimpleProjection(src)
	needProj := false
	for idx, e := range qp.SelectExprs {
		expr, err := e.GetAliasedExpr()
		if err != nil {
			return nil, err
		}
		if !expr.As.IsEmpty() {
			// we are not handling column names correct yet, so let's fail here for now
			return nil, errNotHorizonPlanned
		}
		var offset int
		src, offset, err = src.AddColumn(ctx, expr, canReuseCols)
		if err != nil {
			return nil, err
		}

		if offset != idx {
			needProj = true
		}
		proj.ASTColumns = append(proj.ASTColumns, expr)
		proj.Columns = append(proj.Columns, offset)
	}

	if needProj {
		return proj, nil
	}

	columns, err := src.GetColumns()
	if err != nil {
		return nil, err
	}
	if len(columns) == qp.GetColumnCount() {
		return src, nil
	}
	return proj, nil
}

func aeWrap(e sqlparser.Expr) *sqlparser.AliasedExpr {
	return &sqlparser.AliasedExpr{Expr: e}
}

func planFilter(ctx *plancontext.PlanningContext, in *Filter) error {
	resolveColumn := func(col *sqlparser.ColName) (int, error) {
		newSrc, offset, err := in.Source.AddColumn(ctx, aeWrap(col), true)
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
