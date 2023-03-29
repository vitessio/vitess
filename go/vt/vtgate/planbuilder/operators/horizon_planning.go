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

var errNotHorizonPlanned = vterrors.VT12001("query cannot be fully operator planned")

func planColumns(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	stopAtRoute := func(operator ops.Operator) rewrite.VisitRule {
		_, isRoute := operator.(*Route)
		return rewrite.VisitRule(!isRoute)
	}
	visitor := func(in ops.Operator, _ semantics.TableSet) (ops.Operator, rewrite.TreeIdentity, error) {
		switch in := in.(type) {
		case *Horizon:
			op, err := planHorizon(ctx, in, in == root)
			if err != nil {
				if vterr, ok := err.(*vterrors.VitessError); ok && vterr.ID == "VT13001" {
					// we encountered a bug. let's try to back out
					return nil, rewrite.SameTree, errNotHorizonPlanned
				}
				return nil, rewrite.SameTree, err
			}
			return op, rewrite.NewTree, nil
		case *Derived, *Filter:
			// TODO we need to do column planning on these
			return nil, rewrite.SameTree, errNotHorizonPlanned
		default:
			return in, rewrite.SameTree, nil
		}
	}
	return rewrite.BottomUp(root, TableID, visitor, stopAtRoute)
}

func planHorizon(ctx *plancontext.PlanningContext, in *Horizon, isRoot bool) (ops.Operator, error) {
	rb, isRoute := in.Source.(*Route)
	if !isRoute && ctx.SemTable.NotSingleRouteErr != nil {
		// If we got here, we don't have a single shard plan
		return nil, ctx.SemTable.NotSingleRouteErr
	}
	if isRoute && rb.IsSingleShard() && in.Select.GetLimit() == nil {
		return planSingleRoute(rb, in)
	}

	sel, isSel := in.Select.(*sqlparser.Select)
	if !isSel {
		return nil, errNotHorizonPlanned
	}

	qp, err := CreateQPFromSelect(ctx, sel)
	if err != nil {
		return nil, err
	}

	needsOrdering := len(qp.OrderExprs) > 0
	canShortcut := isRoute && sel.Having == nil && !needsOrdering
	_, isDerived := in.Source.(*Derived)

	// if we are at the root, we have to return the columns the user asked for. in all other levels, we reuse as much as possible
	canReuseCols := !isRoot

	switch {
	case qp.NeedsAggregation() || sel.Having != nil || sel.Limit != nil || isDerived || needsOrdering || qp.NeedsDistinct():
		return nil, errNotHorizonPlanned
	case canShortcut:
		return planSingleRoute(rb, in)
	default:
		src := in.Source
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
			if idx != offset && isRoot {
				// if we are returning something different from what the user asked for,
				// we need to add an operator on top to clean up the output
				return nil, errNotHorizonPlanned
			}
		}
		return src, nil
	}
}

func planSingleRoute(rb *Route, horizon *Horizon) (ops.Operator, error) {
	rb.Source, horizon.Source = horizon, rb.Source
	return rb, nil
}

func aeWrap(e sqlparser.Expr) *sqlparser.AliasedExpr {
	return &sqlparser.AliasedExpr{Expr: e}
}
