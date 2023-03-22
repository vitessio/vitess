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

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
)

var errNotHorizonPlanned = vterrors.VT12001("query cannot be fully operator planned")

func planHorizons(ctx *plancontext.PlanningContext, in ops.Operator) (ops.Operator, error) {
	return rewrite.TopDown(in, func(in ops.Operator) (ops.Operator, rewrite.TreeIdentity, rewrite.VisitRule, error) {
		switch in := in.(type) {
		case *Horizon:
			op, visit, err := planHorizon(ctx, in)
			if err != nil {
				return nil, rewrite.SameTree, rewrite.SkipChildren, err
			}
			return op, rewrite.NewTree, visit, nil
		case *Route:
			return in, rewrite.SameTree, rewrite.SkipChildren, nil
		default:
			return in, rewrite.SameTree, rewrite.VisitChildren, nil
		}
	})
}

func planHorizon(ctx *plancontext.PlanningContext, in *Horizon) (ops.Operator, rewrite.VisitRule, error) {
	rb, isRoute := in.Source.(*Route)
	if !isRoute {
		return in, rewrite.VisitChildren, nil
	}
	if isRoute && rb.IsSingleShard() && in.Select.GetLimit() == nil {
		return planSingleRoute(rb, in)
	}

	sel, isSel := in.Select.(*sqlparser.Select)
	if !isSel {
		return nil, rewrite.VisitChildren, errNotHorizonPlanned
	}

	qp, err := CreateQPFromSelect(ctx, sel)
	if err != nil {
		return nil, rewrite.VisitChildren, err
	}

	needsOrdering := len(qp.OrderExprs) > 0
	canShortcut := isRoute && sel.Having == nil && !needsOrdering

	if !qp.NeedsAggregation() && sel.Having == nil && canShortcut && !needsOrdering && !qp.NeedsDistinct() && in.Select.GetLimit() == nil {
		return planSingleRoute(rb, in)
	}
	return nil, rewrite.VisitChildren, errNotHorizonPlanned
}

func planSingleRoute(rb *Route, horizon *Horizon) (ops.Operator, rewrite.VisitRule, error) {
	rb.Source, horizon.Source = horizon, rb.Source
	return rb, rewrite.SkipChildren, nil
}
