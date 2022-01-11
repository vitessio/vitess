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

package planbuilder

import (
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/context"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/physical"
)

func transformSubQueryOpPlan(ctx *context.PlanningContext, op *physical.SubQueryOp) (logicalPlan, error) {
	innerPlan, err := transformOpToLogicalPlan(ctx, op.Inner)
	if err != nil {
		return nil, err
	}
	innerPlan, err = planHorizon(ctx, innerPlan, op.Extracted.Subquery.Select)
	if err != nil {
		return nil, err
	}

	argName := op.Extracted.GetArgName()
	hasValuesArg := op.Extracted.GetHasValuesArg()
	outerPlan, err := transformOpToLogicalPlan(ctx, op.Outer)

	merged := mergeSubQueryOpPlan(ctx, innerPlan, outerPlan, op)
	if merged != nil {
		return merged, nil
	}
	plan := newPulloutSubquery(engine.PulloutOpcode(op.Extracted.OpCode), argName, hasValuesArg, innerPlan)
	if err != nil {
		return nil, err
	}
	plan.underlying = outerPlan
	return plan, err
}

func transformCorrelatedSubQueryOpPlan(ctx *context.PlanningContext, op *physical.CorrelatedSubQueryOp) (logicalPlan, error) {
	outer, err := transformOpToLogicalPlan(ctx, op.Outer)
	if err != nil {
		return nil, err
	}
	inner, err := transformOpToLogicalPlan(ctx, op.Inner)
	if err != nil {
		return nil, err
	}
	return newSemiJoin(outer, inner, op.Vars), nil
}

func mergeSubQueryOpPlan(ctx *context.PlanningContext, inner, outer logicalPlan, n *physical.SubQueryOp) logicalPlan {
	iroute, ok := inner.(*routeGen4)
	if !ok {
		return nil
	}
	oroute, ok := outer.(*routeGen4)
	if !ok {
		return nil
	}

	if canMergeSubqueryPlans(ctx, iroute, oroute) {
		// n.extracted is an expression that lives in oroute.Select.
		// Instead of looking for it in the AST, we have a copy in the subquery tree that we can update
		n.Extracted.NeedsRewrite = true
		replaceSubQuery(ctx, oroute.Select)
		return mergeSystemTableInformation(oroute, iroute)
	}
	return nil
}
