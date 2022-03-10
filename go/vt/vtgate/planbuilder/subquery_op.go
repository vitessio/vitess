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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/physical"
)

func transformSubQueryPlan(ctx *plancontext.PlanningContext, op *physical.SubQueryOp) (logicalPlan, error) {
	innerPlan, err := transformToLogicalPlan(ctx, op.Inner)
	if err != nil {
		return nil, err
	}
	innerPlan, err = planHorizon(ctx, innerPlan, op.Extracted.Subquery.Select)
	if err != nil {
		return nil, err
	}

	argName := op.Extracted.GetArgName()
	hasValuesArg := op.Extracted.GetHasValuesArg()
	outerPlan, err := transformToLogicalPlan(ctx, op.Outer)

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

func transformCorrelatedSubQueryPlan(ctx *plancontext.PlanningContext, op *physical.CorrelatedSubQueryOp) (logicalPlan, error) {
	outer, err := transformToLogicalPlan(ctx, op.Outer)
	if err != nil {
		return nil, err
	}
	inner, err := transformToLogicalPlan(ctx, op.Inner)
	if err != nil {
		return nil, err
	}
	return newSemiJoin(outer, inner, op.Vars, op.LHSColumns), nil
}

func mergeSubQueryOpPlan(ctx *plancontext.PlanningContext, inner, outer logicalPlan, n *physical.SubQueryOp) logicalPlan {
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

// mergeSystemTableInformation copies over information from the second route to the first and appends to it
func mergeSystemTableInformation(a *routeGen4, b *routeGen4) logicalPlan {
	// safe to append system table schema and system table names, since either the routing will match or either side would be throwing an error
	// during run-time which we want to preserve. For example outer side has User in sys table schema and inner side has User and Main in sys table schema
	// Inner might end up throwing an error at runtime, but if it doesn't then it is safe to merge.
	a.eroute.SysTableTableSchema = append(a.eroute.SysTableTableSchema, b.eroute.SysTableTableSchema...)
	for k, v := range b.eroute.SysTableTableName {
		a.eroute.SysTableTableName[k] = v
	}
	return a
}

func canMergeSubqueryPlans(ctx *plancontext.PlanningContext, a, b *routeGen4) bool {
	// this method should be close to tryMerge below. it does the same thing, but on logicalPlans instead of queryTrees
	if a.eroute.Keyspace.Name != b.eroute.Keyspace.Name {
		return false
	}
	switch a.eroute.Opcode {
	case engine.Unsharded, engine.Reference:
		return a.eroute.Opcode == b.eroute.Opcode
	case engine.DBA:
		return canSelectDBAMerge(a, b)
	case engine.EqualUnique:
		// Check if they target the same shard.
		if b.eroute.Opcode == engine.EqualUnique &&
			a.eroute.Vindex == b.eroute.Vindex &&
			a.condition != nil &&
			b.condition != nil &&
			gen4ValuesEqual(ctx, []sqlparser.Expr{a.condition}, []sqlparser.Expr{b.condition}) {
			return true
		}
	}
	return false
}
