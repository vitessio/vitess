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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
)

func (hp *horizonPlanning) planHorizonOp(ctx *planningContext, op abstract.PhysicalOperator) (abstract.PhysicalOperator, error) {
	_, isRoute := op.(*routeOp)
	if !isRoute && ctx.semTable.ShardedError != nil {
		return nil, ctx.semTable.ShardedError
	}

	qp, err := abstract.CreateQPFromSelect(hp.sel, ctx.semTable)
	if err != nil {
		return nil, err
	}

	hp.qp = qp
	needAggrOrHaving := hp.qp.NeedsAggregation() || hp.sel.Having != nil
	// canShortcut := isRoute && !needAggrOrHaving && len(hp.qp.OrderExprs) == 0

	if needAggrOrHaving {
		// plan, err = hp.planAggregations(ctx, plan)
		// if err != nil {
		//	return nil, err
		// }
	} else {
		// _, isOA := plan.(*orderedAggregate)
		// if isOA {
		//	plan = &simpleProjection{
		//		logicalPlanCommon: newBuilderCommon(plan),
		//		eSimpleProj:       &engine.SimpleProjection{},
		//	}
		// }

		op, err = pushProjectionsOp(ctx, op, hp.qp.SelectExprs)
		if err != nil {
			return nil, err
		}
	}

	return op, nil
}

func pushProjectionsOp(ctx *planningContext, phyOp abstract.PhysicalOperator, selectExprs []abstract.SelectExpr) (abstract.PhysicalOperator, error) {
	for _, e := range selectExprs {
		aliasExpr, err := e.GetAliasedExpr()
		if err != nil {
			return nil, err
		}
		if phyOp, _, _, err = pushProjectionOp(ctx, aliasExpr, phyOp, true, false, false); err != nil {
			return nil, err
		}
	}
	return phyOp, nil
}

// pushProjection pushes a projection to the plan.
func pushProjectionOp(ctx *planningContext, expr *sqlparser.AliasedExpr, phyOp abstract.PhysicalOperator, inner, reuseCol, hasAggregation bool) (op abstract.PhysicalOperator, offset int, added bool, err error) {
	switch node := phyOp.(type) {
	case *routeOp:
		op, offset, added, err := pushProjectionOp(ctx, expr, node.source, inner, reuseCol, hasAggregation)
		node.source = op
		return node, offset, added, err
	case *tableOp:
		colName, isColName := expr.Expr.(*sqlparser.ColName)
		if isColName && expr.As.IsEmpty() {
			op, offsets, err := PushOutputColumns(ctx, node, colName)
			if err != nil {
				return nil, 0, false, err
			}
			return op, offsets[0], true, err
		}
		return nil, 0, false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "[UNIMPLEMENTED] push projection on tableOp for noncolumn expressions")
	default:
		return nil, 0, false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "[BUG] push projection does not yet support: %T", node)
	}
}
