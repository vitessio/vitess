/*
Copyright 2021 The Vitess Authors.

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
	"sort"
	"strings"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"
)

func transformOpToLogicalPlan(ctx *planningContext, op abstract.PhysicalOperator) (logicalPlan, error) {
	switch op := op.(type) {
	case *routeOp:
		return transformRouteOpPlan(ctx, op)
	case *applyJoin:
		return transformApplyJoinOpPlan(ctx, op)
	case *unionOp:
		return transformUnionOpPlan(ctx, op)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unknown type encountered: %T (transformOpToLogicalPlan)", op)
}

func transformApplyJoinOpPlan(ctx *planningContext, n *applyJoin) (logicalPlan, error) {
	// TODO systay we should move the decision of which join to use to the greedy algorithm,
	// and thus represented as a queryTree
	//canHashJoin, lhsInfo, rhsInfo, err := canHashJoin(ctx, n)
	//if err != nil {
	//	return nil, err
	//}

	lhs, err := transformOpToLogicalPlan(ctx, n.LHS)
	if err != nil {
		return nil, err
	}
	rhs, err := transformOpToLogicalPlan(ctx, n.RHS)
	if err != nil {
		return nil, err
	}
	opCode := engine.InnerJoin
	if n.leftJoin {
		opCode = engine.LeftJoin
	}

	// if canHashJoin {
	//	coercedType, err := evalengine.CoerceTo(lhsInfo.typ.Type, rhsInfo.typ.Type)
	//	if err != nil {
	//		return nil, err
	//	}
	//	return &hashJoin{
	//		Left:           lhs,
	//		Right:          rhs,
	//		Cols:           n.columns,
	//		Opcode:         opCode,
	//		LHSKey:         lhsInfo.offset,
	//		RHSKey:         rhsInfo.offset,
	//		Predicate:      sqlparser.AndExpressions(n.predicates...),
	//		ComparisonType: coercedType,
	//		Collation:      lhsInfo.typ.Collation,
	//	}, nil
	//}
	return &joinGen4{
		Left:      lhs,
		Right:     rhs,
		Cols:      n.columns,
		Vars:      n.vars,
		Opcode:    opCode,
		Predicate: n.predicate,
	}, nil
}

func transformRouteOpPlan(ctx *planningContext, op *routeOp) (*routeGen4, error) {

	tableNames := getAllTableNames(op)

	var singleColumn vindexes.SingleColumn
	var value evalengine.Expr
	if op.selectedVindex() != nil {
		vdx, ok := op.selected.foundVindex.(vindexes.SingleColumn)
		if !ok || len(op.selected.values) != 1 {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: multi-column values")
		}
		singleColumn = vdx
		value = op.selected.values[0]
	}

	var condition sqlparser.Expr
	if op.selected != nil && len(op.selected.valueExprs) > 0 {
		condition = op.selected.valueExprs[0]
	}

	var values []evalengine.Expr
	if value != nil {
		values = []evalengine.Expr{value}
	}
	return &routeGen4{
		eroute: &engine.Route{
			Opcode:              op.routeOpCode,
			TableName:           strings.Join(tableNames, ", "),
			Keyspace:            op.keyspace,
			Vindex:              singleColumn,
			Values:              values,
			SysTableTableName:   op.SysTableTableName,
			SysTableTableSchema: op.SysTableTableSchema,
		},
		Select:    toSQL(ctx, op.source),
		tables:    op.TableID(),
		condition: condition,
	}, nil

}

func getAllTableNames(op *routeOp) []string {
	tableNameMap := map[string]interface{}{}
	_ = visitOperators(op, func(op abstract.Operator) (bool, error) {
		tbl, isTbl := op.(*tableOp)
		var name string
		if isTbl {
			if tbl.qtable.IsInfSchema {
				name = sqlparser.String(tbl.qtable.Table)
			} else {
				name = sqlparser.String(tbl.qtable.Table.Name)
			}
			tableNameMap[name] = nil
		}
		return true, nil
	})
	var tableNames []string
	for name := range tableNameMap {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	return tableNames
}

func transformUnionOpPlan(ctx *planningContext, op *unionOp) (logicalPlan, error) {
	var sources []logicalPlan
	var err error
	if op.distinct {
		sources, err = transformAndMergeOp(ctx, op)
	} else {
		sources, err = transformAndMergeOp(ctx, op) // TODO: this is WRONG
	}
	if err != nil {
		return nil, err
	}
	if op.distinct {
		for _, source := range sources {
			pushDistinct(source)
		}
	}
	var result logicalPlan
	if len(sources) == 1 {
		src := sources[0]
		if rb, isRoute := src.(*routeGen4); isRoute && rb.isSingleShard() {
			// if we have a single shard route, we don't need to do anything to make it distinct
			// TODO
			// rb.Select.SetLimit(op.limit)
			// rb.Select.SetOrderBy(op.ordering)
			return src, nil
		}
		result = src
	}
	if op.distinct {
		return newDistinct(result, getCollationsForOp(ctx, op)), nil
	}
	return result, nil

}

func transformAndMergeOp(ctx *planningContext, op *unionOp) ([]logicalPlan, error) {
	var sources []logicalPlan
	for i, source := range op.sources {
		plan, err := createLogicalPlanOp(ctx, source, op.selectStmts[i])
		if err != nil {
			return nil, err
		}
		sources = append(sources, plan)
	}

	idx := 0
	for idx < len(sources) {
		keep := make([]bool, len(sources))
		srcA := sources[idx]
		merged := false
		for j, srcB := range sources {
			if j <= idx {
				continue
			}
			newPlan := mergeUnionLogicalPlans(ctx, srcA, srcB)
			if newPlan != nil {
				sources[idx] = newPlan
				srcA = newPlan
				merged = true
			} else {
				keep[j] = true
			}
		}
		if !merged {
			return sources, nil
		}
		var phase []logicalPlan
		for i, source := range sources {
			if keep[i] || i <= idx {
				phase = append(phase, source)
			}
		}
		idx++
		sources = phase
	}
	return sources, nil
}

func createLogicalPlanOp(ctx *planningContext, source abstract.PhysicalOperator, selStmt *sqlparser.Select) (logicalPlan, error) {
	plan, err := transformOpToLogicalPlan(ctx, source)
	if err != nil {
		return nil, err
	}
	if selStmt != nil {
		plan, err = planHorizon(ctx, plan, selStmt)
		if err != nil {
			return nil, err
		}
		if err := setMiscFunc(plan, selStmt); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func getCollationsForOp(ctx *planningContext, n *unionOp) []collations.ID {
	var colls []collations.ID
	for _, expr := range n.selectStmts[0].SelectExprs {
		aliasedE, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil
		}
		typ := ctx.semTable.CollationFor(aliasedE.Expr)
		colls = append(colls, typ)
	}
	return colls
}
