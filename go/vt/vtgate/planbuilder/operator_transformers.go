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
		// case *derivedTree:
		// 	return transformDerivedPlan(ctx, n)
		// case *subqueryTree:
		// 	return transformSubqueryTree(ctx, n)
		// case *concatenateTree:
		// 	return transformConcatenatePlan(ctx, n)
		// case *vindexTree:
		// 	return transformVindexTree(n)
		// case *correlatedSubqueryTree:
		// 	return transformCorrelatedSubquery(ctx, n)
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
	//if n.leftJoin {
	//	opCode = engine.LeftJoin
	//}

	//if canHashJoin {
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
