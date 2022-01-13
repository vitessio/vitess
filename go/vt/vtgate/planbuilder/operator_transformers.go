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
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/context"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/physical"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"
)

func transformOpToLogicalPlan(ctx *context.PlanningContext, op abstract.PhysicalOperator) (logicalPlan, error) {
	switch op := op.(type) {
	case *physical.Route:
		return transformRouteOpPlan(ctx, op)
	case *physical.ApplyJoin:
		return transformApplyJoinOpPlan(ctx, op)
	case *physical.Union:
		return transformUnionOpPlan(ctx, op)
	case *physical.Vindex:
		return transformVindexOpPlan(ctx, op)
	case *physical.SubQueryOp:
		return transformSubQueryOpPlan(ctx, op)
	case *physical.CorrelatedSubQueryOp:
		return transformCorrelatedSubQueryOpPlan(ctx, op)
	case *physical.Derived:
		return transformDerivedOpToPlan(ctx, op)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unknown type encountered: %T (transformOpToLogicalPlan)", op)
}

func transformApplyJoinOpPlan(ctx *context.PlanningContext, n *physical.ApplyJoin) (logicalPlan, error) {
	// TODO systay we should move the decision of which join to use to the greedy algorithm,
	// and thus represented as a queryTree
	// canHashJoin, lhsInfo, rhsInfo, err := canHashJoin(ctx, n)
	// if err != nil {
	//	return nil, err
	// }

	lhs, err := transformOpToLogicalPlan(ctx, n.LHS)
	if err != nil {
		return nil, err
	}
	rhs, err := transformOpToLogicalPlan(ctx, n.RHS)
	if err != nil {
		return nil, err
	}
	opCode := engine.InnerJoin
	if n.LeftJoin {
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
	//		Opcode:         OpCode,
	//		LHSKey:         lhsInfo.offset,
	//		RHSKey:         rhsInfo.offset,
	//		Predicate:      sqlparser.AndExpressions(n.predicates...),
	//		ComparisonType: coercedType,
	//		Collation:      lhsInfo.typ.Collation,
	//	}, nil
	// }
	return &joinGen4{
		Left:      lhs,
		Right:     rhs,
		Cols:      n.Columns,
		Vars:      n.Vars,
		Opcode:    opCode,
		Predicate: n.Predicate,
	}, nil
}

func transformRouteOpPlan(ctx *context.PlanningContext, op *physical.Route) (*routeGen4, error) {
	tableNames, err := getAllTableNames(op)
	if err != nil {
		return nil, err
	}
	var singleColumn vindexes.SingleColumn
	var value evalengine.Expr
	if op.SelectedVindex() != nil {
		vdx, ok := op.Selected.FoundVindex.(vindexes.SingleColumn)
		if !ok || len(op.Selected.Values) != 1 {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: multi-column values")
		}
		singleColumn = vdx
		value = op.Selected.Values[0]
	}

	condition := getVindexPredicate(ctx, op)

	var values []evalengine.Expr
	if value != nil {
		values = []evalengine.Expr{value}
	}
	sel := toSQL(ctx, op.Source)
	replaceSubQuery(ctx, sel)
	return &routeGen4{
		eroute: &engine.Route{
			Opcode:              op.RouteOpCode,
			TableName:           strings.Join(tableNames, ", "),
			Keyspace:            op.Keyspace,
			Vindex:              singleColumn,
			Values:              values,
			SysTableTableName:   op.SysTableTableName,
			SysTableTableSchema: op.SysTableTableSchema,
		},
		Select:    sel,
		tables:    op.TableID(),
		condition: condition,
	}, nil

}

func replaceSubQuery(ctx *context.PlanningContext, sel sqlparser.SelectStatement) {
	extractedSubqueries := ctx.SemTable.GetSubqueryNeedingRewrite()
	if len(extractedSubqueries) == 0 {
		return
	}
	sqr := &subQReplacer{subqueryToReplace: extractedSubqueries}
	sqlparser.Rewrite(sel, sqr.replacer, nil)
	for sqr.replaced {
		// to handle subqueries inside subqueries, we need to do this again and again until no replacements are left
		sqr.replaced = false
		sqlparser.Rewrite(sel, sqr.replacer, nil)
	}
}

func getVindexPredicate(ctx *context.PlanningContext, op *physical.Route) sqlparser.Expr {
	var condition sqlparser.Expr
	if op.Selected != nil {
		if len(op.Selected.ValueExprs) > 0 {
			condition = op.Selected.ValueExprs[0]
		}
		_, isMultiColumn := op.Selected.FoundVindex.(vindexes.MultiColumn)
		for idx, predicate := range op.Selected.Predicates {
			switch predicate := predicate.(type) {
			case *sqlparser.ComparisonExpr:
				if predicate.Operator == sqlparser.InOp {
					switch predicate.Left.(type) {
					case *sqlparser.ColName:
						if subq, isSubq := predicate.Right.(*sqlparser.Subquery); isSubq {
							extractedSubquery := ctx.SemTable.FindSubqueryReference(subq)
							if extractedSubquery != nil {
								extractedSubquery.SetArgName(engine.ListVarName)
							}
						}
						if isMultiColumn {
							predicate.Right = sqlparser.ListArg(engine.ListVarName + strconv.Itoa(idx))
							continue
						}
						predicate.Right = sqlparser.ListArg(engine.ListVarName)
					}
				}
			}
		}

	}
	return condition
}

func getAllTableNames(op *physical.Route) ([]string, error) {
	tableNameMap := map[string]interface{}{}
	err := physical.VisitOperators(op, func(op abstract.PhysicalOperator) (bool, error) {
		tbl, isTbl := op.(*physical.Table)
		var name string
		if isTbl {
			if tbl.QTable.IsInfSchema {
				name = sqlparser.String(tbl.QTable.Table)
			} else {
				name = sqlparser.String(tbl.QTable.Table.Name)
			}
			tableNameMap[name] = nil
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	var tableNames []string
	for name := range tableNameMap {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	return tableNames, nil
}

func transformUnionOpPlan(ctx *context.PlanningContext, op *physical.Union) (logicalPlan, error) {
	var sources []logicalPlan
	var err error
	if op.Distinct {
		sources, err = transformAndMergeOp(ctx, op)
		if err != nil {
			return nil, err
		}
		for _, source := range sources {
			pushDistinct(source)
		}
	} else {
		sources, err = transformAndMergeInOrderOp(ctx, op)
		if err != nil {
			return nil, err
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
	} else {
		result = &concatenateGen4{sources: sources}
	}
	if op.Distinct {
		return newDistinct(result, getCollationsForOp(ctx, op)), nil
	}
	return result, nil

}

func transformAndMergeOp(ctx *context.PlanningContext, op *physical.Union) (sources []logicalPlan, err error) {
	for i, source := range op.Sources {
		// first we go over all the operator inputs and turn them into logical plans,
		// including horizon planning
		plan, err := createLogicalPlanOp(ctx, source, op.SelectStmts[i])
		if err != nil {
			return nil, err
		}
		sources = append(sources, plan)
	}

	// next we'll go over all the plans from and check if any two can be merged. if they can, they are merged,
	// and we continue checking for pairs of plans that can be merged into a single route
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

func transformAndMergeInOrderOp(ctx *context.PlanningContext, op *physical.Union) (sources []logicalPlan, err error) {
	// We go over all the input operators and turn them into logical plans
	for i, source := range op.Sources {
		plan, err := createLogicalPlanOp(ctx, source, op.SelectStmts[i])
		if err != nil {
			return nil, err
		}
		if i == 0 {
			sources = append(sources, plan)
			continue
		}

		// next we check if the last plan we produced can be merged with this new plan
		last := sources[len(sources)-1]
		newPlan := mergeUnionLogicalPlans(ctx, last, plan)
		if newPlan != nil {
			// if we could merge them, let's replace the last plan with this new merged one
			sources[len(sources)-1] = newPlan
			continue
		}
		// else we just add the new plan to the end of list
		sources = append(sources, plan)
	}
	return sources, nil
}

func createLogicalPlanOp(ctx *context.PlanningContext, source abstract.PhysicalOperator, selStmt *sqlparser.Select) (logicalPlan, error) {
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

func getCollationsForOp(ctx *context.PlanningContext, n *physical.Union) []collations.ID {
	var colls []collations.ID
	for _, expr := range n.SelectStmts[0].SelectExprs {
		aliasedE, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil
		}
		typ := ctx.SemTable.CollationFor(aliasedE.Expr)
		colls = append(colls, typ)
	}
	return colls
}

func transformDerivedOpToPlan(ctx *context.PlanningContext, op *physical.Derived) (logicalPlan, error) {
	// transforming the inner part of the derived table into a logical plan
	// so that we can do horizon planning on the inner. If the logical plan
	// we've produced is a Route, we set its Select.From field to be an aliased
	// expression containing our derived table's inner select and the derived
	// table's alias.

	plan, err := transformOpToLogicalPlan(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	plan, err = planHorizon(ctx, plan, op.Query)
	if err != nil {
		return nil, err
	}

	rb, isRoute := plan.(*routeGen4)
	if !isRoute {
		return &simpleProjection{
			logicalPlanCommon: newBuilderCommon(plan),
			eSimpleProj: &engine.SimpleProjection{
				Cols: op.ColumnsOffset,
			},
		}, nil
	}
	innerSelect := rb.Select
	derivedTable := &sqlparser.DerivedTable{Select: innerSelect}
	tblExpr := &sqlparser.AliasedTableExpr{
		Expr:    derivedTable,
		As:      sqlparser.NewTableIdent(op.Alias),
		Columns: op.ColumnAliases,
	}
	selectExprs := sqlparser.SelectExprs{}
	for _, colName := range op.Columns {
		selectExprs = append(selectExprs, &sqlparser.AliasedExpr{
			Expr: colName,
		})
	}
	rb.Select = &sqlparser.Select{
		From:        []sqlparser.TableExpr{tblExpr},
		SelectExprs: selectExprs,
	}
	return plan, nil
}
