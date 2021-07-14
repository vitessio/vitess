/*
Copyright 2019 The Vitess Authors.

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
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func pushProjection(expr *sqlparser.AliasedExpr, plan logicalPlan, semTable *semantics.SemTable, inner bool, reuseCol bool) (int, bool, error) {
	switch node := plan.(type) {
	case *route:
		value, err := makePlanValue(expr.Expr)
		if err != nil {
			return 0, false, err
		}
		_, isColName := expr.Expr.(*sqlparser.ColName)
		badExpr := value == nil && !isColName
		if !inner && badExpr {
			return 0, false, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard left join and column expressions")
		}
		sel := node.Select.(*sqlparser.Select)
		if reuseCol {
			if i := checkIfAlreadyExists(expr, sel); i != -1 {
				return i, false, nil
			}
		}
		expr = removeQualifierFromColName(expr)

		offset := len(sel.SelectExprs)
		sel.SelectExprs = append(sel.SelectExprs, expr)
		return offset, true, nil
	case *joinGen4:
		lhsSolves := node.Left.ContainsTables()
		rhsSolves := node.Right.ContainsTables()
		deps := semTable.Dependencies(expr.Expr)
		var column int
		var appended bool
		switch {
		case deps.IsSolvedBy(lhsSolves):
			offset, added, err := pushProjection(expr, node.Left, semTable, inner, true)
			if err != nil {
				return 0, false, err
			}
			column = -(offset + 1)
			appended = added
		case deps.IsSolvedBy(rhsSolves):
			offset, added, err := pushProjection(expr, node.Right, semTable, inner && node.Opcode != engine.LeftJoin, true)
			if err != nil {
				return 0, false, err
			}
			column = offset + 1
			appended = added
		default:
			return 0, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown dependencies for %s", sqlparser.String(expr))
		}
		if reuseCol && !appended {
			for idx, col := range node.Cols {
				if column == col {
					return idx, false, nil
				}
			}
			// the column was not appended to either child, but we could not find it in out cols list,
			// so we'll still add it
		}
		node.Cols = append(node.Cols, column)
		return len(node.Cols) - 1, true, nil
	case *orderedAggregate:
		for k, v := range node.columnOffset {
			if sqlparser.EqualsExpr(expr.Expr, k) {
				return v, false, nil
			}
		}
		return 0, false, semantics.Gen4NotSupportedF("column not found in already added list: %s", sqlparser.String(expr))
	default:
		return 0, false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T not yet supported", node)
	}
}

func removeQualifierFromColName(expr *sqlparser.AliasedExpr) *sqlparser.AliasedExpr {
	if _, ok := expr.Expr.(*sqlparser.ColName); ok {
		expr = sqlparser.CloneRefOfAliasedExpr(expr)
		col := expr.Expr.(*sqlparser.ColName)
		col.Qualifier.Qualifier = sqlparser.NewTableIdent("")
	}
	return expr
}

func checkIfAlreadyExists(expr *sqlparser.AliasedExpr, sel *sqlparser.Select) int {
	for i, selectExpr := range sel.SelectExprs {
		if selectExpr, ok := selectExpr.(*sqlparser.AliasedExpr); ok {
			if selectExpr.As.IsEmpty() {
				// we don't have an alias, so we can compare the expressions
				if sqlparser.EqualsExpr(selectExpr.Expr, expr.Expr) {
					return i
				}
				// we have an aliased column, so let's check if the expression is matching the alias
			} else if colName, ok := expr.Expr.(*sqlparser.ColName); ok {
				if selectExpr.As.Equal(colName.Name) {
					return i
				}
			}

		}
	}
	return -1
}

func planAggregations(qp *abstract.QueryProjection, plan logicalPlan, semTable *semantics.SemTable) (logicalPlan, bool, error) {
	eaggr := &engine.OrderedAggregate{}
	oa := &orderedAggregate{
		resultsBuilder: resultsBuilder{
			logicalPlanCommon: newBuilderCommon(plan),
			weightStrings:     make(map[*resultColumn]int),
			truncater:         eaggr,
		},
		eaggr:        eaggr,
		columnOffset: map[sqlparser.Expr]int{},
	}
	for _, e := range qp.SelectExprs {
		offset, _, err := pushProjection(e.Col, plan, semTable, true, false)
		if err != nil {
			return nil, false, err
		}
		if e.Aggr {
			fExpr := e.Col.Expr.(*sqlparser.FuncExpr)
			opcode := engine.SupportedAggregates[fExpr.Name.Lowered()]
			oa.eaggr.Aggregates = append(oa.eaggr.Aggregates, engine.AggregateParams{
				Opcode: opcode,
				Col:    offset,
			})
			oa.columnOffset[e.Col.Expr] = offset
		}
	}

	var colAdded bool
	for _, groupExpr := range qp.GroupByExprs {
		added, err := planGroupByGen4(groupExpr, oa, semTable)
		if err != nil {
			return nil, false, err
		}
		colAdded = colAdded || added
	}

	if !qp.CanPushDownSorting {
		var orderExprs []abstract.OrderBy
		// if we can't at a later stage push down the sorting to our inputs, we have to do ordering here
		for _, groupExpr := range qp.GroupByExprs {
			orderExprs = append(orderExprs, abstract.OrderBy{
				Inner:         &sqlparser.Order{Expr: groupExpr.Inner},
				WeightStrExpr: groupExpr.WeightStrExpr},
			)
		}
		if len(orderExprs) > 0 {
			newInput, added, err := planOrderBy(qp, orderExprs, plan, semTable)
			if err != nil {
				return nil, false, err
			}
			oa.input = newInput
			return oa, colAdded || added, nil
		}
	}
	return oa, colAdded, nil
}

func planGroupByGen4(groupExpr abstract.GroupBy, plan logicalPlan, semTable *semantics.SemTable) (bool, error) {
	switch node := plan.(type) {
	case *route:
		sel := node.Select.(*sqlparser.Select)
		sel.GroupBy = append(sel.GroupBy, groupExpr.Inner)
		return false, nil
	case *orderedAggregate:
		keyCol, weightStringOffset, colAdded, wsNeeded, err := wrapAndPushExpr(groupExpr.Inner, groupExpr.WeightStrExpr, node.input, semTable)
		if err != nil {
			return false, err
		}
		if wsNeeded {
			keyCol = weightStringOffset
		}
		node.eaggr.GroupByKeys = append(node.eaggr.GroupByKeys, engine.GroupbyParams{KeyCol: keyCol, WeightStringCol: weightStringOffset})
		colAddedRecursively, err := planGroupByGen4(groupExpr, node.input, semTable)
		if err != nil {
			return false, err
		}
		node.columnOffset[groupExpr.WeightStrExpr] = keyCol
		return colAdded || colAddedRecursively, nil
	default:
		return false, semantics.Gen4NotSupportedF("group by on: %T", plan)
	}
}

func planOrderByUsingGroupBy(qp *abstract.QueryProjection, plan logicalPlan, semTable *semantics.SemTable) (logicalPlan, bool, error) {
	var orderExprs []abstract.OrderBy
	for _, groupExpr := range qp.GroupByExprs {
		addExpr := true
		for _, orderExpr := range qp.OrderExprs {
			if sqlparser.EqualsExpr(groupExpr.Inner, orderExpr.Inner.Expr) {
				addExpr = false
				break
			}
		}
		if addExpr {
			orderExprs = append(orderExprs, abstract.OrderBy{
				Inner:         &sqlparser.Order{Expr: groupExpr.Inner},
				WeightStrExpr: groupExpr.WeightStrExpr},
			)
		}
	}
	if len(orderExprs) > 0 {
		return planOrderBy(qp, orderExprs, plan, semTable)
	}
	return plan, false, nil
}

func planOrderBy(qp *abstract.QueryProjection, orderExprs []abstract.OrderBy, plan logicalPlan, semTable *semantics.SemTable) (logicalPlan, bool, error) {
	switch plan := plan.(type) {
	case *route:
		return planOrderByForRoute(orderExprs, plan, semTable)
	case *joinGen4:
		return planOrderByForJoin(qp, orderExprs, plan, semTable)
	case *orderedAggregate:
		for _, order := range orderExprs {
			if sqlparser.ContainsAggregation(order.WeightStrExpr) {
				ms, err := createMemorySortPlanOnAggregation(plan, orderExprs)
				if err != nil {
					return nil, false, err
				}
				return ms, false, nil
			}
		}
		newInput, colAdded, err := planOrderBy(qp, orderExprs, plan.input, semTable)
		if err != nil {
			return nil, false, err
		}
		plan.input = newInput
		return plan, colAdded, nil
	case *memorySort:
		return plan, false, nil
	default:
		return nil, false, semantics.Gen4NotSupportedF("ordering on complex query %T", plan)
	}
}

func planOrderByForRoute(orderExprs []abstract.OrderBy, plan *route, semTable *semantics.SemTable) (logicalPlan, bool, error) {
	origColCount := plan.Select.GetColumnCount()
	for _, order := range orderExprs {
		offset, weightStringOffset, _, _, err := wrapAndPushExpr(order.Inner.Expr, order.WeightStrExpr, plan, semTable)
		if err != nil {
			return nil, false, err
		}

		plan.eroute.OrderBy = append(plan.eroute.OrderBy, engine.OrderbyParams{
			Col:             offset,
			WeightStringCol: weightStringOffset,
			Desc:            order.Inner.Direction == sqlparser.DescOrder,
		})
		plan.Select.AddOrder(order.Inner)
	}
	return plan, origColCount != plan.Select.GetColumnCount(), nil
}

func wrapAndPushExpr(expr sqlparser.Expr, weightStrExpr sqlparser.Expr, plan logicalPlan, semTable *semantics.SemTable) (int, int, bool, bool, error) {
	offset, added, err := pushProjection(&sqlparser.AliasedExpr{Expr: expr}, plan, semTable, true, true)
	if err != nil {
		return 0, 0, false, false, err
	}
	colName, ok := expr.(*sqlparser.ColName)
	if !ok {
		return 0, 0, false, false, semantics.Gen4NotSupportedF("group by/order by non-column expression")
	}
	table := semTable.Dependencies(colName)
	tbl, err := semTable.TableInfoFor(table)
	if err != nil {
		return 0, 0, false, false, err
	}
	wsNeeded := needsWeightString(tbl, colName)

	weightStringOffset := -1
	var wAdded bool
	if wsNeeded {
		weightStringOffset, wAdded, err = pushProjection(&sqlparser.AliasedExpr{Expr: weightStringFor(weightStrExpr)}, plan, semTable, true, true)
		if err != nil {
			return 0, 0, false, false, err
		}
	}
	return offset, weightStringOffset, added || wAdded, wsNeeded, nil
}

func weightStringFor(expr sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.FuncExpr{
		Name: sqlparser.NewColIdent("weight_string"),
		Exprs: []sqlparser.SelectExpr{
			&sqlparser.AliasedExpr{
				Expr: expr,
			},
		},
	}

}

func needsWeightString(tbl semantics.TableInfo, colName *sqlparser.ColName) bool {
	for _, c := range tbl.GetColumns() {
		if colName.Name.String() == c.Name {
			return !sqltypes.IsNumber(c.Type)
		}
	}
	return true // we didn't find the column. better to add just to be safe1
}

func planOrderByForJoin(qp *abstract.QueryProjection, orderExprs []abstract.OrderBy, plan *joinGen4, semTable *semantics.SemTable) (logicalPlan, bool, error) {
	if allLeft(orderExprs, semTable, plan.Left.ContainsTables()) {
		newLeft, _, err := planOrderBy(qp, orderExprs, plan.Left, semTable)
		if err != nil {
			return nil, false, err
		}
		plan.Left = newLeft
		return plan, false, nil
	}
	ms, colAdded, err := createMemorySortPlan(plan, orderExprs, semTable)
	if err != nil {
		return nil, false, err
	}
	return ms, colAdded, nil
}

func createMemorySortPlanOnAggregation(plan *orderedAggregate, orderExprs []abstract.OrderBy) (logicalPlan, error) {
	primitive := &engine.MemorySort{}
	ms := &memorySort{
		resultsBuilder: resultsBuilder{
			logicalPlanCommon: newBuilderCommon(plan),
			weightStrings:     make(map[*resultColumn]int),
			truncater:         primitive,
		},
		eMemorySort: primitive,
	}

	for _, order := range orderExprs {
		offset, found := findExprInOrderedAggr(plan, order)
		if !found {
			return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "expected to find this expression")
		}
		ms.eMemorySort.OrderBy = append(ms.eMemorySort.OrderBy, engine.OrderbyParams{
			Col:               offset,
			WeightStringCol:   -1,
			Desc:              order.Inner.Direction == sqlparser.DescOrder,
			StarColFixedIndex: offset,
		})
	}
	return ms, nil
}

func findExprInOrderedAggr(plan *orderedAggregate, order abstract.OrderBy) (int, bool) {
	for expr, i := range plan.columnOffset {
		if sqlparser.EqualsExpr(order.WeightStrExpr, expr) {
			return i, true
		}
	}
	return 0, false
}

func createMemorySortPlan(plan logicalPlan, orderExprs []abstract.OrderBy, semTable *semantics.SemTable) (logicalPlan, bool, error) {
	primitive := &engine.MemorySort{}
	ms := &memorySort{
		resultsBuilder: resultsBuilder{
			logicalPlanCommon: newBuilderCommon(plan),
			weightStrings:     make(map[*resultColumn]int),
			truncater:         primitive,
		},
		eMemorySort: primitive,
	}

	var colAdded bool
	for _, order := range orderExprs {
		offset, weightStringOffset, added, _, err := wrapAndPushExpr(order.Inner.Expr, order.WeightStrExpr, plan, semTable)
		if err != nil {
			return nil, false, err
		}
		colAdded = colAdded || added
		ms.eMemorySort.OrderBy = append(ms.eMemorySort.OrderBy, engine.OrderbyParams{
			Col:               offset,
			WeightStringCol:   weightStringOffset,
			Desc:              order.Inner.Direction == sqlparser.DescOrder,
			StarColFixedIndex: offset,
		})
	}
	return ms, colAdded, nil
}

func allLeft(orderExprs []abstract.OrderBy, semTable *semantics.SemTable, lhsTables semantics.TableSet) bool {
	for _, expr := range orderExprs {
		exprDependencies := semTable.Dependencies(expr.Inner.Expr)
		if !exprDependencies.IsSolvedBy(lhsTables) {
			return false
		}
	}
	return true
}
