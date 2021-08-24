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
			if i := checkIfAlreadyExists(expr, sel, semTable); i != -1 {
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
		deps := semTable.BaseTableDependencies(expr.Expr)
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
	case *pulloutSubquery:
		// push projection to the outer query
		return pushProjection(expr, node.underlying, semTable, inner, reuseCol)
	case *subquery:
		offset, _, err := pushProjection(expr, node.input, semTable, inner, reuseCol)
		if err != nil {
			return 0, false, err
		}
		node.esubquery.Cols = append(node.esubquery.Cols, offset)
		return len(node.esubquery.Cols) - 1, true, nil
	case *orderedAggregate:
		colName, isColName := expr.Expr.(*sqlparser.ColName)
		for _, aggregate := range node.eaggr.Aggregates {
			if sqlparser.EqualsExpr(aggregate.Expr, expr.Expr) {
				return aggregate.Col, false, nil
			}
			if isColName && colName.Name.EqualString(aggregate.Alias) {
				return aggregate.Col, false, nil
			}
		}
		return 0, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "cannot push projections in ordered aggregates")
	default:
		return 0, false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "[BUG] push projection does not yet support: %T", node)
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

func checkIfAlreadyExists(expr *sqlparser.AliasedExpr, sel *sqlparser.Select, semTable *semantics.SemTable) int {
	exprDep := semTable.BaseTableDependencies(expr.Expr)

	for i, selectExpr := range sel.SelectExprs {
		selectExpr, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}

		selectExprCol, isSelectExprCol := selectExpr.Expr.(*sqlparser.ColName)
		exprCol, isExprCol := expr.Expr.(*sqlparser.ColName)
		selectExprDep := semTable.BaseTableDependencies(selectExpr.Expr)

		// Check that the two expressions have the same dependencies
		if selectExprDep != exprDep {
			continue
		}

		if selectExpr.As.IsEmpty() {
			// we don't have an alias

			if isSelectExprCol && isExprCol && exprCol.Name.Equal(selectExprCol.Name) {
				// the expressions are ColName, we compare their name
				return i
			} else if sqlparser.EqualsExpr(selectExpr.Expr, expr.Expr) {
				// the expressions are not ColName, so we just compare the expressions
				return i
			}
		} else if isExprCol && selectExpr.As.Equal(exprCol.Name) {
			// we have an aliased column, checking if the the expression is matching the alias
			return i
		}
	}
	return -1
}

func (hp *horizonPlanning) haveToTruncate(v bool) {
	hp.needsTruncation = hp.needsTruncation || v
}

func (hp *horizonPlanning) planAggregations() error {
	newPlan := hp.plan
	var oa *orderedAggregate
	uniqVindex := hasUniqueVindex(hp.vschema, hp.semTable, hp.qp.GroupByExprs)
	_, joinPlan := hp.plan.(*joinGen4)
	if !uniqVindex || joinPlan {
		eaggr := &engine.OrderedAggregate{}
		oa = &orderedAggregate{
			resultsBuilder: resultsBuilder{
				logicalPlanCommon: newBuilderCommon(hp.plan),
				weightStrings:     make(map[*resultColumn]int),
				truncater:         eaggr,
			},
			eaggr: eaggr,
		}
		newPlan = oa
		hp.vtgateGrouping = true
	}

	for _, e := range hp.qp.SelectExprs {
		// push all expression if they are non-aggregating or the plan is not ordered aggregated plan.
		if !e.Aggr || oa == nil {
			_, _, err := pushProjection(e.Col, hp.plan, hp.semTable, true, false)
			if err != nil {
				return err
			}
			continue
		}

		fExpr, isFunc := e.Col.Expr.(*sqlparser.FuncExpr)
		if !isFunc {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: complex aggregate expression")
		}
		opcode, found := engine.SupportedAggregates[fExpr.Name.Lowered()]
		if !found {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: complex aggregate expression")
		}
		handleDistinct, innerAliased, err := hp.needDistinctHandling(fExpr, opcode, oa.input)
		if err != nil {
			return err
		}

		pushExpr, alias, opcode := hp.createPushExprAndAlias(e, handleDistinct, innerAliased, opcode, oa)
		offset, _, err := pushProjection(pushExpr, oa.input, hp.semTable, true, false)
		if err != nil {
			return err
		}
		oa.eaggr.Aggregates = append(oa.eaggr.Aggregates, &engine.AggregateParams{
			Opcode: opcode,
			Col:    offset,
			Alias:  alias,
			Expr:   fExpr,
		})
	}

	for _, groupExpr := range hp.qp.GroupByExprs {
		added, err := planGroupByGen4(groupExpr, newPlan, hp.semTable)
		if err != nil {
			return err
		}
		hp.haveToTruncate(added)
	}

	if !hp.qp.CanPushDownSorting && oa != nil {
		var orderExprs []abstract.OrderBy
		// if we can't at a later stage push down the sorting to our inputs, we have to do ordering here
		for _, groupExpr := range hp.qp.GroupByExprs {
			orderExprs = append(orderExprs, abstract.OrderBy{
				Inner:         &sqlparser.Order{Expr: groupExpr.Inner},
				WeightStrExpr: groupExpr.WeightStrExpr},
			)
		}
		if len(orderExprs) > 0 {
			newInput, err := hp.planOrderBy(orderExprs, hp.plan)
			if err != nil {
				return err
			}
			oa.input = newInput
			hp.plan = oa
		}
	} else {
		hp.plan = newPlan
	}

	// done with aggregation planning. let's check if we should fail the query
	if _, planIsRoute := hp.plan.(*route); !planIsRoute {
		// if we had to build up additional operators around the route, we have to fail this query
		for _, expr := range hp.qp.SelectExprs {
			colExpr := expr.Col.Expr
			if !sqlparser.IsAggregation(colExpr) && sqlparser.ContainsAggregation(colExpr) {
				return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: complex aggregate expression")
			}
		}
	}

	return nil
}

// createPushExprAndAlias creates the expression that should be pushed down to the leaves,
// and changes the opcode so it is a distinct one if needed
func (hp *horizonPlanning) createPushExprAndAlias(
	expr abstract.SelectExpr,
	handleDistinct bool,
	innerAliased *sqlparser.AliasedExpr,
	opcode engine.AggregateOpcode,
	oa *orderedAggregate,
) (*sqlparser.AliasedExpr, string, engine.AggregateOpcode) {
	pushExpr := expr.Col
	var alias string
	if expr.Col.As.IsEmpty() {
		alias = sqlparser.String(expr.Col.Expr)
	} else {
		alias = expr.Col.As.String()
	}
	if handleDistinct {
		pushExpr = innerAliased

		switch opcode {
		case engine.AggregateCount:
			opcode = engine.AggregateCountDistinct
		case engine.AggregateSum:
			opcode = engine.AggregateSumDistinct
		}

		oa.eaggr.PreProcess = true
		hp.haveToTruncate(true)
		by := abstract.GroupBy{
			Inner:             innerAliased.Expr,
			WeightStrExpr:     innerAliased.Expr,
			DistinctAggrIndex: len(oa.eaggr.Aggregates) + 1,
		}
		hp.qp.GroupByExprs = append(hp.qp.GroupByExprs, by)
	}
	return pushExpr, alias, opcode
}

func hasUniqueVindex(vschema ContextVSchema, semTable *semantics.SemTable, groupByExprs []abstract.GroupBy) bool {
	for _, groupByExpr := range groupByExprs {
		if exprHasUniqueVindex(vschema, semTable, groupByExpr.WeightStrExpr) {
			return true
		}
	}
	return false
}

func planGroupByGen4(groupExpr abstract.GroupBy, plan logicalPlan, semTable *semantics.SemTable) (bool, error) {
	switch node := plan.(type) {
	case *route:
		sel := node.Select.(*sqlparser.Select)
		sel.GroupBy = append(sel.GroupBy, groupExpr.Inner)
		return false, nil
	case *joinGen4:
		_, _, added, err := wrapAndPushExpr(groupExpr.Inner, groupExpr.WeightStrExpr, node, semTable)
		return added, err
	case *orderedAggregate:
		keyCol, wsOffset, colAdded, err := wrapAndPushExpr(groupExpr.Inner, groupExpr.WeightStrExpr, node.input, semTable)
		if err != nil {
			return false, err
		}
		if groupExpr.DistinctAggrIndex == 0 {
			node.eaggr.GroupByKeys = append(node.eaggr.GroupByKeys, &engine.GroupByParams{KeyCol: keyCol, WeightStringCol: wsOffset, Expr: groupExpr.WeightStrExpr})
		} else {
			if wsOffset != -1 {
				node.eaggr.Aggregates[groupExpr.DistinctAggrIndex-1].WAssigned = true
				node.eaggr.Aggregates[groupExpr.DistinctAggrIndex-1].WCol = wsOffset
			}
		}
		colAddedRecursively, err := planGroupByGen4(groupExpr, node.input, semTable)
		if err != nil {
			return false, err
		}
		return colAdded || colAddedRecursively, nil
	default:
		return false, semantics.Gen4NotSupportedF("group by on: %T", plan)
	}
}

func (hp *horizonPlanning) planOrderByUsingGroupBy() (logicalPlan, error) {
	var orderExprs []abstract.OrderBy
	for _, groupExpr := range hp.qp.GroupByExprs {
		addExpr := true
		for _, orderExpr := range hp.qp.OrderExprs {
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
		return hp.planOrderBy(orderExprs, hp.plan)
	}
	return hp.plan, nil
}

func (hp *horizonPlanning) planOrderBy(orderExprs []abstract.OrderBy, plan logicalPlan) (logicalPlan, error) {
	switch plan := plan.(type) {
	case *route:
		newPlan, truncate, err := planOrderByForRoute(orderExprs, plan, hp.semTable)
		if err != nil {
			return nil, err
		}
		hp.haveToTruncate(truncate)
		return newPlan, nil
	case *joinGen4:
		newPlan, err := hp.planOrderByForJoin(orderExprs, plan)
		if err != nil {
			return nil, err
		}

		return newPlan, nil
	case *orderedAggregate:
		// remove ORDER BY NULL from the list of order by expressions since we will be doing the ordering on vtgate level so NULL is not useful
		var orderExprsWithoutNils []abstract.OrderBy
		for _, expr := range orderExprs {
			if sqlparser.IsNull(expr.Inner.Expr) {
				continue
			}
			orderExprsWithoutNils = append(orderExprsWithoutNils, expr)
		}
		orderExprs = orderExprsWithoutNils

		for _, order := range orderExprs {
			if sqlparser.ContainsAggregation(order.WeightStrExpr) {
				ms, err := createMemorySortPlanOnAggregation(plan, orderExprs)
				if err != nil {
					return nil, err
				}
				return ms, nil
			}
		}
		newInput, err := hp.planOrderBy(orderExprs, plan.input)
		if err != nil {
			return nil, err
		}
		plan.input = newInput
		return plan, nil
	case *memorySort:
		return plan, nil
	case *pulloutSubquery:
		newUnderlyingPlan, err := hp.planOrderBy(orderExprs, plan.underlying)
		if err != nil {
			return nil, err
		}
		plan.underlying = newUnderlyingPlan
		return plan, nil
	default:
		return nil, semantics.Gen4NotSupportedF("ordering on complex query %T", plan)
	}
}

func planOrderByForRoute(orderExprs []abstract.OrderBy, plan *route, semTable *semantics.SemTable) (logicalPlan, bool, error) {
	origColCount := plan.Select.GetColumnCount()
	for _, order := range orderExprs {
		plan.Select.AddOrder(order.Inner)
		if sqlparser.IsNull(order.Inner.Expr) {
			continue
		}
		offset, weightStringOffset, _, err := wrapAndPushExpr(order.Inner.Expr, order.WeightStrExpr, plan, semTable)
		if err != nil {
			return nil, false, err
		}

		plan.eroute.OrderBy = append(plan.eroute.OrderBy, engine.OrderByParams{
			Col:             offset,
			WeightStringCol: weightStringOffset,
			Desc:            order.Inner.Direction == sqlparser.DescOrder,
		})
	}
	return plan, origColCount != plan.Select.GetColumnCount(), nil
}

// wrapAndPushExpr pushes the expression and weighted_string function to the plan using semantics.SemTable
// It returns (expr offset, weight_string offset, new_column added, error)
func wrapAndPushExpr(expr sqlparser.Expr, weightStrExpr sqlparser.Expr, plan logicalPlan, semTable *semantics.SemTable) (int, int, bool, error) {
	offset, added, err := pushProjection(&sqlparser.AliasedExpr{Expr: expr}, plan, semTable, true, true)
	if err != nil {
		return 0, 0, false, err
	}
	if weightStrExpr == nil {
		return offset, -1, added, nil
	}
	_, ok := expr.(*sqlparser.ColName)
	if !ok {
		return 0, 0, false, semantics.Gen4NotSupportedF("group by/order by non-column expression")
	}
	qt := semTable.TypeFor(expr)
	wsNeeded := true
	if qt != nil && sqltypes.IsNumber(*qt) {
		wsNeeded = false
	}

	weightStringOffset := -1
	var wAdded bool
	if wsNeeded {
		weightStringOffset, wAdded, err = pushProjection(&sqlparser.AliasedExpr{Expr: weightStringFor(weightStrExpr)}, plan, semTable, true, true)
		if err != nil {
			return 0, 0, false, err
		}
	}
	return offset, weightStringOffset, added || wAdded, nil
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

func (hp *horizonPlanning) planOrderByForJoin(orderExprs []abstract.OrderBy, plan *joinGen4) (logicalPlan, error) {
	if allLeft(orderExprs, hp.semTable, plan.Left.ContainsTables()) {
		newLeft, err := hp.planOrderBy(orderExprs, plan.Left)
		if err != nil {
			return nil, err
		}
		plan.Left = newLeft
		return plan, nil
	}
	sortPlan, err := hp.createMemorySortPlan(plan, orderExprs)
	if err != nil {
		return nil, err
	}
	return sortPlan, nil
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
		offset, woffset, found := findExprInOrderedAggr(plan, order)
		if !found {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected to find the order by expression (%s) in orderedAggregate", sqlparser.String(order.Inner))
		}
		ms.eMemorySort.OrderBy = append(ms.eMemorySort.OrderBy, engine.OrderByParams{
			Col:               offset,
			WeightStringCol:   woffset,
			Desc:              order.Inner.Direction == sqlparser.DescOrder,
			StarColFixedIndex: offset,
		})
	}
	return ms, nil
}

func findExprInOrderedAggr(plan *orderedAggregate, order abstract.OrderBy) (int, int, bool) {
	for _, key := range plan.eaggr.GroupByKeys {
		if sqlparser.EqualsExpr(order.WeightStrExpr, key.Expr) {
			return key.KeyCol, key.WeightStringCol, true
		}
	}
	for _, aggregate := range plan.eaggr.Aggregates {
		if sqlparser.EqualsExpr(order.WeightStrExpr, aggregate.Expr) {
			return aggregate.Col, -1, true
		}
	}
	return 0, 0, false
}

func (hp *horizonPlanning) createMemorySortPlan(plan logicalPlan, orderExprs []abstract.OrderBy) (logicalPlan, error) {
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
		offset, weightStringOffset, added, err := wrapAndPushExpr(order.Inner.Expr, order.WeightStrExpr, plan, hp.semTable)
		if err != nil {
			return nil, err
		}
		hp.haveToTruncate(added)
		ms.eMemorySort.OrderBy = append(ms.eMemorySort.OrderBy, engine.OrderByParams{
			Col:               offset,
			WeightStringCol:   weightStringOffset,
			Desc:              order.Inner.Direction == sqlparser.DescOrder,
			StarColFixedIndex: offset,
		})
	}
	return ms, nil
}

func allLeft(orderExprs []abstract.OrderBy, semTable *semantics.SemTable, lhsTables semantics.TableSet) bool {
	for _, expr := range orderExprs {
		exprDependencies := semTable.BaseTableDependencies(expr.Inner.Expr)
		if !exprDependencies.IsSolvedBy(lhsTables) {
			return false
		}
	}
	return true
}

func (hp *horizonPlanning) planDistinct() error {
	if !hp.qp.NeedsDistinct() {
		return nil
	}
	switch p := hp.plan.(type) {
	case *route:
		// we always make the underlying query distinct,
		// and then we might also add a distinct operator on top if it is needed
		p.Select.MakeDistinct()
		if !p.isSingleShard() && !selectHasUniqueVindex(hp.vschema, hp.semTable, hp.qp.SelectExprs) {
			return hp.addDistinct()
		}
		return nil
	case *joinGen4:
		return hp.addDistinct()
	case *orderedAggregate:
		return hp.planDistinctOA(p)
	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown plan type for DISTINCT %T", hp.plan)
	}
}

func (hp *horizonPlanning) planDistinctOA(currPlan *orderedAggregate) error {
	eaggr := &engine.OrderedAggregate{}
	oa := &orderedAggregate{
		resultsBuilder: resultsBuilder{
			logicalPlanCommon: newBuilderCommon(hp.plan),
			weightStrings:     make(map[*resultColumn]int),
			truncater:         eaggr,
		},
		eaggr: eaggr,
	}
	for _, sExpr := range hp.qp.SelectExprs {
		found := false
		for _, grpParam := range currPlan.eaggr.GroupByKeys {
			if sqlparser.EqualsExpr(sExpr.Col.Expr, grpParam.Expr) {
				found = true
				eaggr.GroupByKeys = append(eaggr.GroupByKeys, grpParam)
				break
			}
		}
		if found {
			continue
		}
		for _, aggrParam := range currPlan.eaggr.Aggregates {
			if sqlparser.EqualsExpr(sExpr.Col.Expr, aggrParam.Expr) {
				found = true
				eaggr.GroupByKeys = append(eaggr.GroupByKeys, &engine.GroupByParams{KeyCol: aggrParam.Col, WeightStringCol: -1})
				break
			}
		}
		if !found {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unable to plan distinct query as the column is not projected: %s", sqlparser.String(sExpr.Col))
		}
	}
	hp.plan = oa
	return nil
}

func (hp *horizonPlanning) addDistinct() error {
	eaggr := &engine.OrderedAggregate{}
	var orderExprs []abstract.OrderBy
	for index, sExpr := range hp.qp.SelectExprs {
		if isAmbiguousOrderBy(index, sExpr.Col.As, hp.qp.SelectExprs) {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "generating order by clause: ambiguous symbol reference: %s", sqlparser.String(sExpr.Col.As))
		}
		grpParam := &engine.GroupByParams{KeyCol: index, WeightStringCol: -1}
		_, wOffset, added, err := wrapAndPushExpr(sExpr.Col.Expr, sExpr.Col.Expr, hp.plan, hp.semTable)
		if err != nil {
			return err
		}
		hp.needsTruncation = hp.needsTruncation || added
		grpParam.WeightStringCol = wOffset
		eaggr.GroupByKeys = append(eaggr.GroupByKeys, grpParam)

		var inner sqlparser.Expr
		if !sExpr.Col.As.IsEmpty() {
			inner = sqlparser.NewColName(sExpr.Col.As.String())
			hp.semTable.CopyDependencies(sExpr.Col.Expr, inner)
		} else {
			inner = sExpr.Col.Expr
		}
		orderExprs = append(orderExprs, abstract.OrderBy{
			Inner:         &sqlparser.Order{Expr: inner},
			WeightStrExpr: sExpr.Col.Expr},
		)
	}
	innerPlan, err := hp.planOrderBy(orderExprs, hp.plan)
	if err != nil {
		return err
	}
	hp.plan = &orderedAggregate{
		resultsBuilder: resultsBuilder{
			logicalPlanCommon: newBuilderCommon(innerPlan),
			weightStrings:     make(map[*resultColumn]int),
			truncater:         eaggr,
		},
		eaggr: eaggr,
	}
	return nil
}

func isAmbiguousOrderBy(index int, col sqlparser.ColIdent, exprs []abstract.SelectExpr) bool {
	if col.String() == "" {
		return false
	}
	for i, expr := range exprs {
		if i == index {
			continue
		}
		alias := expr.Col.As
		if alias.IsEmpty() {
			if col, ok := expr.Col.Expr.(*sqlparser.ColName); ok {
				alias = col.Name
			}
		}
		if col.Equal(alias) {
			return true
		}
	}
	return false
}

func selectHasUniqueVindex(vschema ContextVSchema, semTable *semantics.SemTable, sel []abstract.SelectExpr) bool {
	for _, expr := range sel {
		if exprHasUniqueVindex(vschema, semTable, expr.Col.Expr) {
			return true
		}
	}
	return false
}

// needDistinctHandling returns true if oa needs to handle the distinct clause.
// If true, it will also return the aliased expression that needs to be pushed
// down into the underlying route.
func (hp *horizonPlanning) needDistinctHandling(funcExpr *sqlparser.FuncExpr, opcode engine.AggregateOpcode, input logicalPlan) (bool, *sqlparser.AliasedExpr, error) {
	if !funcExpr.Distinct {
		return false, nil, nil
	}
	if opcode != engine.AggregateCount && opcode != engine.AggregateSum {
		return false, nil, nil
	}
	innerAliased, ok := funcExpr.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return false, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "syntax error: %s", sqlparser.String(funcExpr))
	}
	_, ok = input.(*route)
	if !ok {
		// Unreachable
		return true, innerAliased, nil
	}
	if exprHasUniqueVindex(hp.vschema, hp.semTable, innerAliased.Expr) {
		// if we can see a unique vindex on this table/column,
		// we know the results will be unique, and we don't need to DISTINCTify them
		return false, nil, nil
	}
	return true, innerAliased, nil
}
