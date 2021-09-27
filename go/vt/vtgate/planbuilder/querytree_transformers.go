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

	"vitess.io/vitess/go/sqltypes"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"
)

func transformToLogicalPlan(ctx *planningContext, tree queryTree) (logicalPlan, error) {
	switch n := tree.(type) {
	case *routeTree:
		return transformRoutePlan(ctx, n)
	case *joinTree:
		return transformJoinPlan(ctx, n)
	case *derivedTree:
		return transformDerivedPlan(ctx, n)
	case *subqueryTree:
		return transformSubqueryTree(ctx, n)
	case *concatenateTree:
		return transformConcatenatePlan(ctx, n)
	case *vindexTree:
		return transformVindexTree(n)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unknown query tree encountered: %T", tree)
}

func pushDistinct(plan logicalPlan) {
	switch n := plan.(type) {
	case *route:
		n.Select.MakeDistinct()
	case *concatenateGen4:
		for _, source := range n.sources {
			pushDistinct(source)
		}
	}
}

func transformVindexTree(n *vindexTree) (logicalPlan, error) {
	single, ok := n.vindex.(vindexes.SingleColumn)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "multi-column vindexes not supported")
	}
	plan := &vindexFunc{
		order:         1,
		tableID:       n.solved,
		resultColumns: nil,
		eVindexFunc: &engine.VindexFunc{
			Opcode: n.opCode,
			Vindex: single,
			Value:  n.value,
		},
	}

	for _, col := range n.columns {
		_, err := plan.SupplyProjection(&sqlparser.AliasedExpr{
			Expr: col,
			As:   sqlparser.ColIdent{},
		}, false)
		if err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func transformSubqueryTree(ctx *planningContext, n *subqueryTree) (logicalPlan, error) {
	innerPlan, err := transformToLogicalPlan(ctx, n.inner)
	if err != nil {
		return nil, err
	}
	innerPlan, err = planHorizon(ctx, innerPlan, n.subquery)
	if err != nil {
		return nil, err
	}

	plan := newPulloutSubquery(n.opcode, n.argName, n.hasValues, innerPlan)
	outerPlan, err := transformToLogicalPlan(ctx, n.outer)
	if err != nil {
		return nil, err
	}
	plan.underlying = outerPlan
	return plan, err
}

func transformDerivedPlan(ctx *planningContext, n *derivedTree) (logicalPlan, error) {
	// transforming the inner part of the derived table into a logical plan
	// so that we can do horizon planning on the inner. If the logical plan
	// we've produced is a Route, we set its Select.From field to be an aliased
	// expression containing our derived table's inner select and the derived
	// table's alias.

	plan, err := transformToLogicalPlan(ctx, n.inner)
	if err != nil {
		return nil, err
	}
	plan, err = planHorizon(ctx, plan, n.query)
	if err != nil {
		return nil, err
	}

	rb, isRoute := plan.(*route)
	if !isRoute {
		return &simpleProjection{
			logicalPlanCommon: newBuilderCommon(plan),
			eSimpleProj: &engine.SimpleProjection{
				Cols: n.columnsOffset,
			},
		}, nil
	}
	innerSelect := rb.Select
	derivedTable := &sqlparser.DerivedTable{Select: innerSelect}
	tblExpr := &sqlparser.AliasedTableExpr{
		Expr: derivedTable,
		As:   sqlparser.NewTableIdent(n.alias),
	}
	selectExprs := sqlparser.SelectExprs{}
	for _, colName := range n.columns {
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

func transformConcatenatePlan(ctx *planningContext, n *concatenateTree) (logicalPlan, error) {
	var sources []logicalPlan

	for i, source := range n.sources {
		plan, err := createLogicalPlan(ctx, source, n.selectStmts[i])
		if err != nil {
			return nil, err
		}
		if i == 0 {
			sources = append(sources, plan)
			continue
		}
		last := sources[len(sources)-1]
		newPlan := mergeUnionLogicalPlans(ctx, last, plan)
		if newPlan != nil {
			sources[len(sources)-1] = newPlan
			continue
		}
		sources = append(sources, plan)
	}

	if n.distinct {
		for _, source := range sources {
			pushDistinct(source)
		}
	}

	var result logicalPlan
	if len(sources) == 1 {
		src := sources[0]
		if rb, isRoute := src.(*route); isRoute && rb.isSingleShard() {
			// if we have a single shard route, we don't need to do anything to make it distinct
			rb.Select.SetLimit(n.limit)
			rb.Select.SetOrderBy(n.ordering)
			return src, nil
		}
		result = src
	} else {
		if len(n.ordering) > 0 {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't do ORDER BY on top of UNION")
		}
		result = &concatenateGen4{sources: sources}
	}
	if n.distinct {
		return newDistinct(result), nil
	}
	return result, nil
}

func mergeUnionLogicalPlans(ctx *planningContext, left logicalPlan, right logicalPlan) logicalPlan {
	lroute, ok := left.(*route)
	if !ok {
		return nil
	}
	rroute, ok := right.(*route)
	if !ok {
		return nil
	}

	if canMergePlans(ctx, lroute, rroute) {
		lroute.Select = &sqlparser.Union{Left: lroute.Select, Distinct: false, Right: rroute.Select}
		return lroute
	}
	return nil
}

func createLogicalPlan(ctx *planningContext, source queryTree, selStmt *sqlparser.Select) (logicalPlan, error) {
	plan, err := transformToLogicalPlan(ctx, source)
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

func transformRoutePlan(ctx *planningContext, n *routeTree) (*route, error) {
	var tablesForSelect sqlparser.TableExprs
	tableNameMap := map[string]interface{}{}

	sort.Sort(n.tables)
	for _, t := range n.tables {
		tableExpr, err := relToTableExpr(t)
		if err != nil {
			return nil, err
		}
		tablesForSelect = append(tablesForSelect, tableExpr)
		for _, name := range t.tableNames() {
			tableNameMap[name] = nil
		}
	}

	if n.selected != nil {
		for _, predicate := range n.selected.predicates {
			switch predicate := predicate.(type) {
			case *sqlparser.ComparisonExpr:
				if predicate.Operator == sqlparser.InOp {
					switch predicate.Left.(type) {
					case *sqlparser.ColName:
						predicate.Right = sqlparser.ListArg(engine.ListVarName)
					}
				}
			}
		}
	}

	for _, leftJoin := range n.leftJoins {
		var lft sqlparser.TableExpr
		if len(tablesForSelect) == 1 {
			lft = tablesForSelect[0]
		} else {
			lft = &sqlparser.ParenTableExpr{Exprs: tablesForSelect}
		}

		rightExpr, err := relToTableExpr(leftJoin.right)
		if err != nil {
			return nil, err
		}
		joinExpr := &sqlparser.JoinTableExpr{
			Join: sqlparser.LeftJoinType,
			Condition: &sqlparser.JoinCondition{
				On: leftJoin.pred,
			},
			RightExpr: rightExpr,
			LeftExpr:  lft,
		}
		tablesForSelect = sqlparser.TableExprs{joinExpr}
		for _, tblNames := range leftJoin.right.tableNames() {
			tableNameMap[tblNames] = nil
		}
	}

	predicates := n.Predicates()
	var where *sqlparser.Where
	if predicates != nil {
		where = &sqlparser.Where{Expr: predicates, Type: sqlparser.WhereClause}
	}

	var singleColumn vindexes.SingleColumn
	var values []sqltypes.PlanValue
	if n.selectedVindex() != nil {
		singleColumn = n.selected.foundVindex.(vindexes.SingleColumn)
		values = n.selected.values
	}

	var expressions sqlparser.SelectExprs
	for _, col := range n.columns {
		expressions = append(expressions, &sqlparser.AliasedExpr{
			Expr: col,
			As:   sqlparser.ColIdent{},
		})
	}

	var tableNames []string
	for name := range tableNameMap {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	sel := &sqlparser.Select{
		SelectExprs: expressions,
		From:        tablesForSelect,
		Where:       where,
		Comments:    ctx.semTable.Comments,
	}

	replaceSubQuery(ctx.sqToReplace, ctx.sqToReplaceExpr, sel)

	// TODO clean up when gen4 is the only planner
	var condition sqlparser.Expr
	if n.selected != nil && len(n.selected.valueExprs) > 0 {
		condition = n.selected.valueExprs[0]
	}
	return &route{
		eroute: &engine.Route{
			Opcode:              n.routeOpCode,
			TableName:           strings.Join(tableNames, ", "),
			Keyspace:            n.keyspace,
			Vindex:              singleColumn,
			Values:              values,
			SysTableTableName:   n.SysTableTableName,
			SysTableTableSchema: n.SysTableTableSchema,
		},
		Select:    sel,
		tables:    n.solved,
		condition: condition,
	}, nil
}

func transformJoinPlan(ctx *planningContext, n *joinTree) (logicalPlan, error) {
	lhs, err := transformToLogicalPlan(ctx, n.lhs)
	if err != nil {
		return nil, err
	}
	rhs, err := transformToLogicalPlan(ctx, n.rhs)
	if err != nil {
		return nil, err
	}
	opCode := engine.InnerJoin
	if n.outer {
		opCode = engine.LeftJoin
	}
	return &joinGen4{
		Left:   lhs,
		Right:  rhs,
		Cols:   n.columns,
		Vars:   n.vars,
		Opcode: opCode,
	}, nil
}

func relToTableExpr(t relation) (sqlparser.TableExpr, error) {
	switch t := t.(type) {
	case *routeTable:
		var expr sqlparser.SimpleTableExpr
		if t.qtable.IsInfSchema {
			expr = t.qtable.Table
		} else {
			expr = sqlparser.TableName{
				Name: t.vtable.Name,
			}
		}
		return &sqlparser.AliasedTableExpr{
			Expr:       expr,
			Partitions: nil,
			As:         t.qtable.Alias.As,
			Hints:      t.qtable.Alias.Hints,
		}, nil
	case parenTables:
		tables := sqlparser.TableExprs{}
		for _, t := range t {
			tableExpr, err := relToTableExpr(t)
			if err != nil {
				return nil, err
			}
			tables = append(tables, tableExpr)
		}
		return &sqlparser.ParenTableExpr{Exprs: tables}, nil
	case *joinTables:
		lExpr, err := relToTableExpr(t.lhs)
		if err != nil {
			return nil, err
		}
		rExpr, err := relToTableExpr(t.rhs)
		if err != nil {
			return nil, err
		}
		return &sqlparser.JoinTableExpr{
			LeftExpr:  lExpr,
			Join:      sqlparser.NormalJoinType,
			RightExpr: rExpr,
			Condition: &sqlparser.JoinCondition{
				On: t.pred,
			},
		}, nil
	case *derivedTable:
		innerTables, err := relToTableExpr(t.tables)
		if err != nil {
			return nil, err
		}
		tbls := innerTables.(*sqlparser.ParenTableExpr)

		sel := &sqlparser.Select{
			SelectExprs: sqlparser.GetFirstSelect(t.query).SelectExprs,
			From:        tbls.Exprs,
			Where:       &sqlparser.Where{Expr: sqlparser.AndExpressions(t.predicates...)},
		}
		expr := &sqlparser.DerivedTable{
			Select: sel,
		}
		return &sqlparser.AliasedTableExpr{
			Expr:       expr,
			Partitions: nil,
			As:         sqlparser.NewTableIdent(t.alias),
		}, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unknown relation type: %T", t)
	}
}

type subQReplacer struct {
	sqToReplace     map[string]*sqlparser.Select
	sqToReplaceExpr map[sqlparser.Expr]sqlparser.Expr
	err             error
	replaced        bool
}

func (sqr *subQReplacer) replacer(cursor *sqlparser.Cursor) bool {
	var exprs []sqlparser.Expr
	switch node := cursor.Node().(type) {
	case *sqlparser.AndExpr:
		exprs = sqlparser.SplitAndExpression(nil, node)
	case *sqlparser.OrExpr:
		exprs = sqlparser.SplitOrExpression(nil, node)
	case sqlparser.Argument:
		exprs = append(exprs, node)
	case sqlparser.ListArg:
		exprs = append(exprs, node)
	case *sqlparser.ExistsExpr:
		exprs = append(exprs, node)
	default:
		return true
	}

	var replaceBy sqlparser.Expr
	var remainder sqlparser.Expr
	for _, expr := range exprs {
		found := false
		for sqExprToReplace, replaceByExpr := range sqr.sqToReplaceExpr {
			if sqlparser.EqualsExpr(expr, sqExprToReplace) {
				allReplaceByExprs := sqlparser.SplitAndExpression(nil, replaceBy)
				allReplaceByExprs = append(allReplaceByExprs, replaceByExpr)
				replaceBy = sqlparser.AndExpressions(allReplaceByExprs...)
				found = true
				break
			}
		}
		if !found {
			remainder = sqlparser.AndExpressions(remainder, expr)
		}
	}
	if replaceBy == nil {
		return true
	}
	newNode := sqlparser.AndExpressions(remainder, replaceBy)
	cursor.Replace(newNode)
	sqr.replaced = true
	return false
}

func replaceSubQuery(sqToReplace map[string]*sqlparser.Select, expr map[sqlparser.Expr]sqlparser.Expr, sel *sqlparser.Select) {
	if len(sqToReplace) > 0 {
		sqr := &subQReplacer{sqToReplace: sqToReplace, sqToReplaceExpr: expr}
		sqlparser.Rewrite(sel, sqr.replacer, nil)
		for sqr.replaced {
			// to handle subqueries inside subqueries, we need to do this again and again until no replacements are left
			sqr.replaced = false
			sqlparser.Rewrite(sel, sqr.replacer, nil)
		}
	}
}
