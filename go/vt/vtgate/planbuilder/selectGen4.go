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

	"vitess.io/vitess/go/vt/vtgate/semantics"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func pushProjection(expr *sqlparser.AliasedExpr, plan logicalPlan, semTable *semantics.SemTable, inner bool) (int, bool, error) {
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
		i := checkIfAlreadyExists(expr, sel)
		if i != -1 {
			return i, false, nil
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
			offset, added, err := pushProjection(expr, node.Left, semTable, inner)
			if err != nil {
				return 0, false, err
			}
			column = -(offset + 1)
			appended = added
		case deps.IsSolvedBy(rhsSolves):
			offset, added, err := pushProjection(expr, node.Right, semTable, inner && node.Opcode != engine.LeftJoin)
			if err != nil {
				return 0, false, err
			}
			column = offset + 1
			appended = added
		default:
			return 0, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown dependencies for %s", sqlparser.String(expr))
		}
		if !appended {
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

func planAggregations(qp *queryProjection, plan logicalPlan, semTable *semantics.SemTable) (logicalPlan, error) {
	eaggr := &engine.OrderedAggregate{}
	oa := &orderedAggregate{
		resultsBuilder: resultsBuilder{
			logicalPlanCommon: newBuilderCommon(plan),
			weightStrings:     make(map[*resultColumn]int),
			truncater:         eaggr,
		},
		eaggr: eaggr,
	}
	for _, e := range qp.aggrExprs {
		offset, _, err := pushProjection(e, plan, semTable, true)
		if err != nil {
			return nil, err
		}
		fExpr := e.Expr.(*sqlparser.FuncExpr)
		opcode := engine.SupportedAggregates[fExpr.Name.Lowered()]
		oa.eaggr.Aggregates = append(oa.eaggr.Aggregates, engine.AggregateParams{
			Opcode: opcode,
			Col:    offset,
		})
	}
	return oa, nil
}

func planOrderBy(qp *queryProjection, orderExprs []orderBy, plan logicalPlan, semTable *semantics.SemTable) (logicalPlan, error) {
	switch plan := plan.(type) {
	case *route:
		return planOrderByForRoute(orderExprs, plan, semTable)
	case *joinGen4:
		return planOrderByForJoin(qp, orderExprs, plan, semTable)
	default:
		return nil, semantics.Gen4NotSupportedF("ordering on complex query")
	}
}

func planOrderByForRoute(orderExprs []orderBy, plan *route, semTable *semantics.SemTable) (logicalPlan, error) {
	origColCount := plan.Select.GetColumnCount()
	for _, order := range orderExprs {
		expr := order.inner.Expr
		offset, err := wrapExprAndPush(expr, plan, semTable)
		if err != nil {
			return nil, err
		}
		colName, ok := order.inner.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, semantics.Gen4NotSupportedF("order by non-column expression")
		}

		table := semTable.Dependencies(colName)
		tbl, err := semTable.TableInfoFor(table)
		if err != nil {
			return nil, err
		}
		weightStringNeeded := needsWeightString(tbl, colName)

		weightStringOffset := -1
		if weightStringNeeded {
			weightStringOffset, err = wrapExprAndPush(weightStringFor(order.weightStrExpr), plan, semTable)
			if err != nil {
				return nil, err
			}
		}

		plan.eroute.OrderBy = append(plan.eroute.OrderBy, engine.OrderbyParams{
			Col:             offset,
			WeightStringCol: weightStringOffset,
			Desc:            order.inner.Direction == sqlparser.DescOrder,
		})
		plan.Select.AddOrder(order.inner)
	}
	if origColCount != plan.Select.GetColumnCount() {
		plan.eroute.TruncateColumnCount = origColCount
	}

	return plan, nil
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

func wrapExprAndPush(exp sqlparser.Expr, plan logicalPlan, semTable *semantics.SemTable) (int, error) {
	aliasedExpr := &sqlparser.AliasedExpr{Expr: exp}
	offset, _, err := pushProjection(aliasedExpr, plan, semTable, true)
	return offset, err
}

func planOrderByForJoin(qp *queryProjection, orderExprs []orderBy, plan *joinGen4, semTable *semantics.SemTable) (logicalPlan, error) {
	if allLeft(orderExprs, semTable, plan.Left.ContainsTables()) {
		newLeft, err := planOrderBy(qp, orderExprs, plan.Left, semTable)
		if err != nil {
			return nil, err
		}
		plan.Left = newLeft
		return plan, nil
	}

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
		expr := order.inner.Expr
		offset, err := wrapExprAndPush(expr, plan, semTable)
		if err != nil {
			return nil, err
		}

		table := semTable.Dependencies(expr)
		tbl, err := semTable.TableInfoFor(table)
		if err != nil {
			return nil, err
		}
		col, isCol := expr.(*sqlparser.ColName)
		if !isCol {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "order by complex expression not supported")
		}

		weightStringOffset := -1
		if needsWeightString(tbl, col) {
			weightStringOffset, err = wrapExprAndPush(weightStringFor(order.weightStrExpr), plan, semTable)
			if err != nil {
				return nil, err
			}
		}

		ms.eMemorySort.OrderBy = append(ms.eMemorySort.OrderBy, engine.OrderbyParams{
			Col:               offset,
			WeightStringCol:   weightStringOffset,
			Desc:              order.inner.Direction == sqlparser.DescOrder,
			StarColFixedIndex: offset,
		})
	}

	return ms, nil

}

func allLeft(orderExprs []orderBy, semTable *semantics.SemTable, lhsTables semantics.TableSet) bool {
	for _, expr := range orderExprs {
		exprDependencies := semTable.Dependencies(expr.inner.Expr)
		if !exprDependencies.IsSolvedBy(lhsTables) {
			return false
		}
	}
	return true
}
