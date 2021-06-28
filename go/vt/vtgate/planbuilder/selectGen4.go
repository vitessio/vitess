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

func pushProjection(expr *sqlparser.AliasedExpr, plan logicalPlan, semTable *semantics.SemTable, inner bool) (int, error) {
	switch node := plan.(type) {
	case *route:
		value, err := makePlanValue(expr.Expr)
		if err != nil {
			return 0, err
		}
		_, isColName := expr.Expr.(*sqlparser.ColName)
		badExpr := value == nil && !isColName
		if !inner && badExpr {
			return 0, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard left join and column expressions")
		}
		sel := node.Select.(*sqlparser.Select)
		i := checkIfAlreadyExists(expr, sel)
		if i != -1 {
			return i, nil
		}
		expr = removeQualifierFromColName(expr)

		offset := len(sel.SelectExprs)
		sel.SelectExprs = append(sel.SelectExprs, expr)
		return offset, nil
	case *joinGen4:
		lhsSolves := node.Left.ContainsTables()
		rhsSolves := node.Right.ContainsTables()
		deps := semTable.Dependencies(expr.Expr)
		switch {
		case deps.IsSolvedBy(lhsSolves):
			offset, err := pushProjection(expr, node.Left, semTable, inner)
			if err != nil {
				return 0, err
			}
			node.Cols = append(node.Cols, -(offset + 1))
		case deps.IsSolvedBy(rhsSolves):
			offset, err := pushProjection(expr, node.Right, semTable, inner && node.Opcode != engine.LeftJoin)
			if err != nil {
				return 0, err
			}
			node.Cols = append(node.Cols, offset+1)
		default:
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown dependencies for %s", sqlparser.String(expr))
		}
		return len(node.Cols) - 1, nil
	default:
		return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T not yet supported", node)
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
			if sqlparser.EqualsExpr(selectExpr.Expr, expr.Expr) {
				return i
			}
		}
	}
	return -1
}

func planAggregations(qp *queryProjection, plan logicalPlan, semTable *semantics.SemTable) (logicalPlan, error) {
	eaggr := &engine.OrderedAggregate{}
	oa := &orderedAggregate{
		resultsBuilder: newResultsBuilder(plan, eaggr),
		eaggr:          eaggr,
	}
	for _, e := range qp.aggrExprs {
		offset, err := pushProjection(e, plan, semTable, true)
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

func planOrderBy(qp *queryProjection, plan logicalPlan, semTable *semantics.SemTable) (logicalPlan, error) {
	switch plan := plan.(type) {
	case *route:
		additionalColAdded := false
		for _, order := range qp.orderExprs {
			offset, exists := qp.orderExprColMap[order]
			colName, ok := order.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, semantics.Gen4NotSupportedF("order by non-column expression")
			}
			if !exists {
				expr := &sqlparser.AliasedExpr{
					Expr: order.Expr,
				}
				var err error
				offset, err = pushProjection(expr, plan, semTable, true)
				if err != nil {
					return nil, err
				}
				additionalColAdded = true
			}

			table := semTable.Dependencies(colName)
			tbl, err := semTable.TableInfoFor(table)
			if err != nil {
				return nil, err
			}
			weightStringNeeded := true
			for _, c := range tbl.GetColumns() {
				if colName.Name.String() == c.Name {
					if sqltypes.IsNumber(c.Type) {
						weightStringNeeded = false
					}
					break
				}
			}

			weightStringOffset := -1
			if weightStringNeeded {
				expr := &sqlparser.AliasedExpr{
					Expr: &sqlparser.FuncExpr{
						Name: sqlparser.NewColIdent("weight_string"),
						Exprs: []sqlparser.SelectExpr{
							&sqlparser.AliasedExpr{
								Expr: order.Expr,
							},
						},
					},
				}
				weightStringOffset, err = pushProjection(expr, plan, semTable, true)
				if err != nil {
					return nil, err
				}
				additionalColAdded = true
			}

			plan.eroute.OrderBy = append(plan.eroute.OrderBy, engine.OrderbyParams{
				Col:             offset,
				WeightStringCol: weightStringOffset,
				Desc:            order.Direction == sqlparser.DescOrder,
			})
			plan.Select.AddOrder(order)
		}
		if additionalColAdded {
			plan.eroute.TruncateColumnCount = len(qp.selectExprs) + len(qp.aggrExprs)
		}

		return plan, nil
	default:
		return nil, semantics.Gen4NotSupportedF("ordering on complex query")
	}
}
