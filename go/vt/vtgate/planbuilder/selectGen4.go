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

func pushProjection(expr *sqlparser.AliasedExpr, plan logicalPlan, semTable *semantics.SemTable) (int, error) {
	switch node := plan.(type) {
	case *route:
		sel := node.Select.(*sqlparser.Select)
		offset := len(sel.SelectExprs)
		sel.SelectExprs = append(sel.SelectExprs, expr)
		return offset, nil
	case *joinGen4:
		lhsSolves := node.Left.ContainsTables()
		rhsSolves := node.Right.ContainsTables()
		deps := semTable.Dependencies(expr.Expr)
		switch {
		case deps.IsSolvedBy(lhsSolves):
			offset, err := pushProjection(expr, node.Left, semTable)
			if err != nil {
				return 0, err
			}
			node.Cols = append(node.Cols, -(offset + 1))
		case deps.IsSolvedBy(rhsSolves):
			offset, err := pushProjection(expr, node.Right, semTable)
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

func planAggregations(qp *queryProjection, plan logicalPlan, semTable *semantics.SemTable) (logicalPlan, error) {
	eaggr := &engine.OrderedAggregate{}
	oa := &orderedAggregate{
		resultsBuilder: newResultsBuilder(plan, eaggr),
		eaggr:          eaggr,
	}
	for _, e := range qp.aggrExprs {
		offset, err := pushProjection(e, plan, semTable)
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
				offset, err = pushProjection(expr, plan, semTable)
				if err != nil {
					return nil, err
				}
				additionalColAdded = true
			}

			table := semTable.Dependencies(colName)
			tableInfo, err := semTable.TableInfoFor(table)
			if err != nil {
				return nil, err
			}
			weightStringNeeded := true
			for _, c := range tableInfo.Table.Columns {
				if colName.Name.Equal(c.Name) {
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
				weightStringOffset, err = pushProjection(expr, plan, semTable)
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
