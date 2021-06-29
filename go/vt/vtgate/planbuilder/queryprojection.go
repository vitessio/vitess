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
	"strconv"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type queryProjection struct {
	selectExprs             []*sqlparser.AliasedExpr
	aggrExprs               []*sqlparser.AliasedExpr
	groupOrderingCommonExpr map[sqlparser.Expr]*sqlparser.Order

	orderExprs sqlparser.OrderBy

	// orderExprColMap keeps a map between the Order object and the offset into the select expressions list
	orderExprColMap map[*sqlparser.Order]int
}

func newQueryProjection() *queryProjection {
	return &queryProjection{
		groupOrderingCommonExpr: map[sqlparser.Expr]*sqlparser.Order{},
		orderExprColMap:         map[*sqlparser.Order]int{},
	}
}

func createQPFromSelect(sel *sqlparser.Select) (*queryProjection, error) {
	qp := newQueryProjection()

	for _, selExp := range sel.SelectExprs {
		exp, ok := selExp.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, semantics.Gen4NotSupportedF("%T in select list", selExp)
		}
		fExpr, ok := exp.Expr.(*sqlparser.FuncExpr)
		if ok && fExpr.IsAggregate() {
			if len(fExpr.Exprs) != 1 {
				return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.SyntaxError, "aggregate functions take a single argument '%s'", sqlparser.String(fExpr))
			}
			qp.aggrExprs = append(qp.aggrExprs, exp)
			continue
		}
		if nodeHasAggregates(exp.Expr) {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: complex aggregate expression")
		}
		qp.selectExprs = append(qp.selectExprs, exp)
	}

	qp.orderExprs = sel.OrderBy

	allExpr := append(qp.selectExprs, qp.aggrExprs...)

	for _, order := range sel.OrderBy {
		findOrderExprInAllExprs(order, allExpr, qp)
	}

	if sel.GroupBy == nil || sel.OrderBy == nil {
		return qp, nil
	}

	return qp, nil
}

func findOrderExprInAllExprs(order *sqlparser.Order, allExpr []*sqlparser.AliasedExpr, qp *queryProjection) {
	colExpr, isColName := order.Expr.(*sqlparser.ColName)
	// Case 1: Order by expression is an aliased expression in the select clause
	// Eg - select id as foo from music order by foo
	if isColName && colExpr.Qualifier.IsEmpty() {
		for offset, expr := range allExpr {
			isAliasExpr := !expr.As.IsEmpty()
			if isAliasExpr && colExpr.Name.Equal(expr.As) {
				qp.orderExprColMap[order] = offset
				return
			}
		}
	}
	// Case 2: Order by is the column offset to be used from the select expressions
	// Eg - select id from music order by 1
	literalExpr, isLiteral := order.Expr.(*sqlparser.Literal)
	if isLiteral && literalExpr.Type == sqlparser.IntVal {
		num, _ := strconv.Atoi(literalExpr.Val)
		qp.orderExprColMap[order] = num - 1
	} else {
		// Case 3: Order by is an expression that we already have in the select expression
		// Eg - select id from music order by id
		for offset, expr := range allExpr {
			if sqlparser.EqualsExpr(order.Expr, expr.Expr) {
				qp.orderExprColMap[order] = offset
				return
			}
		}
	}
}
