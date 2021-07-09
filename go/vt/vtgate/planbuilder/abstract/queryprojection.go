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

package abstract

import (
	"strconv"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type se struct {
	Col  *sqlparser.AliasedExpr
	Aggr bool
}

type QueryProjection struct {
	SelectExprs  []se
	HasAggr      bool
	GroupByExprs sqlparser.Exprs
	OrderExprs   []OrderBy
}

type OrderBy struct {
	Inner         *sqlparser.Order
	WeightStrExpr sqlparser.Expr
}

func newQueryProjection() *QueryProjection {
	return &QueryProjection{}
}

func CreateQPFromSelect(sel *sqlparser.Select) (*QueryProjection, error) {
	qp := newQueryProjection()

	hasNonAggr := false
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
			if fExpr.Distinct {
				return nil, semantics.Gen4NotSupportedF("distinct aggregation")
			}
			qp.HasAggr = true
			qp.SelectExprs = append(qp.SelectExprs, se{
				Col:  exp,
				Aggr: true,
			})
			continue
		}
		
		if sqlparser.ContainsAggregation(exp.Expr) {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: complex aggregate expression")
		}
		hasNonAggr = true
		qp.SelectExprs = append(qp.SelectExprs, se{Col: exp})
	}

	if hasNonAggr && qp.HasAggr && sel.GroupBy == nil {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.MixOfGroupFuncAndFields, "Mixing of aggregation and non-aggregation columns is not allowed if there is no GROUP BY clause")
	}

	qp.GroupByExprs = sqlparser.Exprs(sel.GroupBy)

	for _, order := range sel.OrderBy {
		qp.addOrderBy(order, qp.SelectExprs)
	}
	return qp, nil
}

func (qp *QueryProjection) addOrderBy(order *sqlparser.Order, allExpr []se) {
	// Order by is the column offset to be used from the select expressions
	// Eg - select id from music order by 1
	literalExpr, isLiteral := order.Expr.(*sqlparser.Literal)
	if isLiteral && literalExpr.Type == sqlparser.IntVal {
		num, _ := strconv.Atoi(literalExpr.Val)
		aliasedExpr := allExpr[num-1].Col
		expr := aliasedExpr.Expr
		if !aliasedExpr.As.IsEmpty() {
			// the column is aliased, so we'll add an expression ordering by the alias and not the underlying expression
			expr = &sqlparser.ColName{
				Name: aliasedExpr.As,
			}
		}
		qp.OrderExprs = append(qp.OrderExprs, OrderBy{
			Inner: &sqlparser.Order{
				Expr:      expr,
				Direction: order.Direction,
			},
			WeightStrExpr: aliasedExpr.Expr,
		})
		return
	}

	// If the ORDER BY is against a column alias, we need to remember the expression
	// behind the alias. The weightstring(.) calls needs to be done against that expression and not the alias.
	// Eg - select music.foo as bar, weightstring(music.foo) from music order by bar
	colExpr, isColName := order.Expr.(*sqlparser.ColName)
	if isColName && colExpr.Qualifier.IsEmpty() {
		for _, selectExpr := range allExpr {
			isAliasExpr := !selectExpr.Col.As.IsEmpty()
			if isAliasExpr && colExpr.Name.Equal(selectExpr.Col.As) {
				qp.OrderExprs = append(qp.OrderExprs, OrderBy{
					Inner:         order,
					WeightStrExpr: selectExpr.Col.Expr,
				})
				return
			}
		}
	}

	qp.OrderExprs = append(qp.OrderExprs, OrderBy{
		Inner:         order,
		WeightStrExpr: order.Expr,
	})
}
