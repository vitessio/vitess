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
	selectExprs []*sqlparser.AliasedExpr
	aggrExprs   []*sqlparser.AliasedExpr
	orderExprs  []orderBy
}

type orderBy struct {
	inner         *sqlparser.Order
	weightStrExpr sqlparser.Expr
}

func newQueryProjection() *queryProjection {
	return &queryProjection{}
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

	if len(qp.selectExprs) > 0 && len(qp.aggrExprs) > 0 {
		return nil, semantics.Gen4NotSupportedF("aggregation and non-aggregation expressions, together are not supported in cross-shard query")
	}

	allExpr := append(qp.selectExprs, qp.aggrExprs...)

	for _, order := range sel.OrderBy {
		qp.addOrderBy(order, allExpr)
	}
	return qp, nil
}

func (qp *queryProjection) addOrderBy(order *sqlparser.Order, allExpr []*sqlparser.AliasedExpr) {
	// Order by is the column offset to be used from the select expressions
	// Eg - select id from music order by 1
	literalExpr, isLiteral := order.Expr.(*sqlparser.Literal)
	if isLiteral && literalExpr.Type == sqlparser.IntVal {
		num, _ := strconv.Atoi(literalExpr.Val)
		aliasedExpr := allExpr[num-1]
		expr := aliasedExpr.Expr
		if !aliasedExpr.As.IsEmpty() {
			// the column is aliased, so we'll add an expression ordering by the alias and not the underlying expression
			expr = &sqlparser.ColName{
				Name: aliasedExpr.As,
			}
		}
		qp.orderExprs = append(qp.orderExprs, orderBy{
			inner: &sqlparser.Order{
				Expr:      expr,
				Direction: order.Direction,
			},
			weightStrExpr: aliasedExpr.Expr,
		})
		return
	}

	// If the ORDER BY is against a column alias, we need to remember the expression
	// behind the alias. The weightstring(.) calls needs to be done against that expression and not the alias.
	// Eg - select music.foo as bar, weightstring(music.foo) from music order by bar
	colExpr, isColName := order.Expr.(*sqlparser.ColName)
	if isColName && colExpr.Qualifier.IsEmpty() {
		for _, expr := range allExpr {
			isAliasExpr := !expr.As.IsEmpty()
			if isAliasExpr && colExpr.Name.Equal(expr.As) {
				qp.orderExprs = append(qp.orderExprs, orderBy{
					inner:         order,
					weightStrExpr: expr.Expr,
				})
				return
			}
		}
	}

	qp.orderExprs = append(qp.orderExprs, orderBy{
		inner:         order,
		weightStrExpr: order.Expr,
	})
}
