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
	"encoding/json"
	"strconv"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// SelectExpr provides whether the columns is aggregation expression or not.
	SelectExpr struct {
		Col  *sqlparser.AliasedExpr
		Aggr bool
	}

	// QueryProjection  contains the information about the projections, group by and order by expressions used to do horizon planning.
	QueryProjection struct {
		SelectExprs  []SelectExpr
		HasAggr      bool
		GroupByExprs []GroupBy
		OrderExprs   []OrderBy
	}

	// OrderBy contains the expression to used in order by and also if ordering is needed at VTGate level then what the weight_string function expression to be sent down for evaluation.
	OrderBy struct {
		Inner         *sqlparser.Order
		WeightStrExpr sqlparser.Expr
	}

	// GroupBy contains the expression to used in group by and also if grouping is needed at VTGate level then what the weight_string function expression to be sent down for evaluation.
	GroupBy struct {
		Inner         sqlparser.Expr
		WeightStrExpr sqlparser.Expr
	}
)

// CreateQPFromSelect created the QueryProjection for the input *sqlparser.Select
func CreateQPFromSelect(sel *sqlparser.Select) (*QueryProjection, error) {
	qp := &QueryProjection{}

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
			qp.SelectExprs = append(qp.SelectExprs, SelectExpr{
				Col:  exp,
				Aggr: true,
			})
			continue
		}

		if sqlparser.ContainsAggregation(exp.Expr) {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: complex aggregate expression")
		}
		hasNonAggr = true
		qp.SelectExprs = append(qp.SelectExprs, SelectExpr{Col: exp})
	}

	if hasNonAggr && qp.HasAggr && sel.GroupBy == nil {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.MixOfGroupFuncAndFields, "Mixing of aggregation and non-aggregation columns is not allowed if there is no GROUP BY clause")
	}

	for _, group := range sel.GroupBy {
		// todo dont ignore weightstringexpr
		expr, weightStrExpr := qp.getSimplifiedExpr(group)
		qp.GroupByExprs = append(qp.GroupByExprs, GroupBy{Inner: expr, WeightStrExpr: weightStrExpr})
	}

	for _, order := range sel.OrderBy {
		expr, weightStrExpr := qp.getSimplifiedExpr(order.Expr)
		qp.OrderExprs = append(qp.OrderExprs, OrderBy{
			Inner: &sqlparser.Order{
				Expr:      expr,
				Direction: order.Direction,
			},
			WeightStrExpr: weightStrExpr,
		})
	}

	return qp, nil
}

// getSimplifiedExpr takes an expression used in ORDER BY or GROUP BY, which can reference both aliased columns and
// column offsets, and returns an expression that is simpler to evaluate
func (qp *QueryProjection) getSimplifiedExpr(e sqlparser.Expr) (expr sqlparser.Expr, weightStrExpr sqlparser.Expr) {
	// Order by is the column offset to be used from the select expressions
	// Eg - select id from music order by 1
	literalExpr, isLiteral := e.(*sqlparser.Literal)
	if isLiteral && literalExpr.Type == sqlparser.IntVal {
		num, _ := strconv.Atoi(literalExpr.Val)
		aliasedExpr := qp.SelectExprs[num-1].Col
		expr = aliasedExpr.Expr
		if !aliasedExpr.As.IsEmpty() {
			// the column is aliased, so we'll add an expression ordering by the alias and not the underlying expression
			expr = &sqlparser.ColName{
				Name: aliasedExpr.As,
			}
		}

		return expr, aliasedExpr.Expr
	}

	// If the ORDER BY is against a column alias, we need to remember the expression
	// behind the alias. The weightstring(.) calls needs to be done against that expression and not the alias.
	// Eg - select music.foo as bar, weightstring(music.foo) from music order by bar
	colExpr, isColName := e.(*sqlparser.ColName)
	if isColName && colExpr.Qualifier.IsEmpty() {
		for _, selectExpr := range qp.SelectExprs {
			isAliasExpr := !selectExpr.Col.As.IsEmpty()
			if isAliasExpr && colExpr.Name.Equal(selectExpr.Col.As) {
				return e, selectExpr.Col.Expr
			}
		}
	}

	return e, e
}

func (qp *QueryProjection) toString() string {
	/*
		QueryProjection struct {
			SelectExprs  []SelectExpr
			HasAggr      bool
			GroupByExprs sqlparser.Exprs
			OrderExprs   []OrderBy
		}

	*/
	type output struct {
		Select   []string
		Grouping []string
		OrderBy  []string
	}
	out := output{
		Select:   []string{},
		Grouping: []string{},
		OrderBy:  []string{},
	}

	for _, expr := range qp.SelectExprs {
		e := sqlparser.String(expr.Col.Expr)

		if expr.Aggr {
			e = "aggr: " + e
		}

		if !expr.Col.As.IsEmpty() {
			e += " AS " + expr.Col.As.String()
		}
		out.Select = append(out.Select, e)
	}

	for _, expr := range qp.GroupByExprs {
		out.Grouping = append(out.Grouping, sqlparser.String(expr.Inner))
	}
	for _, expr := range qp.OrderExprs {
		out.OrderBy = append(out.OrderBy, sqlparser.String(expr.Inner))
	}

	bytes, _ := json.MarshalIndent(out, "", "  ")
	return string(bytes)
}
