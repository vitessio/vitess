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

	// QueryProjection contains the information about the projections, group by and order by expressions used to do horizon planning.
	QueryProjection struct {
		// If you change the contents here, please update the toString() method
		SelectExprs        []SelectExpr
		HasAggr            bool
		Distinct           bool
		GroupByExprs       []GroupBy
		OrderExprs         []OrderBy
		CanPushDownSorting bool
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

		// This is to add the distinct function expression in grouping column for pushing down but not be to used as grouping key at VTGate level.
		// Starts with 1 so that default (0) means unassigned.
		DistinctAggrIndex int
	}
)

// CreateQPFromSelect creates the QueryProjection for the input *sqlparser.Select
func CreateQPFromSelect(sel *sqlparser.Select, semTable *semantics.SemTable) (*QueryProjection, error) {
	qp := &QueryProjection{
		Distinct: sel.Distinct,
	}

	for _, selExp := range sel.SelectExprs {
		exp, ok := selExp.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, semantics.Gen4NotSupportedF("%T in select list", selExp)
		}

		err := checkForInvalidAggregations(exp)
		if err != nil {
			return nil, err
		}
		col := SelectExpr{
			Col: exp,
		}
		if sqlparser.ContainsAggregation(exp.Expr) {
			col.Aggr = true
			qp.HasAggr = true
		}

		qp.SelectExprs = append(qp.SelectExprs, col)
	}

	for _, group := range sel.GroupBy {
		expr, weightStrExpr, err := qp.getSimplifiedExpr(group, semTable)
		if err != nil {
			return nil, err
		}
		if sqlparser.ContainsAggregation(weightStrExpr) {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongGroupField, "Can't group on '%s'", sqlparser.String(expr))
		}
		qp.GroupByExprs = append(qp.GroupByExprs, GroupBy{Inner: expr, WeightStrExpr: weightStrExpr})
	}

	err := qp.addOrderBy(sel.OrderBy, semTable)
	if err != nil {
		return nil, err
	}

	if qp.HasAggr || len(qp.GroupByExprs) > 0 {
		expr := qp.getNonAggrExprNotMatchingGroupByExprs()
		// if we have aggregation functions, non aggregating columns and GROUP BY,
		// the non-aggregating expressions must all be listed in the GROUP BY list
		if expr != nil {
			if len(qp.GroupByExprs) > 0 {
				return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongFieldWithGroup, "Expression of SELECT list is not in GROUP BY clause and contains nonaggregated column '%s' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by", sqlparser.String(expr))
			}
			return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.MixOfGroupFuncAndFields, "In aggregated query without GROUP BY, expression of SELECT list contains nonaggregated column '%s'; this is incompatible with sql_mode=only_full_group_by", sqlparser.String(expr))
		}
	}

	if qp.Distinct && !qp.HasAggr {
		qp.GroupByExprs = nil
	}

	return qp, nil
}

// CreateQPFromUnion creates the QueryProjection for the input *sqlparser.Union
func CreateQPFromUnion(union *sqlparser.Union, semTable *semantics.SemTable) (*QueryProjection, error) {
	qp := &QueryProjection{}

	sel := sqlparser.GetFirstSelect(union)
	for _, selExp := range sel.SelectExprs {
		exp, ok := selExp.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, semantics.Gen4NotSupportedF("%T in select list", selExp)
		}
		col := SelectExpr{
			Col: exp,
		}
		qp.SelectExprs = append(qp.SelectExprs, col)
	}

	err := qp.addOrderBy(union.OrderBy, semTable)
	if err != nil {
		return nil, err
	}

	return qp, nil
}

func (qp *QueryProjection) addOrderBy(orderBy sqlparser.OrderBy, semTable *semantics.SemTable) error {
	canPushDownSorting := true
	for _, order := range orderBy {
		expr, weightStrExpr, err := qp.getSimplifiedExpr(order.Expr, semTable)
		if err != nil {
			return err
		}
		qp.OrderExprs = append(qp.OrderExprs, OrderBy{
			Inner: &sqlparser.Order{
				Expr:      expr,
				Direction: order.Direction,
			},
			WeightStrExpr: weightStrExpr,
		})
		canPushDownSorting = canPushDownSorting && !sqlparser.ContainsAggregation(weightStrExpr)
	}
	qp.CanPushDownSorting = canPushDownSorting
	return nil
}

func checkForInvalidAggregations(exp *sqlparser.AliasedExpr) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		fExpr, ok := node.(*sqlparser.FuncExpr)
		if ok && fExpr.IsAggregate() {
			if len(fExpr.Exprs) != 1 {
				return false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.SyntaxError, "aggregate functions take a single argument '%s'", sqlparser.String(fExpr))
			}
		}
		return true, nil
	}, exp.Expr)
}

func (qp *QueryProjection) getNonAggrExprNotMatchingGroupByExprs() sqlparser.Expr {
	for _, expr := range qp.SelectExprs {
		if expr.Aggr {
			continue
		}
		isGroupByOk := false
		for _, groupByExpr := range qp.GroupByExprs {
			if sqlparser.EqualsExpr(groupByExpr.WeightStrExpr, expr.Col.Expr) {
				isGroupByOk = true
				break
			}
		}
		if !isGroupByOk {
			return expr.Col.Expr
		}
	}
	for _, order := range qp.OrderExprs {
		// ORDER BY NULL or Aggregation functions need not be present in group by
		if sqlparser.IsNull(order.Inner.Expr) || sqlparser.IsAggregation(order.WeightStrExpr) {
			continue
		}
		isGroupByOk := false
		for _, groupByExpr := range qp.GroupByExprs {
			if sqlparser.EqualsExpr(groupByExpr.WeightStrExpr, order.WeightStrExpr) {
				isGroupByOk = true
				break
			}
		}
		if !isGroupByOk {
			return order.Inner.Expr
		}
	}
	return nil
}

// getSimplifiedExpr takes an expression used in ORDER BY or GROUP BY, which can reference both aliased columns and
// column offsets, and returns an expression that is simpler to evaluate
func (qp *QueryProjection) getSimplifiedExpr(e sqlparser.Expr, semTable *semantics.SemTable) (expr sqlparser.Expr, weightStrExpr sqlparser.Expr, err error) {
	// Order by is the column offset to be used from the select expressions
	// Eg - select id from music order by 1
	literalExpr, isLiteral := e.(*sqlparser.Literal)
	if isLiteral && literalExpr.Type == sqlparser.IntVal {
		num, _ := strconv.Atoi(literalExpr.Val)
		if num > len(qp.SelectExprs) {
			return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column offset does not exist")
		}
		aliasedExpr := qp.SelectExprs[num-1].Col
		expr = aliasedExpr.Expr
		if !aliasedExpr.As.IsEmpty() {
			// the column is aliased, so we'll add an expression ordering by the alias and not the underlying expression
			expr = &sqlparser.ColName{
				Name: aliasedExpr.As,
			}
			semTable.CopyDependencies(e, expr)
		}

		return expr, aliasedExpr.Expr, nil
	}

	// If the ORDER BY is against a column alias, we need to remember the expression
	// behind the alias. The weightstring(.) calls needs to be done against that expression and not the alias.
	// Eg - select music.foo as bar, weightstring(music.foo) from music order by bar
	colExpr, isColName := e.(*sqlparser.ColName)
	if isColName && colExpr.Qualifier.IsEmpty() {
		for _, selectExpr := range qp.SelectExprs {
			isAliasExpr := !selectExpr.Col.As.IsEmpty()
			if isAliasExpr && colExpr.Name.Equal(selectExpr.Col.As) {
				return e, selectExpr.Col.Expr, nil
			}
		}
	}

	if sqlparser.IsNull(e) {
		return e, nil, nil
	}

	return e, e, nil
}

// toString should only be used for tests
func (qp *QueryProjection) toString() string {
	type output struct {
		Select   []string
		Grouping []string
		OrderBy  []string
		Distinct bool
	}
	out := output{
		Select:   []string{},
		Grouping: []string{},
		OrderBy:  []string{},
		Distinct: qp.NeedsDistinct(),
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

// NeedsAggregation returns true if we either have aggregate functions or grouping defined
func (qp *QueryProjection) NeedsAggregation() bool {
	return qp.HasAggr || len(qp.GroupByExprs) > 0
}

func (qp QueryProjection) onlyAggr() bool {
	if !qp.HasAggr {
		return false
	}
	for _, expr := range qp.SelectExprs {
		if !expr.Aggr {
			return false
		}
	}
	return true
}

// NeedsDistinct returns true if the query needs explicit distinct
func (qp *QueryProjection) NeedsDistinct() bool {
	if !qp.Distinct {
		return false
	}
	if qp.onlyAggr() && len(qp.GroupByExprs) == 0 {
		return false
	}
	return true
}
