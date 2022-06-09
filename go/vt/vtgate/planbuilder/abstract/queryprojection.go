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
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/engine"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// SelectExpr provides whether the columns is aggregation expression or not.
	SelectExpr struct {
		Col  sqlparser.SelectExpr
		Aggr bool
	}

	// QueryProjection contains the information about the projections, group by and order by expressions used to do horizon planning.
	QueryProjection struct {
		// If you change the contents here, please update the toString() method
		SelectExprs        []SelectExpr
		HasAggr            bool
		Distinct           bool
		groupByExprs       []GroupBy
		OrderExprs         []OrderBy
		CanPushDownSorting bool
		HasStar            bool

		// AddedColumn keeps a counter for expressions added to solve HAVING expressions the user is not selecting
		AddedColumn int
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

		// The index at which the user expects to see this column. Set to nil, if the user does not ask for it
		InnerIndex *int

		// The original aliased expression that this group by is referring
		aliasedExpr *sqlparser.AliasedExpr
	}

	// Aggr encodes all information needed for aggregation functions
	Aggr struct {
		Original *sqlparser.AliasedExpr
		Func     sqlparser.AggrFunc
		OpCode   engine.AggregateOpcode
		Alias    string
		// The index at which the user expects to see this aggregated function. Set to nil, if the user does not ask for it
		Index    *int
		Distinct bool
	}

	AggrRewriter struct {
		qp  *QueryProjection
		Err error
	}
)

func (b GroupBy) AsOrderBy() OrderBy {
	return OrderBy{
		Inner: &sqlparser.Order{
			Expr:      b.Inner,
			Direction: sqlparser.AscOrder,
		},
		WeightStrExpr: b.WeightStrExpr,
	}
}

func (b GroupBy) AsAliasedExpr() *sqlparser.AliasedExpr {
	if b.aliasedExpr != nil {
		return b.aliasedExpr
	}
	col, isColName := b.Inner.(*sqlparser.ColName)
	if isColName && b.WeightStrExpr != b.Inner {
		return &sqlparser.AliasedExpr{
			Expr: b.WeightStrExpr,
			As:   col.Name,
		}
	}
	if !isColName && b.WeightStrExpr != b.Inner {
		panic("this should not happen - different inner and weighStringExpr and not a column alias")
	}

	return &sqlparser.AliasedExpr{
		Expr: b.WeightStrExpr,
	}
}

// GetExpr returns the underlying sqlparser.Expr of our SelectExpr
func (s SelectExpr) GetExpr() (sqlparser.Expr, error) {
	switch sel := s.Col.(type) {
	case *sqlparser.AliasedExpr:
		return sel.Expr, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] %T does not have expr", s.Col)
	}
}

// GetAliasedExpr returns the SelectExpr as a *sqlparser.AliasedExpr if its type allows it,
// otherwise an error is returned.
func (s SelectExpr) GetAliasedExpr() (*sqlparser.AliasedExpr, error) {
	switch expr := s.Col.(type) {
	case *sqlparser.AliasedExpr:
		return expr, nil
	case *sqlparser.StarExpr:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: '*' expression in cross-shard query")
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not an aliased expression: %T", expr)
	}
}

// CreateQPFromSelect creates the QueryProjection for the input *sqlparser.Select
func CreateQPFromSelect(sel *sqlparser.Select) (*QueryProjection, error) {
	qp := &QueryProjection{
		Distinct: sel.Distinct,
	}

	err := qp.addSelectExpressions(sel)
	if err != nil {
		return nil, err
	}
	for _, group := range sel.GroupBy {
		selectExprIdx, aliasExpr := qp.FindSelectExprIndexForExpr(group)
		expr, weightStrExpr, err := qp.GetSimplifiedExpr(group)
		if err != nil {
			return nil, err
		}
		err = checkForInvalidGroupingExpressions(weightStrExpr)
		if err != nil {
			return nil, err
		}

		groupBy := GroupBy{
			Inner:         expr,
			WeightStrExpr: weightStrExpr,
			InnerIndex:    selectExprIdx,
			aliasedExpr:   aliasExpr,
		}

		qp.groupByExprs = append(qp.groupByExprs, groupBy)
	}

	err = qp.addOrderBy(sel.OrderBy)
	if err != nil {
		return nil, err
	}

	if qp.Distinct && !qp.HasAggr {
		qp.groupByExprs = nil
	}

	return qp, nil
}

// Rewrite will go through an expression, add aggregations to the QP, and rewrite them to use column offset
func (ar *AggrRewriter) Rewrite() func(*sqlparser.Cursor) bool {
	return func(cursor *sqlparser.Cursor) bool {
		if ar.Err != nil {
			return false
		}
		sqlNode := cursor.Node()
		if fExp, ok := sqlNode.(sqlparser.AggrFunc); ok {
			for offset, expr := range ar.qp.SelectExprs {
				ae, err := expr.GetAliasedExpr()
				if err != nil {
					ar.Err = err
					return false
				}
				if sqlparser.EqualsExpr(ae.Expr, fExp) {
					cursor.Replace(sqlparser.NewOffset(offset, fExp))
					return false // no need to visit aggregation children
				}
			}

			col := SelectExpr{
				Aggr: true,
				Col:  &sqlparser.AliasedExpr{Expr: fExp},
			}
			ar.qp.HasAggr = true

			cursor.Replace(sqlparser.NewOffset(len(ar.qp.SelectExprs), fExp))
			ar.qp.SelectExprs = append(ar.qp.SelectExprs, col)
			ar.qp.AddedColumn++
		}

		return true
	}
}

// AggrRewriter extracts
func (qp *QueryProjection) AggrRewriter() *AggrRewriter {
	return &AggrRewriter{qp: qp}
}

func (qp *QueryProjection) addSelectExpressions(sel *sqlparser.Select) error {
	for _, selExp := range sel.SelectExprs {
		switch selExp := selExp.(type) {
		case *sqlparser.AliasedExpr:
			err := checkForInvalidAggregations(selExp)
			if err != nil {
				return err
			}
			col := SelectExpr{
				Col: selExp,
			}
			if sqlparser.ContainsAggregation(selExp.Expr) {
				col.Aggr = true
				qp.HasAggr = true
			}

			qp.SelectExprs = append(qp.SelectExprs, col)
		case *sqlparser.StarExpr:
			qp.HasStar = true
			col := SelectExpr{
				Col: selExp,
			}
			qp.SelectExprs = append(qp.SelectExprs, col)
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] %T in select list", selExp)
		}
	}
	return nil
}

// CreateQPFromUnion creates the QueryProjection for the input *sqlparser.Union
func CreateQPFromUnion(union *sqlparser.Union) (*QueryProjection, error) {
	qp := &QueryProjection{}

	sel := sqlparser.GetFirstSelect(union)
	err := qp.addSelectExpressions(sel)
	if err != nil {
		return nil, err
	}

	err = qp.addOrderBy(union.OrderBy)
	if err != nil {
		return nil, err
	}

	return qp, nil
}

func (qp *QueryProjection) addOrderBy(orderBy sqlparser.OrderBy) error {
	canPushDownSorting := true
	for _, order := range orderBy {
		expr, weightStrExpr, err := qp.GetSimplifiedExpr(order.Expr)
		if err != nil {
			return err
		}
		if sqlparser.IsNull(weightStrExpr) {
			// ORDER BY null can safely be ignored
			continue
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

// GetGrouping returns a copy of the grouping parameters of the QP
func (qp *QueryProjection) GetGrouping() []GroupBy {
	out := make([]GroupBy, len(qp.groupByExprs))
	copy(out, qp.groupByExprs)
	return out
}

func checkForInvalidAggregations(exp *sqlparser.AliasedExpr) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if aggrFunc, isAggregate := node.(sqlparser.AggrFunc); isAggregate {
			if aggrFunc.GetArgs() != nil &&
				len(aggrFunc.GetArgs()) != 1 {
				return false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.SyntaxError, "aggregate functions take a single argument '%s'", sqlparser.String(node))
			}
			return true, nil
		}

		return true, nil
	}, exp.Expr)
}

func (qp *QueryProjection) getNonAggrExprNotMatchingGroupByExprs() sqlparser.SelectExpr {
	for _, expr := range qp.SelectExprs {
		if expr.Aggr {
			continue
		}
		if !qp.isExprInGroupByExprs(expr) {
			return expr.Col
		}
	}
	for _, order := range qp.OrderExprs {
		if !qp.isOrderByExprInGroupBy(order) {
			return &sqlparser.AliasedExpr{
				Expr: order.Inner.Expr,
			}
		}
	}
	return nil
}

func (qp *QueryProjection) isOrderByExprInGroupBy(order OrderBy) bool {
	// ORDER BY NULL or Aggregation functions need not be present in group by
	_, isAggregate := order.WeightStrExpr.(sqlparser.AggrFunc)
	if sqlparser.IsNull(order.Inner.Expr) || isAggregate {
		return true
	}
	for _, groupByExpr := range qp.groupByExprs {
		if sqlparser.EqualsExpr(groupByExpr.WeightStrExpr, order.WeightStrExpr) {
			return true
		}
	}
	return false
}

func (qp *QueryProjection) isExprInGroupByExprs(expr SelectExpr) bool {
	for _, groupByExpr := range qp.groupByExprs {
		exp, err := expr.GetExpr()
		if err != nil {
			return false
		}
		if sqlparser.EqualsExpr(groupByExpr.WeightStrExpr, exp) {
			return true
		}
	}
	return false
}

// GetSimplifiedExpr takes an expression used in ORDER BY or GROUP BY, and returns an expression that is simpler to evaluate
func (qp *QueryProjection) GetSimplifiedExpr(e sqlparser.Expr) (expr sqlparser.Expr, weightStrExpr sqlparser.Expr, err error) {
	// If the ORDER BY is against a column alias, we need to remember the expression
	// behind the alias. The weightstring(.) calls needs to be done against that expression and not the alias.
	// Eg - select music.foo as bar, weightstring(music.foo) from music order by bar

	colExpr, isColName := e.(*sqlparser.ColName)
	if !isColName {
		return e, e, nil
	}

	if sqlparser.IsNull(e) {
		return e, nil, nil
	}

	if colExpr.Qualifier.IsEmpty() {
		for _, selectExpr := range qp.SelectExprs {
			aliasedExpr, isAliasedExpr := selectExpr.Col.(*sqlparser.AliasedExpr)
			if !isAliasedExpr {
				continue
			}
			isAliasExpr := !aliasedExpr.As.IsEmpty()
			if isAliasExpr && colExpr.Name.Equal(aliasedExpr.As) {
				return e, aliasedExpr.Expr, nil
			}
		}
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
		e := sqlparser.String(expr.Col)

		if expr.Aggr {
			e = "aggr: " + e
		}
		out.Select = append(out.Select, e)
	}

	for _, expr := range qp.groupByExprs {
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
	return qp.HasAggr || len(qp.groupByExprs) > 0
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
	if qp.onlyAggr() && len(qp.groupByExprs) == 0 {
		return false
	}
	return true
}

func (qp *QueryProjection) AggregationExpressions() (out []Aggr, err error) {
orderBy:
	for _, orderExpr := range qp.OrderExprs {
		if qp.isOrderByExprInGroupBy(orderExpr) {
			continue orderBy
		}
		orderExpr := orderExpr.Inner.Expr
		for _, expr := range qp.SelectExprs {
			col, ok := expr.Col.(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			if sqlparser.EqualsExpr(col.Expr, orderExpr) {
				continue orderBy // we found the expression we were looking for!
			}
		}
		qp.SelectExprs = append(qp.SelectExprs, SelectExpr{
			Col:  &sqlparser.AliasedExpr{Expr: orderExpr},
			Aggr: sqlparser.ContainsAggregation(orderExpr),
		})
		qp.AddedColumn++
	}

	for idx, expr := range qp.SelectExprs {
		aliasedExpr, err := expr.GetAliasedExpr()
		if err != nil {
			return nil, err
		}

		idxCopy := idx

		if !sqlparser.ContainsAggregation(expr.Col) {
			if !qp.isExprInGroupByExprs(expr) {
				out = append(out, Aggr{
					Original: aliasedExpr,
					OpCode:   engine.AggregateRandom,
					Alias:    aliasedExpr.ColumnName(),
					Index:    &idxCopy,
				})
			}
			continue
		}
		fnc, isAggregate := aliasedExpr.Expr.(sqlparser.AggrFunc)
		if !isAggregate {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: complex aggregate expression")
		}

		opcode, found := engine.SupportedAggregates[strings.ToLower(fnc.AggrName())]
		if !found {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: aggregation function '%s'", fnc.AggrName())
		}

		if opcode == engine.AggregateCount {
			if _, isStar := fnc.(*sqlparser.CountStar); isStar {
				opcode = engine.AggregateCountStar
			}
		}

		aggr, _ := aliasedExpr.Expr.(sqlparser.AggrFunc)

		if aggr.IsDistinct() {
			switch opcode {
			case engine.AggregateCount:
				opcode = engine.AggregateCountDistinct
			case engine.AggregateSum:
				opcode = engine.AggregateSumDistinct
			}
		}

		out = append(out, Aggr{
			Original: aliasedExpr,
			Func:     aggr,
			OpCode:   opcode,
			Alias:    aliasedExpr.ColumnName(),
			Index:    &idxCopy,
			Distinct: aggr.IsDistinct(),
		})
	}
	return
}

// FindSelectExprIndexForExpr returns the index of the given expression in the select expressions, if it is part of it
// returns -1 otherwise.
func (qp *QueryProjection) FindSelectExprIndexForExpr(expr sqlparser.Expr) (*int, *sqlparser.AliasedExpr) {
	colExpr, isCol := expr.(*sqlparser.ColName)

	for idx, selectExpr := range qp.SelectExprs {
		aliasedExpr, isAliasedExpr := selectExpr.Col.(*sqlparser.AliasedExpr)
		if !isAliasedExpr {
			continue
		}
		if isCol {
			isAliasExpr := !aliasedExpr.As.IsEmpty()
			if isAliasExpr && colExpr.Name.Equal(aliasedExpr.As) {
				return &idx, aliasedExpr
			}
		}
		if sqlparser.EqualsExpr(aliasedExpr.Expr, expr) {
			return &idx, aliasedExpr
		}
	}
	return nil, nil
}

// AlignGroupByAndOrderBy aligns the group by and order by columns, so they are in the same order
// The GROUP BY clause is a set - the order between the elements does not make any difference,
// so we can simply re-arrange the column order
// We are also free to add more ORDER BY columns than the user asked for which we leverage,
// so the input is already ordered according to the GROUP BY columns used
func (qp *QueryProjection) AlignGroupByAndOrderBy() {
	// The ORDER BY can be performed before the OA

	var newGrouping []GroupBy
	if len(qp.OrderExprs) == 0 {
		// The query didn't ask for any particular order, so we are free to add arbitrary ordering.
		// We'll align the grouping and ordering by the output columns
		newGrouping = qp.GetGrouping()
		SortGrouping(newGrouping)
		for _, groupBy := range newGrouping {
			qp.OrderExprs = append(qp.OrderExprs, groupBy.AsOrderBy())
		}
	} else {
		// Here we align the GROUP BY and ORDER BY.
		// First step is to make sure that the GROUP BY is in the same order as the ORDER BY
		used := make([]bool, len(qp.groupByExprs))
		for _, orderExpr := range qp.OrderExprs {
			for i, groupingExpr := range qp.groupByExprs {
				if !used[i] && sqlparser.EqualsExpr(groupingExpr.WeightStrExpr, orderExpr.WeightStrExpr) {
					newGrouping = append(newGrouping, groupingExpr)
					used[i] = true
				}
			}
		}
		if len(newGrouping) != len(qp.groupByExprs) {
			// we are missing some groupings. We need to add them both to the new groupings list, but also to the ORDER BY
			for i, added := range used {
				if !added {
					groupBy := qp.groupByExprs[i]
					newGrouping = append(newGrouping, groupBy)
					qp.OrderExprs = append(qp.OrderExprs, groupBy.AsOrderBy())
				}
			}
		}
	}

	qp.groupByExprs = newGrouping
}

// AddGroupBy does just that
func (qp *QueryProjection) AddGroupBy(by GroupBy) {
	qp.groupByExprs = append(qp.groupByExprs, by)
}

func (qp *QueryProjection) GetColumnCount() int {
	return len(qp.SelectExprs) - qp.AddedColumn
}

func checkForInvalidGroupingExpressions(expr sqlparser.Expr) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, isAggregate := node.(sqlparser.AggrFunc); isAggregate {
			return false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongGroupField, "Can't group on '%s'", sqlparser.String(expr))
		}
		_, isSubQ := node.(*sqlparser.Subquery)
		arg, isArg := node.(sqlparser.Argument)
		if isSubQ || (isArg && strings.HasPrefix(string(arg), "__sq")) {
			return false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: subqueries disallowed in GROUP BY")
		}
		return true, nil
	}, expr)
}

func SortAggregations(a []Aggr) {
	sort.Slice(a, func(i, j int) bool {
		return CompareRefInt(a[i].Index, a[j].Index)
	})
}

func SortGrouping(a []GroupBy) {
	sort.Slice(a, func(i, j int) bool {
		return CompareRefInt(a[i].InnerIndex, a[j].InnerIndex)
	})
}

// CompareRefInt compares two references of integers.
// In case either one is nil, it is considered to be smaller
func CompareRefInt(a *int, b *int) bool {
	if a == nil {
		return false
	}
	if b == nil {
		return true
	}
	return *a < *b
}
