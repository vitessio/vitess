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

package operators

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// SelectExpr provides whether the column is aggregation expression or not.
	SelectExpr struct {
		Col  sqlparser.SelectExpr
		Aggr bool
	}

	// QueryProjection contains the information about the projections, group by and order by expressions used to do horizon planning.
	QueryProjection struct {
		// If you change the contents here, please update the toString() method
		SelectExprs  []SelectExpr
		HasAggr      bool
		Distinct     bool
		WithRollup   bool
		groupByExprs []GroupBy
		OrderExprs   []OrderBy

		// AddedColumn keeps a counter for expressions added to solve HAVING expressions the user is not selecting
		AddedColumn int

		hasCheckedAlignment bool
	}

	// GroupBy contains the expression to used in group by and also if grouping is needed at VTGate level then what the weight_string function expression to be sent down for evaluation.
	GroupBy struct {
		Inner sqlparser.Expr

		// The index at which the user expects to see this column. Set to nil, if the user does not ask for it
		InnerIndex *int

		// points to the column on the same aggregator
		ColOffset int
		WSOffset  int
	}

	// Aggr encodes all information needed for aggregation functions
	Aggr struct {
		Original *sqlparser.AliasedExpr // The original SQL expression for the aggregation
		Func     sqlparser.AggrFunc     // The aggregation function (e.g., COUNT, SUM). If nil, it means AggregateAnyValue or AggregateUDF is used
		OpCode   opcode.AggregateOpcode // The opcode representing the type of aggregation being performed

		// OriginalOpCode will contain opcode.AggregateUnassigned unless we are changing the opcode while pushing them down
		OriginalOpCode opcode.AggregateOpcode

		Alias string // The alias name for the aggregation result

		Distinct bool // Whether the aggregation function is DISTINCT

		// Offsets pointing to columns within the same aggregator
		ColOffset int // Offset for the column being aggregated
		WSOffset  int // Offset for the weight string of the column

		SubQueryExpression []*SubQuery // Subqueries associated with this aggregation

		PushedDown bool // Whether the aggregation has been pushed down to the next layer
	}
)

func (aggr Aggr) NeedsWeightString(ctx *plancontext.PlanningContext) bool {
	return aggr.OpCode.NeedsComparableValues() && ctx.NeedsWeightString(aggr.Func.GetArg())
}

func (aggr Aggr) GetTypeCollation(ctx *plancontext.PlanningContext) evalengine.Type {
	if aggr.Func == nil {
		return evalengine.NewUnknownType()
	}
	switch aggr.OpCode {
	case opcode.AggregateMin, opcode.AggregateMax, opcode.AggregateSumDistinct, opcode.AggregateCountDistinct:
		typ, _ := ctx.TypeForExpr(aggr.Func.GetArg())
		return typ

	}
	return evalengine.Type{}
}

// NewGroupBy creates a new group by from the given fields.
func NewGroupBy(inner sqlparser.Expr) GroupBy {
	return GroupBy{
		Inner:     inner,
		ColOffset: -1,
		WSOffset:  -1,
	}
}

func NewAggr(opCode opcode.AggregateOpcode, f sqlparser.AggrFunc, original *sqlparser.AliasedExpr, alias string) Aggr {
	return Aggr{
		Original:  original,
		Func:      f,
		OpCode:    opCode,
		Alias:     alias,
		ColOffset: -1,
		WSOffset:  -1,
	}
}

func (b GroupBy) AsOrderBy() OrderBy {
	return OrderBy{
		Inner: &sqlparser.Order{
			Expr:      b.Inner,
			Direction: sqlparser.AscOrder,
		},
		SimplifiedExpr: b.Inner,
	}
}

// GetExpr returns the underlying sqlparser.Expr of our SelectExpr
func (s SelectExpr) GetExpr() (sqlparser.Expr, error) {
	switch sel := s.Col.(type) {
	case *sqlparser.AliasedExpr:
		return sel.Expr, nil
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("%T does not have an expression", s.Col))
	}
}

// GetAliasedExpr returns the SelectExpr as a *sqlparser.AliasedExpr if its type allows it,
// otherwise an error is returned.
func (s SelectExpr) GetAliasedExpr() (*sqlparser.AliasedExpr, error) {
	switch expr := s.Col.(type) {
	case *sqlparser.AliasedExpr:
		return expr, nil
	case *sqlparser.StarExpr:
		return nil, vterrors.VT09015()
	default:
		return nil, vterrors.VT12001(fmt.Sprintf("not an aliased expression: %T", expr))
	}
}

// createQPFromSelect creates the QueryProjection for the input *sqlparser.Select
func createQPFromSelect(ctx *plancontext.PlanningContext, sel *sqlparser.Select) *QueryProjection {
	qp := &QueryProjection{
		Distinct: sel.Distinct,
	}

	qp.addSelectExpressions(ctx, sel)
	qp.addGroupBy(ctx, sel.GroupBy)
	qp.addOrderBy(ctx, sel.OrderBy)
	if !qp.HasAggr && sel.Having != nil {
		qp.HasAggr = ctx.ContainsAggr(sel.Having.Expr)
	}
	qp.calculateDistinct(ctx)

	return qp
}

func (qp *QueryProjection) addSelectExpressions(ctx *plancontext.PlanningContext, sel *sqlparser.Select) {
	for _, selExp := range sel.GetColumns() {
		switch selExp := selExp.(type) {
		case *sqlparser.AliasedExpr:
			col := SelectExpr{
				Col: selExp,
			}
			if ctx.ContainsAggr(selExp.Expr) {
				col.Aggr = true
				qp.HasAggr = true
			}

			qp.SelectExprs = append(qp.SelectExprs, col)
		case *sqlparser.StarExpr:
			col := SelectExpr{
				Col: selExp,
			}
			qp.SelectExprs = append(qp.SelectExprs, col)
		default:
			panic(vterrors.VT13001(fmt.Sprintf("%T in select list", selExp)))
		}
	}
}

// createQPFromUnion creates the QueryProjection for the input *sqlparser.Union
func createQPFromUnion(ctx *plancontext.PlanningContext, union *sqlparser.Union) *QueryProjection {
	qp := &QueryProjection{}

	sel := getFirstSelect(union)
	qp.addSelectExpressions(ctx, sel)
	qp.addOrderBy(ctx, union.OrderBy)

	return qp
}

type expressionSet struct {
	exprs []sqlparser.Expr
}

func (es *expressionSet) add(ctx *plancontext.PlanningContext, e sqlparser.Expr) bool {
	idx := slices.IndexFunc(es.exprs, func(expr sqlparser.Expr) bool {
		return ctx.SemTable.EqualsExprWithDeps(e, expr)
	})

	// if we already have this expression, there is no need to repeat it
	if idx >= 0 {
		return false
	}
	es.exprs = append(es.exprs, e)
	return true
}

func (qp *QueryProjection) addOrderBy(ctx *plancontext.PlanningContext, orderBy sqlparser.OrderBy) {
	canPushSorting := true
	es := &expressionSet{}
	for _, order := range orderBy {
		if canIgnoreOrdering(ctx, order.Expr) {
			continue
		}
		if !es.add(ctx, order.Expr) {
			continue
		}
		qp.OrderExprs = append(qp.OrderExprs, OrderBy{
			Inner:          ctx.SemTable.Clone(order).(*sqlparser.Order),
			SimplifiedExpr: order.Expr,
		})
		canPushSorting = canPushSorting && !ctx.ContainsAggr(order.Expr)
	}
}

// canIgnoreOrdering returns true if the ordering expression has no effect on the result.
func canIgnoreOrdering(ctx *plancontext.PlanningContext, expr sqlparser.Expr) bool {
	switch expr.(type) {
	case *sqlparser.NullVal, *sqlparser.Literal, *sqlparser.Argument:
		return true
	case *sqlparser.Subquery:
		return ctx.SemTable.RecursiveDeps(expr).IsEmpty()
	default:
		return false
	}
}

func (qp *QueryProjection) calculateDistinct(ctx *plancontext.PlanningContext) {
	if qp.Distinct && !qp.HasAggr {
		distinct := qp.useGroupingOverDistinct(ctx)
		if distinct {
			// if order by exists with overlap with select expressions, we can use the aggregation with ordering over distinct.
			qp.Distinct = false
		} else {
			// grouping and distinct both lead to unique results, so we don't need
			qp.groupByExprs = nil
		}
	}

	if qp.HasAggr && len(qp.groupByExprs) == 0 {
		// this is a scalar aggregation and is inherently distinct
		qp.Distinct = false
	}

	if !qp.Distinct || len(qp.groupByExprs) == 0 {
		return
	}

	for _, gb := range qp.groupByExprs {
		_, found := canReuseColumn(ctx, qp.SelectExprs, gb.Inner, func(expr SelectExpr) sqlparser.Expr {
			getExpr, err := expr.GetExpr()
			if err != nil {
				panic(err)
			}
			return getExpr
		})
		if !found {
			return
		}
	}

	// since we are returning all grouping expressions, we know the results are guaranteed to be unique
	qp.Distinct = false
}

func (qp *QueryProjection) addGroupBy(ctx *plancontext.PlanningContext, groupBy *sqlparser.GroupBy) {
	if groupBy == nil {
		return
	}
	qp.WithRollup = groupBy.WithRollup
	es := &expressionSet{}
	for _, grouping := range groupBy.Exprs {
		selectExprIdx := qp.FindSelectExprIndexForExpr(ctx, grouping)
		checkForInvalidGroupingExpressions(ctx, grouping)

		if !es.add(ctx, grouping) {
			continue
		}

		groupBy := NewGroupBy(grouping)
		groupBy.InnerIndex = selectExprIdx

		qp.groupByExprs = append(qp.groupByExprs, groupBy)
	}
}

// GetGrouping returns a copy of the grouping parameters of the QP
func (qp *QueryProjection) GetGrouping() []GroupBy {
	return slices.Clone(qp.groupByExprs)
}

func (qp *QueryProjection) isExprInGroupByExprs(ctx *plancontext.PlanningContext, expr sqlparser.Expr) bool {
	for _, groupByExpr := range qp.groupByExprs {
		if ctx.SemTable.EqualsExprWithDeps(groupByExpr.Inner, expr) {
			return true
		}
	}
	return false
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

func (qp *QueryProjection) onlyAggr() bool {
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

func (qp *QueryProjection) AggregationExpressions(ctx *plancontext.PlanningContext, allowComplexExpression bool) (out []Aggr, complex bool) {
	qp.addOrderByToSelect(ctx)
	addAggr := func(a Aggr) {
		out = append(out, a)
	}
	makeComplex := func() {
		complex = true
	}
	// Here we go over the expressions we are returning. Since we know we are aggregating,
	// all expressions have to be either grouping expressions or aggregate expressions.
	// If we find an expression that is neither, we treat is as a special aggregation function AggrRandom
	for _, selectExpr := range qp.SelectExprs {
		aliasedExpr, err := selectExpr.GetAliasedExpr()
		if err != nil {
			panic(err)
		}

		if !ctx.ContainsAggr(selectExpr.Col) {
			getExpr, err := selectExpr.GetExpr()
			if err != nil {
				panic(err)
			}
			if !qp.isExprInGroupByExprs(ctx, getExpr) {
				aggr := createNonGroupingAggr(aliasedExpr)
				out = append(out, aggr)
			}
			continue
		}
		if !ctx.IsAggr(aliasedExpr.Expr) && !allowComplexExpression {
			panic(vterrors.VT12001("in scatter query: complex aggregate expression"))
		}

		sqlparser.CopyOnRewrite(aliasedExpr.Expr, qp.extractAggr(ctx, aliasedExpr, addAggr, makeComplex), nil, nil)
	}
	return
}

func (qp *QueryProjection) extractAggr(
	ctx *plancontext.PlanningContext,
	aliasedExpr *sqlparser.AliasedExpr,
	addAggr func(a Aggr),
	makeComplex func(),
) func(node sqlparser.SQLNode, parent sqlparser.SQLNode) bool {
	return func(node, parent sqlparser.SQLNode) bool {
		ex, isExpr := node.(sqlparser.Expr)
		if !isExpr {
			return true
		}
		if aggr, isAggr := node.(sqlparser.AggrFunc); isAggr {
			ae := aeWrap(aggr)
			if aggr == aliasedExpr.Expr {
				ae = aliasedExpr
			}
			aggrFunc := createAggrFromAggrFunc(aggr, ae)
			addAggr(aggrFunc)
			return false
		}
		if ctx.IsAggr(node) {
			// If we are here, we have a function that is an aggregation but not parsed into an AggrFunc.
			// This is the case for UDFs - we have to be careful with these because we can't evaluate them in VTGate.
			aggr := NewAggr(opcode.AggregateUDF, nil, aeWrap(ex), "")
			addAggr(aggr)
			return false
		}
		if ctx.ContainsAggr(node) {
			makeComplex()
			return true
		}
		if !qp.isExprInGroupByExprs(ctx, ex) {
			aggr := createNonGroupingAggr(aeWrap(ex))
			addAggr(aggr)
		}
		return false
	}
}

func (qp *QueryProjection) addOrderByToSelect(ctx *plancontext.PlanningContext) {
orderBy:
	// We need to return all columns that are being used for ordering
	for _, orderExpr := range qp.OrderExprs {
		orderExpr := orderExpr.SimplifiedExpr
		for _, expr := range qp.SelectExprs {
			col, ok := expr.Col.(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			if ctx.SemTable.EqualsExprWithDeps(col.Expr, orderExpr) {
				continue orderBy // we found the expression we were looking for!
			}
		}
		qp.SelectExprs = append(qp.SelectExprs, SelectExpr{
			Col:  &sqlparser.AliasedExpr{Expr: orderExpr},
			Aggr: ctx.ContainsAggr(orderExpr),
		})
		qp.AddedColumn++
	}
}

func createAggrFromAggrFunc(fnc sqlparser.AggrFunc, aliasedExpr *sqlparser.AliasedExpr) Aggr {
	code := opcode.SupportedAggregates[fnc.AggrName()]

	if code == opcode.AggregateCount {
		if _, isStar := fnc.(*sqlparser.CountStar); isStar {
			code = opcode.AggregateCountStar
		}
	}

	distinct := sqlparser.IsDistinct(fnc)
	if distinct {
		switch code {
		case opcode.AggregateCount:
			code = opcode.AggregateCountDistinct
		case opcode.AggregateSum:
			code = opcode.AggregateSumDistinct
		}
	}

	aggr := NewAggr(code, fnc, aliasedExpr, aliasedExpr.ColumnName())
	aggr.Distinct = distinct
	return aggr
}

// FindSelectExprIndexForExpr returns the index of the given expression in the select expressions, if it is part of it
// returns -1 otherwise.
func (qp *QueryProjection) FindSelectExprIndexForExpr(ctx *plancontext.PlanningContext, expr sqlparser.Expr) *int {
	colExpr, isCol := expr.(*sqlparser.ColName)

	for idx, selectExpr := range qp.SelectExprs {
		aliasedExpr, isAliasedExpr := selectExpr.Col.(*sqlparser.AliasedExpr)
		if !isAliasedExpr {
			continue
		}
		if isCol {
			isAliasExpr := aliasedExpr.As.NotEmpty()
			if isAliasExpr && colExpr.Name.Equal(aliasedExpr.As) {
				return &idx
			}
		}
		if ctx.SemTable.EqualsExprWithDeps(aliasedExpr.Expr, expr) {
			return &idx
		}
	}
	return nil
}

// OldAlignGroupByAndOrderBy TODO Remove once all of horizon planning is done on the operators
func (qp *QueryProjection) OldAlignGroupByAndOrderBy(ctx *plancontext.PlanningContext) {
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
				if !used[i] && ctx.SemTable.EqualsExpr(groupingExpr.Inner, orderExpr.SimplifiedExpr) {
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

// AlignGroupByAndOrderBy aligns the group by and order by columns, so they are in the same order
// The GROUP BY clause is a set - the order between the elements does not make any difference,
// so we can simply re-arrange the column order
// We are also free to add more ORDER BY columns than the user asked for which we leverage,
// so the input is already ordered according to the GROUP BY columns used
func (qp *QueryProjection) AlignGroupByAndOrderBy(ctx *plancontext.PlanningContext) bool {
	if qp == nil {
		return false
	}
	if qp.hasCheckedAlignment {
		return false
	}
	qp.hasCheckedAlignment = true
	newGrouping := make([]GroupBy, 0, len(qp.groupByExprs))
	used := make([]bool, len(qp.groupByExprs))

outer:
	for _, orderBy := range qp.OrderExprs {
		for gidx, groupBy := range qp.groupByExprs {
			if ctx.SemTable.EqualsExprWithDeps(groupBy.Inner, orderBy.SimplifiedExpr) {
				newGrouping = append(newGrouping, groupBy)
				used[gidx] = true
				continue outer
			}
		}
		return false
	}

	// if we get here, it means that all the OrderBy expressions are also in the GroupBy clause
	for gidx, gb := range qp.groupByExprs {
		if !used[gidx] {
			newGrouping = append(newGrouping, gb)
			qp.OrderExprs = append(qp.OrderExprs, gb.AsOrderBy())
		}
	}
	qp.groupByExprs = newGrouping
	return true
}

// AddGroupBy does just that
func (qp *QueryProjection) AddGroupBy(by GroupBy) {
	qp.groupByExprs = append(qp.groupByExprs, by)
}

func (qp *QueryProjection) GetColumnCount() int {
	return len(qp.SelectExprs) - qp.AddedColumn
}

func (qp *QueryProjection) orderByOverlapWithSelectExpr(ctx *plancontext.PlanningContext) bool {
	for _, expr := range qp.OrderExprs {
		idx := qp.FindSelectExprIndexForExpr(ctx, expr.SimplifiedExpr)
		if idx != nil {
			return true
		}
	}
	return false
}

func (qp *QueryProjection) useGroupingOverDistinct(ctx *plancontext.PlanningContext) bool {
	if !qp.orderByOverlapWithSelectExpr(ctx) {
		return false
	}
	var gbs []GroupBy
	for idx, selExpr := range qp.SelectExprs {
		ae, err := selExpr.GetAliasedExpr()
		if err != nil {
			// not an alias Expr, cannot continue forward.
			return false
		}
		// check if the grouping already exists on that column.
		found := slices.IndexFunc(qp.groupByExprs, func(gb GroupBy) bool {
			return ctx.SemTable.EqualsExprWithDeps(gb.Inner, ae.Expr)
		})
		if found != -1 {
			continue
		}
		groupBy := NewGroupBy(ae.Expr)
		selectExprIdx := idx
		groupBy.InnerIndex = &selectExprIdx

		gbs = append(gbs, groupBy)
	}
	qp.groupByExprs = append(qp.groupByExprs, gbs...)
	return true
}

// addColumn adds a column to the QueryProjection if it is not already present.
// It will use a column name that is available on the outside of the derived table
func (qp *QueryProjection) addDerivedColumn(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	for _, selectExpr := range qp.SelectExprs {
		getExpr, err := selectExpr.GetExpr()
		if err != nil {
			continue
		}
		if ctx.SemTable.EqualsExprWithDeps(getExpr, expr) {
			return
		}
	}
	qp.SelectExprs = append(qp.SelectExprs, SelectExpr{
		Col:  aeWrap(expr),
		Aggr: ctx.ContainsAggr(expr),
	})
}

func checkForInvalidGroupingExpressions(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if ctx.IsAggr(node) {
			panic(vterrors.VT03005(sqlparser.String(expr)))
		}
		_, isSubQ := node.(*sqlparser.Subquery)
		arg, isArg := node.(*sqlparser.Argument)
		if isSubQ || (isArg && strings.HasPrefix(arg.Name, "__sq")) {
			panic(vterrors.VT12001("subqueries in GROUP BY"))
		}
		return true, nil
	}, expr)
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

func CreateQPFromSelectStatement(ctx *plancontext.PlanningContext, stmt sqlparser.TableStatement) *QueryProjection {
	switch sel := stmt.(type) {
	case *sqlparser.Select:
		return createQPFromSelect(ctx, sel)
	case *sqlparser.Union:
		return createQPFromUnion(ctx, sel)
	}
	panic(vterrors.VT13001("can only create query projection from Union and Select statements"))
}
