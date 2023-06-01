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
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
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
		groupByExprs []GroupBy
		OrderExprs   []ops.OrderBy
		HasStar      bool

		// AddedColumn keeps a counter for expressions added to solve HAVING expressions the user is not selecting
		AddedColumn int

		hasCheckedAlignment bool

		// TODO Remove once all horizon planning is done on the operators
		CanPushDownSorting bool
	}

	// GroupBy contains the expression to used in group by and also if grouping is needed at VTGate level then what the weight_string function expression to be sent down for evaluation.
	GroupBy struct {
		Inner sqlparser.Expr

		// The simplified expressions is the "unaliased expression".
		// In the following query, the group by has the inner expression
		// `x` and the `SimplifiedExpr` is `table.col + 10`:
		// select table.col + 10 as x, count(*) from tbl group by x
		SimplifiedExpr sqlparser.Expr

		// The index at which the user expects to see this column. Set to nil, if the user does not ask for it
		InnerIndex *int

		// The original aliased expression that this group by is referring
		aliasedExpr *sqlparser.AliasedExpr

		// points to the column on the same aggregator
		ColOffset int
		WSOffset  int
	}

	// Aggr encodes all information needed for aggregation functions
	Aggr struct {
		Original *sqlparser.AliasedExpr
		Func     sqlparser.AggrFunc
		OpCode   opcode.AggregateOpcode

		// OriginalOpCode will contain opcode.AggregateUnassigned unless we are changing opcode while pushing them down
		OriginalOpCode opcode.AggregateOpcode

		Alias string
		// The index at which the user expects to see this aggregated function. Set to nil, if the user does not ask for it
		Index    *int
		Distinct bool

		ColOffset int // points to the column on the same aggregator
	}

	AggrRewriter struct {
		qp  *QueryProjection
		st  *semantics.SemTable
		Err error
	}
)

// NewGroupBy creates a new group by from the given fields.
func NewGroupBy(inner, simplified sqlparser.Expr, aliasedExpr *sqlparser.AliasedExpr) GroupBy {
	return GroupBy{
		Inner:          inner,
		SimplifiedExpr: simplified,
		aliasedExpr:    aliasedExpr,
		ColOffset:      -1,
		WSOffset:       -1,
	}
}

func (b GroupBy) AsOrderBy() ops.OrderBy {
	return ops.OrderBy{
		Inner: &sqlparser.Order{
			Expr:      b.Inner,
			Direction: sqlparser.AscOrder,
		},
		SimplifiedExpr: b.SimplifiedExpr,
	}
}

func (b GroupBy) AsAliasedExpr() *sqlparser.AliasedExpr {
	if b.aliasedExpr != nil {
		return b.aliasedExpr
	}
	col, isColName := b.Inner.(*sqlparser.ColName)
	if isColName && b.SimplifiedExpr != b.Inner {
		return &sqlparser.AliasedExpr{
			Expr: b.SimplifiedExpr,
			As:   col.Name,
		}
	}
	if !isColName && b.SimplifiedExpr != b.Inner {
		panic("this should not happen - different inner and weighStringExpr and not a column alias")
	}

	return &sqlparser.AliasedExpr{
		Expr: b.SimplifiedExpr,
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
		return nil, vterrors.VT12001("'*' expression in cross-shard query")
	default:
		return nil, vterrors.VT12001(fmt.Sprintf("not an aliased expression: %T", expr))
	}
}

// CreateQPFromSelect creates the QueryProjection for the input *sqlparser.Select
func CreateQPFromSelect(ctx *plancontext.PlanningContext, sel *sqlparser.Select) (*QueryProjection, error) {
	qp := &QueryProjection{
		Distinct: sel.Distinct,
	}

	if err := qp.addSelectExpressions(sel); err != nil {
		return nil, err
	}
	if err := qp.addGroupBy(ctx, sel.GroupBy); err != nil {
		return nil, err
	}
	if err := qp.addOrderBy(ctx, sel.OrderBy); err != nil {
		return nil, err
	}

	qp.calculateDistinct(ctx)

	return qp, nil
}

// RewriteDown stops the walker from entering inside aggregation functions
func (ar *AggrRewriter) RewriteDown() func(sqlparser.SQLNode, sqlparser.SQLNode) bool {
	return func(node, _ sqlparser.SQLNode) bool {
		if ar.Err != nil {
			return true
		}
		_, ok := node.(sqlparser.AggrFunc)
		return !ok
	}
}

// RewriteUp will go through an expression, add aggregations to the QP, and rewrite them to use column offset
func (ar *AggrRewriter) RewriteUp() func(*sqlparser.Cursor) bool {
	return func(cursor *sqlparser.Cursor) bool {
		if ar.Err != nil {
			return false
		}
		sqlNode := cursor.Node()
		fExp, ok := sqlNode.(sqlparser.AggrFunc)
		if !ok {
			return true
		}
		for offset, expr := range ar.qp.SelectExprs {
			ae, err := expr.GetAliasedExpr()
			if err != nil {
				ar.Err = err
				return false
			}
			if ar.st.EqualsExprWithDeps(ae.Expr, fExp) {
				cursor.Replace(sqlparser.NewOffset(offset, fExp))
				return true
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

		return true
	}
}

// AggrRewriter extracts
func (qp *QueryProjection) AggrRewriter(ctx *plancontext.PlanningContext) *AggrRewriter {
	return &AggrRewriter{
		qp: qp,
		st: ctx.SemTable,
	}
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
			return vterrors.VT13001(fmt.Sprintf("%T in select list", selExp))
		}
	}
	return nil
}

// CreateQPFromUnion creates the QueryProjection for the input *sqlparser.Union
func CreateQPFromUnion(ctx *plancontext.PlanningContext, union *sqlparser.Union) (*QueryProjection, error) {
	qp := &QueryProjection{}

	sel := sqlparser.GetFirstSelect(union)
	err := qp.addSelectExpressions(sel)
	if err != nil {
		return nil, err
	}

	err = qp.addOrderBy(ctx, union.OrderBy)
	if err != nil {
		return nil, err
	}

	return qp, nil
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

func (qp *QueryProjection) addOrderBy(ctx *plancontext.PlanningContext, orderBy sqlparser.OrderBy) error {
	canPushDownSorting := true
	es := &expressionSet{}
	for _, order := range orderBy {
		simpleExpr := qp.GetSimplifiedExpr(order.Expr)
		if sqlparser.IsNull(simpleExpr) {
			// ORDER BY null can safely be ignored
			continue
		}
		if !es.add(ctx, simpleExpr) {
			continue
		}
		qp.OrderExprs = append(qp.OrderExprs, ops.OrderBy{
			Inner:          sqlparser.CloneRefOfOrder(order),
			SimplifiedExpr: simpleExpr,
		})
		canPushDownSorting = canPushDownSorting && !sqlparser.ContainsAggregation(simpleExpr)
	}
	qp.CanPushDownSorting = canPushDownSorting
	return nil
}

func (qp *QueryProjection) calculateDistinct(ctx *plancontext.PlanningContext) {
	if qp.Distinct && !qp.HasAggr {
		// grouping and distinct both lead to unique results, so we don't need
		qp.groupByExprs = nil
	}

	if qp.HasAggr && len(qp.groupByExprs) == 0 {
		// this is a scalar aggregation and is inherently distinct
		qp.Distinct = false
	}

	if !qp.Distinct || len(qp.groupByExprs) == 0 {
		return
	}

	for _, gb := range qp.groupByExprs {
		_, found := canReuseColumn(ctx, qp.SelectExprs, gb.SimplifiedExpr, func(expr SelectExpr) sqlparser.Expr {
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

func (qp *QueryProjection) addGroupBy(ctx *plancontext.PlanningContext, groupBy sqlparser.GroupBy) error {
	es := &expressionSet{}
	for _, group := range groupBy {
		selectExprIdx, aliasExpr := qp.FindSelectExprIndexForExpr(ctx, group)
		simpleExpr := qp.GetSimplifiedExpr(group)
		err := checkForInvalidGroupingExpressions(simpleExpr)
		if err != nil {
			return err
		}

		if !es.add(ctx, simpleExpr) {
			continue
		}

		groupBy := NewGroupBy(group, simpleExpr, aliasExpr)
		groupBy.InnerIndex = selectExprIdx

		qp.groupByExprs = append(qp.groupByExprs, groupBy)
	}
	return nil
}

// GetGrouping returns a copy of the grouping parameters of the QP
func (qp *QueryProjection) GetGrouping() []GroupBy {
	return slices.Clone(qp.groupByExprs)
}

func checkForInvalidAggregations(exp *sqlparser.AliasedExpr) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		aggrFunc, isAggregate := node.(sqlparser.AggrFunc)
		if !isAggregate {
			return true, nil
		}
		args := aggrFunc.GetArgs()
		if args != nil && len(args) != 1 {
			return false, vterrors.VT03001(sqlparser.String(node))
		}
		return true, nil

	}, exp.Expr)
}

func (qp *QueryProjection) isExprInGroupByExprs(ctx *plancontext.PlanningContext, expr SelectExpr) bool {
	for _, groupByExpr := range qp.groupByExprs {
		exp, err := expr.GetExpr()
		if err != nil {
			return false
		}
		if ctx.SemTable.EqualsExprWithDeps(groupByExpr.SimplifiedExpr, exp) {
			return true
		}
	}
	return false
}

// GetSimplifiedExpr takes an expression used in ORDER BY or GROUP BY, and returns an expression that is simpler to evaluate
func (qp *QueryProjection) GetSimplifiedExpr(e sqlparser.Expr) sqlparser.Expr {
	// If the ORDER BY is against a column alias, we need to remember the expression
	// behind the alias. The weightstring(.) calls needs to be done against that expression and not the alias.
	// Eg - select music.foo as bar, weightstring(music.foo) from music order by bar

	colExpr, isColName := e.(*sqlparser.ColName)
	if !(isColName && colExpr.Qualifier.IsEmpty()) {
		// we are only interested in unqualified column names. if it's not a column name and not
		return e
	}

	for _, selectExpr := range qp.SelectExprs {
		aliasedExpr, isAliasedExpr := selectExpr.Col.(*sqlparser.AliasedExpr)
		if !isAliasedExpr {
			continue
		}
		aliased := !aliasedExpr.As.IsEmpty()
		if aliased && colExpr.Name.Equal(aliasedExpr.As) {
			return aliasedExpr.Expr
		}
	}

	return e
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

// NeedsProjecting returns true if we have projections that need to be evaluated at the vtgate level
// and can't be pushed down to MySQL
func (qp *QueryProjection) NeedsProjecting(
	ctx *plancontext.PlanningContext,
	pusher func(expr *sqlparser.AliasedExpr) (int, error),
) (needsVtGateEval bool, expressions []sqlparser.Expr, colNames []string, err error) {
	for _, se := range qp.SelectExprs {
		var ae *sqlparser.AliasedExpr
		ae, err = se.GetAliasedExpr()
		if err != nil {
			return false, nil, nil, err
		}

		expr := ae.Expr
		colNames = append(colNames, ae.ColumnName())

		if _, isCol := expr.(*sqlparser.ColName); isCol {
			offset, err := pusher(ae)
			if err != nil {
				return false, nil, nil, err
			}
			expressions = append(expressions, sqlparser.NewOffset(offset, expr))
			continue
		}

		stopOnError := func(sqlparser.SQLNode, sqlparser.SQLNode) bool {
			return err == nil
		}
		rewriter := func(cursor *sqlparser.CopyOnWriteCursor) {
			col, isCol := cursor.Node().(*sqlparser.ColName)
			if !isCol {
				return
			}
			var tableInfo semantics.TableInfo
			tableInfo, err = ctx.SemTable.TableInfoForExpr(col)
			if err != nil {
				return
			}
			dt, isDT := tableInfo.(*semantics.DerivedTable)
			if !isDT {
				return
			}

			rewritten := semantics.RewriteDerivedTableExpression(col, dt)
			if sqlparser.ContainsAggregation(rewritten) {
				offset, tErr := pusher(&sqlparser.AliasedExpr{Expr: col})
				if tErr != nil {
					err = tErr
					return
				}
				cursor.Replace(sqlparser.NewOffset(offset, col))
			}
		}
		newExpr := sqlparser.CopyOnRewrite(expr, stopOnError, rewriter, nil)

		if err != nil {
			return
		}

		if newExpr != expr {
			// if we changed the expression, it means that we have to evaluate the rest at the vtgate level
			expressions = append(expressions, newExpr.(sqlparser.Expr))
			needsVtGateEval = true
			continue
		}

		// we did not need to push any parts of this expression down. Let's check if we can push all of it
		offset, err := pusher(ae)
		if err != nil {
			return false, nil, nil, err
		}
		expressions = append(expressions, sqlparser.NewOffset(offset, expr))
	}

	return
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

func (qp *QueryProjection) AggregationExpressions(ctx *plancontext.PlanningContext) (out []Aggr, err error) {
orderBy:
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
			Aggr: sqlparser.ContainsAggregation(orderExpr),
		})
		qp.AddedColumn++
	}

	// Here we go over the expressions we are returning. Since we know we are aggregating,
	// all expressions have to be either grouping expressions or aggregate expressions.
	// If we find an expression that is neither, we treat is as a special aggregation function AggrRandom
	for idx, expr := range qp.SelectExprs {
		aliasedExpr, err := expr.GetAliasedExpr()
		if err != nil {
			return nil, err
		}

		idxCopy := idx

		if !sqlparser.ContainsAggregation(expr.Col) {
			if !qp.isExprInGroupByExprs(ctx, expr) {
				out = append(out, Aggr{
					Original: aliasedExpr,
					OpCode:   opcode.AggregateRandom,
					Alias:    aliasedExpr.ColumnName(),
					Index:    &idxCopy,
				})
			}
			continue
		}
		fnc, isAggregate := aliasedExpr.Expr.(sqlparser.AggrFunc)
		if !isAggregate {
			return nil, vterrors.VT12001("in scatter query: complex aggregate expression")
		}

		code := opcode.SupportedAggregates[strings.ToLower(fnc.AggrName())]

		if code == opcode.AggregateCount {
			if _, isStar := fnc.(*sqlparser.CountStar); isStar {
				code = opcode.AggregateCountStar
			}
		}

		aggr, _ := aliasedExpr.Expr.(sqlparser.AggrFunc)

		if aggr.IsDistinct() {
			switch code {
			case opcode.AggregateCount:
				code = opcode.AggregateCountDistinct
			case opcode.AggregateSum:
				code = opcode.AggregateSumDistinct
			}
		}

		out = append(out, Aggr{
			Original: aliasedExpr,
			Func:     aggr,
			OpCode:   code,
			Alias:    aliasedExpr.ColumnName(),
			Index:    &idxCopy,
			Distinct: aggr.IsDistinct(),
		})
	}
	return
}

// FindSelectExprIndexForExpr returns the index of the given expression in the select expressions, if it is part of it
// returns -1 otherwise.
func (qp *QueryProjection) FindSelectExprIndexForExpr(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (*int, *sqlparser.AliasedExpr) {
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
		if ctx.SemTable.EqualsExprWithDeps(aliasedExpr.Expr, expr) {
			return &idx, aliasedExpr
		}
	}
	return nil, nil
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
				if !used[i] && ctx.SemTable.EqualsExpr(groupingExpr.SimplifiedExpr, orderExpr.SimplifiedExpr) {
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
	if qp.hasCheckedAlignment {
		return false
	}
	qp.hasCheckedAlignment = true
	newGrouping := make([]GroupBy, 0, len(qp.groupByExprs))
	used := make([]bool, len(qp.groupByExprs))

outer:
	for _, orderBy := range qp.OrderExprs {
		for gidx, groupBy := range qp.groupByExprs {
			if ctx.SemTable.EqualsExprWithDeps(groupBy.SimplifiedExpr, orderBy.SimplifiedExpr) {
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

// checkAggregationSupported checks if the aggregation is supported on the given operator tree or not.
// We don't currently support planning for operators having derived tables.
func checkAggregationSupported(op ops.Operator) error {
	return rewrite.Visit(op, func(operator ops.Operator) error {
		_, isDerived := operator.(*Derived)
		projection, isProjection := operator.(*Projection)
		if isDerived || (isProjection && projection.TableID != nil) {
			return errHorizonNotPlanned()
		}
		return nil
	})
}

func checkForInvalidGroupingExpressions(expr sqlparser.Expr) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, isAggregate := node.(sqlparser.AggrFunc); isAggregate {
			return false, vterrors.VT03005(sqlparser.String(expr))
		}
		_, isSubQ := node.(*sqlparser.Subquery)
		arg, isArg := node.(*sqlparser.Argument)
		if isSubQ || (isArg && strings.HasPrefix(arg.Name, "__sq")) {
			return false, vterrors.VT12001("subqueries in GROUP BY")
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
