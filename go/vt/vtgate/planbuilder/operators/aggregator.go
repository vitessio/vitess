/*
Copyright 2023 The Vitess Authors.

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
	"fmt"
	"slices"
	"strings"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// Aggregator represents a GroupBy γ relational operator.
	// Both all aggregations and no grouping, and the inverse
	// of all grouping and no aggregations are valid configurations of this operator
	Aggregator struct {
		Source  Operator
		Columns []*sqlparser.AliasedExpr

		Grouping     []GroupBy
		Aggregations []Aggr

		// We support a single distinct aggregation per aggregator. It is stored here.
		// When planning the ordering that the OrderedAggregate will require,
		// this needs to be the last ORDER BY expression
		DistinctExpr sqlparser.Expr

		// Pushed will be set to true once this aggregation has been pushed deeper in the tree
		Pushed        bool
		offsetPlanned bool

		// Original will only be true for the original aggregator created from the AST
		Original      bool
		ResultColumns int

		QP *QueryProjection

		DT *DerivedTable
	}
)

func (a *Aggregator) Clone(inputs []Operator) Operator {
	kopy := *a
	kopy.Source = inputs[0]
	kopy.Columns = slices.Clone(a.Columns)
	kopy.Grouping = slices.Clone(a.Grouping)
	kopy.Aggregations = slices.Clone(a.Aggregations)
	return &kopy
}

func (a *Aggregator) Inputs() []Operator {
	return []Operator{a.Source}
}

func (a *Aggregator) SetInputs(operators []Operator) {
	if len(operators) != 1 {
		panic(fmt.Sprintf("unexpected number of operators as input in aggregator: %d", len(operators)))
	}
	a.Source = operators[0]
}

func (a *Aggregator) AddPredicate(_ *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return newFilter(a, expr)
}

func (a *Aggregator) addColumnWithoutPushing(_ *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, addToGroupBy bool) int {
	offset := len(a.Columns)
	a.Columns = append(a.Columns, expr)

	if addToGroupBy {
		groupBy := NewGroupBy(expr.Expr)
		groupBy.ColOffset = offset
		a.Grouping = append(a.Grouping, groupBy)
	} else {
		var aggr Aggr
		switch e := expr.Expr.(type) {
		case sqlparser.AggrFunc:
			aggr = createAggrFromAggrFunc(e, expr)
		default:
			aggr = NewAggr(opcode.AggregateAnyValue, nil, expr, expr.As.String())
		}
		aggr.ColOffset = offset
		a.Aggregations = append(a.Aggregations, aggr)
	}
	return offset
}

func (a *Aggregator) addColumnsWithoutPushing(ctx *plancontext.PlanningContext, reuse bool, groupby []bool, exprs []*sqlparser.AliasedExpr) (offsets []int) {
	for i, ae := range exprs {
		offset := a.addColumnWithoutPushing(ctx, ae, groupby[i])
		offsets = append(offsets, offset)
	}
	return
}

func (a *Aggregator) isDerived() bool {
	return a.DT != nil
}

func (a *Aggregator) derivedName() string {
	if a.DT == nil {
		return ""
	}

	return a.DT.Alias
}

func (a *Aggregator) FindCol(ctx *plancontext.PlanningContext, in sqlparser.Expr, underRoute bool) int {
	if underRoute && a.isDerived() {
		// We don't want to use columns on this operator if it's a derived table under a route.
		// In this case, we need to add a Projection on top of this operator to make the column available
		return -1
	}

	expr := a.DT.RewriteExpression(ctx, in)
	if offset, found := canReuseColumn(ctx, a.Columns, expr, extractExpr); found {
		return offset
	}
	return -1
}

func (a *Aggregator) AddColumn(ctx *plancontext.PlanningContext, reuse bool, groupBy bool, ae *sqlparser.AliasedExpr) int {
	rewritten := a.DT.RewriteExpression(ctx, ae.Expr)

	ae = &sqlparser.AliasedExpr{
		Expr: rewritten,
		As:   ae.As,
	}

	if reuse {
		offset := a.findColInternal(ctx, ae, groupBy)
		if offset >= 0 {
			return offset
		}
	}

	// Upon receiving a weight string function from an upstream operator, check for an existing grouping on the argument expression.
	// If a grouping is found, continue to push the function down, marking it with 'addToGroupBy' to ensure it's correctly treated as a grouping column.
	// This process also sets the weight string column offset, eliminating the need for a later addition in the aggregator operator's planOffset.
	if wsExpr, isWS := rewritten.(*sqlparser.WeightStringFuncExpr); isWS {
		idx := slices.IndexFunc(a.Grouping, func(by GroupBy) bool {
			return ctx.SemTable.EqualsExprWithDeps(wsExpr.Expr, by.Inner)
		})
		if idx >= 0 {
			a.Grouping[idx].WSOffset = len(a.Columns)
			groupBy = true
		}
	}

	if !groupBy {
		aggr := NewAggr(opcode.AggregateAnyValue, nil, ae, ae.As.String())
		aggr.ColOffset = len(a.Columns)
		a.Aggregations = append(a.Aggregations, aggr)
	}

	offset := len(a.Columns)
	a.Columns = append(a.Columns, ae)
	incomingOffset := a.Source.AddColumn(ctx, false, groupBy, ae)

	if offset != incomingOffset {
		panic(errFailedToPlan(ae))
	}

	return offset
}

func (a *Aggregator) findColInternal(ctx *plancontext.PlanningContext, ae *sqlparser.AliasedExpr, addToGroupBy bool) int {
	expr := ae.Expr
	offset := a.FindCol(ctx, expr, false)
	if offset >= 0 {
		return offset
	}

	// Aggregator is little special and cannot work if the input offset are not matched with the aggregation columns.
	// So, before pushing anything from above the aggregator offset planning needs to be completed.
	a.planOffsets(ctx)
	if offset, found := canReuseColumn(ctx, a.Columns, expr, extractExpr); found {
		return offset
	}

	if addToGroupBy {
		panic(vterrors.VT13001(fmt.Sprintf("did not expect to add group by here: %s", sqlparser.String(expr))))
	}

	return -1
}

func (a *Aggregator) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	if _, isSourceDerived := a.Source.(*Horizon); isSourceDerived {
		return a.Columns
	}

	// we update the incoming columns, so we know about any new columns that have been added
	// in the optimization phase, other operators could be pushed down resulting in additional columns for aggregator.
	// Aggregator should be made aware of these to truncate them in final result.
	columns := a.Source.GetColumns(ctx)

	// if this operator is producing more columns than expected, we want to know about it
	if len(columns) > len(a.Columns) {
		a.Columns = append(a.Columns, columns[len(a.Columns):]...)
	}

	return a.Columns
}

func (a *Aggregator) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return transformColumnsToSelectExprs(ctx, a)
}

func (a *Aggregator) ShortDescription() string {
	columns := slice.Map(a.Columns, func(from *sqlparser.AliasedExpr) string {
		return sqlparser.String(from)
	})
	if a.DT != nil {
		columns = append([]string{a.DT.String()}, columns...)
	}

	org := ""
	if a.Original {
		org = "ORG "
	}

	if len(a.Grouping) == 0 {
		return fmt.Sprintf("%s%s", org, strings.Join(columns, ", "))
	}

	var grouping []string
	for _, gb := range a.Grouping {
		grouping = append(grouping, sqlparser.String(gb.Inner))
	}

	return fmt.Sprintf("%s%s group by %s", org, strings.Join(columns, ", "), strings.Join(grouping, ","))
}

func (a *Aggregator) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return a.Source.GetOrdering(ctx)
}

func (a *Aggregator) planOffsets(ctx *plancontext.PlanningContext) Operator {
	if a.offsetPlanned {
		return nil
	}
	defer func() {
		a.offsetPlanned = true
	}()
	if !a.Pushed {
		a.planOffsetsNotPushed(ctx)
		return nil
	}

	for idx, gb := range a.Grouping {
		if gb.ColOffset == -1 {
			offset := a.internalAddColumn(ctx, aeWrap(gb.Inner), false)
			a.Grouping[idx].ColOffset = offset
		}
		if gb.WSOffset != -1 || !ctx.SemTable.NeedsWeightString(gb.Inner) {
			continue
		}

		offset := a.internalAddColumn(ctx, aeWrap(weightStringFor(gb.Inner)), true)
		a.Grouping[idx].WSOffset = offset
	}

	for idx, aggr := range a.Aggregations {
		if !aggr.NeedsWeightString(ctx) {
			continue
		}
		arg := aggr.getPushColumn()
		offset := a.internalAddColumn(ctx, aeWrap(weightStringFor(arg)), true)
		a.Aggregations[idx].WSOffset = offset
	}
	return nil
}

func (aggr Aggr) getPushColumn() sqlparser.Expr {
	switch aggr.OpCode {
	case opcode.AggregateAnyValue:
		return aggr.Original.Expr
	case opcode.AggregateCountStar:
		return sqlparser.NewIntLiteral("1")
	case opcode.AggregateGroupConcat:
		if len(aggr.Func.GetArgs()) > 1 {
			panic(vterrors.VT12001("group_concat with more than 1 column"))
		}
		return aggr.Func.GetArg()
	default:
		if len(aggr.Func.GetArgs()) > 1 {
			panic(vterrors.VT03001(sqlparser.String(aggr.Func)))
		}
		return aggr.Func.GetArg()
	}
}

func (a *Aggregator) planOffsetsNotPushed(ctx *plancontext.PlanningContext) {
	a.Source = newAliasedProjection(a.Source)
	// we need to keep things in the column order, so we can't iterate over the aggregations or groupings
	for colIdx := range a.Columns {
		idx := a.addIfGroupingColumn(ctx, colIdx)
		if idx >= 0 {
			continue
		}

		idx = a.addIfAggregationColumn(ctx, colIdx)

		if idx < 0 {
			panic(vterrors.VT13001("failed to find the corresponding column"))
		}
	}

	a.pushRemainingGroupingColumnsAndWeightStrings(ctx)
}

func (a *Aggregator) addIfAggregationColumn(ctx *plancontext.PlanningContext, colIdx int) int {
	for _, aggr := range a.Aggregations {
		if aggr.ColOffset != colIdx {
			continue
		}

		wrap := aeWrap(aggr.getPushColumn())
		offset := a.Source.AddColumn(ctx, false, false, wrap)
		if aggr.ColOffset != offset {
			panic(errFailedToPlan(aggr.Original))
		}

		return offset
	}
	return -1
}

func errFailedToPlan(original *sqlparser.AliasedExpr) *vterrors.VitessError {
	return vterrors.VT12001(fmt.Sprintf("failed to plan aggregation on: %s", sqlparser.String(original)))
}

func (a *Aggregator) addIfGroupingColumn(ctx *plancontext.PlanningContext, colIdx int) int {
	for _, gb := range a.Grouping {
		if gb.ColOffset != colIdx {
			continue
		}

		expr := a.Columns[colIdx]
		offset := a.Source.AddColumn(ctx, false, true, expr)
		if gb.ColOffset != offset {
			panic(errFailedToPlan(expr))
		}

		return offset
	}
	return -1
}

// pushRemainingGroupingColumnsAndWeightStrings pushes any grouping column that is not part of the columns list and weight strings needed for performing grouping aggregations.
func (a *Aggregator) pushRemainingGroupingColumnsAndWeightStrings(ctx *plancontext.PlanningContext) {
	for idx, gb := range a.Grouping {
		if gb.ColOffset == -1 {
			offset := a.internalAddColumn(ctx, aeWrap(gb.Inner), false)
			a.Grouping[idx].ColOffset = offset
		}

		if gb.WSOffset != -1 || !ctx.SemTable.NeedsWeightString(gb.Inner) {
			continue
		}

		offset := a.internalAddColumn(ctx, aeWrap(weightStringFor(gb.Inner)), false)
		a.Grouping[idx].WSOffset = offset
	}
	for idx, aggr := range a.Aggregations {
		if aggr.WSOffset != -1 || !aggr.NeedsWeightString(ctx) {
			continue
		}

		arg := aggr.getPushColumn()
		offset := a.internalAddColumn(ctx, aeWrap(weightStringFor(arg)), false)
		a.Aggregations[idx].WSOffset = offset
	}
}

func (a *Aggregator) setTruncateColumnCount(offset int) {
	a.ResultColumns = offset
}

func (a *Aggregator) internalAddColumn(ctx *plancontext.PlanningContext, aliasedExpr *sqlparser.AliasedExpr, addToGroupBy bool) int {
	offset := a.Source.AddColumn(ctx, true, addToGroupBy, aliasedExpr)

	if offset == len(a.Columns) {
		// if we get an offset at the end of our current column list, it means we added a new column
		a.Columns = append(a.Columns, aliasedExpr)
	}
	return offset
}

// SplitAggregatorBelowRoute returns the aggregator that will live under the Route.
// This is used when we are splitting the aggregation so one part is done
// at the mysql level and one part at the vtgate level
func (a *Aggregator) SplitAggregatorBelowRoute(input []Operator) *Aggregator {
	newOp := a.Clone(input).(*Aggregator)
	newOp.Pushed = false
	newOp.Original = false
	newOp.DT = nil
	return newOp
}

func (a *Aggregator) introducesTableID() semantics.TableSet {
	return a.DT.introducesTableID()
}

func (a *Aggregator) checkForInvalidAggregations() {
	for _, aggr := range a.Aggregations {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			aggrFunc, isAggregate := node.(sqlparser.AggrFunc)
			if !isAggregate {
				return true, nil
			}
			args := aggrFunc.GetArgs()
			if args != nil && len(args) != 1 {
				panic(vterrors.VT03001(sqlparser.String(node)))
			}
			return true, nil

		}, aggr.Original.Expr)
	}
}

var _ Operator = (*Aggregator)(nil)
