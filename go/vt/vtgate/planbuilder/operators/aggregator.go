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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// Aggregator represents a GroupBy γ relational operator.
	// Both all aggregations and no grouping, and the inverse
	// of all grouping and no aggregations are valid configurations of this operator
	Aggregator struct {
		Source  ops.Operator
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

func (a *Aggregator) Clone(inputs []ops.Operator) ops.Operator {
	kopy := *a
	kopy.Source = inputs[0]
	kopy.Columns = slices.Clone(a.Columns)
	kopy.Grouping = slices.Clone(a.Grouping)
	kopy.Aggregations = slices.Clone(a.Aggregations)
	return &kopy
}

func (a *Aggregator) Inputs() []ops.Operator {
	return []ops.Operator{a.Source}
}

func (a *Aggregator) SetInputs(operators []ops.Operator) {
	if len(operators) != 1 {
		panic(fmt.Sprintf("unexpected number of operators as input in aggregator: %d", len(operators)))
	}
	a.Source = operators[0]
}

func (a *Aggregator) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) ops.Operator {
	return &Filter{
		Source:     a,
		Predicates: []sqlparser.Expr{expr},
	}
}

func (a *Aggregator) addColumnWithoutPushing(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, addToGroupBy bool) (int, error) {
	offset := len(a.Columns)
	a.Columns = append(a.Columns, expr)

	if addToGroupBy {
		groupBy := NewGroupBy(expr.Expr, expr.Expr, expr)
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
	return offset, nil
}

func (a *Aggregator) addColumnsWithoutPushing(ctx *plancontext.PlanningContext, reuse bool, groupby []bool, exprs []*sqlparser.AliasedExpr) (offsets []int, err error) {
	for i, ae := range exprs {
		offset, err := a.addColumnWithoutPushing(ctx, ae, groupby[i])
		if err != nil {
			return nil, err
		}
		offsets = append(offsets, offset)
	}
	return
}

func (a *Aggregator) isDerived() bool {
	return a.DT != nil
}

func (a *Aggregator) FindCol(ctx *plancontext.PlanningContext, in sqlparser.Expr, _ bool) int {
	expr, err := a.DT.RewriteExpression(ctx, in)
	if err != nil {
		panic(err)
	}
	if offset, found := canReuseColumn(ctx, a.Columns, expr, extractExpr); found {
		return offset
	}
	return -1
}

func (a *Aggregator) AddColumn(ctx *plancontext.PlanningContext, reuse bool, groupBy bool, ae *sqlparser.AliasedExpr) int {
	rewritten, err := a.DT.RewriteExpression(ctx, ae.Expr)
	if err != nil {
		panic(err)
	}

	ae = &sqlparser.AliasedExpr{
		Expr: rewritten,
		As:   ae.As,
	}

	if reuse {
		offset, err := a.findColInternal(ctx, ae, groupBy)
		if err != nil {
			panic(err)
		}
		if offset >= 0 {
			return offset
		}
	}

	// Upon receiving a weight string function from an upstream operator, check for an existing grouping on the argument expression.
	// If a grouping is found, continue to push the function down, marking it with 'addToGroupBy' to ensure it's correctly treated as a grouping column.
	// This process also sets the weight string column offset, eliminating the need for a later addition in the aggregator operator's planOffset.
	if wsExpr, isWS := rewritten.(*sqlparser.WeightStringFuncExpr); isWS {
		idx := slices.IndexFunc(a.Grouping, func(by GroupBy) bool {
			return ctx.SemTable.EqualsExprWithDeps(wsExpr.Expr, by.SimplifiedExpr)
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

func (a *Aggregator) findColInternal(ctx *plancontext.PlanningContext, ae *sqlparser.AliasedExpr, addToGroupBy bool) (int, error) {
	expr := ae.Expr
	offset := a.FindCol(ctx, expr, false)
	if offset >= 0 {
		return offset, nil
	}
	expr, err := a.DT.RewriteExpression(ctx, expr)
	if err != nil {
		return 0, err
	}

	// Aggregator is little special and cannot work if the input offset are not matched with the aggregation columns.
	// So, before pushing anything from above the aggregator offset planning needs to be completed.
	err = a.planOffsets(ctx)
	if err != nil {
		return 0, err
	}

	if offset, found := canReuseColumn(ctx, a.Columns, expr, extractExpr); found {
		return offset, nil
	}
	colName, isColName := expr.(*sqlparser.ColName)
	for i, col := range a.Columns {
		if isColName && colName.Name.EqualString(col.As.String()) {
			return i, nil
		}
	}

	if addToGroupBy {
		return 0, vterrors.VT13001("did not expect to add group by here")
	}

	return -1, nil
}

func (a *Aggregator) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	if _, isSourceDerived := a.Source.(*Horizon); isSourceDerived {
		return a.Columns, nil
	}

	// we update the incoming columns, so we know about any new columns that have been added
	// in the optimization phase, other operators could be pushed down resulting in additional columns for aggregator.
	// Aggregator should be made aware of these to truncate them in final result.
	columns, err := a.Source.GetColumns(ctx)
	if err != nil {
		return nil, err
	}

	// if this operator is producing more columns than expected, we want to know about it
	if len(columns) > len(a.Columns) {
		a.Columns = append(a.Columns, columns[len(a.Columns):]...)
	}

	return a.Columns, nil
}

func (a *Aggregator) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
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
		grouping = append(grouping, sqlparser.String(gb.SimplifiedExpr))
	}

	return fmt.Sprintf("%s%s group by %s", org, strings.Join(columns, ", "), strings.Join(grouping, ","))
}

func (a *Aggregator) GetOrdering() []ops.OrderBy {
	return a.Source.GetOrdering()
}

func (a *Aggregator) planOffsets(ctx *plancontext.PlanningContext) error {
	if a.offsetPlanned {
		return nil
	}
	defer func() {
		a.offsetPlanned = true
	}()
	if !a.Pushed {
		return a.planOffsetsNotPushed(ctx)
	}

	for idx, gb := range a.Grouping {
		if gb.ColOffset == -1 {
			offset, err := a.internalAddColumn(ctx, aeWrap(gb.Inner), false)
			if err != nil {
				return err
			}
			a.Grouping[idx].ColOffset = offset
		}
		if gb.WSOffset != -1 || !ctx.SemTable.NeedsWeightString(gb.SimplifiedExpr) {
			continue
		}

		offset, err := a.internalAddColumn(ctx, aeWrap(weightStringFor(gb.SimplifiedExpr)), true)
		if err != nil {
			return err
		}
		a.Grouping[idx].WSOffset = offset
	}

	for idx, aggr := range a.Aggregations {
		if !aggr.NeedsWeightString(ctx) {
			continue
		}
		offset, err := a.internalAddColumn(ctx, aeWrap(weightStringFor(aggr.Func.GetArg())), true)
		if err != nil {
			return err
		}
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
			panic("more than 1 column")
		}
		fallthrough
	default:
		return aggr.Func.GetArg()
	}
}

func (a *Aggregator) planOffsetsNotPushed(ctx *plancontext.PlanningContext) error {
	a.Source = newAliasedProjection(a.Source)
	// we need to keep things in the column order, so we can't iterate over the aggregations or groupings
	for colIdx := range a.Columns {
		idx, err := a.addIfGroupingColumn(ctx, colIdx)
		if err != nil {
			return err
		}
		if idx >= 0 {
			continue
		}

		idx, err = a.addIfAggregationColumn(ctx, colIdx)
		if err != nil {
			return err
		}

		if idx < 0 {
			return vterrors.VT13001("failed to find the corresponding column")
		}
	}

	return a.pushRemainingGroupingColumnsAndWeightStrings(ctx)
}

func (a *Aggregator) addIfAggregationColumn(ctx *plancontext.PlanningContext, colIdx int) (int, error) {
	for _, aggr := range a.Aggregations {
		if aggr.ColOffset != colIdx {
			continue
		}

		wrap := aeWrap(aggr.getPushColumn())
		offset := a.Source.AddColumn(ctx, false, false, wrap)
		if aggr.ColOffset != offset {
			return -1, errFailedToPlan(aggr.Original)
		}

		return offset, nil
	}
	return -1, nil
}

func errFailedToPlan(original *sqlparser.AliasedExpr) *vterrors.VitessError {
	return vterrors.VT12001(fmt.Sprintf("failed to plan aggregation on: %s", sqlparser.String(original)))
}

func (a *Aggregator) addIfGroupingColumn(ctx *plancontext.PlanningContext, colIdx int) (int, error) {
	for _, gb := range a.Grouping {
		if gb.ColOffset != colIdx {
			continue
		}

		expr := a.Columns[colIdx]
		offset := a.Source.AddColumn(ctx, false, true, expr)
		if gb.ColOffset != offset {
			return -1, errFailedToPlan(expr)
		}

		return offset, nil
	}
	return -1, nil
}

// pushRemainingGroupingColumnsAndWeightStrings pushes any grouping column that is not part of the columns list and weight strings needed for performing grouping aggregations.
func (a *Aggregator) pushRemainingGroupingColumnsAndWeightStrings(ctx *plancontext.PlanningContext) error {
	for idx, gb := range a.Grouping {
		if gb.ColOffset == -1 {
			offset, err := a.internalAddColumn(ctx, aeWrap(gb.Inner), false)
			if err != nil {
				return err
			}
			a.Grouping[idx].ColOffset = offset
		}

		if gb.WSOffset != -1 || !ctx.SemTable.NeedsWeightString(gb.SimplifiedExpr) {
			continue
		}

		offset, err := a.internalAddColumn(ctx, aeWrap(weightStringFor(gb.SimplifiedExpr)), false)
		if err != nil {
			return err
		}
		a.Grouping[idx].WSOffset = offset
	}
	for idx, aggr := range a.Aggregations {
		if aggr.WSOffset != -1 || !aggr.NeedsWeightString(ctx) {
			continue
		}

		offset, err := a.internalAddColumn(ctx, aeWrap(weightStringFor(aggr.Func.GetArg())), false)
		if err != nil {
			return err
		}
		a.Aggregations[idx].WSOffset = offset
	}
	return nil
}

func (a *Aggregator) setTruncateColumnCount(offset int) {
	a.ResultColumns = offset
}

func (a *Aggregator) internalAddColumn(ctx *plancontext.PlanningContext, aliasedExpr *sqlparser.AliasedExpr, addToGroupBy bool) (int, error) {
	offset := a.Source.AddColumn(ctx, true, addToGroupBy, aliasedExpr)

	if offset == len(a.Columns) {
		// if we get an offset at the end of our current column list, it means we added a new column
		a.Columns = append(a.Columns, aliasedExpr)
	}
	return offset, nil
}

// SplitAggregatorBelowRoute returns the aggregator that will live under the Route.
// This is used when we are splitting the aggregation so one part is done
// at the mysql level and one part at the vtgate level
func (a *Aggregator) SplitAggregatorBelowRoute(input []ops.Operator) *Aggregator {
	newOp := a.Clone(input).(*Aggregator)
	newOp.Pushed = false
	newOp.Original = false
	newOp.DT = nil
	return newOp
}

func (a *Aggregator) introducesTableID() semantics.TableSet {
	return a.DT.introducesTableID()
}

var _ ops.Operator = (*Aggregator)(nil)
