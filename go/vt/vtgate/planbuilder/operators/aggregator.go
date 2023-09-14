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
		// TableID will be non-nil for derived tables
		TableID *semantics.TableSet
		Alias   string
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

func (a *Aggregator) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newOp, err := a.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	a.Source = newOp
	return a, nil
}

func (a *Aggregator) addColumnWithoutPushing(expr *sqlparser.AliasedExpr, addToGroupBy bool) int {
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
	return offset
}

func (a *Aggregator) addColumnsWithoutPushing(ctx *plancontext.PlanningContext, reuse bool, groupby []bool, expr []*sqlparser.AliasedExpr) (offsets []int) {
	for i, ae := range expr {
		offsets = append(offsets, a.addColumnWithoutPushing(ae, groupby[i]))
	}
	return
}

func (a *Aggregator) isDerived() bool {
	return a.TableID != nil
}

func (a *Aggregator) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, _ bool) (int, error) {
	if a.isDerived() {
		derivedTBL, err := ctx.SemTable.TableInfoFor(*a.TableID)
		if err != nil {
			return 0, err
		}
		expr = semantics.RewriteDerivedTableExpression(expr, derivedTBL)
	}
	if offset, found := canReuseColumn(ctx, a.Columns, expr, extractExpr); found {
		return offset, nil
	}
	return -1, nil
}

func (a *Aggregator) AddColumns(ctx *plancontext.PlanningContext, reuse bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	offsets := make([]int, len(exprs))

	var groupBys []bool
	var exprsNeeded []*sqlparser.AliasedExpr
	var offsetExpected []int

	for i, expr := range exprs {
		addToGroupBy := addToGroupBy[i]

		if reuse {
			offset, err := a.findColInternal(ctx, expr, addToGroupBy)
			if err != nil {
				return nil, err
			}
			if offset >= 0 {
				offsets[i] = offset
				continue
			}
		}

		// If weight string function is received from above operator. Then check if we have a group on the expression used.
		// If it is found, then continue to push it down but with addToGroupBy true so that is the added to group by sql down in the AddColumn.
		// This also set the weight string column offset so that we would not need to add it later in aggregator operator planOffset.
		if wsExpr, isWS := expr.Expr.(*sqlparser.WeightStringFuncExpr); isWS {
			idx := slices.IndexFunc(a.Grouping, func(by GroupBy) bool {
				return ctx.SemTable.EqualsExprWithDeps(wsExpr.Expr, by.SimplifiedExpr)
			})
			if idx >= 0 {
				a.Grouping[idx].WSOffset = len(a.Columns)
				addToGroupBy = true
			}
		}

		if !addToGroupBy {
			aggr := NewAggr(opcode.AggregateAnyValue, nil, expr, expr.As.String())
			aggr.ColOffset = len(a.Columns)
			a.Aggregations = append(a.Aggregations, aggr)
		}

		offsets[i] = len(a.Columns)
		a.Columns = append(a.Columns, expr)
		groupBys = append(groupBys, addToGroupBy)
		exprsNeeded = append(exprsNeeded, expr)
		offsetExpected = append(offsetExpected, offsets[i])
	}

	incomingOffsets, err := a.Source.AddColumns(ctx, false, groupBys, exprsNeeded)
	if err != nil {
		return nil, err
	}

	for i, offset := range offsetExpected {
		if offset != incomingOffsets[i] {
			return nil, errFailedToPlan(exprsNeeded[i])
		}
	}

	return offsets, nil
}

func (a *Aggregator) findColInternal(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, addToGroupBy bool) (int, error) {
	offset, err := a.FindCol(ctx, expr.Expr, false)
	if err != nil {
		return 0, err
	}
	if offset >= 0 {
		return offset, err
	}
	if a.isDerived() {
		derivedTBL, err := ctx.SemTable.TableInfoFor(*a.TableID)
		if err != nil {
			return 0, err
		}
		expr.Expr = semantics.RewriteDerivedTableExpression(expr.Expr, derivedTBL)
	}

	// Aggregator is little special and cannot work if the input offset are not matched with the aggregation columns.
	// So, before pushing anything from above the aggregator offset planning needs to be completed.
	err = a.planOffsets(ctx)
	if err != nil {
		return 0, err
	}

	if offset, found := canReuseColumn(ctx, a.Columns, expr.Expr, extractExpr); found {
		return offset, nil
	}
	colName, isColName := expr.Expr.(*sqlparser.ColName)
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
	if a.Alias != "" {
		columns = append([]string{"derived[" + a.Alias + "]"}, columns...)
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

func (a *Aggregator) GetOrdering() ([]ops.OrderBy, error) {
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

func (aggr Aggr) getPushDownColumn() sqlparser.Expr {
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
	a.Source = &Projection{Source: a.Source}
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

		wrap := aeWrap(aggr.getPushDownColumn())
		offsets, err := a.Source.AddColumns(ctx, false, []bool{false}, []*sqlparser.AliasedExpr{wrap})
		if err != nil {
			return 0, err
		}
		offset := offsets[0]
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
		offsets, err := a.Source.AddColumns(ctx, false, []bool{true}, []*sqlparser.AliasedExpr{expr})
		if err != nil {
			return -1, err
		}
		offset := offsets[0]
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
	offsets, err := a.Source.AddColumns(ctx, true, []bool{addToGroupBy}, []*sqlparser.AliasedExpr{aliasedExpr})
	if err != nil {
		return 0, err
	}
	offset := offsets[0]
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
	newOp.Alias = ""
	newOp.TableID = nil
	return newOp
}

func (a *Aggregator) introducesTableID() semantics.TableSet {
	if a.TableID == nil {
		return semantics.EmptyTableSet()
	}
	return *a.TableID
}

var _ ops.Operator = (*Aggregator)(nil)
