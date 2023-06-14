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
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/slices2"
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

		// Pushed will be set to true once this aggregation has been pushed deeper in the tree
		Pushed bool

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
	return &Aggregator{
		Source:        inputs[0],
		Columns:       slices.Clone(a.Columns),
		Grouping:      slices.Clone(a.Grouping),
		Aggregations:  slices.Clone(a.Aggregations),
		Pushed:        a.Pushed,
		Original:      a.Original,
		ResultColumns: a.ResultColumns,
		QP:            a.QP,
	}
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
			aggr = NewAggr(opcode.AggregateRandom, nil, expr, expr.As.String())
		}
		aggr.ColOffset = offset
		a.Aggregations = append(a.Aggregations, aggr)
	}
	return offset
}

func (a *Aggregator) isDerived() bool {
	return a.TableID != nil
}

func (a *Aggregator) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, _, addToGroupBy bool) (ops.Operator, int, error) {
	if addToGroupBy {
		return nil, 0, vterrors.VT13001("did not expect to add group by here")
	}
	if offset, found := canReuseColumn(ctx, a.Columns, expr.Expr, extractExpr); found {
		return a, offset, nil
	}
	colName, isColName := expr.Expr.(*sqlparser.ColName)
	for i, col := range a.Columns {
		if isColName && colName.Name.EqualString(col.As.String()) {
			return a, i, nil
		}
	}

	// If weight string function is received from above operator. Then check if we have a group on the expression used.
	// If it is found, then continue to push it down but with addToGroupBy true so that is the added to group by sql down in the AddColumn.
	// This also set the weight string column offset so that we would not need to add it later in aggregator operator planOffset.

	// If the expression is a WeightStringFuncExpr, it checks if a GroupBy
	// already exists on the argument of the expression.
	// If it is found, the column offset for the WeightStringFuncExpr is set,
	// and the column is marked to be added to the GroupBy in the recursive AddColumn call.
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
		aggr := NewAggr(opcode.AggregateRandom, nil, expr, expr.As.String())
		aggr.ColOffset = len(a.Columns)
		a.Aggregations = append(a.Aggregations, aggr)
	}
	a.Columns = append(a.Columns, expr)
	expectedOffset := len(a.Columns) - 1
	newSrc, offset, err := a.Source.AddColumn(ctx, expr, false, addToGroupBy)
	if err != nil {
		return nil, 0, err
	}
	if offset != expectedOffset {
		return nil, 0, vterrors.VT13001("the offset needs to be aligned here")
	}
	a.Source = newSrc
	return a, offset, nil
}

func (a *Aggregator) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	// we update the incoming columns, so we know about any new columns that have been added
	// in the optimization phase, other operators could be pushed down resulting in additional columns for aggregator.
	// Aggregator should be made aware of these to truncate them in final result.
	columns, err := a.Source.GetColumns()
	if err != nil {
		return nil, err
	}

	// if this operator is producing more columns than expected, we want to know about it
	if len(columns) > len(a.Columns) {
		a.Columns = append(a.Columns, columns[len(a.Columns):]...)
	}

	return a.Columns, nil
}

func (a *Aggregator) ShortDescription() string {
	columnns := slices2.Map(a.Columns, func(from *sqlparser.AliasedExpr) string {
		return sqlparser.String(from)
	})

	org := ""
	if a.Original {
		org = "ORG "
	}

	if len(a.Grouping) == 0 {
		return fmt.Sprintf("%s%s", org, strings.Join(columnns, ", "))
	}

	var grouping []string
	for _, gb := range a.Grouping {
		grouping = append(grouping, sqlparser.String(gb.SimplifiedExpr))
	}

	return fmt.Sprintf("%s%s group by %s", org, strings.Join(columnns, ", "), strings.Join(grouping, ","))
}

func (a *Aggregator) GetOrdering() ([]ops.OrderBy, error) {
	return a.Source.GetOrdering()
}

func (a *Aggregator) planOffsets(ctx *plancontext.PlanningContext) error {
	if !a.Pushed {
		return a.planOffsetsNotPushed(ctx)
	}

	addColumn := func(aliasedExpr *sqlparser.AliasedExpr, addToGroupBy bool) (int, error) {
		newSrc, offset, err := a.Source.AddColumn(ctx, aliasedExpr, true, addToGroupBy)
		if err != nil {
			return 0, err
		}
		a.Source = newSrc
		if offset == len(a.Columns) {
			// if we get an offset at the end of our current column list, it means we added a new column
			a.Columns = append(a.Columns, aliasedExpr)
		}
		return offset, nil
	}

	for idx, gb := range a.Grouping {
		if gb.ColOffset == -1 {
			offset, err := addColumn(aeWrap(gb.Inner), false)
			if err != nil {
				return err
			}
			a.Grouping[idx].ColOffset = offset
		}
		if gb.WSOffset != -1 || !ctx.SemTable.NeedsWeightString(gb.SimplifiedExpr) {
			continue
		}

		offset, err := addColumn(aeWrap(weightStringFor(gb.SimplifiedExpr)), true)
		if err != nil {
			return err
		}
		a.Grouping[idx].WSOffset = offset
	}

	for idx, aggr := range a.Aggregations {
		if !aggr.NeedWeightString(ctx) {
			continue
		}
		offset, err := addColumn(aeWrap(weightStringFor(aggr.Func.GetArg())), true)
		if err != nil {
			return err
		}
		a.Aggregations[idx].WSOffset = offset
	}

	return nil
}

func (aggr Aggr) getPushDownColumn() sqlparser.Expr {
	switch aggr.OpCode {
	case opcode.AggregateRandom:
		return aggr.Original.Expr
	case opcode.AggregateCountStar:
		return sqlparser.NewIntLiteral("1")
	default:
		return aggr.Func.GetArg()
	}
}

func (a *Aggregator) planOffsetsNotPushed(ctx *plancontext.PlanningContext) error {
	// we need to keep things in the column order, so we can't iterate over the aggregations or groupings
	for colIdx, col := range a.Columns {
		idx, err := a.addIfGroupingColumn(ctx, col)
		if err != nil {
			return err
		}
		if idx >= 0 {
			if idx != colIdx {
				return vterrors.VT13001(fmt.Sprintf("grouping column on wrong index: want: %d, got: %d", colIdx, idx))
			}
			continue
		}

		idx, err = a.addIfAggregationColumn(ctx, col)
		if err != nil {
			return err
		}

		if idx < 0 {
			return vterrors.VT13001("failed to find the corresponding column")
		}
	}
	return nil
}

func (a *Aggregator) addIfAggregationColumn(ctx *plancontext.PlanningContext, col *sqlparser.AliasedExpr) (int, error) {
	for aggIdx, aggr := range a.Aggregations {
		if !ctx.SemTable.EqualsExprWithDeps(col.Expr, aggr.Original.Expr) {
			continue
		}

		newSrc, offset, err := a.Source.AddColumn(ctx, aeWrap(aggr.getPushDownColumn()), false, false)
		if err != nil {
			return 0, err
		}
		a.Aggregations[aggIdx].ColOffset = offset
		a.Source = newSrc
		return offset, nil
	}
	return -1, nil
}

func (a *Aggregator) addIfGroupingColumn(ctx *plancontext.PlanningContext, col *sqlparser.AliasedExpr) (int, error) {
	for gbIdx, gb := range a.Grouping {
		if !ctx.SemTable.EqualsExprWithDeps(col.Expr, gb.SimplifiedExpr) {
			continue
		}

		newSrc, offset, err := a.Source.AddColumn(ctx, col, false, false)
		if err != nil {
			return 0, err
		}

		a.Grouping[gbIdx].ColOffset = offset
		a.Source = newSrc

		if !ctx.SemTable.NeedsWeightString(gb.SimplifiedExpr) {
			return offset, nil
		}

		// TODO: we need to do stuff
		return offset, nil
	}
	return -1, nil
}

func (a *Aggregator) setTruncateColumnCount(offset int) {
	a.ResultColumns = offset
}

var _ ops.Operator = (*Aggregator)(nil)
