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
		a.Aggregations = append(a.Aggregations, Aggr{
			Original:  expr,
			Func:      nil,
			OpCode:    opcode.AggregateRandom,
			Alias:     expr.As.String(),
			ColOffset: offset,
		})
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
		a.Aggregations = append(a.Aggregations, Aggr{
			Original:  expr,
			Func:      nil,
			OpCode:    opcode.AggregateRandom,
			Alias:     expr.As.String(),
			ColOffset: len(a.Columns),
		})
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

func (a *Aggregator) GetColumns() (columns []*sqlparser.AliasedExpr, err error) {
	return a.Columns, nil
}

func (a *Aggregator) Description() ops.OpDescription {
	return ops.OpDescription{
		OperatorType: "Aggregator",
	}
}

func (a *Aggregator) ShortDescription() string {
	columnns := slices2.Map(a.Columns, func(from *sqlparser.AliasedExpr) string {
		return sqlparser.String(from)
	})

	if len(a.Grouping) == 0 {
		return strings.Join(columnns, ", ")
	}

	var grouping []string
	for _, gb := range a.Grouping {
		grouping = append(grouping, sqlparser.String(gb.SimplifiedExpr))
	}

	org := ""
	if a.Original {
		org = "ORG "
	}

	return fmt.Sprintf("%s%s group by %s", org, strings.Join(columnns, ", "), strings.Join(grouping, ","))
}

func (a *Aggregator) GetOrdering() ([]ops.OrderBy, error) {
	return a.Source.GetOrdering()
}

var _ ops.Operator = (*Aggregator)(nil)

func (a *Aggregator) planOffsets(ctx *plancontext.PlanningContext) error {
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
		if a.Grouping[idx].WSOffset != -1 || !ctx.SemTable.NeedsWeightString(gb.SimplifiedExpr) {
			continue
		}

		offset, err := addColumn(aeWrap(weightStringFor(gb.SimplifiedExpr)), true)
		if err != nil {
			return err
		}
		a.Grouping[idx].WSOffset = offset
	}

	return nil
}

func (a *Aggregator) setTruncateColumnCount(offset int) {
	a.ResultColumns = offset
}
