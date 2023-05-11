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

	"vitess.io/vitess/go/vt/vtgate/engine/opcode"

	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
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

func (a *Aggregator) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, reuseExisting bool) (ops.Operator, int, error) {
	if !reuseExisting {
		return nil, 0, vterrors.VT12001("reuse columns on Aggregator")
	}

	extractExpr := func(expr *sqlparser.AliasedExpr) sqlparser.Expr { return expr.Expr }
	if offset, found := canReuseColumn(ctx, a.Columns, expr.Expr, extractExpr); found {
		return a, offset, nil
	}
	colName, isColName := expr.Expr.(*sqlparser.ColName)
	for i, col := range a.Columns {
		if isColName && colName.Name.EqualString(col.As.String()) {
			return a, i, nil
		}
	}

	// if we didn't already have this column, we add it as a grouping
	a.Aggregations = append(a.Aggregations, Aggr{
		Original:  expr,
		Func:      nil,
		OpCode:    opcode.AggregateRandom,
		Alias:     expr.As.String(),
		ColOffset: len(a.Columns),
	})
	a.Columns = append(a.Columns, expr)
	return a, len(a.Columns) - 1, nil
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
		grouping = append(grouping, sqlparser.String(gb.WeightStrExpr))
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
	addColumn := func(aliasedExpr *sqlparser.AliasedExpr) (int, error) {
		newSrc, offset, err := a.Source.AddColumn(ctx, aliasedExpr, true)
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
		if gb.KeyCol == -1 {
			offset, err := addColumn(aeWrap(gb.Inner))
			if err != nil {
				return err
			}
			a.Grouping[idx].KeyCol = offset
		}
		if !ctx.SemTable.NeedsWeightString(gb.WeightStrExpr) {
			a.Grouping[idx].WSCol = -1
			continue
		}

		offset, err := addColumn(aeWrap(weightStringFor(gb.WeightStrExpr)))
		if err != nil {
			return err
		}
		a.Grouping[idx].WSCol = offset
	}
	return nil
}

func (a *Aggregator) setTruncateColumnCount(offset int) {
	a.ResultColumns = offset
}
