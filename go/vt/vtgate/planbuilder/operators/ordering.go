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
	"slices"
	"strings"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Ordering struct {
	Source  ops.Operator
	Offset  []int
	WOffset []int

	Order         []ops.OrderBy
	ResultColumns int
}

func (o *Ordering) Clone(inputs []ops.Operator) ops.Operator {
	return &Ordering{
		Source:        inputs[0],
		Offset:        slices.Clone(o.Offset),
		WOffset:       slices.Clone(o.WOffset),
		Order:         slices.Clone(o.Order),
		ResultColumns: o.ResultColumns,
	}
}

func (o *Ordering) Inputs() []ops.Operator {
	return []ops.Operator{o.Source}
}

func (o *Ordering) SetInputs(operators []ops.Operator) {
	o.Source = operators[0]
}

func (o *Ordering) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := o.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	o.Source = newSrc
	return o, nil
}

func (o *Ordering) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) (int, error) {
	return o.Source.AddColumn(ctx, reuse, gb, expr)
}

func (o *Ordering) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return o.Source.FindCol(ctx, expr, underRoute)
}

func (o *Ordering) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return o.Source.GetColumns(ctx)
}

func (o *Ordering) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return o.Source.GetSelectExprs(ctx)
}

func (o *Ordering) GetOrdering() ([]ops.OrderBy, error) {
	return o.Order, nil
}

func (o *Ordering) planOffsets(ctx *plancontext.PlanningContext) error {
	for _, order := range o.Order {
		offset, err := o.Source.AddColumn(ctx, true, false, aeWrap(order.SimplifiedExpr))
		if err != nil {
			return err
		}
		o.Offset = append(o.Offset, offset)

		if !ctx.SemTable.NeedsWeightString(order.SimplifiedExpr) {
			o.WOffset = append(o.WOffset, -1)
			continue
		}

		wsExpr := &sqlparser.WeightStringFuncExpr{Expr: order.SimplifiedExpr}
		offset, err = o.Source.AddColumn(ctx, true, false, aeWrap(wsExpr))
		if err != nil {
			return err
		}
		o.WOffset = append(o.WOffset, offset)
	}

	return nil
}

func (o *Ordering) ShortDescription() string {
	ordering := slice.Map(o.Order, func(o ops.OrderBy) string {
		return sqlparser.String(o.SimplifiedExpr)
	})
	return strings.Join(ordering, ", ")
}

func (o *Ordering) setTruncateColumnCount(offset int) {
	o.ResultColumns = offset
}
