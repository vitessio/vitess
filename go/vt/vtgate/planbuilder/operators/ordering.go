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
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/slices2"
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
		Source:  inputs[0],
		Offset:  slices.Clone(o.Offset),
		WOffset: slices.Clone(o.WOffset),
		Order:   slices.Clone(o.Order),
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

func (o *Ordering) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, reuseExisting, addToGroupBy bool) (ops.Operator, int, error) {
	newSrc, offset, err := o.Source.AddColumn(ctx, expr, reuseExisting, addToGroupBy)
	if err != nil {
		return nil, 0, err
	}
	o.Source = newSrc
	return o, offset, nil
}

func (o *Ordering) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return o.Source.GetColumns()
}

func (o *Ordering) GetOrdering() ([]ops.OrderBy, error) {
	return o.Order, nil
}

func (o *Ordering) planOffsets(ctx *plancontext.PlanningContext) error {
	for _, order := range o.Order {
		newSrc, offset, err := o.Source.AddColumn(ctx, aeWrap(order.SimplifiedExpr), true, false)
		if err != nil {
			return err
		}
		o.Source = newSrc
		o.Offset = append(o.Offset, offset)

		if !ctx.SemTable.NeedsWeightString(order.SimplifiedExpr) {
			o.WOffset = append(o.WOffset, -1)
			continue
		}

		wsExpr := &sqlparser.WeightStringFuncExpr{Expr: order.SimplifiedExpr}
		newSrc, offset, err = o.Source.AddColumn(ctx, aeWrap(wsExpr), true, false)
		if err != nil {
			return err
		}
		o.Source = newSrc
		o.WOffset = append(o.WOffset, offset)
	}

	return nil
}

func (o *Ordering) Description() ops.OpDescription {
	return ops.OpDescription{
		OperatorType: "Ordering",
		Other:        map[string]any{},
	}
}

func (o *Ordering) ShortDescription() string {
	ordering := slices2.Map(o.Order, func(o ops.OrderBy) string {
		return sqlparser.String(o.Inner)
	})
	return strings.Join(ordering, ", ")
}

func (o *Ordering) setTruncateColumnCount(offset int) {
	o.ResultColumns = offset
}
