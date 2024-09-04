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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Ordering struct {
	unaryOperator
	Offset  []int
	WOffset []int

	Order         []OrderBy
	ResultColumns int
}

func (o *Ordering) Clone(inputs []Operator) Operator {
	klone := *o
	klone.Source = inputs[0]
	klone.Offset = slices.Clone(o.Offset)
	klone.WOffset = slices.Clone(o.WOffset)
	klone.Order = slices.Clone(o.Order)

	return &klone
}

func newOrdering(src Operator, order []OrderBy) Operator {
	return &Ordering{
		unaryOperator: newUnaryOp(src),
		Order:         order,
	}
}

func (o *Ordering) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	o.Source = o.Source.AddPredicate(ctx, expr)
	return o
}

func (o *Ordering) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) int {
	return o.Source.AddColumn(ctx, reuse, gb, expr)
}

func (o *Ordering) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return o.Source.AddWSColumn(ctx, offset, underRoute)
}

func (o *Ordering) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return o.Source.FindCol(ctx, expr, underRoute)
}

func (o *Ordering) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return truncate(o, o.Source.GetColumns(ctx))
}

func (o *Ordering) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return truncate(o, o.Source.GetSelectExprs(ctx))
}

func (o *Ordering) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return o.Order
}

func (o *Ordering) planOffsets(ctx *plancontext.PlanningContext) Operator {
	var weightStrings []*OrderBy

	for _, order := range o.Order {
		offset := o.Source.AddColumn(ctx, true, false, aeWrap(order.SimplifiedExpr))
		o.Offset = append(o.Offset, offset)

		if !ctx.NeedsWeightString(order.SimplifiedExpr) {
			weightStrings = append(weightStrings, nil)
			continue
		}
		weightStrings = append(weightStrings, &order)
	}

	for i, order := range weightStrings {
		if order == nil {
			o.WOffset = append(o.WOffset, -1)
			continue
		}
		offset := o.Source.AddWSColumn(ctx, o.Offset[i], false)
		o.WOffset = append(o.WOffset, offset)
	}
	return nil
}

func (o *Ordering) ShortDescription() string {
	ordering := slice.Map(o.Order, func(o OrderBy) string {
		return sqlparser.String(o.SimplifiedExpr)
	})
	return strings.Join(ordering, ", ")
}

func (o *Ordering) setTruncateColumnCount(offset int) {
	o.ResultColumns = offset
}

func (o *Ordering) getTruncateColumnCount() int {
	return o.ResultColumns
}
