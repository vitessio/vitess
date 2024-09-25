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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	Distinct struct {
		unaryOperator
		QP *QueryProjection

		// When we go from AST to operator, we place DISTINCT ops in the required places in the op tree
		// These are marked as `Required`, because they are semantically important to the results of the query.
		// During planning, when we can't push down the DISTINCT op any further, we sometimes create and push down
		// additional DISTINCT ops that are not strictly required, but that limit the number of incoming rows so less
		// work has to be done. When we have pushed down these performance DISTINCTs, we set the `PushedPerformance`
		// field to true on the originating op
		Required          bool
		PushedPerformance bool

		// This is only filled in during offset planning
		Columns []engine.CheckCol

		ResultColumns int
	}
)

func newDistinct(src Operator, qp *QueryProjection, required bool) *Distinct {
	return &Distinct{
		unaryOperator: newUnaryOp(src),
		QP:            qp,
		Required:      required,
	}
}

func (d *Distinct) planOffsets(ctx *plancontext.PlanningContext) Operator {
	columns := d.GetColumns(ctx)
	for idx, col := range columns {
		e := col.Expr
		var wsCol *int
		if ctx.NeedsWeightString(e) {
			offset := d.Source.AddWSColumn(ctx, idx, false)
			wsCol = &offset
		}
		typ, _ := ctx.TypeForExpr(e)
		d.Columns = append(d.Columns, engine.CheckCol{
			Col:          idx,
			WsCol:        wsCol,
			Type:         typ,
			CollationEnv: ctx.VSchema.Environment().CollationEnv(),
		})
	}
	return nil
}

func (d *Distinct) Clone(inputs []Operator) Operator {
	kopy := *d
	kopy.Columns = slices.Clone(d.Columns)
	kopy.Source = inputs[0]
	return &kopy
}

func (d *Distinct) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	d.Source = d.Source.AddPredicate(ctx, expr)
	return d
}

func (d *Distinct) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) int {
	return d.Source.AddColumn(ctx, reuse, gb, expr)
}
func (d *Distinct) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return d.Source.AddWSColumn(ctx, offset, underRoute)
}

func (d *Distinct) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return d.Source.FindCol(ctx, expr, underRoute)
}

func (d *Distinct) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return truncate(d, d.Source.GetColumns(ctx))
}

func (d *Distinct) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return truncate(d, d.Source.GetSelectExprs(ctx))
}

func (d *Distinct) ShortDescription() string {
	if d.Required {
		return "Required"
	}
	return "Performance"
}

func (d *Distinct) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return d.Source.GetOrdering(ctx)
}

func (d *Distinct) setTruncateColumnCount(offset int) {
	d.ResultColumns = offset
}

func (d *Distinct) getTruncateColumnCount() int {
	return d.ResultColumns
}
