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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	Distinct struct {
		Source ops.Operator
		QP     *QueryProjection

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

		Truncate int
	}
)

func (d *Distinct) planOffsets(ctx *plancontext.PlanningContext) error {
	columns, err := d.GetColumns(ctx)
	if err != nil {
		return err
	}
	for idx, col := range columns {
		var wsCol *int
		typ, coll, _ := ctx.SemTable.TypeForExpr(col.Expr)

		if ctx.SemTable.NeedsWeightString(col.Expr) {
			offset, err := d.Source.AddColumn(ctx, true, false, aeWrap(weightStringFor(col.Expr)))
			if err != nil {
				return err
			}
			wsCol = &offset
		}

		d.Columns = append(d.Columns, engine.CheckCol{
			Col:       idx,
			WsCol:     wsCol,
			Type:      typ,
			Collation: coll,
		})
	}
	return nil
}

func (d *Distinct) Clone(inputs []ops.Operator) ops.Operator {
	return &Distinct{
		Required:          d.Required,
		Source:            inputs[0],
		Columns:           slices.Clone(d.Columns),
		QP:                d.QP,
		PushedPerformance: d.PushedPerformance,
		Truncate:          d.Truncate,
	}
}

func (d *Distinct) Inputs() []ops.Operator {
	return []ops.Operator{d.Source}
}

func (d *Distinct) SetInputs(operators []ops.Operator) {
	d.Source = operators[0]
}

func (d *Distinct) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := d.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	d.Source = newSrc
	return d, nil
}

func (d *Distinct) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) (int, error) {
	return d.Source.AddColumn(ctx, reuse, gb, expr)
}

func (d *Distinct) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return d.Source.FindCol(ctx, expr, underRoute)
}

func (d *Distinct) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return d.Source.GetColumns(ctx)
}

func (d *Distinct) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return d.Source.GetSelectExprs(ctx)
}

func (d *Distinct) ShortDescription() string {
	if d.Required {
		return "Required"
	}
	return "Performance"
}

func (d *Distinct) GetOrdering() ([]ops.OrderBy, error) {
	return d.Source.GetOrdering()
}

func (d *Distinct) setTruncateColumnCount(offset int) {
	d.Truncate = offset
}
