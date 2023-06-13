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
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	Distinct struct {
		Source ops.Operator
		QP     *QueryProjection
		Pushed bool

		// When offset planning, we'll fill in this field
		Columns []engine.CheckCol

		Truncate int
	}
)

func (d *Distinct) planOffsets(ctx *plancontext.PlanningContext) error {
	columns, err := d.GetColumns()
	if err != nil {
		return err
	}
	d.Columns = nil
	var exprs []sqlparser.Expr
	for _, col := range columns {
		newSrc, offset, err := d.Source.AddColumn(ctx, col, true, false)
		if err != nil {
			return err
		}
		d.Source = newSrc
		e := d.QP.GetSimplifiedExpr(col.Expr)
		exprs = append(exprs, e)
		d.Columns = append(d.Columns, engine.CheckCol{
			Col:       offset,
			Collation: ctx.SemTable.CollationForExpr(e),
		})
	}
	for i, e := range exprs {
		if !ctx.SemTable.NeedsWeightString(e) {
			continue
		}
		newSrc, offset, err := d.Source.AddColumn(ctx, aeWrap(weightStringFor(e)), true, false)
		if err != nil {
			return err
		}
		d.Source = newSrc
		d.Columns[i].WsCol = &offset
	}
	return nil
}

func (d *Distinct) Clone(inputs []ops.Operator) ops.Operator {
	return &Distinct{
		Source:   inputs[0],
		Columns:  slices.Clone(d.Columns),
		QP:       d.QP,
		Pushed:   d.Pushed,
		Truncate: d.Truncate,
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

func (d *Distinct) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, reuseExisting, addToGroupBy bool) (ops.Operator, int, error) {
	newSrc, offset, err := d.Source.AddColumn(ctx, expr, reuseExisting, addToGroupBy)
	if err != nil {
		return nil, 0, err
	}
	d.Source = newSrc
	return d, offset, nil
}

func (d *Distinct) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return d.Source.GetColumns()
}

func (d *Distinct) ShortDescription() string {
	return ""
}

func (d *Distinct) GetOrdering() ([]ops.OrderBy, error) {
	return d.Source.GetOrdering()
}

func (d *Distinct) setTruncateColumnCount(offset int) {
	d.Truncate = offset
}
