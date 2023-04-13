/*
Copyright 2022 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Horizon is an operator that allows us to postpone planning things like SELECT/GROUP BY/ORDER BY/LIMIT until later.
// It contains information about the planning we have to do after deciding how we will send the query to the tablets.
// If we are able to push down the Horizon under a route, we don't have to plan these things separately and can
// just copy over the AST constructs to the query being sent to a tablet.
// If we are not able to push it down, this operator needs to be split up into smaller
// Project/Aggregate/Sort/Limit operations, some which can be pushed down,
// and some that have to be evaluated at the vtgate level.
type Horizon struct {
	Source  ops.Operator
	Select  sqlparser.SelectStatement
	TableID *semantics.TableSet

	// ColumnAliases is alias assigned to the derived table
	// e.g. select 1 from (select a, b from t) as d (x, y)
	// x and y are the column alias available to outer query.
	ColumnAliases sqlparser.Columns

	// Columns are columns exposed to outside of this operator
	Columns []*sqlparser.ColName

	// ColumnsOffset stores the offset to retrieve the column from the input source.
	ColumnsOffset []int
}

var _ ops.Operator = (*Horizon)(nil)
var _ ops.PhysicalOperator = (*Horizon)(nil)

func (h *Horizon) IPhysical() {}

func (h *Horizon) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := h.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	h.Source = newSrc
	return h, nil
}

func (h *Horizon) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr) (ops.Operator, int, error) {
	return nil, 0, vterrors.VT13001("the Horizon operator cannot accept new columns")
}

func (h *Horizon) GetColumns() (exprs []sqlparser.Expr, err error) {
	for _, expr := range sqlparser.GetFirstSelect(h.Select).SelectExprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, errHorizonNotPlanned
		}
		exprs = append(exprs, sqlparser.NewColName(ae.ColumnName()))
	}
	return
}

func (h *Horizon) Clone(inputs []ops.Operator) ops.Operator {
	return &Horizon{
		Source:        inputs[0],
		Select:        h.Select,
		TableID:       h.TableID,
		ColumnAliases: h.ColumnAliases,
		Columns:       sqlparser.CloneSliceOfRefOfColName(h.Columns),
		ColumnsOffset: slices.Clone(h.ColumnsOffset),
	}
}

func (h *Horizon) Inputs() []ops.Operator {
	return []ops.Operator{h.Source}
}

// SetInputs implements the Operator interface
func (h *Horizon) SetInputs(ops []ops.Operator) {
	h.Source = ops[0]
}
