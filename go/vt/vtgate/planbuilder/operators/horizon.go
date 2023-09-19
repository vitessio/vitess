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
	"slices"

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
	Source ops.Operator

	// If this is a derived table, the two following fields will contain the tableID and name of it
	TableId *semantics.TableSet
	Alias   string

	// QP contains the QueryProjection for this op
	QP *QueryProjection

	Query         sqlparser.SelectStatement
	ColumnAliases sqlparser.Columns

	// Columns needed to feed other plans
	Columns       []*sqlparser.ColName
	ColumnsOffset []int
}

// Clone implements the Operator interface
func (h *Horizon) Clone(inputs []ops.Operator) ops.Operator {
	return &Horizon{
		Source:        inputs[0],
		Query:         h.Query,
		Alias:         h.Alias,
		ColumnAliases: sqlparser.CloneColumns(h.ColumnAliases),
		Columns:       slices.Clone(h.Columns),
		ColumnsOffset: slices.Clone(h.ColumnsOffset),
		TableId:       h.TableId,
		QP:            h.QP,
	}
}

// IsMergeable is not a great name for this function. Suggestions for a better one are welcome!
// This function will return false if the derived table inside it has to run on the vtgate side, and so can't be merged with subqueries
// This logic can also be used to check if this is a derived table that can be had on the left hand side of a vtgate join.
// Since vtgate joins are always nested loop joins, we can't execute them on the RHS
// if they do some things, like LIMIT or GROUP BY on wrong columns
func (h *Horizon) IsMergeable(ctx *plancontext.PlanningContext) bool {
	return isMergeable(ctx, h.Query, h)
}

// Inputs implements the Operator interface
func (h *Horizon) Inputs() []ops.Operator {
	return []ops.Operator{h.Source}
}

// SetInputs implements the Operator interface
func (h *Horizon) SetInputs(ops []ops.Operator) {
	h.Source = ops[0]
}

func (h *Horizon) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	if _, isUNion := h.Source.(*Union); isUNion {
		// If we have a derived table on top of a UNION, we can let the UNION do the expression rewriting
		var err error
		h.Source, err = h.Source.AddPredicate(ctx, expr)
		return h, err
	}
	tableInfo, err := ctx.SemTable.TableInfoForExpr(expr)
	if err != nil {
		if err == semantics.ErrNotSingleTable {
			return &Filter{
				Source:     h,
				Predicates: []sqlparser.Expr{expr},
			}, nil
		}
		return nil, err
	}

	newExpr := semantics.RewriteDerivedTableExpression(expr, tableInfo)
	if sqlparser.ContainsAggregation(newExpr) {
		return &Filter{Source: h, Predicates: []sqlparser.Expr{expr}}, nil
	}
	h.Source, err = h.Source.AddPredicate(ctx, newExpr)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (h *Horizon) AddColumns(ctx *plancontext.PlanningContext, reuse bool, _ []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	if !reuse {
		return nil, errNoNewColumns
	}
	offsets := make([]int, len(exprs))
	for i, expr := range exprs {
		col, ok := expr.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, vterrors.VT13001("cannot push non-ColName expression to horizon")
		}
		offset, err := h.FindCol(ctx, col, false)
		if err != nil {
			return nil, err
		}

		if offset < 0 {
			return nil, errNoNewColumns
		}
		offsets[i] = offset
	}

	return offsets, nil
}

var errNoNewColumns = vterrors.VT13001("can't add new columns to Horizon")

// canReuseColumn is generic, so it can be used with slices of different types.
// We don't care about the actual type, as long as we know it's a sqlparser.Expr
func canReuseColumn[T any](
	ctx *plancontext.PlanningContext,
	columns []T,
	col sqlparser.Expr,
	f func(T) sqlparser.Expr,
) (offset int, found bool) {
	for offset, column := range columns {
		if ctx.SemTable.EqualsExprWithDeps(col, f(column)) {
			return offset, true
		}
	}

	return
}

func (h *Horizon) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, _ bool) (int, error) {
	for idx, se := range sqlparser.GetFirstSelect(h.Query).SelectExprs {
		ae, ok := se.(*sqlparser.AliasedExpr)
		if !ok {
			return 0, vterrors.VT09015()
		}
		if ctx.SemTable.EqualsExprWithDeps(ae.Expr, expr) {
			return idx, nil
		}
	}

	return -1, nil
}

func (h *Horizon) GetColumns(ctx *plancontext.PlanningContext) (exprs []*sqlparser.AliasedExpr, err error) {
	for _, expr := range ctx.SemTable.SelectExprs(h.Query) {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, vterrors.VT09015()
		}
		exprs = append(exprs, ae)
	}

	return exprs, nil
}

func (h *Horizon) GetSelectExprs(*plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return sqlparser.GetFirstSelect(h.Query).SelectExprs, nil
}

func (h *Horizon) GetOrdering() ([]ops.OrderBy, error) {
	if h.QP == nil {
		return nil, vterrors.VT13001("QP should already be here")
	}
	return h.QP.OrderExprs, nil
}

// TODO: REMOVE
func (h *Horizon) selectStatement() sqlparser.SelectStatement {
	return h.Query
}

func (h *Horizon) src() ops.Operator {
	return h.Source
}

func (h *Horizon) getQP(ctx *plancontext.PlanningContext) (*QueryProjection, error) {
	if h.QP != nil {
		return h.QP, nil
	}
	qp, err := CreateQPFromSelectStatement(ctx, h.Query)
	if err != nil {
		return nil, err
	}
	h.QP = qp
	return h.QP, nil
}

func (h *Horizon) ShortDescription() string {
	return h.Alias
}

func (h *Horizon) introducesTableID() semantics.TableSet {
	if h.TableId == nil {
		return semantics.EmptyTableSet()
	}

	return *h.TableId
}

func (h *Horizon) IsDerived() bool {
	return h.TableId != nil
}
