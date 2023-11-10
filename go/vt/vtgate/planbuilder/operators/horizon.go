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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
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
	Select sqlparser.SelectStatement
	QP     *QueryProjection
}

func (h *Horizon) AddColumn(*plancontext.PlanningContext, *sqlparser.AliasedExpr, bool, bool) (ops.Operator, int, error) {
	return nil, 0, vterrors.VT13001("the Horizon operator cannot accept new columns")
}

func (h *Horizon) GetColumns() (exprs []*sqlparser.AliasedExpr, err error) {
	for _, expr := range sqlparser.GetFirstSelect(h.Select).SelectExprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, errHorizonNotPlanned()
		}
		exprs = append(exprs, ae)
	}
	return
}

var _ ops.Operator = (*Horizon)(nil)

func (h *Horizon) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := h.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	h.Source = newSrc
	return h, nil
}

func (h *Horizon) Clone(inputs []ops.Operator) ops.Operator {
	return &Horizon{
		Source: inputs[0],
		Select: h.Select,
	}
}

func (h *Horizon) Inputs() []ops.Operator {
	return []ops.Operator{h.Source}
}

// SetInputs implements the Operator interface
func (h *Horizon) SetInputs(ops []ops.Operator) {
	h.Source = ops[0]
}

<<<<<<< HEAD
=======
func (h *Horizon) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) ops.Operator {
	if _, isUNion := h.Source.(*Union); isUNion {
		// If we have a derived table on top of a UNION, we can let the UNION do the expression rewriting
		h.Source = h.Source.AddPredicate(ctx, expr)
		return h
	}
	tableInfo, err := ctx.SemTable.TableInfoForExpr(expr)
	if err != nil {
		if errors.Is(err, semantics.ErrNotSingleTable) {
			return &Filter{
				Source:     h,
				Predicates: []sqlparser.Expr{expr},
			}
		}
		panic(err)
	}

	newExpr := semantics.RewriteDerivedTableExpression(expr, tableInfo)
	if sqlparser.ContainsAggregation(newExpr) {
		return &Filter{Source: h, Predicates: []sqlparser.Expr{expr}}
	}
	h.Source = h.Source.AddPredicate(ctx, newExpr)
	return h
}

func (h *Horizon) AddColumn(ctx *plancontext.PlanningContext, reuse bool, _ bool, expr *sqlparser.AliasedExpr) int {
	if !reuse {
		panic(errNoNewColumns)
	}
	col, ok := expr.Expr.(*sqlparser.ColName)
	if !ok {
		panic(vterrors.VT13001("cannot push non-ColName expression to horizon"))
	}
	offset := h.FindCol(ctx, col, false)
	if offset < 0 {
		panic(errNoNewColumns)
	}
	return offset
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

func (h *Horizon) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	if underRoute && h.IsDerived() {
		// We don't want to use columns on this operator if it's a derived table under a route.
		// In this case, we need to add a Projection on top of this operator to make the column available
		return -1
	}

	for idx, se := range sqlparser.GetFirstSelect(h.Query).SelectExprs {
		ae, ok := se.(*sqlparser.AliasedExpr)
		if !ok {
			panic(vterrors.VT09015())
		}
		if ctx.SemTable.EqualsExprWithDeps(ae.Expr, expr) {
			return idx
		}
	}

	return -1
}

func (h *Horizon) GetColumns(ctx *plancontext.PlanningContext) (exprs []*sqlparser.AliasedExpr) {
	for _, expr := range ctx.SemTable.SelectExprs(h.Query) {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			panic(vterrors.VT09015())
		}
		exprs = append(exprs, ae)
	}

	return exprs
}

func (h *Horizon) GetSelectExprs(*plancontext.PlanningContext) sqlparser.SelectExprs {
	return sqlparser.GetFirstSelect(h.Query).SelectExprs
}

func (h *Horizon) GetOrdering(ctx *plancontext.PlanningContext) []ops.OrderBy {
	if h.QP == nil {
		_, err := h.getQP(ctx)
		if err != nil {
			panic(err)
		}
	}
	return h.QP.OrderExprs
}

// TODO: REMOVE
>>>>>>> 817c24e942 (planbuilder bugfix: expose columns through derived tables (#14501))
func (h *Horizon) selectStatement() sqlparser.SelectStatement {
	return h.Select
}

func (h *Horizon) src() ops.Operator {
	return h.Source
}

func (h *Horizon) GetOrdering() ([]ops.OrderBy, error) {
	if h.QP == nil {
		return nil, vterrors.VT13001("QP should already be here")
	}
	return h.QP.OrderExprs, nil
}

func (h *Horizon) getQP(ctx *plancontext.PlanningContext) (*QueryProjection, error) {
	if h.QP != nil {
		return h.QP, nil
	}
	qp, err := CreateQPFromSelect(ctx, h.Select.(*sqlparser.Select))
	if err != nil {
		return nil, err
	}
	h.QP = qp
	return h.QP, nil
}

func (h *Horizon) setQP(qp *QueryProjection) {
	h.QP = qp
}

func (h *Horizon) Description() ops.OpDescription {
	return ops.OpDescription{
		OperatorType: "Horizon",
	}
}

func (h *Horizon) ShortDescription() string {
	return ""
}
