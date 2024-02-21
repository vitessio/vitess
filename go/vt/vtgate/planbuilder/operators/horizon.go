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
	"errors"
	"slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
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
	Source Operator

	// If this is a derived table, the two following fields will contain the tableID and name of it
	TableId       *semantics.TableSet
	Alias         string
	ColumnAliases sqlparser.Columns // derived tables can have their column aliases specified outside the subquery

	// QP contains the QueryProjection for this op
	QP *QueryProjection

	Query sqlparser.SelectStatement

	// Columns needed to feed other plans
	Columns       []*sqlparser.ColName
	ColumnsOffset []int
}

func newHorizon(src Operator, query sqlparser.SelectStatement) *Horizon {
	return &Horizon{Source: src, Query: query}
}

// Clone implements the Operator interface
func (h *Horizon) Clone(inputs []Operator) Operator {
	klone := *h
	klone.Source = inputs[0]
	klone.ColumnAliases = sqlparser.CloneColumns(h.ColumnAliases)
	klone.Columns = slices.Clone(h.Columns)
	klone.ColumnsOffset = slices.Clone(h.ColumnsOffset)
	klone.QP = h.QP
	return &klone
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
func (h *Horizon) Inputs() []Operator {
	return []Operator{h.Source}
}

// SetInputs implements the Operator interface
func (h *Horizon) SetInputs(ops []Operator) {
	h.Source = ops[0]
}

func (h *Horizon) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	if _, isUNion := h.Source.(*Union); isUNion {
		// If we have a derived table on top of a UNION, we can let the UNION do the expression rewriting
		h.Source = h.Source.AddPredicate(ctx, expr)
		return h
	}
	tableInfo, err := ctx.SemTable.TableInfoForExpr(expr)
	if err != nil {
		if errors.Is(err, semantics.ErrNotSingleTable) {
			return newFilter(h, expr)
		}
		panic(err)
	}

	newExpr := semantics.RewriteDerivedTableExpression(expr, tableInfo)
	if sqlparser.ContainsAggregation(newExpr) {
		return newFilter(h, expr)
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

func (h *Horizon) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	if h.QP == nil {
		h.getQP(ctx)
	}
	return h.QP.OrderExprs
}

// TODO: REMOVE
func (h *Horizon) selectStatement() sqlparser.SelectStatement {
	return h.Query
}

func (h *Horizon) src() Operator {
	return h.Source
}

func (h *Horizon) getQP(ctx *plancontext.PlanningContext) *QueryProjection {
	if h.QP == nil {
		h.QP = CreateQPFromSelectStatement(ctx, h.Query)
	}
	return h.QP
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
