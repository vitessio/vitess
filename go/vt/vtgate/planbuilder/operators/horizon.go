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
	"fmt"
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
	unaryOperator

	// If this is a derived table, the two following fields will contain the tableID and name of it
	TableId       *semantics.TableSet
	Alias         string
	ColumnAliases sqlparser.Columns // derived tables can have their column aliases specified outside the subquery

	// QP contains the QueryProjection for this op
	QP *QueryProjection

	Query sqlparser.TableStatement

	// Columns needed to feed other plans
	Columns       []*sqlparser.ColName
	ColumnsOffset []int

	Truncate bool
}

func newHorizon(src Operator, query sqlparser.TableStatement) *Horizon {
	return &Horizon{
		unaryOperator: newUnaryOp(src),
		Query:         query,
	}
}

// Clone implements the Operator interface
func (h *Horizon) Clone(inputs []Operator) Operator {
	klone := *h
	klone.Source = inputs[0]
	klone.ColumnAliases = sqlparser.Clone(h.ColumnAliases)
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

func (h *Horizon) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	if _, isUNion := h.Source.(*Union); isUNion {
		// If we have a derived table on top of a UNION, we can let the UNION do the expression rewriting
		h.Source = h.Source.AddPredicate(ctx, expr)
		return h
	}
	if h.IsDerived() {
		if _, isValues := h.Query.(*sqlparser.ValuesStatement); isValues {
			return newFilter(h, expr)
		}
	}
	tableInfo, err := ctx.SemTable.TableInfoForExpr(expr)
	if err != nil {
		if errors.Is(err, semantics.ErrNotSingleTable) {
			return newFilter(h, expr)
		}
		panic(err)
	}

	newExpr := semantics.RewriteDerivedTableExpression(expr, tableInfo)
	if ctx.ContainsAggr(newExpr) {
		return newFilter(h, expr)
	}
	h.Source = h.Source.AddPredicate(ctx, newExpr)
	return h
}

func (h *Horizon) AddColumn(ctx *plancontext.PlanningContext, reuse bool, _ bool, expr *sqlparser.AliasedExpr) int {
	if values, ok := h.Query.(*sqlparser.ValuesStatement); ok {
		if reuse {
			offset := h.FindCol(ctx, expr.Expr, false)
			if offset >= 0 {
				return offset
			}
		}
		return h.addColumnToValues(values, expr)
	}
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

func (h *Horizon) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	cols := h.GetColumns(ctx)
	if offset >= len(cols) {
		panic(errNoNewColumns)
	}

	switch stmt := h.Query.(type) {
	case *sqlparser.Select:
		wsOffset := len(cols)
		stmt.AddSelectExpr(aeWrap(weightStringFor(cols[offset].Expr)))
		return wsOffset
	case *sqlparser.ValuesStatement:
		return h.addColumnToValues(stmt, aeWrap(weightStringFor(cols[offset].Expr)))
	default:
		panic(errNoNewColumns)
	}
}

func (h *Horizon) addColumnToValues(values *sqlparser.ValuesStatement, expr *sqlparser.AliasedExpr) int {
	return h.addColumnToValuesUsingColumns(values, values.GetColumns(), expr)
}

func (h *Horizon) addColumnToValuesUsingColumns(values *sqlparser.ValuesStatement, columns []sqlparser.SelectExpr, expr *sqlparser.AliasedExpr) int {
	if len(values.Rows) == 0 {
		panic(errNoNewColumns)
	}

	newOffset := len(values.Rows[0])
	offsets := make(map[string]int, len(columns))
	for offset, column := range columns {
		ae, ok := column.(*sqlparser.AliasedExpr)
		if !ok {
			panic(vterrors.VT09015())
		}
		offsets[ae.ColumnName()] = offset
	}

	for i, row := range values.Rows {
		var missing *sqlparser.ColName
		newExpr := sqlparser.CopyOnRewrite(expr.Expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
			col, ok := cursor.Node().(*sqlparser.ColName)
			if !ok || col.Qualifier.NonEmpty() {
				return
			}
			offset, ok := offsets[col.Name.String()]
			if !ok {
				missing = col
				cursor.StopTreeWalk()
				return
			}
			cursor.Replace(row[offset])
		}, nil).(sqlparser.Expr)
		if missing != nil {
			panic(vterrors.VT13001(fmt.Sprintf("could not find the column '%s' on the VALUES statement", sqlparser.String(missing))))
		}
		values.Rows[i] = append(row, newExpr)
	}

	return newOffset
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

	for idx, se := range ctx.SemTable.SelectExprs(h.Query) {
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

func (h *Horizon) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return ctx.SemTable.SelectExprs(h.Query)
}

func (h *Horizon) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	if h.QP == nil {
		h.getQP(ctx)
	}
	return h.QP.OrderExprs
}

// TODO: REMOVE
func (h *Horizon) selectStatement() sqlparser.TableStatement {
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
	return fmt.Sprintf("Horizon (Alias: %s)", h.Alias)
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
