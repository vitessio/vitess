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
	"fmt"
	"slices"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Union struct {
	Sources []Operator

	// These are the select expressions coming from each source
	Selects  [][]sqlparser.SelectExpr
	distinct bool

	unionColumns              []sqlparser.SelectExpr
	unionColumnsAsAlisedExprs []*sqlparser.AliasedExpr
}

func newUnion(srcs []Operator, sourceSelects [][]sqlparser.SelectExpr, columns []sqlparser.SelectExpr, distinct bool) *Union {
	if columns == nil {
		panic("rt")
	}
	return &Union{
		Sources:      srcs,
		Selects:      sourceSelects,
		distinct:     distinct,
		unionColumns: columns,
	}
}

// Clone implements the Operator interface
func (u *Union) Clone(inputs []Operator) Operator {
	newOp := *u
	newOp.Sources = inputs
	newOp.Selects = slices.Clone(u.Selects)
	return &newOp
}

func (u *Union) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

// Inputs implements the Operator interface
func (u *Union) Inputs() []Operator {
	return u.Sources
}

// SetInputs implements the Operator interface
func (u *Union) SetInputs(ops []Operator) {
	u.Sources = ops
}

// AddPredicate adds a predicate a UNION by pushing the predicate to all sources of the UNION.
/* this is done by offset and expression rewriting. Say we have a query like so:
select * (
 	select foo as col, bar from tbl1
	union
	select id, baz from tbl2
) as X where X.col = 42

We want to push down the `X.col = 42` as far down the operator tree as possible. We want
to end up with an operator tree that looks something like this:

select * (
 	select foo as col, bar from tbl1 where foo = 42
	union
	select id, baz from tbl2 where id = 42
) as X

Notice how `X.col = 42` has been translated to `foo = 42` and `id = 42` on respective WHERE clause.
The first SELECT of the union dictates the column names, and the second is whatever expression
can be found on the same offset. The names of the RHS are discarded.
*/
func (u *Union) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	offsets := make(map[string]int)
	sel := u.GetSelectFor(0)
	for i, selectExpr := range sel.GetColumns() {
		ae, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			panic(vterrors.VT12001("pushing predicates on UNION where the first SELECT contains * or NEXT"))
		}
		offsets[ae.ColumnName()] = i
	}

	needsFilter, exprPerSource := u.predicatePerSource(expr, offsets)
	if needsFilter {
		return newFilter(u, expr)
	}

	for i, src := range u.Sources {
		u.Sources[i] = src.AddPredicate(ctx, exprPerSource[i])
	}

	return u
}

func (u *Union) predicatePerSource(expr sqlparser.Expr, offsets map[string]int) (bool, []sqlparser.Expr) {
	needsFilter := false
	exprPerSource := make([]sqlparser.Expr, len(u.Sources))
	for i := range u.Sources {
		predicate := sqlparser.CopyOnRewrite(expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
			col, ok := cursor.Node().(*sqlparser.ColName)
			if !ok {
				return
			}

			idx, ok := offsets[col.Name.Lowered()]
			if !ok {
				needsFilter = true
				cursor.StopTreeWalk()
				return
			}

			sel := u.GetSelectFor(i)
			ae, ok := sel.GetColumns()[idx].(*sqlparser.AliasedExpr)
			if !ok {
				panic(vterrors.VT09015())
			}
			cursor.Replace(ae.Expr)
		}, nil).(sqlparser.Expr)

		exprPerSource[i] = predicate
	}
	return needsFilter, exprPerSource
}

func (u *Union) GetSelectFor(source int) *sqlparser.Select {
	src := u.Sources[source]
	for {
		switch op := src.(type) {
		case *Horizon:
			return getFirstSelect(op.Query)
		case *Route:
			src = op.Source
		default:
			panic(vterrors.VT13001("expected all sources of the UNION to be horizons"))
		}
	}
}

func (u *Union) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	outputOffset, err := u.addWeightStringToOffset(ctx, offset)
	if err != nil {
		panic(err)
	}
	return outputOffset
}

func (u *Union) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) int {
	if reuse {
		offset := u.FindCol(ctx, expr.Expr, false)
		if offset >= 0 {
			return offset
		}
	}

	switch e := expr.Expr.(type) {
	case *sqlparser.ColName:
		cols := u.GetColumns(ctx)
		// here we deal with pure column access on top of the union
		offset := slices.IndexFunc(cols, func(column *sqlparser.AliasedExpr) bool {
			if column.ColumnName() == expr.ColumnName() {
				return true
			}
			return e.Name.EqualString(column.ColumnName())
		})
		if offset == -1 {
			panic(vterrors.VT13001(fmt.Sprintf("could not find the column '%s' on the UNION", sqlparser.String(e))))
		}
		return offset
	case *sqlparser.WeightStringFuncExpr:
		wsArg := e.Expr
		cols := u.GetColumns(ctx)
		argIdx := slices.IndexFunc(cols, func(expr *sqlparser.AliasedExpr) bool {
			return ctx.SemTable.EqualsExprWithDeps(wsArg, expr.Expr)
		})

		if argIdx == -1 {
			panic(vterrors.VT13001("could not find the argument to the weight_string function: " + sqlparser.String(wsArg)))
		}

		offset, err := u.addWeightStringToOffset(ctx, argIdx)
		if err != nil {
			panic(err)
		}
		return offset
	case *sqlparser.Literal, *sqlparser.Argument:
		return u.addConstantToUnion(ctx, expr)
	default:
		return u.pushColumnToSources(ctx, expr)
	}
}

func (u *Union) pushColumnToSources(ctx *plancontext.PlanningContext, ae *sqlparser.AliasedExpr) int {
	// pushColumnToSources adds a new column (defined by ae) to each source in the UNION
	// while ensuring that the column’s offset is kept consistent across all sources.
	colsToReplace := make(map[sqlparser.ASTPath]int)
	cols := u.Sources[0].GetColumns(ctx)
	expr := sqlparser.Clone(ae.Expr)

	// Step 1: Identify column references in the first source's expression and record their offsets.
	exprForFirstSource := sqlparser.RewriteWithPath(expr, nil, func(cursor *sqlparser.Cursor) bool {
		col, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return true
		}
		// here we deal with pure column access on top of the union
		offset := slices.IndexFunc(cols, func(expr *sqlparser.AliasedExpr) bool {
			return col.Name.EqualString(expr.ColumnName())
		})
		if offset == -1 {
			panic(vterrors.VT13001(fmt.Sprintf("could not find the column '%s' on the UNION", sqlparser.String(col))))
		}
		// we need to replace this column on all sources
		colsToReplace[cursor.Path()] = offset

		expr := cols[offset].Expr
		cursor.Replace(expr)

		return true
	})

	// Step 2: Add the fully substituted expression as a column in the first source's column list.
	offset := u.Sources[0].AddColumn(ctx, false, false, aeWrap(exprForFirstSource.(sqlparser.Expr)))

	// Step 3: For each subsequent source, use the same offsets to replace column references,
	// ensuring consistent expressions across all sources in the UNION.
	for _, src := range u.Sources[1:] {
		cols := src.GetColumns(ctx)
		// we don't want to use the already rewritten expression, as it might have been rewritten to a different column
		expr := sqlparser.Clone(ae.Expr)
		rewritten := sqlparser.RewriteWithPath(expr, nil, func(cursor *sqlparser.Cursor) bool {
			_, ok := cursor.Node().(*sqlparser.ColName)
			if !ok {
				return true
			}
			offset := colsToReplace[cursor.Path()]
			expr := cols[offset].Expr
			cursor.Replace(expr)

			return true
		}).(sqlparser.Expr)
		// Add the rewritten column expression to this source. It must match the same offset as the first source.
		thisOffset := src.AddColumn(ctx, false, false, aeWrap(rewritten))
		if thisOffset != offset {
			tree := ToTree(u)
			panic(vterrors.VT13001(fmt.Sprintf("argument offsets did not line up for UNION. Pushing %s - want %d got %d\n%s", sqlparser.String(ae), offset, thisOffset, tree)))
		}
	}

	// Return the offset for the newly added/rewritten column across all sources.
	return offset
}

func (u *Union) addConstantToUnion(ctx *plancontext.PlanningContext, aexpr *sqlparser.AliasedExpr) (outputOffset int) {
	for i, src := range u.Sources {
		thisOffset := src.AddColumn(ctx, true, false, aexpr)
		// all offsets for the newly added ws need to line up
		if i == 0 {
			outputOffset = thisOffset
		} else {
			if thisOffset != outputOffset {
				tree := ToTree(u)
				panic(vterrors.VT12001(fmt.Sprintf("argument offsets did not line up for UNION. Pushing %s - want %d got %d\n%s", sqlparser.String(aexpr), outputOffset, thisOffset, tree)))
			}
		}
	}
	return
}

func (u *Union) addWeightStringToOffset(ctx *plancontext.PlanningContext, argIdx int) (outputOffset int, err error) {
	for i, src := range u.Sources {
		thisOffset := src.AddWSColumn(ctx, argIdx, false)

		// all offsets for the newly added ws need to line up
		if i == 0 {
			outputOffset = thisOffset
		} else {
			if thisOffset != outputOffset {
				return 0, vterrors.VT13001("weight_string offsets did not line up for UNION")
			}
		}
	}
	return
}

func (u *Union) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	columns := u.GetColumns(ctx)

	for idx, col := range columns {
		if ctx.SemTable.EqualsExprWithDeps(expr, col.Expr) {
			return idx
		}
	}

	return -1
}

func (u *Union) GetColumns(ctx *plancontext.PlanningContext) (result []*sqlparser.AliasedExpr) {
	if u.unionColumnsAsAlisedExprs == nil {
		allOk := true
		u.unionColumnsAsAlisedExprs = slice.Map(u.unionColumns, func(from sqlparser.SelectExpr) *sqlparser.AliasedExpr {
			expr, ok := from.(*sqlparser.AliasedExpr)
			allOk = allOk && ok
			return expr
		})
		if !allOk {
			panic(vterrors.VT09015())
		}
	}

	// if any of the inputs has more columns that we expect, we want to show on top of UNION, so the results can
	// be truncated to the expected result columns and nothing else
	for _, src := range u.Sources {
		columns := src.GetColumns(ctx)
		for len(columns) > len(u.unionColumnsAsAlisedExprs) {
			u.unionColumnsAsAlisedExprs = append(u.unionColumnsAsAlisedExprs, aeWrap(sqlparser.NewIntLiteral("0")))
		}
	}

	return u.unionColumnsAsAlisedExprs
}

func (u *Union) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	// if any of the inputs has more columns that we expect, we want to show on top of UNION, so the results can
	// be truncated to the expected result columns and nothing else
	for _, src := range u.Sources {
		columns := src.GetSelectExprs(ctx)
		for len(columns) > len(u.unionColumns) {
			u.unionColumns = append(u.unionColumns, aeWrap(sqlparser.NewIntLiteral("0")))
		}
	}

	return u.unionColumns
}

func (u *Union) NoLHSTableSet() {}

func (u *Union) ShortDescription() string {
	if u.distinct {
		return "DISTINCT"
	}
	return ""
}
