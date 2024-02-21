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
	Selects  []sqlparser.SelectExprs
	distinct bool

	unionColumns              sqlparser.SelectExprs
	unionColumnsAsAlisedExprs []*sqlparser.AliasedExpr
}

func newUnion(srcs []Operator, sourceSelects []sqlparser.SelectExprs, columns sqlparser.SelectExprs, distinct bool) *Union {
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
	for i, selectExpr := range sel.SelectExprs {
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
			ae, ok := sel.SelectExprs[idx].(*sqlparser.AliasedExpr)
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
			return sqlparser.GetFirstSelect(op.Query)
		case *Route:
			src = op.Source
		default:
			panic(vterrors.VT13001("expected all sources of the UNION to be horizons"))
		}
	}
}

func (u *Union) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) int {
	if reuse {
		offset := u.FindCol(ctx, expr.Expr, false)
		if offset >= 0 {
			return offset
		}
	}
	cols := u.GetColumns(ctx)

	switch e := expr.Expr.(type) {
	case *sqlparser.ColName:
		// here we deal with pure column access on top of the union
		offset := slices.IndexFunc(cols, func(expr *sqlparser.AliasedExpr) bool {
			return e.Name.EqualString(expr.ColumnName())
		})
		if offset == -1 {
			panic(vterrors.VT13001(fmt.Sprintf("could not find the column '%s' on the UNION", sqlparser.String(e))))
		}
		return offset
	case *sqlparser.WeightStringFuncExpr:
		wsArg := e.Expr
		argIdx := slices.IndexFunc(cols, func(expr *sqlparser.AliasedExpr) bool {
			return ctx.SemTable.EqualsExprWithDeps(wsArg, expr.Expr)
		})

		if argIdx == -1 {
			panic(vterrors.VT13001(fmt.Sprintf("could not find the argument to the weight_string function: %s", sqlparser.String(wsArg))))
		}

		return u.addWeightStringToOffset(ctx, argIdx, gb)
	default:
		panic(vterrors.VT13001(fmt.Sprintf("only weight_string function is expected - got %s", sqlparser.String(expr))))
	}
}

func (u *Union) addWeightStringToOffset(ctx *plancontext.PlanningContext, argIdx int, addToGroupBy bool) (outputOffset int) {
	for i, src := range u.Sources {
		exprs := u.Selects[i]
		selectExpr := exprs[argIdx]
		ae, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			panic(vterrors.VT09015())
		}
		thisOffset := src.AddColumn(ctx, false, addToGroupBy, aeWrap(weightStringFor(ae.Expr)))

		// all offsets for the newly added ws need to line up
		if i == 0 {
			outputOffset = thisOffset
		} else {
			if thisOffset != outputOffset {
				panic(vterrors.VT12001("weight_string offsets did not line up for UNION"))
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

func (u *Union) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
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
