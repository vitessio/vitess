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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Union struct {
	Sources []ops.Operator

	// These are the select expressions coming from each source
	Selects  []sqlparser.SelectExprs
	distinct bool

	unionColumns              sqlparser.SelectExprs
	unionColumnsAsAlisedExprs []*sqlparser.AliasedExpr
}

func newUnion(srcs []ops.Operator, sourceSelects []sqlparser.SelectExprs, columns sqlparser.SelectExprs, distinct bool) *Union {
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
func (u *Union) Clone(inputs []ops.Operator) ops.Operator {
	newOp := *u
	newOp.Sources = inputs
	newOp.Selects = slices.Clone(u.Selects)
	return &newOp
}

func (u *Union) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
}

// Inputs implements the Operator interface
func (u *Union) Inputs() []ops.Operator {
	return u.Sources
}

// SetInputs implements the Operator interface
func (u *Union) SetInputs(ops []ops.Operator) {
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
func (u *Union) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	offsets := make(map[string]int)
	sel, err := u.GetSelectFor(0)
	if err != nil {
		return nil, err
	}
	for i, selectExpr := range sel.SelectExprs {
		ae, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, vterrors.VT12001("pushing predicates on UNION where the first SELECT contains * or NEXT")
		}
		offsets[ae.ColumnName()] = i
	}

	needsFilter, exprPerSource, err := u.predicatePerSource(expr, offsets)
	if err != nil {
		return nil, err
	}
	if needsFilter {
		return &Filter{
			Source:     u,
			Predicates: []sqlparser.Expr{expr},
		}, nil
	}

	for i, src := range u.Sources {
		u.Sources[i], err = src.AddPredicate(ctx, exprPerSource[i])
		if err != nil {
			return nil, err
		}
	}

	return u, nil
}

func (u *Union) predicatePerSource(expr sqlparser.Expr, offsets map[string]int) (bool, []sqlparser.Expr, error) {
	needsFilter := false
	exprPerSource := make([]sqlparser.Expr, len(u.Sources))
	for i := range u.Sources {
		var err error
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

			var sel *sqlparser.Select
			sel, err = u.GetSelectFor(i)
			if err != nil {
				cursor.StopTreeWalk()
				return
			}

			ae, ok := sel.SelectExprs[idx].(*sqlparser.AliasedExpr)
			if !ok {
				err = vterrors.VT09015()
				cursor.StopTreeWalk()
				return
			}
			cursor.Replace(ae.Expr)
		}, nil).(sqlparser.Expr)
		if err != nil || needsFilter {
			return needsFilter, nil, err
		}
		exprPerSource[i] = predicate
	}
	return needsFilter, exprPerSource, nil
}

func (u *Union) GetSelectFor(source int) (*sqlparser.Select, error) {
	src := u.Sources[source]
	for {
		switch op := src.(type) {
		case *Horizon:
			return sqlparser.GetFirstSelect(op.Query), nil
		case *Route:
			src = op.Source
		default:
			return nil, vterrors.VT13001("expected all sources of the UNION to be horizons")
		}
	}
}

func (u *Union) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) (int, error) {
	if reuse {
		offset, err := u.FindCol(ctx, expr.Expr, false)
		if err != nil {
			return 0, err
		}

		if offset >= 0 {
			return offset, nil
		}
	}
	cols, err := u.GetColumns(ctx)
	if err != nil {
		return 0, err
	}

	switch e := expr.Expr.(type) {
	case *sqlparser.ColName:
		// here we deal with pure column access on top of the union
		offset := slices.IndexFunc(cols, func(expr *sqlparser.AliasedExpr) bool {
			return e.Name.EqualString(expr.ColumnName())
		})
		if offset == -1 {
			return 0, vterrors.VT13001(fmt.Sprintf("could not find the column '%s' on the UNION", sqlparser.String(e)))
		}
		return offset, nil
	case *sqlparser.WeightStringFuncExpr:
		wsArg := e.Expr
		argIdx := slices.IndexFunc(cols, func(expr *sqlparser.AliasedExpr) bool {
			return ctx.SemTable.EqualsExprWithDeps(wsArg, expr.Expr)
		})

		if argIdx == -1 {
			return 0, vterrors.VT13001(fmt.Sprintf("could not find the argument to the weight_string function: %s", sqlparser.String(wsArg)))
		}

		outputOffset, err := u.addWeightStringToOffset(ctx, argIdx, gb)
		if err != nil {
			return 0, err
		}

		return outputOffset, nil
	default:
		return 0, vterrors.VT13001(fmt.Sprintf("only weight_string function is expected - got %s", sqlparser.String(expr)))
	}
}

func (u *Union) addWeightStringToOffset(ctx *plancontext.PlanningContext, argIdx int, addToGroupBy bool) (outputOffset int, err error) {
	for i, src := range u.Sources {
		exprs := u.Selects[i]
		selectExpr := exprs[argIdx]
		ae, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return 0, vterrors.VT09015()
		}
		thisOffset, err := src.AddColumn(ctx, false, addToGroupBy, aeWrap(weightStringFor(ae.Expr)))
		if err != nil {
			return 0, err
		}

		// all offsets for the newly added ws need to line up
		if i == 0 {
			outputOffset = thisOffset
		} else {
			if thisOffset != outputOffset {
				return 0, vterrors.VT12001("weight_string offsets did not line up for UNION")
			}
		}
	}
	return
}

func (u *Union) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	columns, err := u.GetColumns(ctx)
	if err != nil {
		return 0, err
	}

	for idx, col := range columns {
		if ctx.SemTable.EqualsExprWithDeps(expr, col.Expr) {
			return idx, nil
		}
	}

	return -1, nil
}

func (u *Union) GetColumns(ctx *plancontext.PlanningContext) (result []*sqlparser.AliasedExpr, err error) {
	if u.unionColumnsAsAlisedExprs == nil {
		allOk := true
		u.unionColumnsAsAlisedExprs = slice.Map(u.unionColumns, func(from sqlparser.SelectExpr) *sqlparser.AliasedExpr {
			expr, ok := from.(*sqlparser.AliasedExpr)
			allOk = allOk && ok
			return expr
		})
		if !allOk {
			return nil, vterrors.VT09015()
		}
	}

	// if any of the inputs has more columns that we expect, we want to show on top of UNION, so the results can
	// be truncated to the expected result columns and nothing else
	for _, src := range u.Sources {
		columns, err := src.GetColumns(ctx)
		if err != nil {
			return nil, err
		}

		for len(columns) > len(u.unionColumnsAsAlisedExprs) {
			u.unionColumnsAsAlisedExprs = append(u.unionColumnsAsAlisedExprs, aeWrap(sqlparser.NewIntLiteral("0")))
		}
	}

	return u.unionColumnsAsAlisedExprs, nil
}

func (u *Union) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	// if any of the inputs has more columns that we expect, we want to show on top of UNION, so the results can
	// be truncated to the expected result columns and nothing else
	for _, src := range u.Sources {
		columns, err := src.GetSelectExprs(ctx)
		if err != nil {
			return nil, err
		}

		for len(columns) > len(u.unionColumns) {
			u.unionColumns = append(u.unionColumns, aeWrap(sqlparser.NewIntLiteral("0")))
		}
	}

	return u.unionColumns, nil
}

func (u *Union) NoLHSTableSet() {}

func (u *Union) ShortDescription() string {
	if u.distinct {
		return "DISTINCT"
	}
	return ""
}
