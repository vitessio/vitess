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

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Union struct {
	Sources  []ops.Operator
	Selects  []sqlparser.SelectExprs
	Distinct bool

	// TODO this should be removed. For now it's used to fail queries
	Ordering []ops.OrderBy

	offsetPlanned bool
}

// Clone implements the Operator interface
func (u *Union) Clone(inputs []ops.Operator) ops.Operator {
	newOp := *u
	newOp.Sources = inputs
	newOp.Selects = slices.Clone(u.Selects)
	return &newOp
}

func (u *Union) GetOrdering() ([]ops.OrderBy, error) {
	return u.Ordering, nil
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
		if !ae.As.IsEmpty() {
			offsets[ae.As.String()] = i
			continue
		}
		col, ok := ae.Expr.(*sqlparser.ColName)
		if ok {
			offsets[col.Name.Lowered()] = i
		}
	}

	for i := range u.Sources {
		var err error
		predicate := sqlparser.CopyOnRewrite(expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
			col, ok := cursor.Node().(*sqlparser.ColName)
			if !ok {
				return
			}

			idx, ok := offsets[col.Name.Lowered()]
			if !ok {
				err = vterrors.VT13001("cannot push predicates on concatenate, missing columns from the UNION")
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
				err = vterrors.VT12001("pushing non-aliased expression predicates on concatenate")
				cursor.StopTreeWalk()
				return
			}
			cursor.Replace(ae.Expr)
		}, nil).(sqlparser.Expr)
		if err != nil {
			return nil, err
		}
		u.Sources[i], err = u.Sources[i].AddPredicate(ctx, predicate)
		if err != nil {
			return nil, err
		}
	}

	return u, nil
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

func (u *Union) Compact(*plancontext.PlanningContext) (ops.Operator, *rewrite.ApplyResult, error) {
	var newSources []ops.Operator
	var newSelects []sqlparser.SelectExprs
	merged := false

	for idx, source := range u.Sources {
		other, ok := source.(*Union)

		if ok && (u.Distinct || len(other.Ordering) == 0 && !other.Distinct) {
			newSources = append(newSources, other.Sources...)
			newSelects = append(newSelects, other.Selects...)
			merged = true
			continue
		}

		newSources = append(newSources, source)
		newSelects = append(newSelects, u.Selects[idx])
	}

	if !merged {
		return u, rewrite.SameTree, nil
	}

	u.Sources = newSources
	u.Selects = newSelects
	return u, rewrite.NewTree("merged UNIONs", u), nil
}

func (u *Union) AddColumn(ctx *plancontext.PlanningContext, ae *sqlparser.AliasedExpr, reuseExisting, addToGroupBy bool) (ops.Operator, int, error) {
	err := u.planOffsets(ctx)
	if err != nil {
		return nil, 0, err
	}
	cols, err := u.GetColumns()
	if err != nil {
		return nil, 0, err
	}
	for idx, col := range cols {
		if ctx.SemTable.EqualsExprWithDeps(ae.Expr, col.Expr) {
			return u, idx, nil
		}
	}

	ws, isWeightString := ae.Expr.(*sqlparser.WeightStringFuncExpr)
	if !isWeightString {
		return nil, 0, vterrors.VT13001(fmt.Sprintf("only weight_string function is expected - got %s", sqlparser.String(ae)))
	}

	newSrc, offset, err := u.Sources[0].AddColumn(ctx, ae, true, false)
	if err != nil {
		return nil, 0, err
	}
	u.Sources[0] = newSrc

	// we are adding a weight_string function. let's find the offset of the argument to it
	argIdx := slices.IndexFunc(u.Selects[0], func(se sqlparser.SelectExpr) bool {
		ae, ok := se.(*sqlparser.AliasedExpr)
		if !ok {
			err = vterrors.VT12001("can't handle star expressions inside UNION")
			return false
		}
		return ctx.SemTable.EqualsExprWithDeps(ae.Expr, ws.Expr)
	})
	if argIdx == -1 {
		return nil, 0, vterrors.VT13001(fmt.Sprintf("could not find the actual expression to add weight_string function: %s", sqlparser.String(ws.Expr)))
	}

	for i := 1; i < len(u.Sources); i++ {
		exprs := u.Selects[i]
		selectExpr := exprs[argIdx]
		ae, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, 0, vterrors.VT09015()
		}
		newSrc, this, err := u.Sources[i].AddColumn(ctx, aeWrap(weightStringFor(ae.Expr)), true, false)
		if err != nil {
			return nil, 0, err
		}
		if this != offset {
			panic("uh oh")
		}
		u.Sources[i] = newSrc
	}
	return u, offset, nil
}

func (u *Union) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return u.Sources[0].GetColumns()
}

func (u *Union) GetSelectExprs() (sqlparser.SelectExprs, error) {
	return u.Sources[0].GetSelectExprs()
}

func (u *Union) NoLHSTableSet() {}

func (u *Union) ShortDescription() string {
	return ""
}

func (u *Union) planOffsets(ctx *plancontext.PlanningContext) error {
	if u.offsetPlanned {
		return nil
	}
	u.offsetPlanned = true

	for idx, source := range u.Sources {
		for eIdx, expr := range u.Selects[idx] {
			ae, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				return vterrors.VT09015()
			}
			newOp, offset, err := source.AddColumn(ctx, ae, false, false)
			if err != nil {
				return err
			}
			if offset != eIdx {
				return vterrors.VT13001(fmt.Sprintf("index mismatch while pushing the column for '%s'", sqlparser.String(ae)))
			}
			u.Sources[idx] = newOp
		}
	}
	return nil
}
