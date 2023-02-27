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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Union struct {
	Sources  []ops.Operator
	Distinct bool

	// TODO this should be removed. For now it's used to fail queries
	Ordering sqlparser.OrderBy

	noColumns
}

var _ ops.PhysicalOperator = (*Union)(nil)

// IPhysical implements the PhysicalOperator interface
func (u *Union) IPhysical() {}

// Clone implements the Operator interface
func (u *Union) Clone(inputs []ops.Operator) ops.Operator {
	newOp := *u
	newOp.Sources = inputs
	return &newOp
}

// Inputs implements the Operator interface
func (u *Union) Inputs() []ops.Operator {
	return u.Sources
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
			return sqlparser.GetFirstSelect(op.Select), nil
		case *Route:
			src = op.Source
		default:
			return nil, vterrors.VT13001("expected all sources of the UNION to be horizons")
		}
	}
}

func (u *Union) Compact(*plancontext.PlanningContext) (ops.Operator, rewrite.TreeIdentity, error) {
	var newSources []ops.Operator
	anythingChanged := false
	for _, source := range u.Sources {
		var other *Union
		horizon, ok := source.(*Horizon)
		if ok {
			union, ok := horizon.Source.(*Union)
			if ok {
				other = union
			}
		}
		if other == nil {
			newSources = append(newSources, source)
			continue
		}
		anythingChanged = true
		switch {
		case len(other.Ordering) == 0 && !other.Distinct:
			fallthrough
		case u.Distinct:
			// if the current UNION is a DISTINCT, we can safely ignore everything from children UNIONs, except LIMIT
			newSources = append(newSources, other.Sources...)

		default:
			newSources = append(newSources, other)
		}
	}
	if anythingChanged {
		u.Sources = newSources
	}
	identity := rewrite.SameTree
	if anythingChanged {
		identity = rewrite.NewTree
	}

	return u, identity, nil
}

func (u *Union) NoLHSTableSet() {}
