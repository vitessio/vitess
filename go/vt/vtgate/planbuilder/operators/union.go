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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Union struct {
	Sources  []Operator
	Distinct bool

	// TODO this should be removed. For now it's used to fail queries
	Ordering sqlparser.OrderBy

	noColumns
}

var _ PhysicalOperator = (*Union)(nil)

// IPhysical implements the PhysicalOperator interface
func (u *Union) IPhysical() {}

// clone implements the Operator interface
func (u *Union) clone(inputs []Operator) Operator {
	newOp := *u
	checkSize(inputs, len(u.Sources))
	newOp.Sources = inputs
	return &newOp
}

// inputs implements the Operator interface
func (u *Union) inputs() []Operator {
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
func (u *Union) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Operator, error) {
	offsets := make(map[string]int)
	sel, err := u.GetSelectFor(0)
	if err != nil {
		return nil, err
	}
	for i, selectExpr := range sel.SelectExprs {
		ae, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on UNION where the first SELECT contains star or next")
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
		predicate := sqlparser.CloneExpr(expr)
		var err error
		predicate = sqlparser.Rewrite(predicate, func(cursor *sqlparser.Cursor) bool {
			col, ok := cursor.Node().(*sqlparser.ColName)
			if !ok {
				return err == nil
			}

			idx, ok := offsets[col.Name.Lowered()]
			if !ok {
				err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
				return false
			}

			var sel *sqlparser.Select
			sel, err = u.GetSelectFor(i)
			if err != nil {
				return false
			}

			ae, ok := sel.SelectExprs[idx].(*sqlparser.AliasedExpr)
			if !ok {
				err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
				return false
			}
			cursor.Replace(ae.Expr)
			return false
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
	horizon, ok := u.Sources[source].(*Horizon)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected all sources of the UNION to be horizons")
	}
	return sqlparser.GetFirstSelect(horizon.Select), nil
}
