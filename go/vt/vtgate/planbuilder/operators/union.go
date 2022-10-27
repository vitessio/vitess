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
	Sources     []Operator
	SelectStmts []*sqlparser.Select
	Distinct    bool

	// TODO this should be removed. For now it's used to fail queries
	Ordering sqlparser.OrderBy
}

var _ PhysicalOperator = (*Union)(nil)

// IPhysical implements the PhysicalOperator interface
func (u *Union) IPhysical() {}

// Clone implements the Operator interface
func (u *Union) Clone(inputs []Operator) Operator {
	newOp := *u
	checkSize(inputs, len(u.Sources))
	newOp.Sources = inputs
	return &newOp
}

// Inputs implements the Operator interface
func (u *Union) Inputs() []Operator {
	return u.Sources
}

func (u *Union) addPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error {
	offsets := make(map[string]int)
	for i, selectExpr := range u.SelectStmts[0].SelectExprs {
		ae, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on UNION where the first SELECT contains star or next")
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

			ae, ok := u.SelectStmts[i].SelectExprs[idx].(*sqlparser.AliasedExpr)
			if !ok {
				err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
				return false
			}
			cursor.Replace(ae.Expr)
			return false
		}, nil).(sqlparser.Expr)
		if err != nil {
			return err
		}
		u.Sources[i], err = PushPredicate(ctx, predicate, u.Sources[i])
	}

	return nil
}
