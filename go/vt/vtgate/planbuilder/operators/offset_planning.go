/*
Copyright 2023 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// planOffsets will walk the tree top down, adding offset information to columns in the tree for use in further optimization,
func planOffsets(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	type offsettable interface {
		planOffsets(ctx *plancontext.PlanningContext) error
	}

	visitor := func(in ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, rewrite.ApplyResult, error) {
		var err error
		switch op := in.(type) {
		case *Horizon:
			return nil, false, vterrors.VT13001("should not see Horizons here")
		case *Derived:
			return nil, false, vterrors.VT13001("should not see Derived here")
		case offsettable:
			err = op.planOffsets(ctx)
		case *Projection:
			return op.planOffsetsForProjection(ctx)
		}
		if err != nil {
			return nil, false, err
		}
		return in, rewrite.SameTree, nil
	}

	op, err := rewrite.TopDown(root, TableID, visitor, stopAtRoute)
	if err != nil {
		if vterr, ok := err.(*vterrors.VitessError); ok && vterr.ID == "VT13001" {
			// we encountered a bug. let's try to back out
			return nil, errHorizonNotPlanned()
		}
		return nil, err
	}

	return op, nil
}

func (p *Projection) planOffsetsForProjection(ctx *plancontext.PlanningContext) (ops.Operator, rewrite.ApplyResult, error) {
	var err error
	for i, col := range p.Columns {
		rewritten := sqlparser.CopyOnRewrite(col.GetExpr(), nil, func(cursor *sqlparser.CopyOnWriteCursor) {
			col, ok := cursor.Node().(*sqlparser.ColName)
			if !ok {
				return
			}
			newSrc, offset, terr := p.Source.AddColumn(ctx, aeWrap(col))
			if terr != nil {
				err = terr
				return
			}
			p.Source = newSrc
			cursor.Replace(sqlparser.NewOffset(offset, col))
		}, nil).(sqlparser.Expr)
		if err != nil {
			return nil, false, err
		}

		offset, ok := rewritten.(*sqlparser.Offset)
		if ok {
			// we got a pure offset back. No need to do anything else
			p.Columns[i] = Offset{
				Expr:   col.GetExpr(),
				Offset: offset.V,
			}
			continue
		}

		eexpr, err := evalengine.Translate(rewritten, nil)
		if err != nil {
			return nil, false, err
		}

		p.Columns[i] = Eval{
			Expr:  rewritten,
			EExpr: eexpr,
		}
	}

	return p, rewrite.SameTree, nil
}

func planOffsetsOnJoins(ctx *plancontext.PlanningContext, op ops.Operator) error {
	err := rewrite.Visit(op, func(current ops.Operator) error {
		join, ok := current.(*ApplyJoin)
		if !ok {
			return nil
		}
		return join.planOffsets(ctx)
	})
	return err
}
