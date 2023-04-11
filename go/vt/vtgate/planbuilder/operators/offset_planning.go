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

// planOffsets will walk the tree top down,
func planOffsets(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, rewrite.ApplyResult, error) {
		var err error
		switch op := in.(type) {
		case *Horizon:
			return nil, false, vterrors.VT13001("should not see Horizons here")
		case *Derived:
			return nil, false, vterrors.VT13001("should not see Derived here")
		case *Filter:
			err = planFilter(ctx, op)
		case *Projection:
			return planOffsetsForProjection(ctx, op)
		case *ApplyJoin:
			err = op.planOffsets(ctx)
		}
		if err != nil {
			return nil, false, err
		}
		return in, rewrite.SameTree, nil
	}

	op, err := rewrite.FixedPointTopDown(root, TableID, visitor, stopAtRoute)
	if err != nil {
		if vterr, ok := err.(*vterrors.VitessError); ok && vterr.ID == "VT13001" {
			// we encountered a bug. let's try to back out
			return nil, errNotHorizonPlanned
		}
		return nil, err
	}

	return op, nil
}

func (a *ApplyJoin) planOffsets(ctx *plancontext.PlanningContext) error {
	for _, col := range a.ColumnsAST {
		// Read the type description for JoinColumn to understand the following code
		for i, lhsExpr := range col.LHSExprs {
			offset, err := a.pushColLeft(ctx, aeWrap(lhsExpr))
			if err != nil {
				return err
			}
			if col.RHSExpr == nil {
				// if we don't have a RHS expr, it means that this is a pure LHS expression
				a.Columns = append(a.Columns, -offset-1)
			} else {
				a.Vars[col.BvNames[i]] = offset
			}
		}
		if col.RHSExpr != nil {
			offset, err := a.pushColRight(ctx, aeWrap(col.RHSExpr))
			if err != nil {
				return err
			}
			a.Columns = append(a.Columns, offset+1)
		}
	}

	return nil
}

func planOffsetsForProjection(ctx *plancontext.PlanningContext, op *Projection) (ops.Operator, rewrite.ApplyResult, error) {
	var err error
	isSimple := true
	for i, col := range op.Columns {
		rewritten := sqlparser.CopyOnRewrite(col.GetExpr(), nil, func(cursor *sqlparser.CopyOnWriteCursor) {
			col, ok := cursor.Node().(*sqlparser.ColName)
			if !ok {
				return
			}
			newSrc, offset, terr := op.Source.AddColumn(ctx, aeWrap(col))
			if terr != nil {
				err = terr
			}
			op.Source = newSrc
			cursor.Replace(sqlparser.NewOffset(offset, col))
		}, nil).(sqlparser.Expr)
		if err != nil {
			return nil, false, err
		}

		offset, ok := rewritten.(*sqlparser.Offset)
		if ok {
			// we got a pure offset back. No need to do anything else
			op.Columns[i] = Offset{
				Expr:   col.GetExpr(),
				Offset: offset.V,
			}
			continue
		}
		isSimple = false

		eexpr, err := evalengine.Translate(rewritten, nil)
		if err != nil {
			return nil, false, err
		}

		op.Columns[i] = Eval{
			Expr:  rewritten,
			EExpr: eexpr,
		}
	}
	if !isSimple {
		return op, rewrite.SameTree, nil
	}

	// is we were able to turn all the columns into offsets, we can use the SimpleProjection instead
	sp := &SimpleProjection{
		Source: op.Source,
	}
	for i, column := range op.Columns {
		offset := column.(Offset)
		sp.Columns = append(sp.Columns, offset.Offset)
		sp.ASTColumns = append(sp.ASTColumns, &sqlparser.AliasedExpr{Expr: offset.Expr, As: sqlparser.NewIdentifierCI(op.ColumnNames[i])})
	}
	return sp, rewrite.NewTree, nil
}
