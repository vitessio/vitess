/*
Copyright 2021 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// RemovePredicate is used when we turn a predicate into a plan operator,
// and the predicate needs to be removed as an AST construct
func RemovePredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr, op ops.Operator) (ops.Operator, error) {
	switch op := op.(type) {
	case *Route:
		newSrc, err := RemovePredicate(ctx, expr, op.Source)
		if err != nil {
			return nil, err
		}
		op.Source = newSrc
		return op, err
	case *ApplyJoin:
		isRemoved := false
		deps := ctx.SemTable.RecursiveDeps(expr)
		if deps.IsSolvedBy(TableID(op.LHS)) {
			newSrc, err := RemovePredicate(ctx, expr, op.LHS)
			if err != nil {
				return nil, err
			}
			op.LHS = newSrc
			isRemoved = true
		}

		if deps.IsSolvedBy(TableID(op.RHS)) {
			newSrc, err := RemovePredicate(ctx, expr, op.RHS)
			if err != nil {
				return nil, err
			}
			op.RHS = newSrc
			isRemoved = true
		}

		var keep []sqlparser.Expr
		for _, e := range sqlparser.SplitAndExpression(nil, op.Predicate) {
			if !ctx.SemTable.EqualsExpr(expr, e) {
				keep = append(keep, e)
				isRemoved = true
			}
		}
		op.Predicate = sqlparser.AndExpressions(keep...)

		if !isRemoved {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "remove '%s' predicate not supported on cross-shard join query", sqlparser.String(expr))
		}
		return op, nil
	case *Filter:
		idx := -1
		for i, predicate := range op.Predicates {
			if ctx.SemTable.EqualsExpr(predicate, expr) {
				idx = i
			}
		}
		if idx == -1 {
			// the predicate is not here. let's remove it from our source
			newSrc, err := RemovePredicate(ctx, expr, op.Source)
			if err != nil {
				return nil, err
			}
			op.Source = newSrc
			return op, nil
		}
		if len(op.Predicates) == 1 {
			// no predicates left on this operator, so we just remove it
			return op.Source, nil
		}

		// remove the predicate from this filter
		op.Predicates = append(op.Predicates[:idx], op.Predicates[idx+1:]...)
		return op, nil

	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "this should not happen - tried to remove predicate from table op")
	}
}
