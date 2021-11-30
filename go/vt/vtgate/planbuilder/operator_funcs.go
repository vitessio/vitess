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

package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
)

// PushPredicate is used to push predicates
func PushPredicate(ctx *planningContext, expr sqlparser.Expr, op abstract.PhysicalOperator) (abstract.PhysicalOperator, error) {
	switch op := op.(type) {
	case *routeOp:
		err := op.updateRoutingLogic(expr, ctx.semTable)
		if err != nil {
			return nil, err
		}
		return PushPredicate(ctx, expr, op.source)
	case *applyJoin:
		deps := ctx.semTable.RecursiveDeps(expr)
		switch {
		case deps.IsSolvedBy(op.LHS.TableID()):
			return PushPredicate(ctx, expr, op.LHS)
		case deps.IsSolvedBy(op.RHS.TableID()):
			//if !j.LeftJoin {
			return PushPredicate(ctx, expr, op.RHS)
			//}
			// we are looking for predicates like `tbl.col = <>` or `<> = tbl.col`,
			// where tbl is on the rhs of the left outer join
			//if cmp, isCmp := expr.(*sqlparser.ComparisonExpr); isCmp && cmp.Operator != sqlparser.NullSafeEqualOp &&
			//	sqlparser.IsColName(cmp.Left) && semTable.RecursiveDeps(cmp.Left).IsSolvedBy(j.RHS.TableID()) ||
			//	sqlparser.IsColName(cmp.Right) && semTable.RecursiveDeps(cmp.Right).IsSolvedBy(j.RHS.TableID()) {
			//	// When the predicate we are pushing is using information from an outer table, we can
			//	// check whether the predicate is "null-intolerant" or not. Null-intolerant in this context means that
			//	// the predicate will not return true if the table columns are null.
			//	// Since an outer join is an inner join with the addition of all the rows from the left-hand side that
			//	// matched no rows on the right-hand, if we are later going to remove all the rows where the right-hand
			//	// side did not match, we might as well turn the join into an inner join.
			//
			//	// This is based on the paper "Canonical Abstraction for Outerjoin Optimization" by J Rao et al
			//	j.LeftJoin = false
			//	return j.RHS.PushPredicate(expr, semTable)
			//}
			//// TODO - we should do this on the vtgate level once we have a Filter primitive
			//return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard left join and where clause")
		case deps.IsSolvedBy(op.TableID()):
			bvName, cols, predicate, err := breakExpressionInLHSandRHS(expr, ctx.semTable, op.LHS.TableID())
			if err != nil {
				return nil, err
			}
			out, idxs, err := PushOutputColumns(ctx, op.LHS, cols)
			if err != nil {
				return nil, err
			}
			op.LHS = out
			for i, idx := range idxs {
				op.vars[bvName[i]] = idx
			}
			return PushPredicate(ctx, predicate, op.RHS)
		}
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Cannot push predicate: %s", sqlparser.String(expr))
	case *tableOp:
		return &filterOp{
			source:     op,
			predicates: []sqlparser.Expr{expr},
		}, nil
	case *filterOp:
		op.predicates = append(op.predicates, expr)
		return op, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we cannot push predicates into %T", op)
	}
}

func PushOutputColumns(ctx *planningContext, op abstract.PhysicalOperator, columns []*sqlparser.ColName) (abstract.PhysicalOperator, []int, error) {
	switch op := op.(type) {
	case *routeOp:
		return PushOutputColumns(ctx, op.source, columns)
	case *applyJoin:
		var toTheLeft []bool
		var lhs, rhs []*sqlparser.ColName
		for _, col := range columns {
			col.Qualifier.Qualifier = sqlparser.NewTableIdent("")
			if ctx.semTable.RecursiveDeps(col).IsSolvedBy(op.LHS.TableID()) {
				lhs = append(lhs, col)
				toTheLeft = append(toTheLeft, true)
			} else {
				rhs = append(rhs, col)
				toTheLeft = append(toTheLeft, false)
			}
		}
		out, lhsOffset, err := PushOutputColumns(ctx, op.LHS, lhs)
		if err != nil {
			return nil, nil, err
		}
		op.LHS = out
		out, rhsOffset, err := PushOutputColumns(ctx, op.RHS, rhs)
		if err != nil {
			return nil, nil, err
		}
		op.RHS = out

		outputColumns := make([]int, len(toTheLeft))
		var l, r int
		for i, isLeft := range toTheLeft {
			outputColumns[i] = len(op.columns)
			if isLeft {
				op.columns = append(op.columns, -lhsOffset[l]-1)
				l++
			} else {
				op.columns = append(op.columns, rhsOffset[r]+1)
				r++
			}
		}
		return op, outputColumns, nil
	case *tableOp:
		panic("add column list to tableop")
	case *filterOp:
		return PushOutputColumns(ctx, op.source, columns)
	default:
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we cannot push predicates into %T", op)
	}
}
