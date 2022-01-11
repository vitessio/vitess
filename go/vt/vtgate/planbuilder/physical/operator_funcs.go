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

package physical

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/context"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// PushPredicate is used to push predicates
func PushPredicate(ctx *context.PlanningContext, expr sqlparser.Expr, op abstract.PhysicalOperator) (abstract.PhysicalOperator, error) {
	switch op := op.(type) {
	case *Route:
		err := op.UpdateRoutingLogic(ctx, expr)
		if err != nil {
			return nil, err
		}
		newSrc, err := PushPredicate(ctx, expr, op.Source)
		if err != nil {
			return nil, err
		}
		op.Source = newSrc
		return op, err
	case *ApplyJoin:
		deps := ctx.SemTable.RecursiveDeps(expr)
		switch {
		case deps.IsSolvedBy(op.LHS.TableID()):
			newSrc, err := PushPredicate(ctx, expr, op.LHS)
			if err != nil {
				return nil, err
			}
			op.LHS = newSrc
			return op, err
		case deps.IsSolvedBy(op.RHS.TableID()):
			// if !j.LeftJoin {
			newSrc, err := PushPredicate(ctx, expr, op.RHS)
			if err != nil {
				return nil, err
			}
			op.RHS = newSrc
			return op, err
			// }
			// we are looking for predicates like `tbl.col = <>` or `<> = tbl.col`,
			// where tbl is on the rhs of the left outer join
			// if cmp, isCmp := expr.(*sqlparser.ComparisonExpr); isCmp && cmp.Operator != sqlparser.NullSafeEqualOp &&
			//	sqlparser.IsColName(cmp.Left) && SemTable.RecursiveDeps(cmp.Left).IsSolvedBy(j.RHS.TableID()) ||
			//	sqlparser.IsColName(cmp.Right) && SemTable.RecursiveDeps(cmp.Right).IsSolvedBy(j.RHS.TableID()) {
			//	// When the predicate we are pushing is using information from an outer table, we can
			//	// check whether the predicate is "null-intolerant" or not. Null-intolerant in this context means that
			//	// the predicate will not return true if the table columns are null.
			//	// Since an outer join is an inner join with the addition of all the rows from the left-hand side that
			//	// matched no rows on the right-hand, if we are later going to remove all the rows where the right-hand
			//	// side did not match, we might as well turn the join into an inner join.
			//
			//	// This is based on the paper "Canonical Abstraction for Outerjoin Optimization" by J Rao et al
			//	j.LeftJoin = false
			//	return j.RHS.PushPredicate(expr, SemTable)
			// }
			// // TODO - we should do this on the vtgate level once we have a Filter primitive
			// return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard left join and where clause")
		case deps.IsSolvedBy(op.TableID()):
			bvName, cols, predicate, err := breakExpressionInLHSandRHS(ctx, expr, op.LHS.TableID())
			if err != nil {
				return nil, err
			}
			out, idxs, err := PushOutputColumns(ctx, op.LHS, cols...)
			if err != nil {
				return nil, err
			}
			op.LHS = out
			for i, idx := range idxs {
				op.Vars[bvName[i]] = idx
			}
			newSrc, err := PushPredicate(ctx, predicate, op.RHS)
			if err != nil {
				return nil, err
			}
			op.RHS = newSrc
			op.Predicate = sqlparser.AndExpressions(op.Predicate, expr)
			return op, err
		}
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Cannot push predicate: %s", sqlparser.String(expr))
	case *Table:
		// We do not add the predicate to op.qtable because that is an immutable struct that should not be
		// changed by physical operators.
		return &Filter{
			Source:     op,
			Predicates: []sqlparser.Expr{expr},
		}, nil
	case *Filter:
		op.Predicates = append(op.Predicates, expr)
		return op, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we cannot push predicates into %T", op)
	}
}

func PushOutputColumns(ctx *context.PlanningContext, op abstract.PhysicalOperator, columns ...*sqlparser.ColName) (abstract.PhysicalOperator, []int, error) {
	switch op := op.(type) {
	case *Route:
		retOp, offsets, err := PushOutputColumns(ctx, op.Source, columns...)
		op.Source = retOp
		return op, offsets, err
	case *ApplyJoin:
		var toTheLeft []bool
		var lhs, rhs []*sqlparser.ColName
		for _, col := range columns {
			col.Qualifier.Qualifier = sqlparser.NewTableIdent("")
			if ctx.SemTable.RecursiveDeps(col).IsSolvedBy(op.LHS.TableID()) {
				lhs = append(lhs, col)
				toTheLeft = append(toTheLeft, true)
			} else {
				rhs = append(rhs, col)
				toTheLeft = append(toTheLeft, false)
			}
		}
		out, lhsOffset, err := PushOutputColumns(ctx, op.LHS, lhs...)
		if err != nil {
			return nil, nil, err
		}
		op.LHS = out
		out, rhsOffset, err := PushOutputColumns(ctx, op.RHS, rhs...)
		if err != nil {
			return nil, nil, err
		}
		op.RHS = out

		outputColumns := make([]int, len(toTheLeft))
		var l, r int
		for i, isLeft := range toTheLeft {
			outputColumns[i] = len(op.Columns)
			if isLeft {
				op.Columns = append(op.Columns, -lhsOffset[l]-1)
				l++
			} else {
				op.Columns = append(op.Columns, rhsOffset[r]+1)
				r++
			}
		}
		return op, outputColumns, nil
	case *Table:
		before := len(op.Columns)
		op.Columns = append(op.Columns, columns...)
		var offsets []int
		for i := before; i < len(op.Columns); i++ {
			offsets = append(offsets, i)
		}
		return op, offsets, nil
	case *Filter:
		return PushOutputColumns(ctx, op.Source, columns...)
	case *Vindex:
		idx, err := op.PushOutputColumns(columns)
		return op, idx, err
	default:
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we cannot push output columns into %T", op)
	}
}

func breakExpressionInLHSandRHS(
	ctx *context.PlanningContext,
	expr sqlparser.Expr,
	lhs semantics.TableSet,
) (bvNames []string, columns []*sqlparser.ColName, rewrittenExpr sqlparser.Expr, err error) {
	rewrittenExpr = sqlparser.CloneExpr(expr)
	_ = sqlparser.Rewrite(rewrittenExpr, nil, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.ColName:
			deps := ctx.SemTable.RecursiveDeps(node)
			if deps.NumberOfTables() == 0 {
				err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown column. has the AST been copied?")
				return false
			}
			if deps.IsSolvedBy(lhs) {
				node.Qualifier.Qualifier = sqlparser.NewTableIdent("")
				columns = append(columns, node)
				bvName := node.CompliantName()
				bvNames = append(bvNames, bvName)
				arg := sqlparser.NewArgument(bvName)
				// we are replacing one of the sides of the comparison with an argument,
				// but we don't want to lose the type information we have, so we copy it over
				ctx.SemTable.CopyExprInfo(node, arg)
				cursor.Replace(arg)
			}
		}
		return true
	})
	if err != nil {
		return nil, nil, nil, err
	}
	ctx.JoinPredicates[expr] = append(ctx.JoinPredicates[expr], rewrittenExpr)
	return
}
