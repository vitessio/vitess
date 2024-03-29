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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type JoinOp interface {
	Operator
	GetLHS() Operator
	GetRHS() Operator
	SetLHS(Operator)
	SetRHS(Operator)
	MakeInner()
	IsInner() bool
	AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr)
}

func AddPredicate(
	ctx *plancontext.PlanningContext,
	join JoinOp,
	expr sqlparser.Expr,
	joinPredicates bool,
	newFilter func(Operator, sqlparser.Expr) Operator,
) Operator {
	deps := ctx.SemTable.RecursiveDeps(expr)
	switch {
	case deps.IsSolvedBy(TableID(join.GetLHS())):
		// predicates can always safely be pushed down to the lhs if that is all they depend on
		lhs := join.GetLHS().AddPredicate(ctx, expr)
		join.SetLHS(lhs)
		return join
	case deps.IsSolvedBy(TableID(join.GetRHS())):
		// if we are dealing with an outer join, always start by checking if this predicate can turn
		// the join into an inner join
		if !joinPredicates && !join.IsInner() && canConvertToInner(ctx, expr, TableID(join.GetRHS())) {
			join.MakeInner()
		}

		if !joinPredicates && !join.IsInner() {
			// if we still are dealing with an outer join
			// we need to filter after the join has been evaluated
			return newFilter(join, expr)
		}

		// For inner joins, we can just push the filtering on the RHS
		rhs := join.GetRHS().AddPredicate(ctx, expr)
		join.SetRHS(rhs)
		return join

	case deps.IsSolvedBy(TableID(join)):
		// if we are dealing with an outer join, always start by checking if this predicate can turn
		// the join into an inner join
		if !joinPredicates && !join.IsInner() && canConvertToInner(ctx, expr, TableID(join.GetRHS())) {
			join.MakeInner()
		}

		if !joinPredicates && !join.IsInner() {
			// if we still are dealing with an outer join
			// we need to filter after the join has been evaluated
			return newFilter(join, expr)
		}

		join.AddJoinPredicate(ctx, expr)

		return join
	}
	return nil
}

// we are looking for predicates like `tbl.col = <>` or `<> = tbl.col`,
// where tbl is on the rhs of the left outer join
// When a predicate uses information from an outer table, we can convert from an outer join to an inner join
// if the predicate is "null-intolerant".
//
// Null-intolerant in this context means that the predicate will not be true if the table columns are null.
//
// Since an outer join is an inner join with the addition of all the rows from the left-hand side that
// matched no rows on the right-hand, if we are later going to remove all the rows where the right-hand
// side did not match, we might as well turn the join into an inner join.
//
// This is based on the paper "Canonical Abstraction for Outerjoin Optimization" by J Rao et al.
func canConvertToInner(ctx *plancontext.PlanningContext, expr sqlparser.Expr, rhs semantics.TableSet) bool {
	isColNameFromRHS := func(e sqlparser.Expr) bool {
		return sqlparser.IsColName(e) && ctx.SemTable.RecursiveDeps(e).IsSolvedBy(rhs)
	}
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if expr.Operator == sqlparser.NullSafeEqualOp {
			return false
		}

		return isColNameFromRHS(expr.Left) || isColNameFromRHS(expr.Right)

	case *sqlparser.IsExpr:
		if expr.Right != sqlparser.IsNotNullOp {
			return false
		}

		return isColNameFromRHS(expr.Left)
	default:
		return false
	}
}
