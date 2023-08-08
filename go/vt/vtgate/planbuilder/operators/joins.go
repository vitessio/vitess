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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type JoinOp interface {
	ops.Operator
	GetLHS() ops.Operator
	GetRHS() ops.Operator
	SetLHS(ops.Operator)
	SetRHS(ops.Operator)
	MakeInner()
	IsInner() bool
	AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error
}

func AddPredicate(
	ctx *plancontext.PlanningContext,
	join JoinOp,
	expr sqlparser.Expr,
	joinPredicates bool,
	newFilter func(ops.Operator, sqlparser.Expr) ops.Operator,
) (ops.Operator, error) {
	deps := ctx.SemTable.RecursiveDeps(expr)
	switch {
	case deps.IsSolvedBy(TableID(join.GetLHS())):
		// predicates can always safely be pushed down to the lhs if that is all they depend on
		lhs, err := join.GetLHS().AddPredicate(ctx, expr)
		if err != nil {
			return nil, err
		}
		join.SetLHS(lhs)
		return join, err
	case deps.IsSolvedBy(TableID(join.GetRHS())):
		// if we are dealing with an outer join, always start by checking if this predicate can turn
		// the join into an inner join
		if !joinPredicates && !join.IsInner() && canConvertToInner(ctx, expr, TableID(join.GetRHS())) {
			join.MakeInner()
		}

		if !joinPredicates && !join.IsInner() {
			// if we still are dealing with an outer join
			// we need to filter after the join has been evaluated
			return newFilter(join, expr), nil
		}

		// For inner joins, we can just push the filtering on the RHS
		rhs, err := join.GetRHS().AddPredicate(ctx, expr)
		if err != nil {
			return nil, err
		}
		join.SetRHS(rhs)
		return join, err

	case deps.IsSolvedBy(TableID(join)):
		// if we are dealing with an outer join, always start by checking if this predicate can turn
		// the join into an inner join
		if !joinPredicates && !join.IsInner() && canConvertToInner(ctx, expr, TableID(join.GetRHS())) {
			join.MakeInner()
		}

		if !joinPredicates && !join.IsInner() {
			// if we still are dealing with an outer join
			// we need to filter after the join has been evaluated
			return newFilter(join, expr), nil
		}

		err := join.AddJoinPredicate(ctx, expr)
		if err != nil {
			return nil, err
		}

		return join, nil
	}
	return nil, nil
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
	isRHS := func(col *sqlparser.ColName) bool { return ctx.SemTable.RecursiveDeps(col).IsSolvedBy(rhs) }
	return notTrueWhenRootIsNULL(expr, isRHS)
}

// notTrueWhenRootIsNULL returns true if the expression is not true when the root is NULL.
// not true here means that the expression is false or NULL.
// This is used to check how NULL sensitive an expression is -
// if this is a predicate on an outer table, and we know that it will never return true
// if the outer table is missing, we can turn an outer join into an inner join.
func notTrueWhenRootIsNULL(
	e sqlparser.Expr, // this is the expression that will be checked
	isRoot func(node *sqlparser.ColName) bool,
) bool {
	switch expr := e.(type) {

	case *sqlparser.ColName:
		return isRoot(expr)

	case *sqlparser.BinaryExpr:
		return notTrueWhenRootIsNULL(expr.Left, isRoot) || notTrueWhenRootIsNULL(expr.Right, isRoot)

	case *sqlparser.ComparisonExpr:
		if expr.Operator == sqlparser.NullSafeEqualOp {
			return false
		}
		return notTrueWhenRootIsNULL(expr.Left, isRoot) || notTrueWhenRootIsNULL(expr.Right, isRoot)

	case *sqlparser.AndExpr:
		return notTrueWhenRootIsNULL(expr.Left, isRoot) || notTrueWhenRootIsNULL(expr.Right, isRoot)

	case *sqlparser.BetweenExpr:
		return notTrueWhenRootIsNULL(expr.Left, isRoot) || notTrueWhenRootIsNULL(expr.From, isRoot) || notTrueWhenRootIsNULL(expr.To, isRoot)

	case *sqlparser.IsExpr:
		if !notTrueWhenRootIsNULL(expr.Left, isRoot) {
			return false
		}
		switch expr.Right {
		case sqlparser.IsNotNullOp, sqlparser.IsTrueOp, sqlparser.IsFalseOp:
			return true

		default:
			return false
		}
	default:
		return false
	}
}
