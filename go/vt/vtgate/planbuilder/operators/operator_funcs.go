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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// PushPredicate is used to push predicates. It pushed it as far down as is possible in the tree.
// If we encounter a join and the predicate depends on both sides of the join, the predicate will be split into two parts,
// where data is fetched from the LHS of the join to be used in the evaluation on the RHS
func PushPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr, op Operator) (Operator, error) {
	switch op := op.(type) {
	case *Filter:
		var err error
		op.Source, err = PushPredicate(ctx, expr, op.Source)
		if err != nil {
			return nil, err
		}
		return op, nil
	case *QueryGraph:
		err := op.addPredicate(ctx, expr)
		if err != nil {
			return nil, err
		}
		return op, nil
	case *Route:
		return op.addPredicate(ctx, expr)
	case *ApplyJoin, *Join:
		join := op.(joinOperator) // stupid golang doesn't understand this without an explicit cast
		return addPredicate(join, ctx, expr, false)
	case *Table:
		// We do not add the predicate to op.qtable because that is an immutable struct that should not be
		// changed by physical operators.
		return newFilter(op, expr), nil
	case *Derived:
		err := op.addPredicate(ctx, expr)
		if err != nil {
			return nil, err
		}
		return op, nil
	case *Vindex:
		err := op.addPredicate(ctx, expr)
		if err != nil {
			return nil, err
		}
		return op, nil
	case *Union:
		err := op.addPredicate(ctx, expr)
		if err != nil {
			return nil, err
		}
		return op, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we cannot push predicates into %T", op)
	}
}

// PushOutputColumns will push the columns to the table they originate from,
// making sure that intermediate operators pass the data through
func PushOutputColumns(ctx *plancontext.PlanningContext, op Operator, columns ...*sqlparser.ColName) (PhysicalOperator, []int, error) {
	switch op := op.(type) {
	case *Route:
		retOp, offsets, err := PushOutputColumns(ctx, op.Source, columns...)
		op.Source = retOp
		return op, offsets, err
	case *ApplyJoin:
		var toTheLeft []bool
		var lhs, rhs []*sqlparser.ColName
		for _, col := range columns {
			col.Qualifier.Qualifier = sqlparser.NewIdentifierCS("")
			if ctx.SemTable.RecursiveDeps(col).IsSolvedBy(TableID(op.LHS)) {
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
		var offsets []int
		for _, col := range columns {
			exists := false
			for idx, opCol := range op.Columns {
				if sqlparser.EqualsRefOfColName(col, opCol) {
					exists = true
					offsets = append(offsets, idx)
					break
				}
			}
			if !exists {
				offsets = append(offsets, len(op.Columns))
				op.Columns = append(op.Columns, col)
			}
		}
		return op, offsets, nil
	case *Filter:
		newSrc, ints, err := PushOutputColumns(ctx, op.Source, columns...)
		op.Source = newSrc
		return op, ints, err
	case *Vindex:
		idx, err := op.PushOutputColumns(columns)
		return op, idx, err
	case *Derived:
		var noQualifierNames []*sqlparser.ColName
		var offsets []int
		if len(columns) == 0 {
			return op, nil, nil
		}
		for _, col := range columns {
			i, err := op.findOutputColumn(col)
			if err != nil {
				return nil, nil, err
			}
			var pos int
			op.ColumnsOffset, pos = addToIntSlice(op.ColumnsOffset, i)
			offsets = append(offsets, pos)
			// skip adding to columns as it exists already.
			if i > -1 {
				continue
			}
			op.Columns = append(op.Columns, col)
			noQualifierNames = append(noQualifierNames, sqlparser.NewColName(col.Name.String()))
		}
		if len(noQualifierNames) > 0 {
			_, _, err := PushOutputColumns(ctx, op.Source, noQualifierNames...)
			if err != nil {
				return nil, nil, err
			}
		}
		return op, offsets, nil

	default:
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we cannot push output columns into %T", op)
	}
}

func addToIntSlice(columnOffset []int, valToAdd int) ([]int, int) {
	for idx, val := range columnOffset {
		if val == valToAdd {
			return columnOffset, idx
		}
	}
	columnOffset = append(columnOffset, valToAdd)
	return columnOffset, len(columnOffset) - 1
}

// RemovePredicate is used when we turn a predicate into a plan operator,
// and the predicate needs to be removed as an AST construct
func RemovePredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr, op Operator) (Operator, error) {
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
			if !sqlparser.EqualsExpr(expr, e) {
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
			if sqlparser.EqualsExpr(predicate, expr) {
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

// BreakExpressionInLHSandRHS takes an expression and
// extracts the parts that are coming from one of the sides into `ColName`s that are needed
func BreakExpressionInLHSandRHS(
	ctx *plancontext.PlanningContext,
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
				node.Qualifier.Qualifier = sqlparser.NewIdentifierCS("")
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

type joinOperator interface {
	Operator
	getLHS() Operator
	getRHS() Operator
	setLHS(Operator)
	setRHS(Operator)
	makeInner()
	isInner() bool
	addJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error
}

func addPredicate(join joinOperator, ctx *plancontext.PlanningContext, expr sqlparser.Expr, joinPredicates bool) (Operator, error) {
	deps := ctx.SemTable.RecursiveDeps(expr)
	switch {
	case deps.IsSolvedBy(TableID(join.getLHS())):
		// predicates can always safely be pushed down to the lhs if that is all they depend on
		lhs, err := PushPredicate(ctx, expr, join.getLHS())
		if err != nil {
			return nil, err
		}
		join.setLHS(lhs)
		return join, err
	case deps.IsSolvedBy(TableID(join.getRHS())):
		// if we are dealing with an outer join, always start by checking if this predicate can turn
		// the join into an inner join
		if !join.isInner() && canConvertToInner(ctx, expr, TableID(join.getRHS())) {
			join.makeInner()
		}

		if !joinPredicates && !join.isInner() {
			// if we still are dealing with an outer join
			// we need to filter after the join has been evaluated
			return newFilter(join, expr), nil
		}

		// For inner joins, we can just push the filtering on the RHS
		rhs, err := PushPredicate(ctx, expr, join.getRHS())
		if err != nil {
			return nil, err
		}
		join.setRHS(rhs)
		return join, err

	case deps.IsSolvedBy(TableID(join)):
		// if we are dealing with an outer join, always start by checking if this predicate can turn
		// the join into an inner join
		if !joinPredicates && !join.isInner() && canConvertToInner(ctx, expr, TableID(join.getRHS())) {
			join.makeInner()
		}

		if !joinPredicates && !join.isInner() {
			// if we still are dealing with an outer join
			// we need to filter after the join has been evaluated
			return newFilter(join, expr), nil
		}

		err := join.addJoinPredicate(ctx, expr)
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
// This is based on the paper "Canonical Abstraction for Outerjoin Optimization" by J Rao et al
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
