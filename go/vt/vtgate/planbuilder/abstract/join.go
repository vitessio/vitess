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

package abstract

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Join represents a join. If we have a predicate, this is an inner join. If no predicate exists, it is a cross join
type Join struct {
	LHS, RHS  LogicalOperator
	Predicate sqlparser.Expr
	LeftJoin  bool
}

var _ LogicalOperator = (*Join)(nil)

// iLogical implements the LogicalOperator interface
func (*Join) iLogical() {}

// PushPredicate implements the Operator interface
func (j *Join) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error) {
	deps := semTable.RecursiveDeps(expr)
	switch {
	case deps.IsSolvedBy(j.LHS.TableID()):
		lhs, err := j.LHS.PushPredicate(expr, semTable)
		if err != nil {
			return nil, err
		}
		j.LHS = lhs
		return j, nil
	case deps.IsSolvedBy(j.RHS.TableID()):
		// we are looking for predicates like `tbl.col = <>` or `<> = tbl.col`,
		// where tbl is on the rhs of the left outer join
		if cmp, isCmp := expr.(*sqlparser.ComparisonExpr); isCmp && cmp.Operator != sqlparser.NullSafeEqualOp &&
			(sqlparser.IsColName(cmp.Left) && semTable.RecursiveDeps(cmp.Left).IsSolvedBy(j.RHS.TableID()) ||
				sqlparser.IsColName(cmp.Right) && semTable.RecursiveDeps(cmp.Right).IsSolvedBy(j.RHS.TableID())) {
			// When the predicate we are pushing is using information from an outer table, we can
			// check whether the predicate is "null-intolerant" or not. Null-intolerant in this context means that
			// the predicate will not return true if the table columns are null.
			// Since an outer join is an inner join with the addition of all the rows from the left-hand side that
			// matched no rows on the right-hand, if we are later going to remove all the rows where the right-hand
			// side did not match, we might as well turn the join into an inner join.

			// This is based on the paper "Canonical Abstraction for Outerjoin Optimization" by J Rao et al
			j.LeftJoin = false
		}
		if !j.LeftJoin {
			rhs, err := j.RHS.PushPredicate(expr, semTable)
			if err != nil {
				return nil, err
			}
			j.RHS = rhs
			return j, err
		}
		op := &Filter{
			Source:     j,
			Predicates: []sqlparser.Expr{expr},
		}
		return op, nil
	case deps.IsSolvedBy(j.LHS.TableID().Merge(j.RHS.TableID())):
		j.Predicate = sqlparser.AndExpressions(j.Predicate, expr)
		return j, nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Cannot push predicate: %s", sqlparser.String(expr))
}

// TableID implements the Operator interface
func (j *Join) TableID() semantics.TableSet {
	return j.RHS.TableID().Merge(j.LHS.TableID())
}

// UnsolvedPredicates implements the Operator interface
func (j *Join) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	ts := j.TableID()
	var result []sqlparser.Expr
	for _, expr := range j.LHS.UnsolvedPredicates(semTable) {
		deps := semTable.DirectDeps(expr)
		if !deps.IsSolvedBy(ts) {
			result = append(result, expr)
		}
	}
	for _, expr := range j.RHS.UnsolvedPredicates(semTable) {
		deps := semTable.DirectDeps(expr)
		if !deps.IsSolvedBy(ts) {
			result = append(result, expr)
		}
	}
	return result
}

// CheckValid implements the Operator interface
func (j *Join) CheckValid() error {
	err := j.LHS.CheckValid()
	if err != nil {
		return err
	}

	return j.RHS.CheckValid()
}

// Compact implements the Operator interface
func (j *Join) Compact(semTable *semantics.SemTable) (LogicalOperator, error) {
	if j.LeftJoin {
		// we can't merge outer joins into a single QG
		return j, nil
	}

	lqg, lok := j.LHS.(*QueryGraph)
	rqg, rok := j.RHS.(*QueryGraph)
	if !lok || !rok {
		return j, nil
	}

	op := &QueryGraph{
		Tables:     append(lqg.Tables, rqg.Tables...),
		innerJoins: append(lqg.innerJoins, rqg.innerJoins...),
		NoDeps:     sqlparser.AndExpressions(lqg.NoDeps, rqg.NoDeps),
	}
	err := op.collectPredicate(j.Predicate, semTable)
	if err != nil {
		return nil, err
	}
	return op, nil
}
