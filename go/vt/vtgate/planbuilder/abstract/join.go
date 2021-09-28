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
	LHS, RHS  Operator
	Predicate sqlparser.Expr
	LeftJoin  bool
}

var _ Operator = (*Join)(nil)

// PushPredicate implements the Operator interface
func (j *Join) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	deps := semTable.RecursiveDeps(expr)
	switch {
	case deps.IsSolvedBy(j.LHS.TableID()):
		return j.LHS.PushPredicate(expr, semTable)
	case deps.IsSolvedBy(j.RHS.TableID()):
		if !j.LeftJoin {
			return j.RHS.PushPredicate(expr, semTable)
		}
		// we are looking for predicates like `tbl.col = <>` or `<> = tbl.col`,
		// where tbl is on the rhs of the left outer join
		if cmp, isCmp := expr.(*sqlparser.ComparisonExpr); isCmp && cmp.Operator != sqlparser.NullSafeEqualOp &&
			sqlparser.IsColName(cmp.Left) && semTable.RecursiveDeps(cmp.Left).IsSolvedBy(j.RHS.TableID()) ||
			sqlparser.IsColName(cmp.Right) && semTable.RecursiveDeps(cmp.Right).IsSolvedBy(j.RHS.TableID()) {
			// When the predicate we are pushing is using information from an outer table, we can
			// check whether the predicate is "null-intolerant" or not. Null-intolerant in this context means that
			// the predicate will not return true if the table columns are null.
			// Since an outer join is an inner join with the addition of all the rows from the left-hand side that
			// matched no rows on the right-hand, if we are later going to remove all the rows where the right-hand
			// side did not match, we might as well turn the join into an inner join.

			// This is based on the paper "Canonical Abstraction for Outerjoin Optimization" by J Rao et al
			j.LeftJoin = false
			return j.RHS.PushPredicate(expr, semTable)
		}
		// TODO - we should do this on the vtgate level once we have a Filter primitive
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard left join and where clause")
	case deps.IsSolvedBy(j.LHS.TableID().Merge(j.RHS.TableID())):
		j.Predicate = sqlparser.AndExpressions(j.Predicate, expr)
		return nil
	}

	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Cannot push predicate: %s", sqlparser.String(expr))
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
func (j *Join) Compact() Operator {
	return j
}
