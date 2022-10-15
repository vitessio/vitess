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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Join represents a join. If we have a predicate, this is an inner join. If no predicate exists, it is a cross join
type Join struct {
	LHS, RHS  Operator
	Predicate sqlparser.Expr
	LeftJoin  bool
}

var _ Operator = (*Join)(nil)

// ThisIsAnOperator implements the Operator interface
func (*Join) ThisIsAnOperator() {}

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
func (j *Join) tryConvertToInnerJoin(expr sqlparser.Expr, semTable *semantics.SemTable) {
	if !j.LeftJoin {
		return
	}

	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if expr.Operator == sqlparser.NullSafeEqualOp {
			return
		}

		if sqlparser.IsColName(expr.Left) && semTable.RecursiveDeps(expr.Left).IsSolvedBy(TableID(j.RHS)) ||
			sqlparser.IsColName(expr.Right) && semTable.RecursiveDeps(expr.Right).IsSolvedBy(TableID(j.RHS)) {
			j.LeftJoin = false
		}

	case *sqlparser.IsExpr:
		if expr.Right != sqlparser.IsNotNullOp {
			return
		}

		if sqlparser.IsColName(expr.Left) && semTable.RecursiveDeps(expr.Left).IsSolvedBy(TableID(j.RHS)) {
			j.LeftJoin = false
		}
	}
}
