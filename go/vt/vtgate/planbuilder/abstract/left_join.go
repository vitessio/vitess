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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// LeftJoin represents an outerjoin.
type LeftJoin struct {
	Left, Right Operator
	Predicate   sqlparser.Expr
}

var _ Operator = (*LeftJoin)(nil)

// PushPredicate implements the Operator interface
func (oj *LeftJoin) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	deps := semTable.RecursiveDeps(expr)
	if deps.IsSolvedBy(oj.Left.TableID()) {
		return oj.Left.PushPredicate(expr, semTable)
	}

	return semantics.Gen4NotSupportedF("cannot push predicates to the RHS of an outer join")
}

// TableID implements the Operator interface
func (oj *LeftJoin) TableID() semantics.TableSet {
	return oj.Right.TableID().Merge(oj.Left.TableID())
}

// UnsolvedPredicates implements the Operator interface
func (oj *LeftJoin) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	ts := oj.TableID()
	var result []sqlparser.Expr
	for _, expr := range oj.Left.UnsolvedPredicates(semTable) {
		deps := semTable.DirectDeps(expr)
		if !deps.IsSolvedBy(ts) {
			result = append(result, expr)
		}
	}
	for _, expr := range oj.Right.UnsolvedPredicates(semTable) {
		deps := semTable.DirectDeps(expr)
		if !deps.IsSolvedBy(ts) {
			result = append(result, expr)
		}
	}
	return result
}

// CheckValid implements the Operator interface
func (oj *LeftJoin) CheckValid() error {
	err := oj.Left.CheckValid()
	if err != nil {
		return err
	}
	return oj.Right.CheckValid()
}
