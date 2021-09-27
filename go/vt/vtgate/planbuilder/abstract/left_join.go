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

// LeftJoin represents an outerjoin.
type LeftJoin struct {
	Left, Right Operator
	Predicate   sqlparser.Expr
}

var _ Operator = (*LeftJoin)(nil)

// PushPredicate implements the Operator interface
func (lj *LeftJoin) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	deps := semTable.RecursiveDeps(expr)
	if deps.IsSolvedBy(lj.Left.TableID()) {
		return lj.Left.PushPredicate(expr, semTable)
	}

	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Cannot push predicate: %s", sqlparser.String(expr))
}

// TableID implements the Operator interface
func (lj *LeftJoin) TableID() semantics.TableSet {
	return lj.Right.TableID().Merge(lj.Left.TableID())
}

// UnsolvedPredicates implements the Operator interface
func (lj *LeftJoin) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	ts := lj.TableID()
	var result []sqlparser.Expr
	for _, expr := range lj.Left.UnsolvedPredicates(semTable) {
		deps := semTable.DirectDeps(expr)
		if !deps.IsSolvedBy(ts) {
			result = append(result, expr)
		}
	}
	for _, expr := range lj.Right.UnsolvedPredicates(semTable) {
		deps := semTable.DirectDeps(expr)
		if !deps.IsSolvedBy(ts) {
			result = append(result, expr)
		}
	}
	return result
}

// CheckValid implements the Operator interface
func (lj *LeftJoin) CheckValid() error {
	err := lj.Left.CheckValid()
	if err != nil {
		return err
	}
	return lj.Right.CheckValid()
}

// Compact implements the Operator interface
func (lj *LeftJoin) Compact() Operator {
	return lj
}
