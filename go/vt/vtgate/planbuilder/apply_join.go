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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type applyJoin struct {
	LHS, RHS abstract.PhysicalOperator
	// columns stores the column indexes of the columns coming from the left and right side
	// negative value comes from LHS and positive from RHS
	columns []int
	// arguments that need to be copied from the LHS/RHS
	vars map[string]int

	predicate sqlparser.Expr
}

var _ abstract.PhysicalOperator = (*applyJoin)(nil)

// IPhysical implements the PhysicalOperator interface
func (a *applyJoin) IPhysical() {}

// TableID implements the PhysicalOperator interface
func (a *applyJoin) TableID() semantics.TableSet {
	return a.LHS.TableID().Merge(a.RHS.TableID())
}

// PushPredicate implements the PhysicalOperator interface
func (a *applyJoin) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	panic("unimplemented")
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (a *applyJoin) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	panic("implement me")
}

// CheckValid implements the PhysicalOperator interface
func (a *applyJoin) CheckValid() error {
	err := a.LHS.CheckValid()
	if err != nil {
		return err
	}
	return a.RHS.CheckValid()
}

// Compact implements the PhysicalOperator interface
func (a *applyJoin) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return a, nil
}

// Cost implements the PhysicalOperator interface
func (a *applyJoin) Cost() int {
	return a.LHS.Cost() + a.RHS.Cost()
}

// Clone implements the PhysicalOperator interface
func (a *applyJoin) Clone() abstract.PhysicalOperator {
	varsClone := map[string]int{}
	for key, value := range a.vars {
		varsClone[key] = value
	}
	columnsClone := make([]int, len(a.columns))
	copy(columnsClone, a.columns)
	return &applyJoin{
		LHS:     a.LHS.Clone(),
		RHS:     a.RHS.Clone(),
		columns: columnsClone,
		vars:    varsClone,
	}
}
