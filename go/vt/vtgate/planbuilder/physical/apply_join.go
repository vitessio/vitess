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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// ApplyJoin is a nested loop join - for each row on the LHS,
// we'll execute the plan on the RHS, feeding data from left to right
type ApplyJoin struct {
	LHS, RHS abstract.PhysicalOperator

	// Columns stores the column indexes of the columns coming from the left and right side
	// negative value comes from LHS and positive from RHS
	Columns []int

	// Vars are the arguments that need to be copied from the LHS to the RHS
	Vars map[string]int

	// LeftJoin will be true in the case of an outer join
	LeftJoin bool

	// JoinCols are the columns from the LHS used for the join.
	// These are the same columns pushed on the LHS that are now used in the Vars field
	LHSColumns []*sqlparser.ColName

	Predicate sqlparser.Expr
}

var _ abstract.PhysicalOperator = (*ApplyJoin)(nil)

// IPhysical implements the PhysicalOperator interface
func (a *ApplyJoin) IPhysical() {}

// TableID implements the PhysicalOperator interface
func (a *ApplyJoin) TableID() semantics.TableSet {
	return a.LHS.TableID().Merge(a.RHS.TableID())
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (a *ApplyJoin) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	panic("implement me")
}

// CheckValid implements the PhysicalOperator interface
func (a *ApplyJoin) CheckValid() error {
	err := a.LHS.CheckValid()
	if err != nil {
		return err
	}
	return a.RHS.CheckValid()
}

// Compact implements the PhysicalOperator interface
func (a *ApplyJoin) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return a, nil
}

// Cost implements the PhysicalOperator interface
func (a *ApplyJoin) Cost() int {
	return a.LHS.Cost() + a.RHS.Cost()
}

// Clone implements the PhysicalOperator interface
func (a *ApplyJoin) Clone() abstract.PhysicalOperator {
	varsClone := map[string]int{}
	for key, value := range a.Vars {
		varsClone[key] = value
	}
	columnsClone := make([]int, len(a.Columns))
	copy(columnsClone, a.Columns)
	lhsColumns := make([]*sqlparser.ColName, len(a.LHSColumns))
	copy(lhsColumns, a.LHSColumns)
	return &ApplyJoin{
		LHS:        a.LHS.Clone(),
		RHS:        a.RHS.Clone(),
		Columns:    columnsClone,
		Vars:       varsClone,
		LeftJoin:   a.LeftJoin,
		Predicate:  sqlparser.CloneExpr(a.Predicate),
		LHSColumns: lhsColumns,
	}
}
