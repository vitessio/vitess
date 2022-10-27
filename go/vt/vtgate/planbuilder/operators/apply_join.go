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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// ApplyJoin is a nested loop join - for each row on the LHS,
// we'll execute the plan on the RHS, feeding data from left to right
type ApplyJoin struct {
	LHS, RHS Operator

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

var _ PhysicalOperator = (*ApplyJoin)(nil)

// IPhysical implements the PhysicalOperator interface
func (a *ApplyJoin) IPhysical() {}

// Clone implements the Operator interface
func (a *ApplyJoin) Clone(inputs []Operator) Operator {
	checkSize(inputs, 2)
	varsClone := map[string]int{}
	for key, value := range a.Vars {
		varsClone[key] = value
	}
	columnsClone := make([]int, len(a.Columns))
	copy(columnsClone, a.Columns)
	lhsColumns := make([]*sqlparser.ColName, len(a.LHSColumns))
	copy(lhsColumns, a.LHSColumns)
	return &ApplyJoin{
		LHS:        inputs[0],
		RHS:        inputs[1],
		Columns:    columnsClone,
		Vars:       varsClone,
		LeftJoin:   a.LeftJoin,
		Predicate:  sqlparser.CloneExpr(a.Predicate),
		LHSColumns: lhsColumns,
	}
}

// Inputs implements the Operator interface
func (a *ApplyJoin) Inputs() []Operator {
	return []Operator{a.LHS, a.RHS}
}

var _ joinOperator = (*ApplyJoin)(nil)

func (a *ApplyJoin) tableID() semantics.TableSet {
	return TableID(a)
}

func (a *ApplyJoin) getLHS() Operator {
	return a.LHS
}

func (a *ApplyJoin) getRHS() Operator {
	return a.RHS
}

func (a *ApplyJoin) setLHS(operator Operator) {
	a.LHS = operator
}

func (a *ApplyJoin) setRHS(operator Operator) {
	a.RHS = operator
}

func (a *ApplyJoin) makeInner() {
	a.LeftJoin = false
}

func (a *ApplyJoin) isInner() bool {
	return !a.LeftJoin
}

func (a *ApplyJoin) addJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error {
	bvName, cols, predicate, err := BreakExpressionInLHSandRHS(ctx, expr, TableID(a.LHS))
	if err != nil {
		return err
	}
	lhs, idxs, err := PushOutputColumns(ctx, a.LHS, cols...)
	if err != nil {
		return err
	}
	a.LHSColumns = append(a.LHSColumns, cols...)

	a.LHS = lhs
	if a.Vars == nil {
		a.Vars = map[string]int{}
	}
	for i, idx := range idxs {
		a.Vars[bvName[i]] = idx
	}
	rhs, err := PushPredicate(ctx, predicate, a.RHS)
	if err != nil {
		return err
	}
	a.RHS = rhs

	a.Predicate = sqlparser.AndExpressions(expr, a.Predicate)
	return nil
}
