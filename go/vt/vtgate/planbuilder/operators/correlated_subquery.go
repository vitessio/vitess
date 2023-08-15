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
)

type (
	// CorrelatedSubQueryOp is a correlated subquery that is used for filtering rows from the outer query.
	// It is a join between the outer query and the subquery, where the subquery is the RHS.
	// We are only interested in the existence of rows in the RHS, so we only need to know if
	CorrelatedSubQueryOp struct {
		LHS, RHS  ops.Operator
		Extracted *sqlparser.ExtractedSubquery

		// JoinCols are the columns from the LHS used for the join.
		// These are the same columns pushed on the LHS that are now used in the Vars field
		LHSColumns []*sqlparser.ColName

		// arguments that need to be copied from the outer to inner
		Vars map[string]int

		noColumns
		noPredicates
	}

	// UncorrelatedSubQuery is a subquery that can be executed indendently of the outer query,
	// so we pull it out and execute before the outer query, and feed the result into a bindvar
	// that is fed to the outer query
	UncorrelatedSubQuery struct {
		Outer, Inner ops.Operator
		Extracted    *sqlparser.ExtractedSubquery

		noColumns
		noPredicates
	}
)

// Clone implements the Operator interface
func (s *UncorrelatedSubQuery) Clone(inputs []ops.Operator) ops.Operator {
	result := &UncorrelatedSubQuery{
		Outer:     inputs[0],
		Inner:     inputs[1],
		Extracted: s.Extracted,
	}
	return result
}

func (s *UncorrelatedSubQuery) GetOrdering() ([]ops.OrderBy, error) {
	return s.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (s *UncorrelatedSubQuery) Inputs() []ops.Operator {
	return []ops.Operator{s.Outer, s.Inner}
}

// SetInputs implements the Operator interface
func (s *UncorrelatedSubQuery) SetInputs(ops []ops.Operator) {
	s.Outer, s.Inner = ops[0], ops[1]
}

func (s *UncorrelatedSubQuery) ShortDescription() string {
	return ""
}

// Clone implements the Operator interface
func (c *CorrelatedSubQueryOp) Clone(inputs []ops.Operator) ops.Operator {
	columns := make([]*sqlparser.ColName, len(c.LHSColumns))
	copy(columns, c.LHSColumns)
	vars := make(map[string]int, len(c.Vars))
	for k, v := range c.Vars {
		vars[k] = v
	}

	result := &CorrelatedSubQueryOp{
		LHS:        inputs[0],
		RHS:        inputs[1],
		Extracted:  c.Extracted,
		LHSColumns: columns,
		Vars:       vars,
	}
	return result
}

func (c *CorrelatedSubQueryOp) GetOrdering() ([]ops.OrderBy, error) {
	return c.LHS.GetOrdering()
}

// Inputs implements the Operator interface
func (c *CorrelatedSubQueryOp) Inputs() []ops.Operator {
	return []ops.Operator{c.LHS, c.RHS}
}

// SetInputs implements the Operator interface
func (c *CorrelatedSubQueryOp) SetInputs(ops []ops.Operator) {
	c.LHS, c.RHS = ops[0], ops[1]
}

func (c *CorrelatedSubQueryOp) ShortDescription() string {
	return ""
}
