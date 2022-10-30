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
)

type (
	CorrelatedSubQueryOp struct {
		Outer, Inner Operator
		Extracted    *sqlparser.ExtractedSubquery

		// JoinCols are the columns from the LHS used for the join.
		// These are the same columns pushed on the LHS that are now used in the Vars field
		LHSColumns []*sqlparser.ColName

		// arguments that need to be copied from the outer to inner
		Vars map[string]int
	}

	SubQueryOp struct {
		Outer, Inner Operator
		Extracted    *sqlparser.ExtractedSubquery
	}
)

var _ PhysicalOperator = (*SubQueryOp)(nil)
var _ PhysicalOperator = (*CorrelatedSubQueryOp)(nil)

// IPhysical implements the PhysicalOperator interface
func (s *SubQueryOp) IPhysical() {}

// Clone implements the Operator interface
func (s *SubQueryOp) Clone(inputs []Operator) Operator {
	checkSize(inputs, 2)
	result := &SubQueryOp{
		Outer:     inputs[0],
		Inner:     inputs[1],
		Extracted: s.Extracted,
	}
	return result
}

// Inputs implements the Operator interface
func (s *SubQueryOp) Inputs() []Operator {
	return []Operator{s.Outer, s.Inner}
}

// IPhysical implements the PhysicalOperator interface
func (c *CorrelatedSubQueryOp) IPhysical() {}

// Clone implements the Operator interface
func (c *CorrelatedSubQueryOp) Clone(inputs []Operator) Operator {
	checkSize(inputs, 2)
	columns := make([]*sqlparser.ColName, len(c.LHSColumns))
	copy(columns, c.LHSColumns)
	vars := make(map[string]int, len(c.Vars))
	for k, v := range c.Vars {
		vars[k] = v
	}

	result := &CorrelatedSubQueryOp{
		Outer:      inputs[0],
		Inner:      inputs[1],
		Extracted:  c.Extracted,
		LHSColumns: columns,
		Vars:       vars,
	}
	return result
}

// Inputs implements the Operator interface
func (c *CorrelatedSubQueryOp) Inputs() []Operator {
	return []Operator{c.Outer, c.Inner}
}
