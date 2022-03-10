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

package physical

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	CorrelatedSubQueryOp struct {
		Outer, Inner abstract.PhysicalOperator
		Extracted    *sqlparser.ExtractedSubquery

		// JoinCols are the columns from the LHS used for the join.
		// These are the same columns pushed on the LHS that are now used in the Vars field
		LHSColumns []*sqlparser.ColName

		// arguments that need to be copied from the outer to inner
		Vars map[string]int
	}

	SubQueryOp struct {
		Outer, Inner abstract.PhysicalOperator
		Extracted    *sqlparser.ExtractedSubquery
	}

	SubQueryInner struct {
		Inner abstract.LogicalOperator

		// ExtractedSubquery contains all information we need about this subquery
		ExtractedSubquery *sqlparser.ExtractedSubquery
	}
)

var _ abstract.PhysicalOperator = (*SubQueryOp)(nil)
var _ abstract.PhysicalOperator = (*CorrelatedSubQueryOp)(nil)

// TableID implements the PhysicalOperator interface
func (s *SubQueryOp) TableID() semantics.TableSet {
	return s.Inner.TableID().Merge(s.Outer.TableID())
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (s *SubQueryOp) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return append(s.Outer.UnsolvedPredicates(semTable), s.Inner.UnsolvedPredicates(semTable)...)
}

// CheckValid implements the PhysicalOperator interface
func (s *SubQueryOp) CheckValid() error {
	err := s.Inner.CheckValid()
	if err != nil {
		return err
	}
	return s.Outer.CheckValid()
}

// IPhysical implements the PhysicalOperator interface
func (s *SubQueryOp) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (s *SubQueryOp) Cost() int {
	return s.Inner.Cost() + s.Outer.Cost()
}

// Clone implements the PhysicalOperator interface
func (s *SubQueryOp) Clone() abstract.PhysicalOperator {
	result := &SubQueryOp{
		Outer:     s.Outer.Clone(),
		Inner:     s.Inner.Clone(),
		Extracted: s.Extracted,
	}
	return result
}

func (c *CorrelatedSubQueryOp) TableID() semantics.TableSet {
	return c.Inner.TableID().Merge(c.Outer.TableID())
}

func (c *CorrelatedSubQueryOp) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return append(c.Outer.UnsolvedPredicates(semTable), c.Inner.UnsolvedPredicates(semTable)...)
}

func (c *CorrelatedSubQueryOp) CheckValid() error {
	err := c.Inner.CheckValid()
	if err != nil {
		return err
	}
	return c.Outer.CheckValid()
}

func (c *CorrelatedSubQueryOp) IPhysical() {}

func (c *CorrelatedSubQueryOp) Cost() int {
	return c.Inner.Cost() + c.Outer.Cost()
}

func (c *CorrelatedSubQueryOp) Clone() abstract.PhysicalOperator {
	columns := make([]*sqlparser.ColName, len(c.LHSColumns))
	copy(columns, c.LHSColumns)
	result := &CorrelatedSubQueryOp{
		Outer:      c.Outer.Clone(),
		Inner:      c.Inner.Clone(),
		Extracted:  c.Extracted,
		LHSColumns: columns,
	}
	return result
}
