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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*semiJoin)(nil)

// semiJoin is the logicalPlan for engine.SemiJoin.
// This gets built if a rhs is correlated and can
// be pulled out but requires some variables to be supplied from outside.
type semiJoin struct {
	rhs  logicalPlan
	lhs  logicalPlan
	vars map[string]int
	cols []int
}

// newSemiJoin builds a new semiJoin.
func newSemiJoin(lhs, rhs logicalPlan, vars map[string]int) *semiJoin {
	return &semiJoin{
		rhs:  rhs,
		lhs:  lhs,
		vars: vars,
	}
}

// Order implements the logicalPlan interface
func (ps *semiJoin) Order() int {
	panic("[BUG]: should not be called. This is a Gen4 primitive")
}

// Reorder implements the logicalPlan interface
func (ps *semiJoin) Reorder(order int) {
	panic("[BUG]: should not be called. This is a Gen4 primitive")
}

// Primitive implements the logicalPlan interface
func (ps *semiJoin) Primitive() engine.Primitive {
	return &engine.SemiJoin{
		Left:  ps.lhs.Primitive(),
		Right: ps.rhs.Primitive(),
		Vars:  ps.vars,
		Cols:  ps.cols,
	}
}

// ResultColumns implements the logicalPlan interface
func (ps *semiJoin) ResultColumns() []*resultColumn {
	panic("[BUG]: should not be called. This is a Gen4 primitive")
}

// Wireup implements the logicalPlan interface
func (ps *semiJoin) Wireup(plan logicalPlan, jt *jointab) error {
	panic("[BUG]: should not be called. This is a Gen4 primitive")
}

// WireupGen4 implements the logicalPlan interface
func (ps *semiJoin) WireupGen4(semTable *semantics.SemTable) error {
	if err := ps.lhs.WireupGen4(semTable); err != nil {
		return err
	}
	return ps.rhs.WireupGen4(semTable)
}

// SupplyVar implements the logicalPlan interface
func (ps *semiJoin) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("[BUG]: should not be called. This is a Gen4 primitive")
}

// SupplyCol implements the logicalPlan interface
func (ps *semiJoin) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("[BUG]: should not be called. This is a Gen4 primitive")
}

// SupplyWeightString implements the logicalPlan interface
func (ps *semiJoin) SupplyWeightString(colNumber int, alsoAddToGroupBy bool) (weightcolNumber int, err error) {
	panic("[BUG]: should not be called. This is a Gen4 primitive")
}

// Rewrite implements the logicalPlan interface
func (ps *semiJoin) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 2 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "semiJoin: wrong number of inputs")
	}
	ps.lhs = inputs[0]
	ps.rhs = inputs[1]
	return nil
}

// ContainsTables implements the logicalPlan interface
func (ps *semiJoin) ContainsTables() semantics.TableSet {
	return ps.lhs.ContainsTables().Merge(ps.rhs.ContainsTables())
}

// Inputs implements the logicalPlan interface
func (ps *semiJoin) Inputs() []logicalPlan {
	return []logicalPlan{ps.lhs, ps.rhs}
}
