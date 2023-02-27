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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*semiJoin)(nil)

// semiJoin is the logicalPlan for engine.SemiJoin.
// This gets built if a rhs is correlated and can
// be pulled out but requires some variables to be supplied from outside.
type semiJoin struct {
	gen4Plan
	rhs  logicalPlan
	lhs  logicalPlan
	cols []int

	vars map[string]int

	// LHSColumns are the columns from the LHS used for the join.
	// These are the same columns pushed on the LHS that are now used in the vars field
	LHSColumns []*sqlparser.ColName
}

// newSemiJoin builds a new semiJoin.
func newSemiJoin(lhs, rhs logicalPlan, vars map[string]int, lhsCols []*sqlparser.ColName) *semiJoin {
	return &semiJoin{
		rhs:        rhs,
		lhs:        lhs,
		vars:       vars,
		LHSColumns: lhsCols,
	}
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

// WireupGen4 implements the logicalPlan interface
func (ps *semiJoin) WireupGen4(ctx *plancontext.PlanningContext) error {
	if err := ps.lhs.WireupGen4(ctx); err != nil {
		return err
	}
	return ps.rhs.WireupGen4(ctx)
}

// Rewrite implements the logicalPlan interface
func (ps *semiJoin) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 2 {
		return vterrors.VT13001("semiJoin: wrong number of inputs")
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

// OutputColumns implements the logicalPlan interface
func (ps *semiJoin) OutputColumns() []sqlparser.SelectExpr {
	return ps.lhs.OutputColumns()
}
