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
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ logicalPlan = (*semiJoin)(nil)

// semiJoin is the logicalPlan for engine.SemiJoin.
// This gets built if a rhs is correlated and can
// be pulled out but requires some variables to be supplied from outside.
type semiJoin struct {
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
