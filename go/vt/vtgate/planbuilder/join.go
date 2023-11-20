/*
Copyright 2019 The Vitess Authors.

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

var _ logicalPlan = (*join)(nil)

// join is used to build a Join primitive.
// It's used to build an inner join and only used by the Gen4 planner
type join struct {
	// Left and Right are the nodes for the join.
	Left, Right logicalPlan

	// The Opcode tells us if this is an inner or outer join
	Opcode engine.JoinOpcode

	// These are the columns that will be produced by this plan.
	// Negative offsets come from the LHS, and positive from the RHS
	Cols []int

	// Vars are the columns that will be sent from the LHS to the RHS
	// the number is the offset on the LHS result, and the string is the bind variable name used in the RHS
	Vars map[string]int

	// LHSColumns are the columns from the LHS used for the join.
	// These are the same columns pushed on the LHS that are now used in the Vars field
	LHSColumns []*sqlparser.ColName
}

// Primitive implements the logicalPlan interface
func (j *join) Primitive() engine.Primitive {
	return &engine.Join{
		Left:   j.Left.Primitive(),
		Right:  j.Right.Primitive(),
		Cols:   j.Cols,
		Vars:   j.Vars,
		Opcode: j.Opcode,
	}
}

type hashJoin struct {
	lhs, rhs logicalPlan
	inner    *engine.HashJoin
}

func (hj *hashJoin) Primitive() engine.Primitive {
	lhs := hj.lhs.Primitive()
	rhs := hj.rhs.Primitive()
	hj.inner.Left = lhs
	hj.inner.Right = rhs
	return hj.inner
}
