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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

// jointab manages procurement and naming of join
// variables across primitives.
type jointab struct {
	refs     map[*column]string
	reserved *sqlparser.ReservedVars
	varIndex int
}

// newJointab creates a new jointab for the current plan
// being built. It also needs the current list of bind vars
// used in the original query to make sure that the names
// it generates don't collide with those already in use.
func newJointab(reserved *sqlparser.ReservedVars) *jointab {
	return &jointab{
		refs:     make(map[*column]string),
		reserved: reserved,
	}
}

// Procure requests for the specified column from the plan
// and returns the join var name for it.
func (jt *jointab) Procure(plan logicalPlan, col *sqlparser.ColName, to int) string {
	from, joinVar := jt.Lookup(col)
	// If joinVar is empty, generate a unique name.
	if joinVar == "" {
		joinVar = jt.reserved.ReserveColName(col)
		jt.refs[col.Metadata.(*column)] = joinVar
	}
	plan.SupplyVar(from, to, col, joinVar)
	return joinVar
}

// GenerateSubqueryVars generates substitution variable names for
// a subquery. It returns two names based on: __sq, __sq_has_values.
// The appropriate names can be used for substitution
// depending on the scenario.
func (jt *jointab) GenerateSubqueryVars() (sq, hasValues string) {
	for {
		jt.varIndex++
		var1 := fmt.Sprintf("__sq%d", jt.varIndex)
		var2 := fmt.Sprintf("__sq_has_values%d", jt.varIndex)
		if !jt.reserved.ReserveAll(var1, var2) {
			continue
		}
		return var1, var2
	}
}

// Lookup returns the order of the route that supplies the column and
// the join var name if one has already been assigned for it.
func (jt *jointab) Lookup(col *sqlparser.ColName) (order int, joinVar string) {
	c := col.Metadata.(*column)
	return c.Origin().Order(), jt.refs[c]
}
