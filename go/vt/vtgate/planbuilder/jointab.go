/*
Copyright 2017 Google Inc.

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
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
)

// jointab manages procurement and naming of join
// variables across primitives.
type jointab struct {
	refs     map[*column]string
	vars     map[string]struct{}
	varIndex int
}

// newJointab creates a new jointab for the current plan
// being built. It also needs the current list of bind vars
// used in the original query to make sure that the names
// it generates don't collide with those already in use.
func newJointab(bindvars map[string]struct{}) *jointab {
	return &jointab{
		refs: make(map[*column]string),
		vars: bindvars,
	}
}

// Procure requests for the specified column from the plan
// and returns the join var name for it.
func (jt *jointab) Procure(bldr builder, col *sqlparser.ColName, to int) string {
	from, joinVar := jt.Lookup(col)
	// If joinVar is empty, generate a unique name.
	if joinVar == "" {
		suffix := ""
		i := 0
		for {
			if !col.Qualifier.IsEmpty() {
				joinVar = col.Qualifier.Name.CompliantName() + "_" + col.Name.CompliantName() + suffix
			} else {
				joinVar = col.Name.CompliantName() + suffix
			}
			if _, ok := jt.vars[joinVar]; !ok {
				break
			}
			i++
			suffix = strconv.Itoa(i)
		}
		jt.vars[joinVar] = struct{}{}
		jt.refs[col.Metadata.(*column)] = joinVar
	}
	bldr.SupplyVar(from, to, col, joinVar)
	return joinVar
}

// GenerateSubqueryVars generates substitution variable names for
// a subquery. It returns two names based on: __sq, __sq_has_values.
// The appropriate names can be used for substitution
// depending on the scenario.
func (jt *jointab) GenerateSubqueryVars() (sq, hasValues string) {
	for {
		jt.varIndex++
		suffix := strconv.Itoa(jt.varIndex)
		var1 := "__sq" + suffix
		var2 := "__sq_has_values" + suffix
		if jt.containsAny(var1, var2) {
			continue
		}
		return var1, var2
	}
}

func (jt *jointab) containsAny(names ...string) bool {
	for _, name := range names {
		if _, ok := jt.vars[name]; ok {
			return true
		}
		jt.vars[name] = struct{}{}
	}
	return false
}

// Lookup returns the order of the route that supplies the column and
// the join var name if one has already been assigned for it.
func (jt *jointab) Lookup(col *sqlparser.ColName) (order int, joinVar string) {
	c := col.Metadata.(*column)
	return c.Origin().Order(), jt.refs[c]
}
