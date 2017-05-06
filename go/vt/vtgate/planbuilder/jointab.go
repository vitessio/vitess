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

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// jointab manages procurement and naming of join
// variables across primitives.
type jointab struct {
	refs map[colref]string
	vars map[string]struct{}
}

// newJointab creates a new jointab for the current plan
// being built. It also needs the current list of bind vars
// used in the original query to make sure that the names
// it generates don't collide with those already in use.
func newJointab(bindvars map[string]struct{}) *jointab {
	return &jointab{
		refs: make(map[colref]string),
		vars: bindvars,
	}
}

// Procure requests for the specified column from the plan
// and returns the join var name for it.
func (jt *jointab) Procure(bldr builder, col *sqlparser.ColName, to int) string {
	from, joinVar := jt.Lookup(col)
	// If joinVar is empty, jterate a unique name.
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
		jt.refs[newColref(col)] = joinVar
	}
	bldr.SupplyVar(from, to, col, joinVar)
	return joinVar
}

// Lookup returns the order of the route that supplies the column and
// the join var name if one has already been assigned for it.
func (jt *jointab) Lookup(col *sqlparser.ColName) (order int, joinVar string) {
	ref := newColref(col)
	return ref.Route().Order(), jt.refs[ref]
}
