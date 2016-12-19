// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
