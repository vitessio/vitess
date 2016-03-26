// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"strconv"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// ListVarName is the bind var name used for plans
// that require VTGate to compute custom list values,
// like for IN clauses.
const ListVarName = "__vals"

// generator is used for the generation and wire-up phase
// of plan building.
type generator struct {
	refs map[colref]string
	vars map[string]struct{}
}

// newGenerator creates a new generator for the current plan
// being built. It also needs the current list of bind vars
// used in the original query to make sure that the names
// it generates don't collide with those already in use.
func newGenerator(bindvars map[string]struct{}) *generator {
	return &generator{
		refs: make(map[colref]string),
		vars: bindvars,
	}
}

// Procure requests for the specified column from the tree
// and returns the join var name for it.
func (gen *generator) Procure(plan planBuilder, col *sqlparser.ColName, to int) string {
	from, joinVar := gen.Lookup(col)
	// If joinVar is empty, generate a unique name.
	if joinVar == "" {
		suffix := ""
		i := 0
		for {
			if col.Qualifier != "" {
				joinVar = string(col.Qualifier) + "_" + string(col.Name) + suffix
			} else {
				joinVar = string(col.Name) + suffix
			}
			if _, ok := gen.vars[joinVar]; !ok {
				break
			}
			i++
			suffix = strconv.Itoa(i)
		}
		gen.vars[joinVar] = struct{}{}
		gen.refs[newColref(col)] = joinVar
	}
	plan.SupplyVar(from, to, col, joinVar)
	return joinVar
}

// Lookup returns the order of the route that supplies the column, and
// returns the join var name if one has already been assigned
// for it.
func (gen *generator) Lookup(col *sqlparser.ColName) (order int, joinVar string) {
	ref := newColref(col)
	switch meta := col.Metadata.(type) {
	case *colsym:
		return meta.Route().Order(), gen.refs[ref]
	case *tableAlias:
		return meta.Route().Order(), gen.refs[ref]
	}
	panic("unreachable")
}
