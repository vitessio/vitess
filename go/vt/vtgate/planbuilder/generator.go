// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"strconv"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

type generator struct {
	refs map[colref]string
	vars map[string]struct{}
}

type colref struct {
	metadata interface{}
	name     sqlparser.SQLName
}

func newGenerator() *generator {
	return &generator{
		refs: make(map[colref]string),
		vars: make(map[string]struct{}),
	}
}

func (gen *generator) Generate(plan planBuilder) {
	gen.wireup(plan)
	gen.generateQueries(plan)
}

func (gen *generator) wireup(plan planBuilder) {
	switch plan := plan.(type) {
	case *joinBuilder:
		gen.wireup(plan.Left)
		gen.wireup(plan.Right)
	case *routeBuilder:
		gen.wireRouter(plan)
	}
}

func (gen *generator) generateQueries(plan planBuilder) {
	switch plan := plan.(type) {
	case *joinBuilder:
		gen.generateQueries(plan.Left)
		gen.generateQueries(plan.Right)
	case *routeBuilder:
		gen.generateQuery(plan)
	}
}

func (gen *generator) wireRouter(route *routeBuilder) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			gen.resolve(node, route)
			return false, nil
		}
		return true, nil
	}, &route.Select)
}

func (gen *generator) resolve(col *sqlparser.ColName, toRoute *routeBuilder) {
	fromRoute, joinVar := gen.lookup(col)
	if fromRoute == toRoute {
		return
	}
	if joinVar != "" {
		return
	}
	gen.join(fromRoute, col, toRoute)
}

func (gen *generator) lookup(col *sqlparser.ColName) (route *routeBuilder, joinVar string) {
	ref := colref{
		metadata: col.Metadata,
		name:     col.Name,
	}
	switch meta := col.Metadata.(type) {
	case *colsym:
		return meta.Route, gen.refs[ref]
	case *tableAlias:
		return meta.Route, gen.refs[ref]
	}
	panic("unreachable")
}

func (gen *generator) join(fromRoute *routeBuilder, col *sqlparser.ColName, toRoute *routeBuilder) {
	suffix := ""
	i := 0
	var joinVar string
	for {
		joinVar = string(col.Name) + suffix
		if _, ok := gen.vars[joinVar]; !ok {
			break
		}
		i++
		suffix = strconv.Itoa(i)
	}
	gen.vars[joinVar] = struct{}{}
	ref := colref{
		metadata: col.Metadata,
		name:     col.Name,
	}
	gen.refs[ref] = joinVar
	fromRoute.SupplyJoinVar(col, joinVar)
	toRoute.Route.UseVars[joinVar] = struct{}{}
}

func (gen *generator) generateQuery(route *routeBuilder) {
	buf := sqlparser.NewTrackedBuffer(func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			fromRoute, joinVar := gen.lookup(node)
			if fromRoute != route {
				buf.Myprintf("%a", ":"+joinVar)
				return
			}
		}
		node.Format(buf)
	})
	route.Select.Format(buf)
	route.Route.Query = buf.ParsedQuery().Query
}
