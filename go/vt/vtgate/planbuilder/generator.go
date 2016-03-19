// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"
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
	plan planBuilder
}

// newGenerator creates a new generator for the current plan
// being built. It also needs the current list of bind vars
// used in the original query to make sure that the names
// it generates don't collide with those already in use.
func newGenerator(plan planBuilder, bindvars map[string]struct{}) *generator {
	return &generator{
		refs: make(map[colref]string),
		vars: bindvars,
		plan: plan,
	}
}

// Generate performs all the steps necessary to generate
// the plan: wire-up, fixup and query generation.
func (gen *generator) Generate() error {
	gen.wireup(gen.plan)
	gen.fixupSelect(gen.plan)
	return gen.generateQueries(gen.plan)
}

// wireup finds all external references, replaces them
// with bind vars and makes the source of the reference
// supply the value accordingly.
func (gen *generator) wireup(plan planBuilder) {
	switch plan := plan.(type) {
	case *joinBuilder:
		gen.wireup(plan.Left)
		gen.wireup(plan.Right)
	case *routeBuilder:
		gen.wireRouter(plan)
	}
}

// fixupSelect fixes up the select statements for the routes:
// If the select expression list is empty, it adds a '1' to it.
// If comparison expressions are of the form 1 = col, it changes
// them to col = 1.
func (gen *generator) fixupSelect(plan planBuilder) {
	switch plan := plan.(type) {
	case *joinBuilder:
		gen.fixupSelect(plan.Left)
		gen.fixupSelect(plan.Right)
	case *routeBuilder:
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			switch node := node.(type) {
			case *sqlparser.Select:
				if len(node.SelectExprs) == 0 {
					node.SelectExprs = sqlparser.SelectExprs([]sqlparser.SelectExpr{
						&sqlparser.NonStarExpr{
							Expr: sqlparser.NumVal([]byte{'1'}),
						},
					})
				}
			case *sqlparser.ComparisonExpr:
				if node.Operator == sqlparser.EqualStr {
					if exprIsValue(node.Left, plan) && !exprIsValue(node.Right, plan) {
						node.Left, node.Right = node.Right, node.Left
					}
				}
			}
			return true, nil
		}, &plan.Select)
	}
}

// generateQueries converts the AST in the routes to actual queries.
func (gen *generator) generateQueries(plan planBuilder) error {
	switch plan := plan.(type) {
	case *joinBuilder:
		err := gen.generateQueries(plan.Left)
		if err != nil {
			return err
		}
		return gen.generateQueries(plan.Right)
	case *routeBuilder:
		return gen.generateQuery(plan)
	}
	panic("unreachable")
}

// wireRouter performs the wireup for the specified router.
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

// resolve performs the wireup for the specified column reference.
// If the reference is local, then it's a no-op. Otherwise, it assigns
// a bind var name to it and performs the wire-up.
func (gen *generator) resolve(col *sqlparser.ColName, toRoute *routeBuilder) {
	fromRoute, joinVar := gen.lookup(col)
	if fromRoute == toRoute {
		return
	}
	if _, ok := toRoute.Route.JoinVars[joinVar]; ok {
		// Already using.
		return
	}
	gen.join(fromRoute, col, joinVar, toRoute)
}

// lookup returns the route that supplies the column, and
// returns the join var name if one has already been assigned
// for it.
func (gen *generator) lookup(col *sqlparser.ColName) (route *routeBuilder, joinVar string) {
	ref := newColref(col)
	switch meta := col.Metadata.(type) {
	case *colsym:
		return meta.Route(), gen.refs[ref]
	case *tableAlias:
		return meta.Route(), gen.refs[ref]
	}
	panic("unreachable")
}

// join performs the wireup starting from the fromRoute all the way to the toRoute.
// It first finds the common join node for the two routes, and makes it request
// the column from the LHS, and then associates the join var name to the
// returned column number.
func (gen *generator) join(fromRoute *routeBuilder, col *sqlparser.ColName, joinVar string, toRoute *routeBuilder) {
	suffix := ""
	i := 0
	if joinVar == "" {
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
	join := gen.commonJoin(fromRoute, toRoute)
	join.SupplyVar(col, joinVar)
	toRoute.Route.JoinVars[joinVar] = struct{}{}
}

// commonJoin returns the common join between two routes.
func (gen *generator) commonJoin(fromRoute *routeBuilder, toRoute *routeBuilder) *joinBuilder {
	node := gen.plan.(*joinBuilder)
	from := fromRoute.Order()
	to := toRoute.Order()
	for {
		if from > node.LeftOrder {
			node = node.Right.(*joinBuilder)
			continue
		}
		if to <= node.LeftOrder {
			node = node.Left.(*joinBuilder)
			continue
		}
		return node
	}
}

// generateQueries converts the AST in the route to the actual query.
func (gen *generator) generateQuery(route *routeBuilder) error {
	var err error
	switch vals := route.Route.Values.(type) {
	case *sqlparser.ComparisonExpr:
		// It's an IN clause.
		route.Route.Values, err = gen.convert(route, vals.Right)
		if err != nil {
			return err
		}
		vals.Right = sqlparser.ListArg("::" + ListVarName)
	default:
		route.Route.Values, err = gen.convert(route, vals)
		if err != nil {
			return err
		}
	}
	// Generation of select cannot fail.
	query, _ := gen.convert(route, &route.Select)
	route.Route.Query = query.(string)
	route.Route.FieldQuery = gen.generateFieldQuery(route, &route.Select)
	return nil
}

// convert converts the input value to the type that the route need.
// If the input type is sqlparser.SQLNode, it generates the string representation,
// and substitutes external references with bind vars. So, this function can
// be used for generating the SELECT query for the route as well as resolving
// the Values field of the Route.
func (gen *generator) convert(route *routeBuilder, val interface{}) (interface{}, error) {
	varFormatter := func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			fromRoute, joinVar := gen.lookup(node)
			if fromRoute != route {
				buf.Myprintf("%a", ":"+joinVar)
				return
			}
		}
		node.Format(buf)
	}
	buf := sqlparser.NewTrackedBuffer(varFormatter)
	switch val := val.(type) {
	case nil:
		return nil, nil
	case *sqlparser.Select, *sqlparser.ColName:
		varFormatter(buf, val.(sqlparser.SQLNode))
		return buf.ParsedQuery().Query, nil
	case sqlparser.ValTuple:
		vals := make([]interface{}, 0, len(val))
		for _, val := range val {
			v, err := gen.convert(route, val)
			if err != nil {
				return nil, err
			}
			vals = append(vals, v)
		}
		return vals, nil
	case sqlparser.ListArg:
		return string(val), nil
	case sqlparser.ValExpr:
		return valConvert(val)
	}
	panic("unrecognized symbol")
}

// valConvert converts an AST value to the Value field in the route.
func valConvert(node sqlparser.ValExpr) (interface{}, error) {
	switch node := node.(type) {
	case sqlparser.ValArg:
		return string(node), nil
	case sqlparser.StrVal:
		return []byte(node), nil
	case sqlparser.NumVal:
		val := string(node)
		signed, err := strconv.ParseInt(val, 0, 64)
		if err == nil {
			return signed, nil
		}
		unsigned, err := strconv.ParseUint(val, 0, 64)
		if err == nil {
			return unsigned, nil
		}
		return nil, err
	case *sqlparser.NullVal:
		return nil, nil
	}
	return nil, fmt.Errorf("%v is not a value", sqlparser.String(node))
}

// generateFieldQuery generates a query with an impossible where.
// This will be used on the RHS node to fetch field info if the LHS
// returns no result.
func (gen *generator) generateFieldQuery(route *routeBuilder, sel *sqlparser.Select) string {
	formatter := func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case *sqlparser.Select:
			buf.Myprintf("select %v from %v where 1 != 1", node.SelectExprs, node.From)
			return
		case *sqlparser.JoinTableExpr:
			if node.Join == sqlparser.LeftJoinStr || node.Join == sqlparser.RightJoinStr {
				// ON clause is requried
				buf.Myprintf("%v %s %v on 1 != 1", node.LeftExpr, node.Join, node.RightExpr)
			} else {
				buf.Myprintf("%v %s %v", node.LeftExpr, node.Join, node.RightExpr)
			}
			return
		case *sqlparser.ColName:
			fromRoute, joinVar := gen.lookup(node)
			if fromRoute != route {
				buf.Myprintf("%a", ":"+joinVar)
				return
			}
		}
		node.Format(buf)
	}
	buf := sqlparser.NewTrackedBuffer(formatter)
	formatter(buf, sel)
	return buf.ParsedQuery().Query
}
