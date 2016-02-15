// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// ListVarName is the bind var name used for plans
// that require VTGate to compute custom list values,
// like for IN clauses.
const ListVarName = "_vals"

type generator struct {
	refs map[colref]string
	vars map[string]struct{}
	plan planBuilder
}

func newGenerator(plan planBuilder) *generator {
	return &generator{
		refs: make(map[colref]string),
		vars: make(map[string]struct{}),
		plan: plan,
	}
}

func (gen *generator) Generate() error {
	gen.wireup(gen.plan)
	gen.fixupSelect(gen.plan)
	return gen.generateQueries(gen.plan)
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
	return nil
}

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
	ref := newColref(col)
	switch meta := col.Metadata.(type) {
	case *colsym:
		return meta.Route(), gen.refs[ref]
	case *tableAlias:
		return meta.Route(), gen.refs[ref]
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
	gen.refs[newColref(col)] = joinVar
	gen.commonJoin(fromRoute, toRoute).SupplyVar(col, joinVar)
	toRoute.Route.JoinVars[joinVar] = struct{}{}
}

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
	query, err := gen.convert(route, &route.Select)
	if err != nil {
		return err
	}
	route.Route.Query = query.(string)
	return nil
}

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
	return nil, errors.New("unrecognized symbol")
}

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
