// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"strconv"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func processHaving(having *sqlparser.Where, syms *symtab) error {
	if having == nil {
		return nil
	}
	for _, filter := range splitAndExpression(nil, having.Expr) {
		var route *routeBuilder
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.Subquery:
				// TODO(sougou): better error.
				return false, errors.New("subqueries not supported")
			case *sqlparser.ColName:
				newRoute, err := syms.Find(node, true)
				if err != nil {
					return false, err
				}
				if newRoute != nil {
					if route == nil {
						route = newRoute
					} else if route != newRoute {
						// TODO(sougou): better error.
						return false, errors.New("having clause is too complex")
					}
				}
			}
			return true, nil
		}, filter)
		if err != nil {
			return err
		}
		if route == nil {
			route = syms.FirstRoute
		}
		route.Select.AddHaving(filter)
	}
	return nil
}

func processOrderBy(orderBy sqlparser.OrderBy, syms *symtab) error {
	if orderBy == nil {
		return nil
	}
	routeNumber := 0
	for _, order := range orderBy {
		var route *routeBuilder
		var err error
		switch node := order.Expr.(type) {
		case *sqlparser.ColName:
			route, err = syms.Find(node, true)
			if err != nil {
				return err
			}
		case sqlparser.NumVal:
			num, err := strconv.ParseInt(string(node), 0, 64)
			if err != nil {
				// TODO(sougou): better error.
				return errors.New("error parsing order by clause")
			}
			if num < 1 || num > int64(len(syms.Colsyms)) {
				// TODO(sougou): better error.
				return errors.New("order by column number out of range")
			}
			route = syms.Colsyms[num-1].Route
		default:
			// TODO(sougou): better error.
			return errors.New("order by clause is too complex")
		}
		if route == nil || route.Order() < routeNumber {
			// TODO(sougou): better error.
			return errors.New("order by clause is too complex")
		}
		routeNumber = route.Order()
		route.Select.OrderBy = append(route.Select.OrderBy, order)
	}
	return nil
}

func processLimit(limit *sqlparser.Limit, plan planBuilder) error {
	if limit == nil {
		return nil
	}
	route, ok := plan.(*routeBuilder)
	if !ok {
		return errors.New("query is too complex to allow limits")
	}
	if !route.IsSingle() {
		return errors.New("query is too complex to allow limits")
	}
	route.Select.Limit = limit
	return nil
}
