// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"strconv"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func processGroupBy(groupBy sqlparser.GroupBy, plan planBuilder) error {
	if groupBy == nil {
		return nil
	}
	route, ok := plan.(*routeBuilder)
	if !ok {
		return errors.New("query is too complex to allow group by")
	}
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			_, isLocal, err := plan.Symtab().Find(node, true)
			if err != nil {
				return false, err
			}
			if !isLocal {
				return false, errors.New("unsupported: subquery references outer query in group by")
			}
		case *sqlparser.Subquery:
			// TODO(sougou): better error.
			return false, errors.New("subqueries not supported in group by")
		}
		return true, nil
	}, groupBy)
	if err != nil {
		return err
	}
	if route.IsSingle() {
		route.Select.GroupBy = groupBy
		return nil
	}
	// It's a scatter route. We can allow group by if it references a
	// column with a unique vindex.
	for _, expr := range groupBy {
		vindex := plan.Symtab().Vindex(expr, route, true)
		if vindex != nil && IsUnique(vindex) {
			route.Select.GroupBy = groupBy
			return nil
		}
	}
	return errors.New("query is too complex to allow group by")
}

func processOrderBy(orderBy sqlparser.OrderBy, plan planBuilder) error {
	if orderBy == nil {
		return nil
	}
	routeNumber := 0
	for _, order := range orderBy {
		var route *routeBuilder
		switch node := order.Expr.(type) {
		case *sqlparser.ColName:
			var isLocal bool
			var err error
			route, isLocal, err = plan.Symtab().Find(node, true)
			if err != nil {
				return err
			}
			if !isLocal {
				// TODO(sougou): better error.
				return errors.New("unsupported: subquery references outer query in order by")
			}
		case sqlparser.NumVal:
			num, err := strconv.ParseInt(string(node), 0, 64)
			if err != nil {
				// TODO(sougou): better error.
				return errors.New("error parsing order by clause")
			}
			if num < 1 || num > int64(len(plan.Symtab().Colsyms)) {
				// TODO(sougou): better error.
				return errors.New("order by column number out of range")
			}
			route = plan.Symtab().Colsyms[num-1].Route()
		default:
			// TODO(sougou): better error.
			return errors.New("order by clause is too complex")
		}
		if route.Order() < routeNumber {
			// TODO(sougou): better error.
			return errors.New("order by clause is too complex")
		}
		if !route.IsSingle() {
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

func processMisc(sel *sqlparser.Select, plan planBuilder) {
	switch plan := plan.(type) {
	case *joinBuilder:
		processMisc(sel, plan.Left)
		processMisc(sel, plan.Right)
	case *routeBuilder:
		plan.Select.Comments = sel.Comments
		plan.Select.Lock = sel.Lock
	}
}
