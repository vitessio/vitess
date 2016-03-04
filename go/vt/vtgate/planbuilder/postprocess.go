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

// This file has functions to analyze postprocessing
// clauses like GROUP BY, etc.

// pushGroupBy processes the group by clause. It resolves all symbols,
// and ensures that there are no subqueries. It also verifies that the
// references don't addres an outer query. We only support group by
// for unsharded or single shard routes.
func pushGroupBy(groupBy sqlparser.GroupBy, plan planBuilder) error {
	if groupBy == nil {
		return nil
	}
	route, ok := plan.(*routeBuilder)
	if !ok {
		return errors.New("unsupported: complex join and group by")
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
			return false, errors.New("unsupported: subqueries in group by expression")
		}
		return true, nil
	}, groupBy)
	if err != nil {
		return err
	}
	if route.IsSingle() {
		route.SetGroupBy(groupBy)
		return nil
	}
	// It's a scatter route. We can allow group by if it references a
	// column with a unique vindex.
	for _, expr := range groupBy {
		vindex := plan.Symtab().Vindex(expr, route, true)
		if vindex != nil && IsUnique(vindex) {
			route.SetGroupBy(groupBy)
			return nil
		}
	}
	return errors.New("unsupported: scatter and group by")
}

// pushOrderBy pushes the order by clause to the appropriate routes.
// In the case of a join, this is allowed only if the order by columns
// match the join order. Otherwise, it's an error.
// If column numbers were used to reference the columns, those numbers
// are readjusted on push-down to match the numbers of the individual
// queries.
func pushOrderBy(orderBy sqlparser.OrderBy, plan planBuilder) error {
	if orderBy == nil {
		return nil
	}
	routeNumber := 0
	for _, order := range orderBy {
		// Only generator is allowed to change the AST.
		// If we have to change the order by expression,
		// we have to build a new node.
		pushOrder := order
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
				return errors.New("unsupported: subquery references outer query in order by")
			}
		case sqlparser.NumVal:
			num, err := strconv.ParseInt(string(node), 0, 64)
			if err != nil {
				return fmt.Errorf("error parsing order by clause: %s", string(node))
			}
			if num < 1 || num > int64(len(plan.Symtab().Colsyms)) {
				return errors.New("order by column number out of range")
			}
			colsym := plan.Symtab().Colsyms[num-1]
			route = colsym.Route()
			// We have to recompute the column number.
			for num, s := range route.Colsyms {
				if s == colsym {
					pushOrder = &sqlparser.Order{
						Expr:      sqlparser.NumVal(strconv.AppendInt(nil, int64(num+1), 10)),
						Direction: order.Direction,
					}
				}
			}
			if pushOrder == order {
				panic("unexpected: column not found for order by")
			}
		default:
			return errors.New("unsupported: complex expression in order by")
		}
		if route.Order() < routeNumber {
			return errors.New("unsupported: complex join and out of sequence order by")
		}
		if !route.IsSingle() {
			return errors.New("unsupported: scatter and order by")
		}
		routeNumber = route.Order()
		if err := route.AddOrder(pushOrder); err != nil {
			return err
		}
	}
	return nil
}

func pushLimit(limit *sqlparser.Limit, plan planBuilder) error {
	if limit == nil {
		return nil
	}
	route, ok := plan.(*routeBuilder)
	if !ok {
		return errors.New("unsupported: limits with complex joins")
	}
	if !route.IsSingle() {
		return errors.New("unsupported: limits with scatter")
	}
	route.SetLimit(limit)
	return nil
}

// pushMisc pushes comments and 'for update' clauses
// down to all routes.
func pushMisc(sel *sqlparser.Select, plan planBuilder) {
	switch plan := plan.(type) {
	case *joinBuilder:
		pushMisc(sel, plan.Left)
		pushMisc(sel, plan.Right)
	case *routeBuilder:
		plan.SetMisc(sel.Comments, sel.Lock)
	}
}
