// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// This file has functions to analyze postprocessing
// clauses like GROUP BY, etc.

// pushGroupBy processes the group by clause. It resolves all symbols,
// and ensures that there are no subqueries.
func pushGroupBy(groupBy sqlparser.GroupBy, bldr builder) error {
	if groupBy == nil {
		return nil
	}
	rb, ok := bldr.(*route)
	if !ok {
		return errors.New("unsupported: complex join and group by")
	}
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			_, _, err := bldr.Symtab().Find(node, true)
			if err != nil {
				return false, err
			}
		case *sqlparser.Subquery:
			return false, errors.New("unsupported: subqueries in group by expression")
		}
		return true, nil
	}, groupBy)
	if err != nil {
		return err
	}
	if rb.IsSingle() {
		rb.SetGroupBy(groupBy)
		return nil
	}
	// It's a scatter route. We can allow group by if it references a
	// column with a unique vindex.
	for _, expr := range groupBy {
		vindex := bldr.Symtab().Vindex(expr, rb, true)
		if vindex != nil && vindexes.IsUnique(vindex) {
			rb.SetGroupBy(groupBy)
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
func pushOrderBy(orderBy sqlparser.OrderBy, bldr builder) error {
	switch len(orderBy) {
	case 0:
		return nil
	case 1:
		// Special handling for ORDER BY NULL. Push it everywhere.
		if _, ok := orderBy[0].Expr.(*sqlparser.NullVal); ok {
			bldr.PushOrderByNull()
			return nil
		}
	}
	routeNumber := 0
	for _, order := range orderBy {
		// Only generator is allowed to change the AST.
		// If we have to change the order by expression,
		// we have to build a new node.
		pushOrder := order
		var rb *route

		if node, ok := order.Expr.(*sqlparser.SQLVal); ok && node.Type == sqlparser.IntVal {
			num, err := strconv.ParseInt(string(node.Val), 0, 64)
			if err != nil {
				return fmt.Errorf("error parsing order by clause: %s", sqlparser.String(node))
			}
			if num < 1 || num > int64(len(bldr.Symtab().Colsyms)) {
				return errors.New("order by column number out of range")
			}
			colsym := bldr.Symtab().Colsyms[num-1]
			rb = colsym.Route()
			// We have to recompute the column number.
			for num, s := range rb.Colsyms {
				if s == colsym {
					pushOrder = &sqlparser.Order{
						Expr:      sqlparser.NewIntVal(strconv.AppendInt(nil, int64(num+1), 10)),
						Direction: order.Direction,
					}
				}
			}
			if pushOrder == order {
				panic("unexpected: column not found for order by")
			}
		} else {
			// Analyze column references within the expression to make sure they all
			// go to the same route.
			err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch node := node.(type) {
				case *sqlparser.ColName:
					curRoute, _, err := bldr.Symtab().Find(node, true)
					if err != nil {
						return false, err
					}
					if rb == nil || rb == curRoute {
						rb = curRoute
						return true, nil
					}
					return false, errors.New("unsupported: complex join and complex order by")
				}
				return true, nil
			}, order.Expr)
			if err != nil {
				return err
			}
		}
		if rb == nil {
			return errors.New("unsupported: complex order by")
		}
		if rb.Order() < routeNumber {
			return errors.New("unsupported: complex join and out of sequence order by")
		}
		if !rb.IsSingle() {
			return errors.New("unsupported: scatter and order by")
		}
		routeNumber = rb.Order()
		if err := rb.AddOrder(pushOrder); err != nil {
			return err
		}
	}
	return nil
}

func pushLimit(limit *sqlparser.Limit, bldr builder) error {
	if limit == nil {
		return nil
	}
	rb, ok := bldr.(*route)
	if !ok {
		return errors.New("unsupported: limits with complex joins")
	}
	if !rb.IsSingle() {
		return errors.New("unsupported: limits with scatter")
	}
	rb.SetLimit(limit)
	return nil
}
