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
			// This block handles constructs that use ordinals for 'ORDER BY'. For example:
			// SELECT a, b, c FROM t1, t2 ORDER BY 1, 2, 3.
			// If this query is broken into two, the ordinals would have to be renumbered
			// as follows:
			// 1. SELECT a, b FROM t1 ORDER BY 1, 2
			// 2. SELECT c FROM t2 ORDER BY 1 // instead of 3.
			num, err := strconv.ParseInt(string(node.Val), 0, 64)
			if err != nil {
				return fmt.Errorf("error parsing order by clause: %s", sqlparser.String(node))
			}
			if num < 1 || num > int64(len(bldr.Symtab().ResultColumns)) {
				return errors.New("order by column number out of range")
			}
			rc := bldr.Symtab().ResultColumns[num-1]
			rb = rc.column.Route()
			// We have to recompute the column number.
			for num, s := range rb.ResultColumns {
				if s == rc {
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
		if rb.Order < routeNumber {
			return errors.New("unsupported: complex join and out of sequence order by")
		}
		if !rb.IsSingle() {
			return errors.New("unsupported: scatter and order by")
		}
		routeNumber = rb.Order
		if err := rb.AddOrderBy(pushOrder); err != nil {
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
