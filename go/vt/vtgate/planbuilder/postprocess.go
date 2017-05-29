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
)

// This file has functions to analyze postprocessing
// clauses like GROUP BY, etc.

// pushGroupBy processes the group by clause. It resolves all symbols,
// and ensures that there are no subqueries.
func pushGroupBy(groupBy sqlparser.GroupBy, bldr builder) error {
	if groupBy == nil {
		return nil
	}
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			_, _, err := bldr.Symtab().Find(node)
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

	// We can be here only if it was a route. All sanity checks have been
	// performed by checkAggregates.
	bldr.(*route).SetGroupBy(groupBy)
	return nil
}

// pushOrderBy pushes the order by clause to the appropriate route.
// In the case of a join, it's only possible to push down if the
// order by references columns of the left-most route. Otherwise, the
// function returns an unsupported error.
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

	rb, ok := bldr.Leftmost().(*route)
	if !ok {
		return errors.New("unsupported: cannot order by on a cross-shard subquery")
	}
	for _, order := range orderBy {
		if node, ok := order.Expr.(*sqlparser.SQLVal); ok && node.Type == sqlparser.IntVal {
			// This block handles constructs that use ordinals for 'ORDER BY'. For example:
			// SELECT a, b, c FROM t1, t2 ORDER BY 1, 2, 3.
			num, err := strconv.ParseInt(string(node.Val), 0, 64)
			if err != nil {
				return fmt.Errorf("error parsing order by clause: %s", sqlparser.String(node))
			}
			if num < 1 || num > int64(len(bldr.Symtab().ResultColumns)) {
				return errors.New("order by column number out of range")
			}
			target := bldr.Symtab().ResultColumns[num-1].column.Origin()
			if target != rb {
				return errors.New("unsupported: order by spans across shards")
			}
		} else {
			// Analyze column references within the expression to make sure they all
			// go to the same route.
			err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch node := node.(type) {
				case *sqlparser.ColName:
					target, _, err := bldr.Symtab().Find(node)
					if err != nil {
						return false, err
					}
					if target != rb {
						return false, errors.New("unsupported: order by spans across shards")
					}
				}
				return true, nil
			}, order.Expr)
			if err != nil {
				return err
			}
		}

		// The check for scatter route must be done at this level.
		// Future primitives may still want to push an order by clause
		// into a scatter route for the sake of optimization.
		if !rb.IsSingle() {
			return errors.New("unsupported: scatter and order by")
		}
		if err := bldr.PushOrderBy(order, rb); err != nil {
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
		return errors.New("unsupported: limits with cross-shard joins")
	}
	if !rb.IsSingle() {
		return errors.New("unsupported: limits with scatter")
	}
	rb.SetLimit(limit)
	return nil
}
