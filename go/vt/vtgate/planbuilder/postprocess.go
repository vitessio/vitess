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

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// This file has functions to analyze postprocessing
// clauses like ORDER BY, etc.

// groupByHandler is a primitive that can handle a group by expression.
type groupByHandler interface {
	builder
	SetGroupBy(sqlparser.GroupBy) error
	MakeDistinct() error
}

// pushGroupBy processes the group by clause. It resolves all symbols,
// and ensures that there are no subqueries.
func pushGroupBy(sel *sqlparser.Select, bldr builder) error {
	if sel.Distinct != "" {
		if len(sel.GroupBy) != 0 {
			return errors.New("unsupported: distinct and group by in the same query")
		}
		return bldr.(groupByHandler).MakeDistinct()
	}

	if sel.GroupBy == nil {
		return nil
	}
	if err := bldr.Symtab().ResolveSymbols(sel.GroupBy); err != nil {
		return fmt.Errorf("unsupported: in group by: %v", err)
	}

	// We can be here only if the builder could handle a group by.
	return bldr.(groupByHandler).SetGroupBy(sel.GroupBy)
}

// pushOrderBy pushes the order by clause to the appropriate route.
// In the case of a join, it's only possible to push down if the
// order by references columns of the left-most route. Otherwise, the
// function returns an unsupported error.
func pushOrderBy(orderBy sqlparser.OrderBy, bldr builder) error {
	if oa, ok := bldr.(*orderedAggregate); ok {
		return oa.PushOrderBy(orderBy)
	}
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

	leftmostRB, ok := bldr.Leftmost().(*route)
	if !ok {
		return errors.New("unsupported: cannot order by on a cross-shard subquery")
	}
	for _, order := range orderBy {
		if node, ok := order.Expr.(*sqlparser.SQLVal); ok {
			// This block handles constructs that use ordinals for 'ORDER BY'. For example:
			// SELECT a, b, c FROM t1, t2 ORDER BY 1, 2, 3.
			num, err := bldr.Symtab().ResultFromNumber(node)
			if err != nil {
				return err
			}
			target := bldr.Symtab().ResultColumns[num].column.Origin()
			if target != leftmostRB {
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
					if target != leftmostRB {
						return false, errors.New("unsupported: order by spans across shards")
					}
				case *sqlparser.Subquery:
					return false, errors.New("unsupported: order by has subquery")
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
		if !leftmostRB.IsSingle() {
			return errors.New("unsupported: scatter and order by")
		}
		leftmostRB.PushOrderBy(order)
	}
	return nil
}

func pushLimit(limit *sqlparser.Limit, bldr builder) error {
	if limit == nil {
		return nil
	}
	rb, ok := bldr.(*route)
	if !ok {
		return errors.New("unsupported: limits with cross-shard query")
	}
	if !rb.IsSingle() {
		return errors.New("unsupported: limits with scatter")
	}
	rb.SetLimit(limit)
	return nil
}
