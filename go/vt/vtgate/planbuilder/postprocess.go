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
// and ensures that there are no subqueries. It also verifies that the
// references don't addres an outer query. We only support group by
// for unsharded or single shard routes.
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
			_, isLocal, err := bldr.Symtab().Find(node, true)
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
	s := bldr.Symtab().SetState(symtabOrderBy)
	defer bldr.Symtab().SetState(s)

	if orderBy == nil {
		return nil
	}
	routeNumber := 0
	for _, order := range orderBy {
		// Only generator is allowed to change the AST.
		// If we have to change the order by expression,
		// we have to build a new node.
		pushOrder := order
		var rb *route
		switch node := order.Expr.(type) {
		case *sqlparser.ColName:
			var isLocal bool
			var err error
			rb, isLocal, err = bldr.Symtab().Find(node, true)
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
			if num < 1 || num > int64(len(bldr.Symtab().Colsyms)) {
				return errors.New("order by column number out of range")
			}
			colsym := bldr.Symtab().Colsyms[num-1]
			rb = colsym.Route()
			// We have to recompute the column number.
			for num, s := range rb.Colsyms {
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
