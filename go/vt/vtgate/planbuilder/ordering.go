/*
Copyright 2020 The Vitess Authors.
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
	"fmt"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func planOrdering(pb *primitiveBuilder, input logicalPlan, orderBy sqlparser.OrderBy) (logicalPlan, error) {
	switch node := input.(type) {
	case *subquery, *vindexFunc:
		if len(orderBy) == 0 {
			return node, nil
		}
		return newMemorySort(node, orderBy)
	case *distinct:
		// TODO: this is weird, but needed
		newInput, err := planOrdering(pb, node.input, orderBy)
		node.input = newInput
		return node, err
	case *pulloutSubquery:
		plan, err := planOrdering(pb, node.underlying, orderBy)
		if err != nil {
			return nil, err
		}
		node.underlying = plan
		return node, nil
	case *route:
		return planRouteOrdering(orderBy, node)
	case *join:
		return planJoinOrdering(pb, orderBy, node)
	case *orderedAggregate:
		return planOAOrdering(pb, orderBy, node)
	case *mergeSort:
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "can't do ORDER BY on top of ORDER BY")
	}
	if orderBy == nil {
		return input, nil
	}
	return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "[BUG] unreachable %T.ordering", input)
}

func planOAOrdering(pb *primitiveBuilder, orderBy sqlparser.OrderBy, oa *orderedAggregate) (logicalPlan, error) {
	// The requested order must be such that the ordering can be done
	// before the group by, which will allow us to push it down to the
	// route. This is actually true in most use cases, except for situations
	// where ordering is requested on values of an aggregate result.
	// Such constructs will need to be handled by a separate 'Sorter'
	// primitive, after aggregation is done. For example, the following
	// constructs are allowed:
	// 'select a, b, count(*) from t group by a, b order by a desc, b asc'
	// 'select a, b, count(*) from t group by a, b order by b'
	// The following construct is not allowed:
	// 'select a, count(*) from t group by a order by count(*)'
	// Treat order by null as nil order by.
	if len(orderBy) == 1 {
		if _, ok := orderBy[0].Expr.(*sqlparser.NullVal); ok {
			orderBy = nil
		}
	}

	// referenced tracks the keys referenced by the order by clause.
	referenced := make([]bool, len(oa.eaggr.Keys))
	postSort := false
	selOrderBy := make(sqlparser.OrderBy, 0, len(orderBy))
	for _, order := range orderBy {
		// Identify the order by column.
		var orderByCol *column
		switch expr := order.Expr.(type) {
		case *sqlparser.Literal:
			num, err := ResultFromNumber(oa.resultColumns, expr)
			if err != nil {
				return nil, err
			}
			orderByCol = oa.resultColumns[num].column
		case *sqlparser.ColName:
			orderByCol = expr.Metadata.(*column)
		case *sqlparser.UnaryExpr:
			col, ok := expr.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("unsupported: in scatter query: complex order by expression: %s", sqlparser.String(expr))
			}
			orderByCol = col.Metadata.(*column)
		default:
			return nil, fmt.Errorf("unsupported: in scatter query: complex order by expression: %v", sqlparser.String(expr))
		}

		// Match orderByCol against the group by columns.
		found := false
		for j, key := range oa.eaggr.Keys {
			if oa.resultColumns[key].column != orderByCol {
				continue
			}

			found = true
			referenced[j] = true
			selOrderBy = append(selOrderBy, order)
			break
		}
		if !found {
			postSort = true
		}
	}

	// Append any unreferenced keys at the end of the order by.
	for i, key := range oa.eaggr.Keys {
		if referenced[i] {
			continue
		}
		// Build a brand new reference for the key.
		col, err := BuildColName(oa.input.ResultColumns(), key)
		if err != nil {
			return nil, vterrors.Wrapf(err, "generating order by clause")
		}
		selOrderBy = append(selOrderBy, &sqlparser.Order{Expr: col, Direction: sqlparser.AscOrder})
	}

	// Append the distinct aggregate if any.
	if oa.extraDistinct != nil {
		selOrderBy = append(selOrderBy, &sqlparser.Order{Expr: oa.extraDistinct, Direction: sqlparser.AscOrder})
	}

	// Push down the order by.
	// It's ok to push the original AST down because all references
	// should point to the route. Only aggregate functions are originated
	// by node, and we currently don't allow the ORDER BY to reference them.
	plan, err := planOrdering(pb, oa.input, selOrderBy)
	if err != nil {
		return nil, err
	}
	oa.input = plan
	if postSort {
		return newMemorySort(oa, orderBy)
	}
	return oa, nil
}

func planJoinOrdering(pb *primitiveBuilder, orderBy sqlparser.OrderBy, node *join) (logicalPlan, error) {
	isSpecial := false
	switch len(orderBy) {
	case 0:
		isSpecial = true
	case 1:
		if _, ok := orderBy[0].Expr.(*sqlparser.NullVal); ok {
			isSpecial = true
		} else if f, ok := orderBy[0].Expr.(*sqlparser.FuncExpr); ok {
			if f.Name.Lowered() == "rand" {
				isSpecial = true
			}
		}
	}
	if isSpecial {
		l, err := planOrdering(pb, node.Left, orderBy)
		if err != nil {
			return nil, err
		}
		node.Left = l
		r, err := planOrdering(pb, node.Right, orderBy)
		if err != nil {
			return nil, err
		}
		node.Right = r
		return node, nil
	}

	for _, order := range orderBy {
		if e, ok := order.Expr.(*sqlparser.Literal); ok {
			// This block handles constructs that use ordinals for 'ORDER BY'. For example:
			// SELECT a, b, c FROM t1, t2 ORDER BY 1, 2, 3.
			num, err := ResultFromNumber(node.ResultColumns(), e)
			if err != nil {
				return nil, err
			}
			if node.ResultColumns()[num].column.Origin().Order() > node.Left.Order() {
				return newMemorySort(node, orderBy)
			}
		} else {
			// Analyze column references within the expression to make sure they all
			// go to the left.
			err := sqlparser.Walk(func(in sqlparser.SQLNode) (kontinue bool, err error) {
				switch e := in.(type) {
				case *sqlparser.ColName:
					if e.Metadata.(*column).Origin().Order() > node.Left.Order() {
						return false, vterrors.New(vtrpc.Code_UNIMPLEMENTED, "unsupported: order by spans across shards")
					}
				case *sqlparser.Subquery:
					// Unreachable because ResolveSymbols perfoms this check up above.
					return false, vterrors.New(vtrpc.Code_UNIMPLEMENTED, "unsupported: order by has subquery")
				}
				return true, nil
			}, order.Expr)
			if err != nil {
				return newMemorySort(node, orderBy)
			}
		}
	}

	// There were no errors. We can push the order by to the left-most route.
	l, err := planOrdering(pb, node.Left, orderBy)
	if err != nil {
		return nil, err
	}
	node.Left = l
	// Still need to push an empty order by to the right.
	r, err := planOrdering(pb, node.Right, nil)
	if err != nil {
		return nil, err
	}
	node.Right = r
	return node, nil
}

func planRouteOrdering(orderBy sqlparser.OrderBy, node *route) (logicalPlan, error) {
	switch len(orderBy) {
	case 0:
		return node, nil
	case 1:
		isSpecial := false
		if _, ok := orderBy[0].Expr.(*sqlparser.NullVal); ok {
			isSpecial = true
		} else if f, ok := orderBy[0].Expr.(*sqlparser.FuncExpr); ok {
			if f.Name.Lowered() == "rand" {
				isSpecial = true
			}
		}
		if isSpecial {
			node.Select.AddOrder(orderBy[0])
			return node, nil
		}
	}

	if node.isSingleShard() {
		for _, order := range orderBy {
			node.Select.AddOrder(order)
		}
		return node, nil
	}

	// If it's a scatter, we have to populate the OrderBy field.
	for _, order := range orderBy {
		colNumber := -1
		switch expr := order.Expr.(type) {
		case *sqlparser.Literal:
			var err error
			if colNumber, err = ResultFromNumber(node.resultColumns, expr); err != nil {
				return nil, err
			}
		case *sqlparser.ColName:
			c := expr.Metadata.(*column)
			for i, rc := range node.resultColumns {
				if rc.column == c {
					colNumber = i
					break
				}
			}
		case *sqlparser.UnaryExpr:
			col, ok := expr.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("unsupported: in scatter query: complex order by expression: %s", sqlparser.String(expr))
			}
			c := col.Metadata.(*column)
			for i, rc := range node.resultColumns {
				if rc.column == c {
					colNumber = i
					break
				}
			}
		default:
			return nil, fmt.Errorf("unsupported: in scatter query: complex order by expression: %s", sqlparser.String(expr))
		}
		// If column is not found, then the order by is referencing
		// a column that's not on the select list.
		if colNumber == -1 {
			return nil, fmt.Errorf("unsupported: in scatter query: order by must reference a column in the select list: %s", sqlparser.String(order))
		}
		ob := engine.OrderbyParams{
			Col:             colNumber,
			WeightStringCol: -1,
			Desc:            order.Direction == sqlparser.DescOrder,
		}
		node.eroute.OrderBy = append(node.eroute.OrderBy, ob)

		node.Select.AddOrder(order)
	}
	return newMergeSort(node), nil
}
