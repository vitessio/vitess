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
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

var _ builder = (*orderedAggregate)(nil)

// orderedAggregate is the builder for engine.OrderedAggregate.
// This gets built if there are aggregations on a SelectScatter
// route. The primitive requests the underlying route to order
// the results by the grouping columns. This will allow the
// engine code to aggregate the results as they come.
// For example: 'select col1, col2, count(*) from t group by col1, col2'
// will be sent to the scatter route as:
// 'select col1, col2, count(*) from t group by col1, col2 order by col1, col2`
// The orderAggregate primitive built for this will be:
//    &engine.OrderedAggregate {
//      // Aggregates has one column. It computes the count
//      // using column 2 of the underlying route.
//      Aggregates: []AggregateParams{{
//        Opcode: AggregateCount,
//        Col: 2,
//      }},
//
//      // Keys has the two group by values for col1 and col2.
//      // The column numbers are from the underlying route.
//      // These values will be used to perform the grouping
//      // of the ordered results as they come from the underlying
//      // route.
//      Keys: []int{0, 1},
//      Input: (Scatter Route with the order by request),
//    }
type orderedAggregate struct {
	symtab        *symtab
	resultColumns []*resultColumn
	order         int
	input         *route
	eaggr         *engine.OrderedAggregate
}

// checkAggregates analyzes the select expression for aggregates. If it determines
// that a primitive is needed to handle the aggregation, it builds an orderedAggregate
// primitive and returns it.
func checkAggregates(sel *sqlparser.Select, bldr builder) (builder, error) {
	rb, isRoute := bldr.(*route)
	if isRoute && rb.IsSingle() {
		return bldr, nil
	}

	// Check if we can allow aggregates.
	hasAggregates := false
	if sel.Distinct != "" {
		hasAggregates = true
	} else {
		hasAggregates = nodeHasAggregates(sel.SelectExprs)
	}
	if len(sel.GroupBy) > 0 {
		hasAggregates = true
	}
	if !hasAggregates {
		return bldr, nil
	}
	if hasAggregates && !isRoute {
		return bldr, errors.New("unsupported: cross-shard query with aggregates")
	}

	// If there is a distinct clause, we can check the select list
	// to see if it has a unique vindex reference. For example,
	// if the query was 'select distinct id, col from t' (with id
	// as a unique vindex), then the distinct operation can be
	// safely pushed down because the unique vindex guarantees
	// that each id can only be in a single shard. Without the
	// unique vindex property, the id could come from multiple
	// shards, which will require us to perform the grouping
	// at the vtgate level.
	if sel.Distinct != "" {
		for _, selectExpr := range sel.SelectExprs {
			switch selectExpr := selectExpr.(type) {
			case *sqlparser.AliasedExpr:
				vindex := bldr.Symtab().Vindex(selectExpr.Expr, rb)
				if vindex != nil && vindexes.IsUnique(vindex) {
					return bldr, nil
				}
			}
		}
	}

	// The group by clause could also reference a unique vindex. The above
	// example could itself have been written as
	// 'select id, col from t group by id, col', or a query could be like
	// 'select id, count(*) from t group by id'. In the above cases,
	// the grouping can be done at the shard level, which allows the entire query
	// to be pushed down. However, we cannot analyze group by clauses here
	// because it can only be done after the symbol table has been updated
	// with select expressions.
	// For the sake of simplicity, we won't perform this optimization because
	// these use cases are rare.

	// We need an aggregator primitive.
	return &orderedAggregate{
		symtab: rb.Symtab(),
		order:  rb.Order() + 1,
		input:  rb,
		eaggr:  &engine.OrderedAggregate{},
	}, nil
}

func nodeHasAggregates(node sqlparser.SQLNode) bool {
	hasAggregates := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.FuncExpr:
			if node.IsAggregate() {
				hasAggregates = true
				return false, errors.New("unused error")
			}
		case *sqlparser.GroupConcatExpr:
			hasAggregates = true
			return false, errors.New("unused error")
		case *sqlparser.Subquery:
			// Subqueries are analyzed by themselves.
			return false, nil
		}
		return true, nil
	}, node)
	return hasAggregates
}

// Symtab satisfies the builder interface.
func (oa *orderedAggregate) Symtab() *symtab {
	return oa.symtab
}

// MaxOrder satisfies the builder interface.
func (oa *orderedAggregate) MaxOrder() int {
	return oa.order
}

// Order returns the order.
func (oa *orderedAggregate) Order() int {
	return oa.order
}

// SetOrder satisfies the builder interface.
func (oa *orderedAggregate) SetOrder(order int) {
	panic("BUG: reordering can only happen within the FROM clause")
}

// Primitive satisfies the builder interface.
func (oa *orderedAggregate) Primitive() engine.Primitive {
	oa.eaggr.Input = oa.input.Primitive()
	return oa.eaggr
}

// Leftmost satisfies the builder interface.
func (oa *orderedAggregate) Leftmost() columnOriginator {
	return oa.input.Leftmost()
}

// ResultColumns satisfies the builder interface.
func (oa *orderedAggregate) ResultColumns() []*resultColumn {
	return oa.resultColumns
}

// PushFilter satisfies the builder interface.
func (oa *orderedAggregate) PushFilter(_ sqlparser.Expr, whereType string, _ columnOriginator) error {
	return errors.New("unsupported: filtering on results of aggregates")
}

// PushSelect satisfies the builder interface.
// oa can accept expressions that are normal (a+b), or aggregate (MAX(v)).
// Normal expressions are pushed through to the underlying route. But aggregate
// expressions require post-processing. In such cases, oa shares the work with
// the underlying route: It asks the scatter route to perform the MAX operation
// also, and only performs the final aggregation with what the route returns.
// Since the results are expected to be ordered, this is something that can
// be performed 'as they come'. In this respect, oa is the originator for
// aggregate expressions like MAX, which will be added to symtab. The underlying
// MAX sent to the route will not be added to symtab and will not be reachable by
// others. This functionality depends on the the PushOrderBy to request that
// the rows be correctly ordered for a merge sort.
func (oa *orderedAggregate) PushSelect(expr *sqlparser.AliasedExpr, origin columnOriginator) (rc *resultColumn, colnum int, err error) {
	if inner, ok := expr.Expr.(*sqlparser.FuncExpr); ok {
		if opcode, ok := engine.SupportedAggregates[inner.Name.Lowered()]; ok {
			innerRC, innerCol, _ := oa.input.PushSelect(expr, origin)

			// Add to Aggregates.
			oa.eaggr.Aggregates = append(oa.eaggr.Aggregates, engine.AggregateParams{
				Opcode: opcode,
				Col:    innerCol,
			})

			// Build a new rc with oa as origin because it's semantically different
			// from the expression we pushed down.
			rc := &resultColumn{alias: innerRC.alias, column: &column{origin: oa}}
			oa.resultColumns = append(oa.resultColumns, rc)
			return rc, len(oa.resultColumns) - 1, nil
		}
	}

	// Ensure that there are no aggregates in the expression.
	if nodeHasAggregates(expr.Expr) {
		return nil, 0, errors.New("unsupported: in scatter query: complex aggregate expression")
	}

	innerRC, _, _ := oa.input.PushSelect(expr, origin)
	oa.resultColumns = append(oa.resultColumns, innerRC)
	return innerRC, len(oa.resultColumns) - 1, nil
}

func (oa *orderedAggregate) MakeDistinct() error {
	for i, rc := range oa.resultColumns {
		// If the column origin is oa (and not the underlying route),
		// it means that it's an aggregate function supplied by oa.
		// So, the distinct 'operator' cannot be pushed down into the
		// route.
		if rc.column.Origin() == oa {
			return errors.New("unsupported: distinct cannot be combined with aggregate functions")
		}
		oa.eaggr.Keys = append(oa.eaggr.Keys, i)
	}
	return oa.input.MakeDistinct()
}

// SetGroupBy satisfies the builder interface.
func (oa *orderedAggregate) SetGroupBy(groupBy sqlparser.GroupBy) (builder, error) {
	colnum := -1
	for _, expr := range groupBy {
		switch node := expr.(type) {
		case *sqlparser.ColName:
			c := node.Metadata.(*column)
			if c.Origin() == oa {
				return nil, fmt.Errorf("group by expression cannot reference an aggregate function: %v", sqlparser.String(node))
			}
			for i, rc := range oa.resultColumns {
				if rc.column == c {
					colnum = i
					break
				}
			}
			if colnum == -1 {
				return nil, errors.New("unsupported: in scatter query: group by column must reference column in SELECT list")
			}
		case *sqlparser.SQLVal:
			num, err := ResultFromNumber(oa.resultColumns, node)
			if err != nil {
				return nil, err
			}
			colnum = num
		default:
			return nil, errors.New("unsupported: in scatter query: only simple references allowed")
		}
		if vindexes.IsUnique(oa.resultColumns[colnum].column.Vindex) {
			oa.setDefunct()
			return oa.input.SetGroupBy(groupBy)
		}
		oa.eaggr.Keys = append(oa.eaggr.Keys, colnum)
	}

	_, _ = oa.input.SetGroupBy(groupBy)
	return oa, nil
}

// setDefunct replaces the column references originated by
// oa into the ones of the underlying route, effectively
// removing itself as an originator. Because resultColumns
// are shared objects, this change equally affects the ones
// in symtab. So, the change is global. All future resolves
// will yield the column originated by the underlying route.
func (oa *orderedAggregate) setDefunct() {
	for i, rc := range oa.resultColumns {
		rc.column = oa.input.ResultColumns()[i].column
	}
}

// PushOrderBy pushes the order by expression into the primitive.
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
func (oa *orderedAggregate) PushOrderBy(orderBy sqlparser.OrderBy) error {
	// Treat order by null as nil order by.
	if len(orderBy) == 1 {
		if _, ok := orderBy[0].Expr.(*sqlparser.NullVal); ok {
			orderBy = nil
		}
	}

	// referenced tracks the keys referenced by the order by clause.
	referenced := make([]bool, len(oa.eaggr.Keys))
	for _, order := range orderBy {
		// Identify the order by column.
		var orderByCol *column
		switch expr := order.Expr.(type) {
		case *sqlparser.SQLVal:
			num, err := ResultFromNumber(oa.resultColumns, expr)
			if err != nil {
				return fmt.Errorf("invalid order by: %v", err)
			}
			orderByCol = oa.input.ResultColumns()[num].column
		case *sqlparser.ColName:
			_, _, err := oa.Symtab().Find(expr)
			if err != nil {
				return fmt.Errorf("invalid order by: %v", err)
			}
			orderByCol = expr.Metadata.(*column)
		default:
			return fmt.Errorf("unsupported: in scatter query: complex order by expression: %v", sqlparser.String(expr))
		}

		// Match orderByCol against the group by columns.
		found := false
		for j, key := range oa.eaggr.Keys {
			inputForKey := oa.input.ResultColumns()[key]
			if inputForKey.column != orderByCol {
				continue
			}

			found = true
			referenced[j] = true
			break
		}
		if !found {
			return fmt.Errorf("unsupported: in scatter query: order by column must reference group by expression: %v", sqlparser.String(order))
		}

		// Push down the order by.
		// It's ok to push the original AST down because all references
		// should point to the route. Only aggregate functions are originated
		// by oa, and we currently don't allow the ORDER BY to reference them.
		oa.input.PushOrderBy(order)
	}

	// Append any unreferenced keys at the end of the order by.
	for i, key := range oa.eaggr.Keys {
		if referenced[i] {
			continue
		}
		// Build a brand new reference for the key.
		col, err := oa.input.BuildColName(key)
		if err != nil {
			return fmt.Errorf("generating order by clause: %v", err)
		}
		order := &sqlparser.Order{Expr: col, Direction: sqlparser.AscScr}
		oa.input.PushOrderBy(order)
	}
	return nil
}

// PushOrderByNull satisfies the builder interface.
func (oa *orderedAggregate) PushOrderByNull() {
	panic("BUG: unreachable")
}

// SetUpperLimit satisfies the builder interface.
func (oa *orderedAggregate) SetUpperLimit(count interface{}) {
	oa.input.SetUpperLimit(count)
}

// PushMisc satisfies the builder interface.
func (oa *orderedAggregate) PushMisc(sel *sqlparser.Select) {
	oa.input.PushMisc(sel)
}

// Wireup satisfies the builder interface.
func (oa *orderedAggregate) Wireup(bldr builder, jt *jointab) error {
	return oa.input.Wireup(bldr, jt)
}

// SupplyVar satisfies the builder interface.
func (oa *orderedAggregate) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("BUG: orderedAggregate should only have atomic nodes under it")
}

// SupplyCol satisfies the builder interface.
func (oa *orderedAggregate) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colnum int) {
	panic("BUG: nothing should depend on orderedAggregate")
}
