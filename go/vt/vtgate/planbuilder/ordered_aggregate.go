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
// route. Then this primitve pushes down the aggregation to all
// shards and forces the results to be ordered. This will allow
// for the engine code to merge sort and aggregate the results
// as they come.
// orderedAggregate relies on the values of eaggr, mainly Results
// and Keys. To understand this code, it will be important to keep
// in mind that the numbers in those fields are indexes into the
// underlying 'input' primitive.
type orderedAggregate struct {
	symtab        *symtab
	resultColumns []*resultColumn
	order         int
	input         *route
	eaggr         *engine.OrderedAggregate
}

// checkAggregates analyzes the select expresstion for aggregates. If it determines
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
	// to see if it has a unique vindex reference. The group by clause
	// could also have this property, but it cannot be analyzed until
	// the symbol table has been updated with select expressions.
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

	// The group by clause could also reference a unique vindex, but
	// it cannot be analyzed until the symbol table has been
	// updated with select expressions. In such cases, we'll live with
	// an orderedAggregate because it's not a very common use case.

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
				return false, errors.New("dummy")
			}
		case *sqlparser.GroupConcatExpr:
			hasAggregates = true
			return false, errors.New("dummy")
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
func (oa *orderedAggregate) PushSelect(expr *sqlparser.AliasedExpr, origin columnOriginator) (rc *resultColumn, colnum int, err error) {
	if inner, ok := expr.Expr.(*sqlparser.FuncExpr); ok {
		if opcode, ok := engine.SupportedAggregates[inner.Name.Lowered()]; ok {
			innerRC, innerCol, _ := oa.input.PushSelect(expr, origin)

			// Add to Aggregates.
			oa.eaggr.Aggregates = append(oa.eaggr.Aggregates, engine.AggregateParams{
				Opcode: opcode,
				Col:    innerCol,
			})

			// Add to Results.
			// Build a new rc with oa as origin because it's semantically different
			// from the expression we pushed down.
			rc := &resultColumn{alias: innerRC.alias, column: &column{origin: oa}}
			oa.resultColumns = append(oa.resultColumns, rc)
			// Index should be made negative to indicate aggregate function.
			oa.eaggr.Results = append(oa.eaggr.Results, -len(oa.eaggr.Aggregates))
			return rc, len(oa.resultColumns) - 1, nil
		}
	}

	// Ensure that there are no aggregates in the expression.
	if nodeHasAggregates(expr.Expr) {
		return nil, 0, errors.New("unsupported: in scatter query: complex aggregate expression")
	}

	innerRC, innerCol, _ := oa.input.PushSelect(expr, origin)
	oa.resultColumns = append(oa.resultColumns, innerRC)
	oa.eaggr.Results = append(oa.eaggr.Results, innerCol)
	return innerRC, len(oa.resultColumns) - 1, nil
}

func (oa *orderedAggregate) MakeDistinct() error {
	for i := range oa.resultColumns {
		if oa.eaggr.Results[i] < 0 {
			return errors.New("unsupported: distinct cannot be combined with aggregate functions")
		}
		oa.eaggr.Keys = append(oa.eaggr.Keys, engine.AggregateKey{Col: oa.eaggr.Results[i]})
	}
	return oa.input.MakeDistinct()
}

// SetGroupBy satisfies the builder interface.
func (oa *orderedAggregate) SetGroupBy(groupBy sqlparser.GroupBy) error {
outer:
	for _, expr := range groupBy {
		switch node := expr.(type) {
		case *sqlparser.ColName:
			c := node.Metadata.(*column)
			for i, rc := range oa.resultColumns {
				if rc.column == c {
					if oa.eaggr.Results[i] < 0 {
						return fmt.Errorf("group by expression cannot reference an aggregate function: %v", sqlparser.String(node))
					}
					oa.eaggr.Keys = append(oa.eaggr.Keys, engine.AggregateKey{Col: oa.eaggr.Results[i]})
					continue outer
				}
			}
			return errors.New("unsupported: in scatter query: group by column must reference column in SELECT list")
		case *sqlparser.SQLVal:
			num, err := oa.Symtab().ResultFromNumber(node)
			if err != nil {
				return err
			}
			oa.eaggr.Keys = append(oa.eaggr.Keys, engine.AggregateKey{Col: oa.eaggr.Results[num]})
		default:
			return errors.New("unsupported: in scatter query: only simple references allowed")
		}
	}

	return oa.input.SetGroupBy(groupBy)
}

// PushOrderBy satisfies the builder interface.
func (oa *orderedAggregate) PushOrderBy(orderBy sqlparser.OrderBy) error {
	// Treat order by null as nil order by.
	if len(orderBy) == 1 {
		if _, ok := orderBy[0].Expr.(*sqlparser.NullVal); ok {
			orderBy = nil
		}
	}

	// If no order by was specified, we specify the ordering to be
	// the same as the group by keys.
	if orderBy == nil {
		if len(oa.eaggr.Keys) == 0 {
			return nil
		}
		for _, key := range oa.eaggr.Keys {
			// Since the aggregation can be caused by a group by or distinct,
			// it will be confusing to reuse those AST elements. It's also
			// possible that those references are ambiguous. So, it's better
			// to build brand new non-ambiguous AST elements that are safe
			// to push down. See the test cases that shows how BuildColName
			// can fail.
			col, err := oa.input.BuildColName(key.Col)
			if err != nil {
				return fmt.Errorf("generating order by clause: %v", err)
			}
			order := &sqlparser.Order{Expr: col, Direction: sqlparser.AscScr}
			oa.input.PushOrderBy(order)
		}
		return nil
	}

	// An order by was specified. Reorder the group by keys to match
	// the specified ordering. This requires that the order by columns
	// be present in the group by list also.
	for i, order := range orderBy {
		// Identify the order by column.
		var orderByCol *column
		switch expr := order.Expr.(type) {
		case *sqlparser.SQLVal:
			num, err := oa.Symtab().ResultFromNumber(expr)
			if err != nil {
				return fmt.Errorf("in order by: %v", err)
			}
			inputCol := oa.eaggr.Results[num]
			if inputCol < 0 {
				return fmt.Errorf("unsupported: in scatter query: order by expression cannot reference an aggregate function: %v", sqlparser.String(expr))
			}
			orderByCol = oa.input.ResultColumns()[inputCol].column
		case *sqlparser.ColName:
			_, _, err := oa.Symtab().Find(expr)
			if err != nil {
				return fmt.Errorf("in order by: %v", err)
			}
			orderByCol = expr.Metadata.(*column)
		default:
			return fmt.Errorf("unsupported: in scatter query: complex order by expression: %v", sqlparser.String(expr))
		}

		// Match orderByCol against the group by columns
		// and reorder the group by keys accordingly. This
		// will ensure that the merge-sort performed by vtgate
		// will follow the same order as the input results.
		found := false
		for j := i; j < len(oa.eaggr.Keys); j++ {
			key := oa.eaggr.Keys[j]
			inputForKey := oa.input.ResultColumns()[key.Col]
			if inputForKey.column != orderByCol {
				continue
			}

			found = true
			for k := i; k <= j; k++ {
				key, oa.eaggr.Keys[k] = oa.eaggr.Keys[k], key
			}
			break
		}
		if !found {
			return fmt.Errorf("unsupported: in scatter query: order by column must reference group by expression: %v", sqlparser.String(order))
		}

		// At this point, we're guaranteed that Keys[i] represents
		// the same column as the order by expression.
		if order.Direction == sqlparser.DescScr {
			oa.eaggr.Keys[i].Desc = true
		}

		// Push down the order by.
		// It's ok to push the original AST down because all references
		// should point to the route. Only aggregate functions are originated
		// by oa, and we currently don't allow the ORDER BY to reference them.
		oa.input.PushOrderBy(order)
	}
	return nil
}

// PushOrderByNull satisfies the builder interface.
func (oa *orderedAggregate) PushOrderByNull() {
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
