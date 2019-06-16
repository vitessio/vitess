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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
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
	resultColumns []*resultColumn
	order         int
	extraDistinct *sqlparser.ColName
	input         *route
	eaggr         *engine.OrderedAggregate
}

// checkAggregates analyzes the select expression for aggregates. If it determines
// that a primitive is needed to handle the aggregation, it builds an orderedAggregate
// primitive and returns it. It returns a groupByHandler if there is aggregation it
// can handle.
func (pb *primitiveBuilder) checkAggregates(sel *sqlparser.Select) error {
	rb, isRoute := pb.bldr.(*route)
	if isRoute && rb.removeMultishardOptions() {
		return nil
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
		return nil
	}

	// The query has aggregates. We can proceed only
	// if the underlying primitive is a route because
	// we need the ability to push down group by and
	// order by clauses.
	if !isRoute {
		return errors.New("unsupported: cross-shard query with aggregates")
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
		success := rb.removeOptions(func(ro *routeOption) bool {
			for _, selectExpr := range sel.SelectExprs {
				switch selectExpr := selectExpr.(type) {
				case *sqlparser.AliasedExpr:
					vindex := ro.FindVindex(pb, selectExpr.Expr)
					if vindex != nil && vindex.IsUnique() {
						return true
					}
				}
			}
			return false
		})
		if success {
			return nil
		}
	}

	// The group by clause could also reference a unique vindex. The above
	// example could itself have been written as
	// 'select id, col from t group by id, col', or a query could be like
	// 'select id, count(*) from t group by id'. In the above cases,
	// the grouping can be done at the shard level, which allows the entire query
	// to be pushed down. In order to perform this analysis, we're going to look
	// ahead at the group by clause to see if it references a unique vindex.
	if pb.groupByHasUniqueVindex(sel, rb) {
		return nil
	}

	// We need an aggregator primitive.
	pb.bldr = &orderedAggregate{
		input: rb,
		eaggr: &engine.OrderedAggregate{},
	}
	pb.bldr.Reorder(0)
	return nil
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

// groupbyHasUniqueVindex looks ahead at the group by expression to see if
// it references a unique vindex.
//
// The vitess group by rules are different from MySQL because it's not possible
// to match the MySQL behavior without knowing the schema. For example:
// 'select id as val from t group by val' will have different interpretations
// under MySQL depending on whether t has a val column or not.
// In vitess, we always assume that 'val' references 'id'. This is achieved
// by the symbol table resolving against the select list before searching
// the tables.
//
// In order to look ahead, we have to overcome the chicken-and-egg problem:
// group by needs the select aliases to be built. Select aliases are built
// on push-down. But push-down decision depends on whether group by expressions
// reference a vindex.
// To overcome this, the look-ahead has to perform a search that matches
// the group by analyzer. The flow is similar to oa.PushGroupBy, except that
// we don't search the ResultColumns because they're not created yet. Also,
// error conditions are treated as no match for simplicity; They will be
// subsequently caught downstream.
func (pb *primitiveBuilder) groupByHasUniqueVindex(sel *sqlparser.Select, rb *route) bool {
	return rb.removeOptions(func(ro *routeOption) bool {
		for _, expr := range sel.GroupBy {
			var matchedExpr sqlparser.Expr
			switch node := expr.(type) {
			case *sqlparser.ColName:
				if expr := findAlias(node, sel.SelectExprs); expr != nil {
					matchedExpr = expr
				} else {
					matchedExpr = node
				}
			case *sqlparser.SQLVal:
				if node.Type != sqlparser.IntVal {
					continue
				}
				num, err := strconv.ParseInt(string(node.Val), 0, 64)
				if err != nil {
					continue
				}
				if num < 1 || num > int64(len(sel.SelectExprs)) {
					continue
				}
				expr, ok := sel.SelectExprs[num-1].(*sqlparser.AliasedExpr)
				if !ok {
					continue
				}
				matchedExpr = expr.Expr
			default:
				continue
			}
			vindex := ro.FindVindex(pb, matchedExpr)
			if vindex != nil && vindex.IsUnique() {
				return true
			}
		}
		return false
	})
}

func findAlias(colname *sqlparser.ColName, selects sqlparser.SelectExprs) sqlparser.Expr {
	// Qualified column names cannot match an (unqualified) alias.
	if !colname.Qualifier.IsEmpty() {
		return nil
	}
	// See if this references an alias.
	for _, selectExpr := range selects {
		selectExpr, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		if colname.Name.Equal(selectExpr.As) {
			return selectExpr.Expr
		}
	}
	return nil
}

// Order satisfies the builder interface.
func (oa *orderedAggregate) Order() int {
	return oa.order
}

// Reorder satisfies the builder interface.
func (oa *orderedAggregate) Reorder(order int) {
	oa.input.Reorder(order)
	oa.order = oa.input.Order() + 1
}

// Primitive satisfies the builder interface.
func (oa *orderedAggregate) Primitive() engine.Primitive {
	oa.eaggr.Input = oa.input.Primitive()
	return oa.eaggr
}

// First satisfies the builder interface.
func (oa *orderedAggregate) First() builder {
	return oa.input.First()
}

// ResultColumns satisfies the builder interface.
func (oa *orderedAggregate) ResultColumns() []*resultColumn {
	return oa.resultColumns
}

// PushFilter satisfies the builder interface.
func (oa *orderedAggregate) PushFilter(_ *primitiveBuilder, _ sqlparser.Expr, whereType string, _ builder) error {
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
// others. This functionality depends on the PushOrderBy to request that
// the rows be correctly ordered.
func (oa *orderedAggregate) PushSelect(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colNumber int, err error) {
	if inner, ok := expr.Expr.(*sqlparser.FuncExpr); ok {
		if _, ok := engine.SupportedAggregates[inner.Name.Lowered()]; ok {
			return oa.pushAggr(pb, expr, origin)
		}
	}

	// Ensure that there are no aggregates in the expression.
	if nodeHasAggregates(expr.Expr) {
		return nil, 0, errors.New("unsupported: in scatter query: complex aggregate expression")
	}

	innerRC, _, _ := oa.input.PushSelect(pb, expr, origin)
	oa.resultColumns = append(oa.resultColumns, innerRC)
	return innerRC, len(oa.resultColumns) - 1, nil
}

func (oa *orderedAggregate) pushAggr(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colNumber int, err error) {
	funcExpr := expr.Expr.(*sqlparser.FuncExpr)
	opcode := engine.SupportedAggregates[funcExpr.Name.Lowered()]
	if len(funcExpr.Exprs) != 1 {
		return nil, 0, fmt.Errorf("unsupported: only one expression allowed inside aggregates: %s", sqlparser.String(funcExpr))
	}
	var innerRC *resultColumn
	var innerCol int
	handleDistinct, innerAliased, err := oa.needDistinctHandling(pb, funcExpr, opcode)
	if err != nil {
		return nil, 0, err
	}
	if handleDistinct {
		if oa.extraDistinct != nil {
			return nil, 0, fmt.Errorf("unsupported: only one distinct aggregation allowed in a select: %s", sqlparser.String(funcExpr))
		}
		// Push the expression that's inside the aggregate.
		// The column will eventually get added to the group by and order by clauses.
		innerRC, innerCol, _ = oa.input.PushSelect(pb, innerAliased, origin)
		col, err := oa.input.BuildColName(innerCol)
		if err != nil {
			return nil, 0, err
		}
		oa.extraDistinct = col
		oa.eaggr.HasDistinct = true
		var alias string
		if expr.As.IsEmpty() {
			alias = sqlparser.String(expr.Expr)
		} else {
			alias = expr.As.String()
		}
		switch opcode {
		case engine.AggregateCount:
			opcode = engine.AggregateCountDistinct
		case engine.AggregateSum:
			opcode = engine.AggregateSumDistinct
		}
		oa.eaggr.Aggregates = append(oa.eaggr.Aggregates, engine.AggregateParams{
			Opcode: opcode,
			Col:    innerCol,
			Alias:  alias,
		})
	} else {
		innerRC, innerCol, _ = oa.input.PushSelect(pb, expr, origin)
		oa.eaggr.Aggregates = append(oa.eaggr.Aggregates, engine.AggregateParams{
			Opcode: opcode,
			Col:    innerCol,
		})
	}

	// Build a new rc with oa as origin because it's semantically different
	// from the expression we pushed down.
	rc = &resultColumn{alias: innerRC.alias, column: &column{origin: oa}}
	oa.resultColumns = append(oa.resultColumns, rc)
	return rc, len(oa.resultColumns) - 1, nil
}

// needDistinctHandling returns true if oa needs to handle the distinct clause.
// If true, it will also return the aliased expression that needs to be pushed
// down into the underlying route.
func (oa *orderedAggregate) needDistinctHandling(pb *primitiveBuilder, funcExpr *sqlparser.FuncExpr, opcode engine.AggregateOpcode) (bool, *sqlparser.AliasedExpr, error) {
	if !funcExpr.Distinct {
		return false, nil, nil
	}
	if opcode != engine.AggregateCount && opcode != engine.AggregateSum {
		return false, nil, nil
	}
	innerAliased, ok := funcExpr.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return false, nil, fmt.Errorf("syntax error: %s", sqlparser.String(funcExpr))
	}
	success := oa.input.removeOptions(func(ro *routeOption) bool {
		vindex := ro.FindVindex(pb, innerAliased.Expr)
		if vindex != nil && vindex.IsUnique() {
			return true
		}
		return false
	})
	return !success, innerAliased, nil
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

// PushGroupBy satisfies the builder interface.
func (oa *orderedAggregate) PushGroupBy(groupBy sqlparser.GroupBy) error {
	colNumber := -1
	for _, expr := range groupBy {
		switch node := expr.(type) {
		case *sqlparser.ColName:
			c := node.Metadata.(*column)
			if c.Origin() == oa {
				return fmt.Errorf("group by expression cannot reference an aggregate function: %v", sqlparser.String(node))
			}
			for i, rc := range oa.resultColumns {
				if rc.column == c {
					colNumber = i
					break
				}
			}
			if colNumber == -1 {
				return errors.New("unsupported: in scatter query: group by column must reference column in SELECT list")
			}
		case *sqlparser.SQLVal:
			num, err := ResultFromNumber(oa.resultColumns, node)
			if err != nil {
				return err
			}
			colNumber = num
		default:
			return errors.New("unsupported: in scatter query: only simple references allowed")
		}
		oa.eaggr.Keys = append(oa.eaggr.Keys, colNumber)
	}
	// Append the distinct aggregate if any.
	if oa.extraDistinct != nil {
		groupBy = append(groupBy, oa.extraDistinct)
	}

	_ = oa.input.PushGroupBy(groupBy)

	return nil
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
func (oa *orderedAggregate) PushOrderBy(orderBy sqlparser.OrderBy) (builder, error) {
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
		case *sqlparser.SQLVal:
			num, err := ResultFromNumber(oa.resultColumns, expr)
			if err != nil {
				return nil, err
			}
			orderByCol = oa.resultColumns[num].column
		case *sqlparser.ColName:
			orderByCol = expr.Metadata.(*column)
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
		col, err := oa.input.BuildColName(key)
		if err != nil {
			return nil, fmt.Errorf("generating order by clause: %v", err)
		}
		selOrderBy = append(selOrderBy, &sqlparser.Order{Expr: col, Direction: sqlparser.AscScr})
	}

	// Append the distinct aggregate if any.
	if oa.extraDistinct != nil {
		selOrderBy = append(selOrderBy, &sqlparser.Order{Expr: oa.extraDistinct, Direction: sqlparser.AscScr})
	}

	// Push down the order by.
	// It's ok to push the original AST down because all references
	// should point to the route. Only aggregate functions are originated
	// by oa, and we currently don't allow the ORDER BY to reference them.
	// TODO(sougou): PushOrderBy will return a mergeSort primitive, which
	// we should ideally replace oa.input with.
	_, err := oa.input.PushOrderBy(selOrderBy)
	if err != nil {
		return nil, err
	}
	if postSort {
		return newMemorySort(oa, orderBy)
	}
	return oa, nil
}

// SetUpperLimit satisfies the builder interface.
func (oa *orderedAggregate) SetUpperLimit(count *sqlparser.SQLVal) {
	oa.input.SetUpperLimit(count)
}

// PushMisc satisfies the builder interface.
func (oa *orderedAggregate) PushMisc(sel *sqlparser.Select) {
	oa.input.PushMisc(sel)
}

// Wireup satisfies the builder interface.
// If text columns are detected in the keys, then the function modifies
// the primitive to pull a corresponding weight_string from mysql and
// compare those instead. This is because we currently don't have the
// ability to mimic mysql's collation behavior.
func (oa *orderedAggregate) Wireup(bldr builder, jt *jointab) error {
	for i, colNumber := range oa.eaggr.Keys {
		if sqltypes.IsText(oa.resultColumns[colNumber].column.typ) {
			// len(oa.resultColumns) does not change. No harm using the value multiple times.
			oa.eaggr.TruncateColumnCount = len(oa.resultColumns)
			oa.eaggr.Keys[i] = oa.input.SupplyWeightString(colNumber)
		}
	}
	return oa.input.Wireup(bldr, jt)
}

// SupplyVar satisfies the builder interface.
func (oa *orderedAggregate) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("BUG: orderedAggregate should only have atomic nodes under it")
}

// SupplyCol satisfies the builder interface.
func (oa *orderedAggregate) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("BUG: nothing should depend on orderedAggregate")
}
