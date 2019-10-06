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
	"fmt"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ builder = (*route)(nil)

// route is used to build a Route primitive.
// It's used to build one of the Select routes like
// SelectScatter, etc. Portions of the original Select AST
// are moved into this node, which will be used to build
// the final SQL for this route.
// A route can have multiple routeOptions. They are kept
// up-to-date as the route improves. Those that don't
// qualify are continuously removed from the options.
// A single best route is chosen before the Wireup phase.
type route struct {
	order int

	// Redirect may point to another route if this route
	// was merged with it. The Resolve function chases
	// this pointer till the last un-redirected route.
	Redirect *route

	// Select is the AST for the query fragment that will be
	// executed by this route.
	Select sqlparser.SelectStatement

	// resultColumns represent the columns returned by this route.
	resultColumns []*resultColumn

	// weight_string keeps track of the weight_string expressions
	// that were added additionally for each column. These expressions
	// are added to be used for collation of text columns.
	weightStrings map[*resultColumn]int

	routeOptions []*routeOption
}

func newRoute(stmt sqlparser.SelectStatement) (*route, *symtab) {
	rb := &route{
		Select:        stmt,
		order:         1,
		weightStrings: make(map[*resultColumn]int),
	}
	return rb, newSymtabWithRoute(rb)
}

// Resolve resolves redirects, and returns the last
// un-redirected route.
func (rb *route) Resolve() *route {
	for rb.Redirect != nil {
		rb = rb.Redirect
	}
	return rb
}

// Order satisfies the builder interface.
func (rb *route) Order() int {
	return rb.order
}

// Reorder satisfies the builder interface.
func (rb *route) Reorder(order int) {
	rb.order = order + 1
}

// Primitive satisfies the builder interface.
func (rb *route) Primitive() engine.Primitive {
	return rb.routeOptions[0].eroute
}

// First satisfies the builder interface.
func (rb *route) First() builder {
	return rb
}

// ResultColumns satisfies the builder interface.
func (rb *route) ResultColumns() []*resultColumn {
	return rb.resultColumns
}

// PushFilter satisfies the builder interface.
// The primitive will be updated if the new filter improves the plan.
func (rb *route) PushFilter(pb *primitiveBuilder, filter sqlparser.Expr, whereType string, _ builder) error {
	sel := rb.Select.(*sqlparser.Select)
	switch whereType {
	case sqlparser.WhereStr:
		sel.AddWhere(filter)
	case sqlparser.HavingStr:
		sel.AddHaving(filter)
	}
	rb.UpdatePlans(pb, filter)
	return nil
}

func (rb *route) UpdatePlans(pb *primitiveBuilder, filter sqlparser.Expr) {
	for _, ro := range rb.routeOptions {
		ro.UpdatePlan(pb, filter)
	}
}

// PushSelect satisfies the builder interface.
func (rb *route) PushSelect(_ *primitiveBuilder, expr *sqlparser.AliasedExpr, _ builder) (rc *resultColumn, colNumber int, err error) {
	sel := rb.Select.(*sqlparser.Select)
	sel.SelectExprs = append(sel.SelectExprs, expr)

	rc = newResultColumn(expr, rb)
	rb.resultColumns = append(rb.resultColumns, rc)

	return rc, len(rb.resultColumns) - 1, nil
}

// PushAnonymous pushes an anonymous expression like '*' or NEXT VALUES
// into the select expression list of the route. This function is
// similar to PushSelect.
func (rb *route) PushAnonymous(expr sqlparser.SelectExpr) *resultColumn {
	sel := rb.Select.(*sqlparser.Select)
	sel.SelectExprs = append(sel.SelectExprs, expr)

	// We just create a place-holder resultColumn. It won't
	// match anything.
	rc := &resultColumn{column: &column{origin: rb}}
	rb.resultColumns = append(rb.resultColumns, rc)

	return rc
}

// MakeDistinct satisfies the builder interface.
func (rb *route) MakeDistinct() error {
	rb.Select.(*sqlparser.Select).Distinct = sqlparser.DistinctStr
	return nil
}

// PushGroupBy satisfies the builder interface.
func (rb *route) PushGroupBy(groupBy sqlparser.GroupBy) error {
	rb.Select.(*sqlparser.Select).GroupBy = groupBy
	return nil
}

// PushOrderBy satisfies the builder interface.
func (rb *route) PushOrderBy(orderBy sqlparser.OrderBy) (builder, error) {
	switch len(orderBy) {
	case 0:
		return rb, nil
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
			rb.Select.AddOrder(orderBy[0])
			return rb, nil
		}
	}

	if rb.removeMultishardOptions() {
		for _, order := range orderBy {
			rb.Select.AddOrder(order)
		}
		return rb, nil
	}

	// If it's a scatter, we have to populate the OrderBy field.
	for _, order := range orderBy {
		colNumber := -1
		switch expr := order.Expr.(type) {
		case *sqlparser.SQLVal:
			var err error
			if colNumber, err = ResultFromNumber(rb.resultColumns, expr); err != nil {
				return nil, err
			}
		case *sqlparser.ColName:
			c := expr.Metadata.(*column)
			for i, rc := range rb.resultColumns {
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
			Col:  colNumber,
			Desc: order.Direction == sqlparser.DescScr,
		}
		for _, ro := range rb.routeOptions {
			ro.eroute.OrderBy = append(ro.eroute.OrderBy, ob)
		}

		rb.Select.AddOrder(order)
	}
	return newMergeSort(rb), nil
}

// SetLimit adds a LIMIT clause to the route.
func (rb *route) SetLimit(limit *sqlparser.Limit) {
	rb.Select.SetLimit(limit)
}

// SetUpperLimit satisfies the builder interface.
// The route pushes the limit regardless of the plan.
// If it's a scatter query, the rows returned will be
// more than the upper limit, but enough for the limit
// primitive to chop off where needed.
func (rb *route) SetUpperLimit(count *sqlparser.SQLVal) {
	rb.Select.SetLimit(&sqlparser.Limit{Rowcount: count})
}

// PushMisc satisfies the builder interface.
func (rb *route) PushMisc(sel *sqlparser.Select) {
	rb.Select.(*sqlparser.Select).Comments = sel.Comments
	rb.Select.(*sqlparser.Select).Lock = sel.Lock
}

// Wireup satisfies the builder interface.
func (rb *route) Wireup(bldr builder, jt *jointab) error {
	rb.finalizeOptions()

	// Precaution: update ERoute.Values only if it's not set already.
	ro := rb.routeOptions[0]
	if ro.eroute.Values == nil {
		// Resolve values stored in the builder.
		switch vals := ro.condition.(type) {
		case *sqlparser.ComparisonExpr:
			pv, err := rb.procureValues(bldr, jt, vals.Right)
			if err != nil {
				return err
			}
			ro.eroute.Values = []sqltypes.PlanValue{pv}
			vals.Right = sqlparser.ListArg("::" + engine.ListVarName)
		case nil:
			// no-op.
		default:
			pv, err := rb.procureValues(bldr, jt, vals)
			if err != nil {
				return err
			}
			ro.eroute.Values = []sqltypes.PlanValue{pv}
		}
	}

	// Fix up the AST.
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.Select:
			if len(node.SelectExprs) == 0 {
				node.SelectExprs = sqlparser.SelectExprs([]sqlparser.SelectExpr{
					&sqlparser.AliasedExpr{
						Expr: sqlparser.NewIntVal([]byte{'1'}),
					},
				})
			}
		case *sqlparser.ComparisonExpr:
			if node.Operator == sqlparser.EqualStr {
				if ro.exprIsValue(node.Left) && !ro.exprIsValue(node.Right) {
					node.Left, node.Right = node.Right, node.Left
				}
			}
		}
		return true, nil
	}, rb.Select)

	// Substitute table names
	for _, sub := range ro.substitutions {
		*sub.oldExpr = *sub.newExpr
	}

	// Generate query while simultaneously resolving values.
	varFormatter := func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			if !rb.isLocal(node) {
				joinVar := jt.Procure(bldr, node, rb.Order())
				buf.Myprintf("%a", ":"+joinVar)
				return
			}
		case sqlparser.TableName:
			if !systemTable(node.Qualifier.String()) {
				node.Name.Format(buf)
				return
			}
			node.Format(buf)
			return
		}
		node.Format(buf)
	}
	buf := sqlparser.NewTrackedBuffer(varFormatter)
	varFormatter(buf, rb.Select)
	ro.eroute.Query = buf.ParsedQuery().Query
	ro.eroute.FieldQuery = rb.generateFieldQuery(rb.Select, jt)
	return nil
}

func systemTable(qualifier string) bool {
	return strings.EqualFold(qualifier, "information_schema") ||
		strings.EqualFold(qualifier, "performance_schema") ||
		strings.EqualFold(qualifier, "sys") ||
		strings.EqualFold(qualifier, "mysql")
}

func (rb *route) finalizeOptions() {
	bestOption := rb.routeOptions[0]
	for i := 1; i < len(rb.routeOptions); i++ {
		if cur := rb.routeOptions[i]; cur.isBetterThan(bestOption) {
			bestOption = cur
		}
	}
	rb.routeOptions = []*routeOption{bestOption}
}

// procureValues procures and converts the input into
// the expected types for rb.Values.
func (rb *route) procureValues(bldr builder, jt *jointab, val sqlparser.Expr) (sqltypes.PlanValue, error) {
	switch val := val.(type) {
	case sqlparser.ValTuple:
		pv := sqltypes.PlanValue{}
		for _, val := range val {
			v, err := rb.procureValues(bldr, jt, val)
			if err != nil {
				return pv, err
			}
			pv.Values = append(pv.Values, v)
		}
		return pv, nil
	case *sqlparser.ColName:
		joinVar := jt.Procure(bldr, val, rb.Order())
		return sqltypes.PlanValue{Key: joinVar}, nil
	default:
		return sqlparser.NewPlanValue(val)
	}
}

func (rb *route) isLocal(col *sqlparser.ColName) bool {
	return col.Metadata.(*column).Origin() == rb
}

// generateFieldQuery generates a query with an impossible where.
// This will be used on the RHS node to fetch field info if the LHS
// returns no result.
func (rb *route) generateFieldQuery(sel sqlparser.SelectStatement, jt *jointab) string {
	formatter := func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			if !rb.isLocal(node) {
				_, joinVar := jt.Lookup(node)
				buf.Myprintf("%a", ":"+joinVar)
				return
			}
		case sqlparser.TableName:
			if !systemTable(node.Qualifier.String()) {
				node.Name.Format(buf)
				return
			}
			node.Format(buf)
			return
		}
		sqlparser.FormatImpossibleQuery(buf, node)
	}

	return sqlparser.NewTrackedBuffer(formatter).WriteNode(sel).ParsedQuery().Query
}

// SupplyVar satisfies the builder interface.
func (rb *route) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	// route is an atomic primitive. So, SupplyVar cannot be
	// called on it.
	panic("BUG: route is an atomic node.")
}

// SupplyCol satisfies the builder interface.
func (rb *route) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	c := col.Metadata.(*column)
	for i, rc := range rb.resultColumns {
		if rc.column == c {
			return rc, i
		}
	}

	// A new result has to be returned.
	rc = &resultColumn{column: c}
	rb.resultColumns = append(rb.resultColumns, rc)
	sel := rb.Select.(*sqlparser.Select)
	sel.SelectExprs = append(sel.SelectExprs, &sqlparser.AliasedExpr{Expr: col})
	return rc, len(rb.resultColumns) - 1
}

// SupplyWeightString satisfies the builder interface.
func (rb *route) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	rc := rb.resultColumns[colNumber]
	if weightcolNumber, ok := rb.weightStrings[rc]; ok {
		return weightcolNumber, nil
	}
	expr := &sqlparser.AliasedExpr{
		Expr: &sqlparser.FuncExpr{
			Name: sqlparser.NewColIdent("weight_string"),
			Exprs: []sqlparser.SelectExpr{
				rb.Select.(*sqlparser.Select).SelectExprs[colNumber],
			},
		},
	}
	// It's ok to pass nil for pb and builder because PushSelect doesn't use them.
	_, weightcolNumber, _ = rb.PushSelect(nil, expr, nil)
	rb.weightStrings[rc] = weightcolNumber
	return weightcolNumber, nil
}

// MergeSubquery returns true if the subquery route could successfully be merged
// with the outer route.
func (rb *route) MergeSubquery(pb *primitiveBuilder, inner *route) bool {
	var mergedRouteOptions []*routeOption
outer:
	for _, lro := range rb.routeOptions {
		for _, rro := range inner.routeOptions {
			if lro.SubqueryCanMerge(pb, rro) {
				lro.MergeSubquery(rro)
				mergedRouteOptions = append(mergedRouteOptions, lro)
				continue outer
			}
		}
	}
	if len(mergedRouteOptions) != 0 {
		rb.routeOptions = mergedRouteOptions
		inner.Redirect = rb
		return true
	}
	return false
}

// MergeUnion returns true if the rhs route could successfully be merged
// with the rb route.
func (rb *route) MergeUnion(right *route) bool {
	var mergedRouteOptions []*routeOption
outer:
	for _, lro := range rb.routeOptions {
		for _, rro := range right.routeOptions {
			if lro.UnionCanMerge(rro) {
				lro.MergeUnion(rro)
				mergedRouteOptions = append(mergedRouteOptions, lro)
				continue outer
			}
		}
	}
	if len(mergedRouteOptions) != 0 {
		rb.routeOptions = mergedRouteOptions
		right.Redirect = rb
		return true
	}
	return false
}

// removeMultishardOptions removes all multi-shard options from the
// route. It returns false if no such options exist.
func (rb *route) removeMultishardOptions() bool {
	return rb.removeOptions(func(ro *routeOption) bool {
		switch ro.eroute.Opcode {
		case engine.SelectUnsharded, engine.SelectDBA, engine.SelectNext, engine.SelectEqualUnique, engine.SelectReference:
			return true
		}
		return false
	})
}

// removeShardedOptions removes all sharded options from the
// route. It returns false if no such options exist.
// This is used for constructs that are only supported for unsharded
// keyspaces like last_insert_id.
func (rb *route) removeShardedOptions() bool {
	return rb.removeOptions(func(ro *routeOption) bool {
		return ro.eroute.Opcode == engine.SelectUnsharded
	})
}

func (rb *route) containsDualTable() bool {
	for _, ro := range rb.routeOptions {
		if ro.vschemaTable.Name.String() == "dual" {
			return true
		}
	}
	return false
}

// removeOptionsWithUnmatchedKeyspace removes all options that don't match
// the specified keyspace. It returns false if no such options exist.
func (rb *route) removeOptionsWithUnmatchedKeyspace(keyspace string) bool {
	return rb.removeOptions(func(ro *routeOption) bool {
		return ro.eroute.Keyspace.Name == keyspace
	})
}

// removeOptions removes all options that don't match on
// the criteria function. It returns false if no such options exist.
func (rb *route) removeOptions(match func(*routeOption) bool) bool {
	var newOptions []*routeOption
	for _, ro := range rb.routeOptions {
		if match(ro) {
			newOptions = append(newOptions, ro)
		}
	}
	if len(newOptions) == 0 {
		return false
	}
	rb.routeOptions = newOptions
	return true
}

// queryTimeout returns DirectiveQueryTimeout value if set, otherwise returns 0.
func queryTimeout(d sqlparser.CommentDirectives) int {
	if d == nil {
		return 0
	}

	val, ok := d[sqlparser.DirectiveQueryTimeout]
	if !ok {
		return 0
	}

	intVal, ok := val.(int)
	if ok {
		return intVal
	}
	return 0
}
