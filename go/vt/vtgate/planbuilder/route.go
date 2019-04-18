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
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ builder = (*route)(nil)

var errIntermixingUnsupported = errors.New("unsupported: intermixing of information_schema and regular tables")

// route is used to build a Route primitive.
// It's used to build one of the Select routes like
// SelectScatter, etc. Portions of the original Select AST
// are moved into this node, which will be used to build
// the final SQL for this route.
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

	// condition stores the AST condition that will be used
	// to resolve the ERoute Values field.
	condition sqlparser.Expr

	// weight_string keeps track of the weight_string expressions
	// that were added additionally for each column. These expressions
	// are added to be used for collation of text columns.
	weightStrings map[*resultColumn]int

	// ERoute is the primitive being built.
	ERoute *engine.Route
}

func newRoute(stmt sqlparser.SelectStatement, eroute *engine.Route, condition sqlparser.Expr) (*route, *symtab) {
	rb := &route{
		Select:        stmt,
		order:         1,
		condition:     condition,
		weightStrings: make(map[*resultColumn]int),
		ERoute:        eroute,
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
	return rb.ERoute
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
	rb.UpdatePlan(pb, filter)
	return nil
}

// UpdatePlan evaluates the primitive against the specified
// filter. If it's an improvement, the primitive is updated.
// We assume that the filter has already been pushed into
// the route. This function should only be used when merging
// routes, where the ON clause gets implicitly pushed into
// the merged route.
func (rb *route) UpdatePlan(pb *primitiveBuilder, filter sqlparser.Expr) {
	opcode, vindex, values := rb.computePlan(pb, filter)
	if opcode == engine.SelectScatter {
		return
	}
	switch rb.ERoute.Opcode {
	case engine.SelectEqualUnique:
		if opcode == engine.SelectEqualUnique && vindex.Cost() < rb.ERoute.Vindex.Cost() {
			rb.updateRoute(opcode, vindex, values)
		}
	case engine.SelectEqual:
		switch opcode {
		case engine.SelectEqualUnique:
			rb.updateRoute(opcode, vindex, values)
		case engine.SelectEqual:
			if vindex.Cost() < rb.ERoute.Vindex.Cost() {
				rb.updateRoute(opcode, vindex, values)
			}
		}
	case engine.SelectIN:
		switch opcode {
		case engine.SelectEqualUnique, engine.SelectEqual:
			rb.updateRoute(opcode, vindex, values)
		case engine.SelectIN:
			if vindex.Cost() < rb.ERoute.Vindex.Cost() {
				rb.updateRoute(opcode, vindex, values)
			}
		}
	case engine.SelectScatter:
		switch opcode {
		case engine.SelectEqualUnique, engine.SelectEqual, engine.SelectIN:
			rb.updateRoute(opcode, vindex, values)
		}
	}
}

func (rb *route) updateRoute(opcode engine.RouteOpcode, vindex vindexes.Vindex, condition sqlparser.Expr) {
	rb.ERoute.Opcode = opcode
	rb.ERoute.Vindex = vindex
	rb.condition = condition
}

// computePlan computes the plan for the specified filter.
func (rb *route) computePlan(pb *primitiveBuilder, filter sqlparser.Expr) (opcode engine.RouteOpcode, vindex vindexes.Vindex, condition sqlparser.Expr) {
	switch node := filter.(type) {
	case *sqlparser.ComparisonExpr:
		switch node.Operator {
		case sqlparser.EqualStr:
			return rb.computeEqualPlan(pb, node)
		case sqlparser.InStr:
			return rb.computeINPlan(pb, node)
		}
	case *sqlparser.ParenExpr:
		return rb.computePlan(pb, node.Expr)
	}
	return engine.SelectScatter, nil, nil
}

// computeEqualPlan computes the plan for an equality constraint.
func (rb *route) computeEqualPlan(pb *primitiveBuilder, comparison *sqlparser.ComparisonExpr) (opcode engine.RouteOpcode, vindex vindexes.Vindex, condition sqlparser.Expr) {
	left := comparison.Left
	right := comparison.Right
	vindex = pb.st.Vindex(left, rb)
	if vindex == nil {
		left, right = right, left
		vindex = pb.st.Vindex(left, rb)
		if vindex == nil {
			return engine.SelectScatter, nil, nil
		}
	}
	if !rb.exprIsValue(right) {
		return engine.SelectScatter, nil, nil
	}
	if vindex.IsUnique() {
		return engine.SelectEqualUnique, vindex, right
	}
	return engine.SelectEqual, vindex, right
}

// computeINPlan computes the plan for an IN constraint.
func (rb *route) computeINPlan(pb *primitiveBuilder, comparison *sqlparser.ComparisonExpr) (opcode engine.RouteOpcode, vindex vindexes.Vindex, condition sqlparser.Expr) {
	vindex = pb.st.Vindex(comparison.Left, rb)
	if vindex == nil {
		return engine.SelectScatter, nil, nil
	}
	switch node := comparison.Right.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			if !rb.exprIsValue(n) {
				return engine.SelectScatter, nil, nil
			}
		}
		return engine.SelectIN, vindex, comparison
	case sqlparser.ListArg:
		return engine.SelectIN, vindex, comparison
	}
	return engine.SelectScatter, nil, nil
}

// exprIsValue returns true if the expression can be treated as a value
// for the route. External references are treated as value.
func (rb *route) exprIsValue(expr sqlparser.Expr) bool {
	if node, ok := expr.(*sqlparser.ColName); ok {
		return node.Metadata.(*column).Origin() != rb
	}
	return sqlparser.IsValue(expr)
}

// PushSelect satisfies the builder interface.
func (rb *route) PushSelect(expr *sqlparser.AliasedExpr, _ builder) (rc *resultColumn, colnum int, err error) {
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

// MakeDistinct sets the DISTINCT property to the select.
func (rb *route) MakeDistinct() error {
	rb.Select.(*sqlparser.Select).Distinct = sqlparser.DistinctStr
	return nil
}

// SetGroupBy sets the GROUP BY clause for the route.
func (rb *route) SetGroupBy(groupBy sqlparser.GroupBy) error {
	rb.Select.(*sqlparser.Select).GroupBy = groupBy
	return nil
}

// PushOrderBy sets the order by for the route.
func (rb *route) PushOrderBy(order *sqlparser.Order) error {
	if rb.IsSingle() {
		rb.Select.AddOrder(order)
		return nil
	}

	// If it's a scatter, we have to populate the OrderBy field.
	colnum := -1
	switch expr := order.Expr.(type) {
	case *sqlparser.SQLVal:
		var err error
		if colnum, err = ResultFromNumber(rb.resultColumns, expr); err != nil {
			return fmt.Errorf("invalid order by: %v", err)
		}
	case *sqlparser.ColName:
		c := expr.Metadata.(*column)
		for i, rc := range rb.resultColumns {
			if rc.column == c {
				colnum = i
				break
			}
		}
	default:
		return fmt.Errorf("unsupported: in scatter query: complex order by expression: %s", sqlparser.String(expr))
	}
	// If column is not found, then the order by is referencing
	// a column that's not on the select list.
	if colnum == -1 {
		return fmt.Errorf("unsupported: in scatter query: order by must reference a column in the select list: %s", sqlparser.String(order))
	}
	rb.ERoute.OrderBy = append(rb.ERoute.OrderBy, engine.OrderbyParams{
		Col:  colnum,
		Desc: order.Direction == sqlparser.DescScr,
	})

	rb.Select.AddOrder(order)
	return nil
}

// PushOrderByNull satisfies the builder interface.
func (rb *route) PushOrderByNull() {
	rb.Select.(*sqlparser.Select).OrderBy = sqlparser.OrderBy{&sqlparser.Order{Expr: &sqlparser.NullVal{}}}
}

// PushOrderByRand satisfies the builder interface.
func (rb *route) PushOrderByRand() {
	rb.Select.(*sqlparser.Select).OrderBy = sqlparser.OrderBy{&sqlparser.Order{Expr: &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("rand")}}}
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
	// Precaution: update ERoute.Values only if it's not set already.
	if rb.ERoute.Values == nil {
		// Resolve values stored in the builder.
		switch vals := rb.condition.(type) {
		case *sqlparser.ComparisonExpr:
			pv, err := rb.procureValues(bldr, jt, vals.Right)
			if err != nil {
				return err
			}
			rb.ERoute.Values = []sqltypes.PlanValue{pv}
			vals.Right = sqlparser.ListArg("::" + engine.ListVarName)
		case nil:
			// no-op.
		default:
			pv, err := rb.procureValues(bldr, jt, vals)
			if err != nil {
				return err
			}
			rb.ERoute.Values = []sqltypes.PlanValue{pv}
		}
	}

	// If rb has to do the ordering, and if any columns are Text,
	// we have to request the corresponding weight_string from mysql
	// and use that value instead. This is because we cannot mimic
	// mysql's collation behavior yet.
	for i, orderby := range rb.ERoute.OrderBy {
		rc := rb.resultColumns[orderby.Col]
		if sqltypes.IsText(rc.column.typ) {
			// If a weight string was previously requested (by OrderedAggregator),
			// reuse it.
			if colnum, ok := rb.weightStrings[rc]; ok {
				rb.ERoute.OrderBy[i].Col = colnum
				continue
			}

			// len(rb.resultColumns) does not change. No harm using the value multiple times.
			rb.ERoute.TruncateColumnCount = len(rb.resultColumns)

			// This code is partially duplicated from SupplyWeightString and PushSelect.
			// We should not update resultColumns because it's not returned in the result.
			// This is why we don't call PushSelect (or SupplyWeightString).
			expr := &sqlparser.AliasedExpr{
				Expr: &sqlparser.FuncExpr{
					Name: sqlparser.NewColIdent("weight_string"),
					Exprs: []sqlparser.SelectExpr{
						rb.Select.(*sqlparser.Select).SelectExprs[orderby.Col],
					},
				},
			}
			sel := rb.Select.(*sqlparser.Select)
			sel.SelectExprs = append(sel.SelectExprs, expr)
			rb.ERoute.OrderBy[i].Col = len(sel.SelectExprs) - 1
			// We don't really have to update weightStrings, but we're doing it
			// for good measure.
			rb.weightStrings[rc] = len(sel.SelectExprs) - 1
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
				if rb.exprIsValue(node.Left) && !rb.exprIsValue(node.Right) {
					node.Left, node.Right = node.Right, node.Left
				}
			}
		}
		return true, nil
	}, rb.Select)

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
	rb.ERoute.Query = buf.ParsedQuery().Query
	rb.ERoute.FieldQuery = rb.generateFieldQuery(rb.Select, jt)
	return nil
}

func systemTable(qualifier string) bool {
	return strings.EqualFold(qualifier, "information_schema") ||
		strings.EqualFold(qualifier, "performance_schema") ||
		strings.EqualFold(qualifier, "sys") ||
		strings.EqualFold(qualifier, "mysql")
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
func (rb *route) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colnum int) {
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

func (rb *route) SupplyWeightString(colnum int) (weightColnum int) {
	rc := rb.resultColumns[colnum]
	if weightColnum, ok := rb.weightStrings[rc]; ok {
		return weightColnum
	}
	expr := &sqlparser.AliasedExpr{
		Expr: &sqlparser.FuncExpr{
			Name: sqlparser.NewColIdent("weight_string"),
			Exprs: []sqlparser.SelectExpr{
				rb.Select.(*sqlparser.Select).SelectExprs[colnum],
			},
		},
	}
	_, weightColnum, _ = rb.PushSelect(expr, nil)
	rb.weightStrings[rc] = weightColnum
	return weightColnum
}

// BuildColName builds a *sqlparser.ColName for the resultColumn specified
// by the index. The built ColName will correctly reference the resultColumn
// it was built from, which is safe to push down into the route.
func (rb *route) BuildColName(index int) (*sqlparser.ColName, error) {
	alias := rb.resultColumns[index].alias
	if alias.IsEmpty() {
		return nil, errors.New("cannot reference a complex expression")
	}
	for i, rc := range rb.resultColumns {
		if i == index {
			continue
		}
		if rc.alias.Equal(alias) {
			return nil, fmt.Errorf("ambiguous symbol reference: %v", alias)
		}
	}
	return &sqlparser.ColName{
		Metadata: rb.resultColumns[index].column,
		Name:     alias,
	}, nil
}

// IsSingle returns true if the route targets only one database.
func (rb *route) IsSingle() bool {
	switch rb.ERoute.Opcode {
	// Even thought SelectNext is a single-shard query, we don't
	// include it here because it can't be combined with any other construct.
	case engine.SelectUnsharded, engine.SelectDBA, engine.SelectEqualUnique:
		return true
	}
	return false
}

// SubqueryCanMerge returns nil if the supplied route that represents
// a subquery can be merged with the outer route. If not, it
// returns an appropriate error.
func (rb *route) SubqueryCanMerge(pb *primitiveBuilder, inner *route) bool {
	if rb.ERoute.Keyspace.Name != inner.ERoute.Keyspace.Name {
		return false
	}
	switch inner.ERoute.Opcode {
	case engine.SelectUnsharded:
		if rb.ERoute.Opcode == engine.SelectUnsharded {
			return true
		}
		return false
	case engine.SelectDBA:
		if rb.ERoute.Opcode == engine.SelectDBA {
			return true
		}
		return false
	case engine.SelectNext:
		return false
	case engine.SelectEqualUnique:
		// This checks for the case where the subquery is dependent
		// on the vindex column of the outer query:
		// select ... from a where a.id = 5 ... (select ... from b where b.id = a.id).
		// If b.id and a.id have the same vindex, it becomes a single-shard
		// query: the subquery can merge with the outer query.
		switch vals := inner.condition.(type) {
		case *sqlparser.ColName:
			if pb.st.Vindex(vals, rb) == inner.ERoute.Vindex {
				return true
			}
		}
	default:
		return false
	}
	return rb.isSameShardedRoute(inner) == nil
}

// UnionCanMerge returns nil if the supplied route that represents
// the RHS of a union can be merged with the current route. If not, it
// returns an appropriate error.
func (rb *route) UnionCanMerge(right *route) error {
	if rb.ERoute.Opcode == engine.SelectNext || right.ERoute.Opcode == engine.SelectNext {
		return errors.New("unsupported: UNION on sequence tables")
	}
	if rb.ERoute.Keyspace.Name != right.ERoute.Keyspace.Name {
		return errors.New("unsupported: UNION on different keyspaces")
	}
	switch rb.ERoute.Opcode {
	case engine.SelectUnsharded:
		if right.ERoute.Opcode == engine.SelectUnsharded {
			return nil
		}
		return errIntermixingUnsupported
	case engine.SelectDBA:
		if right.ERoute.Opcode == engine.SelectDBA {
			return nil
		}
		return errIntermixingUnsupported
	}
	return rb.isSameShardedRoute(right)
}

// isSameShardedRoute returns nil if the supplied route has
// the same single shard target as the current route. If not, it
// returns an appropriate error.
func (rb *route) isSameShardedRoute(right *route) error {
	if rb.ERoute.Opcode != engine.SelectEqualUnique || right.ERoute.Opcode != engine.SelectEqualUnique {
		return errors.New("unsupported: UNION or subquery containing multi-shard queries")
	}
	if rb.ERoute.Vindex != right.ERoute.Vindex {
		return errors.New("unsupported: UNION or subquery on different shards: vindexes are different")
	}
	if !valEqual(rb.condition, right.condition) {
		return errors.New("unsupported: UNION or subquery on different shards: vindex values are different")
	}
	return nil
}

// SetOpcode changes the opcode to the specified value.
// If the change is not allowed, it returns an error.
func (rb *route) SetOpcode(code engine.RouteOpcode) error {
	switch code {
	case engine.SelectNext:
		if rb.ERoute.Opcode != engine.SelectUnsharded {
			return errors.New("NEXT used on a sharded table")
		}
	default:
		panic(fmt.Sprintf("BUG: unrecognized transition: %v", code))
	}
	rb.ERoute.Opcode = code
	return nil
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
