// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// planBuilder represents any object that's used to
// build a plan. The top-level planBuilder will be a
// tree that points to other planBuilder objects.
// Currently, joinBuilder and routeBuilder are the
// only two supported planBuilder objects. More will be
// added as we extend the functionality.
// Each Builder object builds a Plan object, and they
// will mirror the same tree. Once all the plans are built,
// the builder objects will be discarded, and only
// the Plan objects will remain. Because of the near-equivalent
// meaning of a planBuilder object and its plan, the variable
// names are overloaded. The separation exists only because the
// information in the planBuilder objects are not ultimately
// required externally.
// For example, a route variable usually refers to a
// routeBuilder object, which in turn, has a Route field.
// This should not cause confusion because we almost never
// reference the inner Route directly.
type planBuilder interface {
	// Symtab returns the associated symtab.
	Symtab() *symtab
	// SetSymtab sets the symtab for the current node and
	// its non-subquery children.
	SetSymtab(*symtab)
	// Order is a number that signifies execution order.
	// A lower Order number Route is executed before a
	// higher one. For a node that contains other nodes,
	// the Order represents the highest order of the leaf
	// nodes. This function is used to travel from a root
	// node to a target node.
	Order() int
	// SetOrder sets the order for the underlying routes.
	SetOrder(int)
	// Primitve returns the primitive built by the builder.
	Primitive() Primitive
	// Leftmost returns the leftmost route.
	Leftmost() *routeBuilder
	// Join joins the two planBuilder objects. The outcome
	// can be a new planBuilder or a modified one.
	Join(rhs planBuilder, Join *sqlparser.JoinTableExpr) (planBuilder, error)
	// SetRHS marks all routes under this node as RHS.
	SetRHS()
	// PushSelect pushes the select expression through the tree
	// all the way to the route that colsym points to.
	// PushSelect is similar to SupplyCol except that it always
	// adds a new column, whereas SupplyCol can reuse an existing
	// column. This is required because the ORDER BY clause
	// may refer to columns by number. The function must return
	// a colsym for the expression and the column number of the result.
	PushSelect(expr *sqlparser.NonStarExpr, route *routeBuilder) (colsym *colsym, colnum int, err error)
	// PushMisc pushes miscelleaneous constructs to all the routes.
	PushMisc(sel *sqlparser.Select)
	// SupplyCol will be used for the wire-up process. This function
	// takes a column reference as input, changes the plan
	// to supply the requested column and returns the column number of
	// the result for it. The request is passed down recursively
	// as needed.
	SupplyCol(col *sqlparser.ColName) int
}

// routeBuilder is used to build a Route primitive.
// It's used to build one of the Select routes like
// SelectScatter, etc. Portions of the original Select AST
// are moved into this node, which will be used to build
// the final SQL for this route.
type routeBuilder struct {
	// Redirect may point to another route if this route
	// was merged with it. The Resolve function chases
	// this pointer till the last un-redirected route.
	Redirect *routeBuilder
	// IsRHS is true if the routeBuilder is the RHS of a
	// LEFT JOIN. If so, many restrictions come into play.
	IsRHS bool
	// Select is the AST for the query fragment that will be
	// executed by this route.
	Select sqlparser.Select
	order  int
	symtab *symtab
	// Colsyms represent the columns returned by this route.
	Colsyms []*colsym
	// Route is the plan object being built. It will contain all the
	// information necessary to execute the route operation.
	Route *Route
}

func newRouteBuilder(from sqlparser.TableExprs, route *Route, table *Table, vschema *VSchema, alias sqlparser.SQLName) *routeBuilder {
	// We have some circular pointer references here:
	// The routeBuilder points to the symtab idicating
	// the symtab that should be used to resolve symbols
	// for it. This is same as the SELECT statement's symtab.
	// This pointer is needed because each subquery will have
	// its own symtab. Multiple routes can point to the same
	// symtab.
	// The tabelAlias, which is inside the symtab, points back
	// to the route to indidcate that the symbol is produced
	// by this route. A symbol referenced in a route can actually
	// be pointing to a different route. This information is used
	// to determine if symbol references are local or not.
	rtb := &routeBuilder{
		Select: sqlparser.Select{From: from},
		symtab: newSymtab(vschema),
		order:  1,
		Route:  route,
	}
	_ = rtb.symtab.AddAlias(alias, table, rtb)
	return rtb
}

// Resolve resolves redirects, and returns the last
// un-redirected route.
func (rtb *routeBuilder) Resolve() *routeBuilder {
	for rtb.Redirect != nil {
		rtb = rtb.Redirect
	}
	return rtb
}

// Symtab returns the associated symtab.
func (rtb *routeBuilder) Symtab() *symtab {
	return rtb.symtab
}

// SetSymtab sets the symtab.
func (rtb *routeBuilder) SetSymtab(symtab *symtab) {
	rtb.symtab = symtab
}

// Order returns the order of the node.
func (rtb *routeBuilder) Order() int {
	return rtb.order
}

// SetOrder sets the order to one above the specified number.
func (rtb *routeBuilder) SetOrder(order int) {
	rtb.order = order + 1
}

// Primitve returns the built primitive.
func (rtb *routeBuilder) Primitive() Primitive {
	return rtb.Route
}

// Leftmost returns the current route.
func (rtb *routeBuilder) Leftmost() *routeBuilder {
	return rtb
}

// Join joins with the RHS. This could produce a merged route
// or a new join node.
func (rtb *routeBuilder) Join(rhs planBuilder, join *sqlparser.JoinTableExpr) (planBuilder, error) {
	rRoute, ok := rhs.(*routeBuilder)
	if !ok {
		return newJoinBuilder(rtb, rhs, join)
	}
	if rtb.Route.Keyspace.Name != rRoute.Route.Keyspace.Name {
		return newJoinBuilder(rtb, rRoute, join)
	}
	if rtb.Route.Opcode == SelectUnsharded {
		// Two Routes from the same unsharded keyspace can be merged.
		return rtb.merge(rRoute, join)
	}

	// Both routeBuilder are sharded routes. Analyze join condition for merging.
	for _, filter := range splitAndExpression(nil, join.On) {
		if rtb.isSameRoute(rRoute, filter) {
			return rtb.merge(rRoute, join)
		}
	}

	// Both l & r routes point to the same shard.
	if rtb.Route.Opcode == SelectEqualUnique && rRoute.Route.Opcode == SelectEqualUnique {
		if valEqual(rtb.Route.Values, rRoute.Route.Values) {
			return rtb.merge(rRoute, join)
		}
	}

	return newJoinBuilder(rtb, rRoute, join)
}

// SetRHS marks the route as RHS.
func (rtb *routeBuilder) SetRHS() {
	rtb.IsRHS = true
}

// merge merges the two routes. The ON clause is also analyzed to
// see if the plan can be improved. The operation can fail if
// the expression contains a non-pushable subquery.
func (rtb *routeBuilder) merge(rhs *routeBuilder, join *sqlparser.JoinTableExpr) (planBuilder, error) {
	rtb.Select.From = sqlparser.TableExprs{join}
	if join.Join == sqlparser.LeftJoinStr {
		rhs.Symtab().SetRHS()
	}
	err := rtb.Symtab().Merge(rhs.Symtab())
	rhs.Redirect = rtb
	if err != nil {
		return nil, err
	}
	for _, filter := range splitAndExpression(nil, join.On) {
		// If VTGate evolves, this section should be rewritten
		// to use processBoolExpr.
		_, err = findRoute(filter, rtb)
		if err != nil {
			return nil, err
		}
		rtb.UpdatePlan(filter)
	}
	return rtb, nil
}

// isSameRoute returns true if the join constraint makes the routes
// mergeable by unique vindex. The constraint has to be an equality
// like a.id = b.id where both columns have the same unique vindex.
func (rtb *routeBuilder) isSameRoute(rhs *routeBuilder, filter sqlparser.BoolExpr) bool {
	comparison, ok := filter.(*sqlparser.ComparisonExpr)
	if !ok {
		return false
	}
	if comparison.Operator != sqlparser.EqualStr {
		return false
	}
	left := comparison.Left
	right := comparison.Right
	lVindex := rtb.Symtab().Vindex(left, rtb, false)
	if lVindex == nil {
		left, right = right, left
		lVindex = rtb.Symtab().Vindex(left, rtb, false)
	}
	if lVindex == nil || !IsUnique(lVindex) {
		return false
	}
	rVindex := rhs.Symtab().Vindex(right, rhs, false)
	if rVindex == nil {
		return false
	}
	if rVindex != lVindex {
		return false
	}
	return true
}

// PushFilter pushes the filter into the route. The plan will
// be updated if the new filter improves it.
func (rtb *routeBuilder) PushFilter(filter sqlparser.BoolExpr, whereType string) error {
	if rtb.IsRHS {
		return errors.New("unsupported: complex left join and where claused")
	}
	switch whereType {
	case sqlparser.WhereStr:
		rtb.Select.AddWhere(filter)
	case sqlparser.HavingStr:
		rtb.Select.AddHaving(filter)
	}
	rtb.UpdatePlan(filter)
	return nil
}

// UpdatePlan evaluates the plan against the specified
// filter. If it's an improvement, the plan is updated.
// We assume that the filter has already been pushed into
// the route. This function should only be used when merging
// routes, where the ON clause gets implicitly pushed into
// the merged route.
func (rtb *routeBuilder) UpdatePlan(filter sqlparser.BoolExpr) {
	opcode, vindex, values := rtb.computePlan(filter)
	if opcode == SelectScatter {
		return
	}
	switch rtb.Route.Opcode {
	case SelectEqualUnique:
		if opcode == SelectEqualUnique && vindex.Cost() < rtb.Route.Vindex.Cost() {
			rtb.setPlan(opcode, vindex, values)
		}
	case SelectEqual:
		switch opcode {
		case SelectEqualUnique:
			rtb.setPlan(opcode, vindex, values)
		case SelectEqual:
			if vindex.Cost() < rtb.Route.Vindex.Cost() {
				rtb.setPlan(opcode, vindex, values)
			}
		}
	case SelectIN:
		switch opcode {
		case SelectEqualUnique, SelectEqual:
			rtb.setPlan(opcode, vindex, values)
		case SelectIN:
			if vindex.Cost() < rtb.Route.Vindex.Cost() {
				rtb.setPlan(opcode, vindex, values)
			}
		}
	case SelectScatter:
		switch opcode {
		case SelectEqualUnique, SelectEqual, SelectIN:
			rtb.setPlan(opcode, vindex, values)
		}
	}
}

// setPlan updates the plan info for the route.
func (rtb *routeBuilder) setPlan(opcode RouteOpcode, vindex Vindex, values interface{}) {
	rtb.Route.Opcode = opcode
	rtb.Route.Vindex = vindex
	rtb.Route.Values = values
}

// ComputePlan computes the plan for the specified filter.
func (rtb *routeBuilder) computePlan(filter sqlparser.BoolExpr) (opcode RouteOpcode, vindex Vindex, values interface{}) {
	switch node := filter.(type) {
	case *sqlparser.ComparisonExpr:
		switch node.Operator {
		case sqlparser.EqualStr:
			return rtb.computeEqualPlan(node)
		case sqlparser.InStr:
			return rtb.computeINPlan(node)
		}
	}
	return SelectScatter, nil, nil
}

// computeEqualPlan computes the plan for an equality constraint.
func (rtb *routeBuilder) computeEqualPlan(comparison *sqlparser.ComparisonExpr) (opcode RouteOpcode, vindex Vindex, values interface{}) {
	left := comparison.Left
	right := comparison.Right
	vindex = rtb.Symtab().Vindex(left, rtb, true)
	if vindex == nil {
		left, right = right, left
		vindex = rtb.Symtab().Vindex(left, rtb, true)
		if vindex == nil {
			return SelectScatter, nil, nil
		}
	}
	if !exprIsValue(right, rtb) {
		return SelectScatter, nil, nil
	}
	if IsUnique(vindex) {
		return SelectEqualUnique, vindex, right
	}
	return SelectEqual, vindex, right
}

// computeINPlan computes the plan for an IN constraint.
func (rtb *routeBuilder) computeINPlan(comparison *sqlparser.ComparisonExpr) (opcode RouteOpcode, vindex Vindex, values interface{}) {
	vindex = rtb.Symtab().Vindex(comparison.Left, rtb, true)
	if vindex == nil {
		return SelectScatter, nil, nil
	}
	switch node := comparison.Right.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			if !exprIsValue(n, rtb) {
				return SelectScatter, nil, nil
			}
		}
		return SelectIN, vindex, comparison
	case sqlparser.ListArg:
		return SelectIN, vindex, comparison
	}
	return SelectScatter, nil, nil
}

// PushSelect pushes the select expression into the route.
func (rtb *routeBuilder) PushSelect(expr *sqlparser.NonStarExpr, _ *routeBuilder) (colsym *colsym, colnum int, err error) {
	colsym = newColsym(rtb, rtb.Symtab())
	if expr.As != "" {
		colsym.Alias = expr.As
	}
	if col, ok := expr.Expr.(*sqlparser.ColName); ok {
		if colsym.Alias == "" {
			colsym.Alias = sqlparser.SQLName(sqlparser.String(col))
		}
		colsym.Vindex = rtb.Symtab().Vindex(col, rtb, true)
		colsym.Underlying = newColref(col)
	} else {
		if rtb.IsRHS {
			return nil, 0, errors.New("unsupported: complex left join and column expressions")
		}
	}
	rtb.Select.SelectExprs = append(rtb.Select.SelectExprs, expr)
	rtb.Colsyms = append(rtb.Colsyms, colsym)
	return colsym, len(rtb.Colsyms) - 1, nil
}

// PushStar pushes the '*' expression into the route.
func (rtb *routeBuilder) PushStar(expr *sqlparser.StarExpr) *colsym {
	colsym := newColsym(rtb, rtb.Symtab())
	colsym.Alias = sqlparser.SQLName(sqlparser.String(expr))
	rtb.Select.SelectExprs = append(rtb.Select.SelectExprs, expr)
	rtb.Colsyms = append(rtb.Colsyms, colsym)
	return colsym
}

// MakeDistinct sets the DISTINCT property to the select.
func (rtb *routeBuilder) MakeDistinct() {
	rtb.Select.Distinct = sqlparser.DistinctStr
}

// SetGroupBy sets the GROUP BY clause for the route.
func (rtb *routeBuilder) SetGroupBy(groupBy sqlparser.GroupBy) {
	rtb.Select.GroupBy = groupBy
}

// AddOrder adds an ORDER BY expression to the route.
func (rtb *routeBuilder) AddOrder(order *sqlparser.Order) error {
	if rtb.IsRHS {
		return errors.New("unsupported: complex left join and order by")
	}
	rtb.Select.OrderBy = append(rtb.Select.OrderBy, order)
	return nil
}

// SetLimit adds a LIMIT clause to the route.
func (rtb *routeBuilder) SetLimit(limit *sqlparser.Limit) {
	rtb.Select.Limit = limit
}

// PushMisc updates the comments & 'for update' sections of the route.
func (rtb *routeBuilder) PushMisc(sel *sqlparser.Select) {
	rtb.Select.Comments = sel.Comments
	rtb.Select.Lock = sel.Lock
}

// SupplyCol changes the router to supply the requested column
// name, and returns the result column number. If the column
// is already in the list, it's reused.
func (rtb *routeBuilder) SupplyCol(col *sqlparser.ColName) int {
	// We already know it's a tableAlias.
	meta := col.Metadata.(*tableAlias)
	ref := newColref(col)
	for i, colsym := range rtb.Colsyms {
		if colsym.Underlying == ref {
			return i
		}
	}
	rtb.Colsyms = append(rtb.Colsyms, &colsym{
		Alias:      sqlparser.SQLName(sqlparser.String(col)),
		Underlying: ref,
	})
	rtb.Select.SelectExprs = append(
		rtb.Select.SelectExprs,
		&sqlparser.NonStarExpr{
			Expr: &sqlparser.ColName{
				Metadata:  col.Metadata,
				Qualifier: meta.Alias,
				Name:      col.Name,
			},
		},
	)
	return len(rtb.Colsyms) - 1
}

// IsSingle returns true if the route targets only one database.
func (rtb *routeBuilder) IsSingle() bool {
	return rtb.Route.Opcode == SelectUnsharded || rtb.Route.Opcode == SelectEqualUnique
}
