// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// route is used to build a Route primitive.
// It's used to build one of the Select routes like
// SelectScatter, etc. Portions of the original Select AST
// are moved into this node, which will be used to build
// the final SQL for this route.
type route struct {
	// Redirect may point to another route if this route
	// was merged with it. The Resolve function chases
	// this pointer till the last un-redirected route.
	Redirect *route
	// IsRHS is true if the route is the RHS of a
	// LEFT JOIN. If so, many restrictions come into play.
	IsRHS bool
	// Select is the AST for the query fragment that will be
	// executed by this route.
	Select sqlparser.SelectStatement
	order  int
	symtab *symtab
	// Colsyms represent the columns returned by this route.
	Colsyms []*colsym
	// ERoute is the primitive being built.
	ERoute *engine.Route
}

func newRoute(stmt sqlparser.SelectStatement, eroute *engine.Route, table *vindexes.Table, vschema VSchema, alias *sqlparser.TableName, astName sqlparser.TableIdent) *route {
	// We have some circular pointer references here:
	// The route points to the symtab idicating
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
	rb := &route{
		Select: stmt,
		symtab: newSymtab(vschema),
		order:  1,
		ERoute: eroute,
	}
	rb.symtab.AddAlias(alias, astName, table, rb)
	return rb
}

// Resolve resolves redirects, and returns the last
// un-redirected route.
func (rb *route) Resolve() *route {
	for rb.Redirect != nil {
		rb = rb.Redirect
	}
	return rb
}

// Symtab returns the associated symtab.
func (rb *route) Symtab() *symtab {
	return rb.symtab
}

// SetSymtab sets the symtab.
func (rb *route) SetSymtab(symtab *symtab) {
	rb.symtab = symtab
}

// Order returns the order of the node.
func (rb *route) Order() int {
	return rb.order
}

// SetOrder sets the order to one above the specified number.
func (rb *route) SetOrder(order int) {
	rb.order = order + 1
}

// Primitve returns the built primitive.
func (rb *route) Primitive() engine.Primitive {
	return rb.ERoute
}

// Leftmost returns the current route.
func (rb *route) Leftmost() *route {
	return rb
}

// Join joins with the RHS. This could produce a merged route
// or a new join node.
func (rb *route) Join(rhs builder, ajoin *sqlparser.JoinTableExpr) (builder, error) {
	rRoute, ok := rhs.(*route)
	if !ok {
		return newJoin(rb, rhs, ajoin)
	}
	if rb.ERoute.Keyspace.Name != rRoute.ERoute.Keyspace.Name {
		return newJoin(rb, rRoute, ajoin)
	}
	if rb.ERoute.Opcode == engine.SelectUnsharded {
		// Two Routes from the same unsharded keyspace can be merged.
		return rb.merge(rRoute, ajoin)
	}

	// Both route are sharded routes. For ',' joins (ajoin==nil), don't
	// analyze mergeability.
	if ajoin == nil {
		return newJoin(rb, rRoute, nil)
	}

	// Both route are sharded routes. Analyze join condition for merging.
	for _, filter := range splitAndExpression(nil, ajoin.On) {
		if rb.isSameRoute(rRoute, filter) {
			return rb.merge(rRoute, ajoin)
		}
	}

	// Both l & r routes point to the same shard.
	if rb.ERoute.Opcode == engine.SelectEqualUnique && rRoute.ERoute.Opcode == engine.SelectEqualUnique {
		if valEqual(rb.ERoute.Values, rRoute.ERoute.Values) {
			return rb.merge(rRoute, ajoin)
		}
	}

	return newJoin(rb, rRoute, ajoin)
}

// SetRHS marks the route as RHS.
func (rb *route) SetRHS() {
	rb.IsRHS = true
}

// merge merges the two routes. The ON clause is also analyzed to
// see if the primitive can be improved. The operation can fail if
// the expression contains a non-pushable subquery. ajoin can be nil
// if the join is on a ',' operator.
func (rb *route) merge(rhs *route, ajoin *sqlparser.JoinTableExpr) (builder, error) {
	sel := rb.Select.(*sqlparser.Select)
	if ajoin == nil {
		rhsSel := rhs.Select.(*sqlparser.Select)
		sel.From = append(sel.From, rhsSel.From...)
	} else {
		sel.From = sqlparser.TableExprs{ajoin}
		if ajoin.Join == sqlparser.LeftJoinStr {
			rhs.Symtab().SetRHS()
		}
	}
	err := rb.Symtab().Merge(rhs.Symtab())
	rhs.Redirect = rb
	if err != nil {
		return nil, err
	}
	if ajoin == nil {
		return rb, nil
	}
	for _, filter := range splitAndExpression(nil, ajoin.On) {
		// If VTGate evolves, this section should be rewritten
		// to use processExpr.
		_, err = findRoute(filter, rb)
		if err != nil {
			return nil, err
		}
		rb.UpdatePlan(filter)
	}
	return rb, nil
}

// isSameRoute returns true if the join constraint makes the routes
// mergeable by unique vindex. The constraint has to be an equality
// like a.id = b.id where both columns have the same unique vindex.
func (rb *route) isSameRoute(rhs *route, filter sqlparser.Expr) bool {
	comparison, ok := filter.(*sqlparser.ComparisonExpr)
	if !ok {
		return false
	}
	if comparison.Operator != sqlparser.EqualStr {
		return false
	}
	left := comparison.Left
	right := comparison.Right
	lVindex := rb.Symtab().Vindex(left, rb, false)
	if lVindex == nil {
		left, right = right, left
		lVindex = rb.Symtab().Vindex(left, rb, false)
	}
	if lVindex == nil || !vindexes.IsUnique(lVindex) {
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

// PushFilter pushes the filter into the route. The primitive will
// be updated if the new filter improves it.
func (rb *route) PushFilter(filter sqlparser.Expr, whereType string) error {
	if rb.IsRHS {
		return errors.New("unsupported: complex left join and where clause")
	}
	sel := rb.Select.(*sqlparser.Select)
	switch whereType {
	case sqlparser.WhereStr:
		sel.AddWhere(filter)
	case sqlparser.HavingStr:
		sel.AddHaving(filter)
	}
	rb.UpdatePlan(filter)
	return nil
}

// UpdatePlan evaluates the primitive against the specified
// filter. If it's an improvement, the primitive is updated.
// We assume that the filter has already been pushed into
// the route. This function should only be used when merging
// routes, where the ON clause gets implicitly pushed into
// the merged route.
func (rb *route) UpdatePlan(filter sqlparser.Expr) {
	opcode, vindex, values := rb.computePlan(filter)
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

func (rb *route) updateRoute(opcode engine.RouteOpcode, vindex vindexes.Vindex, values interface{}) {
	rb.ERoute.Opcode = opcode
	rb.ERoute.Vindex = vindex
	rb.ERoute.Values = values
}

// ComputePlan computes the plan for the specified filter.
func (rb *route) computePlan(filter sqlparser.Expr) (opcode engine.RouteOpcode, vindex vindexes.Vindex, values interface{}) {
	switch node := filter.(type) {
	case *sqlparser.ComparisonExpr:
		switch node.Operator {
		case sqlparser.EqualStr:
			return rb.computeEqualPlan(node)
		case sqlparser.InStr:
			return rb.computeINPlan(node)
		}
	case *sqlparser.ParenExpr:
		return rb.computePlan(node.Expr)
	}
	return engine.SelectScatter, nil, nil
}

// computeEqualPlan computes the plan for an equality constraint.
func (rb *route) computeEqualPlan(comparison *sqlparser.ComparisonExpr) (opcode engine.RouteOpcode, vindex vindexes.Vindex, values interface{}) {
	left := comparison.Left
	right := comparison.Right
	vindex = rb.Symtab().Vindex(left, rb, true)
	if vindex == nil {
		left, right = right, left
		vindex = rb.Symtab().Vindex(left, rb, true)
		if vindex == nil {
			return engine.SelectScatter, nil, nil
		}
	}
	if !exprIsValue(right, rb) {
		return engine.SelectScatter, nil, nil
	}
	if vindexes.IsUnique(vindex) {
		return engine.SelectEqualUnique, vindex, right
	}
	return engine.SelectEqual, vindex, right
}

// computeINPlan computes the plan for an IN constraint.
func (rb *route) computeINPlan(comparison *sqlparser.ComparisonExpr) (opcode engine.RouteOpcode, vindex vindexes.Vindex, values interface{}) {
	vindex = rb.Symtab().Vindex(comparison.Left, rb, true)
	if vindex == nil {
		return engine.SelectScatter, nil, nil
	}
	switch node := comparison.Right.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			if !exprIsValue(n, rb) {
				return engine.SelectScatter, nil, nil
			}
		}
		return engine.SelectIN, vindex, comparison
	case sqlparser.ListArg:
		return engine.SelectIN, vindex, comparison
	}
	return engine.SelectScatter, nil, nil
}

// PushSelect pushes the select expression into the route.
func (rb *route) PushSelect(expr *sqlparser.NonStarExpr, _ *route) (colsym *colsym, colnum int, err error) {
	colsym = newColsym(rb, rb.Symtab())
	colsym.Alias = expr.As
	if col, ok := expr.Expr.(*sqlparser.ColName); ok {
		// If no alias was specified, then the base name
		// of the column becomes the alias.
		if colsym.Alias.IsEmpty() {
			colsym.Alias = col.Name
		}
		colsym.Vindex = rb.Symtab().Vindex(col, rb, true)
		colsym.Underlying = newColref(col)
	} else {
		if rb.IsRHS {
			return nil, 0, errors.New("unsupported: complex left join and column expressions")
		}
		// We should ideally generate an alias based on the
		// expression, but we currently don't have the ability
		// to reference such expressions. So, we leave the
		// alias blank.
	}
	sel := rb.Select.(*sqlparser.Select)
	sel.SelectExprs = append(sel.SelectExprs, expr)
	rb.Colsyms = append(rb.Colsyms, colsym)
	return colsym, len(rb.Colsyms) - 1, nil
}

// PushStar pushes the '*' expression into the route.
func (rb *route) PushStar(expr *sqlparser.StarExpr) *colsym {
	// We just create a place-holder colsym. It won't
	// match anything.
	colsym := newColsym(rb, rb.Symtab())
	sel := rb.Select.(*sqlparser.Select)
	sel.SelectExprs = append(sel.SelectExprs, expr)
	rb.Colsyms = append(rb.Colsyms, colsym)
	return colsym
}

// MakeDistinct sets the DISTINCT property to the select.
func (rb *route) MakeDistinct() {
	rb.Select.(*sqlparser.Select).Distinct = sqlparser.DistinctStr
}

// SetGroupBy sets the GROUP BY clause for the route.
func (rb *route) SetGroupBy(groupBy sqlparser.GroupBy) {
	rb.Select.(*sqlparser.Select).GroupBy = groupBy
}

// AddOrder adds an ORDER BY expression to the route.
func (rb *route) AddOrder(order *sqlparser.Order) error {
	if rb.IsRHS {
		return errors.New("unsupported: complex left join and order by")
	}
	rb.Select.AddOrder(order)
	return nil
}

// SetLimit adds a LIMIT clause to the route.
func (rb *route) SetLimit(limit *sqlparser.Limit) {
	rb.Select.SetLimit(limit)
}

// PushOrderByNull updates the comments & 'for update' sections of the route.
func (rb *route) PushOrderByNull() {
	rb.Select.(*sqlparser.Select).OrderBy = sqlparser.OrderBy{&sqlparser.Order{Expr: &sqlparser.NullVal{}}}
}

// PushMisc updates the comments & 'for update' sections of the route.
func (rb *route) PushMisc(sel *sqlparser.Select) {
	rb.Select.(*sqlparser.Select).Comments = sel.Comments
	rb.Select.(*sqlparser.Select).Lock = sel.Lock
}

// Wireup performs the wire-up tasks.
func (rb *route) Wireup(bldr builder, jt *jointab) error {
	// Resolve values stored in the builder.
	var err error
	switch vals := rb.ERoute.Values.(type) {
	case *sqlparser.ComparisonExpr:
		// A comparison expression is stored only if it was an IN clause.
		// We have to convert it to use a list argutment and resolve values.
		rb.ERoute.Values, err = rb.procureValues(bldr, jt, vals.Right)
		if err != nil {
			return err
		}
		vals.Right = sqlparser.ListArg("::" + engine.ListVarName)
	default:
		rb.ERoute.Values, err = rb.procureValues(bldr, jt, vals)
		if err != nil {
			return err
		}
	}

	// Fix up the AST.
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.Select:
			if len(node.SelectExprs) == 0 {
				node.SelectExprs = sqlparser.SelectExprs([]sqlparser.SelectExpr{
					&sqlparser.NonStarExpr{
						Expr: sqlparser.NewIntVal([]byte{'1'}),
					},
				})
			}
		case *sqlparser.ComparisonExpr:
			if node.Operator == sqlparser.EqualStr {
				if exprIsValue(node.Left, rb) && !exprIsValue(node.Right, rb) {
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
				rb.ERoute.JoinVars[joinVar] = struct{}{}
				buf.Myprintf("%a", ":"+joinVar)
				return
			}
		case *sqlparser.TableName:
			node.Name.Format(buf)
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

// procureValues procures and converts the input into
// the expected types for rb.Values.
func (rb *route) procureValues(bldr builder, jt *jointab, val interface{}) (interface{}, error) {
	switch val := val.(type) {
	case nil:
		return nil, nil
	case sqlparser.ValTuple:
		vals := make([]interface{}, 0, len(val))
		for _, val := range val {
			v, err := rb.procureValues(bldr, jt, val)
			if err != nil {
				return nil, err
			}
			vals = append(vals, v)
		}
		return vals, nil
	case *sqlparser.ColName:
		joinVar := jt.Procure(bldr, val, rb.Order())
		rb.ERoute.JoinVars[joinVar] = struct{}{}
		return ":" + joinVar, nil
	case sqlparser.ListArg:
		return string(val), nil
	case sqlparser.Expr:
		return valConvert(val)
	}
	panic("unrecognized symbol")
}

func (rb *route) isLocal(col *sqlparser.ColName) bool {
	return col.Metadata.(sym).Route() == rb
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
		case *sqlparser.TableName:
			node.Name.Format(buf)
			return
		}
		sqlparser.FormatImpossibleQuery(buf, node)
	}

	return sqlparser.NewTrackedBuffer(formatter).WriteNode(sel).ParsedQuery().Query
}

// SupplyVar should be unreachable.
func (rb *route) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("unreachable")
}

// SupplyCol changes the router to supply the requested column
// name, and returns the result column number. If the column
// is already in the list, it's reused.
func (rb *route) SupplyCol(ref colref) int {
	for i, colsym := range rb.Colsyms {
		if colsym.Underlying == ref {
			return i
		}
	}
	rb.Colsyms = append(rb.Colsyms, &colsym{Underlying: ref})
	sel := rb.Select.(*sqlparser.Select)
	sel.SelectExprs = append(
		sel.SelectExprs,
		&sqlparser.NonStarExpr{
			Expr: &sqlparser.ColName{
				Metadata:  ref.Meta,
				Qualifier: &sqlparser.TableName{Name: ref.Meta.(*tabsym).ASTName},
				Name:      ref.Name(),
			},
		},
	)
	return len(rb.Colsyms) - 1
}

// IsSingle returns true if the route targets only one database.
func (rb *route) IsSingle() bool {
	return rb.ERoute.Opcode == engine.SelectUnsharded || rb.ERoute.Opcode == engine.SelectEqualUnique
}
