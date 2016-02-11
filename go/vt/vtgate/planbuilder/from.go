// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// processTableExprs analyzes the FROM clause. It produces a planBuilder
// and the associated symtab with all the routes identified.
func processTableExprs(tableExprs sqlparser.TableExprs, vschema *VSchema) (planBuilder, *symtab, error) {
	if len(tableExprs) != 1 {
		// TODO(sougou): better error message.
		return nil, nil, errors.New("lists are not supported")
	}
	return processTableExpr(tableExprs[0], vschema)
}

// processTableExpr produces a planBuilder subtree and symtab
// for the given TableExpr.
func processTableExpr(tableExpr sqlparser.TableExpr, vschema *VSchema) (planBuilder, *symtab, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return processAliasedTable(tableExpr, vschema)
	case *sqlparser.ParenTableExpr:
		plan, syms, err := processTableExprs(tableExpr.Exprs, vschema)
		// We want to point to the higher level parenthesis because
		// more routes can be merged with this one. If so, the order
		// should be maintained as dictated by the parenthesis.
		if route, ok := plan.(*routeBuilder); ok {
			route.Select.From = sqlparser.TableExprs{tableExpr}
		}
		return plan, syms, err
	case *sqlparser.JoinTableExpr:
		return processJoin(tableExpr, vschema)
	}
	panic("unreachable")
}

// processAliasedTable produces a planBuilder subtree and symtab
// for the given AliasedTableExpr.
func processAliasedTable(tableExpr *sqlparser.AliasedTableExpr, vschema *VSchema) (planBuilder, *symtab, error) {
	switch expr := tableExpr.Expr.(type) {
	case *sqlparser.TableName:
		route, table, err := getTablePlan(expr, vschema)
		if err != nil {
			return nil, nil, err
		}
		plan := &routeBuilder{
			Select: sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})},
			order:  1,
			Route:  route,
		}
		alias := expr.Name
		if tableExpr.As != "" {
			alias = tableExpr.As
		}
		syms := newSymtab(alias, table, plan, vschema)
		return plan, syms, nil
	case *sqlparser.Subquery:
		// TODO(sougou): implement.
		return nil, nil, errors.New("no subqueries")
	}
	panic("unreachable")
}

// getTablePlan produces the initial Route for the specified TableName.
// It also returns the associated vschema info (*Table) so that
// it can be used to create the symbol table entry.
func getTablePlan(tableName *sqlparser.TableName, vschema *VSchema) (*Route, *Table, error) {
	if tableName.Qualifier != "" {
		// TODO(sougou): better error message.
		return nil, nil, errors.New("tablename qualifier not allowed")
	}
	table, err := vschema.FindTable(string(tableName.Name))
	if err != nil {
		return nil, nil, err
	}
	if table.Keyspace.Sharded {
		return &Route{
			PlanID:   SelectScatter,
			Keyspace: table.Keyspace,
			JoinVars: make(map[string]struct{}),
		}, table, nil
	}
	return &Route{
		PlanID:   SelectUnsharded,
		Keyspace: table.Keyspace,
		JoinVars: make(map[string]struct{}),
	}, table, nil
}

// processJoin produces a planBuilder subtree and symtab
// for the given Join. If the left and right nodes can be part
// of the same route, then it's a routeBuilder. Otherwise,
// it's a joinBuilder.
func processJoin(join *sqlparser.JoinTableExpr, vschema *VSchema) (planBuilder, *symtab, error) {
	switch join.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr, sqlparser.LeftJoinStr:
	default:
		// TODO(sougou): better error message.
		return nil, nil, errors.New("unsupported join")
	}
	lplan, lsyms, err := processTableExpr(join.LeftExpr, vschema)
	if err != nil {
		return nil, nil, err
	}
	rplan, rsyms, err := processTableExpr(join.RightExpr, vschema)
	if err != nil {
		return nil, nil, err
	}
	switch lplan := lplan.(type) {
	case *joinBuilder:
		return makejoinBuilder(lplan, lsyms, rplan, rsyms, join)
	case *routeBuilder:
		switch rplan := rplan.(type) {
		case *joinBuilder:
			return makejoinBuilder(lplan, lsyms, rplan, rsyms, join)
		case *routeBuilder:
			return joinRoutes(lplan, lsyms, rplan, rsyms, join)
		}
	}
	panic("unreachable")
}

// makejoinBuilder creates a new joinBuilder node out of the two builders.
// This function is called when the two builders cannot be part of
// the same route.
func makejoinBuilder(lplan planBuilder, lsyms *symtab, rplan planBuilder, rsyms *symtab, join *sqlparser.JoinTableExpr) (planBuilder, *symtab, error) {
	// This function converts ON clauses to WHERE clauses. The WHERE clause
	// scope can see all tables, whereas the ON clause can only see the
	// participants of the JOIN. However, since the ON clause doesn't allow
	// external references, and the FROM clause doesn't allow duplicates,
	// it's safe to perform this conversion and still expect the same behavior.

	// For LEFT JOIN, you have to push the ON clause into the RHS first.
	isLeft := false
	if join.Join == sqlparser.LeftJoinStr {
		isLeft = true
		err := processBoolExpr(join.On, rsyms, sqlparser.WhereStr)
		if err != nil {
			return nil, nil, err
		}
		setRHS(rplan)
	}

	err := lsyms.Add(rsyms)
	if err != nil {
		return nil, nil, err
	}
	assignOrder(rplan, lplan.Order())
	// For normal joins, the ON clause can go to both sides.
	// The push has to happen after the order is assigned.
	if !isLeft {
		err := processBoolExpr(join.On, lsyms, sqlparser.WhereStr)
		if err != nil {
			return nil, nil, err
		}
	}
	return &joinBuilder{
		LeftOrder:  lplan.Order(),
		RightOrder: rplan.Order(),
		Left:       lplan,
		Right:      rplan,
		Join: &Join{
			IsLeft: isLeft,
			Left:   getUnderlyingPlan(lplan),
			Right:  getUnderlyingPlan(rplan),
			Vars:   make(map[string]int),
		},
	}, lsyms, nil
}

func getUnderlyingPlan(plan planBuilder) interface{} {
	switch plan := plan.(type) {
	case *joinBuilder:
		return plan.Join
	case *routeBuilder:
		return plan.Route
	}
	panic("unreachable")
}

// assignOrder sets the order for the nodes of the tree based on the
// starting order.
func assignOrder(plan planBuilder, order int) {
	switch plan := plan.(type) {
	case *joinBuilder:
		assignOrder(plan.Left, order)
		plan.LeftOrder = plan.Left.Order()
		assignOrder(plan.Right, plan.Left.Order())
		plan.RightOrder = plan.Right.Order()
	case *routeBuilder:
		plan.order = order + 1
	}
}

// setRHS marks all routes under the plan as RHS of a left join.
func setRHS(plan planBuilder) {
	switch plan := plan.(type) {
	case *joinBuilder:
		setRHS(plan.Left)
		setRHS(plan.Right)
	case *routeBuilder:
		plan.IsRHS = true
	}
}

// joinRoutes attempts to join two routeBuilder objects into one.
// If it's possible, it produces a joined routeBuilder.
// Otherwise, it's a joinBuilder.
func joinRoutes(lRoute *routeBuilder, lsyms *symtab, rRoute *routeBuilder, rsyms *symtab, join *sqlparser.JoinTableExpr) (planBuilder, *symtab, error) {
	if lRoute.Route.Keyspace.Name != rRoute.Route.Keyspace.Name {
		return makejoinBuilder(lRoute, lsyms, rRoute, rsyms, join)
	}
	if lRoute.Route.PlanID == SelectUnsharded {
		// Two Routes from the same unsharded keyspace can be merged.
		return mergeRoutes(lRoute, lsyms, rRoute, rsyms, join)
	}

	// TODO(sougou): Handle special case for SelectEqual

	// Both routeBuilder are sharded routes. Analyze join condition for merging.
	for _, filter := range splitAndExpression(nil, join.On) {
		if isSameRoute(lRoute, lsyms, rRoute, rsyms, filter) {
			return mergeRoutes(lRoute, lsyms, rRoute, rsyms, join)
		}
	}
	return makejoinBuilder(lRoute, lsyms, rRoute, rsyms, join)
}

// mergeRoutes makes a new routeBuilder by joining the left and right
// nodes of a join. The merged routeBuilder inherits the plan of the
// left Route. This function is called if two routes can be merged.
func mergeRoutes(lRoute *routeBuilder, lsyms *symtab, rRoute *routeBuilder, rsyms *symtab, join *sqlparser.JoinTableExpr) (planBuilder, *symtab, error) {
	lRoute.Select.From = sqlparser.TableExprs{join}
	if join.Join == sqlparser.LeftJoinStr {
		rsyms.SetRHS()
	}
	err := lsyms.Merge(rsyms, lRoute)
	if err != nil {
		return nil, nil, err
	}
	for _, filter := range splitAndExpression(nil, join.On) {
		// If VTGate evolves, this section should be rewritten
		// to use processBoolExpr.
		_, err = findRoute(filter, lsyms)
		if err != nil {
			return nil, nil, err
		}
		updateRoute(lRoute, lsyms, filter)
	}
	return lRoute, lsyms, nil
}

// isSameRoute returns true if the filter constraint causes the
// left and right routes to be part of the same route. For this
// to happen, the constraint has to be an equality like a.id = b.id,
// one should address a table from the left side, the other from the
// right, the referenced columns have to be the same Vindex, and the
// Vindex must be unique.
func isSameRoute(lRoute *routeBuilder, lsyms *symtab, rRoute *routeBuilder, rsyms *symtab, filter sqlparser.BoolExpr) bool {
	comparison, ok := filter.(*sqlparser.ComparisonExpr)
	if !ok {
		return false
	}
	if comparison.Operator != sqlparser.EqualStr {
		return false
	}
	left := comparison.Left
	right := comparison.Right
	lVindex := lsyms.Vindex(left, lRoute, false)
	if lVindex == nil {
		left, right = right, left
		lVindex = lsyms.Vindex(left, lRoute, false)
	}
	if lVindex == nil || !IsUnique(lVindex) {
		return false
	}
	rVindex := rsyms.Vindex(right, rRoute, false)
	if rVindex == nil {
		return false
	}
	if rVindex != lVindex {
		return false
	}
	return true
}
