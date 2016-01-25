// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// PlanBuilder represents any object that's used to
// build a plan. The top-level PlanBuilder will be a
// tree that points to other PlanBuilder objects.
// Currently, JoinBuilder and RouteBuilder are the
// only two supported PlanBuilder objects. More will be
// added as we extend the functionality.
// Each Builder object builds a Plan object, and they
// will mirror the same tree. Once all the plans are built,
// the builder objects will be discarded, and only
// the Plan objects will remain.
type PlanBuilder interface {
	// Order is a number that signifies execution order.
	// A lower Order number Route is executed before a
	// higher one. For a node that contains other nodes,
	// the Order represents the highest order of the leaf
	// nodes.
	Order() int
}

// JoinBuilder is used to build a Join primitive.
// It's used to buid a normal join or a left join
// operation.
// TODO(sougou): struct is incomplete.
type JoinBuilder struct {
	// IsLeft is true if the operation is a left join.
	IsLeft bool
	order  int
	// Left and Right are the nodes for the join.
	Left, Right PlanBuilder
}

// Order returns the order of the node.
func (jb *JoinBuilder) Order() int {
	return jb.order
}

// MarshalJSON marshals JoinBuilder into a readable form.
// It's used for testing and diagnostics. The representation
// cannot be used to reconstruct a JoinBuilder.
func (jb *JoinBuilder) MarshalJSON() ([]byte, error) {
	marshalJoin := struct {
		IsLeft      bool
		Order       int
		Left, Right PlanBuilder
	}{
		IsLeft: jb.IsLeft,
		Order:  jb.order,
		Left:   jb.Left,
		Right:  jb.Right,
	}
	return json.Marshal(marshalJoin)
}

// RouteBuilder is used to build a Route primitive.
// It's used to build one of the Select routes like
// SelectScatter, etc. Portions of the original Select AST
// are moved into this node, which will be used to build
// the final SQL for this route.
// TODO(sougou): struct is incomplete.
type RouteBuilder struct {
	// From points to the portion of the AST this route represents.
	From  sqlparser.TableExpr
	order int
	// Route is the plan object being built. It will contain all the
	// information necessary to execute the route operation.
	Route *Route
}

// Order returns the order of the node.
func (rtb *RouteBuilder) Order() int {
	return rtb.order
}

// MarshalJSON marshals RouteBuilder into a readable form.
// It's used for testing and diagnostics. The representation
// cannot be used to reconstruct a RouteBuilder.
func (rtb *RouteBuilder) MarshalJSON() ([]byte, error) {
	marshalRoute := struct {
		From  string `json:",omitempty"`
		Order int
		Route *Route
	}{
		From:  sqlparser.String(rtb.From),
		Order: rtb.order,
		Route: rtb.Route,
	}
	return json.Marshal(marshalRoute)
}

// Route is a Plan object that represents a route.
// It can be any one of the Select primitives from PlanID.
// Some plan ids correspond to a multi-shard query,
// and some are for a single-shard query. The rules
// of what can be merged, or what can be pushed down
// depend on the PlanID. They're explained in code
// where such decisions are made.
// TODO(sougou): struct is incomplete.
// TODO(sougou): integrate with the older v3 Plan.
type Route struct {
	// PlanID will be one of the Select IDs from PlanID.
	PlanID PlanID
	// Keypsace represents the keyspace to which
	// the query will be sent.
	Keyspace *Keyspace
	// Vindex represents the vindex that will be used
	// to resolve the route.
	Vindex Vindex `json:",omitempty"`
	// Values represents a single value or a list of
	// values that will be used as input to the Vindex
	// to compute the target shard(s) where the query must
	// be sent.
	// TODO(sougou): explain contents of Values.
	Values interface{} `json:",omitempty"`
}

// buildSelectPlan2 is the new function to build a Select plan.
// TODO(sougou): rename after deprecating old one.
func buildSelectPlan2(sel *sqlparser.Select, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	return processTableExprs(sel.From, schema)
}

// processTableExprs analyzes the FROM clause. It produces a PlanBuilder
// and the associated SymbolTable with all the routes identified.
func processTableExprs(tableExprs sqlparser.TableExprs, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	if len(tableExprs) != 1 {
		// TODO(sougou): better error message.
		return nil, nil, errors.New("no list")
	}
	return processTableExpr(tableExprs[0], schema)
}

// processTableExpr produces a PlanBuilder subtree and SymbolTable
// for the given TableExpr.
func processTableExpr(tableExpr sqlparser.TableExpr, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return processAliasedTable(tableExpr, schema)
	case *sqlparser.ParenTableExpr:
		planBuilder, symbols, err := processTableExprs(tableExpr.Exprs, schema)
		if route, ok := planBuilder.(*RouteBuilder); ok {
			route.From = tableExpr
		}
		return planBuilder, symbols, err
	case *sqlparser.JoinTableExpr:
		return processJoin(tableExpr, schema)
	}
	panic("unreachable")
}

// processAliasedTable produces a PlanBuilder subtree and SymbolTable
// for the given AliasedTableExpr.
func processAliasedTable(tableExpr *sqlparser.AliasedTableExpr, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	switch expr := tableExpr.Expr.(type) {
	case *sqlparser.TableName:
		route, table, err := getTablePlan(expr, schema)
		if err != nil {
			return nil, nil, err
		}
		planBuilder := &RouteBuilder{
			From:  tableExpr,
			order: 1,
			Route: route,
		}
		alias := expr.Name
		if tableExpr.As != "" {
			alias = tableExpr.As
		}
		symbols := NewSymbolTable(alias, table, planBuilder)
		return planBuilder, symbols, nil
	case *sqlparser.Subquery:
		// TODO(sougou): implement.
		return nil, nil, errors.New("no subqueries")
	}
	panic("unreachable")
}

// getTablePlan produces the initial Route for the specified TableName.
// It also returns the associated vschema info (*Table) so that
// it can be used to create the symbol table entry.
func getTablePlan(tableName *sqlparser.TableName, schema *Schema) (*Route, *Table, error) {
	if tableName.Qualifier != "" {
		// TODO(sougou): better error message.
		return nil, nil, errors.New("tablename qualifier not allowed")
	}
	table, reason := schema.FindTable(string(tableName.Name))
	if reason != "" {
		return nil, nil, errors.New(reason)
	}
	if table.Keyspace.Sharded {
		return &Route{
			PlanID:   SelectScatter,
			Keyspace: table.Keyspace,
		}, table, nil
	}
	return &Route{
		PlanID:   SelectUnsharded,
		Keyspace: table.Keyspace,
	}, table, nil
}

// processJoin produces a PlanBuilder subtree and SymbolTable
// for the given Join. If the left and right nodes can be part
// of the same route, then it's a RouteBuilder. Otherwise,
// it's a JoinBuilder.
func processJoin(join *sqlparser.JoinTableExpr, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	switch join.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr, sqlparser.LeftJoinStr:
	default:
		// TODO(sougou): better error message.
		return nil, nil, errors.New("unsupported join")
	}
	lplanBuilder, lsymbols, err := processTableExpr(join.LeftExpr, schema)
	if err != nil {
		return nil, nil, err
	}
	rplanBuilder, rsymbols, err := processTableExpr(join.RightExpr, schema)
	if err != nil {
		return nil, nil, err
	}
	switch lplanBuilder := lplanBuilder.(type) {
	case *JoinBuilder:
		return makeJoinBuilder(lplanBuilder, lsymbols, rplanBuilder, rsymbols, join)
	case *RouteBuilder:
		switch rplanBuilder := rplanBuilder.(type) {
		case *JoinBuilder:
			return makeJoinBuilder(lplanBuilder, lsymbols, rplanBuilder, rsymbols, join)
		case *RouteBuilder:
			return joinRoutes(lplanBuilder, lsymbols, rplanBuilder, rsymbols, join)
		}
	}
	panic("unreachable")
}

// makeJoinBuilder creates a new JoinBuilder node out of the two builders.
// This function is called when the two builders cannot be part of
// the same route.
func makeJoinBuilder(lplanBuilder PlanBuilder, lsymbols *SymbolTable, rplanBuilder PlanBuilder, rsymbols *SymbolTable, join *sqlparser.JoinTableExpr) (PlanBuilder, *SymbolTable, error) {
	err := lsymbols.Add(rsymbols)
	if err != nil {
		return nil, nil, err
	}
	isLeft := false
	if join.Join == sqlparser.LeftJoinStr {
		isLeft = true
	}
	assignOrder(rplanBuilder, lplanBuilder.Order())
	return &JoinBuilder{
		IsLeft: isLeft,
		order:  rplanBuilder.Order(),
		Left:   lplanBuilder,
		Right:  rplanBuilder,
	}, lsymbols, nil
}

// assignOrder sets the order for the nodes of the tree based on the
// starting order.
func assignOrder(planBuilder PlanBuilder, order int) {
	switch planBuilder := planBuilder.(type) {
	case *JoinBuilder:
		assignOrder(planBuilder.Left, order)
		assignOrder(planBuilder.Right, planBuilder.Left.Order())
		planBuilder.order = planBuilder.Right.Order()
	case *RouteBuilder:
		planBuilder.order = order + 1
	}
}

// joinRoutes attempts to join two RouteBuilder objects into one.
// If it's possible, it produces a joined RouteBuilder.
// Otherwise, it's a JoinBuilder.
func joinRoutes(lRouteBuilder *RouteBuilder, lsymbols *SymbolTable, rRouteBuilder *RouteBuilder, rsymbols *SymbolTable, join *sqlparser.JoinTableExpr) (PlanBuilder, *SymbolTable, error) {
	if lRouteBuilder.Route.Keyspace.Name != rRouteBuilder.Route.Keyspace.Name {
		return makeJoinBuilder(lRouteBuilder, lsymbols, rRouteBuilder, rsymbols, join)
	}
	if lRouteBuilder.Route.PlanID == SelectUnsharded {
		if rRouteBuilder.Route.PlanID == SelectUnsharded {
			// Two Routes from the same unsharded keyspace can be merged.
			return mergeRoutes(lRouteBuilder, lsymbols, rsymbols, join)
		}
		return makeJoinBuilder(lRouteBuilder, lsymbols, rRouteBuilder, rsymbols, join)
	}
	// lRouteBuilder is a sharded route. It can't merge with an unsharded route.
	if rRouteBuilder.Route.PlanID == SelectUnsharded {
		return makeJoinBuilder(lRouteBuilder, lsymbols, rRouteBuilder, rsymbols, join)
	}
	// TODO(sougou): Handle special case for SelectEqual and SelectKeyrange.
	// Both RouteBuilder are sharded routes. Analyze join condition for merging.
	return joinShardedRoutes(lRouteBuilder, lsymbols, rRouteBuilder, rsymbols, join)
}

// mergeRoutes makes a new RouteBuilder by joining the left and right
// nodes of a join. This is called if two routes can be merged.
func mergeRoutes(lRouteBuilder *RouteBuilder, lsymbols, rsymbols *SymbolTable, join *sqlparser.JoinTableExpr) (PlanBuilder, *SymbolTable, error) {
	lRouteBuilder.From = join
	err := lsymbols.Merge(rsymbols, lRouteBuilder)
	if err != nil {
		return nil, nil, err
	}
	return lRouteBuilder, lsymbols, nil
}

// joinShardedRoutes tries to join two sharded routes into a RouteBuilder.
// If a merge is possible, it builds one using lRouteBuilder as the base route.
// If not, it builds a JoinBuilder instead.
func joinShardedRoutes(lRouteBuilder *RouteBuilder, lsymbols *SymbolTable, rRouteBuilder *RouteBuilder, rsymbols *SymbolTable, join *sqlparser.JoinTableExpr) (PlanBuilder, *SymbolTable, error) {
	onFilters := appendFilters(nil, join.On)
	for _, filter := range onFilters {
		if !isSameRoute(filter, lsymbols, rsymbols) {
			continue
		}
		return mergeRoutes(lRouteBuilder, lsymbols, rsymbols, join)
	}
	return makeJoinBuilder(lRouteBuilder, lsymbols, rRouteBuilder, rsymbols, join)
}

// isSameRoute returns true if the filter constraint causes the
// left and right routes to be part of the same route. For this
// to happen, the constraint has to be an equality like a.id = b.id,
// one should address a table from the left side, the other from the
// right, the referenced columns have to be the same Vindex, and the
// Vindex must be unique.
func isSameRoute(filter sqlparser.BoolExpr, lsymbols, rsymbols *SymbolTable) bool {
	comparison, ok := filter.(*sqlparser.ComparisonExpr)
	if !ok {
		return false
	}
	if comparison.Operator != sqlparser.EqualStr {
		return false
	}
	lcol, ok := comparison.Left.(*sqlparser.ColName)
	if !ok {
		return false
	}
	rcol, ok := comparison.Right.(*sqlparser.ColName)
	if !ok {
		return false
	}
	_, lColVindex := lsymbols.FindColumn(lcol, false)
	if lColVindex == nil {
		lcol, rcol = rcol, lcol
		_, lColVindex = lsymbols.FindColumn(lcol, false)
	}
	if lColVindex == nil || !IsUnique(lColVindex.Vindex) {
		return false
	}
	_, rColVindex := rsymbols.FindColumn(rcol, false)
	if rColVindex == nil {
		return false
	}
	if rColVindex.Vindex != lColVindex.Vindex {
		return false
	}
	return true
}
