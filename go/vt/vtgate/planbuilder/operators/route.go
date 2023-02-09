/*
Copyright 2021 The Vitess Authors.

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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	Route struct {
		Source ops.Operator

		// Routes that have been merged into this one.
		MergedWith []*Route

		Routing Routing
	}

	// VindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
	VindexPlusPredicates struct {
		TableID   semantics.TableSet
		ColVindex *vindexes.ColumnVindex

		// during planning, we store the alternatives found for this route in this slice
		Options []*VindexOption
	}

	// VindexOption stores the information needed to know if we have all the information needed to use a vindex
	VindexOption struct {
		Ready  bool
		Values []evalengine.Expr
		// columns that we have seen so far. Used only for multi-column vindexes so that we can track how many columns part of the vindex we have seen
		ColsSeen    map[string]any
		ValueExprs  []sqlparser.Expr
		Predicates  []sqlparser.Expr
		OpCode      engine.Opcode
		FoundVindex vindexes.Vindex
		Cost        Cost
	}

	// Cost is used to make it easy to compare the Cost of two plans with each other
	Cost struct {
		VindexCost int
		IsUnique   bool
		OpCode     engine.Opcode
	}

	Routing interface {
		UpdateRoutingParams(rp *engine.RoutingParameters)
		Clone() Routing
		UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error)
		Cost() int
		OpCode() engine.Opcode
		Keyspace() *vindexes.Keyspace // note that all routings do not have a keyspace, so this method can return nil
	}
)

var _ ops.PhysicalOperator = (*Route)(nil)

// IPhysical implements the PhysicalOperator interface
func (*Route) IPhysical() {}

// Cost implements the Operator interface
func (r *Route) Cost() int {
	return r.Routing.Cost()
}

// Clone implements the Operator interface
func (r *Route) Clone(inputs []ops.Operator) ops.Operator {
	cloneRoute := *r
	cloneRoute.Source = inputs[0]
	cloneRoute.Routing = r.Routing.Clone()
	return &cloneRoute
}

// Inputs implements the Operator interface
func (r *Route) Inputs() []ops.Operator {
	return []ops.Operator{r.Source}
}

func createOption(
	colVindex *vindexes.ColumnVindex,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) *VindexOption {
	values := make([]evalengine.Expr, len(colVindex.Columns))
	predicates := make([]sqlparser.Expr, len(colVindex.Columns))
	vindex := vfunc(colVindex)

	return &VindexOption{
		Values:      values,
		Predicates:  predicates,
		ColsSeen:    map[string]any{},
		FoundVindex: vindex,
	}
}

func copyOption(orig *VindexOption) *VindexOption {
	colsSeen := make(map[string]any, len(orig.ColsSeen))
	valueExprs := make([]sqlparser.Expr, len(orig.ValueExprs))
	values := make([]evalengine.Expr, len(orig.Values))
	predicates := make([]sqlparser.Expr, len(orig.Predicates))

	copy(values, orig.Values)
	copy(valueExprs, orig.ValueExprs)
	copy(predicates, orig.Predicates)
	for k, v := range orig.ColsSeen {
		colsSeen[k] = v
	}
	vo := &VindexOption{
		Values:      values,
		ColsSeen:    colsSeen,
		ValueExprs:  valueExprs,
		Predicates:  predicates,
		OpCode:      orig.OpCode,
		FoundVindex: orig.FoundVindex,
		Cost:        orig.Cost,
	}
	return vo
}

func (option *VindexOption) updateWithNewColumn(
	colLoweredName string,
	valueExpr sqlparser.Expr,
	indexOfCol int,
	value evalengine.Expr,
	node sqlparser.Expr,
	colVindex *vindexes.ColumnVindex,
	opcode func(*vindexes.ColumnVindex) engine.Opcode,
) bool {
	option.ColsSeen[colLoweredName] = true
	option.ValueExprs = append(option.ValueExprs, valueExpr)
	option.Values[indexOfCol] = value
	option.Predicates[indexOfCol] = node
	option.Ready = len(option.ColsSeen) == len(colVindex.Columns)
	routeOpcode := opcode(colVindex)
	if option.OpCode < routeOpcode {
		option.OpCode = routeOpcode
		option.Cost = costFor(colVindex, routeOpcode)
	}
	return option.Ready
}

func (r *Route) IsSingleShard() bool {
	switch r.Routing.OpCode() {
	case engine.Unsharded, engine.DBA, engine.Next, engine.EqualUnique, engine.Reference:
		return true
	}
	return false
}

func tupleAccess(expr sqlparser.Expr, coordinates []int) sqlparser.Expr {
	tuple, _ := expr.(sqlparser.ValTuple)
	for _, idx := range coordinates {
		if idx >= len(tuple) {
			return nil
		}
		expr = tuple[idx]
		tuple, _ = expr.(sqlparser.ValTuple)
	}
	return expr
}

func equalOrEqualUnique(vindex *vindexes.ColumnVindex) engine.Opcode {
	if vindex.IsPartialVindex() {
		return engine.SubShard
	}
	if vindex.IsUnique() {
		return engine.EqualUnique
	}

	return engine.Equal
}

func justTheVindex(vindex *vindexes.ColumnVindex) vindexes.Vindex {
	return vindex.Vindex
}

// costFor returns a cost struct to make route choices easier to compare
func costFor(foundVindex *vindexes.ColumnVindex, opcode engine.Opcode) Cost {
	switch opcode {
	// For these opcodes, we should not have a vindex, so we just return the opcode as the cost
	case engine.Unsharded, engine.Next, engine.DBA, engine.Reference, engine.None, engine.Scatter:
		return Cost{
			OpCode: opcode,
		}
	}

	return Cost{
		VindexCost: foundVindex.Cost(),
		IsUnique:   foundVindex.IsUnique(),
		OpCode:     opcode,
	}
}

// less compares two costs and returns true if the first cost is cheaper than the second
func less(c1, c2 Cost) bool {
	switch {
	case c1.OpCode != c2.OpCode:
		return c1.OpCode < c2.OpCode
	case c1.IsUnique == c2.IsUnique:
		return c1.VindexCost <= c2.VindexCost
	default:
		return c1.IsUnique
	}
}

func (vpp *VindexPlusPredicates) bestOption() *VindexOption {
	var best *VindexOption
	var keepOptions []*VindexOption
	for _, option := range vpp.Options {
		if option.Ready {
			if best == nil || less(option.Cost, best.Cost) {
				best = option
			}
		} else {
			keepOptions = append(keepOptions, option)
		}
	}
	if best != nil {
		keepOptions = append(keepOptions, best)
	}
	vpp.Options = keepOptions
	return best
}

// createRoute returns either an information_schema route, or else consults the
// VSchema to find a suitable table, and then creates a route from that.
func createRoute(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	solves semantics.TableSet,
) (ops.Operator, error) {
	if queryTable.IsInfSchema {
		return createInfSchemaRoute(ctx, queryTable)
	}
	return findVSchemaTableAndCreateRoute(ctx, queryTable, queryTable.Table, solves, true /*planAlternates*/)
}

// findVSchemaTableAndCreateRoute consults the VSchema to find a suitable
// table, and then creates a route from that.
func findVSchemaTableAndCreateRoute(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	tableName sqlparser.TableName,
	solves semantics.TableSet,
	planAlternates bool,
) (*Route, error) {
	vschemaTable, _, _, _, target, err := ctx.VSchema.FindTableOrVindex(tableName)
	if target != nil {
		return nil, vterrors.VT12001("SELECT with a target destination")
	}
	if err != nil {
		return nil, err
	}

	return createRouteFromVSchemaTable(
		ctx,
		queryTable,
		vschemaTable,
		solves,
		planAlternates,
	)
}

// createRouteFromTable creates a route from the given VSchema table.
func createRouteFromVSchemaTable(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	vschemaTable *vindexes.Table,
	solves semantics.TableSet,
	planAlternates bool,
) (*Route, error) {
	if vschemaTable.Name.String() != queryTable.Table.Name.String() {
		// we are dealing with a routed table
		queryTable = queryTable.Clone()
		name := queryTable.Table.Name
		queryTable.Table.Name = vschemaTable.Name
		astTable, ok := queryTable.Alias.Expr.(sqlparser.TableName)
		if !ok {
			return nil, vterrors.VT13001("a derived table should never be a routed table")
		}
		realTableName := sqlparser.NewIdentifierCS(vschemaTable.Name.String())
		astTable.Name = realTableName
		if queryTable.Alias.As.IsEmpty() {
			// if the user hasn't specified an alias, we'll insert one here so the old table name still works
			queryTable.Alias.As = sqlparser.NewIdentifierCS(name.String())
		}
	}
	plan := &Route{
		Source: &Table{
			QTable: queryTable,
			VTable: vschemaTable,
		},
	}

	// We create the appropiate Routing struct here, depending on the type of table we are dealing with.
	routing := createRoutingForVTable(vschemaTable, solves)
	for _, predicate := range queryTable.Predicates {
		var err error
		plan.Routing, err = routing.UpdateRoutingLogic(ctx, predicate)
		if err != nil {
			return nil, err
		}
	}

	plan.Routing = routing

	tr, ok := plan.Routing.(*ShardedRouting)
	if !ok {
		// if we don't have a ShardedRouting, the rest of the logic doesn't really make sense,
		// so we can bail out early
		return plan, nil
	}

	if tr.isScatter() && len(queryTable.Predicates) > 0 {
		var err error
		// If we have a scatter query, it's worth spending a little extra time seeing if we can't improve it
		plan.Routing, err = tr.tryImprove(ctx, queryTable)
		if err != nil {
			return nil, err
		}
	}

	// if planAlternates {
	// 	alternates, err := createAlternateRoutesFromVSchemaTable(ctx, queryTable, vschemaTable, solves)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	plan.Alternates = alternates
	// }

	return plan, nil
}

func createRoutingForVTable(vschemaTable *vindexes.Table, id semantics.TableSet) Routing {
	switch {
	case vschemaTable.Type == vindexes.TypeSequence:
		return &SequenceRouting{}
	case vschemaTable.Type == vindexes.TypeReference && vschemaTable.Name.String() == "dual":
		return &DualRouting{}
	case vschemaTable.Type == vindexes.TypeReference:
		return &ReferenceRouting{keyspace: vschemaTable.Keyspace}
	case !vschemaTable.Keyspace.Sharded:
		return &UnshardedRouting{keyspace: vschemaTable.Keyspace}
	default:
		return newShardedRouting(vschemaTable, id)
	}
}

func createAlternateRoutesFromVSchemaTable(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	vschemaTable *vindexes.Table,
	solves semantics.TableSet,
) (map[*vindexes.Keyspace]*Route, error) {
	routes := make(map[*vindexes.Keyspace]*Route)

	switch vschemaTable.Type {
	case "", vindexes.TypeReference:
		for ksName, referenceTable := range vschemaTable.ReferencedBy {
			route, err := findVSchemaTableAndCreateRoute(
				ctx,
				queryTable,
				sqlparser.TableName{
					Name:      referenceTable.Name,
					Qualifier: sqlparser.NewIdentifierCS(ksName),
				},
				solves,
				false, /*planAlternates*/
			)
			if err != nil {
				return nil, err
			}
			routes[referenceTable.Keyspace] = route
		}

		if vschemaTable.Source != nil {
			route, err := findVSchemaTableAndCreateRoute(
				ctx,
				queryTable,
				vschemaTable.Source.TableName,
				solves,
				false, /*planAlternates*/
			)
			if err != nil {
				return nil, err
			}
			routes[vschemaTable.Keyspace] = route
		}
	}

	return routes, nil
}

func (r *Route) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	// first we see if the predicate changes how we route
	newRouting, err := r.Routing.UpdateRoutingLogic(ctx, expr)
	if err != nil {
		return nil, err
	}
	r.Routing = newRouting

	// we also need to push the predicate down into the query
	newSrc, err := r.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	r.Source = newSrc
	return r, err
}

func (r *Route) AddColumn(ctx *plancontext.PlanningContext, e sqlparser.Expr) (int, error) {
	return r.Source.AddColumn(ctx, e)
}

// func (r *Route) AlternateInKeyspace(keyspace *vindexes.Keyspace) *Route {
// 	if keyspace.Name == r.Keyspace.Name {
// 		return nil
// 	}
//
// 	if route, ok := r.Alternates[keyspace]; ok {
// 		return route
// 	}
//
// 	return nil
// }

// TablesUsed returns tables used by MergedWith routes, which are not included
// in Inputs() and thus not a part of the operator tree
func (r *Route) TablesUsed() []string {
	addString, collect := collectSortedUniqueStrings()
	for _, mw := range r.MergedWith {
		for _, u := range TablesUsed(mw) {
			addString(u)
		}
	}
	return collect()
}
