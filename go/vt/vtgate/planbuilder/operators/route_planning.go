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
	"bytes"
	"fmt"
	"io"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type (
	tableSetPair struct {
		left, right semantics.TableSet
	}

	opCacheMap map[tableSetPair]Operator
)

// TransformToPhysical takes an operator tree and rewrites any parts that have not yet been planned as physical operators.
// This is where a lot of the optimisations of the query plans are done.
// Here we try to merge query parts into the same route primitives. At the end of this process,
// all the operators in the tree are guaranteed to be PhysicalOperators
func TransformToPhysical(ctx *plancontext.PlanningContext, in Operator) (Operator, error) {
	op, _, err := rewriteBottomUp(ctx, in, func(context *plancontext.PlanningContext, operator Operator) (newOp Operator, changed bool, err error) {
		switch op := operator.(type) {
		case *QueryGraph:
			return optimizeQueryGraph(ctx, op)
		case *Join:
			return optimizeJoin(ctx, op)
		case *Derived:
			return optimizeDerived(ctx, op)
		case *SubQuery:
			return optimizeSubQuery(ctx, op)
		case *Filter:
			return optimizeFilter(op)
		default:
			return operator, false, nil
		}
	})

	if err != nil {
		return nil, err
	}

	err = VisitTopDown(op, func(op Operator) error {
		if _, isPhys := op.(PhysicalOperator); !isPhys {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to transform %T to a physical operator", op)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return op, nil
}

func optimizeFilter(op *Filter) (Operator, bool, error) {
	if route, ok := op.Source.(*Route); ok {
		// let's push the filter into the route
		op.Source = route.Source
		route.Source = op
		return route, true, nil
	}

	return op, false, nil
}

func optimizeDerived(ctx *plancontext.PlanningContext, op *Derived) (Operator, bool, error) {
	innerRoute, ok := op.Source.(*Route)
	if !ok {
		return op, false, nil
	}

	if innerRoute.RouteOpCode == engine.EqualUnique {
		// no need to check anything if we are sure that we will only hit a single shard
	} else if !op.IsMergeable(ctx) {
		return op, false, nil
	}

	op.Source = innerRoute.Source
	innerRoute.Source = op

	return innerRoute, true, nil
}

func optimizeJoin(ctx *plancontext.PlanningContext, op *Join) (Operator, bool, error) {
	join, err := mergeOrJoin(ctx, op.LHS, op.RHS, sqlparser.SplitAndExpression(nil, op.Predicate), !op.LeftJoin)
	if err != nil {
		return nil, false, err
	}
	return join, true, nil
}

func optimizeQueryGraph(ctx *plancontext.PlanningContext, op *QueryGraph) (result Operator, changed bool, err error) {
	changed = true
	switch {
	case ctx.PlannerVersion == querypb.ExecuteOptions_Gen4Left2Right:
		result, err = leftToRightSolve(ctx, op)
	default:
		result, err = greedySolve(ctx, op)
	}

	unresolved := op.UnsolvedPredicates(ctx.SemTable)
	if len(unresolved) > 0 {
		// if we have any predicates that none of the joins or tables took care of,
		// we add a single filter on top, so we don't lose it. This is used for sub-query planning
		result = &Filter{Source: result, Predicates: unresolved}
	}

	return
}

func buildVindexTableForDML(ctx *plancontext.PlanningContext, tableInfo semantics.TableInfo, table *QueryTable, dmlType string) (*vindexes.Table, engine.Opcode, key.Destination, error) {
	vindexTable := tableInfo.GetVindexTable()
	opCode := engine.Unsharded
	if vindexTable.Keyspace.Sharded {
		opCode = engine.Scatter
	}

	var dest key.Destination
	var typ topodatapb.TabletType
	var err error
	tblName, ok := table.Alias.Expr.(sqlparser.TableName)
	if ok {
		_, _, _, typ, dest, err = ctx.VSchema.FindTableOrVindex(tblName)
		if err != nil {
			return nil, 0, nil, err
		}
		if dest != nil {
			if typ != topodatapb.TabletType_PRIMARY {
				return nil, 0, nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.InnodbReadOnly, "unsupported: %v statement with a replica target", dmlType)
			}
			// we are dealing with an explicitly targeted UPDATE
			opCode = engine.ByDestination
		}
	}
	return vindexTable, opCode, dest, nil
}

func generateOwnedVindexQuery(tblExpr sqlparser.TableExpr, del *sqlparser.Delete, table *vindexes.Table, ksidCols []sqlparser.IdentifierCI) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	for idx, col := range ksidCols {
		if idx == 0 {
			buf.Myprintf("select %v", col)
		} else {
			buf.Myprintf(", %v", col)
		}
	}
	for _, cv := range table.Owned {
		for _, column := range cv.Columns {
			buf.Myprintf(", %v", column)
		}
	}
	buf.Myprintf(" from %v%v%v%v for update", tblExpr, del.Where, del.OrderBy, del.Limit)
	return buf.String()
}

func getUpdateVindexInformation(
	updStmt *sqlparser.Update,
	vindexTable *vindexes.Table,
	tableID semantics.TableSet,
	predicates []sqlparser.Expr,
) ([]*VindexPlusPredicates, map[string]*engine.VindexValues, string, error) {
	if !vindexTable.Keyspace.Sharded {
		return nil, nil, "", nil
	}

	primaryVindex, vindexAndPredicates, err := getVindexInformation(tableID, predicates, vindexTable)
	if err != nil {
		return nil, nil, "", err
	}

	changedVindexValues, ownedVindexQuery, err := buildChangedVindexesValues(updStmt, vindexTable, primaryVindex.Columns)
	if err != nil {
		return nil, nil, "", err
	}
	return vindexAndPredicates, changedVindexValues, ownedVindexQuery, nil
}

/*
		The greedy planner will plan a query by finding first finding the best route plan for every table.
	    Then, iteratively, it finds the cheapest join that can be produced between the remaining plans,
		and removes the two inputs to this cheapest plan and instead adds the join.
		As an optimization, it first only considers joining tables that have predicates defined between them
*/
func greedySolve(ctx *plancontext.PlanningContext, qg *QueryGraph) (Operator, error) {
	routeOps, err := seedOperatorList(ctx, qg)
	planCache := opCacheMap{}
	if err != nil {
		return nil, err
	}

	op, err := mergeRoutes(ctx, qg, routeOps, planCache, false)
	if err != nil {
		return nil, err
	}
	return op, nil
}

func leftToRightSolve(ctx *plancontext.PlanningContext, qg *QueryGraph) (Operator, error) {
	plans, err := seedOperatorList(ctx, qg)
	if err != nil {
		return nil, err
	}

	var acc Operator
	for _, plan := range plans {
		if acc == nil {
			acc = plan
			continue
		}
		joinPredicates := qg.GetPredicates(TableID(acc), TableID(plan))
		acc, err = mergeOrJoin(ctx, acc, plan, joinPredicates, true)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

// seedOperatorList returns a route for each table in the qg
func seedOperatorList(ctx *plancontext.PlanningContext, qg *QueryGraph) ([]Operator, error) {
	plans := make([]Operator, len(qg.Tables))

	// we start by seeding the table with the single routes
	for i, table := range qg.Tables {
		solves := ctx.SemTable.TableSetFor(table.Alias)
		plan, err := createRoute(ctx, table, solves)
		if err != nil {
			return nil, err
		}
		if qg.NoDeps != nil {
			plan.Source = &Filter{
				Source:     plan.Source,
				Predicates: []sqlparser.Expr{qg.NoDeps},
			}
		}
		plans[i] = plan
	}
	return plans, nil
}

func createRoute(ctx *plancontext.PlanningContext, table *QueryTable, solves semantics.TableSet) (*Route, error) {
	if table.IsInfSchema {
		return createInfSchemaRoute(ctx, table)
	}
	vschemaTable, _, _, _, target, err := ctx.VSchema.FindTableOrVindex(table.Table)
	if target != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: SELECT with a target destination")
	}
	if err != nil {
		return nil, err
	}
	if vschemaTable.Name.String() != table.Table.Name.String() {
		// we are dealing with a routed table
		name := table.Table.Name
		table.Table.Name = vschemaTable.Name
		astTable, ok := table.Alias.Expr.(sqlparser.TableName)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] a derived table should never be a routed table")
		}
		realTableName := sqlparser.NewIdentifierCS(vschemaTable.Name.String())
		astTable.Name = realTableName
		if table.Alias.As.IsEmpty() {
			// if the user hasn't specified an alias, we'll insert one here so the old table name still works
			table.Alias.As = sqlparser.NewIdentifierCS(name.String())
		}
	}
	plan := &Route{
		Source: &Table{
			QTable: table,
			VTable: vschemaTable,
		},
		Keyspace: vschemaTable.Keyspace,
	}

	for _, columnVindex := range vschemaTable.ColumnVindexes {
		plan.VindexPreds = append(plan.VindexPreds, &VindexPlusPredicates{ColVindex: columnVindex, TableID: solves})
	}

	switch {
	case vschemaTable.Type == vindexes.TypeSequence:
		plan.RouteOpCode = engine.Next
	case vschemaTable.Type == vindexes.TypeReference:
		plan.RouteOpCode = engine.Reference
	case !vschemaTable.Keyspace.Sharded:
		plan.RouteOpCode = engine.Unsharded
	case vschemaTable.Pinned != nil:
		// Pinned tables have their keyspace ids already assigned.
		// Use the Binary vindex, which is the identity function
		// for keyspace id.
		plan.RouteOpCode = engine.EqualUnique
		vindex, _ := vindexes.NewBinary("binary", nil)
		plan.Selected = &VindexOption{
			Ready:       true,
			Values:      []evalengine.Expr{evalengine.NewLiteralString(vschemaTable.Pinned, collations.TypedCollation{})},
			ValueExprs:  nil,
			Predicates:  nil,
			OpCode:      engine.EqualUnique,
			FoundVindex: vindex,
			Cost: Cost{
				OpCode: engine.EqualUnique,
			},
		}
	default:
		plan.RouteOpCode = engine.Scatter
	}
	for _, predicate := range table.Predicates {
		err = plan.UpdateRoutingLogic(ctx, predicate)
		if err != nil {
			return nil, err
		}
	}

	if plan.RouteOpCode == engine.Scatter && len(table.Predicates) > 0 {
		// If we have a scatter query, it's worth spending a little extra time seeing if we can't improve it
		for _, pred := range table.Predicates {
			rewritten := tryRewriteOrToIn(pred)
			if rewritten != nil {
				err = plan.UpdateRoutingLogic(ctx, rewritten)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return plan, nil
}

func tryRewriteOrToIn(expr sqlparser.Expr) sqlparser.Expr {
	rewrote := false
	newPred := sqlparser.Rewrite(sqlparser.CloneExpr(expr), func(cursor *sqlparser.Cursor) bool {
		_, ok := cursor.Node().(*sqlparser.OrExpr)
		return ok
	}, func(cursor *sqlparser.Cursor) bool {
		// we are looking for the pattern WHERE c = 1 or c = 2
		switch or := cursor.Node().(type) {
		case *sqlparser.OrExpr:
			lftCmp, ok := or.Left.(*sqlparser.ComparisonExpr)
			if !ok {
				return true
			}
			rgtCmp, ok := or.Right.(*sqlparser.ComparisonExpr)
			if !ok {
				return true
			}

			col, ok := lftCmp.Left.(*sqlparser.ColName)
			if !ok || !sqlparser.EqualsExpr(lftCmp.Left, rgtCmp.Left) {
				return true
			}

			var tuple sqlparser.ValTuple
			switch lftCmp.Operator {
			case sqlparser.EqualOp:
				tuple = sqlparser.ValTuple{lftCmp.Right}
			case sqlparser.InOp:
				lft, ok := lftCmp.Right.(sqlparser.ValTuple)
				if !ok {
					return true
				}
				tuple = lft
			default:
				return true
			}

			switch rgtCmp.Operator {
			case sqlparser.EqualOp:
				tuple = append(tuple, rgtCmp.Right)
			case sqlparser.InOp:
				lft, ok := rgtCmp.Right.(sqlparser.ValTuple)
				if !ok {
					return true
				}
				tuple = append(tuple, lft...)
			default:
				return true
			}

			rewrote = true
			cursor.Replace(&sqlparser.ComparisonExpr{
				Operator: sqlparser.InOp,
				Left:     col,
				Right:    tuple,
			})
		}
		return true
	})
	if rewrote {
		return newPred.(sqlparser.Expr)
	}
	return nil
}

func createInfSchemaRoute(ctx *plancontext.PlanningContext, table *QueryTable) (*Route, error) {
	ks, err := ctx.VSchema.AnyKeyspace()
	if err != nil {
		return nil, err
	}
	var src Operator = &Table{
		QTable: table,
		VTable: &vindexes.Table{
			Name:     table.Table.Name,
			Keyspace: ks,
		},
	}
	r := &Route{
		RouteOpCode: engine.DBA,
		Source:      src,
		Keyspace:    ks,
	}
	for _, pred := range table.Predicates {
		isTableSchema, bvName, out, err := extractInfoSchemaRoutingPredicate(pred, ctx.ReservedVars)
		if err != nil {
			return nil, err
		}
		if out == nil {
			// we didn't find a predicate to use for routing, continue to look for next predicate
			continue
		}

		if isTableSchema {
			r.SysTableTableSchema = append(r.SysTableTableSchema, out)
		} else {
			if r.SysTableTableName == nil {
				r.SysTableTableName = map[string]evalengine.Expr{}
			}
			r.SysTableTableName[bvName] = out
		}
	}
	return r, nil
}

func mergeRoutes(ctx *plancontext.PlanningContext, qg *QueryGraph, physicalOps []Operator, planCache opCacheMap, crossJoinsOK bool) (Operator, error) {
	if len(physicalOps) == 0 {
		return nil, nil
	}
	for len(physicalOps) > 1 {
		bestTree, lIdx, rIdx, err := findBestJoin(ctx, qg, physicalOps, planCache, crossJoinsOK)
		if err != nil {
			return nil, err
		}
		// if we found a plan, we'll replace the two plans that were joined with the join plan created
		if bestTree != nil {
			// we remove one plan, and replace the other
			if rIdx > lIdx {
				physicalOps = removeAt(physicalOps, rIdx)
				physicalOps = removeAt(physicalOps, lIdx)
			} else {
				physicalOps = removeAt(physicalOps, lIdx)
				physicalOps = removeAt(physicalOps, rIdx)
			}
			physicalOps = append(physicalOps, bestTree)
		} else {
			if crossJoinsOK {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "should not happen")
			}
			// we will only fail to find a join plan when there are only cross joins left
			// when that happens, we switch over to allow cross joins as well.
			// this way we prioritize joining physicalOps with predicates first
			crossJoinsOK = true
		}
	}
	return physicalOps[0], nil
}

func removeAt(plans []Operator, idx int) []Operator {
	return append(plans[:idx], plans[idx+1:]...)
}

func findBestJoin(
	ctx *plancontext.PlanningContext,
	qg *QueryGraph,
	plans []Operator,
	planCache opCacheMap,
	crossJoinsOK bool,
) (bestPlan Operator, lIdx int, rIdx int, err error) {
	for i, lhs := range plans {
		for j, rhs := range plans {
			if i == j {
				continue
			}
			joinPredicates := qg.GetPredicates(TableID(lhs), TableID(rhs))
			if len(joinPredicates) == 0 && !crossJoinsOK {
				// if there are no predicates joining the two tables,
				// creating a join between them would produce a
				// cartesian product, which is almost always a bad idea
				continue
			}
			plan, err := getJoinFor(ctx, planCache, lhs, rhs, joinPredicates)
			if err != nil {
				return nil, 0, 0, err
			}
			if bestPlan == nil || CostOf(plan) < CostOf(bestPlan) {
				bestPlan = plan
				// remember which plans we based on, so we can remove them later
				lIdx = i
				rIdx = j
			}
		}
	}
	return bestPlan, lIdx, rIdx, nil
}

func getJoinFor(ctx *plancontext.PlanningContext, cm opCacheMap, lhs, rhs Operator, joinPredicates []sqlparser.Expr) (Operator, error) {
	solves := tableSetPair{left: TableID(lhs), right: TableID(rhs)}
	cachedPlan := cm[solves]
	if cachedPlan != nil {
		return cachedPlan, nil
	}

	join, err := mergeOrJoin(ctx, lhs, rhs, joinPredicates, true)
	if err != nil {
		return nil, err
	}
	cm[solves] = join
	return join, nil
}

// requiresSwitchingSides will return true if any of the operators with the root from the given operator tree
// is of the type that should not be on the RHS of a join
func requiresSwitchingSides(ctx *plancontext.PlanningContext, op Operator) bool {
	required := false

	_ = VisitTopDown(op, func(current Operator) error {
		derived, isDerived := current.(*Derived)

		if isDerived && !derived.IsMergeable(ctx) {
			required = true
			return io.EOF
		}

		return nil
	})

	return required
}

func mergeOrJoin(ctx *plancontext.PlanningContext, lhs, rhs Operator, joinPredicates []sqlparser.Expr, inner bool) (Operator, error) {
	merger := func(a, b *Route) (*Route, error) {
		return createRouteOperatorForJoin(a, b, joinPredicates, inner)
	}

	newPlan, _ := tryMerge(ctx, lhs, rhs, joinPredicates, merger)
	if newPlan != nil {
		return newPlan, nil
	}

	if len(joinPredicates) > 0 && requiresSwitchingSides(ctx, rhs) {
		if !inner {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: LEFT JOIN not supported for derived tables")
		}

		if requiresSwitchingSides(ctx, lhs) {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: JOIN not supported between derived tables")
		}

		join := &ApplyJoin{
			LHS:      Clone(rhs),
			RHS:      Clone(lhs),
			Vars:     map[string]int{},
			LeftJoin: !inner,
		}

		return pushJoinPredicates(ctx, joinPredicates, join)
	}

	join := &ApplyJoin{
		LHS:      Clone(lhs),
		RHS:      Clone(rhs),
		Vars:     map[string]int{},
		LeftJoin: !inner,
	}

	return pushJoinPredicates(ctx, joinPredicates, join)
}

func createRouteOperatorForJoin(aRoute, bRoute *Route, joinPredicates []sqlparser.Expr, inner bool) (*Route, error) {
	// append system table names from both the routes.
	sysTableName := aRoute.SysTableTableName
	if sysTableName == nil {
		sysTableName = bRoute.SysTableTableName
	} else {
		for k, v := range bRoute.SysTableTableName {
			sysTableName[k] = v
		}
	}

	r := &Route{
		RouteOpCode:         aRoute.RouteOpCode,
		Keyspace:            aRoute.Keyspace,
		VindexPreds:         append(aRoute.VindexPreds, bRoute.VindexPreds...),
		SysTableTableSchema: append(aRoute.SysTableTableSchema, bRoute.SysTableTableSchema...),
		SeenPredicates:      append(aRoute.SeenPredicates, bRoute.SeenPredicates...),
		SysTableTableName:   sysTableName,
		Source: &ApplyJoin{
			LHS:       aRoute.Source,
			RHS:       bRoute.Source,
			Vars:      map[string]int{},
			LeftJoin:  !inner,
			Predicate: sqlparser.AndExpressions(joinPredicates...),
		},
	}

	if aRoute.SelectedVindex() == bRoute.SelectedVindex() {
		r.Selected = aRoute.Selected
	}

	return r, nil
}

type mergeFunc func(a, b *Route) (*Route, error)

func operatorsToRoutes(a, b Operator) (*Route, *Route) {
	aRoute, ok := a.(*Route)
	if !ok {
		return nil, nil
	}
	bRoute, ok := b.(*Route)
	if !ok {
		return nil, nil
	}
	return aRoute, bRoute
}

func tryMerge(
	ctx *plancontext.PlanningContext,
	a, b Operator,
	joinPredicates []sqlparser.Expr,
	merger mergeFunc,
) (Operator, error) {
	aRoute, bRoute := operatorsToRoutes(Clone(a), Clone(b))
	if aRoute == nil || bRoute == nil {
		return nil, nil
	}

	sameKeyspace := aRoute.Keyspace == bRoute.Keyspace

	if sameKeyspace || (isDualTable(aRoute) || isDualTable(bRoute)) {
		tree, err := tryMergeReferenceTable(aRoute, bRoute, merger)
		if tree != nil || err != nil {
			return tree, err
		}
	}

	switch aRoute.RouteOpCode {
	case engine.Unsharded, engine.DBA:
		if aRoute.RouteOpCode == bRoute.RouteOpCode && sameKeyspace {
			return merger(aRoute, bRoute)
		}
	case engine.EqualUnique:
		// If the two routes fully match, they can be merged together.
		if bRoute.RouteOpCode == engine.EqualUnique {
			aVdx := aRoute.SelectedVindex()
			bVdx := bRoute.SelectedVindex()
			aExpr := aRoute.VindexExpressions()
			bExpr := bRoute.VindexExpressions()
			if aVdx == bVdx && gen4ValuesEqual(ctx, aExpr, bExpr) {
				return merger(aRoute, bRoute)
			}
		}

		// If the two routes don't match, fall through to the next case and see if we
		// can merge via join predicates instead.
		fallthrough

	case engine.Scatter, engine.IN, engine.None:
		if len(joinPredicates) == 0 {
			// If we are doing two Scatters, we have to make sure that the
			// joins are on the correct vindex to allow them to be merged
			// no join predicates - no vindex
			return nil, nil
		}

		if !sameKeyspace {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard correlated subquery")
		}

		canMerge := canMergeOnFilters(ctx, aRoute, bRoute, joinPredicates)
		if !canMerge {
			return nil, nil
		}
		r, err := merger(aRoute, bRoute)
		if err != nil {
			return nil, err
		}

		// If we have a `None` route opcode, we want to keep it -
		// we only try to find a better Vindex for other route opcodes
		if aRoute.RouteOpCode != engine.None {
			r.PickBestAvailableVindex()
		}

		return r, nil
	}
	return nil, nil
}

func isDualTable(route *Route) bool {
	sources := leaves(route)
	if len(sources) > 1 {
		return false
	}
	src, ok := sources[0].(*Table)
	if !ok {
		return false
	}
	return src.VTable.Name.String() == "dual" && src.QTable.Table.Qualifier.IsEmpty()
}

func leaves(op Operator) (sources []Operator) {
	switch op := op.(type) {
	// these are the leaves
	case *Table:
		return []Operator{op}
		// physical
	case *ApplyJoin:
		return []Operator{op.LHS, op.RHS}
	case *Filter:
		return []Operator{op.Source}
	case *Route:
		return []Operator{op.Source}
	}

	panic(fmt.Sprintf("leaves unknown type: %T", op))
}

func tryMergeReferenceTable(aRoute, bRoute *Route, merger mergeFunc) (*Route, error) {
	var (
		// if either side is a reference table, we can just merge it and use the opcode of the other side
		opCode engine.Opcode
		vindex *VindexOption
		ks     *vindexes.Keyspace
	)

	switch {
	case aRoute.RouteOpCode == engine.Reference:
		vindex = bRoute.Selected
		opCode = bRoute.RouteOpCode
		ks = bRoute.Keyspace
	case bRoute.RouteOpCode == engine.Reference:
		vindex = aRoute.Selected
		opCode = aRoute.RouteOpCode
		ks = aRoute.Keyspace
	default:
		return nil, nil
	}

	r, err := merger(aRoute, bRoute)
	if err != nil {
		return nil, err
	}
	r.RouteOpCode = opCode
	r.Selected = vindex
	r.Keyspace = ks
	return r, nil
}

func canMergeOnFilter(ctx *plancontext.PlanningContext, a, b *Route, predicate sqlparser.Expr) bool {
	comparison, ok := predicate.(*sqlparser.ComparisonExpr)
	if !ok {
		return false
	}
	if comparison.Operator != sqlparser.EqualOp {
		return false
	}
	left := comparison.Left
	right := comparison.Right

	lVindex := findColumnVindex(ctx, a, left)
	if lVindex == nil {
		left, right = right, left
		lVindex = findColumnVindex(ctx, a, left)
	}
	if lVindex == nil || !lVindex.IsUnique() {
		return false
	}
	rVindex := findColumnVindex(ctx, b, right)
	if rVindex == nil {
		return false
	}
	return rVindex == lVindex
}

func findColumnVindex(ctx *plancontext.PlanningContext, a Operator, exp sqlparser.Expr) vindexes.SingleColumn {
	_, isCol := exp.(*sqlparser.ColName)
	if !isCol {
		return nil
	}

	exp = unwrapDerivedTables(ctx, exp)
	if exp == nil {
		return nil
	}

	var singCol vindexes.SingleColumn

	// for each equality expression that exp has with other column name, we check if it
	// can be solved by any table in our routeTree. If an equality expression can be solved,
	// we check if the equality expression and our table share the same vindex, if they do:
	// the method will return the associated vindexes.SingleColumn.
	for _, expr := range ctx.SemTable.GetExprAndEqualities(exp) {
		col, isCol := expr.(*sqlparser.ColName)
		if !isCol {
			continue
		}

		deps := ctx.SemTable.RecursiveDeps(expr)

		_ = VisitTopDown(a, func(rel Operator) error {
			to, isTableOp := rel.(tableIDIntroducer)
			if !isTableOp {
				return nil
			}
			id := to.Introduces()
			if deps.IsSolvedBy(id) {
				tableInfo, err := ctx.SemTable.TableInfoFor(id)
				if err != nil {
					// an error here is OK, we just can't ask this operator about its column vindexes
					return nil
				}
				vtable := tableInfo.GetVindexTable()
				if vtable != nil {
					for _, vindex := range vtable.ColumnVindexes {
						sC, isSingle := vindex.Vindex.(vindexes.SingleColumn)
						if isSingle && vindex.Columns[0].Equal(col.Name) {
							singCol = sC
							return io.EOF
						}
					}
				}
			}
			return nil
		})
		if singCol != nil {
			return singCol
		}
	}

	return singCol
}

// unwrapDerivedTables we want to find the bottom layer of derived tables
// nolint
func unwrapDerivedTables(ctx *plancontext.PlanningContext, exp sqlparser.Expr) sqlparser.Expr {
	for {
		// if we are dealing with derived tables in derived tables
		tbl, err := ctx.SemTable.TableInfoForExpr(exp)
		if err != nil {
			return nil
		}
		_, ok := tbl.(*semantics.DerivedTable)
		if !ok {
			break
		}

		exp, err = semantics.RewriteDerivedTableExpression(exp, tbl)
		if err != nil {
			return nil
		}
		exp = getColName(exp)
		if exp == nil {
			return nil
		}
	}
	return exp
}

func getColName(exp sqlparser.Expr) *sqlparser.ColName {
	switch exp := exp.(type) {
	case *sqlparser.ColName:
		return exp
	case *sqlparser.Max, *sqlparser.Min:
		aggr := exp.(sqlparser.AggrFunc).GetArg()
		colName, ok := aggr.(*sqlparser.ColName)
		if ok {
			return colName
		}
	}
	// for any other expression than a column, or the extremum of a column, we return nil
	return nil
}

func canMergeOnFilters(ctx *plancontext.PlanningContext, a, b *Route, joinPredicates []sqlparser.Expr) bool {
	for _, predicate := range joinPredicates {
		for _, expr := range sqlparser.SplitAndExpression(nil, predicate) {
			if canMergeOnFilter(ctx, a, b, expr) {
				return true
			}
		}
	}
	return false
}

func gen4ValuesEqual(ctx *plancontext.PlanningContext, a, b []sqlparser.Expr) bool {
	if len(a) != len(b) {
		return false
	}

	// TODO: check SemTable's columnEqualities for better plan

	for i, aExpr := range a {
		bExpr := b[i]
		if !gen4ValEqual(ctx, aExpr, bExpr) {
			return false
		}
	}
	return true
}

func gen4ValEqual(ctx *plancontext.PlanningContext, a, b sqlparser.Expr) bool {
	switch a := a.(type) {
	case *sqlparser.ColName:
		if b, ok := b.(*sqlparser.ColName); ok {
			if !a.Name.Equal(b.Name) {
				return false
			}

			return ctx.SemTable.DirectDeps(a) == ctx.SemTable.DirectDeps(b)
		}
	case sqlparser.Argument:
		b, ok := b.(sqlparser.Argument)
		if !ok {
			return false
		}
		return a == b
	case *sqlparser.Literal:
		b, ok := b.(*sqlparser.Literal)
		if !ok {
			return false
		}
		switch a.Type {
		case sqlparser.StrVal:
			switch b.Type {
			case sqlparser.StrVal:
				return a.Val == b.Val
			case sqlparser.HexVal:
				return hexEqual(b, a)
			}
		case sqlparser.HexVal:
			return hexEqual(a, b)
		case sqlparser.IntVal:
			if b.Type == (sqlparser.IntVal) {
				return a.Val == b.Val
			}
		}
	}
	return false
}

func hexEqual(a, b *sqlparser.Literal) bool {
	v, err := a.HexDecode()
	if err != nil {
		return false
	}
	switch b.Type {
	case sqlparser.StrVal:
		return bytes.Equal(v, b.Bytes())
	case sqlparser.HexVal:
		v2, err := b.HexDecode()
		if err != nil {
			return false
		}
		return bytes.Equal(v, v2)
	}
	return false
}

func pushJoinPredicates(
	ctx *plancontext.PlanningContext,
	exprs []sqlparser.Expr,
	op Operator,
) (Operator, error) {
	if len(exprs) == 0 {
		return op, nil
	}

	switch op := op.(type) {
	case *ApplyJoin:
		return pushJoinPredicateOnJoin(ctx, exprs, op)
	case *Route:
		return pushJoinPredicateOnRoute(ctx, exprs, op)
	case *Table:
		return PushPredicate(ctx, sqlparser.AndExpressions(exprs...), op)
	case *Derived:
		return pushJoinPredicateOnDerived(ctx, exprs, op)
	case *Filter:
		op.Predicates = append(op.Predicates, exprs...)
		return op, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown type %T pushJoinPredicates", op)
	}
}

func pushJoinPredicateOnRoute(ctx *plancontext.PlanningContext, exprs []sqlparser.Expr, op *Route) (Operator, error) {
	for _, expr := range exprs {
		err := op.UpdateRoutingLogic(ctx, expr)
		if err != nil {
			return nil, err
		}
	}
	newSrc, err := pushJoinPredicates(ctx, exprs, op.Source)
	op.Source = newSrc
	return op, err
}

func pushJoinPredicateOnJoin(ctx *plancontext.PlanningContext, exprs []sqlparser.Expr, node *ApplyJoin) (Operator, error) {
	node = Clone(node).(*ApplyJoin)
	var rhsPreds []sqlparser.Expr
	var lhsPreds []sqlparser.Expr
	var lhsVarsName []string
	for _, expr := range exprs {
		// We find the dependencies for the given expression and if they are solved entirely by one
		// side of the join tree, then we push the predicate there and do not break it into parts.
		// In case a predicate has no dependencies, then it is pushed to both sides so that we can filter
		// rows as early as possible making join cheaper on the vtgate level.
		depsForExpr := ctx.SemTable.RecursiveDeps(expr)
		singleSideDeps := false
		lhsTables := TableID(node.LHS)
		if depsForExpr.IsSolvedBy(lhsTables) {
			lhsPreds = append(lhsPreds, expr)
			singleSideDeps = true
		}
		if depsForExpr.IsSolvedBy(TableID(node.RHS)) {
			rhsPreds = append(rhsPreds, expr)
			singleSideDeps = true
		}

		if singleSideDeps {
			continue
		}

		bvName, cols, predicate, err := BreakExpressionInLHSandRHS(ctx, expr, lhsTables)
		if err != nil {
			return nil, err
		}
		node.LHSColumns = append(node.LHSColumns, cols...)
		lhsVarsName = append(lhsVarsName, bvName...)
		rhsPreds = append(rhsPreds, predicate)
	}
	if node.LHSColumns != nil && lhsVarsName != nil {
		newNode, offsets, err := PushOutputColumns(ctx, node.LHS, node.LHSColumns...)
		if err != nil {
			return nil, err
		}
		node.LHS = newNode
		for i, idx := range offsets {
			node.Vars[lhsVarsName[i]] = idx
		}
	}
	lhsPlan, err := pushJoinPredicates(ctx, lhsPreds, node.LHS)
	if err != nil {
		return nil, err
	}

	rhsPlan, err := pushJoinPredicates(ctx, rhsPreds, node.RHS)
	if err != nil {
		return nil, err
	}

	node.LHS = lhsPlan
	node.RHS = rhsPlan
	// If the predicate field is previously non-empty
	// keep that predicate too
	if node.Predicate != nil {
		exprs = append(exprs, node.Predicate)
	}
	node.Predicate = sqlparser.AndExpressions(exprs...)
	return node, nil
}

func pushJoinPredicateOnDerived(ctx *plancontext.PlanningContext, exprs []sqlparser.Expr, node *Derived) (Operator, error) {
	node = Clone(node).(*Derived)

	newExpressions := make([]sqlparser.Expr, 0, len(exprs))
	for _, expr := range exprs {
		tblInfo, err := ctx.SemTable.TableInfoForExpr(expr)
		if err != nil {
			return nil, err
		}
		rewritten, err := semantics.RewriteDerivedTableExpression(expr, tblInfo)
		if err != nil {
			return nil, err
		}
		newExpressions = append(newExpressions, rewritten)
	}

	newInner, err := pushJoinPredicates(ctx, newExpressions, node.Source)
	if err != nil {
		return nil, err
	}

	node.Source = newInner
	return node, nil
}
