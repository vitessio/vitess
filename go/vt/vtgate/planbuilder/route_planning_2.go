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

package planbuilder

import (
	"io"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	opCacheMap map[tableSetPair]abstract.PhysicalOperator
)

func createPhysicalOperator(ctx *planningContext, opTree abstract.LogicalOperator) (abstract.PhysicalOperator, error) {
	switch op := opTree.(type) {
	case *abstract.QueryGraph:
		switch {
		// case ctx.vschema.Planner() == Gen4Left2Right:
		//	return leftToRightSolve(ctx, op)
		default:
			return greedySolve2(ctx, op)
		}
	// case *abstract.Join:
	//	treeInner, err := optimizeQuery(ctx, op.LHS)
	//	if err != nil {
	//		return nil, err
	//	}
	//	treeOuter, err := optimizeQuery(ctx, op.RHS)
	//	if err != nil {
	//		return nil, err
	//	}
	//	return mergeOrJoin(ctx, treeInner, treeOuter, sqlparser.SplitAndExpression(nil, op.Predicate), !op.LeftJoin)
	// case *abstract.Derived:
	//	treeInner, err := optimizeQuery(ctx, op.Inner)
	//	if err != nil {
	//		return nil, err
	//	}
	//	return &derivedTree{
	//		query:         op.Sel,
	//		inner:         treeInner,
	//		alias:         op.Alias,
	//		columnAliases: op.ColumnAliases,
	//	}, nil
	// case *abstract.SubQuery:
	//	return optimizeSubQuery(ctx, op)
	// case *abstract.Vindex:
	//	return createVindexTree(ctx, op)
	// case *abstract.Concatenate:
	//	return optimizeUnion(ctx, op)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid operator tree: %T", op)
	}
}

/*
	The greedy planner will plan a query by finding first finding the best route plan for every table.
    Then, iteratively, it finds the cheapest join that can be produced between the remaining plans,
	and removes the two inputs to this cheapest plan and instead adds the join.
	As an optimization, it first only considers joining tables that have predicates defined between them
*/
func greedySolve2(ctx *planningContext, qg *abstract.QueryGraph) (abstract.PhysicalOperator, error) {
	routeOps, err := seedOperatorList(ctx, qg)
	planCache := opCacheMap{}
	if err != nil {
		return nil, err
	}

	op, err := mergeRouteOps(ctx, qg, routeOps, planCache, false)
	if err != nil {
		return nil, err
	}
	return op, nil
}

// seedOperatorList returns a route for each table in the qg
func seedOperatorList(ctx *planningContext, qg *abstract.QueryGraph) ([]abstract.PhysicalOperator, error) {
	plans := make([]abstract.PhysicalOperator, len(qg.Tables))

	// we start by seeding the table with the single routes
	for i, table := range qg.Tables {
		solves := ctx.semTable.TableSetFor(table.Alias)
		plan, err := createRouteOperator(ctx, table, solves)
		if err != nil {
			return nil, err
		}
		// if qg.NoDeps != nil {
		//	plan.predicates = append(plan.predicates, sqlparser.SplitAndExpression(nil, qg.NoDeps)...)
		// }
		plans[i] = plan
	}
	return plans, nil
}

func createRouteOperator(ctx *planningContext, table *abstract.QueryTable, solves semantics.TableSet) (*routeOp, error) {
	// if table.IsInfSchema {
	//	ks, err := ctx.vschema.AnyKeyspace()
	//	if err != nil {
	//		return nil, err
	//	}
	//	rp := &routeTree{
	//		routeOpCode: engine.SelectDBA,
	//		solved:      solves,
	//		keyspace:    ks,
	//		tables: []relation{&routeTable{
	//			qtable: table,
	//			vtable: &vindexes.Table{
	//				Name:     table.Table.Name,
	//				Keyspace: ks,
	//			},
	//		}},
	//		predicates: table.Predicates,
	//	}
	//	err = rp.findSysInfoRoutingPredicatesGen4(ctx.reservedVars)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	return rp, nil
	// }
	vschemaTable, _, _, _, _, err := ctx.vschema.FindTableOrVindex(table.Table)
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
		realTableName := sqlparser.NewTableIdent(vschemaTable.Name.String())
		astTable.Name = realTableName
		if table.Alias.As.IsEmpty() {
			// if the user hasn't specified an alias, we'll insert one here so the old table name still works
			table.Alias.As = sqlparser.NewTableIdent(name.String())
		}
	}
	plan := &routeOp{
		source: &tableOp{
			qtable: table,
			vtable: vschemaTable,
		},
		keyspace: vschemaTable.Keyspace,
	}

	for _, columnVindex := range vschemaTable.ColumnVindexes {
		plan.vindexPreds = append(plan.vindexPreds, &vindexPlusPredicates{colVindex: columnVindex, tableID: solves})
	}

	switch {
	case vschemaTable.Type == vindexes.TypeSequence:
		plan.routeOpCode = engine.SelectNext
	case vschemaTable.Type == vindexes.TypeReference:
		plan.routeOpCode = engine.SelectReference
	case !vschemaTable.Keyspace.Sharded:
		plan.routeOpCode = engine.SelectUnsharded
	case vschemaTable.Pinned != nil:
		// Pinned tables have their keyspace ids already assigned.
		// Use the Binary vindex, which is the identity function
		// for keyspace id.
		plan.routeOpCode = engine.SelectEqualUnique
		vindex, _ := vindexes.NewBinary("binary", nil)
		plan.selected = &vindexOption{
			ready:       true,
			values:      []evalengine.Expr{evalengine.NewLiteralString(vschemaTable.Pinned, collations.TypedCollation{})},
			valueExprs:  nil,
			predicates:  nil,
			opcode:      engine.SelectEqualUnique,
			foundVindex: vindex,
			cost: cost{
				opCode: engine.SelectEqualUnique,
			},
		}
	default:
		plan.routeOpCode = engine.SelectScatter
	}
	for _, predicate := range table.Predicates {
		err = plan.updateRoutingLogic(ctx, predicate)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}

func mergeRouteOps(ctx *planningContext, qg *abstract.QueryGraph, physicalOps []abstract.PhysicalOperator, planCache opCacheMap, crossJoinsOK bool) (abstract.PhysicalOperator, error) {
	if len(physicalOps) == 0 {
		return nil, nil
	}
	for len(physicalOps) > 1 {
		bestTree, lIdx, rIdx, err := findBestJoinOp(ctx, qg, physicalOps, planCache, crossJoinsOK)
		if err != nil {
			return nil, err
		}
		// if we found a best plan, we'll replace the two plans that were joined with the join plan created
		if bestTree != nil {
			// we need to remove the larger of the two plans first
			if rIdx > lIdx {
				physicalOps = removeOpAt(physicalOps, rIdx)
				physicalOps = removeOpAt(physicalOps, lIdx)
			} else {
				physicalOps = removeOpAt(physicalOps, lIdx)
				physicalOps = removeOpAt(physicalOps, rIdx)
			}
			physicalOps = append(physicalOps, bestTree)
		} else {
			// we will only fail to find a join plan when there are only cross joins left
			// when that happens, we switch over to allow cross joins as well.
			// this way we prioritize joining physicalOps with predicates first
			crossJoinsOK = true
		}
	}
	return physicalOps[0], nil
}

func removeOpAt(plans []abstract.PhysicalOperator, idx int) []abstract.PhysicalOperator {
	return append(plans[:idx], plans[idx+1:]...)
}

func findBestJoinOp(
	ctx *planningContext,
	qg *abstract.QueryGraph,
	plans []abstract.PhysicalOperator,
	planCache opCacheMap,
	crossJoinsOK bool,
) (bestPlan abstract.PhysicalOperator, lIdx int, rIdx int, err error) {
	for i, lhs := range plans {
		for j, rhs := range plans {
			if i == j {
				continue
			}
			joinPredicates := qg.GetPredicates(lhs.TableID(), rhs.TableID())
			if len(joinPredicates) == 0 && !crossJoinsOK {
				// if there are no predicates joining the two tables,
				// creating a join between them would produce a
				// cartesian product, which is almost always a bad idea
				continue
			}
			plan, err := getJoinOpFor(ctx, planCache, lhs, rhs, joinPredicates)
			if err != nil {
				return nil, 0, 0, err
			}
			if bestPlan == nil || plan.Cost() < bestPlan.Cost() {
				bestPlan = plan
				// remember which plans we based on, so we can remove them later
				lIdx = i
				rIdx = j
			}
		}
	}
	return bestPlan, lIdx, rIdx, nil
}

func getJoinOpFor(ctx *planningContext, cm opCacheMap, lhs, rhs abstract.PhysicalOperator, joinPredicates []sqlparser.Expr) (abstract.PhysicalOperator, error) {
	solves := tableSetPair{left: lhs.TableID(), right: rhs.TableID()}
	cachedPlan := cm[solves]
	if cachedPlan != nil {
		return cachedPlan, nil
	}

	join, err := mergeOrJoinOp(ctx, lhs, rhs, joinPredicates, true)
	if err != nil {
		return nil, err
	}
	cm[solves] = join
	return join, nil
}

func mergeOrJoinOp(ctx *planningContext, lhs, rhs abstract.PhysicalOperator, joinPredicates []sqlparser.Expr, inner bool) (abstract.PhysicalOperator, error) {

	merger := func(a, b *routeOp) (*routeOp, error) {
		// if inner {
		return createRouteOperatorForInnerJoin(ctx, a, b, joinPredicates)
		// }
		// return createRoutePlanForOuter(ctx, a, b, newTabletSet, joinPredicates), nil
	}

	newPlan, err := tryMergeOp(ctx, lhs, rhs, joinPredicates, merger)
	if err != nil {
		return nil, err
	}
	if newPlan != nil {
		return newPlan, nil
	}

	var tree abstract.PhysicalOperator = &applyJoin{LHS: lhs.Clone(), RHS: rhs.Clone(), vars: map[string]int{}}
	for _, predicate := range joinPredicates {
		tree, err = PushPredicate(ctx, predicate, tree)
		if err != nil {
			return nil, err
		}
	}
	return tree, nil
}

func createRouteOperatorForInnerJoin(ctx *planningContext, aRoute, bRoute *routeOp, joinPredicates []sqlparser.Expr) (*routeOp, error) {
	// append system table names from both the routes.
	sysTableName := aRoute.SysTableTableName
	if sysTableName == nil {
		sysTableName = bRoute.SysTableTableName
	} else {
		for k, v := range bRoute.SysTableTableName {
			sysTableName[k] = v
		}
	}

	r := &routeOp{
		routeOpCode:         aRoute.routeOpCode,
		keyspace:            aRoute.keyspace,
		vindexPreds:         append(aRoute.vindexPreds, bRoute.vindexPreds...),
		SysTableTableSchema: append(aRoute.SysTableTableSchema, bRoute.SysTableTableSchema...),
		SysTableTableName:   sysTableName,
		source: &applyJoin{
			LHS:  aRoute.source,
			RHS:  bRoute.source,
			vars: map[string]int{},
		},
	}

	for _, predicate := range joinPredicates {
		op, err := PushPredicate(ctx, predicate, r)
		if err != nil {
			return nil, err
		}
		route, ok := op.(*routeOp)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] did not expect type to change when pushing predicates")
		}
		r = route
	}

	if aRoute.selectedVindex() == bRoute.selectedVindex() {
		r.selected = aRoute.selected
	}

	return r, nil
}

type mergeOpFunc func(a, b *routeOp) (*routeOp, error)

func makeRouteOp(j abstract.PhysicalOperator) *routeOp {
	rb, ok := j.(*routeOp)
	if ok {
		return rb
	}

	return nil

	// x, ok := j.(*derivedTree)
	// if !ok {
	//	return nil
	// }
	// dp := x.Clone().(*derivedTree)
	//
	// inner := makeRouteOp(dp.inner)
	// if inner == nil {
	//	return nil
	// }

	// dt := &derivedTable{
	//	tables:     inner.tables,
	//	query:      dp.query,
	//	predicates: inner.predicates,
	//	leftJoins:  inner.leftJoins,
	//	alias:      dp.alias,
	// }

	// inner.tables = parenTables{dt}
	// inner.predicates = nil
	// inner.leftJoins = nil
	// return inner
}

func operatorsToRoutes(a, b abstract.PhysicalOperator) (*routeOp, *routeOp) {
	aRoute := makeRouteOp(a)
	if aRoute == nil {
		return nil, nil
	}
	bRoute := makeRouteOp(b)
	if bRoute == nil {
		return nil, nil
	}
	return aRoute, bRoute
}

func tryMergeOp(ctx *planningContext, a, b abstract.PhysicalOperator, joinPredicates []sqlparser.Expr, merger mergeOpFunc) (abstract.PhysicalOperator, error) {
	aRoute, bRoute := operatorsToRoutes(a.Clone(), b.Clone())
	if aRoute == nil || bRoute == nil {
		return nil, nil
	}

	sameKeyspace := aRoute.keyspace == bRoute.keyspace

	// if sameKeyspace || (isDualTable(aRoute) || isDualTable(bRoute)) {
	//	tree, err := tryMergeReferenceTable(aRoute, bRoute, merger)
	//	if tree != nil || err != nil {
	//		return tree, err
	//	}
	// }

	switch aRoute.routeOpCode {
	case engine.SelectUnsharded, engine.SelectDBA:
		if aRoute.routeOpCode == bRoute.routeOpCode {
			return merger(aRoute, bRoute)
		}
	case engine.SelectEqualUnique:
		// if they are already both being sent to the same shard, we can merge
		if bRoute.routeOpCode == engine.SelectEqualUnique {
			if aRoute.selectedVindex() == bRoute.selectedVindex() &&
				gen4ValuesEqual(ctx, aRoute.vindexExpressions(), bRoute.vindexExpressions()) {
				return merger(aRoute, bRoute)
			}
			return nil, nil
		}
		fallthrough
	case engine.SelectScatter, engine.SelectIN:
		if len(joinPredicates) == 0 {
			// If we are doing two Scatters, we have to make sure that the
			// joins are on the correct vindex to allow them to be merged
			// no join predicates - no vindex
			return nil, nil
		}
		if !sameKeyspace {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard correlated subquery")
		}

		canMerge := canMergeOpsOnFilters(ctx, aRoute, bRoute, joinPredicates)
		if !canMerge {
			return nil, nil
		}
		r, err := merger(aRoute, bRoute)
		if err != nil {
			return nil, err
		}
		r.pickBestAvailableVindex()
		return r, nil
	}
	return nil, nil
}

func (r *routeOp) selectedVindex() vindexes.Vindex {
	if r.selected == nil {
		return nil
	}
	return r.selected.foundVindex
}
func (r *routeOp) vindexExpressions() []sqlparser.Expr {
	if r.selected == nil {
		return nil
	}
	return r.selected.valueExprs
}

func canMergeOpsOnFilter(ctx *planningContext, a, b *routeOp, predicate sqlparser.Expr) bool {
	comparison, ok := predicate.(*sqlparser.ComparisonExpr)
	if !ok {
		return false
	}
	if comparison.Operator != sqlparser.EqualOp {
		return false
	}
	left := comparison.Left
	right := comparison.Right

	lVindex := findColumnVindexOnOps(ctx, a, left)
	if lVindex == nil {
		left, right = right, left
		lVindex = findColumnVindexOnOps(ctx, a, left)
	}
	if lVindex == nil || !lVindex.IsUnique() {
		return false
	}
	rVindex := findColumnVindexOnOps(ctx, b, right)
	if rVindex == nil {
		return false
	}
	return rVindex == lVindex
}

func findColumnVindexOnOps(ctx *planningContext, a *routeOp, exp sqlparser.Expr) vindexes.SingleColumn {
	_, isCol := exp.(*sqlparser.ColName)
	if !isCol {
		return nil
	}

	var singCol vindexes.SingleColumn

	// for each equality expression that exp has with other column name, we check if it
	// can be solved by any table in our routeTree a. If an equality expression can be solved,
	// we check if the equality expression and our table share the same vindex, if they do:
	// the method will return the associated vindexes.SingleColumn.
	for _, expr := range ctx.semTable.GetExprAndEqualities(exp) {
		col, isCol := expr.(*sqlparser.ColName)
		if !isCol {
			continue
		}
		leftDep := ctx.semTable.RecursiveDeps(expr)

		_ = visitOperators(a, func(rel abstract.Operator) (bool, error) {
			to, isTableOp := rel.(*tableOp)
			if !isTableOp {
				return true, nil
			}
			if leftDep.IsSolvedBy(to.qtable.ID) {
				for _, vindex := range to.vtable.ColumnVindexes {
					sC, isSingle := vindex.Vindex.(vindexes.SingleColumn)
					if isSingle && vindex.Columns[0].Equal(col.Name) {
						singCol = sC
						return false, io.EOF
					}
				}
			}
			return false, nil
		})
		if singCol != nil {
			return singCol
		}
	}

	return singCol
}

func canMergeOpsOnFilters(ctx *planningContext, a, b *routeOp, joinPredicates []sqlparser.Expr) bool {
	for _, predicate := range joinPredicates {
		for _, expr := range sqlparser.SplitAndExpression(nil, predicate) {
			if canMergeOpsOnFilter(ctx, a, b, expr) {
				return true
			}
		}
	}
	return false
}

// visitOperators visits all the operators.
func visitOperators(op abstract.Operator, f func(tbl abstract.Operator) (bool, error)) error {
	kontinue, err := f(op)
	if err != nil {
		return err
	}
	if !kontinue {
		return nil
	}

	switch op := op.(type) {
	case *tableOp, *abstract.QueryGraph, *abstract.Vindex:
		// leaf - no children to visit
	case *routeOp:
		err := visitOperators(op.source, f)
		if err != nil {
			return err
		}
	case *applyJoin:
		err := visitOperators(op.LHS, f)
		if err != nil {
			return err
		}
		err = visitOperators(op.RHS, f)
		if err != nil {
			return err
		}
	case *filterOp:
		err := visitOperators(op.source, f)
		if err != nil {
			return err
		}
	case *abstract.Concatenate:
		for _, source := range op.Sources {
			err := visitOperators(source, f)
			if err != nil {
				return err
			}
		}
	case *abstract.Derived:
		err := visitOperators(op.Inner, f)
		if err != nil {
			return err
		}
	case *abstract.Join:
		err := visitOperators(op.LHS, f)
		if err != nil {
			return err
		}
		err = visitOperators(op.RHS, f)
		if err != nil {
			return err
		}
	case *abstract.SubQuery:
		err := visitOperators(op.Outer, f)
		if err != nil {
			return err
		}
		for _, source := range op.Inner {
			err := visitOperators(source.Inner, f)
			if err != nil {
				return err
			}
		}
	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown operator type while visiting - %T", op)
	}
	return nil
}
