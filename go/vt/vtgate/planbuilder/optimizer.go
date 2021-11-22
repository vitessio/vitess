package planbuilder

import (
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func optimizeTree(ctx *planningContext, node abstract.Operator) (abstract.Operator, error) {
	switch node := node.(type) {
	case *abstract.Projection:
		newSrc, err := optimizeTree(ctx, node.Source)
		if err != nil {
			return nil, err
		}
		node.Source = newSrc
	case *abstract.QueryGraph:
		op, err := greedySolveOp(ctx, node)
		if err != nil {
			return nil, err
		}
		return op, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "do me next! %T", node)
	}
	return node, nil
}

func greedySolveOp(ctx *planningContext, qg *abstract.QueryGraph) (abstract.Operator, error) {
	tables, err := seedOpList(ctx, qg)
	if err != nil {
		return nil, err
	}
	opCache := opCacheMap{}
	op, err := mergeOperators(ctx, qg, tables, opCache)
	if err != nil {
		return nil, err
	}
	return op, nil
}

func removeIdx(plans []abstract.Operator, idx int) []abstract.Operator {
	return append(plans[:idx], plans[idx+1:]...)
}

func mergeOperators(ctx *planningContext, qg *abstract.QueryGraph, tables []abstract.Operator, cache opCacheMap) (abstract.Operator, error) {
	crossJoinsOK := false
	if len(tables) == 0 {
		return nil, nil
	}
	for len(tables) > 1 {
		bestTree, lIdx, rIdx, err := findBestJoinOp(ctx, qg, tables, cache, crossJoinsOK)
		if err != nil {
			return nil, err
		}
		// if we found a best plan, we'll replace the two plans that were joined with the join plan created
		if bestTree != nil {
			// we need to remove the larger of the two plans first
			if rIdx > lIdx {
				tables = removeIdx(tables, rIdx)
				tables = removeIdx(tables, lIdx)
			} else {
				tables = removeIdx(tables, lIdx)
				tables = removeIdx(tables, rIdx)
			}
			tables = append(tables, bestTree)
		} else {
			// we will only fail to find a join plan when there are only cross joins left
			// when that happens, we switch over to allow cross joins as well.
			// this way we prioritize joining operators with predicates first
			crossJoinsOK = true
		}

	}
	return tables[0], nil
}

func findBestJoinOp(
	ctx *planningContext,
	qg *abstract.QueryGraph,
	plans []abstract.Operator,
	planCache opCacheMap,
	crossJoinsOK bool,
) (bestPlan abstract.Operator, lIdx int, rIdx int, err error) {
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
			plan, err := planCache.getJoinTreeFor(ctx, lhs, rhs, joinPredicates)
			if err != nil {
				return nil, 0, 0, err
			}
			if bestPlan == nil || plan.cost() < bestPlan.cost() {
				bestPlan = plan
				// remember which plans we based on, so we can remove them later
				lIdx = i
				rIdx = j
			}
		}
	}
	return bestPlan, lIdx, rIdx, nil
}

func seedOpList(ctx *planningContext, qg *abstract.QueryGraph) ([]abstract.Operator, error) {
	ops := make([]abstract.Operator, len(qg.Tables))
	for i, table := range qg.Tables {
		id := ctx.semTable.TableSetFor(table.Alias)
		op, err := createRouteOp(ctx, table, id)
		if err != nil {
			return nil, err
		}
		ops[i] = op
	}
	return ops, nil
}

func createRouteOp(ctx *planningContext, table *abstract.QueryTable, solves semantics.TableSet) (*Route, error) {
	if table.IsInfSchema {
		ks, err := ctx.vschema.AnyKeyspace()
		if err != nil {
			return nil, err
		}

		op := &Route{
			Source:      table,
			RouteOpCode: engine.SelectDBA,
			Keyspace:    ks,
		}

		err = FindSysInfoRoutingPredicatesGen4(op, ctx.reservedVars, table.Predicates)
		if err != nil {
			return nil, err
		}

		return op, nil
	}
	vschemaTable, _, _, _, _, err := ctx.vschema.FindTableOrVindex(table.Table)
	if err != nil {
		return nil, err
	}
	if vschemaTable.Name.String() != table.Table.Name.String() {
		// we "are dealing with a routed table
		panic("implement me")
	}
	plan := &Route{
		Source:   table,
		Keyspace: vschemaTable.Keyspace,
	}

	for _, columnVindex := range vschemaTable.ColumnVindexes {
		plan.vindexPreds = append(plan.vindexPreds, &vindexPlusPredicates{colVindex: columnVindex, tableID: solves})
	}

	switch {
	case vschemaTable.Type == vindexes.TypeSequence:
		plan.RouteOpCode = engine.SelectNext
	case vschemaTable.Type == vindexes.TypeReference:
		plan.RouteOpCode = engine.SelectReference
	case !vschemaTable.Keyspace.Sharded:
		plan.RouteOpCode = engine.SelectUnsharded
	case vschemaTable.Pinned != nil:
		// Pinned tables have their keyspace ids already assigned.
		// Use the Binary vindex, which is the identity function
		// for keyspace id.
		plan.RouteOpCode = engine.SelectEqualUnique
		vindex, _ := vindexes.NewBinary("binary", nil)
		plan.selected = &vindexOption{
			ready:       true,
			values:      []sqltypes.PlanValue{{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, vschemaTable.Pinned)}},
			valueExprs:  nil,
			predicates:  nil,
			opcode:      engine.SelectEqualUnique,
			foundVindex: vindex,
			cost: cost{
				opCode: engine.SelectEqualUnique,
			},
		}
	default:
		plan.RouteOpCode = engine.SelectScatter
	}

	err = plan.Inspect(ctx, table.Predicates)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func FindSysInfoRoutingPredicatesGen4(rp *Route, vars *sqlparser.ReservedVars, predicates []sqlparser.Expr) error {
	for _, pred := range predicates {
		isTableSchema, bvName, out, err := extractInfoSchemaRoutingPredicate(pred, vars)
		if err != nil {
			return err
		}
		if out == nil {
			// we didn't find a predicate to use for routing, continue to look for next predicate
			continue
		}

		if isTableSchema {
			rp.SysTableTableSchema = append(rp.SysTableTableSchema, out)
		} else {
			if rp.SysTableTableName == nil {
				rp.SysTableTableName = map[string]evalengine.Expr{}
			}
			rp.SysTableTableName[bvName] = out
		}
	}
	return nil
}

type opCacheMap map[tableSetPair]abstract.Operator

func (cm opCacheMap) getJoinTreeFor(ctx *planningContext, lhs, rhs abstract.Operator, joinPredicates []sqlparser.Expr) (abstract.Operator, error) {
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

func mergeOrJoinOp(ctx *planningContext, lhs, rhs abstract.Operator, joinPredicates []sqlparser.Expr, inner bool) (abstract.Operator, error) {
	// newTabletSet := lhs.TableID().Merge(rhs.TableID())

	merger := func(a, b *Route) (*Route, error) {
		if inner {
			return createRoutePlanForInnerOp(a, b, joinPredicates), nil
		}
		panic("implement me!")
		// return createRoutePlanForOuterOp(ctx, a, b, newTabletSet, joinPredicates), nil
	}

	newPlan, err := tryMergeOp(ctx, lhs, rhs, joinPredicates, merger)
	if err != nil {
		return nil, err
	}
	if newPlan != nil {
		return newPlan, nil
	}

	// tree := &joinTree{lhs: lhs.clone(), rhs: rhs.clone(), leftJoin: !inner, vars: map[string]int{}}
	tree := &abstract.Join{
		LHS:       lhs,
		RHS:       rhs,
		Predicate: nil,
		LeftJoin:  false,
	}
	return pushJoinPredicateOp(ctx, joinPredicates, tree)
}

func tryMergeOp(ctx *planningContext, lhs, rhs abstract.Operator, predicates []sqlparser.Expr, merger func(a *Route, b *Route) (*Route, error)) (abstract.Operator, error) {
	lRoute, ok := lhs.(*Route)
	if !ok {
		return nil, nil
	}
	rRoute, ok := rhs.(*Route)
	if !ok {
		return nil, nil
	}

	sameKeyspace := lRoute.Keyspace == rRoute.Keyspace

	if sameKeyspace || (isDualTableOp(lRoute) || isDualTableOp(rRoute)) {
		tree, err := tryMergeReferenceTableOp(lRoute, rRoute, merger)
		if tree != nil || err != nil {
			return tree, err
		}
	}

	switch lRoute.RouteOpCode {
	case engine.SelectUnsharded, engine.SelectDBA:
		if lRoute.RouteOpCode == rRoute.RouteOpCode {
			return merger(lRoute, rRoute)
		}
	case engine.SelectEqualUnique:
		// if they are already both being sent to the same shard, we can merge
		if rRoute.RouteOpCode == engine.SelectEqualUnique {
			if lRoute.selectedVindex() == rRoute.selectedVindex() &&
				gen4ValuesEqual(ctx, lRoute.vindexExpressions(), rRoute.vindexExpressions()) {
				return merger(lRoute, rRoute)
			}
			return nil, nil
		}
		fallthrough
	case engine.SelectScatter, engine.SelectIN:
		if len(predicates) == 0 {
			// If we are doing two Scatters, we have to make sure that the
			// joins are on the correct vindex to allow them to be merged
			// no join predicates - no vindex
			return nil, nil
		}
		if !sameKeyspace {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard correlated subquery")
		}

		canMerge := canMergeOnFiltersOp(ctx, lRoute, rRoute, predicates)
		if !canMerge {
			return nil, nil
		}
		r, err := merger(lRoute, rRoute)
		r.pickBestAvailableVindex()
		return r, err
	}

}

func createRoutePlanForInnerOp(aRoute, bRoute *Route, joinPredicates []sqlparser.Expr) *Route {
	// append system table names from both the routes.
	sysTableName := aRoute.SysTableTableName
	if sysTableName == nil {
		sysTableName = bRoute.SysTableTableName
	} else {
		for k, v := range bRoute.SysTableTableName {
			sysTableName[k] = v
		}
	}

	source := &abstract.Join{
		LHS:       aRoute.Source,
		RHS:       bRoute.Source,
		Predicate: sqlparser.AndExpressions(joinPredicates...),
		LeftJoin:  false,
	}
	op := &Route{
		Source:              source,
		RouteOpCode:         aRoute.RouteOpCode,
		Keyspace:            aRoute.Keyspace,
		SysTableTableSchema: append(aRoute.SysTableTableSchema, bRoute.SysTableTableSchema...),
		SysTableTableName:   sysTableName,
		vindexPreds:         append(aRoute.vindexPreds, bRoute.vindexPreds...),
	}

	// TODO: join predicates

	if aRoute.selectedVindex() == bRoute.selectedVindex() {
		op.selected = aRoute.selected
	}

	return op
}

func (rp *Route) selectedVindex() vindexes.Vindex {
	if rp.selected == nil {
		return nil
	}
	return rp.selected.foundVindex
}

func (rp *Route) vindexExpressions() []sqlparser.Expr {
	if rp.selected == nil {
		return nil
	}
	return rp.selected.valueExprs
}
