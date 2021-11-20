package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type opCacheMap map[tableSetPair]abstract.Operator

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

func seedOpList(ctx *planningContext, qg *abstract.QueryGraph) ([]abstract.Operator, error) {
	ops := make([]abstract.Operator, len(qg.Tables))
	for i, table := range qg.Tables {
		id := ctx.semTable.TableSetFor(table.Alias)
		op, err := createRouteOp(ctx, table, id)
		if err != nil {
			return nil, err
		}
		if len(table.Predicates) == 0 {
			ops[i] = op
			continue
		}
		ops[i] = &abstract.Filter{
			Source:     op,
			Predicates: table.Predicates,
		}
	}
	return ops, nil
}

func addPredicates(op abstract.Operator, predicates []sqlparser.Expr) abstract.Operator {
	if len(predicates) == 0 {
		return op
	}
	return &abstract.Filter{
		Source:     op,
		Predicates: predicates,
	}
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
