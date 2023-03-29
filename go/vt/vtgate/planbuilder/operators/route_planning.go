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

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type (
	tableSetPair struct {
		left, right semantics.TableSet
	}

	opCacheMap map[tableSetPair]ops.Operator
)

// TransformToPhysical takes an operator tree and rewrites any parts that have not yet been planned as physical operators.
// This is where a lot of the optimisations of the query plans are done.
// Here we try to merge query parts into the same route primitives. At the end of this process,
// all the operators in the tree are guaranteed to be PhysicalOperators
func transformToPhysical(ctx *plancontext.PlanningContext, in ops.Operator) (ops.Operator, error) {
	op, err := rewrite.BottomUpAll(in, TableID, func(operator ops.Operator, ts semantics.TableSet) (ops.Operator, rewrite.TreeIdentity, error) {
		switch op := operator.(type) {
		case *QueryGraph:
			return optimizeQueryGraph(ctx, op)
		case *Join:
			return optimizeJoin(ctx, op)
		case *Derived:
			return optimizeDerived(ctx, op)
		case *SubQuery:
			return optimizeSubQuery(ctx, op, ts)
		case *Filter:
			return optimizeFilter(op)
		default:
			return operator, rewrite.SameTree, nil
		}
	})

	if err != nil {
		return nil, err
	}

	err = rewrite.Visit(op, func(op ops.Operator) error {
		if _, isPhys := op.(ops.PhysicalOperator); !isPhys {
			return vterrors.VT13001(fmt.Sprintf("failed to transform %T to a physical operator", op))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return Compact(ctx, op)
}

func optimizeFilter(op *Filter) (ops.Operator, rewrite.TreeIdentity, error) {
	if route, ok := op.Source.(*Route); ok {
		// let's push the filter into the route
		op.Source = route.Source
		route.Source = op
		return route, rewrite.NewTree, nil
	}

	return op, rewrite.SameTree, nil
}

func optimizeDerived(ctx *plancontext.PlanningContext, op *Derived) (ops.Operator, rewrite.TreeIdentity, error) {
	innerRoute, ok := op.Source.(*Route)
	if !ok {
		return op, rewrite.SameTree, nil
	}

	if !(innerRoute.Routing.OpCode() == engine.EqualUnique) && !op.IsMergeable(ctx) {
		// no need to check anything if we are sure that we will only hit a single shard
		return op, rewrite.SameTree, nil
	}

	op.Source = innerRoute.Source
	innerRoute.Source = op

	return innerRoute, rewrite.NewTree, nil
}

func optimizeJoin(ctx *plancontext.PlanningContext, op *Join) (ops.Operator, rewrite.TreeIdentity, error) {
	join, err := mergeOrJoin(ctx, op.LHS, op.RHS, sqlparser.SplitAndExpression(nil, op.Predicate), !op.LeftJoin)
	if err != nil {
		return nil, false, err
	}
	return join, rewrite.NewTree, nil
}

func optimizeQueryGraph(ctx *plancontext.PlanningContext, op *QueryGraph) (result ops.Operator, changed rewrite.TreeIdentity, err error) {
	changed = rewrite.NewTree
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
		result = newFilter(result, ctx.SemTable.AndExpressions(unresolved...))
	}

	return
}

func buildVindexTableForDML(
	ctx *plancontext.PlanningContext,
	tableInfo semantics.TableInfo,
	table *QueryTable,
	dmlType string,
) (*vindexes.Table, Routing, error) {
	vindexTable := tableInfo.GetVindexTable()
	if vindexTable.Source != nil {
		sourceTable, _, _, _, _, err := ctx.VSchema.FindTableOrVindex(vindexTable.Source.TableName)
		if err != nil {
			return nil, nil, err
		}
		vindexTable = sourceTable
	}

	if !vindexTable.Keyspace.Sharded {
		return vindexTable, &AnyShardRouting{keyspace: vindexTable.Keyspace}, nil
	}

	var dest key.Destination
	var typ topodatapb.TabletType
	var err error
	tblName, ok := table.Alias.Expr.(sqlparser.TableName)
	if !ok {
		return nil, nil, vterrors.VT12001("multi shard UPDATE with LIMIT")
	}

	_, _, _, typ, dest, err = ctx.VSchema.FindTableOrVindex(tblName)
	if err != nil {
		return nil, nil, err
	}
	if dest == nil {
		routing := &ShardedRouting{
			keyspace:    vindexTable.Keyspace,
			RouteOpCode: engine.Scatter,
		}
		return vindexTable, routing, nil
	}

	if typ != topodatapb.TabletType_PRIMARY {
		return nil, nil, vterrors.VT09002(dmlType)
	}

	// we are dealing with an explicitly targeted UPDATE
	routing := &TargetedRouting{
		keyspace:          vindexTable.Keyspace,
		TargetDestination: dest,
	}
	return vindexTable, routing, nil
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
func greedySolve(ctx *plancontext.PlanningContext, qg *QueryGraph) (ops.Operator, error) {
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

func leftToRightSolve(ctx *plancontext.PlanningContext, qg *QueryGraph) (ops.Operator, error) {
	plans, err := seedOperatorList(ctx, qg)
	if err != nil {
		return nil, err
	}

	var acc ops.Operator
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
func seedOperatorList(ctx *plancontext.PlanningContext, qg *QueryGraph) ([]ops.Operator, error) {
	plans := make([]ops.Operator, len(qg.Tables))

	// we start by seeding the table with the single routes
	for i, table := range qg.Tables {
		solves := ctx.SemTable.TableSetFor(table.Alias)
		plan, err := createRoute(ctx, table, solves)
		if err != nil {
			return nil, err
		}
		if qg.NoDeps != nil {
			plan, err = plan.AddPredicate(ctx, qg.NoDeps)
			if err != nil {
				return nil, err
			}
		}
		plans[i] = plan
	}
	return plans, nil
}

func createInfSchemaRoute(ctx *plancontext.PlanningContext, table *QueryTable) (ops.Operator, error) {
	ks, err := ctx.VSchema.AnyKeyspace()
	if err != nil {
		return nil, err
	}
	var src ops.Operator = &Table{
		QTable: table,
		VTable: &vindexes.Table{
			Name:     table.Table.Name,
			Keyspace: ks,
		},
	}
	var routing Routing = &InfoSchemaRouting{}
	for _, pred := range table.Predicates {
		routing, err = UpdateRoutingLogic(ctx, pred, routing)
		if err != nil {
			return nil, err
		}
	}
	return &Route{
		Source:  src,
		Routing: routing,
	}, nil
}

func mergeRoutes(ctx *plancontext.PlanningContext, qg *QueryGraph, physicalOps []ops.Operator, planCache opCacheMap, crossJoinsOK bool) (ops.Operator, error) {
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
				return nil, vterrors.VT13001("should not happen: we should be able to merge cross joins")
			}
			// we will only fail to find a join plan when there are only cross joins left
			// when that happens, we switch over to allow cross joins as well.
			// this way we prioritize joining physicalOps with predicates first
			crossJoinsOK = true
		}
	}
	return physicalOps[0], nil
}

func removeAt(plans []ops.Operator, idx int) []ops.Operator {
	return append(plans[:idx], plans[idx+1:]...)
}

func findBestJoin(
	ctx *plancontext.PlanningContext,
	qg *QueryGraph,
	plans []ops.Operator,
	planCache opCacheMap,
	crossJoinsOK bool,
) (bestPlan ops.Operator, lIdx int, rIdx int, err error) {
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

func getJoinFor(ctx *plancontext.PlanningContext, cm opCacheMap, lhs, rhs ops.Operator, joinPredicates []sqlparser.Expr) (ops.Operator, error) {
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
func requiresSwitchingSides(ctx *plancontext.PlanningContext, op ops.Operator) bool {
	required := false

	_ = rewrite.Visit(op, func(current ops.Operator) error {
		derived, isDerived := current.(*Derived)

		if isDerived && !derived.IsMergeable(ctx) {
			required = true
			return io.EOF
		}

		return nil
	})

	return required
}

func mergeOrJoin(ctx *plancontext.PlanningContext, lhs, rhs ops.Operator, joinPredicates []sqlparser.Expr, inner bool) (ops.Operator, error) {
	newPlan, err := Merge(ctx, lhs, rhs, joinPredicates, newJoinMerge(ctx, joinPredicates, inner))
	if err != nil {
		return nil, err
	}
	if newPlan != nil {
		return newPlan, nil
	}

	if len(joinPredicates) > 0 && requiresSwitchingSides(ctx, rhs) {
		if !inner {
			return nil, vterrors.VT12001("LEFT JOIN with derived tables")
		}

		if requiresSwitchingSides(ctx, lhs) {
			return nil, vterrors.VT12001("JOIN between derived tables")
		}

		join := NewApplyJoin(Clone(rhs), Clone(lhs), nil, !inner)
		return pushJoinPredicates(ctx, joinPredicates, join)
	}

	join := NewApplyJoin(Clone(lhs), Clone(rhs), nil, !inner)
	return pushJoinPredicates(ctx, joinPredicates, join)
}

func operatorsToRoutes(a, b ops.Operator) (*Route, *Route) {
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

func findColumnVindex(ctx *plancontext.PlanningContext, a ops.Operator, exp sqlparser.Expr) vindexes.SingleColumn {
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

		_ = rewrite.Visit(a, func(rel ops.Operator) error {
			to, isTableOp := rel.(TableIDIntroducer)
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

		exp = semantics.RewriteDerivedTableExpression(exp, tbl)
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
	case *sqlparser.Argument:
		b, ok := b.(*sqlparser.Argument)
		if !ok {
			return false
		}
		return a.Name == b.Name
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

func pushJoinPredicates(ctx *plancontext.PlanningContext, exprs []sqlparser.Expr, op *ApplyJoin) (ops.Operator, error) {
	if len(exprs) == 0 {
		return op, nil
	}

	for _, expr := range exprs {
		_, err := AddPredicate(op, ctx, expr, true, newFilter)
		if err != nil {
			return nil, err
		}
	}

	return op, nil
}
