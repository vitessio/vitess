/*
Copyright 2020 The Vitess Authors.

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
	"fmt"
	"io"
	"sort"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ selectPlanner = gen4Planner

func gen4Planner(_ string) func(sqlparser.Statement, *sqlparser.ReservedVars, ContextVSchema) (engine.Primitive, error) {
	return func(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema ContextVSchema) (engine.Primitive, error) {
		sel, ok := stmt.(*sqlparser.Select)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T not yet supported", stmt)
		}
		return newBuildSelectPlan(sel, vschema)
	}
}

func newBuildSelectPlan(sel *sqlparser.Select, vschema ContextVSchema) (engine.Primitive, error) {

	directives := sqlparser.ExtractCommentDirectives(sel.Comments)
	if len(directives) > 0 {
		return nil, semantics.Gen4NotSupportedF("comment directives")
	}
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil {
		return nil, err
	}
	semTable, err := semantics.Analyze(sel, keyspace.Name, vschema)
	if err != nil {
		return nil, err
	}

	sel, err = expandStar(sel, semTable)
	if err != nil {
		return nil, err
	}

	opTree, err := abstract.CreateOperatorFromSelect(sel, semTable)
	if err != nil {
		return nil, err
	}

	tree, err := optimizeQuery(opTree, semTable, vschema)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(tree, semTable)
	if err != nil {
		return nil, err
	}

	plan, err = planHorizon(sel, plan, semTable)
	if err != nil {
		return nil, err
	}

	plan, err = planLimit(sel.Limit, plan)
	if err != nil {
		return nil, err
	}

	if err := plan.WireupGen4(semTable); err != nil {
		return nil, err
	}
	return plan.Primitive(), nil
}

func optimizeQuery(opTree abstract.Operator, semTable *semantics.SemTable, vschema ContextVSchema) (joinTree, error) {
	switch op := opTree.(type) {
	case *abstract.QueryGraph:
		switch {
		case vschema.Planner() == Gen4Left2Right:
			return leftToRightSolve(op, semTable, vschema)
		default:
			return greedySolve(op, semTable, vschema)
		}
	case *abstract.LeftJoin:
		treeInner, err := optimizeQuery(op.Left, semTable, vschema)
		if err != nil {
			return nil, err
		}
		treeOuter, err := optimizeQuery(op.Right, semTable, vschema)
		if err != nil {
			return nil, err
		}
		return mergeOrJoin(treeInner, treeOuter, []sqlparser.Expr{op.Predicate}, semTable, false)
	case *abstract.Join:
		treeInner, err := optimizeQuery(op.LHS, semTable, vschema)
		if err != nil {
			return nil, err
		}
		treeOuter, err := optimizeQuery(op.RHS, semTable, vschema)
		if err != nil {
			return nil, err
		}
		return mergeOrJoin(treeInner, treeOuter, []sqlparser.Expr{op.Exp}, semTable, true)

	default:
		return nil, semantics.Gen4NotSupportedF("optimizeQuery")
	}
}

func planLimit(limit *sqlparser.Limit, plan logicalPlan) (logicalPlan, error) {
	if limit == nil {
		return plan, nil
	}
	rb, ok := plan.(*route)
	if ok && rb.isSingleShard() {
		rb.SetLimit(limit)
		return plan, nil
	}

	lPlan, err := createLimit(plan, limit)
	if err != nil {
		return nil, err
	}

	// visit does not modify the plan.
	_, err = visit(lPlan, setUpperLimit)
	if err != nil {
		return nil, err
	}
	return lPlan, nil
}

func planHorizon(sel *sqlparser.Select, plan logicalPlan, semTable *semantics.SemTable) (logicalPlan, error) {
	rb, ok := plan.(*route)
	if !ok && semTable.ProjectionErr != nil {
		return nil, semTable.ProjectionErr
	}

	if ok && rb.isSingleShard() {
		createSingleShardRoutePlan(sel, rb)
		return plan, nil
	}

	if err := checkUnsupportedConstructs(sel); err != nil {
		return nil, err
	}

	qp, err := createQPFromSelect(sel)
	if err != nil {
		return nil, err
	}
	for _, e := range qp.selectExprs {
		if _, _, err := pushProjection(e, plan, semTable, true); err != nil {
			return nil, err
		}
	}

	for _, expr := range qp.aggrExprs {
		funcExpr, ok := expr.Expr.(*sqlparser.FuncExpr)
		if !ok {
			return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "expected an aggregation here")
		}
		if funcExpr.Distinct {
			return nil, semantics.Gen4NotSupportedF("distinct aggregation")
		}
	}

	if len(qp.aggrExprs) > 0 {
		plan, err = planAggregations(qp, plan, semTable)
		if err != nil {
			return nil, err
		}
	}
	if len(sel.OrderBy) > 0 {
		plan, err = planOrderBy(qp, qp.orderExprs, plan, semTable)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}

func createSingleShardRoutePlan(sel *sqlparser.Select, rb *route) {
	ast := rb.Select.(*sqlparser.Select)
	ast.Distinct = sel.Distinct
	ast.GroupBy = sel.GroupBy
	ast.OrderBy = sel.OrderBy
	ast.Comments = sel.Comments
	ast.SelectExprs = sel.SelectExprs
	for i, expr := range ast.SelectExprs {
		if aliasedExpr, ok := expr.(*sqlparser.AliasedExpr); ok {
			ast.SelectExprs[i] = removeQualifierFromColName(aliasedExpr)
		}
	}
}

func checkUnsupportedConstructs(sel *sqlparser.Select) error {
	if sel.Distinct {
		return semantics.Gen4NotSupportedF("DISTINCT")
	}
	if sel.GroupBy != nil {
		return semantics.Gen4NotSupportedF("GROUP BY")
	}
	if sel.Having != nil {
		return semantics.Gen4NotSupportedF("HAVING")
	}
	return nil
}

func pushJoinPredicate(exprs []sqlparser.Expr, tree joinTree, semTable *semantics.SemTable) (joinTree, error) {
	switch node := tree.(type) {
	case *routePlan:
		plan := node.clone().(*routePlan)
		err := plan.addPredicate(exprs...)
		if err != nil {
			return nil, err
		}
		return plan, nil

	case *joinPlan:
		node = node.clone().(*joinPlan)

		// we break up the predicates so that colnames from the LHS are replaced by arguments
		var rhsPreds []sqlparser.Expr
		var lhsColumns []*sqlparser.ColName
		lhsSolves := node.lhs.tableID()
		for _, expr := range exprs {
			cols, predicate, err := breakPredicateInLHSandRHS(expr, semTable, lhsSolves)
			if err != nil {
				return nil, err
			}
			lhsColumns = append(lhsColumns, cols...)
			rhsPreds = append(rhsPreds, predicate)
		}
		node.pushOutputColumns(lhsColumns, semTable)
		rhsPlan, err := pushJoinPredicate(rhsPreds, node.rhs, semTable)
		if err != nil {
			return nil, err
		}

		return &joinPlan{
			lhs:   node.lhs,
			rhs:   rhsPlan,
			outer: node.outer,
		}, nil
	default:
		panic(fmt.Sprintf("BUG: unknown type %T", node))
	}
}

func breakPredicateInLHSandRHS(expr sqlparser.Expr, semTable *semantics.SemTable, lhs semantics.TableSet) (columns []*sqlparser.ColName, predicate sqlparser.Expr, err error) {
	predicate = sqlparser.CloneExpr(expr)
	_ = sqlparser.Rewrite(predicate, nil, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.ColName:
			deps := semTable.Dependencies(node)
			if deps == 0 {
				err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown column. has the AST been copied?")
				return false
			}
			if deps.IsSolvedBy(lhs) {
				node.Qualifier.Qualifier = sqlparser.NewTableIdent("")
				columns = append(columns, node)
				arg := sqlparser.NewArgument(node.CompliantName())
				cursor.Replace(arg)
			}
		}
		return true
	})
	if err != nil {
		return nil, nil, err
	}
	return
}

func mergeOrJoinInner(lhs, rhs joinTree, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable) (joinTree, error) {
	return mergeOrJoin(lhs, rhs, joinPredicates, semTable, true)
}

func mergeOrJoin(lhs, rhs joinTree, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable, inner bool) (joinTree, error) {
	newPlan := tryMerge(lhs, rhs, joinPredicates, semTable, inner)
	if newPlan != nil {
		return newPlan, nil
	}

	tree := &joinPlan{lhs: lhs.clone(), rhs: rhs.clone(), outer: !inner}
	return pushJoinPredicate(joinPredicates, tree, semTable)
}

type (
	tableSetPair struct {
		left, right semantics.TableSet
	}
	cacheMap map[tableSetPair]joinTree
)

/*
	The greedy planner will plan a query by finding first finding the best route plan for every table.
    Then, iteratively, it finds the cheapest join that can be produced between the remaining plans,
	and removes the two inputs to this cheapest plan and instead adds the join.
	As an optimization, it first only considers joining tables that have predicates defined between them
*/
func greedySolve(qg *abstract.QueryGraph, semTable *semantics.SemTable, vschema ContextVSchema) (joinTree, error) {
	joinTrees, err := seedPlanList(qg, semTable, vschema)
	planCache := cacheMap{}
	if err != nil {
		return nil, err
	}

	tree, err := mergeJoinTrees(qg, semTable, joinTrees, planCache, false)
	if err != nil {
		return nil, err
	}
	return tree, nil
}

func mergeJoinTrees(qg *abstract.QueryGraph, semTable *semantics.SemTable, joinTrees []joinTree, planCache cacheMap, crossJoinsOK bool) (joinTree, error) {
	if len(joinTrees) == 0 {
		return nil, nil
	}
	for len(joinTrees) > 1 {
		bestTree, lIdx, rIdx, err := findBestJoinTree(qg, semTable, joinTrees, planCache, crossJoinsOK)
		if err != nil {
			return nil, err
		}
		// if we found a best plan, we'll replace the two plans that were joined with the join plan created
		if bestTree != nil {
			// we need to remove the larger of the two plans first
			if rIdx > lIdx {
				joinTrees = removeAt(joinTrees, rIdx)
				joinTrees = removeAt(joinTrees, lIdx)
			} else {
				joinTrees = removeAt(joinTrees, lIdx)
				joinTrees = removeAt(joinTrees, rIdx)
			}
			joinTrees = append(joinTrees, bestTree)
		} else {
			// we will only fail to find a join plan when there are only cross joins left
			// when that happens, we switch over to allow cross joins as well.
			// this way we prioritize joining joinTrees with predicates first
			crossJoinsOK = true
		}
	}
	return joinTrees[0], nil
}

func (cm cacheMap) getJoinTreeFor(lhs, rhs joinTree, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable) (joinTree, error) {
	solves := tableSetPair{left: lhs.tableID(), right: rhs.tableID()}
	cachedPlan := cm[solves]
	if cachedPlan != nil {
		return cachedPlan, nil
	}

	join, err := mergeOrJoinInner(lhs, rhs, joinPredicates, semTable)
	if err != nil {
		return nil, err
	}
	cm[solves] = join
	return join, nil
}

func findBestJoinTree(
	qg *abstract.QueryGraph,
	semTable *semantics.SemTable,
	plans []joinTree,
	planCache cacheMap,
	crossJoinsOK bool,
) (bestPlan joinTree, lIdx int, rIdx int, err error) {
	for i, lhs := range plans {
		for j, rhs := range plans {
			if i == j {
				continue
			}
			joinPredicates := qg.GetPredicates(lhs.tableID(), rhs.tableID())
			if len(joinPredicates) == 0 && !crossJoinsOK {
				// if there are no predicates joining the two tables,
				// creating a join between them would produce a
				// cartesian product, which is almost always a bad idea
				continue
			}
			plan, err := planCache.getJoinTreeFor(lhs, rhs, joinPredicates, semTable)
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

func leftToRightSolve(qg *abstract.QueryGraph, semTable *semantics.SemTable, vschema ContextVSchema) (joinTree, error) {
	plans, err := seedPlanList(qg, semTable, vschema)
	if err != nil {
		return nil, err
	}

	var acc joinTree
	for _, plan := range plans {
		if acc == nil {
			acc = plan
			continue
		}
		joinPredicates := qg.GetPredicates(acc.tableID(), plan.tableID())
		acc, err = mergeOrJoinInner(acc, plan, joinPredicates, semTable)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

// seedPlanList returns a routePlan for each table in the qg
func seedPlanList(qg *abstract.QueryGraph, semTable *semantics.SemTable, vschema ContextVSchema) ([]joinTree, error) {
	plans := make([]joinTree, len(qg.Tables))

	// we start by seeding the table with the single routes
	for i, table := range qg.Tables {
		solves := semTable.TableSetFor(table.Alias)
		plan, err := createRoutePlan(table, solves, vschema)
		if err != nil {
			return nil, err
		}
		if qg.NoDeps != nil {
			plan.predicates = append(plan.predicates, sqlparser.SplitAndExpression(nil, qg.NoDeps)...)
		}
		plans[i] = plan
	}
	return plans, nil
}

func removeAt(plans []joinTree, idx int) []joinTree {
	return append(plans[:idx], plans[idx+1:]...)
}

func createRoutePlan(table *abstract.QueryTable, solves semantics.TableSet, vschema ContextVSchema) (*routePlan, error) {
	vschemaTable, _, _, _, _, err := vschema.FindTableOrVindex(table.Table)
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
	plan := &routePlan{
		solved: solves,
		tables: []relation{&routeTable{
			qtable: table,
			vtable: vschemaTable,
		}},
		keyspace: vschemaTable.Keyspace,
	}

	for _, columnVindex := range vschemaTable.ColumnVindexes {
		plan.vindexPreds = append(plan.vindexPreds, &vindexPlusPredicates{colVindex: columnVindex})
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
	default:
		plan.routeOpCode = engine.SelectScatter
	}
	err = plan.addPredicate(table.Predicates...)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func findColumnVindex(a *routePlan, exp sqlparser.Expr, sem *semantics.SemTable) vindexes.SingleColumn {
	left, isCol := exp.(*sqlparser.ColName)
	if !isCol {
		return nil
	}
	leftDep := sem.Dependencies(left)

	var singCol vindexes.SingleColumn

	_ = visitTables(a.tables, func(table *routeTable) error {
		if leftDep.IsSolvedBy(table.qtable.TableID) {
			for _, vindex := range table.vtable.ColumnVindexes {
				sC, isSingle := vindex.Vindex.(vindexes.SingleColumn)
				if isSingle && vindex.Columns[0].Equal(left.Name) {
					singCol = sC
					return io.EOF
				}
			}
		}
		return nil
	})

	return singCol
}

func canMergeOnFilter(a, b *routePlan, predicate sqlparser.Expr, sem *semantics.SemTable) bool {
	comparison, ok := predicate.(*sqlparser.ComparisonExpr)
	if !ok {
		return false
	}
	if comparison.Operator != sqlparser.EqualOp {
		return false
	}
	left := comparison.Left
	right := comparison.Right

	lVindex := findColumnVindex(a, left, sem)
	if lVindex == nil {
		left, right = right, left
		lVindex = findColumnVindex(a, left, sem)
	}
	if lVindex == nil || !lVindex.IsUnique() {
		return false
	}
	rVindex := findColumnVindex(b, right, sem)
	if rVindex == nil {
		return false
	}
	return rVindex == lVindex
}

func canMergeOnFilters(a, b *routePlan, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable) bool {
	for _, predicate := range joinPredicates {
		for _, expr := range sqlparser.SplitAndExpression(nil, predicate) {
			if canMergeOnFilter(a, b, expr, semTable) {
				return true
			}
		}
	}
	return false
}

func tryMerge(a, b joinTree, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable, inner bool) joinTree {
	aRoute, bRoute := joinTreesToRoutes(a, b)
	if aRoute == nil || bRoute == nil {
		return nil
	}

	newTabletSet := aRoute.solved | bRoute.solved

	var r *routePlan
	if inner {
		r = createRoutePlanForInner(aRoute, bRoute, newTabletSet, joinPredicates)
	} else {
		r = createRoutePlanForOuter(aRoute, bRoute, semTable, newTabletSet, joinPredicates)
	}

	switch aRoute.routeOpCode {
	case engine.SelectUnsharded, engine.SelectDBA:
		if aRoute.routeOpCode != bRoute.routeOpCode {
			return nil
		}
	case engine.SelectScatter, engine.SelectEqualUnique:
		if len(joinPredicates) == 0 {
			// If we are doing two Scatters, we have to make sure that the
			// joins are on the correct vindex to allow them to be merged
			// no join predicates - no vindex
			return nil
		}

		canMerge := canMergeOnFilters(aRoute, bRoute, joinPredicates, semTable)
		if !canMerge {
			return nil
		}
		r.pickBestAvailableVindex()
	}
	return r
}

func joinTreesToRoutes(a, b joinTree) (*routePlan, *routePlan) {
	aRoute, ok := a.(*routePlan)
	if !ok {
		return nil, nil
	}
	bRoute, ok := b.(*routePlan)
	if !ok {
		return nil, nil
	}
	if aRoute.keyspace != bRoute.keyspace {
		return nil, nil
	}
	return aRoute, bRoute
}

func createRoutePlanForInner(aRoute *routePlan, bRoute *routePlan, newTabletSet semantics.TableSet, joinPredicates []sqlparser.Expr) *routePlan {
	var tables parenTables
	if !aRoute.hasOuterjoins() {
		tables = append(aRoute.tables, bRoute.tables...)
	} else {
		tables = append(parenTables{aRoute.tables}, bRoute.tables...)
	}
	return &routePlan{
		routeOpCode: aRoute.routeOpCode,
		solved:      newTabletSet,
		tables:      tables,
		predicates: append(
			append(aRoute.predicates, bRoute.predicates...),
			joinPredicates...),
		keyspace:    aRoute.keyspace,
		vindexPreds: append(aRoute.vindexPreds, bRoute.vindexPreds...),
		leftJoins:   append(aRoute.leftJoins, bRoute.leftJoins...),
	}
}

func findTables(deps semantics.TableSet, tables parenTables) (relation, relation, parenTables) {
	foundTables := parenTables{}
	newTables := parenTables{}

	for i, t := range tables {
		if t.tableID().IsSolvedBy(deps) {
			foundTables = append(foundTables, t)
			if len(foundTables) == 2 {
				return foundTables[0], foundTables[1], append(newTables, tables[i:])
			}
		} else {
			newTables = append(newTables, t)
		}
	}
	return nil, nil, tables
}

func createRoutePlanForOuter(aRoute, bRoute *routePlan, semTable *semantics.SemTable, newTabletSet semantics.TableSet, joinPredicates []sqlparser.Expr) *routePlan {
	// create relation slice with all tables
	tables := bRoute.tables
	// we are doing an outer join where the outer part contains multiple tables - we have to turn the outer part into a join or two
	for _, predicate := range bRoute.predicates {
		deps := semTable.Dependencies(predicate)
		aTbl, bTbl, newTables := findTables(deps, tables)
		tables = newTables
		if aTbl != nil && bTbl != nil {
			tables = append(tables, &leJoin{
				lhs:  aTbl,
				rhs:  bTbl,
				pred: predicate,
			})
		}
	}

	var outer relation
	if len(tables) == 1 {
		// if we have a single relation, no need to put it inside parens
		outer = tables[0]
	} else {
		outer = tables
	}

	return &routePlan{
		routeOpCode: aRoute.routeOpCode,
		solved:      newTabletSet,
		tables:      aRoute.tables,
		leftJoins: append(aRoute.leftJoins, &outerTable{
			right: outer,
			pred:  sqlparser.AndExpressions(joinPredicates...),
		}),
		keyspace:    aRoute.keyspace,
		vindexPreds: append(aRoute.vindexPreds, bRoute.vindexPreds...),
	}
}

var _ sort.Interface = (parenTables)(nil)

func (p parenTables) Len() int {
	return len(p)
}

func (p parenTables) Less(i, j int) bool {
	return p[i].tableID() < p[j].tableID()
}

func (p parenTables) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
