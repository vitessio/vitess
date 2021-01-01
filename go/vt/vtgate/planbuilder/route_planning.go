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
	"container/heap"
	"sort"
	"strings"

	"vitess.io/vitess/go/sqltypes"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func newBuildSelectPlan(sel *sqlparser.Select, vschema ContextVSchema) (engine.Primitive, error) {
	semTable, err := semantics.Analyse(sel, nil) // TODO no nil no
	if err != nil {
		return nil, err
	}

	qgraph, err := createQGFromSelect(sel, semTable)
	if err != nil {
		return nil, err
	}

	var tree joinTree

	switch {
	case vschema.Planner() == V4GreedyOnly || len(qgraph.tables) > 10:
		tree, err = greedySolve(qgraph, semTable, vschema)
	case vschema.Planner() == V4GreedyOptimized:
		tree, err = greedySolveOptimized(qgraph, semTable, vschema)
	case vschema.Planner() == V4Left2Right:
		tree, err = leftToRightSolve(qgraph, semTable, vschema)
	default:
		tree, err = dpSolve(qgraph, semTable, vschema)
	}

	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(tree, semTable)
	if err != nil {
		return nil, err
	}

	if err := planProjections(sel, plan, semTable); err != nil {
		return nil, err
	}

	if err := plan.Wireup2(semTable); err != nil {
		return nil, err
	}
	return plan.Primitive(), nil
}

func planProjections(sel *sqlparser.Select, plan logicalPlan, semTable *semantics.SemTable) error {
	rb, ok := plan.(*route)
	if ok {
		rb.Select = sel
	} else {
		var projections []*sqlparser.AliasedExpr

		// TODO real horizon planning to be done
		for _, expr := range sel.SelectExprs {
			switch e := expr.(type) {
			case *sqlparser.AliasedExpr:
				projections = append(projections, e)
			default:
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not yet supported %T", e)
			}
		}

		if _, err := pushProjection(projections, plan, semTable); err != nil {
			return err
		}
	}
	return nil
}

type (
	joinTree interface {
		// solves returns the table identifiers that are solved by this plan
		solves() semantics.TableSet

		// cost is simply the number of routes in the joinTree
		cost() int
	}
	routeTable struct {
		qtable *queryTable
		vtable *vindexes.Table
	}
	routePlan struct {
		routeOpCode engine.RouteOpcode
		solved      semantics.TableSet
		keyspace    *vindexes.Keyspace

		// tables contains all the tables that are solved by this plan.
		// the tables also contain any predicates that only depend on that particular table
		tables []*routeTable

		// extraPredicates are the predicates that depend on multiple tables
		extraPredicates []sqlparser.Expr

		// vindex and conditions is set if a vindex will be used for this route.
		vindex     vindexes.Vindex
		conditions []sqlparser.Expr
	}
	joinPlan struct {
		predicates []sqlparser.Expr
		lhs, rhs   joinTree
	}
)

// solves implements the joinTree interface
func (rp *routePlan) solves() semantics.TableSet {
	return rp.solved
}

// cost implements the joinTree interface
func (*routePlan) cost() int {
	return 1
}

// vindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
type vindexPlusPredicates struct {
	vindex     *vindexes.ColumnVindex
	covered    bool
	predicates []sqlparser.Expr
}

func (rp *routePlan) addPredicate(predicates ...sqlparser.Expr) error {
	if len(rp.tables) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "addPredicate should only be called when the route has a single table")
	}

	var vindexPreds []*vindexPlusPredicates

	// Add all the column vindexes to the list of vindexPlusPredicates
	for _, columnVindex := range rp.tables[0].vtable.ColumnVindexes {
		vindexPreds = append(vindexPreds, &vindexPlusPredicates{vindex: columnVindex})
	}

	for _, filter := range predicates {
		switch node := filter.(type) {
		case *sqlparser.ComparisonExpr:
			switch node.Operator {
			case sqlparser.EqualOp:
				if sqlparser.IsNull(node.Left) || sqlparser.IsNull(node.Right) {
					// we are looking at ANDed predicates in the WHERE clause.
					// since we know that nothing returns true when compared to NULL,
					// so we can safely bail out here
					rp.routeOpCode = engine.SelectNone
					return nil
				}
				// TODO(Manan,Andres): Remove the predicates that are repeated eg. Id=1 AND Id=1
				for _, v := range vindexPreds {
					column := node.Left.(*sqlparser.ColName)
					for _, col := range v.vindex.Columns {
						// If the column for the predicate matches any column in the vindex add it to the list
						if column.Name.Equal(col) {
							v.predicates = append(v.predicates, node)
							// Vindex is covered if all the columns in the vindex have a associated predicate
							v.covered = len(v.predicates) == len(v.vindex.Columns)
						}
					}
				}
			}
		}
	}

	//TODO (Manan,Andres): Improve cost metric for vindexes
	for _, v := range vindexPreds {
		if !v.covered {
			continue
		}
		// Choose the minimum cost vindex from the ones which are covered
		if rp.vindex == nil || v.vindex.Vindex.Cost() < rp.vindex.Cost() {
			rp.vindex = v.vindex.Vindex
			rp.conditions = v.predicates
		}
	}

	if rp.vindex != nil {
		rp.routeOpCode = engine.SelectEqual
		if rp.vindex.IsUnique() {
			rp.routeOpCode = engine.SelectEqualUnique
		}
	}
	return nil
}

// Predicates takes all known predicates for this route and ANDs them together
func (rp *routePlan) Predicates() sqlparser.Expr {
	var result sqlparser.Expr
	add := func(e sqlparser.Expr) {
		if result == nil {
			result = e
			return
		}
		result = &sqlparser.AndExpr{
			Left:  result,
			Right: e,
		}
	}
	for _, t := range rp.tables {
		for _, predicate := range t.qtable.predicates {
			add(predicate)
		}
	}
	for _, p := range rp.extraPredicates {
		add(p)
	}
	return result
}

func (jp *joinPlan) solves() semantics.TableSet {
	return jp.lhs.solves() | jp.rhs.solves()
}
func (jp *joinPlan) cost() int {
	return jp.lhs.cost() + jp.rhs.cost()
}

/*
	we use dynamic programming to find the cheapest route/join tree possible,
	where the cost of a plan is the number of joins
*/
func dpSolve(qg *queryGraph, semTable *semantics.SemTable, vschema ContextVSchema) (joinTree, error) {
	size := len(qg.tables)
	dpTable := makeDPTable()

	var allTables semantics.TableSet

	// we start by seeding the table with the single routes
	for _, table := range qg.tables {
		solves := semTable.TableSetFor(table.alias)
		allTables |= solves
		plan, err := createRoutePlan(table, solves, vschema)
		if err != nil {
			return nil, err
		}
		dpTable.add(plan)
	}

	/*
		Next we'll solve bigger and bigger joins, using smaller plans to build larger ones,
		until we have a join tree covering all tables in the FROM clause
	*/
	for currentSize := 2; currentSize <= size; currentSize++ {
		lefts := dpTable.bitSetsOfSize(currentSize - 1)
		rights := dpTable.bitSetsOfSize(1)
		for _, lhs := range lefts {
			for _, rhs := range rights {
				if lhs.solves().IsOverlapping(rhs.solves()) {
					// at least one of the tables is solved on both sides
					continue
				}
				solves := lhs.solves() | rhs.solves()
				oldPlan := dpTable.planFor(solves)
				if oldPlan != nil && oldPlan.cost() == 1 {
					// we already have the perfect plan. keep it
					continue
				}
				joinPredicates := qg.getPredicates(lhs.solves(), rhs.solves())
				newPlan := createJoin(lhs, rhs, joinPredicates, semTable)
				if oldPlan == nil || newPlan.cost() < oldPlan.cost() {
					dpTable.add(newPlan)
				}
			}
		}
	}

	return dpTable.planFor(allTables), nil
}

func createJoin(lhs joinTree, rhs joinTree, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable) joinTree {
	newPlan := tryMerge(lhs, rhs, joinPredicates, semTable)
	if newPlan == nil {
		newPlan = &joinPlan{
			lhs:        lhs,
			rhs:        rhs,
			predicates: joinPredicates,
		}
	}
	return newPlan
}

type priorityQueueItem struct {
	plan     joinTree
	cost     int
	lhsSolve semantics.TableSet
	rhsSolve semantics.TableSet
}

func (pqi *priorityQueueItem) solves() semantics.TableSet {
	return pqi.lhsSolve | pqi.rhsSolve
}

type priorityQueuePlans []*priorityQueueItem

// Len implements the Heap interface
func (pq priorityQueuePlans) Len() int { return len(pq) }

// Less implements the Heap interface
func (pq priorityQueuePlans) Less(i, j int) bool {
	// We want Pop to give us the lowest cost so we use lesser than here.
	if pq[i].cost == pq[j].cost {
		return pq[i].solves() < pq[j].solves()
	}
	return pq[i].cost < pq[j].cost
}

// Swap implements the Heap interface
func (pq priorityQueuePlans) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push implements the Heap interface
func (pq *priorityQueuePlans) Push(x interface{}) {
	item := x.(*priorityQueueItem)
	*pq = append(*pq, item)
}

// Pop implements the Heap interface
func (pq *priorityQueuePlans) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

func greedySolveOptimized(qg *queryGraph, semTable *semantics.SemTable, vschema ContextVSchema) (joinTree, error) {
	routePlans := make([]*routePlan, len(qg.tables))
	intermediatePlans := map[semantics.TableSet]*priorityQueueItem{}

	for i, table := range qg.tables {
		solves := semTable.TableSetFor(table.alias)
		plan, err := createRoutePlan(table, solves, vschema)
		if err != nil {
			return nil, err
		}
		routePlans[i] = plan
		intermediatePlans[solves] = &priorityQueueItem{
			plan:     plan,
			cost:     plan.cost(),
			lhsSolve: 0,
		}
	}

	if len(qg.tables) == 1 {
		return routePlans[0], nil
	}

	pq := priorityQueuePlans{}

	for i, lhs := range routePlans {
		for j := i + 1; j < len(routePlans); j++ {
			rhs := routePlans[j]
			joinPredicates := qg.getPredicates(lhs.solves(), rhs.solves())
			plan := createJoin(lhs, rhs, joinPredicates, semTable)
			pq.Push(&priorityQueueItem{
				plan:     plan,
				cost:     plan.cost(),
				lhsSolve: lhs.solves(),
				rhsSolve: rhs.solves(),
			})
		}
	}

	heap.Init(&pq)

	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*priorityQueueItem)
		_, isLeftAvail := intermediatePlans[item.lhsSolve]
		_, isRightAvail := intermediatePlans[item.rhsSolve]
		if !isLeftAvail || !isRightAvail {
			continue
		}
		delete(intermediatePlans, item.lhsSolve)
		delete(intermediatePlans, item.rhsSolve)
		solves := item.lhsSolve | item.rhsSolve
		plan := item.plan

		for tableSet, intermPlan := range intermediatePlans {
			newPlan := createJoin(intermPlan.plan, plan, qg.getPredicates(solves, tableSet), semTable)
			heap.Push(&pq, &priorityQueueItem{
				plan:     newPlan,
				cost:     newPlan.cost(),
				lhsSolve: tableSet,
				rhsSolve: solves,
			})
		}
		intermediatePlans[solves] = item
	}

	// intermediatePlans should only have 1 value now
	if len(intermediatePlans) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "their should be only 1 intermediate planner now")
	}

	for _, item := range intermediatePlans {
		return item.plan, nil
	}

	return nil, nil
}

/*
	The greedy planner will plan a query by finding first finding the best route plan for every table.
    Then, iteratively, it finds the cheapest join that can be produced between the remaining plans,
	and removes the two inputs to this cheapest plan and instead adds the join.
	As an optimization, it first only considers joining tables that have predicates defined between them
*/
func greedySolve(qg *queryGraph, semTable *semantics.SemTable, vschema ContextVSchema) (joinTree, error) {
	plans := make([]joinTree, len(qg.tables))
	planCache := map[semantics.TableSet]joinTree{}

	// we start by seeding the table with the single routes
	for i, table := range qg.tables {
		solves := semTable.TableSetFor(table.alias)
		plan, err := createRoutePlan(table, solves, vschema)
		if err != nil {
			return nil, err
		}
		plans[i] = plan
	}

	crossJoinsOK := false
	for len(plans) > 1 {
		bestPlan, lIdx, rIdx := findBestJoin(qg, semTable, plans, planCache, crossJoinsOK)
		if bestPlan != nil {
			// if we found a best plan, we'll replace the two plans that were joined with the join plan created
			plans = removeAt(plans, rIdx)
			plans = removeAt(plans, lIdx)
			plans = append(plans, bestPlan)
		} else {
			// we will only fail to find a join plan when there are only cross joins left
			// when that happens, we switch over to allow cross joins as well.
			// this way we prioritize joining plans with predicates first
			crossJoinsOK = true
		}
	}

	return plans[0], nil
}

func findBestJoin(
	qg *queryGraph,
	semTable *semantics.SemTable,
	plans []joinTree,
	planCache map[semantics.TableSet]joinTree,
	crossJoinsOK bool,
) (joinTree, int, int) {
	var lIdx, rIdx int
	var bestPlan joinTree

	for i, lhs := range plans {
		for j := i + 1; j < len(plans); j++ {
			rhs := plans[j]
			solves := lhs.solves() | rhs.solves()
			joinPredicates := qg.getPredicates(lhs.solves(), rhs.solves())
			if len(joinPredicates) == 0 && !crossJoinsOK {
				// if there are no predicates joining the to tables,
				// creating a join between them would produce a
				// cartesian product, which is almost always a bad idea
				continue
			}
			plan := planCache[solves]
			if plan == nil {
				plan = createJoin(lhs, rhs, joinPredicates, semTable)
				planCache[solves] = plan
				if plan.cost() == 1 {
					// if we are able to merge the two inputs into a single route,
					// we shortcut here and pick this plan. this limits the search space
					return plan, i, j
				}
			}

			if bestPlan == nil || plan.cost() < bestPlan.cost() {
				bestPlan = plan
				// remember which plans we based on, so we can remove them later
				lIdx = i
				rIdx = j
			}
		}
	}
	return bestPlan, lIdx, rIdx
}

func leftToRightSolve(qg *queryGraph, semTable *semantics.SemTable, vschema ContextVSchema) (joinTree, error) {
	plans := make([]joinTree, len(qg.tables))

	// we start by seeding the table with the single routes
	for i, table := range qg.tables {
		solves := semTable.TableSetFor(table.alias)
		plan, err := createRoutePlan(table, solves, vschema)
		if err != nil {
			return nil, err
		}
		plans[i] = plan
	}

	var acc joinTree
	for _, plan := range plans {
		if acc == nil {
			acc = plan
			continue
		}
		joinPredicates := qg.getPredicates(acc.solves(), plan.solves())
		acc = createJoin(acc, plan, joinPredicates, semTable)
	}

	return acc, nil
}

func removeAt(plans []joinTree, idx int) []joinTree {
	return append(plans[:idx], plans[idx+1:]...)
}

func createRoutePlan(table *queryTable, solves semantics.TableSet, vschema ContextVSchema) (*routePlan, error) {
	vschemaTable, _, _, _, _, err := vschema.FindTableOrVindex(table.table)
	if err != nil {
		return nil, err
	}
	plan := &routePlan{
		solved: solves,
		tables: []*routeTable{{
			qtable: table,
			vtable: vschemaTable,
		}},
		keyspace: vschemaTable.Keyspace,
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
	err = plan.addPredicate(table.predicates...)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func transformToLogicalPlan(tree joinTree, semTable *semantics.SemTable) (logicalPlan, error) {
	switch n := tree.(type) {
	case *routePlan:
		var tablesForSelect sqlparser.TableExprs
		tableNameMap := map[string]interface{}{}

		for _, t := range n.tables {
			tablesForSelect = append(tablesForSelect, t.qtable.alias)
			tableNameMap[sqlparser.String(t.qtable.alias.Expr)] = nil
		}
		predicates := n.Predicates()
		var where *sqlparser.Where
		if predicates != nil {
			where = &sqlparser.Where{Expr: predicates, Type: sqlparser.WhereClause}
		}
		var values []sqltypes.PlanValue
		if len(n.conditions) == 1 {
			value, err := sqlparser.NewPlanValue(n.conditions[0].(*sqlparser.ComparisonExpr).Right)
			if err != nil {
				return nil, err
			}
			values = []sqltypes.PlanValue{value}
		}
		var singleColumn vindexes.SingleColumn
		if n.vindex != nil {
			singleColumn = n.vindex.(vindexes.SingleColumn)
		}

		var tableNames []string
		for name := range tableNameMap {
			tableNames = append(tableNames, name)
		}
		sort.Strings(tableNames)

		return &route{
			eroute: &engine.Route{
				Opcode:    n.routeOpCode,
				TableName: strings.Join(tableNames, ", "),
				Keyspace:  n.keyspace,
				Vindex:    singleColumn,
				Values:    values,
			},
			Select: &sqlparser.Select{
				From:  tablesForSelect,
				Where: where,
			},
			solvedTables: n.solved,
		}, nil

	case *joinPlan:

		lhsSolves := n.lhs.solves()
		lhsColMap := map[*sqlparser.ColName]sqlparser.Argument{}
		for _, predicate := range n.predicates {
			sqlparser.Rewrite(predicate, func(cursor *sqlparser.Cursor) bool {
				switch node := cursor.Node().(type) {
				case *sqlparser.ColName:
					if semTable.Dependencies(node).IsSolvedBy(lhsSolves) {
						arg := sqlparser.NewArgument([]byte(":" + node.CompliantName("")))
						lhsColMap[node] = arg
						cursor.Replace(arg)
					}
				}
				return true
			}, nil)
		}

		var lhsColList []*sqlparser.ColName
		for col := range lhsColMap {
			lhsColList = append(lhsColList, col)
		}

		var lhsColExpr []*sqlparser.AliasedExpr
		for _, col := range lhsColList {
			lhsColExpr = append(lhsColExpr, &sqlparser.AliasedExpr{
				Expr: col,
			})
		}

		lhs, err := transformToLogicalPlan(n.lhs, semTable)
		if err != nil {
			return nil, err
		}
		offset, err := pushProjection(lhsColExpr, lhs, semTable)
		if err != nil {
			return nil, err
		}

		vars := map[string]int{}

		for _, col := range lhsColList {
			vars[col.CompliantName("")] = offset
			offset++
		}

		rhs, err := transformToLogicalPlan(n.rhs, semTable)
		if err != nil {
			return nil, err
		}

		err = pushPredicate(n.predicates, rhs, semTable)
		if err != nil {
			return nil, err
		}

		return &join2{
			Left:  lhs,
			Right: rhs,
			Vars:  vars,
		}, nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unknown type encountered: %T", tree)
}

func findColumnVindex(a *routePlan, exp sqlparser.Expr, sem *semantics.SemTable) vindexes.SingleColumn {
	left, isCol := exp.(*sqlparser.ColName)
	if !isCol {
		return nil
	}
	leftDep := sem.Dependencies(left)
	for _, table := range a.tables {
		if leftDep.IsSolvedBy(table.qtable.tableID) {
			for _, vindex := range table.vtable.ColumnVindexes {
				singCol, isSingle := vindex.Vindex.(vindexes.SingleColumn)
				if isSingle && vindex.Columns[0].Equal(left.Name) {
					return singCol
				}
			}
		}
	}
	return nil
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
		if canMergeOnFilter(a, b, predicate, semTable) {
			return true
		}
	}
	return false
}

func tryMerge(a, b joinTree, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable) joinTree {
	aRoute, ok := a.(*routePlan)
	if !ok {
		return nil
	}
	bRoute, ok := b.(*routePlan)
	if !ok {
		return nil
	}
	if aRoute.keyspace != bRoute.keyspace {
		return nil
	}

	newTabletSet := aRoute.solved | bRoute.solved
	r := &routePlan{
		routeOpCode:     aRoute.routeOpCode,
		solved:          newTabletSet,
		tables:          append(aRoute.tables, bRoute.tables...),
		extraPredicates: append(aRoute.extraPredicates, bRoute.extraPredicates...),
		keyspace:        aRoute.keyspace,
	}
	r.extraPredicates = append(r.extraPredicates, joinPredicates...)

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
		if aRoute.routeOpCode == engine.SelectEqualUnique {
			r.vindex = aRoute.vindex
			r.conditions = aRoute.conditions
		} else if bRoute.routeOpCode == engine.SelectEqualUnique {
			r.routeOpCode = bRoute.routeOpCode
			r.vindex = bRoute.vindex
			r.conditions = bRoute.conditions
		}
	}

	return r
}
