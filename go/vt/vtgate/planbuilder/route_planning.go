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
	semTable, err := semantics.Analyse(sel) // TODO no nil no
	if err != nil {
		return nil, err
	}

	qgraph, err := createQGFromSelect(sel, semTable)
	if err != nil {
		return nil, err
	}

	var tree joinTree

	switch {
	case vschema.Planner() == V4Left2Right:
		tree, err = leftToRightSolve(qgraph, semTable, vschema)
	default:
		tree, err = greedySolve(qgraph, semTable, vschema)
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

	plan, err = planLimit(sel.Limit, plan)
	if err != nil {
		return nil, err
	}

	if err := plan.WireupV4(semTable); err != nil {
		return nil, err
	}
	return plan.Primitive(), nil
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

func planProjections(sel *sqlparser.Select, plan logicalPlan, semTable *semantics.SemTable) error {
	rb, ok := plan.(*route)
	if ok {
		ast := rb.Select.(*sqlparser.Select)
		ast.Distinct = sel.Distinct
		ast.GroupBy = sel.GroupBy
		ast.OrderBy = sel.OrderBy
		ast.SelectExprs = sel.SelectExprs
		ast.Comments = sel.Comments
	} else {

		// TODO real horizon planning to be done
		for _, expr := range sel.SelectExprs {
			switch e := expr.(type) {
			case *sqlparser.AliasedExpr:
				if _, err := pushProjection(e, plan, semTable); err != nil {
					return err
				}
			default:
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not yet supported %T", e)
			}
		}

	}
	return nil
}

type (
	joinTree interface {
		// tables returns the table identifiers that are solved by this plan
		tables() semantics.TableSet

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

		// _tables contains all the tables that are solved by this plan.
		// the tables also contain any predicates that only depend on that particular table
		_tables routeTables

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
	routeTables []*routeTable
)

// tables implements the joinTree interface
func (rp *routePlan) tables() semantics.TableSet {
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
	if len(rp._tables) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "addPredicate should only be called when the route has a single table")
	}

	var vindexPreds []*vindexPlusPredicates

	// Add all the column vindexes to the list of vindexPlusPredicates
	for _, columnVindex := range rp._tables[0].vtable.ColumnVindexes {
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
	for _, t := range rp._tables {
		for _, predicate := range t.qtable.predicates {
			add(predicate)
		}
	}
	for _, p := range rp.extraPredicates {
		add(p)
	}
	return result
}

func (jp *joinPlan) tables() semantics.TableSet {
	return jp.lhs.tables() | jp.rhs.tables()
}
func (jp *joinPlan) cost() int {
	return jp.lhs.cost() + jp.rhs.cost()
}

func createJoin(lhs, rhs joinTree, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable) joinTree {
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
func greedySolve(qg *queryGraph, semTable *semantics.SemTable, vschema ContextVSchema) (joinTree, error) {
	joinTrees, err := seedPlanList(qg, semTable, vschema)
	planCache := cacheMap{}
	if err != nil {
		return nil, err
	}

	crossJoinsOK := false
	for len(joinTrees) > 1 {
		bestTree, lIdx, rIdx := findBestJoinTree(qg, semTable, joinTrees, planCache, crossJoinsOK)
		if bestTree != nil {
			// if we found a best plan, we'll replace the two joinTrees that were joined with the join plan created
			joinTrees = removeAt(joinTrees, rIdx)
			joinTrees = removeAt(joinTrees, lIdx)
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

func (cm cacheMap) getJoinTreeFor(lhs, rhs joinTree, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable) joinTree {
	solves := tableSetPair{left: lhs.tables(), right: rhs.tables()}
	plan := cm[solves]
	if plan == nil {
		plan = createJoin(lhs, rhs, joinPredicates, semTable)
		cm[solves] = plan
	}
	return plan
}

func findBestJoinTree(
	qg *queryGraph,
	semTable *semantics.SemTable,
	plans []joinTree,
	planCache cacheMap,
	crossJoinsOK bool,
) (joinTree, int, int) {
	var lIdx, rIdx int
	var bestPlan joinTree

	for i, lhs := range plans {
		for j, rhs := range plans {
			if i == j {
				continue
			}
			joinPredicates := qg.getPredicates(lhs.tables(), rhs.tables())
			if len(joinPredicates) == 0 && !crossJoinsOK {
				// if there are no predicates joining the two tables,
				// creating a join between them would produce a
				// cartesian product, which is almost always a bad idea
				continue
			}
			plan := planCache.getJoinTreeFor(lhs, rhs, joinPredicates, semTable)

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
		joinPredicates := qg.getPredicates(acc.tables(), plan.tables())
		acc = createJoin(acc, plan, joinPredicates, semTable)
	}

	return acc, nil
}

// seedPlanList returns a routePlan for each table in the qg
func seedPlanList(qg *queryGraph, semTable *semantics.SemTable, vschema ContextVSchema) ([]joinTree, error) {
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
	return plans, nil
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
		_tables: []*routeTable{{
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
		return transformRoutePlan(n)

	case *joinPlan:
		return transformJoinPlan(n, semTable)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unknown type encountered: %T", tree)
}

func transformJoinPlan(n *joinPlan, semTable *semantics.SemTable) (*joinV4, error) {
	lhsColList := extractColumnsNeededFromLHS(n, semTable, n.lhs.tables())

	lhs, err := transformToLogicalPlan(n.lhs, semTable)
	if err != nil {
		return nil, err
	}

	vars := map[string]int{}
	for _, col := range lhsColList {
		offset, err := pushProjection(&sqlparser.AliasedExpr{Expr: col}, lhs, semTable)
		if err != nil {
			return nil, err
		}
		vars[col.CompliantName("")] = offset
	}

	rhs, err := transformToLogicalPlan(n.rhs, semTable)
	if err != nil {
		return nil, err
	}

	err = pushPredicate(n.predicates, rhs, semTable)
	if err != nil {
		return nil, err
	}

	return &joinV4{
		Left:  lhs,
		Right: rhs,
		Vars:  vars,
	}, nil
}

func extractColumnsNeededFromLHS(n *joinPlan, semTable *semantics.SemTable, lhsSolves semantics.TableSet) []*sqlparser.ColName {
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
	return lhsColList
}

func transformRoutePlan(n *routePlan) (*route, error) {
	var tablesForSelect sqlparser.TableExprs
	tableNameMap := map[string]interface{}{}

	sort.Sort(n._tables)
	for _, t := range n._tables {
		alias := sqlparser.AliasedTableExpr{
			Expr: sqlparser.TableName{
				Name: t.vtable.Name,
			},
			Partitions: nil,
			As:         t.qtable.alias.As,
			Hints:      nil,
		}
		tablesForSelect = append(tablesForSelect, &alias)
		tableNameMap[sqlparser.String(t.qtable.table.Name)] = nil
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
		tables: n.solved,
	}, nil
}

func findColumnVindex(a *routePlan, exp sqlparser.Expr, sem *semantics.SemTable) vindexes.SingleColumn {
	left, isCol := exp.(*sqlparser.ColName)
	if !isCol {
		return nil
	}
	leftDep := sem.Dependencies(left)
	for _, table := range a._tables {
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
		routeOpCode: aRoute.routeOpCode,
		solved:      newTabletSet,
		_tables:     append(aRoute._tables, bRoute._tables...),
		extraPredicates: append(
			append(aRoute.extraPredicates, bRoute.extraPredicates...),
			joinPredicates...),
		keyspace: aRoute.keyspace,
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

var _ sort.Interface = (routeTables)(nil)

func (r routeTables) Len() int {
	return len(r)
}

func (r routeTables) Less(i, j int) bool {
	return r[i].qtable.tableID < r[j].qtable.tableID
}

func (r routeTables) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
