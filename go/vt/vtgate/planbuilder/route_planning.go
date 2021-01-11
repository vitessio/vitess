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
	"sort"

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

	if err := plan.WireupV4(semTable); err != nil {
		return nil, err
	}
	return plan.Primitive(), nil
}

func planProjections(sel *sqlparser.Select, plan logicalPlan, semTable *semantics.SemTable) error {
	rb, ok := plan.(*route)
	if ok {
		ast := rb.Select.(*sqlparser.Select)
		ast.Distinct = sel.Distinct
		ast.GroupBy = sel.GroupBy
		ast.OrderBy = sel.OrderBy
		ast.Limit = sel.Limit
		ast.SelectExprs = sel.SelectExprs
		ast.Comments = sel.Comments
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
		// tables returns the table identifiers that are solved by this plan
		tables() semantics.TableSet

		// cost is simply the number of routes in the joinTree
		cost() int

		// creates a copy of the joinTree that can be updated without changing the original
		clone() joinTree
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

		// predicates are the predicates evaluated by this plan
		predicates []sqlparser.Expr

		// vindex and conditions is set if a vindex will be used for this route.
		vindex     vindexes.Vindex
		conditions []sqlparser.Expr

		// here we store the possible vindexes we can use so that when we add predicates to the plan,
		// we can quickly check if the new predicates enables any new vindex options
		vindexPreds []*vindexPlusPredicates
	}
	joinPlan struct {
		predicates []sqlparser.Expr
		lhs, rhs   joinTree
	}
	routeTables []*routeTable
)

// clone returns a copy of the struct with copies of slices,
// so changing the the contents of them will not be reflected in the original
func (rp *routePlan) clone() joinTree {
	result := *rp
	result.vindexPreds = make([]*vindexPlusPredicates, len(rp.vindexPreds))
	for i, pred := range rp.vindexPreds {
		// we do this to create a copy of the struct
		p := *pred
		result.vindexPreds[i] = &p
	}
	return &result
}

// tables implements the joinTree interface
func (rp *routePlan) tables() semantics.TableSet {
	return rp.solved
}

// cost implements the joinTree interface
func (rp *routePlan) cost() int {
	switch rp.routeOpCode {
	case
		engine.SelectDBA,
		engine.SelectEqualUnique,
		engine.SelectNext,
		engine.SelectNone,
		engine.SelectReference,
		engine.SelectUnsharded:
		return 1
	case engine.SelectEqual:
		return 5
	case engine.SelectIN:
		return 10
	case engine.SelectMultiEqual:
		return 10
	case engine.SelectScatter:
		return 20
	}
	return 1
}

// vindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
type vindexPlusPredicates struct {
	vindex     *vindexes.ColumnVindex
	predicates []sqlparser.Expr
	// Vindex is covered if all the columns in the vindex have an associated predicate
	covered bool
}

// addPredicate clones this routePlan and returns a new one with these predicates added to it. if the predicates can help,
// they will improve the routeOpCode
func (rp *routePlan) addPredicate(predicates ...sqlparser.Expr) error {
	newVindexFound := false
	for _, filter := range predicates {
		switch node := filter.(type) {
		case *sqlparser.ComparisonExpr:
			switch node.Operator {
			case sqlparser.EqualOp:
				// here we are searching for predicates in the form n.col = XYZ
				if sqlparser.IsNull(node.Left) || sqlparser.IsNull(node.Right) {
					// we are looking at ANDed predicates in the WHERE clause.
					// since we know that nothing returns true when compared to NULL,
					// so we can safely bail out here
					rp.routeOpCode = engine.SelectNone
					return nil
				}
				// TODO(Manan,Andres): Remove the predicates that are repeated eg. Id=1 AND Id=1
				for _, v := range rp.vindexPreds {
					if v.covered {
						// already covered by an earlier predicate
						continue
					}
					column := node.Left.(*sqlparser.ColName)
					for _, col := range v.vindex.Columns {
						// If the column for the predicate matches any column in the vindex add it to the list
						if column.Name.Equal(col) {
							v.predicates = append(v.predicates, node)
							// Vindex is covered if all the columns in the vindex have a associated predicate
							v.covered = len(v.predicates) == len(v.vindex.Columns)
							newVindexFound = newVindexFound || v.covered
						}
					}
				}
			}
		}
	}

	// if we didn't open up any new vindex options, no need to enter here
	if newVindexFound {
		rp.pickBestAvailableVindex()
	}

	// any predicates that cover more than a single table need to be added here
	rp.predicates = append(rp.predicates, predicates...)

	return nil
}

// pickBestAvailableVindex goes over the available vindexes for this route and picks the best one available.
func (rp *routePlan) pickBestAvailableVindex() {
	//TODO (Manan,Andres): Improve cost metric for vindexes
	for _, v := range rp.vindexPreds {
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
	for _, p := range rp.predicates {
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
func (jp *joinPlan) clone() joinTree {
	result := &joinPlan{
		predicates: make([]sqlparser.Expr, len(jp.predicates)),
		lhs:        jp.lhs.clone(),
		rhs:        jp.rhs.clone(),
	}
	copy(result.predicates, jp.predicates)
	return result
}

func pushPredicate2(exprs []sqlparser.Expr, tree joinTree, semTable *semantics.SemTable) (joinTree, error) {
	switch node := tree.(type) {
	case *routePlan:
		plan := node.clone().(*routePlan)
		err := plan.addPredicate(exprs...)
		if err != nil {
			return nil, err
		}
		return plan, nil

	case *joinPlan:
		// we need to figure out which predicates go where
		var lhsPreds, rhsPreds []sqlparser.Expr
		lhsSolves := node.lhs.tables()
		rhsSolves := node.rhs.tables()
		for _, expr := range exprs {
			// TODO: we should not do this.
			// we should instead do the same as go/vt/vtgate/planbuilder/jointree_transformers.go:46
			deps := semTable.Dependencies(expr)
			switch {
			case deps.IsSolvedBy(lhsSolves):
				lhsPreds = append(lhsPreds, expr)
			case deps.IsSolvedBy(rhsSolves):
				rhsPreds = append(rhsPreds, expr)
			default:
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown dependencies for %s", sqlparser.String(expr))
			}
		}
		lhsPlan, err := pushPredicate2(lhsPreds, node.lhs, semTable)
		if err != nil {
			return nil, err
		}
		rhsPlan, err := pushPredicate2(rhsPreds, node.rhs, semTable)
		if err != nil {
			return nil, err
		}
		return &joinPlan{
			predicates: exprs,
			lhs:        lhsPlan,
			rhs:        rhsPlan,
		}, nil
	default:
		panic(fmt.Sprintf("BUG: unknown type %T", node))
	}
}

func mergeOrJoin(lhs, rhs joinTree, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable) (joinTree, error) {
	newPlan := tryMerge(lhs, rhs, joinPredicates, semTable)
	if newPlan != nil {
		return newPlan, nil
	}

	return pushPredicate2(joinPredicates, &joinPlan{lhs: lhs, rhs: rhs}, semTable)
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
		bestTree, lIdx, rIdx, err := findBestJoinTree(qg, semTable, joinTrees, planCache, crossJoinsOK)
		if err != nil {
			return nil, err
		}
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

func (cm cacheMap) getJoinTreeFor(lhs, rhs joinTree, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable) (joinTree, error) {
	solves := tableSetPair{left: lhs.tables(), right: rhs.tables()}
	cachedPlan := cm[solves]
	if cachedPlan != nil {
		return cachedPlan, nil
	}

	join, err := mergeOrJoin(lhs, rhs, joinPredicates, semTable)
	if err != nil {
		return nil, err
	}
	cm[solves] = join
	return join, nil
}

func findBestJoinTree(
	qg *queryGraph,
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
			joinPredicates := qg.getPredicates(lhs.tables(), rhs.tables())
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
		acc, err = mergeOrJoin(acc, plan, joinPredicates, semTable)
		if err != nil {
			return nil, err
		}
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

	for _, columnVindex := range vschemaTable.ColumnVindexes {
		plan.vindexPreds = append(plan.vindexPreds, &vindexPlusPredicates{vindex: columnVindex})
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
		predicates: append(
			append(aRoute.predicates, bRoute.predicates...),
			joinPredicates...),
		keyspace:    aRoute.keyspace,
		vindexPreds: append(aRoute.vindexPreds, bRoute.vindexPreds...),
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
