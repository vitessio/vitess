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
	"strings"

	"vitess.io/vitess/go/sqltypes"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ selectPlanner = gen4Planner

func gen4Planner(_ string) func(sqlparser.Statement, sqlparser.BindVars, ContextVSchema) (engine.Primitive, error) {
	return func(stmt sqlparser.Statement, reservedVars sqlparser.BindVars, vschema ContextVSchema) (engine.Primitive, error) {
		sel, ok := stmt.(*sqlparser.Select)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T not yet supported", stmt)
		}
		return newBuildSelectPlan(sel, vschema)
	}
}

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
	case vschema.Planner() == Gen4Left2Right:
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
	if ok && rb.isSingleShard() {
		ast := rb.Select.(*sqlparser.Select)
		ast.Distinct = sel.Distinct
		ast.GroupBy = sel.GroupBy
		ast.OrderBy = sel.OrderBy
		ast.SelectExprs = sel.SelectExprs
		ast.Comments = sel.Comments
	} else {
		// TODO real horizon planning to be done
		if sel.Distinct {
			return semantics.Gen4NotSupportedF("DISTINCT")
		}
		if sel.GroupBy != nil {
			return semantics.Gen4NotSupportedF("GROUP BY")
		}
		for _, expr := range sel.SelectExprs {
			switch e := expr.(type) {
			case *sqlparser.AliasedExpr:
				if nodeHasAggregates(e.Expr) {
					return semantics.Gen4NotSupportedF("aggregation [%s]", sqlparser.String(e))
				}
				if _, err := pushProjection(e, plan, semTable); err != nil {
					return err
				}
			default:
				return semantics.Gen4NotSupportedF("%T", e)
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

		// creates a copy of the joinTree that can be updated without changing the original
		clone() joinTree

		pushOutputColumns([]*sqlparser.ColName, *semantics.SemTable) int
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

		// vindex and vindexValues is set if a vindex will be used for this route.
		vindex       vindexes.Vindex
		vindexValues []sqltypes.PlanValue

		// here we store the possible vindexes we can use so that when we add predicates to the plan,
		// we can quickly check if the new predicates enables any new vindex options
		vindexPreds []*vindexPlusPredicates

		// columns needed to feed other plans
		columns []*sqlparser.ColName
	}
	joinPlan struct {
		// columns needed to feed other plans
		columns []int

		// arguments that need to be copied from the LHS/RHS
		vars map[string]int

		lhs, rhs joinTree
	}
	routeTables []*routeTable
)

var _ joinTree = (*routePlan)(nil)
var _ joinTree = (*joinPlan)(nil)

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
	case // these op codes will never be compared with each other - they are assigned by a rule and not a comparison
		engine.SelectDBA,
		engine.SelectNext,
		engine.SelectNone,
		engine.SelectReference,
		engine.SelectUnsharded:
		return 0
	// TODO revisit these costs when more of the gen4 planner is done
	case engine.SelectEqualUnique:
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
	vindex *vindexes.ColumnVindex
	values []sqltypes.PlanValue
	// Vindex is covered if all the columns in the vindex have an associated predicate
	covered bool
}

// addPredicate clones this routePlan and returns a new one with these predicates added to it. if the predicates can help,
// they will improve the routeOpCode
func (rp *routePlan) addPredicate(predicates ...sqlparser.Expr) error {
	newVindexFound, err := rp.searchForNewVindexes(predicates)
	if err != nil {
		return err
	}

	// if we didn't open up any new vindex options, no need to enter here
	if newVindexFound {
		rp.pickBestAvailableVindex()
	}

	// any predicates that cover more than a single table need to be added here
	rp.predicates = append(rp.predicates, predicates...)

	return nil
}

func (rp *routePlan) searchForNewVindexes(predicates []sqlparser.Expr) (bool, error) {
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
					return false, nil
				}
				// TODO(Manan,Andres): Remove the predicates that are repeated eg. Id=1 AND Id=1
				for _, v := range rp.vindexPreds {
					if v.covered {
						// already covered by an earlier predicate
						continue
					}
					column, ok := node.Left.(*sqlparser.ColName)
					other := node.Right
					if !ok {
						column, ok = node.Right.(*sqlparser.ColName)
						other = node.Left
					}
					value, err := sqlparser.NewPlanValue(other)
					if err != nil {
						// if we are unable to create a PlanValue, we can't use a vindex, but we don't have to fail
						if strings.Contains(err.Error(), "expression is too complex") {
							continue
						}
						// something else went wrong, return the error
						return false, err
					}
					if ok {
						for _, col := range v.vindex.Columns {
							// If the column for the predicate matches any column in the vindex add it to the list
							if column.Name.Equal(col) {
								v.values = append(v.values, value)
								// Vindex is covered if all the columns in the vindex have a associated predicate
								v.covered = len(v.values) == len(v.vindex.Columns)
								newVindexFound = newVindexFound || v.covered
							}
						}
					}
				}
			default:
				return false, semantics.Gen4NotSupportedF("%s", sqlparser.String(filter))
			}
		}
	}
	return newVindexFound, nil
}

// pickBestAvailableVindex goes over the available vindexes for this route and picks the best one available.
func (rp *routePlan) pickBestAvailableVindex() {
	for _, v := range rp.vindexPreds {
		if !v.covered {
			continue
		}
		// Choose the minimum cost vindex from the ones which are covered
		if rp.vindex == nil || v.vindex.Vindex.Cost() < rp.vindex.Cost() {
			rp.vindex = v.vindex.Vindex
			rp.vindexValues = v.values
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

func (rp *routePlan) pushOutputColumns(col []*sqlparser.ColName, _ *semantics.SemTable) int {
	newCol := len(rp.columns)
	rp.columns = append(rp.columns, col...)
	return newCol
}

func (jp *joinPlan) tables() semantics.TableSet {
	return jp.lhs.tables() | jp.rhs.tables()
}

func (jp *joinPlan) cost() int {
	return jp.lhs.cost() + jp.rhs.cost()
}

func (jp *joinPlan) clone() joinTree {
	result := &joinPlan{
		lhs: jp.lhs.clone(),
		rhs: jp.rhs.clone(),
	}
	return result
}

func (jp *joinPlan) pushOutputColumns(columns []*sqlparser.ColName, semTable *semantics.SemTable) int {
	resultIdx := len(jp.columns)
	var toTheLeft []bool
	var lhs, rhs []*sqlparser.ColName
	for _, col := range columns {
		if semTable.Dependencies(col).IsSolvedBy(jp.lhs.tables()) {
			lhs = append(lhs, col)
			toTheLeft = append(toTheLeft, true)
		} else {
			rhs = append(rhs, col)
			toTheLeft = append(toTheLeft, false)
		}
	}
	lhsOffset := jp.lhs.pushOutputColumns(lhs, semTable)
	rhsOffset := -jp.rhs.pushOutputColumns(rhs, semTable)

	for _, left := range toTheLeft {
		if left {
			jp.columns = append(jp.columns, lhsOffset)
			lhsOffset++
		} else {
			jp.columns = append(jp.columns, rhsOffset)
			rhsOffset--
		}
	}
	return resultIdx
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
		// we break up the predicates so that colnames from the LHS are replaced by arguments
		var rhsPreds []sqlparser.Expr
		var lhsColumns []*sqlparser.ColName
		lhsSolves := node.lhs.tables()
		for _, expr := range exprs {
			cols, predicate, err := breakPredicateInLHSandRHS(expr, semTable, lhsSolves)
			if err != nil {
				return nil, err
			}
			lhsColumns = append(lhsColumns, cols...)
			rhsPreds = append(rhsPreds, predicate)
		}
		node.pushOutputColumns(lhsColumns, semTable)
		rhsPlan, err := pushPredicate2(rhsPreds, node.rhs, semTable)
		if err != nil {
			return nil, err
		}
		return &joinPlan{
			lhs: node.lhs,
			rhs: rhsPlan,
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
				columns = append(columns, node)
				arg := sqlparser.NewArgument(":" + node.CompliantName(""))
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

func mergeOrJoin(lhs, rhs joinTree, joinPredicates []sqlparser.Expr, semTable *semantics.SemTable) (joinTree, error) {
	newPlan := tryMerge(lhs, rhs, joinPredicates, semTable)
	if newPlan != nil {
		return newPlan, nil
	}

	tree := &joinPlan{lhs: lhs.clone(), rhs: rhs.clone()}
	return pushPredicate2(joinPredicates, tree, semTable)
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
	if vschemaTable.Name.String() != table.table.Name.String() {
		return nil, semantics.Gen4NotSupportedF("routed tables")
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
