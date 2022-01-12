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

package physical

import (
	"bytes"
	"fmt"
	"io"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/context"

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
	tableSetPair struct {
		left, right semantics.TableSet
	}

	opCacheMap map[tableSetPair]abstract.PhysicalOperator
)

func CreatePhysicalOperator(ctx *context.PlanningContext, opTree abstract.LogicalOperator) (abstract.PhysicalOperator, error) {
	switch op := opTree.(type) {
	case *abstract.QueryGraph:
		switch {
		// case ctx.vschema.Planner() == Gen4Left2Right:
		//	return leftToRightSolve(ctx, op)
		default:
			return greedySolve(ctx, op)
		}
	case *abstract.Join:
		opInner, err := CreatePhysicalOperator(ctx, op.LHS)
		if err != nil {
			return nil, err
		}
		opOuter, err := CreatePhysicalOperator(ctx, op.RHS)
		if err != nil {
			return nil, err
		}
		return mergeOrJoinOp(ctx, opInner, opOuter, sqlparser.SplitAndExpression(nil, op.Predicate), !op.LeftJoin)
	case *abstract.Derived:
		opInner, err := CreatePhysicalOperator(ctx, op.Inner)
		if err != nil {
			return nil, err
		}
		return &Derived{
			Source:        opInner,
			Query:         op.Sel,
			Alias:         op.Alias,
			ColumnAliases: op.ColumnAliases,
		}, nil
	case *abstract.SubQuery:
		return optimizeSubQueryOp(ctx, op)
	case *abstract.Vindex:
		return optimizeVindexOp(ctx, op)
	case *abstract.Concatenate:
		return optimizeUnionOp(ctx, op)
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
func greedySolve(ctx *context.PlanningContext, qg *abstract.QueryGraph) (abstract.PhysicalOperator, error) {
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
func seedOperatorList(ctx *context.PlanningContext, qg *abstract.QueryGraph) ([]abstract.PhysicalOperator, error) {
	plans := make([]abstract.PhysicalOperator, len(qg.Tables))

	// we start by seeding the table with the single routes
	for i, table := range qg.Tables {
		solves := ctx.SemTable.TableSetFor(table.Alias)
		plan, err := createRouteOperator(ctx, table, solves)
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

func createRouteOperator(ctx *context.PlanningContext, table *abstract.QueryTable, solves semantics.TableSet) (*Route, error) {
	if table.IsInfSchema {
		return createInfSchemaRoute(ctx, table)
	}
	vschemaTable, _, _, _, _, err := ctx.VSchema.FindTableOrVindex(table.Table)
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
		plan.Selected = &VindexOption{
			Ready:       true,
			Values:      []evalengine.Expr{evalengine.NewLiteralString(vschemaTable.Pinned, collations.TypedCollation{})},
			ValueExprs:  nil,
			Predicates:  nil,
			OpCode:      engine.SelectEqualUnique,
			FoundVindex: vindex,
			Cost: Cost{
				OpCode: engine.SelectEqualUnique,
			},
		}
	default:
		plan.RouteOpCode = engine.SelectScatter
	}
	for _, predicate := range table.Predicates {
		err = plan.UpdateRoutingLogic(ctx, predicate)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}

func createInfSchemaRoute(ctx *context.PlanningContext, table *abstract.QueryTable) (*Route, error) {
	ks, err := ctx.VSchema.AnyKeyspace()
	if err != nil {
		return nil, err
	}
	var src abstract.PhysicalOperator = &Table{
		QTable: table,
		VTable: &vindexes.Table{
			Name:     table.Table.Name,
			Keyspace: ks,
		},
	}
	r := &Route{
		RouteOpCode: engine.SelectDBA,
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

func mergeRouteOps(ctx *context.PlanningContext, qg *abstract.QueryGraph, physicalOps []abstract.PhysicalOperator, planCache opCacheMap, crossJoinsOK bool) (abstract.PhysicalOperator, error) {
	if len(physicalOps) == 0 {
		return nil, nil
	}
	for len(physicalOps) > 1 {
		bestTree, lIdx, rIdx, err := findBestJoinOp(ctx, qg, physicalOps, planCache, crossJoinsOK)
		if err != nil {
			return nil, err
		}
		// if we found a plan, we'll replace the two plans that were joined with the join plan created
		if bestTree != nil {
			// we remove one plan, and replace the other
			if rIdx > lIdx {
				physicalOps = removeOpAt(physicalOps, rIdx)
				physicalOps = removeOpAt(physicalOps, lIdx)
			} else {
				physicalOps = removeOpAt(physicalOps, lIdx)
				physicalOps = removeOpAt(physicalOps, rIdx)
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

func removeOpAt(plans []abstract.PhysicalOperator, idx int) []abstract.PhysicalOperator {
	return append(plans[:idx], plans[idx+1:]...)
}

func findBestJoinOp(
	ctx *context.PlanningContext,
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

func getJoinOpFor(ctx *context.PlanningContext, cm opCacheMap, lhs, rhs abstract.PhysicalOperator, joinPredicates []sqlparser.Expr) (abstract.PhysicalOperator, error) {
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

func mergeOrJoinOp(ctx *context.PlanningContext, lhs, rhs abstract.PhysicalOperator, joinPredicates []sqlparser.Expr, inner bool) (abstract.PhysicalOperator, error) {

	merger := func(a, b *Route) (*Route, error) {
		return createRouteOperatorForJoin(ctx, a, b, joinPredicates, inner)
	}

	newPlan, _ := tryMergeOp(ctx, lhs, rhs, joinPredicates, merger)
	if newPlan != nil {
		return newPlan, nil
	}

	var tree abstract.PhysicalOperator = &ApplyJoin{
		LHS:      lhs.Clone(),
		RHS:      rhs.Clone(),
		Vars:     map[string]int{},
		LeftJoin: !inner,
	}
	for _, predicate := range joinPredicates {
		var err error
		tree, err = PushPredicate(ctx, predicate, tree)
		if err != nil {
			return nil, err
		}
	}
	return tree, nil
}

func createRouteOperatorForJoin(ctx *context.PlanningContext, aRoute, bRoute *Route, joinPredicates []sqlparser.Expr, inner bool) (*Route, error) {
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
		SysTableTableName:   sysTableName,
		Source: &ApplyJoin{
			LHS:      aRoute.Source,
			RHS:      bRoute.Source,
			Vars:     map[string]int{},
			LeftJoin: !inner,
		},
	}

	for _, predicate := range joinPredicates {
		op, err := PushPredicate(ctx, predicate, r)
		if err != nil {
			return nil, err
		}
		route, ok := op.(*Route)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] did not expect type to change when pushing predicates")
		}
		r = route
	}

	if aRoute.SelectedVindex() == bRoute.SelectedVindex() {
		r.Selected = aRoute.Selected
	}

	return r, nil
}

type mergeOpFunc func(a, b *Route) (*Route, error)

// makeRouteOp return the input as a Route.
// if the input is a Derived operator and has a Route as its source,
// we push the Derived inside the Route and return it.
func makeRouteOp(j abstract.PhysicalOperator) *Route {
	route, ok := j.(*Route)
	if ok {
		return route
	}

	derived, ok := j.(*Derived)
	if !ok {
		return nil
	}
	dp := derived.Clone().(*Derived)

	route = makeRouteOp(dp.Source)
	if route == nil {
		return nil
	}

	derived.Source = route.Source
	route.Source = derived
	return route
}

func operatorsToRoutes(a, b abstract.PhysicalOperator) (*Route, *Route) {
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

func tryMergeOp(ctx *context.PlanningContext, a, b abstract.PhysicalOperator, joinPredicates []sqlparser.Expr, merger mergeOpFunc) (abstract.PhysicalOperator, error) {
	aRoute, bRoute := operatorsToRoutes(a.Clone(), b.Clone())
	if aRoute == nil || bRoute == nil {
		return nil, nil
	}

	sameKeyspace := aRoute.Keyspace == bRoute.Keyspace

	if sameKeyspace || (isDualTableOp(aRoute) || isDualTableOp(bRoute)) {
		tree, err := tryMergeReferenceTableOp(aRoute, bRoute, merger)
		if tree != nil || err != nil {
			return tree, err
		}
	}

	switch aRoute.RouteOpCode {
	case engine.SelectUnsharded, engine.SelectDBA:
		if aRoute.RouteOpCode == bRoute.RouteOpCode {
			return merger(aRoute, bRoute)
		}
	case engine.SelectEqualUnique:
		// if they are already both being sent to the same shard, we can merge
		if bRoute.RouteOpCode == engine.SelectEqualUnique {
			aVdx := aRoute.SelectedVindex()
			bVdx := bRoute.SelectedVindex()
			aExpr := aRoute.VindexExpressions()
			bExpr := bRoute.VindexExpressions()
			if aVdx == bVdx && gen4ValuesEqual(ctx, aExpr, bExpr) {
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
		r.PickBestAvailableVindex()
		return r, nil
	}
	return nil, nil
}

func isDualTableOp(route *Route) bool {
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

func leaves(op abstract.Operator) (sources []abstract.Operator) {
	switch op := op.(type) {
	// these are the leaves
	case *abstract.QueryGraph, *abstract.Vindex, *Table:
		return []abstract.Operator{op}

		// logical
	case *abstract.Concatenate:
		for _, source := range op.Sources {
			sources = append(sources, leaves(source)...)
		}
		return
	case *abstract.Derived:
		return []abstract.Operator{op.Inner}
	case *abstract.Join:
		return []abstract.Operator{op.LHS, op.RHS}
	case *abstract.SubQuery:
		sources = []abstract.Operator{op.Outer}
		for _, inner := range op.Inner {
			sources = append(sources, inner.Inner)
		}
		return
		// physical
	case *ApplyJoin:
		return []abstract.Operator{op.LHS, op.RHS}
	case *Filter:
		return []abstract.Operator{op.Source}
	case *Route:
		return []abstract.Operator{op.Source}
	}

	panic(fmt.Sprintf("leaves unknown type: %T", op))
}

func tryMergeReferenceTableOp(aRoute, bRoute *Route, merger mergeOpFunc) (*Route, error) {
	// if either side is a reference table, we can just merge it and use the opcode of the other side
	var opCode engine.RouteOpcode
	var selected *VindexOption

	switch {
	case aRoute.RouteOpCode == engine.SelectReference:
		selected = bRoute.Selected
		opCode = bRoute.RouteOpCode
	case bRoute.RouteOpCode == engine.SelectReference:
		selected = aRoute.Selected
		opCode = aRoute.RouteOpCode
	default:
		return nil, nil
	}

	r, err := merger(aRoute, bRoute)
	if err != nil {
		return nil, err
	}
	r.RouteOpCode = opCode
	r.Selected = selected
	return r, nil
}

func canMergeOpsOnFilter(ctx *context.PlanningContext, a, b *Route, predicate sqlparser.Expr) bool {
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

func findColumnVindexOnOps(ctx *context.PlanningContext, a *Route, exp sqlparser.Expr) vindexes.SingleColumn {
	_, isCol := exp.(*sqlparser.ColName)
	if !isCol {
		return nil
	}

	var singCol vindexes.SingleColumn

	// for each equality expression that exp has with other column name, we check if it
	// can be solved by any table in our routeTree a. If an equality expression can be solved,
	// we check if the equality expression and our table share the same vindex, if they do:
	// the method will return the associated vindexes.SingleColumn.
	for _, expr := range ctx.SemTable.GetExprAndEqualities(exp) {
		col, isCol := expr.(*sqlparser.ColName)
		if !isCol {
			continue
		}
		leftDep := ctx.SemTable.RecursiveDeps(expr)

		_ = VisitOperators(a, func(rel abstract.PhysicalOperator) (bool, error) {
			to, isTableOp := rel.(*Table)
			if !isTableOp {
				return true, nil
			}
			if leftDep.IsSolvedBy(to.QTable.ID) {
				for _, vindex := range to.VTable.ColumnVindexes {
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

func canMergeOpsOnFilters(ctx *context.PlanningContext, a, b *Route, joinPredicates []sqlparser.Expr) bool {
	for _, predicate := range joinPredicates {
		for _, expr := range sqlparser.SplitAndExpression(nil, predicate) {
			if canMergeOpsOnFilter(ctx, a, b, expr) {
				return true
			}
		}
	}
	return false
}

// VisitOperators visits all the operators.
func VisitOperators(op abstract.PhysicalOperator, f func(tbl abstract.PhysicalOperator) (bool, error)) error {
	kontinue, err := f(op)
	if err != nil {
		return err
	}
	if !kontinue {
		return nil
	}

	switch op := op.(type) {
	case *Table, *Vindex:
		// leaf - no children to visit
	case *Route:
		err := VisitOperators(op.Source, f)
		if err != nil {
			return err
		}
	case *ApplyJoin:
		err := VisitOperators(op.LHS, f)
		if err != nil {
			return err
		}
		err = VisitOperators(op.RHS, f)
		if err != nil {
			return err
		}
	case *Filter:
		err := VisitOperators(op.Source, f)
		if err != nil {
			return err
		}
	case *CorrelatedSubQueryOp:
		err := VisitOperators(op.Outer, f)
		if err != nil {
			return err
		}
		err = VisitOperators(op.Inner, f)
		if err != nil {
			return err
		}
	case *SubQueryOp:
		err := VisitOperators(op.Outer, f)
		if err != nil {
			return err
		}
		err = VisitOperators(op.Inner, f)
		if err != nil {
			return err
		}
	case *Derived:
		err := VisitOperators(op.Source, f)
		if err != nil {
			return err
		}
	case *Union:
		for _, source := range op.Sources {
			err := VisitOperators(source, f)
			if err != nil {
				return err
			}
		}
	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown operator type while visiting - %T", op)
	}
	return nil
}

func optimizeUnionOp(ctx *context.PlanningContext, op *abstract.Concatenate) (abstract.PhysicalOperator, error) {
	var sources []abstract.PhysicalOperator

	for _, source := range op.Sources {
		qt, err := CreatePhysicalOperator(ctx, source)
		if err != nil {
			return nil, err
		}

		sources = append(sources, qt)
	}
	return &Union{Sources: sources, SelectStmts: op.SelectStmts, Distinct: op.Distinct}, nil
}

func gen4ValuesEqual(ctx *context.PlanningContext, a, b []sqlparser.Expr) bool {
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

func gen4ValEqual(ctx *context.PlanningContext, a, b sqlparser.Expr) bool {
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
