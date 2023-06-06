/*
Copyright 2023 The Vitess Authors.

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
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	popcode "vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// ShardedRouting is what we use for all tables that exist in a sharded keyspace
// It knows about available vindexes and can use them for routing when applicable
type ShardedRouting struct {
	// here we store the possible vindexes we can use so that when we add predicates to the plan,
	// we can quickly check if the new predicates enables any new vindex Options
	VindexPreds []*VindexPlusPredicates

	// the best option available is stored here
	Selected *VindexOption

	keyspace *vindexes.Keyspace

	RouteOpCode engine.Opcode

	// SeenPredicates contains all the predicates that have had a chance to influence routing.
	// If we need to replan routing, we'll use this list
	SeenPredicates []sqlparser.Expr
}

var _ Routing = (*ShardedRouting)(nil)

func newShardedRouting(vtable *vindexes.Table, id semantics.TableSet) Routing {
	routing := &ShardedRouting{
		RouteOpCode: engine.Scatter,
		keyspace:    vtable.Keyspace,
	}

	if vtable.Pinned != nil {
		// Pinned tables have their keyspace ids already assigned.
		// Use the Binary vindex, which is the identity function
		// for keyspace id.
		routing.RouteOpCode = engine.EqualUnique
		vindex, _ := vindexes.NewBinary("binary", nil)
		routing.Selected = &VindexOption{
			Ready:       true,
			Values:      []evalengine.Expr{evalengine.NewLiteralString(vtable.Pinned, collations.TypedCollation{})},
			ValueExprs:  nil,
			Predicates:  nil,
			OpCode:      engine.EqualUnique,
			FoundVindex: vindex,
			Cost: Cost{
				OpCode: engine.EqualUnique,
			},
		}

	}
	for _, columnVindex := range vtable.ColumnVindexes {
		routing.VindexPreds = append(routing.VindexPreds, &VindexPlusPredicates{ColVindex: columnVindex, TableID: id})
	}
	return routing
}

func (tr *ShardedRouting) isScatter() bool {
	return tr.RouteOpCode == engine.Scatter
}

// tryImprove rewrites the predicates for this query to see if we can produce a better plan.
// The rewrites are two:
//  1. first we turn the predicate a conjunctive normal form - an AND of ORs.
//     This can sometimes push a predicate to the top, so it's not hiding inside an OR
//  2. If that is not enough, an additional rewrite pass is performed where we try to
//     turn ORs into IN, which is easier for the planner to plan
func (tr *ShardedRouting) tryImprove(ctx *plancontext.PlanningContext, queryTable *QueryTable) (Routing, error) {
	oldPredicates := queryTable.Predicates
	queryTable.Predicates = nil
	tr.SeenPredicates = nil
	var routing Routing = tr
	var err error
	for _, pred := range oldPredicates {
		rewritten := sqlparser.RewritePredicate(pred)
		predicates := sqlparser.SplitAndExpression(nil, rewritten.(sqlparser.Expr))
		for _, predicate := range predicates {
			queryTable.Predicates = append(queryTable.Predicates, predicate)

			routing, err = UpdateRoutingLogic(ctx, predicate, routing)
			if err != nil {
				return nil, err
			}
		}
	}

	// If we have something other than a sharded routing with scatter, we are done
	if sr, ok := routing.(*ShardedRouting); !ok || !sr.isScatter() {
		return routing, nil
	}

	// if we _still_ haven't found a better route, we can run this additional rewrite on any ORs we have
	for _, expr := range queryTable.Predicates {
		or, ok := expr.(*sqlparser.OrExpr)
		if !ok {
			continue
		}
		for _, predicate := range sqlparser.ExtractINFromOR(or) {
			routing, err = UpdateRoutingLogic(ctx, predicate, routing)
			if err != nil {
				return nil, err
			}
		}
	}

	return routing, nil
}

func (tr *ShardedRouting) UpdateRoutingParams(_ *plancontext.PlanningContext, rp *engine.RoutingParameters) error {
	rp.Keyspace = tr.keyspace
	if tr.Selected != nil {
		rp.Vindex = tr.Selected.FoundVindex
		rp.Values = tr.Selected.Values
	}
	return nil
}

func (tr *ShardedRouting) Clone() Routing {
	var selected *VindexOption
	if tr.Selected != nil {
		t := *tr.Selected
		selected = &t
	}
	return &ShardedRouting{
		VindexPreds:    slices.Clone(tr.VindexPreds),
		Selected:       selected,
		keyspace:       tr.keyspace,
		RouteOpCode:    tr.RouteOpCode,
		SeenPredicates: slices.Clone(tr.SeenPredicates),
	}
}

func (tr *ShardedRouting) updateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	tr.SeenPredicates = append(tr.SeenPredicates, expr)

	newRouting, newVindexFound, err := tr.searchForNewVindexes(ctx, expr)
	if err != nil {
		return nil, err
	}

	if newRouting != nil {
		// we found something that we can route with something other than ShardedRouting
		return newRouting, nil
	}

	// if we didn't open up any new vindex Options, no need to enter here
	if newVindexFound {
		tr.PickBestAvailableVindex()
	}

	return tr, nil
}

func (tr *ShardedRouting) ResetRoutingLogic(ctx *plancontext.PlanningContext) (Routing, error) {
	tr.RouteOpCode = engine.Scatter
	tr.Selected = nil
	for i, vp := range tr.VindexPreds {
		tr.VindexPreds[i] = &VindexPlusPredicates{ColVindex: vp.ColVindex, TableID: vp.TableID}
	}

	var routing Routing = tr
	for _, predicate := range tr.SeenPredicates {
		var err error
		routing, err = UpdateRoutingLogic(ctx, predicate, routing)
		if err != nil {
			return nil, err
		}
	}
	return routing, nil
}

func (tr *ShardedRouting) searchForNewVindexes(ctx *plancontext.PlanningContext, predicate sqlparser.Expr) (Routing, bool, error) {
	newVindexFound := false
	switch node := predicate.(type) {
	case *sqlparser.ExtractedSubquery:
		originalCmp, ok := node.Original.(*sqlparser.ComparisonExpr)
		if !ok {
			break
		}

		// using the node.subquery which is the rewritten version of our subquery
		cmp := &sqlparser.ComparisonExpr{
			Left:     node.OtherSide,
			Right:    &sqlparser.Subquery{Select: node.Subquery.Select},
			Operator: originalCmp.Operator,
		}
		return tr.planComparison(ctx, cmp)
	case *sqlparser.ComparisonExpr:
		return tr.planComparison(ctx, node)

	case *sqlparser.IsExpr:
		found := tr.planIsExpr(ctx, node)
		newVindexFound = newVindexFound || found
	}

	return nil, newVindexFound, nil
}

func (tr *ShardedRouting) planComparison(ctx *plancontext.PlanningContext, cmp *sqlparser.ComparisonExpr) (routing Routing, foundNew bool, err error) {
	switch cmp.Operator {
	case sqlparser.EqualOp:
		found := tr.planEqualOp(ctx, cmp)
		return nil, found, nil
	case sqlparser.InOp:
		found := tr.planInOp(ctx, cmp)
		return nil, found, nil
	case sqlparser.LikeOp:
		found := tr.planLikeOp(ctx, cmp)
		return nil, found, nil

	}
	return nil, false, nil
}

func (tr *ShardedRouting) planIsExpr(ctx *plancontext.PlanningContext, node *sqlparser.IsExpr) bool {
	// we only handle IS NULL correct. IsExpr can contain other expressions as well
	if node.Right != sqlparser.IsNullOp {
		return false
	}
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false
	}
	vdValue := &sqlparser.NullVal{}
	val := makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}
	opcodeF := func(vindex *vindexes.ColumnVindex) engine.Opcode {
		if _, ok := vindex.Vindex.(vindexes.Lookup); ok {
			return engine.Scatter
		}
		return equalOrEqualUnique(vindex)
	}

	return tr.haveMatchingVindex(ctx, node, vdValue, column, val, opcodeF, justTheVindex)
}

func (tr *ShardedRouting) planInOp(ctx *plancontext.PlanningContext, cmp *sqlparser.ComparisonExpr) bool {
	switch left := cmp.Left.(type) {
	case *sqlparser.ColName:
		vdValue := cmp.Right

		valTuple, isTuple := vdValue.(sqlparser.ValTuple)
		if isTuple && len(valTuple) == 1 {
			return tr.planEqualOp(ctx, &sqlparser.ComparisonExpr{Left: left, Right: valTuple[0], Operator: sqlparser.EqualOp})
		}

		value := makeEvalEngineExpr(ctx, vdValue)
		if value == nil {
			return false
		}
		opcode := func(*vindexes.ColumnVindex) engine.Opcode { return engine.IN }
		return tr.haveMatchingVindex(ctx, cmp, vdValue, left, value, opcode, justTheVindex)
	case sqlparser.ValTuple:
		right, rightIsValTuple := cmp.Right.(sqlparser.ValTuple)
		if !rightIsValTuple {
			return false
		}
		return tr.planCompositeInOpRecursive(ctx, cmp, left, right, nil)
	}

	return false
}

func (tr *ShardedRouting) planLikeOp(ctx *plancontext.PlanningContext, node *sqlparser.ComparisonExpr) bool {
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false
	}

	vdValue := node.Right
	val := makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}
	selectEqual := func(*vindexes.ColumnVindex) engine.Opcode { return engine.Equal }
	vdx := func(vindex *vindexes.ColumnVindex) vindexes.Vindex {
		if prefixable, ok := vindex.Vindex.(vindexes.Prefixable); ok {
			return prefixable.PrefixVindex()
		}

		// if we can't use the vindex as a prefix-vindex, we can't use this vindex at all
		return nil
	}
	return tr.haveMatchingVindex(ctx, node, vdValue, column, val, selectEqual, vdx)
}

func (tr *ShardedRouting) Cost() int {
	switch tr.RouteOpCode {
	case engine.EqualUnique:
		return 1
	case engine.Equal:
		return 5
	case engine.IN:
		return 10
	case engine.MultiEqual:
		return 10
	case engine.Scatter:
		return 20
	default:
		panic("this switch should be exhaustive")
	}
}

func (tr *ShardedRouting) OpCode() engine.Opcode {
	return tr.RouteOpCode
}

func (tr *ShardedRouting) Keyspace() *vindexes.Keyspace {
	return tr.keyspace
}

// PickBestAvailableVindex goes over the available vindexes for this route and picks the best one available.
func (tr *ShardedRouting) PickBestAvailableVindex() {
	for _, v := range tr.VindexPreds {
		option := v.bestOption()
		if option != nil && (tr.Selected == nil || less(option.Cost, tr.Selected.Cost)) {
			tr.Selected = option
			tr.RouteOpCode = option.OpCode
		}
	}
}

func (tr *ShardedRouting) haveMatchingVindex(
	ctx *plancontext.PlanningContext,
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value evalengine.Expr,
	opcode func(*vindexes.ColumnVindex) engine.Opcode,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) bool {
	newVindexFound := false

	for _, v := range tr.VindexPreds {
		// Check if the dependency is solved by the table ID.
		if !ctx.SemTable.DirectDeps(column).IsSolvedBy(v.TableID) {
			continue
		}

		switch v.ColVindex.Vindex.(type) {
		case vindexes.SingleColumn:
			newVindexFound = tr.processSingleColumnVindex(node, valueExpr, column, value, opcode, vfunc, v, newVindexFound)

		case vindexes.MultiColumn:
			newVindexFound = tr.processMultiColumnVindex(node, valueExpr, column, value, opcode, vfunc, v, newVindexFound)
		}
	}

	return newVindexFound
}

func (tr *ShardedRouting) processSingleColumnVindex(
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value evalengine.Expr,
	opcode func(*vindexes.ColumnVindex) engine.Opcode,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
	vindexPlusPredicates *VindexPlusPredicates,
	newVindexFound bool,
) bool {
	col := vindexPlusPredicates.ColVindex.Columns[0]
	if !column.Name.Equal(col) {
		return newVindexFound
	}

	routeOpcode := opcode(vindexPlusPredicates.ColVindex)
	vindex := vfunc(vindexPlusPredicates.ColVindex)
	if vindex == nil || routeOpcode == engine.Scatter {
		return newVindexFound
	}

	vindexPlusPredicates.Options = append(vindexPlusPredicates.Options, &VindexOption{
		Values:      []evalengine.Expr{value},
		ValueExprs:  []sqlparser.Expr{valueExpr},
		Predicates:  []sqlparser.Expr{node},
		OpCode:      routeOpcode,
		FoundVindex: vindex,
		Cost:        costFor(vindexPlusPredicates.ColVindex, routeOpcode),
		Ready:       true,
	})
	return true
}

func (tr *ShardedRouting) processMultiColumnVindex(
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value evalengine.Expr,
	opcode func(*vindexes.ColumnVindex) engine.Opcode,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
	v *VindexPlusPredicates,
	newVindexFound bool,
) bool {
	colLoweredName, indexOfCol := tr.getLoweredNameAndIndex(v.ColVindex, column)

	if colLoweredName == "" {
		return newVindexFound
	}

	var newOption []*VindexOption
	for _, op := range v.Options {
		if op.Ready {
			continue
		}
		_, isPresent := op.ColsSeen[colLoweredName]
		if isPresent {
			continue
		}
		option := copyOption(op)
		optionReady := option.updateWithNewColumn(colLoweredName, valueExpr, indexOfCol, value, node, v.ColVindex, opcode)
		if optionReady {
			newVindexFound = true
		}
		newOption = append(newOption, option)
	}
	v.Options = append(v.Options, newOption...)

	// Multi-column vindex - just always add as new option
	option := createOption(v.ColVindex, vfunc)
	optionReady := option.updateWithNewColumn(colLoweredName, valueExpr, indexOfCol, value, node, v.ColVindex, opcode)
	if optionReady {
		newVindexFound = true
	}
	v.Options = append(v.Options, option)

	return newVindexFound
}

func (tr *ShardedRouting) getLoweredNameAndIndex(colVindex *vindexes.ColumnVindex, column *sqlparser.ColName) (string, int) {
	colLoweredName := ""
	indexOfCol := -1
	for idx, col := range colVindex.Columns {
		if column.Name.Equal(col) {
			colLoweredName = column.Name.Lowered()
			indexOfCol = idx
			break
		}
	}
	return colLoweredName, indexOfCol
}

func (tr *ShardedRouting) planEqualOp(ctx *plancontext.PlanningContext, node *sqlparser.ComparisonExpr) bool {
	column, ok := node.Left.(*sqlparser.ColName)
	other := node.Right
	vdValue := other
	if !ok {
		column, ok = node.Right.(*sqlparser.ColName)
		if !ok {
			// either the LHS or RHS have to be a column to be useful for the vindex
			return false
		}
		vdValue = node.Left
	}
	val := makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}

	return tr.haveMatchingVindex(ctx, node, vdValue, column, val, equalOrEqualUnique, justTheVindex)
}

func (tr *ShardedRouting) planCompositeInOpRecursive(
	ctx *plancontext.PlanningContext,
	cmp *sqlparser.ComparisonExpr,
	left, right sqlparser.ValTuple,
	coordinates []int,
) bool {
	foundVindex := false
	cindex := len(coordinates)
	coordinates = append(coordinates, 0)
	for i, expr := range left {
		coordinates[cindex] = i
		switch expr := expr.(type) {
		case sqlparser.ValTuple:
			ok := tr.planCompositeInOpRecursive(ctx, cmp, expr, right, coordinates)
			return ok || foundVindex
		case *sqlparser.ColName:
			// check if left col is a vindex
			if !tr.hasVindex(expr) {
				continue
			}

			rightVals := make(sqlparser.ValTuple, len(right))
			for j, currRight := range right {
				switch currRight := currRight.(type) {
				case sqlparser.ValTuple:
					val := tupleAccess(currRight, coordinates)
					if val == nil {
						return false
					}
					rightVals[j] = val
				default:
					return false
				}
			}
			newPlanValues := makeEvalEngineExpr(ctx, rightVals)
			if newPlanValues == nil {
				return false
			}

			opcode := func(*vindexes.ColumnVindex) engine.Opcode { return engine.MultiEqual }
			newVindex := tr.haveMatchingVindex(ctx, cmp, rightVals, expr, newPlanValues, opcode, justTheVindex)
			foundVindex = newVindex || foundVindex
		}
	}
	return foundVindex
}

func (tr *ShardedRouting) hasVindex(column *sqlparser.ColName) bool {
	for _, v := range tr.VindexPreds {
		for _, col := range v.ColVindex.Columns {
			if column.Name.Equal(col) {
				return true
			}
		}
	}
	return false
}

// Reset all vindex predicates on this route and re-build their options from
// the list of seen routing predicates.
func (tr *ShardedRouting) resetRoutingSelections(ctx *plancontext.PlanningContext) error {
	tr.RouteOpCode = engine.Scatter
	tr.Selected = nil
	for i, vp := range tr.VindexPreds {
		tr.VindexPreds[i] = &VindexPlusPredicates{ColVindex: vp.ColVindex, TableID: vp.TableID}
	}

	var routing Routing = tr
	for _, predicate := range tr.SeenPredicates {
		var err error
		routing, err = UpdateRoutingLogic(ctx, predicate, routing)
		if err != nil {
			return err
		}
	}
	if routing != tr {
		return vterrors.VT13001("uh-oh. we ended up with a different type of routing")
	}
	return nil
}

func (tr *ShardedRouting) SelectedVindex() vindexes.Vindex {
	if tr.Selected == nil {
		return nil
	}
	return tr.Selected.FoundVindex
}

func (tr *ShardedRouting) VindexExpressions() []sqlparser.Expr {
	if tr.Selected == nil {
		return nil
	}
	return tr.Selected.ValueExprs
}

func tryMergeShardedRouting(ctx *plancontext.PlanningContext, routeA *Route, routeB *Route, m merger, joinPredicates []sqlparser.Expr) (ops.Operator, error) {
	sameKeyspace := routeA.Routing.Keyspace() == routeB.Routing.Keyspace()
	tblA := routeA.Routing.(*ShardedRouting)
	tblB := routeB.Routing.(*ShardedRouting)

	switch tblA.RouteOpCode {
	case engine.EqualUnique:
		// If the two routes fully match, they can be merged together.
		if tblB.RouteOpCode == engine.EqualUnique {
			aVdx := tblA.SelectedVindex()
			bVdx := tblB.SelectedVindex()
			aExpr := tblA.VindexExpressions()
			bExpr := tblB.VindexExpressions()
			if aVdx == bVdx && gen4ValuesEqual(ctx, aExpr, bExpr) {
				return m.mergeTables(tblA, tblB, routeA, routeB)
			}
		}

		// If the two routes don't match, fall through to the next case and see if we
		// can merge via join predicates instead.
		fallthrough

	case engine.Scatter, engine.IN, engine.None:
		if len(joinPredicates) == 0 {
			// If we are doing two Scatters, we have to make sure that the
			// joins are on the correct vindex to allow them to be merged
			// no join predicates - no vindex
			return nil, nil
		}

		if !sameKeyspace {
			return nil, vterrors.VT12001("cross-shard correlated subquery")
		}

		canMerge := canMergeOnFilters(ctx, routeA, routeB, joinPredicates)
		if !canMerge {
			return nil, nil
		}
		return m.mergeTables(tblA, tblB, routeA, routeB)
	}
	return nil, nil
}

// makeEvalEngineExpr transforms the given sqlparser.Expr into an evalengine expression
func makeEvalEngineExpr(ctx *plancontext.PlanningContext, n sqlparser.Expr) evalengine.Expr {
	if ctx.IsSubQueryToReplace(n) {
		return nil
	}

	for _, expr := range ctx.SemTable.GetExprAndEqualities(n) {
		if subq, isSubq := expr.(*sqlparser.Subquery); isSubq {
			extractedSubquery := ctx.SemTable.FindSubqueryReference(subq)
			if extractedSubquery == nil {
				continue
			}
			switch popcode.PulloutOpcode(extractedSubquery.OpCode) {
			case popcode.PulloutIn, popcode.PulloutNotIn:
				expr = sqlparser.NewListArg(extractedSubquery.GetArgName())
			case popcode.PulloutValue, popcode.PulloutExists:
				expr = sqlparser.NewArgument(extractedSubquery.GetArgName())
			}
		}
		ee, _ := evalengine.Translate(expr, &evalengine.Config{Collation: ctx.SemTable.Collation})
		if ee != nil {
			return ee
		}
	}

	return nil
}
