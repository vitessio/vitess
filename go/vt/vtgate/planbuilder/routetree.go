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

package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/context"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	routeTree struct {
		routeOpCode engine.RouteOpcode
		solved      semantics.TableSet
		keyspace    *vindexes.Keyspace

		// tables contains inner tables that are solved by this plan.
		// the tables also contain any predicates that only depend on that particular table
		tables parenTables

		// predicates are the predicates evaluated by this plan
		predicates []sqlparser.Expr

		// leftJoins are the join conditions evaluated by this plan
		leftJoins []*outerTable

		// here we store the possible vindexes we can use so that when we add predicates to the plan,
		// we can quickly check if the new predicates enables any new vindex options
		vindexPreds []*vindexPlusPredicates

		// the best option available is stored here
		selected *vindexOption

		// columns needed to feed other plans
		columns []*sqlparser.ColName

		// The following two fields are used when routing information_schema queries
		SysTableTableSchema []evalengine.Expr
		SysTableTableName   map[string]evalengine.Expr
	}

	// cost is used to make it easy to compare the cost of two plans with each other
	cost struct {
		vindexCost int
		isUnique   bool
		opCode     engine.RouteOpcode
	}

	// vindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
	vindexPlusPredicates struct {
		tableID   semantics.TableSet
		colVindex *vindexes.ColumnVindex

		// during planning, we store the alternatives found for this route in this slice
		options []*vindexOption
	}

	// vindexOption stores the information needed to know if we have all the information needed to use a vindex
	vindexOption struct {
		ready  bool
		values []evalengine.Expr
		// columns that we have seen so far. Used only for multi-column vindexes so that we can track how many columns part of the vindex we have seen
		colsSeen    map[string]interface{}
		valueExprs  []sqlparser.Expr
		predicates  []sqlparser.Expr
		opcode      engine.RouteOpcode
		foundVindex vindexes.Vindex
		cost        cost
	}
)

var _ queryTree = (*routeTree)(nil)

// tables implements the queryTree interface
func (rp *routeTree) tableID() semantics.TableSet {
	return rp.solved
}

func (rp *routeTree) selectedVindex() vindexes.Vindex {
	if rp.selected == nil {
		return nil
	}
	return rp.selected.foundVindex
}
func (rp *routeTree) vindexExpressions() []sqlparser.Expr {
	if rp.selected == nil {
		return nil
	}
	return rp.selected.valueExprs
}

// clone returns a copy of the struct with copies of slices,
// so changing the contents of them will not be reflected in the original
func (rp *routeTree) clone() queryTree {
	result := *rp
	result.vindexPreds = make([]*vindexPlusPredicates, len(rp.vindexPreds))
	for i, pred := range rp.vindexPreds {
		// we do this to create a copy of the struct
		p := *pred
		result.vindexPreds[i] = &p
	}
	return &result
}

// cost implements the queryTree interface
func (rp *routeTree) cost() int {
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

// pushOutputColumns implements the queryTree interface
func (rp *routeTree) pushOutputColumns(col []*sqlparser.ColName, _ *semantics.SemTable) ([]int, error) {
	idxs := make([]int, len(col))
outer:
	for i, newCol := range col {
		for j, existingCol := range rp.columns {
			if sqlparser.EqualsExpr(newCol, existingCol) {
				idxs[i] = j
				continue outer
			}
		}
		idxs[i] = len(rp.columns)
		rp.columns = append(rp.columns, newCol)
	}
	return idxs, nil
}

func (rp *routeTree) pushPredicate(ctx *context.PlanningContext, expr sqlparser.Expr) error {
	return rp.addPredicate(ctx, expr)
}

func (rp *routeTree) removePredicate(ctx *context.PlanningContext, expr sqlparser.Expr) error {
	for i, predicate := range rp.predicates {
		if sqlparser.EqualsExpr(predicate, expr) {
			rp.predicates = append(rp.predicates[0:i], rp.predicates[i+1:]...)
			return rp.resetRoutingSelections(ctx)
		}
	}
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s not found in predicates", sqlparser.String(expr))
}

// addPredicate adds these predicates added to it. if the predicates can help,
// they will improve the routeOpCode
func (rp *routeTree) addPredicate(ctx *context.PlanningContext, predicates ...sqlparser.Expr) error {
	if rp.canImprove() {
		newVindexFound := rp.searchForNewVindexes(ctx, predicates)
		// if we didn't open up any new vindex options, no need to enter here
		if newVindexFound {
			rp.pickBestAvailableVindex()
		}
	}

	// any predicates that cover more than a single table need to be added here
	rp.predicates = append(rp.predicates, predicates...)

	return nil
}

// canImprove returns true if additional predicates could help improving this plan
func (rp *routeTree) canImprove() bool {
	return rp.routeOpCode != engine.SelectNone
}

func (rp *routeTree) searchForNewVindexes(ctx *context.PlanningContext, predicates []sqlparser.Expr) bool {
	newVindexFound := false
	for _, filter := range predicates {
		switch node := filter.(type) {
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
			found, exitEarly := rp.planComparison(ctx, cmp)
			if exitEarly {
				return false
			}
			newVindexFound = newVindexFound || found
		case *sqlparser.ComparisonExpr:
			found, exitEarly := rp.planComparison(ctx, node)
			if exitEarly {
				return false
			}
			newVindexFound = newVindexFound || found
		case *sqlparser.IsExpr:
			found := rp.planIsExpr(ctx, node)
			newVindexFound = newVindexFound || found
		}
	}
	return newVindexFound
}

func (rp *routeTree) planComparison(ctx *context.PlanningContext, node *sqlparser.ComparisonExpr) (bool, bool) {
	if sqlparser.IsNull(node.Left) || sqlparser.IsNull(node.Right) {
		// we are looking at ANDed predicates in the WHERE clause.
		// since we know that nothing returns true when compared to NULL,
		// so we can safely bail out here
		rp.routeOpCode = engine.SelectNone
		return false, true
	}

	switch node.Operator {
	case sqlparser.EqualOp:
		return rp.planEqualOp(ctx, node), false
	case sqlparser.InOp:
		if rp.isImpossibleIN(node) {
			return false, true
		}
		found := rp.planInOp(ctx, node)
		return found, false
	case sqlparser.NotInOp:
		// NOT IN is always a scatter, except when we can be sure it would return nothing
		if rp.isImpossibleNotIN(node) {
			return false, true
		}
	case sqlparser.LikeOp:
		found := rp.planLikeOp(ctx, node)
		return found, false
	}
	return false, false
}

func (rp *routeTree) isImpossibleIN(node *sqlparser.ComparisonExpr) bool {
	switch nodeR := node.Right.(type) {
	case sqlparser.ValTuple:
		// WHERE col IN (null)
		if len(nodeR) == 1 && sqlparser.IsNull(nodeR[0]) {
			rp.routeOpCode = engine.SelectNone
			return true
		}
	}
	return false
}

func (rp *routeTree) isImpossibleNotIN(node *sqlparser.ComparisonExpr) bool {
	switch node := node.Right.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			if sqlparser.IsNull(n) {
				rp.routeOpCode = engine.SelectNone
				return true
			}
		}
	}

	return false
}

func (rp *routeTree) planEqualOp(ctx *context.PlanningContext, node *sqlparser.ComparisonExpr) bool {
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
	val := rp.makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}

	return rp.haveMatchingVindex(ctx, node, vdValue, column, val, equalOrEqualUnique, justTheVindex)
}

func (rp *routeTree) planSimpleInOp(ctx *context.PlanningContext, node *sqlparser.ComparisonExpr, left *sqlparser.ColName) bool {
	vdValue := node.Right
	value := rp.makeEvalEngineExpr(ctx, vdValue)
	if value == nil {
		return false
	}
	switch nodeR := vdValue.(type) {
	case sqlparser.ValTuple:
		if len(nodeR) == 1 && sqlparser.IsNull(nodeR[0]) {
			rp.routeOpCode = engine.SelectNone
			return false
		}
	}
	opcode := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectIN }
	return rp.haveMatchingVindex(ctx, node, vdValue, left, value, opcode, justTheVindex)
}

func (rp *routeTree) planCompositeInOp(ctx *context.PlanningContext, node *sqlparser.ComparisonExpr, left sqlparser.ValTuple) bool {
	right, rightIsValTuple := node.Right.(sqlparser.ValTuple)
	if !rightIsValTuple {
		return false
	}
	return rp.planCompositeInOpRecursive(ctx, node, left, right, nil)
}

func (rp *routeTree) planCompositeInOpRecursive(ctx *context.PlanningContext, node *sqlparser.ComparisonExpr, left, right sqlparser.ValTuple, coordinates []int) bool {
	foundVindex := false
	cindex := len(coordinates)
	coordinates = append(coordinates, 0)
	for i, expr := range left {
		coordinates[cindex] = i
		switch expr := expr.(type) {
		case sqlparser.ValTuple:
			ok := rp.planCompositeInOpRecursive(ctx, node, expr, right, coordinates)
			return ok || foundVindex
		case *sqlparser.ColName:
			// check if left col is a vindex
			if !rp.hasVindex(expr) {
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
			newPlanValues := rp.makeEvalEngineExpr(ctx, rightVals)
			if newPlanValues == nil {
				return false
			}

			opcode := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectMultiEqual }
			newVindex := rp.haveMatchingVindex(ctx, node, rightVals, expr, newPlanValues, opcode, justTheVindex)
			foundVindex = newVindex || foundVindex
		}
	}
	return foundVindex
}

func (rp *routeTree) planInOp(ctx *context.PlanningContext, node *sqlparser.ComparisonExpr) bool {
	switch left := node.Left.(type) {
	case *sqlparser.ColName:
		return rp.planSimpleInOp(ctx, node, left)
	case sqlparser.ValTuple:
		return rp.planCompositeInOp(ctx, node, left)
	}
	return false
}

func (rp *routeTree) planLikeOp(ctx *context.PlanningContext, node *sqlparser.ComparisonExpr) bool {
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false
	}

	vdValue := node.Right
	val := rp.makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}

	selectEqual := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectEqual }
	vdx := func(vindex *vindexes.ColumnVindex) vindexes.Vindex {
		if prefixable, ok := vindex.Vindex.(vindexes.Prefixable); ok {
			return prefixable.PrefixVindex()
		}

		// if we can't use the vindex as a prefix-vindex, we can't use this vindex at all
		return nil
	}

	return rp.haveMatchingVindex(ctx, node, vdValue, column, val, selectEqual, vdx)
}

func (rp *routeTree) planIsExpr(ctx *context.PlanningContext, node *sqlparser.IsExpr) bool {
	// we only handle IS NULL correct. IsExpr can contain other expressions as well
	if node.Right != sqlparser.IsNullOp {
		return false
	}
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false
	}
	vdValue := &sqlparser.NullVal{}
	val := rp.makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}

	return rp.haveMatchingVindex(ctx, node, vdValue, column, val, equalOrEqualUnique, justTheVindex)
}

// makeEvalEngineExpr transforms the given sqlparser.Expr into a evalengine.Expr.
// If the given sqlparser.Expr is an argument and can be found in the rp.argToReplaceBySelect then the
// method will stops and return nil values.
// Otherwise, the method will try to apply makePlanValue for any equality the sqlparser.Expr n has.
// The first PlanValue that is successfully produced will be returned.
func (rp *routeTree) makeEvalEngineExpr(ctx *context.PlanningContext, n sqlparser.Expr) evalengine.Expr {
	if ctx.IsSubQueryToReplace(n) {
		return nil
	}

	for _, expr := range ctx.SemTable.GetExprAndEqualities(n) {
		if subq, isSubq := expr.(*sqlparser.Subquery); isSubq {
			extractedSubquery := ctx.SemTable.FindSubqueryReference(subq)
			if extractedSubquery == nil {
				continue
			}
			switch engine.PulloutOpcode(extractedSubquery.OpCode) {
			case engine.PulloutIn, engine.PulloutNotIn:
				expr = sqlparser.NewListArg(extractedSubquery.GetArgName())
			case engine.PulloutValue, engine.PulloutExists:
				expr = sqlparser.NewArgument(extractedSubquery.GetArgName())
			}
		}
		pv, _ := evalengine.Convert(expr, ctx.SemTable)
		if pv != nil {
			return pv
		}
	}

	return nil
}

func (rp *routeTree) hasVindex(column *sqlparser.ColName) bool {
	for _, v := range rp.vindexPreds {
		for _, col := range v.colVindex.Columns {
			if column.Name.Equal(col) {
				return true
			}
		}
	}
	return false
}

func (rp *routeTree) haveMatchingVindex(
	ctx *context.PlanningContext,
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value evalengine.Expr,
	opcode func(*vindexes.ColumnVindex) engine.RouteOpcode,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) bool {
	newVindexFound := false
	for _, v := range rp.vindexPreds {
		// check that the
		if !ctx.SemTable.DirectDeps(column).IsSolvedBy(v.tableID) {
			continue
		}
		switch v.colVindex.Vindex.(type) {
		case vindexes.SingleColumn:
			col := v.colVindex.Columns[0]
			if column.Name.Equal(col) {
				// single column vindex - just add the option
				routeOpcode := opcode(v.colVindex)
				vindex := vfunc(v.colVindex)
				if vindex == nil {
					continue
				}
				v.options = append(v.options, &vindexOption{
					values:      []evalengine.Expr{value},
					valueExprs:  []sqlparser.Expr{valueExpr},
					predicates:  []sqlparser.Expr{node},
					opcode:      routeOpcode,
					foundVindex: vindex,
					cost:        costFor(v.colVindex, routeOpcode),
					ready:       true,
				})
				newVindexFound = true
			}
		case vindexes.MultiColumn:
			colLoweredName := ""
			indexOfCol := -1
			for idx, col := range v.colVindex.Columns {
				if column.Name.Equal(col) {
					colLoweredName = column.Name.Lowered()
					indexOfCol = idx
					break
				}
			}
			if colLoweredName == "" {
				break
			}

			var newOption []*vindexOption
			for _, op := range v.options {
				if op.ready {
					continue
				}
				_, isPresent := op.colsSeen[colLoweredName]
				if isPresent {
					continue
				}
				option := copyOption(op)
				optionReady := option.updateWithNewColumn(colLoweredName, valueExpr, indexOfCol, value, node, v.colVindex, opcode)
				if optionReady {
					newVindexFound = true
				}
				newOption = append(newOption, option)
			}
			v.options = append(v.options, newOption...)

			// multi column vindex - just always add as new option for furince we do not have one already
			option := createOption(v.colVindex, vfunc)
			optionReady := option.updateWithNewColumn(colLoweredName, valueExpr, indexOfCol, value, node, v.colVindex, opcode)
			if optionReady {
				newVindexFound = true
			}
			v.options = append(v.options, option)
		}
	}
	return newVindexFound
}

func createOption(
	colVindex *vindexes.ColumnVindex,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) *vindexOption {
	values := make([]evalengine.Expr, len(colVindex.Columns))
	predicates := make([]sqlparser.Expr, len(colVindex.Columns))
	vindex := vfunc(colVindex)

	return &vindexOption{
		values:      values,
		predicates:  predicates,
		colsSeen:    map[string]interface{}{},
		foundVindex: vindex,
	}
}

func copyOption(orig *vindexOption) *vindexOption {
	colsSeen := make(map[string]interface{}, len(orig.colsSeen))
	valueExprs := make([]sqlparser.Expr, len(orig.valueExprs))
	values := make([]evalengine.Expr, len(orig.values))
	predicates := make([]sqlparser.Expr, len(orig.predicates))

	copy(values, orig.values)
	copy(valueExprs, orig.valueExprs)
	copy(predicates, orig.predicates)
	for k, v := range orig.colsSeen {
		colsSeen[k] = v
	}
	vo := &vindexOption{
		values:      values,
		colsSeen:    colsSeen,
		valueExprs:  valueExprs,
		predicates:  predicates,
		opcode:      orig.opcode,
		foundVindex: orig.foundVindex,
		cost:        orig.cost,
	}
	return vo
}

func (option *vindexOption) updateWithNewColumn(
	colLoweredName string,
	valueExpr sqlparser.Expr,
	indexOfCol int,
	value evalengine.Expr,
	node sqlparser.Expr,
	colVindex *vindexes.ColumnVindex,
	opcode func(*vindexes.ColumnVindex) engine.RouteOpcode,
) bool {
	option.colsSeen[colLoweredName] = true
	option.valueExprs = append(option.valueExprs, valueExpr)
	option.values[indexOfCol] = value
	option.predicates[indexOfCol] = node
	option.ready = len(option.colsSeen) == len(colVindex.Columns)
	routeOpcode := opcode(colVindex)
	if option.opcode < routeOpcode {
		option.opcode = routeOpcode
		option.cost = costFor(colVindex, routeOpcode)
	}
	return option.ready
}

// pickBestAvailableVindex goes over the available vindexes for this route and picks the best one available.
func (rp *routeTree) pickBestAvailableVindex() {
	for _, v := range rp.vindexPreds {
		option := v.bestOption()
		if option != nil && (rp.selected == nil || less(option.cost, rp.selected.cost)) {
			rp.selected = option
			rp.routeOpCode = option.opcode
		}
	}
}

func (vpp *vindexPlusPredicates) bestOption() *vindexOption {
	var best *vindexOption
	var keepOptions []*vindexOption
	for _, option := range vpp.options {
		if option.ready {
			if best == nil || less(option.cost, best.cost) {
				best = option
			}
		} else {
			keepOptions = append(keepOptions, option)
		}
	}
	if best != nil {
		keepOptions = append(keepOptions, best)
	}
	vpp.options = keepOptions
	return best
}

// Predicates takes all known predicates for this route and ANDs them together
func (rp *routeTree) Predicates() sqlparser.Expr {
	predicates := rp.predicates
	_ = visitRelations(rp.tables, func(tbl relation) (bool, error) {
		switch tbl := tbl.(type) {
		case *routeTable:
			predicates = append(predicates, tbl.qtable.Predicates...)
		case *derivedTable:
			// no need to copy the inner predicates to the outside
			return false, nil
		}

		return true, nil
	})
	return sqlparser.AndExpressions(predicates...)
}

// resetRoutingSelections resets all vindex selections and replans this routeTree
func (rp *routeTree) resetRoutingSelections(ctx *context.PlanningContext) error {

	vschemaTable := rp.tables[0].(*routeTable).vtable

	switch {
	case rp.routeOpCode == engine.SelectDBA:
		// don't change it if we know it is a system table
	case vschemaTable.Type == vindexes.TypeSequence:
		rp.routeOpCode = engine.SelectNext
	case vschemaTable.Type == vindexes.TypeReference:
		rp.routeOpCode = engine.SelectReference
	case !vschemaTable.Keyspace.Sharded:
		rp.routeOpCode = engine.SelectUnsharded
	case vschemaTable.Pinned != nil:

		// Pinned tables have their keyspace ids already assigned.
		// Use the Binary vindex, which is the identity function
		// for keyspace id.
		rp.routeOpCode = engine.SelectEqualUnique
	default:
		rp.routeOpCode = engine.SelectScatter
	}

	rp.selected = nil
	for i, vp := range rp.vindexPreds {
		rp.vindexPreds[i] = &vindexPlusPredicates{colVindex: vp.colVindex, tableID: vp.tableID}
	}

	predicates := rp.predicates
	rp.predicates = nil
	return rp.addPredicate(ctx, predicates...)
}

func (rp *routeTree) hasOuterjoins() bool {
	return len(rp.leftJoins) > 0
}

func justTheVindex(vindex *vindexes.ColumnVindex) vindexes.Vindex {
	return vindex.Vindex
}

func equalOrEqualUnique(vindex *vindexes.ColumnVindex) engine.RouteOpcode {
	if vindex.IsUnique() {
		return engine.SelectEqualUnique
	}

	return engine.SelectEqual
}

// costFor returns a cost struct to make route choices easier to compare
func costFor(foundVindex *vindexes.ColumnVindex, opcode engine.RouteOpcode) cost {
	switch opcode {
	// For these opcodes, we should not have a vindex, so we just return the opcode as the cost
	case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA, engine.SelectReference, engine.SelectNone, engine.SelectScatter:
		return cost{
			opCode: opcode,
		}
	}

	return cost{
		vindexCost: foundVindex.Cost(),
		isUnique:   foundVindex.IsUnique(),
		opCode:     opcode,
	}
}

// less compares two costs and returns true if the first cost is cheaper than the second
func less(c1, c2 cost) bool {
	switch {
	case c1.opCode != c2.opCode:
		return c1.opCode < c2.opCode
	case c1.isUnique == c2.isUnique:
		return c1.vindexCost <= c2.vindexCost
	default:
		return c1.isUnique
	}
}
