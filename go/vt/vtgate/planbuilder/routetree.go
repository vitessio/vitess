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
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type routeTree struct {
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

	// these fields are set if a vindex will be used for this route
	currentCost cost // currentCost tracks the cost of the chosen access method
	vindex      vindexes.Vindex

	// vindexValues contains the values that the vindex needs to be queried for
	vindexValues []sqltypes.PlanValue

	// vindexPredicates contains the comparisons that were the source of vindexValues
	vindexPredicates []sqlparser.Expr
	valueExprs       []sqlparser.Expr

	// here we store the possible vindexes we can use so that when we add predicates to the plan,
	// we can quickly check if the new predicates enables any new vindex options
	vindexPreds []*vindexPlusPredicates

	// columns needed to feed other plans`
	columns []*sqlparser.ColName

	// The following two fields are used when routing information_schema queries
	SysTableTableSchema []evalengine.Expr
	SysTableTableName   map[string]evalengine.Expr
}

var _ queryTree = (*routeTree)(nil)

// cost is used to make it easy to compare the cost of two plans with each other
type cost struct {
	vindexCost int
	isUnique   bool
	opCode     engine.RouteOpcode
}

// vindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
type vindexPlusPredicates struct {
	colVindex  *vindexes.ColumnVindex
	values     []sqltypes.PlanValue
	valueExprs []sqlparser.Expr

	// when we have the predicates found, we also know how to interact with this vindex
	foundVindex vindexes.Vindex
	opcode      engine.RouteOpcode
	predicates  []sqlparser.Expr
}

// tables implements the queryTree interface
func (rp *routeTree) tableID() semantics.TableSet {
	return rp.solved
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

// addPredicate adds these predicates added to it. if the predicates can help,
// they will improve the routeOpCode
func (rp *routeTree) addPredicate(ctx optimizeContext, predicates ...sqlparser.Expr) error {
	if rp.canImprove() {
		newVindexFound, err := rp.searchForNewVindexes(ctx, predicates)
		if err != nil {
			return err
		}

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

func (rp *routeTree) searchForNewVindexes(ctx optimizeContext, predicates []sqlparser.Expr) (bool, error) {
	newVindexFound := false
	for _, filter := range predicates {
		switch node := filter.(type) {
		case *sqlparser.ComparisonExpr:
			if sqlparser.IsNull(node.Left) || sqlparser.IsNull(node.Right) {
				// we are looking at ANDed predicates in the WHERE clause.
				// since we know that nothing returns true when compared to NULL,
				// so we can safely bail out here
				rp.routeOpCode = engine.SelectNone
				return false, nil
			}

			switch node.Operator {
			case sqlparser.EqualOp:
				found, err := rp.planEqualOp(ctx, node)
				if err != nil {
					return false, err
				}
				newVindexFound = newVindexFound || found
			case sqlparser.InOp:
				if rp.isImpossibleIN(node) {
					return false, nil
				}
				found, err := rp.planInOp(ctx, node)
				if err != nil {
					return false, err
				}
				newVindexFound = newVindexFound || found
			case sqlparser.NotInOp:
				// NOT IN is always a scatter, except when we can be sure it would return nothing
				if rp.isImpossibleNotIN(node) {
					return false, nil
				}
			case sqlparser.LikeOp:
				found, err := rp.planLikeOp(ctx, node)
				if err != nil {
					return false, err
				}
				newVindexFound = newVindexFound || found
			}
		case *sqlparser.IsExpr:
			found, err := rp.planIsExpr(ctx, node)
			if err != nil {
				return false, err
			}
			newVindexFound = newVindexFound || found
		}
	}
	return newVindexFound, nil
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

func (rp *routeTree) planEqualOp(ctx optimizeContext, node *sqlparser.ComparisonExpr) (bool, error) {
	column, ok := node.Left.(*sqlparser.ColName)
	other := node.Right
	vdValue := other
	if !ok {
		column, ok = node.Right.(*sqlparser.ColName)
		if !ok {
			// either the LHS or RHS have to be a column to be useful for the vindex
			return false, nil
		}
		vdValue = node.Left
	}
	val, err := rp.makePlanValue(ctx, vdValue)
	if err != nil || val == nil {
		return false, err
	}

	return rp.haveMatchingVindex(node, vdValue, column, *val, equalOrEqualUnique, justTheVindex), err
}

func (rp *routeTree) planSimpleInOp(ctx optimizeContext, node *sqlparser.ComparisonExpr, left *sqlparser.ColName) (bool, error) {
	vdValue := node.Right
	value, err := rp.makePlanValue(ctx, vdValue)
	if err != nil || value == nil {
		return false, err
	}
	switch nodeR := vdValue.(type) {
	case sqlparser.ValTuple:
		if len(nodeR) == 1 && sqlparser.IsNull(nodeR[0]) {
			rp.routeOpCode = engine.SelectNone
			return false, nil
		}
	}
	opcode := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectIN }
	return rp.haveMatchingVindex(node, vdValue, left, *value, opcode, justTheVindex), err
}

func (rp *routeTree) planCompositeInOp(ctx optimizeContext, node *sqlparser.ComparisonExpr, left sqlparser.ValTuple) (bool, error) {
	right, rightIsValTuple := node.Right.(sqlparser.ValTuple)
	if !rightIsValTuple {
		return false, nil
	}
	return rp.planCompositeInOpRecursive(ctx, node, left, right, nil)
}

func (rp *routeTree) planCompositeInOpRecursive(ctx optimizeContext, node *sqlparser.ComparisonExpr, left, right sqlparser.ValTuple, coordinates []int) (bool, error) {
	foundVindex := false
	cindex := len(coordinates)
	coordinates = append(coordinates, 0)
	for i, expr := range left {
		coordinates[cindex] = i
		switch expr := expr.(type) {
		case sqlparser.ValTuple:
			ok, err := rp.planCompositeInOpRecursive(ctx, node, expr, right, coordinates)
			if err != nil {
				return false, err
			}
			return ok || foundVindex, nil
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
						return false, nil
					}
					rightVals[j] = val
				default:
					return false, nil
				}
			}
			newPlanValues, err := rp.makePlanValue(ctx, rightVals)
			if newPlanValues == nil || err != nil {
				return false, err
			}

			opcode := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectMultiEqual }
			newVindex := rp.haveMatchingVindex(node, rightVals, expr, *newPlanValues, opcode, justTheVindex)
			foundVindex = newVindex || foundVindex
		}
	}
	return foundVindex, nil
}

func (rp *routeTree) planInOp(ctx optimizeContext, node *sqlparser.ComparisonExpr) (bool, error) {
	switch left := node.Left.(type) {
	case *sqlparser.ColName:
		return rp.planSimpleInOp(ctx, node, left)
	case sqlparser.ValTuple:
		return rp.planCompositeInOp(ctx, node, left)
	}
	return false, nil
}

func (rp *routeTree) planLikeOp(ctx optimizeContext, node *sqlparser.ComparisonExpr) (bool, error) {
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false, nil
	}

	vdValue := node.Right
	val, err := rp.makePlanValue(ctx, vdValue)
	if err != nil || val == nil {
		return false, err
	}

	selectEqual := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectEqual }
	vdx := func(vindex *vindexes.ColumnVindex) vindexes.Vindex {
		if prefixable, ok := vindex.Vindex.(vindexes.Prefixable); ok {
			return prefixable.PrefixVindex()
		}

		// if we can't use the vindex as a prefix-vindex, we can't use this vindex at all
		return nil
	}

	return rp.haveMatchingVindex(node, vdValue, column, *val, selectEqual, vdx), err
}

func (rp *routeTree) planIsExpr(ctx optimizeContext, node *sqlparser.IsExpr) (bool, error) {
	// we only handle IS NULL correct. IsExpr can contain other expressions as well
	if node.Right != sqlparser.IsNullOp {
		return false, nil
	}
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false, nil
	}
	vdValue := &sqlparser.NullVal{}
	val, err := rp.makePlanValue(ctx, vdValue)
	if err != nil || val == nil {
		return false, err
	}

	return rp.haveMatchingVindex(node, vdValue, column, *val, equalOrEqualUnique, justTheVindex), err
}

// makePlanValue transforms the given sqlparser.Expr into a sqltypes.PlanValue.
// If the given sqlparser.Expr is an argument and can be found in the rp.sqToReplace then the
// method will stops and return nil values.
// Otherwise, the method will try to apply makePlanValue for any equality the sqlparser.Expr n has.
// The first PlanValue that is successfully produced will be returned.
func (rp *routeTree) makePlanValue(ctx optimizeContext, n sqlparser.Expr) (*sqltypes.PlanValue, error) {
	if ctx.isSubQueryToReplace(argumentName(n)) {
		return nil, nil
	}

	for _, expr := range ctx.semTable.GetExprAndEqualities(n) {
		pv, err := makePlanValue(expr)
		if err != nil {
			return nil, err
		}
		if pv != nil {
			return pv, nil
		}
	}

	return nil, nil
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
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value sqltypes.PlanValue,
	opcode func(*vindexes.ColumnVindex) engine.RouteOpcode,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) bool {
	newVindexFound := false
	for _, v := range rp.vindexPreds {
		if v.foundVindex != nil {
			continue
		}
		for _, col := range v.colVindex.Columns {
			// If the column for the predicate matches any column in the vindex add it to the list
			if column.Name.Equal(col) {
				v.values = append(v.values, value)
				v.valueExprs = append(v.valueExprs, valueExpr)
				v.predicates = append(v.predicates, node)
				// Vindex is covered if all the columns in the vindex have a associated predicate
				covered := len(v.values) == len(v.colVindex.Columns)
				if covered {
					v.opcode = opcode(v.colVindex)
					v.foundVindex = vfunc(v.colVindex)
				}
				newVindexFound = newVindexFound || covered
			}
		}
	}
	return newVindexFound
}

// pickBestAvailableVindex goes over the available vindexes for this route and picks the best one available.
func (rp *routeTree) pickBestAvailableVindex() {
	for _, v := range rp.vindexPreds {
		if v.foundVindex == nil {
			continue
		}
		// Choose the minimum cost vindex from the ones which are covered
		thisCost := costFor(v.foundVindex, v.opcode)
		if rp.vindex == nil || less(thisCost, rp.currentCost) {
			rp.currentCost = thisCost
			rp.routeOpCode = v.opcode
			rp.vindex = v.foundVindex
			rp.vindexValues = v.values
			rp.vindexPredicates = v.predicates
			rp.valueExprs = v.valueExprs
		}
	}
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
func (rp *routeTree) resetRoutingSelections(ctx optimizeContext) error {

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

	rp.vindex = nil
	rp.vindexValues = nil
	rp.valueExprs = nil
	rp.vindexPredicates = nil

	for i, vp := range rp.vindexPreds {
		rp.vindexPreds[i] = &vindexPlusPredicates{colVindex: vp.colVindex}
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
	if vindex.Vindex.IsUnique() {
		return engine.SelectEqualUnique
	}

	return engine.SelectEqual
}

// makePlanValue transforms a sqlparser.Expr into a sqltypes.PlanValue.
// If the expression is too complex e.g: not an argument/literal/tuple/null/unary, then
// the method will not fail, instead it will exit with nil values.
func makePlanValue(n sqlparser.Expr) (*sqltypes.PlanValue, error) {
	value, err := sqlparser.NewPlanValue(n)
	if err != nil {
		// if we are unable to create a PlanValue, we can't use a vindex, but we don't have to fail
		if strings.Contains(err.Error(), "expression is too complex") {
			return nil, nil
		}
		// something else went wrong, return the error
		return nil, err
	}
	return &value, nil
}

func argumentName(node sqlparser.SQLNode) string {
	var argName string
	switch node := node.(type) {
	case sqlparser.ListArg:
		argName = string(node)
	case sqlparser.Argument:
		argName = string(node)
	}
	return argName
}

// costFor returns a cost struct to make route choices easier to compare
func costFor(foundVindex vindexes.Vindex, opcode engine.RouteOpcode) cost {
	switch opcode {
	// For these opcodes, we should not have a vindex, so we just return the opcode as the cost
	case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA, engine.SelectReference, engine.SelectNone, engine.SelectScatter:
		return cost{
			opCode: opcode,
		}
	}

	// if we have a multiplier that is non-zero, we should have a vindex
	if foundVindex == nil {
		panic("expected a vindex")
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
