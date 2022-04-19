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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	joinTree interface {
		// tableID returns the table identifiers that are solved by this plan
		tableID() semantics.TableSet

		// cost is simply the number of routes in the joinTree
		cost() int

		// creates a copy of the joinTree that can be updated without changing the original
		clone() joinTree

		pushOutputColumns([]*sqlparser.ColName, *semantics.SemTable) []int
	}

	relation interface {
		tableID() semantics.TableSet
		tableNames() []string
	}

	leJoin struct {
		lhs, rhs relation
		pred     sqlparser.Expr
	}

	routeTable struct {
		qtable *abstract.QueryTable
		vtable *vindexes.Table
	}

	outerTable struct {
		right relation
		pred  sqlparser.Expr
	}

	routePlan struct {
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

		// vindex and vindexValues is set if a vindex will be used for this route.
		vindex           vindexes.Vindex
		vindexValues     []sqltypes.PlanValue
		vindexPredicates []sqlparser.Expr

		// here we store the possible vindexes we can use so that when we add predicates to the plan,
		// we can quickly check if the new predicates enables any new vindex options
		vindexPreds []*vindexPlusPredicates

		// columns needed to feed other plans`
		columns []*sqlparser.ColName
	}

	joinPlan struct {
		// columns needed to feed other plans
		columns []int

		// arguments that need to be copied from the LHS/RHS
		vars map[string]int

		// the children of this plan
		lhs, rhs joinTree

		outer bool
	}

	parenTables []relation
)

// type assertions
var _ joinTree = (*routePlan)(nil)
var _ joinTree = (*joinPlan)(nil)
var _ relation = (*routeTable)(nil)
var _ relation = (*leJoin)(nil)
var _ relation = (parenTables)(nil)

func (rp *routeTable) tableID() semantics.TableSet { return rp.qtable.TableID }

func (rp *leJoin) tableID() semantics.TableSet { return rp.lhs.tableID().Merge(rp.rhs.tableID()) }

func (rp *leJoin) tableNames() []string {
	return append(rp.lhs.tableNames(), rp.rhs.tableNames()...)
}

func (rp *routeTable) tableNames() []string {
	return []string{sqlparser.String(rp.qtable.Table.Name)}
}

func (p parenTables) tableNames() []string {
	var result []string
	for _, r := range p {
		result = append(result, r.tableNames()...)
	}
	return result
}

func (p parenTables) tableID() semantics.TableSet {
	res := semantics.TableSet(0)
	for _, r := range p {
		res = res.Merge(r.tableID())
	}
	return res
}

// visit will traverse the route tables, going inside parenTables and visiting all routeTables
func visitTables(r relation, f func(tbl *routeTable) error) error {
	switch r := r.(type) {
	case *routeTable:
		err := f(r)
		if err != nil {
			return err
		}
	case parenTables:
		for _, r := range r {
			err := visitTables(r, f)
			if err != nil {
				return err
			}
		}
		return nil
	case *leJoin:
		err := visitTables(r.lhs, f)
		if err != nil {
			return err
		}
		err = visitTables(r.rhs, f)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

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
func (rp *routePlan) tableID() semantics.TableSet {
	return rp.solved
}

func (rp *routePlan) hasOuterjoins() bool {
	return len(rp.leftJoins) > 0
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
	colVindex *vindexes.ColumnVindex
	values    []sqltypes.PlanValue

	// when we have the predicates found, we also know how to interact with this vindex
	foundVindex vindexes.Vindex
	opcode      engine.RouteOpcode
	predicates  []sqlparser.Expr
}

// addPredicate adds these predicates added to it. if the predicates can help,
// they will improve the routeOpCode
func (rp *routePlan) addPredicate(predicates ...sqlparser.Expr) error {
	if rp.canImprove() {
		newVindexFound, err := rp.searchForNewVindexes(predicates)
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
func (rp *routePlan) canImprove() bool {
	return rp.routeOpCode != engine.SelectNone
}

func (rp *routePlan) isImpossibleIN(node *sqlparser.ComparisonExpr) bool {
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

func (rp *routePlan) isImpossibleNotIN(node *sqlparser.ComparisonExpr) bool {
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

func (rp *routePlan) searchForNewVindexes(predicates []sqlparser.Expr) (bool, error) {
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
				found, err := rp.planEqualOp(node)
				if err != nil {
					return false, err
				}
				newVindexFound = newVindexFound || found
			case sqlparser.InOp:
				if rp.isImpossibleIN(node) {
					return false, nil
				}
				found, err := rp.planInOp(node)
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
				found, err := rp.planLikeOp(node)
				if err != nil {
					return false, err
				}
				newVindexFound = newVindexFound || found

			default:
				return false, semantics.Gen4NotSupportedF("%s", sqlparser.String(filter))
			}
		case *sqlparser.IsExpr:
			found, err := rp.planIsExpr(node)
			if err != nil {
				return false, err
			}
			newVindexFound = newVindexFound || found
		}
	}
	return newVindexFound, nil
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

func (rp *routePlan) planEqualOp(node *sqlparser.ComparisonExpr) (bool, error) {
	column, ok := node.Left.(*sqlparser.ColName)
	other := node.Right
	if !ok {
		column, ok = node.Right.(*sqlparser.ColName)
		if !ok {
			// either the LHS or RHS have to be a column to be useful for the vindex
			return false, nil
		}
		other = node.Left
	}
	val, err := makePlanValue(other)
	if err != nil || val == nil {
		return false, err
	}

	return rp.haveMatchingVindex(node, column, *val, equalOrEqualUnique, justTheVindex), err
}

func (rp *routePlan) planSimpleInOp(node *sqlparser.ComparisonExpr, left *sqlparser.ColName) (bool, error) {
	value, err := sqlparser.NewPlanValue(node.Right)
	if err != nil {
		// if we are unable to create a PlanValue, we can't use a vindex, but we don't have to fail
		if strings.Contains(err.Error(), "expression is too complex") {
			return false, nil
		}
		// something else went wrong, return the error
		return false, err
	}
	switch nodeR := node.Right.(type) {
	case sqlparser.ValTuple:
		if len(nodeR) == 1 && sqlparser.IsNull(nodeR[0]) {
			rp.routeOpCode = engine.SelectNone
			return false, nil
		}
	}
	opcode := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectIN }
	return rp.haveMatchingVindex(node, left, value, opcode, justTheVindex), err
}

func (rp *routePlan) planCompositeInOp(node *sqlparser.ComparisonExpr, left sqlparser.ValTuple) (bool, error) {
	right, rightIsValTuple := node.Right.(sqlparser.ValTuple)
	if !rightIsValTuple {
		return false, nil
	}
	return rp.planCompositeInOpRecursive(node, left, right, nil)
}

func (rp *routePlan) planCompositeInOpRecursive(node *sqlparser.ComparisonExpr, left, right sqlparser.ValTuple, coordinates []int) (bool, error) {
	foundVindex := false
	cindex := len(coordinates)
	coordinates = append(coordinates, 0)
	for i, expr := range left {
		coordinates[cindex] = i
		switch expr := expr.(type) {
		case sqlparser.ValTuple:
			ok, err := rp.planCompositeInOpRecursive(node, expr, right, coordinates)
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
			newPlanValues, err := makePlanValue(rightVals)
			if newPlanValues == nil || err != nil {
				return false, err
			}

			opcode := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectMultiEqual }
			newVindex := rp.haveMatchingVindex(node, expr, *newPlanValues, opcode, justTheVindex)
			foundVindex = newVindex || foundVindex
		}
	}
	return foundVindex, nil
}

func (rp *routePlan) planInOp(node *sqlparser.ComparisonExpr) (bool, error) {
	switch left := node.Left.(type) {
	case *sqlparser.ColName:
		return rp.planSimpleInOp(node, left)
	case sqlparser.ValTuple:
		return rp.planCompositeInOp(node, left)
	}
	return false, nil
}

func (rp *routePlan) planLikeOp(node *sqlparser.ComparisonExpr) (bool, error) {
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false, nil
	}

	val, err := makePlanValue(node.Right)
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

	return rp.haveMatchingVindex(node, column, *val, selectEqual, vdx), err
}

func (rp *routePlan) planIsExpr(node *sqlparser.IsExpr) (bool, error) {
	// we only handle IS NULL correct. IsExpr can contain other expressions as well
	if node.Right != sqlparser.IsNullOp {
		return false, nil
	}
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false, nil
	}
	val, err := makePlanValue(&sqlparser.NullVal{})
	if err != nil || val == nil {
		return false, err
	}

	return rp.haveMatchingVindex(node, column, *val, equalOrEqualUnique, justTheVindex), err
}

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

func (rp routePlan) hasVindex(column *sqlparser.ColName) bool {
	for _, v := range rp.vindexPreds {
		for _, col := range v.colVindex.Columns {
			if column.Name.Equal(col) {
				return true
			}
		}
	}
	return false
}

func (rp *routePlan) haveMatchingVindex(
	node sqlparser.Expr,
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
func (rp *routePlan) pickBestAvailableVindex() {
	for _, v := range rp.vindexPreds {
		if v.foundVindex == nil {
			continue
		}
		// Choose the minimum cost vindex from the ones which are covered
		if rp.vindex == nil || v.colVindex.Vindex.Cost() < rp.vindex.Cost() {
			rp.routeOpCode = v.opcode
			rp.vindex = v.foundVindex
			rp.vindexValues = v.values
			rp.vindexPredicates = v.predicates
		}
	}
}

// Predicates takes all known predicates for this route and ANDs them together
func (rp *routePlan) Predicates() sqlparser.Expr {
	predicates := rp.predicates
	_ = visitTables(rp.tables, func(tbl *routeTable) error {
		predicates = append(predicates, tbl.qtable.Predicates...)
		return nil
	})
	return sqlparser.AndExpressions(predicates...)
}

func (rp *routePlan) pushOutputColumns(col []*sqlparser.ColName, _ *semantics.SemTable) []int {
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
	return idxs
}

func (jp *joinPlan) tableID() semantics.TableSet {
	return jp.lhs.tableID() | jp.rhs.tableID()
}

func (jp *joinPlan) cost() int {
	return jp.lhs.cost() + jp.rhs.cost()
}

func (jp *joinPlan) clone() joinTree {
	result := &joinPlan{
		lhs:   jp.lhs.clone(),
		rhs:   jp.rhs.clone(),
		outer: jp.outer,
	}
	return result
}

func (jp *joinPlan) pushOutputColumns(columns []*sqlparser.ColName, semTable *semantics.SemTable) []int {
	var toTheLeft []bool
	var lhs, rhs []*sqlparser.ColName
	for _, col := range columns {
		col.Qualifier.Qualifier = sqlparser.NewTableIdent("")
		if semTable.Dependencies(col).IsSolvedBy(jp.lhs.tableID()) {
			lhs = append(lhs, col)
			toTheLeft = append(toTheLeft, true)
		} else {
			rhs = append(rhs, col)
			toTheLeft = append(toTheLeft, false)
		}
	}
	lhsOffset := jp.lhs.pushOutputColumns(lhs, semTable)
	rhsOffset := jp.rhs.pushOutputColumns(rhs, semTable)
	outputColumns := make([]int, len(toTheLeft))
	var l, r int
	for i, isLeft := range toTheLeft {
		outputColumns[i] = i
		if isLeft {
			jp.columns = append(jp.columns, -lhsOffset[l]-1)
			l++
		} else {
			jp.columns = append(jp.columns, rhsOffset[r]+1)
			r++
		}
	}
	return outputColumns
}
