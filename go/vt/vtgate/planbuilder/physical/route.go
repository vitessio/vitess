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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	Route struct {
		Source abstract.PhysicalOperator

		RouteOpCode engine.Opcode
		Keyspace    *vindexes.Keyspace

		// here we store the possible vindexes we can use so that when we add predicates to the plan,
		// we can quickly check if the new predicates enables any new vindex Options
		VindexPreds []*VindexPlusPredicates

		// the best option available is stored here
		Selected *VindexOption

		// The following two fields are used when routing information_schema queries
		SysTableTableSchema []evalengine.Expr
		SysTableTableName   map[string]evalengine.Expr

		// SeenPredicates contains all the predicates that have had a chance to influence routing.
		// If we need to replan routing, we'll use this list
		SeenPredicates []sqlparser.Expr
	}

	// VindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
	VindexPlusPredicates struct {
		TableID   semantics.TableSet
		ColVindex *vindexes.ColumnVindex

		// during planning, we store the alternatives found for this route in this slice
		Options []*VindexOption
	}

	// VindexOption stores the information needed to know if we have all the information needed to use a vindex
	VindexOption struct {
		Ready  bool
		Values []evalengine.Expr
		// columns that we have seen so far. Used only for multi-column vindexes so that we can track how many columns part of the vindex we have seen
		ColsSeen    map[string]interface{}
		ValueExprs  []sqlparser.Expr
		Predicates  []sqlparser.Expr
		OpCode      engine.Opcode
		FoundVindex vindexes.Vindex
		Cost        Cost
	}

	// Cost is used to make it easy to compare the Cost of two plans with each other
	Cost struct {
		VindexCost int
		IsUnique   bool
		OpCode     engine.Opcode
	}
)

var _ abstract.PhysicalOperator = (*Route)(nil)

// IPhysical implements the PhysicalOperator interface
func (*Route) IPhysical() {}

// TableID implements the Operator interface
func (r *Route) TableID() semantics.TableSet {
	return r.Source.TableID()
}

// Cost implements the Operator interface
func (r *Route) Cost() int {
	switch r.RouteOpCode {
	case // these op codes will never be compared with each other - they are assigned by a rule and not a comparison
		engine.DBA,
		engine.Next,
		engine.None,
		engine.Reference,
		engine.Unsharded:
		return 0
	// TODO revisit these costs when more of the gen4 planner is done
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
	}
	return 1
}

// Clone implements the PhysicalOperator interface
func (r *Route) Clone() abstract.PhysicalOperator {
	cloneRoute := *r
	cloneRoute.Source = r.Source.Clone()
	cloneRoute.VindexPreds = make([]*VindexPlusPredicates, len(r.VindexPreds))
	for i, pred := range r.VindexPreds {
		// we do this to create a copy of the struct
		p := *pred
		cloneRoute.VindexPreds[i] = &p
	}
	return &cloneRoute
}

func (r *Route) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error {
	r.SeenPredicates = append(r.SeenPredicates, expr)
	if r.canImprove() {
		newVindexFound, err := r.searchForNewVindexes(ctx, expr)
		if err != nil {
			return err
		}

		// if we didn't open up any new vindex Options, no need to enter here
		if newVindexFound {
			r.PickBestAvailableVindex()
		}
	}
	return nil
}

func (r *Route) searchForNewVindexes(ctx *plancontext.PlanningContext, predicate sqlparser.Expr) (bool, error) {
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
		found, exitEarly, err := r.planComparison(ctx, cmp)
		if err != nil || exitEarly {
			return false, err
		}
		newVindexFound = newVindexFound || found

	case *sqlparser.ComparisonExpr:
		found, exitEarly, err := r.planComparison(ctx, node)
		if err != nil || exitEarly {
			return false, err
		}
		newVindexFound = newVindexFound || found
	case *sqlparser.IsExpr:
		found := r.planIsExpr(ctx, node)
		newVindexFound = newVindexFound || found
	}
	return newVindexFound, nil
}

func (r *Route) planComparison(ctx *plancontext.PlanningContext, cmp *sqlparser.ComparisonExpr) (found bool, exitEarly bool, err error) {
	if sqlparser.IsNull(cmp.Left) || sqlparser.IsNull(cmp.Right) {
		// we are looking at ANDed predicates in the WHERE clause.
		// since we know that nothing returns true when compared to NULL,
		// so we can safely bail out here
		r.setSelectNoneOpcode()
		return false, true, nil
	}

	switch cmp.Operator {
	case sqlparser.EqualOp:
		found := r.planEqualOp(ctx, cmp)
		return found, false, nil
	case sqlparser.InOp:
		if r.isImpossibleIN(cmp) {
			return false, true, nil
		}
		found := r.planInOp(ctx, cmp)
		return found, false, nil
	case sqlparser.NotInOp:
		// NOT IN is always a scatter, except when we can be sure it would return nothing
		if r.isImpossibleNotIN(cmp) {
			return false, true, nil
		}
	case sqlparser.LikeOp:
		found := r.planLikeOp(ctx, cmp)
		return found, false, nil

	}
	return false, false, nil
}

func (r *Route) setSelectNoneOpcode() {
	r.RouteOpCode = engine.None
	// clear any chosen vindex as this query does not need to be sent down.
	r.Selected = nil
}

func (r *Route) planEqualOp(ctx *plancontext.PlanningContext, node *sqlparser.ComparisonExpr) bool {
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
	val := r.makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}

	return r.haveMatchingVindex(ctx, node, vdValue, column, val, equalOrEqualUnique, justTheVindex)
}

// makePlanValue transforms the given sqlparser.Expr into a sqltypes.PlanValue.
// If the given sqlparser.Expr is an argument and can be found in the r.argToReplaceBySelect then the
// method will stops and return nil values.
// Otherwise, the method will try to apply makePlanValue for any equality the sqlparser.Expr n has.
// The first PlanValue that is successfully produced will be returned.
func (r *Route) makeEvalEngineExpr(ctx *plancontext.PlanningContext, n sqlparser.Expr) evalengine.Expr {
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
		pv, _ := evalengine.Translate(expr, ctx.SemTable)
		if pv != nil {
			return pv
		}
	}

	return nil
}

func (r *Route) hasVindex(column *sqlparser.ColName) bool {
	for _, v := range r.VindexPreds {
		for _, col := range v.ColVindex.Columns {
			if column.Name.Equal(col) {
				return true
			}
		}
	}
	return false
}

func (r *Route) haveMatchingVindex(
	ctx *plancontext.PlanningContext,
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value evalengine.Expr,
	opcode func(*vindexes.ColumnVindex) engine.Opcode,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) bool {
	newVindexFound := false
	for _, v := range r.VindexPreds {
		// check that the
		if !ctx.SemTable.DirectDeps(column).IsSolvedBy(v.TableID) {
			continue
		}
		switch v.ColVindex.Vindex.(type) {
		case vindexes.SingleColumn:
			col := v.ColVindex.Columns[0]
			if column.Name.Equal(col) {
				// single column vindex - just add the option
				routeOpcode := opcode(v.ColVindex)
				vindex := vfunc(v.ColVindex)
				if vindex == nil {
					continue
				}
				v.Options = append(v.Options, &VindexOption{
					Values:      []evalengine.Expr{value},
					ValueExprs:  []sqlparser.Expr{valueExpr},
					Predicates:  []sqlparser.Expr{node},
					OpCode:      routeOpcode,
					FoundVindex: vindex,
					Cost:        costFor(v.ColVindex, routeOpcode),
					Ready:       true,
				})
				newVindexFound = true
			}
		case vindexes.MultiColumn:
			colLoweredName := ""
			indexOfCol := -1
			for idx, col := range v.ColVindex.Columns {
				if column.Name.Equal(col) {
					colLoweredName = column.Name.Lowered()
					indexOfCol = idx
					break
				}
			}
			if colLoweredName == "" {
				break
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

			// multi column vindex - just always add as new option for furince we do not have one already
			option := createOption(v.ColVindex, vfunc)
			optionReady := option.updateWithNewColumn(colLoweredName, valueExpr, indexOfCol, value, node, v.ColVindex, opcode)
			if optionReady {
				newVindexFound = true
			}
			v.Options = append(v.Options, option)
		}
	}
	return newVindexFound
}

func createOption(
	colVindex *vindexes.ColumnVindex,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) *VindexOption {
	values := make([]evalengine.Expr, len(colVindex.Columns))
	predicates := make([]sqlparser.Expr, len(colVindex.Columns))
	vindex := vfunc(colVindex)

	return &VindexOption{
		Values:      values,
		Predicates:  predicates,
		ColsSeen:    map[string]interface{}{},
		FoundVindex: vindex,
	}
}

func copyOption(orig *VindexOption) *VindexOption {
	colsSeen := make(map[string]interface{}, len(orig.ColsSeen))
	valueExprs := make([]sqlparser.Expr, len(orig.ValueExprs))
	values := make([]evalengine.Expr, len(orig.Values))
	predicates := make([]sqlparser.Expr, len(orig.Predicates))

	copy(values, orig.Values)
	copy(valueExprs, orig.ValueExprs)
	copy(predicates, orig.Predicates)
	for k, v := range orig.ColsSeen {
		colsSeen[k] = v
	}
	vo := &VindexOption{
		Values:      values,
		ColsSeen:    colsSeen,
		ValueExprs:  valueExprs,
		Predicates:  predicates,
		OpCode:      orig.OpCode,
		FoundVindex: orig.FoundVindex,
		Cost:        orig.Cost,
	}
	return vo
}

func (option *VindexOption) updateWithNewColumn(
	colLoweredName string,
	valueExpr sqlparser.Expr,
	indexOfCol int,
	value evalengine.Expr,
	node sqlparser.Expr,
	colVindex *vindexes.ColumnVindex,
	opcode func(*vindexes.ColumnVindex) engine.Opcode,
) bool {
	option.ColsSeen[colLoweredName] = true
	option.ValueExprs = append(option.ValueExprs, valueExpr)
	option.Values[indexOfCol] = value
	option.Predicates[indexOfCol] = node
	option.Ready = len(option.ColsSeen) == len(colVindex.Columns)
	routeOpcode := opcode(colVindex)
	if option.OpCode < routeOpcode {
		option.OpCode = routeOpcode
		option.Cost = costFor(colVindex, routeOpcode)
	}
	return option.Ready
}

// PickBestAvailableVindex goes over the available vindexes for this route and picks the best one available.
func (r *Route) PickBestAvailableVindex() {
	for _, v := range r.VindexPreds {
		option := v.bestOption()
		if option != nil && (r.Selected == nil || less(option.Cost, r.Selected.Cost)) {
			r.Selected = option
			r.RouteOpCode = option.OpCode
		}
	}
}

// canImprove returns true if additional predicates could help improving this plan
func (r *Route) canImprove() bool {
	return r.RouteOpCode != engine.None
}

// UnsolvedPredicates implements the Operator interface
func (r *Route) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return r.Source.UnsolvedPredicates(semTable)
}

// CheckValid implements the Operator interface
func (r *Route) CheckValid() error {
	return r.Source.CheckValid()
}

// Compact implements the Operator interface
func (r *Route) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return r, nil
}

func (r *Route) IsSingleShard() bool {
	switch r.RouteOpCode {
	case engine.Unsharded, engine.DBA, engine.Next, engine.EqualUnique, engine.Reference:
		return true
	}
	return false
}

func (r *Route) SelectedVindex() vindexes.Vindex {
	if r.Selected == nil {
		return nil
	}
	return r.Selected.FoundVindex
}

func (r *Route) VindexExpressions() []sqlparser.Expr {
	if r.Selected == nil {
		return nil
	}
	return r.Selected.ValueExprs
}

func (r *Route) isImpossibleIN(node *sqlparser.ComparisonExpr) bool {
	switch nodeR := node.Right.(type) {
	case sqlparser.ValTuple:
		// WHERE col IN (null)
		if len(nodeR) == 1 && sqlparser.IsNull(nodeR[0]) {
			r.setSelectNoneOpcode()
			return true
		}
	}
	return false
}

func (r *Route) planInOp(ctx *plancontext.PlanningContext, cmp *sqlparser.ComparisonExpr) bool {
	switch left := cmp.Left.(type) {
	case *sqlparser.ColName:
		vdValue := cmp.Right
		value := r.makeEvalEngineExpr(ctx, vdValue)
		if value == nil {
			return false
		}
		opcode := func(*vindexes.ColumnVindex) engine.Opcode { return engine.IN }
		return r.haveMatchingVindex(ctx, cmp, vdValue, left, value, opcode, justTheVindex)
	case sqlparser.ValTuple:
		right, rightIsValTuple := cmp.Right.(sqlparser.ValTuple)
		if !rightIsValTuple {
			return false
		}
		return r.planCompositeInOpRecursive(ctx, cmp, left, right, nil)
	}

	return false
}

func (r *Route) isImpossibleNotIN(node *sqlparser.ComparisonExpr) bool {
	switch node := node.Right.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			if sqlparser.IsNull(n) {
				r.setSelectNoneOpcode()
				return true
			}
		}
	}

	return false
}

func (r *Route) planLikeOp(ctx *plancontext.PlanningContext, node *sqlparser.ComparisonExpr) bool {
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false
	}

	vdValue := node.Right
	val := r.makeEvalEngineExpr(ctx, vdValue)
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
	return r.haveMatchingVindex(ctx, node, vdValue, column, val, selectEqual, vdx)

}

func (r *Route) planCompositeInOpRecursive(
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
			ok := r.planCompositeInOpRecursive(ctx, cmp, expr, right, coordinates)
			return ok || foundVindex
		case *sqlparser.ColName:
			// check if left col is a vindex
			if !r.hasVindex(expr) {
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
			newPlanValues := r.makeEvalEngineExpr(ctx, rightVals)
			if newPlanValues == nil {
				return false
			}

			opcode := func(*vindexes.ColumnVindex) engine.Opcode { return engine.MultiEqual }
			newVindex := r.haveMatchingVindex(ctx, cmp, rightVals, expr, newPlanValues, opcode, justTheVindex)
			foundVindex = newVindex || foundVindex
		}
	}
	return foundVindex
}

func (r *Route) resetRoutingSelections(ctx *plancontext.PlanningContext) error {
	switch r.RouteOpCode {
	case engine.DBA, engine.Next, engine.Reference, engine.Unsharded:
		// these we keep as is
	default:
		r.RouteOpCode = engine.Scatter
	}

	r.Selected = nil
	for i, vp := range r.VindexPreds {
		r.VindexPreds[i] = &VindexPlusPredicates{ColVindex: vp.ColVindex, TableID: vp.TableID}
	}

	for _, predicate := range r.SeenPredicates {
		err := r.UpdateRoutingLogic(ctx, predicate)
		if err != nil {
			return err
		}
	}
	return nil
}

func tupleAccess(expr sqlparser.Expr, coordinates []int) sqlparser.Expr {
	tuple, _ := expr.(sqlparser.ValTuple)
	for _, idx := range coordinates {
		if idx >= len(tuple) {
			return nil
		}
		expr = tuple[idx]
		tuple, _ = expr.(sqlparser.ValTuple)
	}
	return expr
}

func equalOrEqualUnique(vindex *vindexes.ColumnVindex) engine.Opcode {
	if vindex.IsUnique() {
		return engine.EqualUnique
	}

	return engine.Equal
}

func justTheVindex(vindex *vindexes.ColumnVindex) vindexes.Vindex {
	return vindex.Vindex
}

// costFor returns a cost struct to make route choices easier to compare
func costFor(foundVindex *vindexes.ColumnVindex, opcode engine.Opcode) Cost {
	switch opcode {
	// For these opcodes, we should not have a vindex, so we just return the opcode as the cost
	case engine.Unsharded, engine.Next, engine.DBA, engine.Reference, engine.None, engine.Scatter:
		return Cost{
			OpCode: opcode,
		}
	}

	return Cost{
		VindexCost: foundVindex.Cost(),
		IsUnique:   foundVindex.IsUnique(),
		OpCode:     opcode,
	}
}

// less compares two costs and returns true if the first cost is cheaper than the second
func less(c1, c2 Cost) bool {
	switch {
	case c1.OpCode != c2.OpCode:
		return c1.OpCode < c2.OpCode
	case c1.IsUnique == c2.IsUnique:
		return c1.VindexCost <= c2.VindexCost
	default:
		return c1.IsUnique
	}
}

func (vpp *VindexPlusPredicates) bestOption() *VindexOption {
	var best *VindexOption
	var keepOptions []*VindexOption
	for _, option := range vpp.Options {
		if option.Ready {
			if best == nil || less(option.Cost, best.Cost) {
				best = option
			}
		} else {
			keepOptions = append(keepOptions, option)
		}
	}
	if best != nil {
		keepOptions = append(keepOptions, best)
	}
	vpp.Options = keepOptions
	return best
}

func (r *Route) planIsExpr(ctx *plancontext.PlanningContext, node *sqlparser.IsExpr) bool {
	// we only handle IS NULL correct. IsExpr can contain other expressions as well
	if node.Right != sqlparser.IsNullOp {
		return false
	}
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false
	}
	vdValue := &sqlparser.NullVal{}
	val := r.makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}

	return r.haveMatchingVindex(ctx, node, vdValue, column, val, equalOrEqualUnique, justTheVindex)
}
