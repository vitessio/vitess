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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/context"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	Route struct {
		Source abstract.PhysicalOperator

		RouteOpCode engine.RouteOpcode
		Keyspace    *vindexes.Keyspace

		// here we store the possible vindexes we can use so that when we add predicates to the plan,
		// we can quickly check if the new predicates enables any new vindex Options
		VindexPreds []*VindexPlusPredicates

		// the best option available is stored here
		Selected *VindexOption

		// The following two fields are used when routing information_schema queries
		SysTableTableSchema []evalengine.Expr
		SysTableTableName   map[string]evalengine.Expr
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
		OpCode      engine.RouteOpcode
		FoundVindex vindexes.Vindex
		Cost        Cost
	}

	// Cost is used to make it easy to compare the Cost of two plans with each other
	Cost struct {
		VindexCost int
		IsUnique   bool
		OpCode     engine.RouteOpcode
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

func (r *Route) UpdateRoutingLogic(ctx *context.PlanningContext, expr sqlparser.Expr) error {
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

func (r *Route) searchForNewVindexes(ctx *context.PlanningContext, predicate sqlparser.Expr) (bool, error) {
	newVindexFound := false
	switch node := predicate.(type) {
	case *sqlparser.ComparisonExpr:
		found, exitEarly, err := r.planComparison(ctx, node)
		if err != nil || exitEarly {
			return false, err
		}
		newVindexFound = newVindexFound || found
	}
	return newVindexFound, nil
}

func (r *Route) planComparison(ctx *context.PlanningContext, cmp *sqlparser.ComparisonExpr) (found bool, exitEarly bool, err error) {
	if sqlparser.IsNull(cmp.Left) || sqlparser.IsNull(cmp.Right) {
		// we are looking at ANDed predicates in the WHERE clause.
		// since we know that nothing returns true when compared to NULL,
		// so we can safely bail out here
		r.RouteOpCode = engine.SelectNone
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
	}
	return false, false, nil
}

func (r *Route) planEqualOp(ctx *context.PlanningContext, node *sqlparser.ComparisonExpr) bool {
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
func (r *Route) makeEvalEngineExpr(ctx *context.PlanningContext, n sqlparser.Expr) evalengine.Expr {
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
	ctx *context.PlanningContext,
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value evalengine.Expr,
	opcode func(*vindexes.ColumnVindex) engine.RouteOpcode,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) bool {
	newVindexFound := false
	for _, v := range r.VindexPreds {
		// check that the
		if !ctx.SemTable.DirectDeps(column).IsSolvedBy(v.TableID) {
			continue
		}
		// Ignore MultiColumn vindexes for finding matching Vindex.
		if _, isSingleCol := v.ColVindex.Vindex.(vindexes.SingleColumn); !isSingleCol {
			continue
		}

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
	}
	return newVindexFound
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
	return r.RouteOpCode != engine.SelectNone
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
	case engine.SelectUnsharded, engine.SelectDBA, engine.SelectNext, engine.SelectEqualUnique, engine.SelectReference:
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
			r.RouteOpCode = engine.SelectNone
			return true
		}
	}
	return false
}

func (r *Route) planInOp(ctx *context.PlanningContext, cmp *sqlparser.ComparisonExpr) bool {
	switch left := cmp.Left.(type) {
	case *sqlparser.ColName:
		vdValue := cmp.Right
		value := r.makeEvalEngineExpr(ctx, vdValue)
		if value == nil {
			return false
		}
		opcode := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectIN }
		return r.haveMatchingVindex(ctx, cmp, vdValue, left, value, opcode, justTheVindex)
	}

	return false
}

func (r *Route) isImpossibleNotIN(node *sqlparser.ComparisonExpr) bool {
	switch node := node.Right.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			if sqlparser.IsNull(n) {
				r.RouteOpCode = engine.SelectNone
				return true
			}
		}
	}

	return false
}

func equalOrEqualUnique(vindex *vindexes.ColumnVindex) engine.RouteOpcode {
	if vindex.IsUnique() {
		return engine.SelectEqualUnique
	}

	return engine.SelectEqual
}

func justTheVindex(vindex *vindexes.ColumnVindex) vindexes.Vindex {
	return vindex.Vindex
}

// costFor returns a cost struct to make route choices easier to compare
func costFor(foundVindex *vindexes.ColumnVindex, opcode engine.RouteOpcode) Cost {
	switch opcode {
	// For these opcodes, we should not have a vindex, so we just return the opcode as the cost
	case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA, engine.SelectReference, engine.SelectNone, engine.SelectScatter:
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
