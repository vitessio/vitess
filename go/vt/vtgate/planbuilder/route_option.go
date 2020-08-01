/*
Copyright 2019 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// routeOption contains all the information for one route option.
// A route can have multiple options.
type routeOption struct {
	rb *route

	// vschemaTable is set only if a single table is referenced
	// in the from clause. It's used only for DMLs.
	vschemaTable *vindexes.Table

	// substitutions contain the list of table expressions that
	// have to be substituted in the route's query.
	substitutions []*tableSubstitution

	// vindexMap is a map of all vindexMap that can be used
	// for the routeOption.
	vindexMap map[*column]vindexes.SingleColumn

	// condition stores the AST condition that will be used
	// to resolve the ERoute Values field.
	condition sqlparser.Expr

	// eroute is the primitive being built.
	eroute *engine.Route
}

type tableSubstitution struct {
	newExpr, oldExpr *sqlparser.AliasedTableExpr
}

func newSimpleRouteOption(rb *route, eroute *engine.Route) *routeOption {
	return &routeOption{
		rb:     rb,
		eroute: eroute,
	}
}

func newRouteOption(rb *route, vst *vindexes.Table, sub *tableSubstitution, vindexMap map[*column]vindexes.SingleColumn, eroute *engine.Route) *routeOption {
	var subs []*tableSubstitution
	if sub != nil && sub.newExpr != nil {
		subs = []*tableSubstitution{sub}
	}
	return &routeOption{
		rb:            rb,
		vschemaTable:  vst,
		substitutions: subs,
		vindexMap:     vindexMap,
		eroute:        eroute,
	}
}

func (ro *routeOption) JoinCanMerge(pb *primitiveBuilder, rro *routeOption, ajoin *sqlparser.JoinTableExpr) bool {
	return ro.canMerge(rro, func() bool {
		if ajoin == nil {
			return false
		}
		for _, filter := range splitAndExpression(nil, ajoin.Condition.On) {
			if ro.canMergeOnFilter(pb, rro, filter) {
				return true
			}
		}
		return false
	})
}

func (ro *routeOption) MergeJoin(rro *routeOption, isLeftJoin bool) {
	ro.merge(rro)
	ro.vschemaTable = nil
	for c, v := range rro.vindexMap {
		if ro.vindexMap == nil {
			ro.vindexMap = make(map[*column]vindexes.SingleColumn)
		}
		ro.vindexMap[c] = v
	}
}

// merge merges two routeOptions. If the LHS (ro) is a SelectReference,
// then the RHS option values supersede LHS.
func (ro *routeOption) merge(rro *routeOption) {
	if ro.eroute.Opcode == engine.SelectReference {
		// Swap the values and then merge.
		*ro, *rro = *rro, *ro
	}
	ro.substitutions = append(ro.substitutions, rro.substitutions...)
}

func (ro *routeOption) SubqueryCanMerge(pb *primitiveBuilder, inner *routeOption) bool {
	return ro.canMerge(inner, func() bool {
		switch vals := inner.condition.(type) {
		case *sqlparser.ColName:
			if ro.FindVindex(pb, vals) == inner.eroute.Vindex {
				return true
			}
		}
		return false
	})
}

func (ro *routeOption) MergeSubquery(subqueryOption *routeOption) {
	ro.merge(subqueryOption)
}

func (ro *routeOption) UnionCanMerge(rro *routeOption) bool {
	return ro.canMerge(rro, func() bool { return false })
}

func (ro *routeOption) MergeUnion(rro *routeOption) {
	ro.merge(rro)
	ro.vschemaTable = nil
}

func (ro *routeOption) SubqueryToTable(rb *route, vindexMap map[*column]vindexes.SingleColumn) {
	ro.rb = rb
	ro.vschemaTable = nil
	ro.vindexMap = vindexMap
}

func (ro *routeOption) canMerge(rro *routeOption, customCheck func() bool) bool {
	if ro.eroute.Keyspace.Name != rro.eroute.Keyspace.Name {
		return false
	}
	if rro.eroute.Opcode == engine.SelectReference {
		// Any opcode can join with a reference table.
		return true
	}
	switch ro.eroute.Opcode {
	case engine.SelectUnsharded, engine.SelectDBA:
		return ro.eroute.Opcode == rro.eroute.Opcode
	case engine.SelectEqualUnique:
		// Check if they target the same shard.
		if rro.eroute.Opcode == engine.SelectEqualUnique && ro.eroute.Vindex == rro.eroute.Vindex && valEqual(ro.condition, rro.condition) {
			return true
		}
	case engine.SelectReference:
		return true
	case engine.SelectNext:
		return false
	}
	if customCheck != nil {
		return customCheck()
	}
	return false
}

// canMergeOnFilter returns true if the join constraint makes the routes
// mergeable by unique vindex. The constraint has to be an equality
// like a.id = b.id where both columns have the same unique vindex.
func (ro *routeOption) canMergeOnFilter(pb *primitiveBuilder, rro *routeOption, filter sqlparser.Expr) bool {
	comparison, ok := filter.(*sqlparser.ComparisonExpr)
	if !ok {
		return false
	}
	if comparison.Operator != sqlparser.EqualStr {
		return false
	}
	left := comparison.Left
	right := comparison.Right
	lVindex := ro.FindVindex(pb, left)
	if lVindex == nil {
		left, right = right, left
		lVindex = ro.FindVindex(pb, left)
	}
	if lVindex == nil || !lVindex.IsUnique() {
		return false
	}
	rVindex := rro.FindVindex(pb, right)
	if rVindex == nil {
		return false
	}
	return rVindex == lVindex
}

// UpdatePlan evaluates the primitive against the specified
// filter. If it's an improvement, the primitive is updated.
// We assume that the filter has already been pushed into
// the route.
func (ro *routeOption) UpdatePlan(pb *primitiveBuilder, filter sqlparser.Expr) {
	switch ro.eroute.Opcode {
	// For these opcodes, a new filter will not make any difference, so we can just exit early
	case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA, engine.SelectReference, engine.SelectNone:
		return
	}
	opcode, vindex, values := ro.computePlan(pb, filter)
	if opcode == engine.SelectScatter {
		return
	}
	// If we get SelectNone in next filters, override the previous route plan.
	if opcode == engine.SelectNone {
		ro.updateRoute(opcode, vindex, values)
		return
	}
	switch ro.eroute.Opcode {
	case engine.SelectEqualUnique:
		if opcode == engine.SelectEqualUnique && vindex.Cost() < ro.eroute.Vindex.Cost() {
			ro.updateRoute(opcode, vindex, values)
		}
	case engine.SelectEqual:
		switch opcode {
		case engine.SelectEqualUnique:
			ro.updateRoute(opcode, vindex, values)
		case engine.SelectEqual:
			if vindex.Cost() < ro.eroute.Vindex.Cost() {
				ro.updateRoute(opcode, vindex, values)
			}
		}
	case engine.SelectIN:
		switch opcode {
		case engine.SelectEqualUnique, engine.SelectEqual:
			ro.updateRoute(opcode, vindex, values)
		case engine.SelectIN:
			if vindex.Cost() < ro.eroute.Vindex.Cost() {
				ro.updateRoute(opcode, vindex, values)
			}
		}
	case engine.SelectScatter:
		switch opcode {
		case engine.SelectEqualUnique, engine.SelectEqual, engine.SelectIN, engine.SelectNone:
			ro.updateRoute(opcode, vindex, values)
		}
	}
}

func (ro *routeOption) updateRoute(opcode engine.RouteOpcode, vindex vindexes.SingleColumn, condition sqlparser.Expr) {
	ro.eroute.Opcode = opcode
	ro.eroute.Vindex = vindex
	ro.condition = condition
}

// computePlan computes the plan for the specified filter.
func (ro *routeOption) computePlan(pb *primitiveBuilder, filter sqlparser.Expr) (opcode engine.RouteOpcode, vindex vindexes.SingleColumn, condition sqlparser.Expr) {
	switch node := filter.(type) {
	case *sqlparser.ComparisonExpr:
		switch node.Operator {
		case sqlparser.EqualStr:
			return ro.computeEqualPlan(pb, node)
		case sqlparser.InStr:
			return ro.computeINPlan(pb, node)
		case sqlparser.NotInStr:
			return ro.computeNotInPlan(node.Right), nil, nil
		}
	case *sqlparser.IsExpr:
		return ro.computeISPlan(pb, node)
	}
	return engine.SelectScatter, nil, nil
}

// computeEqualPlan computes the plan for an equality constraint.
func (ro *routeOption) computeEqualPlan(pb *primitiveBuilder, comparison *sqlparser.ComparisonExpr) (opcode engine.RouteOpcode, vindex vindexes.SingleColumn, condition sqlparser.Expr) {
	left := comparison.Left
	right := comparison.Right

	if sqlparser.IsNull(right) {
		return engine.SelectNone, nil, nil
	}

	vindex = ro.FindVindex(pb, left)
	if vindex == nil {
		left, right = right, left
		vindex = ro.FindVindex(pb, left)
		if vindex == nil {
			return engine.SelectScatter, nil, nil
		}
	}
	if !ro.exprIsValue(right) {
		return engine.SelectScatter, nil, nil
	}
	if vindex.IsUnique() {
		return engine.SelectEqualUnique, vindex, right
	}
	return engine.SelectEqual, vindex, right
}

// computeEqualPlan computes the plan for an equality constraint.
func (ro *routeOption) computeISPlan(pb *primitiveBuilder, comparison *sqlparser.IsExpr) (opcode engine.RouteOpcode, vindex vindexes.SingleColumn, condition sqlparser.Expr) {
	// we only handle IS NULL correct. IsExpr can contain other expressions as well
	if comparison.Operator != sqlparser.IsNullStr {
		return engine.SelectScatter, nil, nil
	}

	vindex = ro.FindVindex(pb, comparison.Expr)
	// fallback to scatter gather if there is no vindex
	if vindex == nil {
		return engine.SelectScatter, nil, nil
	}
	if vindex.IsUnique() {
		return engine.SelectEqualUnique, vindex, &sqlparser.NullVal{}
	}
	return engine.SelectEqual, vindex, &sqlparser.NullVal{}
}

// computeINPlan computes the plan for an IN constraint.
func (ro *routeOption) computeINPlan(pb *primitiveBuilder, comparison *sqlparser.ComparisonExpr) (opcode engine.RouteOpcode, vindex vindexes.SingleColumn, condition sqlparser.Expr) {
	vindex = ro.FindVindex(pb, comparison.Left)
	if vindex == nil {
		return engine.SelectScatter, nil, nil
	}
	switch node := comparison.Right.(type) {
	case sqlparser.ValTuple:
		if len(node) == 1 && sqlparser.IsNull(node[0]) {
			return engine.SelectNone, nil, nil
		}

		for _, n := range node {
			if !ro.exprIsValue(n) {
				return engine.SelectScatter, nil, nil
			}
		}
		return engine.SelectIN, vindex, comparison
	case sqlparser.ListArg:
		return engine.SelectIN, vindex, comparison
	}
	return engine.SelectScatter, nil, nil
}

// computeNotInPlan looks for null values to produce a SelectNone if found
func (*routeOption) computeNotInPlan(right sqlparser.Expr) engine.RouteOpcode {
	switch node := right.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			if sqlparser.IsNull(n) {
				return engine.SelectNone
			}
		}
	}

	return engine.SelectScatter
}

var planCost = map[engine.RouteOpcode]int{
	engine.SelectUnsharded:   0,
	engine.SelectNext:        0,
	engine.SelectDBA:         0,
	engine.SelectReference:   0,
	engine.SelectEqualUnique: 1,
	engine.SelectIN:          2,
	engine.SelectEqual:       3,
	engine.SelectScatter:     4,
}

func (ro *routeOption) isBetterThan(other *routeOption) bool {
	ropc := planCost[ro.eroute.Opcode]
	otherpc := planCost[other.eroute.Opcode]
	if ropc < otherpc {
		return true
	}
	if ropc == otherpc {
		switch other.eroute.Opcode {
		case engine.SelectEqualUnique, engine.SelectIN, engine.SelectEqual:
			return ro.eroute.Vindex.Cost() < other.eroute.Vindex.Cost()
		}
	}
	return false
}

func (ro *routeOption) FindVindex(pb *primitiveBuilder, expr sqlparser.Expr) vindexes.SingleColumn {
	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		return nil
	}
	if col.Metadata == nil {
		// Find will set the Metadata.
		if _, _, err := pb.st.Find(col); err != nil {
			return nil
		}
	}
	c := col.Metadata.(*column)
	if c.Origin() != ro.rb {
		return nil
	}
	return ro.vindexMap[c]
}

// exprIsValue returns true if the expression can be treated as a value
// for the routeOption. External references are treated as value.
func (ro *routeOption) exprIsValue(expr sqlparser.Expr) bool {
	if node, ok := expr.(*sqlparser.ColName); ok {
		return node.Metadata.(*column).Origin() != ro.rb
	}
	return sqlparser.IsValue(expr)
}
