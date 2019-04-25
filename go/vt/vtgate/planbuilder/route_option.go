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
	vindexMap map[*column]vindexes.Vindex

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

func newRouteOption(rb *route, vst *vindexes.Table, sub *tableSubstitution, vindexMap map[*column]vindexes.Vindex, eroute *engine.Route) *routeOption {
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
	ro.vschemaTable = nil
	ro.substitutions = append(ro.substitutions, rro.substitutions...)
	if isLeftJoin {
		return
	}
	// Add RHS vindexes only if it's not a left join.
	for c, v := range rro.vindexMap {
		if ro.vindexMap == nil {
			ro.vindexMap = make(map[*column]vindexes.Vindex)
		}
		ro.vindexMap[c] = v
	}
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
	ro.substitutions = append(ro.substitutions, subqueryOption.substitutions...)
}

func (ro *routeOption) UnionCanMerge(rro *routeOption) bool {
	return ro.canMerge(rro, func() bool { return false })
}

func (ro *routeOption) MergeUnion(rro *routeOption) {
	ro.vschemaTable = nil
	ro.substitutions = append(ro.substitutions, rro.substitutions...)
}

func (ro *routeOption) SubqueryToTable(rb *route, vindexMap map[*column]vindexes.Vindex) {
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
		// TODO(sougou): this can be changed to true, but we'll have
		// to merge against rro insteal of ro.
		return false
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
	filter = skipParenthesis(filter)
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
	case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA, engine.SelectReference:
		return
	}
	opcode, vindex, values := ro.computePlan(pb, filter)
	if opcode == engine.SelectScatter {
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
		case engine.SelectEqualUnique, engine.SelectEqual, engine.SelectIN:
			ro.updateRoute(opcode, vindex, values)
		}
	}
}

func (ro *routeOption) updateRoute(opcode engine.RouteOpcode, vindex vindexes.Vindex, condition sqlparser.Expr) {
	ro.eroute.Opcode = opcode
	ro.eroute.Vindex = vindex
	ro.condition = condition
}

// computePlan computes the plan for the specified filter.
func (ro *routeOption) computePlan(pb *primitiveBuilder, filter sqlparser.Expr) (opcode engine.RouteOpcode, vindex vindexes.Vindex, condition sqlparser.Expr) {
	switch node := filter.(type) {
	case *sqlparser.ComparisonExpr:
		switch node.Operator {
		case sqlparser.EqualStr:
			return ro.computeEqualPlan(pb, node)
		case sqlparser.InStr:
			return ro.computeINPlan(pb, node)
		}
	case *sqlparser.ParenExpr:
		return ro.computePlan(pb, node.Expr)
	}
	return engine.SelectScatter, nil, nil
}

// computeEqualPlan computes the plan for an equality constraint.
func (ro *routeOption) computeEqualPlan(pb *primitiveBuilder, comparison *sqlparser.ComparisonExpr) (opcode engine.RouteOpcode, vindex vindexes.Vindex, condition sqlparser.Expr) {
	left := comparison.Left
	right := comparison.Right
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

// computeINPlan computes the plan for an IN constraint.
func (ro *routeOption) computeINPlan(pb *primitiveBuilder, comparison *sqlparser.ComparisonExpr) (opcode engine.RouteOpcode, vindex vindexes.Vindex, condition sqlparser.Expr) {
	vindex = ro.FindVindex(pb, comparison.Left)
	if vindex == nil {
		return engine.SelectScatter, nil, nil
	}
	switch node := comparison.Right.(type) {
	case sqlparser.ValTuple:
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

func (ro *routeOption) FindVindex(pb *primitiveBuilder, expr sqlparser.Expr) vindexes.Vindex {
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
