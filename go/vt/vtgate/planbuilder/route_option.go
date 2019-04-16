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

type routeOption struct {
	rb *route

	// vschemaTable is set for DMLs, only if a single table
	// is referenced in the from clause.
	vschemaTable *vindexes.Table

	// substitutions contains the list of table expressions that
	// have to be substituted in the route's query.
	substitutions []*tableSubstitution

	// vindexes is a map of all vindexes that can be used
	// for the routeOption.
	vindexes map[*column]vindexes.Vindex

	// condition stores the AST condition that will be used
	// to resolve the ERoute Values field.
	condition sqlparser.Expr

	// ERoute is the primitive being built.
	ERoute *engine.Route
}

type tableSubstitution struct {
	newExpr, oldExpr *sqlparser.AliasedTableExpr
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

func (ro *routeOption) SubqueryCanMerge(pb *primitiveBuilder, inner *routeOption) bool {
	return ro.canMerge(inner, func() bool {
		switch vals := inner.condition.(type) {
		case *sqlparser.ColName:
			if ro.FindVindex(pb, vals) == inner.ERoute.Vindex {
				return true
			}
		}
		return false
	})
}

func (ro *routeOption) UnionCanMerge(rro *routeOption) bool {
	return ro.canMerge(rro, func() bool { return false })
}

func (ro *routeOption) canMerge(rro *routeOption, customCheck func() bool) bool {
	if ro.ERoute.Keyspace.Name != rro.ERoute.Keyspace.Name {
		return false
	}
	switch ro.ERoute.Opcode {
	case engine.SelectUnsharded:
		if rro.ERoute.Opcode == engine.SelectUnsharded {
			return true
		}
		return false
	case engine.SelectDBA:
		if rro.ERoute.Opcode == engine.SelectDBA {
			return true
		}
		return false
	case engine.SelectEqualUnique:
		// Check if they target the same shard.
		if rro.ERoute.Opcode == engine.SelectEqualUnique && ro.ERoute.Vindex == rro.ERoute.Vindex && valEqual(ro.condition, rro.condition) {
			return true
		}
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
	switch ro.ERoute.Opcode {
	case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA:
		return
	}
	opcode, vindex, values := ro.computePlan(pb, filter)
	if opcode == engine.SelectScatter {
		return
	}
	switch ro.ERoute.Opcode {
	case engine.SelectEqualUnique:
		if opcode == engine.SelectEqualUnique && vindex.Cost() < ro.ERoute.Vindex.Cost() {
			ro.updateRoute(opcode, vindex, values)
		}
	case engine.SelectEqual:
		switch opcode {
		case engine.SelectEqualUnique:
			ro.updateRoute(opcode, vindex, values)
		case engine.SelectEqual:
			if vindex.Cost() < ro.ERoute.Vindex.Cost() {
				ro.updateRoute(opcode, vindex, values)
			}
		}
	case engine.SelectIN:
		switch opcode {
		case engine.SelectEqualUnique, engine.SelectEqual:
			ro.updateRoute(opcode, vindex, values)
		case engine.SelectIN:
			if vindex.Cost() < ro.ERoute.Vindex.Cost() {
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
	ro.ERoute.Opcode = opcode
	ro.ERoute.Vindex = vindex
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

func (ro *routeOption) isBetterThan(other *routeOption) bool {
	switch other.ERoute.Opcode {
	case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA:
		return false
	case engine.SelectEqualUnique:
		switch ro.ERoute.Opcode {
		case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA:
			return true
		case engine.SelectEqualUnique:
			if ro.ERoute.Vindex.Cost() < other.ERoute.Vindex.Cost() {
				return true
			}
		}
		return false
	case engine.SelectIN:
		switch ro.ERoute.Opcode {
		case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA, engine.SelectEqualUnique:
			return true
		case engine.SelectIN:
			if ro.ERoute.Vindex.Cost() < other.ERoute.Vindex.Cost() {
				return true
			}
		}
		return false
	case engine.SelectEqual:
		switch ro.ERoute.Opcode {
		case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA, engine.SelectEqualUnique, engine.SelectIN:
			return true
		case engine.SelectEqual:
			if ro.ERoute.Vindex.Cost() < other.ERoute.Vindex.Cost() {
				return true
			}
		}
		return false
	case engine.SelectScatter:
		return false
	}
	// Unreachable.
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
	return ro.vindexes[c]
}

// exprIsValue returns true if the expression can be treated as a value
// for the routeOption. External references are treated as value.
func (ro *routeOption) exprIsValue(expr sqlparser.Expr) bool {
	if node, ok := expr.(*sqlparser.ColName); ok {
		return node.Metadata.(*column).Origin() != ro.rb
	}
	return sqlparser.IsValue(expr)
}
