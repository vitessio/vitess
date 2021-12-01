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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type routeOp struct {
	source abstract.PhysicalOperator

	routeOpCode engine.RouteOpcode
	keyspace    *vindexes.Keyspace

	// here we store the possible vindexes we can use so that when we add predicates to the plan,
	// we can quickly check if the new predicates enables any new vindex options
	vindexPreds []*vindexPlusPredicates

	// the best option available is stored here
	selected *vindexOption

	// The following two fields are used when routing information_schema queries
	SysTableTableSchema []evalengine.Expr
	SysTableTableName   map[string]evalengine.Expr
}

var _ abstract.PhysicalOperator = (*routeOp)(nil)

// IPhysical implements the PhysicalOperator interface
func (*routeOp) IPhysical() {}

// TableID implements the Operator interface
func (r *routeOp) TableID() semantics.TableSet {
	return r.source.TableID()
}

// Cost implements the Operator interface
func (r *routeOp) Cost() int {
	return 1
}

// Clone implements the PhysicalOperator interface
func (r *routeOp) Clone() abstract.PhysicalOperator {
	cloneRoute := *r
	cloneRoute.source = r.source.Clone()
	cloneRoute.vindexPreds = make([]*vindexPlusPredicates, len(r.vindexPreds))
	for i, pred := range r.vindexPreds {
		// we do this to create a copy of the struct
		p := *pred
		cloneRoute.vindexPreds[i] = &p
	}
	return &cloneRoute
}

func (r *routeOp) updateRoutingLogic(ctx *planningContext, expr sqlparser.Expr) error {
	if r.canImprove() {
		newVindexFound, err := r.searchForNewVindexes(ctx, expr)
		if err != nil {
			return err
		}

		// if we didn't open up any new vindex options, no need to enter here
		if newVindexFound {
			r.pickBestAvailableVindex()
		}
	}
	return nil
}

func (r *routeOp) searchForNewVindexes(ctx *planningContext, predicate sqlparser.Expr) (bool, error) {
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

func (r *routeOp) planComparison(ctx *planningContext, node *sqlparser.ComparisonExpr) (bool, bool, error) {
	if sqlparser.IsNull(node.Left) || sqlparser.IsNull(node.Right) {
		// we are looking at ANDed predicates in the WHERE clause.
		// since we know that nothing returns true when compared to NULL,
		// so we can safely bail out here
		r.routeOpCode = engine.SelectNone
		return false, true, nil
	}

	switch node.Operator {
	case sqlparser.EqualOp:
		found := r.planEqualOp(ctx, node)
		return found, false, nil
	}
	return false, false, nil
}

func (r *routeOp) planEqualOp(ctx *planningContext, node *sqlparser.ComparisonExpr) bool {
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
func (r *routeOp) makeEvalEngineExpr(ctx *planningContext, n sqlparser.Expr) evalengine.Expr {
	for _, expr := range ctx.semTable.GetExprAndEqualities(n) {
		if subq, isSubq := expr.(*sqlparser.Subquery); isSubq {
			extractedSubquery := ctx.semTable.FindSubqueryReference(subq)
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
		pv, _ := evalengine.Convert(expr, &noColumnLookup{semTable: ctx.semTable})
		if pv != nil {
			return pv
		}
	}

	return nil
}

func (r *routeOp) hasVindex(column *sqlparser.ColName) bool {
	for _, v := range r.vindexPreds {
		for _, col := range v.colVindex.Columns {
			if column.Name.Equal(col) {
				return true
			}
		}
	}
	return false
}

func (r *routeOp) haveMatchingVindex(
	ctx *planningContext,
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value evalengine.Expr,
	opcode func(*vindexes.ColumnVindex) engine.RouteOpcode,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) bool {
	newVindexFound := false
	for _, v := range r.vindexPreds {
		// check that the
		if !ctx.semTable.DirectDeps(column).IsSolvedBy(v.tableID) {
			continue
		}
		// Ignore MultiColumn vindexes for finding matching Vindex.
		if _, isSingleCol := v.colVindex.Vindex.(vindexes.SingleColumn); !isSingleCol {
			continue
		}

		col := v.colVindex.Columns[0]
		if column.Name.Equal(col) {
			// single column vindex - just add the option
			routeOpcode := opcode(v.colVindex)
			vindex := vfunc(v.colVindex)
			v.options = append(v.options, &vindexOption{
				values:      []evalengine.Expr{value},
				valueExprs:  []sqlparser.Expr{valueExpr},
				predicates:  []sqlparser.Expr{node},
				opcode:      routeOpcode,
				foundVindex: vindex,
				cost:        costFor(vindex, routeOpcode),
				ready:       true,
			})
			newVindexFound = true
		}
	}
	return newVindexFound
}

// pickBestAvailableVindex goes over the available vindexes for this route and picks the best one available.
func (r *routeOp) pickBestAvailableVindex() {
	for _, v := range r.vindexPreds {
		option := v.bestOption()
		if option != nil && (r.selected == nil || less(option.cost, r.selected.cost)) {
			r.selected = option
			r.routeOpCode = option.opcode
		}
	}
}

// canImprove returns true if additional predicates could help improving this plan
func (r *routeOp) canImprove() bool {
	return r.routeOpCode != engine.SelectNone
}

// UnsolvedPredicates implements the Operator interface
func (r *routeOp) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return r.source.UnsolvedPredicates(semTable)
}

// CheckValid implements the Operator interface
func (r *routeOp) CheckValid() error {
	return r.source.CheckValid()
}

// Compact implements the Operator interface
func (r *routeOp) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return r, nil
}
