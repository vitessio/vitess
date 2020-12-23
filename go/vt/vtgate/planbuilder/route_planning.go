/*
Copyright 2020 The Vitess Authors.

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

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func newBuildSelectPlan(sel *sqlparser.Select, vschema ContextVSchema) (engine.Primitive, error) {
	semTable, err := semantics.Analyse(sel, nil) // TODO no nil no
	if err != nil {
		return nil, err
	}

	qgraph, err := createQGFromSelect(sel, semTable)
	if err != nil {
		return nil, err
	}

	tree, err := solve(qgraph, semTable, vschema)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(tree)
	if err != nil {
		return nil, err
	}

	if err := planProjections(sel, plan, semTable); err != nil {
		return nil, err
	}

	if err := plan.Wireup2(semTable); err != nil {
		return nil, err
	}
	return plan.Primitive(), nil
}

func planProjections(sel *sqlparser.Select, plan logicalPlan, semTable *semantics.SemTable) error {
	rb, ok := plan.(*route)
	if ok {
		rb.Select = sel
	} else {
		var projections []*sqlparser.AliasedExpr

		// TODO real horizon planning to be done
		for _, expr := range sel.SelectExprs {
			switch e := expr.(type) {
			case *sqlparser.AliasedExpr:
				projections = append(projections, e)
			default:
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not yet supported %T", e)
			}
		}

		if _, err := pushProjection(projections, plan, semTable); err != nil {
			return err
		}
	}
	return nil
}

type (
	joinTree interface {
		solves() semantics.TableSet
		cost() int
	}
	routePlan struct {
		routeOpCode     engine.RouteOpcode
		solved          semantics.TableSet
		tables          []*queryTable
		extraPredicates []sqlparser.Expr
		keyspace        *vindexes.Keyspace

		// vindex and conditions is set if a vindex will be used for this route.
		vindex     vindexes.Vindex
		conditions []sqlparser.Expr

		// this state keeps track of which vindexes are available and
		// whether we have seen enough predicates to satisfy the vindex
		vindexPlusPredicates []*vindexPlusPredicates
		vtable               *vindexes.Table
	}
	joinPlan struct {
		predicates []sqlparser.Expr

		lhs, rhs joinTree
	}
)

func (rp *routePlan) solves() semantics.TableSet {
	return rp.solved
}
func (*routePlan) cost() int {
	return 1
}

// vindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
type vindexPlusPredicates struct {
	vindex     *vindexes.ColumnVindex
	covered    bool
	predicates []sqlparser.Expr
}

func (rp *routePlan) addPredicate(predicates ...sqlparser.Expr) error {

	if len(rp.tables) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "addPredicate should only be called when the route has a single table")
	}

	if rp.vindexPlusPredicates == nil {
		// Add all the column vindexes to the list of vindexPlusPredicates
		for _, columnVindex := range rp.vtable.ColumnVindexes {
			rp.vindexPlusPredicates = append(rp.vindexPlusPredicates, &vindexPlusPredicates{vindex: columnVindex})
		}
	}

	for _, filter := range predicates {
		switch node := filter.(type) {
		case *sqlparser.ComparisonExpr:
			switch node.Operator {
			case sqlparser.EqualOp:
				// TODO(Manan,Andres): Remove the predicates that are repeated eg. Id=1 AND Id=1
				for _, v := range rp.vindexPlusPredicates {
					column := node.Left.(*sqlparser.ColName)
					for _, col := range v.vindex.Columns {
						// If the column for the predicate matches any column in the vindex add it to the list
						if column.Name.Equal(col) {
							v.predicates = append(v.predicates, node)
							// Vindex is covered if all the columns in the vindex have a associated predicate
							v.covered = len(v.predicates) == len(v.vindex.Columns)
						}
					}
				}
			}
		}
	}

	//TODO (Manan,Andres): Improve cost metric for vindexes
	for _, v := range rp.vindexPlusPredicates {
		if !v.covered {
			continue
		}
		// Choose the minimum cost vindex from the ones which are covered
		if rp.vindex == nil || v.vindex.Vindex.Cost() < rp.vindex.Cost() {
			rp.vindex = v.vindex.Vindex
			rp.conditions = v.predicates
		}
	}

	if rp.vindex != nil {
		rp.routeOpCode = engine.SelectEqual
		if rp.vindex.IsUnique() {
			rp.routeOpCode = engine.SelectEqualUnique
		}
	}
	return nil
}

// Predicates takes all known predicates for this route and ANDs them together
func (rp *routePlan) Predicates() sqlparser.Expr {
	var result sqlparser.Expr
	add := func(e sqlparser.Expr) {
		if result == nil {
			result = e
			return
		}
		result = &sqlparser.AndExpr{
			Left:  result,
			Right: e,
		}
	}
	for _, t := range rp.tables {
		for _, predicate := range t.predicates {
			add(predicate)
		}
	}
	for _, p := range rp.extraPredicates {
		add(p)
	}
	return result
}

func (jp *joinPlan) solves() semantics.TableSet {
	return jp.lhs.solves() | jp.rhs.solves()
}
func (jp *joinPlan) cost() int {
	return jp.lhs.cost() + jp.rhs.cost()
}

/*
	we use dynamic programming to find the cheapest route/join tree possible,
	where the cost of a plan is the number of joins
*/
func solve(qg *queryGraph, semTable *semantics.SemTable, vschema ContextVSchema) (joinTree, error) {
	size := len(qg.tables)
	dpTable := makeDPTable()

	var allTables semantics.TableSet

	// we start by seeding the table with the single routes
	for _, table := range qg.tables {
		solves := semTable.TableSetFor(table.alias)
		allTables |= solves
		plan, err := createRoutePlan(table, solves, vschema)
		if err != nil {
			return nil, err
		}
		dpTable.add(plan)
	}

	for currentSize := 2; currentSize <= size; currentSize++ {
		lefts := dpTable.bitSetsOfSize(currentSize - 1)
		rights := dpTable.bitSetsOfSize(1)
		for _, lhs := range lefts {
			for _, rhs := range rights {
				if semantics.IsOverlapping(lhs.solves(), rhs.solves()) {
					// at least one of the tables is solved on both sides
					continue
				}
				solves := lhs.solves() | rhs.solves()
				oldPlan := dpTable.planFor(solves)
				if oldPlan != nil && oldPlan.cost() == 1 {
					// we already have the perfect plan. keep it
					continue
				}
				joinPredicates := qg.crossTable[solves]
				newPlan := qg.tryMerge(lhs, rhs, joinPredicates)
				if newPlan == nil {
					newPlan = &joinPlan{
						lhs:        lhs,
						rhs:        rhs,
						predicates: joinPredicates,
					}
				}
				if oldPlan == nil || newPlan.cost() < oldPlan.cost() {
					dpTable.add(newPlan)
				}
			}
		}
	}

	return dpTable.planFor(allTables), nil
}

func createRoutePlan(table *queryTable, solves semantics.TableSet, vschema ContextVSchema) (*routePlan, error) {
	vschemaTable, _, _, _, _, err := vschema.FindTableOrVindex(table.table)
	if err != nil {
		return nil, err
	}
	plan := &routePlan{
		solved:   solves,
		tables:   []*queryTable{table},
		keyspace: vschemaTable.Keyspace,
		vtable:   vschemaTable,
	}

	switch {
	case vschemaTable.Type == vindexes.TypeSequence:
		plan.routeOpCode = engine.SelectNext
	case vschemaTable.Type == vindexes.TypeReference:
		plan.routeOpCode = engine.SelectReference
	case !vschemaTable.Keyspace.Sharded:
		plan.routeOpCode = engine.SelectUnsharded
	case vschemaTable.Pinned != nil:

		// Pinned tables have their keyspace ids already assigned.
		// Use the Binary vindex, which is the identity function
		// for keyspace id.
		plan.routeOpCode = engine.SelectEqualUnique
	default:
		plan.routeOpCode = engine.SelectScatter
		err := plan.addPredicate(table.predicates...)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}

func transformToLogicalPlan(tree joinTree) (logicalPlan, error) {
	switch n := tree.(type) {
	case *routePlan:
		var tablesForSelect sqlparser.TableExprs
		tableNameMap := map[string]interface{}{}

		for _, t := range n.tables {
			tablesForSelect = append(tablesForSelect, t.alias)
			tableNameMap[sqlparser.String(t.alias.Expr)] = nil
		}
		predicates := n.Predicates()
		var where *sqlparser.Where
		if predicates != nil {
			where = &sqlparser.Where{Expr: predicates, Type: sqlparser.WhereClause}
		}
		var values []sqltypes.PlanValue
		if len(n.conditions) == 1 {
			value, err := sqlparser.NewPlanValue(n.conditions[0].(*sqlparser.ComparisonExpr).Right)
			if err != nil {
				return nil, err
			}
			values = []sqltypes.PlanValue{value}
		}
		var singleColumn vindexes.SingleColumn
		if n.vindex != nil {
			singleColumn = n.vindex.(vindexes.SingleColumn)
		}

		var tableNames []string
		for name := range tableNameMap {
			tableNames = append(tableNames, name)
		}

		return &route{
			eroute: &engine.Route{
				Opcode:    n.routeOpCode,
				TableName: strings.Join(tableNames, ", "),
				Keyspace:  n.keyspace,
				Vindex:    singleColumn,
				Values:    values,
			},
			Select: &sqlparser.Select{
				From:  tablesForSelect,
				Where: where,
			},
			solvedTables: n.solved,
		}, nil

	case *joinPlan:
		lhs, err := transformToLogicalPlan(n.lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := transformToLogicalPlan(n.rhs)
		if err != nil {
			return nil, err
		}
		return &join2{
			Left:  lhs,
			Right: rhs,
		}, nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unknown type encountered: %T", tree)
}
