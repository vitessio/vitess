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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	/*
	 queryGraph represents the FROM and WHERE parts of a query.
	 It is an intermediate representation of the query that makes it easier for the planner
	 to find all possible join combinations. Instead of storing the query information in a form that is close
	 to the syntax (AST), we extract the interesting parts into a graph form with the nodes being tables in the FROM
	 clause and the edges between them being predicates. We keep predicates in a hash map keyed by the dependencies of
	 the predicate. This makes it very fast to look up connections between tables in the query.
	*/
	queryGraph struct {
		// the tables, including predicates that only depend on this particular table
		tables []*queryTable

		// crossTable contains the predicates that need multiple tables
		crossTable map[semantics.TableSet][]sqlparser.Expr

		// noDeps contains the predicates that can be evaluated anywhere.
		noDeps sqlparser.Expr
	}

	// queryTable is a single FROM table, including all predicates particular to this table
	queryTable struct {
		tableID    semantics.TableSet
		alias      *sqlparser.AliasedTableExpr
		table      sqlparser.TableName
		predicates []sqlparser.Expr
	}
)

func createQGFromSelect(sel *sqlparser.Select, semTable *semantics.SemTable) (*queryGraph, error) {
	qg := newQueryGraph()
	if err := qg.collectTables(sel.From, semTable); err != nil {
		return nil, err
	}

	if sel.Where != nil {
		err := qg.collectPredicates(sel, semTable)
		if err != nil {
			return nil, err
		}
	}
	return qg, nil
}

func newQueryGraph() *queryGraph {
	return &queryGraph{
		crossTable: map[semantics.TableSet][]sqlparser.Expr{},
	}
}

func (qg *queryGraph) collectTable(t sqlparser.TableExpr, semTable *semantics.SemTable) error {
	switch table := t.(type) {
	case *sqlparser.AliasedTableExpr:
		tableName := table.Expr.(sqlparser.TableName)
		qt := &queryTable{alias: table, table: tableName, tableID: semTable.TableSetFor(table)}
		qg.tables = append(qg.tables, qt)
	case *sqlparser.JoinTableExpr:
		if err := qg.collectTable(table.LeftExpr, semTable); err != nil {
			return err
		}
		if err := qg.collectTable(table.RightExpr, semTable); err != nil {
			return err
		}
		if err := qg.collectPredicate(table.Condition.On, semTable); err != nil {
			return err
		}
	case *sqlparser.ParenTableExpr:
		if err := qg.collectTables(table.Exprs, semTable); err != nil {
			return err
		}
	}
	return nil
}

func (qg *queryGraph) collectTables(t sqlparser.TableExprs, semTable *semantics.SemTable) error {
	for _, expr := range t {
		if err := qg.collectTable(expr, semTable); err != nil {
			return err
		}
	}
	return nil
}

func (qg *queryGraph) collectPredicates(sel *sqlparser.Select, semTable *semantics.SemTable) error {
	predicates := splitAndExpression(nil, sel.Where.Expr)

	for _, predicate := range predicates {
		err := qg.collectPredicate(predicate, semTable)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qg *queryGraph) collectPredicate(predicate sqlparser.Expr, semTable *semantics.SemTable) error {
	deps := semTable.Dependencies(predicate)
	switch deps.NumberOfTables() {
	case 0:
		qg.addNoDepsPredicate(predicate)
	case 1:
		found := qg.addToSingleTable(deps, predicate)
		if !found {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "table %v for predicate %v not found", deps, sqlparser.String(predicate))
		}
	default:
		allPredicates, found := qg.crossTable[deps]
		if found {
			allPredicates = append(allPredicates, predicate)
		} else {
			allPredicates = []sqlparser.Expr{predicate}
		}
		qg.crossTable[deps] = allPredicates
	}
	return nil
}

func (qg *queryGraph) addToSingleTable(table semantics.TableSet, predicate sqlparser.Expr) bool {
	for _, t := range qg.tables {
		if table == t.tableID {
			t.predicates = append(t.predicates, predicate)
			return true
		}
	}
	return false
}

func (qg *queryGraph) addNoDepsPredicate(predicate sqlparser.Expr) {
	if qg.noDeps == nil {
		qg.noDeps = predicate
	} else {
		qg.noDeps = &sqlparser.AndExpr{
			Left:  qg.noDeps,
			Right: predicate,
		}
	}
}

func tryMerge(a, b joinTree, joinPredicates []sqlparser.Expr) joinTree {
	aRoute, ok := a.(*routePlan)
	if !ok {
		return nil
	}
	bRoute, ok := b.(*routePlan)
	if !ok {
		return nil
	}
	if aRoute.keyspace != bRoute.keyspace {
		return nil
	}

	switch aRoute.routeOpCode {
	case engine.SelectUnsharded, engine.SelectDBA:
		if aRoute.routeOpCode != bRoute.routeOpCode {
			return nil
		}
	case engine.SelectEqualUnique:
		return nil
	case engine.SelectScatter:
		//if len(joinPredicates) == 0 {
		// If we are doing two Scatters, we have to make sure that the
		// joins are on the correct vindex to allow them to be merged
		// no join predicates - no vindex
		return nil
		//}
	}

	newTabletSet := aRoute.solved | bRoute.solved
	r := &routePlan{
		routeOpCode:          aRoute.routeOpCode,
		solved:               newTabletSet,
		tables:               append(aRoute.tables, bRoute.tables...),
		extraPredicates:      append(aRoute.extraPredicates, bRoute.extraPredicates...),
		keyspace:             aRoute.keyspace,
		vindexPlusPredicates: append(aRoute.vindexPlusPredicates, bRoute.vindexPlusPredicates...),
	}

	r.extraPredicates = append(r.extraPredicates, joinPredicates...)
	return r
}
