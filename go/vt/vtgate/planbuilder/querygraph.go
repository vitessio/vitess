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

		// subqueries contains the subqueries that depend on this query graph
		subqueries map[*sqlparser.Subquery][]*queryGraph
	}

	// queryTable is a single FROM table, including all predicates particular to this table
	queryTable struct {
		tableID    semantics.TableSet
		alias      *sqlparser.AliasedTableExpr
		table      sqlparser.TableName
		predicates []sqlparser.Expr
	}
)

func (qg *queryGraph) getPredicates(lhs, rhs semantics.TableSet) []sqlparser.Expr {
	var allExprs []sqlparser.Expr
	for tableSet, exprs := range qg.crossTable {
		if tableSet.IsSolvedBy(lhs|rhs) &&
			tableSet.IsOverlapping(rhs) &&
			tableSet.IsOverlapping(lhs) {
			allExprs = append(allExprs, exprs...)
		}
	}
	return allExprs
}

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

func createQGFromSelectStatement(selStmt sqlparser.SelectStatement, semTable *semantics.SemTable) ([]*queryGraph, error) {
	switch stmt := selStmt.(type) {
	case *sqlparser.Select:
		qg, err := createQGFromSelect(stmt, semTable)
		if err != nil {
			return nil, err
		}
		return []*queryGraph{qg}, err
	case *sqlparser.Union:
		qg, err := createQGFromSelectStatement(stmt.FirstStatement, semTable)
		if err != nil {
			return nil, err
		}
		for _, sel := range stmt.UnionSelects {
			qgr, err := createQGFromSelectStatement(sel.Statement, semTable)
			if err != nil {
				return nil, err
			}
			qg = append(qg, qgr...)
		}
		return qg, nil
	case *sqlparser.ParenSelect:
		return createQGFromSelectStatement(stmt.Select, semTable)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] not reachable %T", selStmt)
}

func newQueryGraph() *queryGraph {
	return &queryGraph{
		crossTable: map[semantics.TableSet][]sqlparser.Expr{},
		subqueries: map[*sqlparser.Subquery][]*queryGraph{},
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
		if table.Condition.On != nil {
			for _, predicate := range splitAndExpression(nil, table.Condition.On) {
				err := qg.collectPredicate(predicate, semTable)
				if err != nil {
					return err
				}
			}
		}
	case *sqlparser.ParenTableExpr:
		for _, expr := range table.Exprs {
			if err := qg.collectTable(expr, semTable); err != nil {
				return err
			}
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
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch subQuery := node.(type) {
		case *sqlparser.Subquery:

			qgr, err := createQGFromSelectStatement(subQuery.Select, semTable)
			if err != nil {
				return false, err
			}
			qg.subqueries[subQuery] = qgr
		}
		return true, nil
	}, predicate)

	return err
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
