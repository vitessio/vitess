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

		// innerJoins contains the predicates that need multiple tables
		innerJoins map[semantics.TableSet][]sqlparser.Expr

		outerJoins map[outerJoinTables][]sqlparser.Expr

		// noDeps contains the predicates that can be evaluated anywhere.
		noDeps sqlparser.Expr

		// subqueries contains the subqueries that depend on this query graph
		subqueries map[*sqlparser.Subquery][]*queryGraph
	}

	outerJoinTables struct {
		inner, outer semantics.TableSet
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
	for tableSet, exprs := range qg.innerJoins {
		if tableSet.IsSolvedBy(lhs|rhs) &&
			tableSet.IsOverlapping(rhs) &&
			tableSet.IsOverlapping(lhs) {
			allExprs = append(allExprs, exprs...)
		}
	}
	return allExprs
}

func (qg *queryGraph) getOuterJoins(lhs, rhs semantics.TableSet) []sqlparser.Expr {
	var allExprs []sqlparser.Expr
	for tableSet, exprs := range qg.outerJoins {
		if tableSet.outer.IsSolvedBy(rhs) && tableSet.inner.IsSolvedBy(lhs) {
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
		innerJoins: map[semantics.TableSet][]sqlparser.Expr{},
		outerJoins: map[outerJoinTables][]sqlparser.Expr{},
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
				err := qg.collectPredicateTable(t, predicate, semTable)
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

func (qg *queryGraph) collectPredicateTable(t sqlparser.TableExpr, predicate sqlparser.Expr, semTable *semantics.SemTable) error {
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
		switch table := t.(type) {
		case *sqlparser.JoinTableExpr:
			switch table.Join {
			case sqlparser.NormalJoinType:
				allPredicates, found := qg.innerJoins[deps]
				if found {
					allPredicates = append(allPredicates, predicate)
				} else {
					allPredicates = []sqlparser.Expr{predicate}
				}
				qg.innerJoins[deps] = allPredicates
			case sqlparser.LeftJoinType, sqlparser.RightJoinType:
				var outerJoinTable outerJoinTables
				var err error
				if table.Join == sqlparser.LeftJoinType {
					outerJoinTable, err = qg.getOuterJoinTables(table.LeftExpr, table.RightExpr, right, left, semTable)
					if err != nil {
						return err
					}
				} else {
					outerJoinTable, err = qg.getOuterJoinTables(table.RightExpr, table.LeftExpr, left, right, semTable)
					if err != nil {
						return err
					}
				}

				allPredicates := qg.outerJoins[outerJoinTable]
				allPredicates = append(allPredicates, predicate)
				qg.outerJoins[outerJoinTable] = allPredicates
			}
		}
	}
	err := qg.walkQGSubQueries(semTable, predicate)
	return err
}

func (qg *queryGraph) getOuterJoinTables(lhs, rhs sqlparser.TableExpr, l, r extractor, semTable *semantics.SemTable) (outerJoinTables, error) {
	t, err := l(lhs)
	if err != nil {
		return outerJoinTables{}, err
	}
	lft := semTable.TableSetFor(t)
	t, err = r(rhs)
	if err != nil {
		return outerJoinTables{}, err
	}
	rgt := semTable.TableSetFor(t)
	return outerJoinTables{
		inner: lft,
		outer: rgt,
	}, nil
}

type extractor func(sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, error)

func left(tbl sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, error) {
	switch tbl := tbl.(type) {
	case *sqlparser.AliasedTableExpr:
		return tbl, nil
	case *sqlparser.JoinTableExpr:
		return left(tbl.LeftExpr)
	case *sqlparser.ParenTableExpr:
		return left(tbl.Exprs[0])
	default:
		return nil, semantics.Gen4NotSupportedF(sqlparser.String(tbl))
	}
}
func right(tbl sqlparser.TableExpr) (*sqlparser.AliasedTableExpr, error) {
	switch tbl := tbl.(type) {
	case *sqlparser.AliasedTableExpr:
		return tbl, nil
	case *sqlparser.JoinTableExpr:
		return right(tbl.RightExpr)
	case *sqlparser.ParenTableExpr:
		return right(tbl.Exprs[len(tbl.Exprs)])
	default:
		return nil, semantics.Gen4NotSupportedF(sqlparser.String(tbl))
	}
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
		allPredicates, found := qg.innerJoins[deps]
		if found {
			allPredicates = append(allPredicates, predicate)
		} else {
			allPredicates = []sqlparser.Expr{predicate}
		}
		qg.innerJoins[deps] = allPredicates
	}
	err := qg.walkQGSubQueries(semTable, predicate)
	return err
}

func (qg *queryGraph) walkQGSubQueries(semTable *semantics.SemTable, predicate sqlparser.Expr) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
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
