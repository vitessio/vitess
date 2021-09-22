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

package abstract

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	/*QueryGraph represents the FROM and WHERE parts of a query.
	It is an intermediate representation of the query that makes it easier for the planner
	to find all possible join combinations. Instead of storing the query information in a form that is close
	to the syntax (AST), we extract the interesting parts into a graph form with the nodes being tables in the FROM
	clause and the edges between them being predicates. We keep predicates in a hash map keyed by the dependencies of
	the predicate. This makes it very fast to look up connections between tables in the query.
	*/
	QueryGraph struct {
		// the Tables, including predicates that only depend on this particular table
		Tables []*QueryTable

		// innerJoins contains the predicates that need multiple Tables
		innerJoins map[semantics.TableSet][]sqlparser.Expr

		// NoDeps contains the predicates that can be evaluated anywhere.
		NoDeps sqlparser.Expr
	}

	// QueryTable is a single FROM table, including all predicates particular to this table
	QueryTable struct {
		TableID     semantics.TableSet
		Alias       *sqlparser.AliasedTableExpr
		Table       sqlparser.TableName
		Predicates  []sqlparser.Expr
		IsInfSchema bool
	}
)

var _ Operator = (*QueryGraph)(nil)

// PushPredicate implements the Operator interface
func (qg *QueryGraph) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		err := qg.collectPredicate(e, semTable)
		if err != nil {
			return err
		}
	}
	return nil
}

// TableID implements the Operator interface
func (qg *QueryGraph) TableID() semantics.TableSet {
	var ts semantics.TableSet
	for _, table := range qg.Tables {
		ts = ts.Merge(table.TableID)
	}
	return ts
}

// GetPredicates returns the predicates that are applicable for the two given TableSets
func (qg *QueryGraph) GetPredicates(lhs, rhs semantics.TableSet) []sqlparser.Expr {
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

func newQueryGraph() *QueryGraph {
	return &QueryGraph{
		innerJoins: map[semantics.TableSet][]sqlparser.Expr{},
	}
}

func (qg *QueryGraph) collectPredicates(sel *sqlparser.Select, semTable *semantics.SemTable) error {
	predicates := sqlparser.SplitAndExpression(nil, sel.Where.Expr)

	for _, predicate := range predicates {
		err := qg.collectPredicate(predicate, semTable)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qg *QueryGraph) collectPredicate(predicate sqlparser.Expr, semTable *semantics.SemTable) error {
	deps := semTable.RecursiveDeps(predicate)
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
	return nil
}

func (qg *QueryGraph) addToSingleTable(table semantics.TableSet, predicate sqlparser.Expr) bool {
	for _, t := range qg.Tables {
		if table == t.TableID {
			t.Predicates = append(t.Predicates, predicate)
			return true
		}
	}
	return false
}

func (qg *QueryGraph) addNoDepsPredicate(predicate sqlparser.Expr) {
	if qg.NoDeps == nil {
		qg.NoDeps = predicate
	} else {
		qg.NoDeps = &sqlparser.AndExpr{
			Left:  qg.NoDeps,
			Right: predicate,
		}
	}
}

// UnsolvedPredicates implements the Operator interface
func (qg *QueryGraph) UnsolvedPredicates(_ *semantics.SemTable) []sqlparser.Expr {
	var result []sqlparser.Expr
	for set, exprs := range qg.innerJoins {
		if !set.IsSolvedBy(qg.TableID()) {
			result = append(result, exprs...)
		}
	}
	return result
}

// CheckValid implements the Operator interface
func (qg *QueryGraph) CheckValid() error {
	return nil
}
