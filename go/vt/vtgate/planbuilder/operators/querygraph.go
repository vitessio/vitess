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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
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
		innerJoins []*innerJoin

		// NoDeps contains the predicates that can be evaluated anywhere.
		NoDeps sqlparser.Expr

		noInputs
	}

	innerJoin struct {
		deps  semantics.TableSet
		exprs []sqlparser.Expr
	}

	// QueryTable is a single FROM table, including all predicates particular to this table
	// This is to be used as an immutable data structure which is created in the logical Operator Tree
	QueryTable struct {
		ID          semantics.TableSet
		Alias       *sqlparser.AliasedTableExpr
		Table       sqlparser.TableName
		Predicates  []sqlparser.Expr
		IsInfSchema bool
	}
)

var _ Operator = (*QueryGraph)(nil)

// Introduces implements the tableIDIntroducer interface
func (qg *QueryGraph) Introduces() semantics.TableSet {
	var ts semantics.TableSet
	for _, table := range qg.Tables {
		ts = ts.Merge(table.ID)
	}
	return ts
}

// GetPredicates returns the predicates that are applicable for the two given TableSets
func (qg *QueryGraph) GetPredicates(lhs, rhs semantics.TableSet) []sqlparser.Expr {
	var allExprs []sqlparser.Expr
	for _, join := range qg.innerJoins {
		tableSet, exprs := join.deps, join.exprs
		if tableSet.IsSolvedBy(lhs.Merge(rhs)) &&
			tableSet.IsOverlapping(rhs) &&
			tableSet.IsOverlapping(lhs) {
			allExprs = append(allExprs, exprs...)
		}
	}
	return allExprs
}

func newQueryGraph() *QueryGraph {
	return &QueryGraph{}
}

func (qg *QueryGraph) collectPredicates(ctx *plancontext.PlanningContext, sel *sqlparser.Select) error {
	predicates := sqlparser.SplitAndExpression(nil, sel.Where.Expr)

	for _, predicate := range predicates {
		err := qg.collectPredicate(ctx, predicate)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qg *QueryGraph) getPredicateByDeps(ts semantics.TableSet) ([]sqlparser.Expr, bool) {
	for _, join := range qg.innerJoins {
		if join.deps == ts {
			return join.exprs, true
		}
	}
	return nil, false
}
func (qg *QueryGraph) addJoinPredicates(ts semantics.TableSet, expr sqlparser.Expr) {
	for _, join := range qg.innerJoins {
		if join.deps == ts {
			join.exprs = append(join.exprs, expr)
			return
		}
	}

	qg.innerJoins = append(qg.innerJoins, &innerJoin{
		deps:  ts,
		exprs: []sqlparser.Expr{expr},
	})
}

func (qg *QueryGraph) collectPredicate(ctx *plancontext.PlanningContext, predicate sqlparser.Expr) error {
	deps := ctx.SemTable.RecursiveDeps(predicate)
	switch deps.NumberOfTables() {
	case 0:
		qg.addNoDepsPredicate(predicate)
	case 1:
		found := qg.addToSingleTable(deps, predicate)
		if !found {
			// this could be a predicate that only has dependencies from outside this QG
			qg.addJoinPredicates(deps, predicate)
		}
	default:
		qg.addJoinPredicates(deps, predicate)
	}
	return nil
}

func (qg *QueryGraph) addToSingleTable(table semantics.TableSet, predicate sqlparser.Expr) bool {
	for _, t := range qg.Tables {
		if table == t.ID {
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

// UnsolvedPredicates implements the unresolved interface
func (qg *QueryGraph) UnsolvedPredicates(_ *semantics.SemTable) []sqlparser.Expr {
	var result []sqlparser.Expr
	tables := TableID(qg)
	for _, join := range qg.innerJoins {
		set, exprs := join.deps, join.exprs
		if !set.IsSolvedBy(tables) {
			result = append(result, exprs...)
		}
	}
	return result
}

// Clone implements the Operator interface
func (qg *QueryGraph) Clone(inputs []Operator) Operator {
	checkSize(inputs, 0)
	result := &QueryGraph{
		Tables:     nil,
		innerJoins: nil,
		NoDeps:     nil,
	}

	result.Tables = append([]*QueryTable{}, qg.Tables...)
	result.innerJoins = append([]*innerJoin{}, qg.innerJoins...)
	result.NoDeps = qg.NoDeps
	return result
}
