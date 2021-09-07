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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// queryTree interface and implementations
// These representation helps in optimizing the join planning using tables and predicates.
type (
	queryTree interface {
		// tableID returns the table identifiers that are solved by this plan
		tableID() semantics.TableSet

		// creates a copy of the queryTree that can be updated without changing the original
		clone() queryTree

		// cost is simply the number of routes in the queryTree
		cost() int

		pushOutputColumns([]*sqlparser.ColName, *semantics.SemTable) ([]int, error)
	}
)

// relation interface and implementations
// They are representation of the tables in a routeTree
// When we are able to merge queryTree then it lives as relation otherwise it stays as joinTree
type (
	relation interface {
		tableID() semantics.TableSet
		tableNames() []string
	}

	joinTables struct {
		lhs, rhs relation
		pred     sqlparser.Expr
	}

	routeTable struct {
		qtable *abstract.QueryTable
		vtable *vindexes.Table
	}

	parenTables []relation

	derivedTable struct {
		// tables contains inner tables that are solved by this plan.
		// the tables also contain any predicates that only depend on that particular table
		tables parenTables

		// predicates are the predicates evaluated by this plan
		predicates []sqlparser.Expr

		// leftJoins are the join conditions evaluated by this plan
		leftJoins []*outerTable

		alias string

		query sqlparser.SelectStatement
	}
)

type outerTable struct {
	right relation
	pred  sqlparser.Expr
}

// type assertions
var _ relation = (*routeTable)(nil)
var _ relation = (*joinTables)(nil)
var _ relation = (parenTables)(nil)
var _ relation = (*derivedTable)(nil)

func (d *derivedTable) tableID() semantics.TableSet { return d.tables.tableID() }

func (d *derivedTable) tableNames() []string { return d.tables.tableNames() }

func (rp *routeTable) tableID() semantics.TableSet { return rp.qtable.TableID }

func (rp *joinTables) tableID() semantics.TableSet { return rp.lhs.tableID().Merge(rp.rhs.tableID()) }

func (rp *joinTables) tableNames() []string {
	return append(rp.lhs.tableNames(), rp.rhs.tableNames()...)
}

func (rp *routeTable) tableNames() []string {
	var name string
	if rp.qtable.IsInfSchema {
		name = sqlparser.String(rp.qtable.Table)
	} else {
		name = sqlparser.String(rp.qtable.Table.Name)
	}
	return []string{name}
}

func (p parenTables) tableNames() []string {
	var result []string
	for _, r := range p {
		result = append(result, r.tableNames()...)
	}
	return result
}

func (p parenTables) tableID() semantics.TableSet {
	res := semantics.TableSet(0)
	for _, r := range p {
		res = res.Merge(r.tableID())
	}
	return res
}

// visitRelations visits all relations recursively and applies the function f on them
// If the function f returns false: the children of the current relation will not be visited.
func visitRelations(r relation, f func(tbl relation) (bool, error)) error {
	kontinue, err := f(r)
	if err != nil {
		return err
	}
	if !kontinue {
		return nil
	}

	switch r := r.(type) {
	case *routeTable:
		// already visited when entering this method
	case parenTables:
		for _, r := range r {
			err := visitRelations(r, f)
			if err != nil {
				return err
			}
		}
		return nil
	case *joinTables:
		err := visitRelations(r.lhs, f)
		if err != nil {
			return err
		}
		err = visitRelations(r.rhs, f)
		if err != nil {
			return err
		}
		return nil
	case *derivedTable:
		err := visitRelations(r.tables, f)
		if err != nil {
			return err
		}
	}
	return nil
}
