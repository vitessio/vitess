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

package semantic

import (
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	scope struct {
		parent     *scope
		tableExprs []*table
	}
	scoper struct {
		current []*scope
		left    [][]*scope
	}
)

// Pushes the current scope to the right, so a disconnected scope can be created.
// This is used for derived tables - they don't share scope with parents
func (s *scoper) Right(newScope *scope) {
	s.left = append(s.left, s.current)
	s.current = []*scope{newScope}
}

// Once we are done with the derived table, we can move left again to regain the outer scope
func (s *scoper) Left() {
	s.current = s.left[len(s.left)-1]
	s.left = s.left[:len(s.left)-1]
}

func (s *scoper) Push(newScope *scope) {
	s.current = append(s.current, newScope)
}

func (s *scoper) Pop() {
	s.current = s.current[:len(s.current)-1]
}

func (s *scoper) Current() *scope {
	return s.current[len(s.current)-1]
}
func (s *scoper) Result() (*scope, error) {
	if len(s.current) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "stack error")
	}
	return s.current[0], nil
}

func (s *scoper) VisitUp(current, parent sqlparser.SQLNode) {
	switch current.(type) {
	case *sqlparser.Subquery:
		if inFromClause(parent) {
			s.Left()
		} else {
			s.Pop()
		}
	}
}

func (s *scoper) VisitDown(current, parent sqlparser.SQLNode) *scope {
	switch current.(type) {
	case *sqlparser.Subquery:
		// We use the same struct for subqueries and derived tables,
		// but they have different scoping rules, so here we check if we are in the From-clause
		if inFromClause(parent) {
			s.Right(&scope{})
		} else {
			newScope := &scope{parent: s.current[0]}
			s.Push(newScope)
		}
	}
	return s.Current()
}

func inFromClause(parent sqlparser.SQLNode) bool {
	_, isDerivedTable := parent.(*sqlparser.AliasedTableExpr)
	return isDerivedTable
}

//FindTable returns the table that this column refers to, and whether the table is defined in the same scope or not
func (s *scope) FindTable(qualifier, table string) (*table, bool) {
	if qualifier == "" && table == "" {
		if len(s.tableExprs) != 1 {
			panic("i dont know!!!!")
		}
		return s.tableExprs[0], true
	}
	for _, expr := range s.tableExprs {
		if expr.hasAlias() {
			if qualifier != "" {
				// if we have an alias, we can't access it through any ColName that has a Qualifier
				continue
			}
			if expr.alias == table {
				return expr, true
			}
		} else {
			if (qualifier == "" || qualifier == expr.qualifier) && expr.name == table {
				return expr, true
			}
		}
	}
	if s.parent != nil {
		findTable, _ := s.parent.FindTable(qualifier, table)
		return findTable, false
	}
	return nil, false
}
