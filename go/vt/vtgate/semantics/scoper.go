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

package semantics

import (
	"reflect"

	"vitess.io/vitess/go/vt/sqlparser"
)

// scoper is responsible for figuring out the scoping for the query,
// and keeps the current scope when walking the tree
type scoper struct {
	rScope       map[*sqlparser.Select]*scope
	wScope       map[*sqlparser.Select]*scope
	sqlNodeScope map[scopeKey]*scope
	scopes       []*scope
	org          originable
}

type scopeKey struct {
	typ  keyType
	node sqlparser.SQLNode
}

type keyType int8

const (
	_ keyType = iota
	orderBy
	groupBy
	having
)

func newScoper() *scoper {
	return &scoper{
		rScope:       map[*sqlparser.Select]*scope{},
		wScope:       map[*sqlparser.Select]*scope{},
		sqlNodeScope: map[scopeKey]*scope{},
	}
}

func (s *scoper) down(cursor *sqlparser.Cursor) {
	switch node := cursor.Node().(type) {
	case *sqlparser.Select:
		currScope := newScope(s.currentScope())
		s.push(currScope)

		// Needed for order by with Literal to find the Expression.
		currScope.selectStmt = node

		s.rScope[node] = currScope
		s.wScope[node] = newScope(nil)
		s.sqlNodeScope[scopeKey{node: node}] = currScope
	case sqlparser.TableExpr:
		if isParentSelect(cursor) {
			// when checking the expressions used in JOIN conditions, special rules apply where the ON expression
			// can only see the two tables involved in the JOIN, and no other tables.
			// To create this special context, we create a special scope here that is then merged with
			// the surrounding scope when we come back out from the JOIN
			nScope := newScope(nil)
			nScope.selectStmt = cursor.Parent().(*sqlparser.Select)
			s.push(nScope)
			s.sqlNodeScope[scopeKey{node: node}] = nScope
		}
	case sqlparser.SelectExprs:
		sel, parentIsSelect := cursor.Parent().(*sqlparser.Select)
		if !parentIsSelect {
			break
		}

		// adding a VTableInfo for each SELECT, so it can be used by GROUP BY, HAVING, ORDER BY
		// the VTableInfo we are creating here should not be confused with derived tables' VTableInfo
		wScope, exists := s.wScope[sel]
		if !exists {
			break
		}
		wScope.tables = append(wScope.tables, createVTableInfoForExpressions(node, s.currentScope().tables, s.org))
	case sqlparser.OrderBy:
		s.changeScopeForNode(cursor, scopeKey{node: cursor.Parent(), typ: orderBy})
	case sqlparser.GroupBy:
		s.changeScopeForNode(cursor, scopeKey{node: cursor.Parent(), typ: groupBy})
	case *sqlparser.Where:
		if node.Type != sqlparser.HavingClause {
			break
		}
		s.changeScopeForNode(cursor, scopeKey{node: cursor.Parent(), typ: having})
	}
}

func (s *scoper) up(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.Select, sqlparser.OrderBy, sqlparser.GroupBy:
		s.popScope()
	case *sqlparser.Where:
		if node.Type != sqlparser.HavingClause {
			break
		}
		s.popScope()
	case sqlparser.TableExpr:
		if isParentSelect(cursor) {
			curScope := s.currentScope()
			s.popScope()
			earlierScope := s.currentScope()
			// copy curScope into the earlierScope
			for _, table := range curScope.tables {
				err := earlierScope.addTable(table)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *scoper) downPost(cursor *sqlparser.Cursor) {
	var scope *scope
	var found bool

	switch node := cursor.Node().(type) {
	case sqlparser.OrderBy:
		scope, found = s.sqlNodeScope[scopeKey{node: cursor.Parent(), typ: orderBy}]
	case sqlparser.GroupBy:
		scope, found = s.sqlNodeScope[scopeKey{node: cursor.Parent(), typ: groupBy}]
	case *sqlparser.Where:
		if node.Type != sqlparser.HavingClause {
			break
		}
		scope, found = s.sqlNodeScope[scopeKey{node: cursor.Parent(), typ: having}]
	default:
		if validAsMapKey(node) {
			scope, found = s.sqlNodeScope[scopeKey{node: node}]
		}
	}

	if found {
		s.push(scope)
	}
}

func validAsMapKey(s sqlparser.SQLNode) bool {
	return reflect.TypeOf(s).Comparable()
}

func (s *scoper) upPost(cursor *sqlparser.Cursor) error {
	var found bool

	switch node := cursor.Node().(type) {
	case sqlparser.OrderBy:
		_, found = s.sqlNodeScope[scopeKey{node: cursor.Parent(), typ: orderBy}]
	case sqlparser.GroupBy:
		_, found = s.sqlNodeScope[scopeKey{node: cursor.Parent(), typ: groupBy}]
	case *sqlparser.Where:
		if node.Type != sqlparser.HavingClause {
			break
		}
		_, found = s.sqlNodeScope[scopeKey{node: cursor.Parent(), typ: having}]
	default:
		if validAsMapKey(node) {
			_, found = s.sqlNodeScope[scopeKey{node: node}]
		}
	}

	if found {
		s.popScope()
	}
	return nil
}

func (s *scoper) changeScopeForNode(cursor *sqlparser.Cursor, k scopeKey) {
	switch parent := cursor.Parent().(type) {
	case *sqlparser.Select:
		// In ORDER BY, GROUP BY and HAVING, we can see both the scope in the FROM part of the query, and the SELECT columns created
		// so before walking the rest of the tree, we change the scope to match this behaviour
		incomingScope := s.currentScope()
		nScope := newScope(incomingScope)
		s.push(nScope)
		s.sqlNodeScope[k] = nScope
		wScope := s.wScope[parent]
		nScope.tables = append(nScope.tables, wScope.tables...)
		nScope.selectStmt = incomingScope.selectStmt

		if s.rScope[parent] != incomingScope {
			panic("BUG: scope counts did not match")
		}
	case *sqlparser.Union:
		nScope := newScope(nil)
		nScope.selectStmt = sqlparser.GetFirstSelect(parent)
		s.push(nScope)
		s.sqlNodeScope[k] = nScope
	default:
		return
	}
}

func (s *scoper) currentScope() *scope {
	size := len(s.scopes)
	if size == 0 {
		return nil
	}
	return s.scopes[size-1]
}

func (s *scoper) push(sc *scope) {
	s.scopes = append(s.scopes, sc)
}

func (s *scoper) popScope() {
	l := len(s.scopes) - 1
	s.scopes = s.scopes[:l]
}
