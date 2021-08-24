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

import "vitess.io/vitess/go/vt/sqlparser"

// scoper is responsible for figuring out the scoping for the query,
// and keeps the current scope when walking the tree
type scoper struct {
	rScope       map[*sqlparser.Select]*scope
	wScope       map[*sqlparser.Select]*scope
	sqlNodeScope map[sqlparser.SQLNode]*scope
	scopes       []*scope
}

func newScoper() *scoper {
	return &scoper{
		rScope:       map[*sqlparser.Select]*scope{},
		wScope:       map[*sqlparser.Select]*scope{},
		sqlNodeScope: map[sqlparser.SQLNode]*scope{},
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
		s.sqlNodeScope[node] = currScope
	case sqlparser.TableExpr:
		if isParentSelect(cursor) {
			// when checking the expressions used in JOIN conditions, special rules apply where the ON expression
			// can only see the two tables involved in the JOIN, and no other tables.
			// To create this special context, we create a special scope here that is then merged with
			// the surrounding scope when we come back out from the JOIN
			nScope := newScope(nil)
			nScope.selectStmt = cursor.Parent().(*sqlparser.Select)
			s.push(nScope)
			s.sqlNodeScope[node] = nScope
		}
	case *sqlparser.Union:
		scope := newScope(s.currentScope())
		s.push(scope)
		s.sqlNodeScope[node] = scope
	case sqlparser.SelectExprs:
		sel, parentIsSelect := cursor.Parent().(*sqlparser.Select)
		if !parentIsSelect {
			break
		}

		wScope, exists := s.wScope[sel]
		if !exists {
			break
		}

		wScope.tables = append(wScope.tables, createVTableInfoForExpressions(node))
	}
}

func (s *scoper) downPost(cursor *sqlparser.Cursor) {
	switch node := cursor.Node().(type) {
	case *sqlparser.Select:
		s.push(s.sqlNodeScope[node])
	case sqlparser.TableExpr:
		if isParentSelect(cursor) {
			s.push(s.sqlNodeScope[node])
		}
	case *sqlparser.Union:
		s.push(s.sqlNodeScope[node])
	case sqlparser.OrderBy, sqlparser.GroupBy:
		// ORDER BY and GROUP BY live in a special scope where they can access the SELECT expressions declared,
		// but also the tables in the FROM clause
		s.changeScopeForOrderBy(cursor)
	}
}

func (s *scoper) up(cursor *sqlparser.Cursor) error {
	switch cursor.Node().(type) {
	case *sqlparser.Union, *sqlparser.Select:
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

func (s *scoper) upPost(cursor *sqlparser.Cursor) error {
	switch cursor.Node().(type) {
	case *sqlparser.Union, *sqlparser.Select, sqlparser.OrderBy, sqlparser.GroupBy:
		s.popScope()
	case sqlparser.TableExpr:
		if isParentSelect(cursor) {
			s.popScope()
		}
	}
	return nil
}

func (s *scoper) changeScopeForOrderBy(cursor *sqlparser.Cursor) {
	sel, ok := cursor.Parent().(*sqlparser.Select)
	if !ok {
		return
	}
	// In ORDER BY, we can see both the scope in the FROM part of the query, and the SELECT columns created
	// so before walking the rest of the tree, we change the scope to match this behaviour
	incomingScope := s.currentScope()
	nScope := newScope(incomingScope)
	s.push(nScope)
	wScope := s.wScope[sel]
	nScope.tables = append(nScope.tables, wScope.tables...)
	nScope.selectStmt = incomingScope.selectStmt

	if s.rScope[sel] != incomingScope {
		panic("BUG: scope counts did not match")
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
