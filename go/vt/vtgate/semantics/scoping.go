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

package semantics

import (
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	scope struct {
		i      int
		parent *scope
		tables map[string]*sqlparser.AliasedTableExpr
	}
)

var i = 0

func newScope(parent *scope) *scope {
	i++
	return &scope{i: i, tables: map[string]*sqlparser.AliasedTableExpr{}, parent: parent}
}

func (s *scope) addTable(name string, table *sqlparser.AliasedTableExpr) error {
	_, found := s.tables[name]
	if found {
		return mysql.NewSQLError(mysql.ERNonUniqTable, mysql.SSSyntaxErrorOrAccessViolation, "Not unique table/alias: '%s'", name)
	}
	s.tables[name] = table
	return nil
}

func (a *analyzer) scopeExprs(cursor *sqlparser.Cursor) bool {
	n := cursor.Node()
	current := a.peek()
	log(n, "%d scopeExprs %T", current.i, n)
	switch expr := n.(type) {
	case *sqlparser.Subquery:
		a.exprScope[expr] = current
		a.push(newScope(current))
		a.analyze(expr.Select)
		a.pop()
		return false
	case sqlparser.Expr:
		a.exprScope[expr] = current
	}
	return true
}

func (a *analyzer) analyzeTableExprs(tablExprs sqlparser.TableExprs) {
	for _, tableExpr := range tablExprs {
		a.analyzeTableExpr(tableExpr)
	}
}

func (a *analyzer) analyzeTableExpr(tableExpr sqlparser.TableExpr) bool {
	log(tableExpr, "analyzeTableExpr %T", tableExpr)
	switch table := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		expr := table.Expr
		switch t := expr.(type) {
		case *sqlparser.DerivedTable:
			a.push(newScope(nil))
			a.analyze(t.Select)
			a.pop()
			scope := a.peek()
			a.err = scope.addTable(table.As.String(), table)
		case sqlparser.TableName:
			scope := a.peek()
			if table.As.IsEmpty() {
				a.err = scope.addTable(t.Name.String(), table) //.tables[t.Name.String()] = table
			} else {
				a.err = scope.addTable(table.As.String(), table)
			}
		}
	case *sqlparser.JoinTableExpr:
		a.analyzeTableExpr(table.LeftExpr)
		a.analyzeTableExpr(table.RightExpr)
	case *sqlparser.ParenTableExpr:
		a.analyzeTableExprs(table.Exprs)
	}
	return a.err == nil
}
