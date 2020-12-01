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

func (a *analyzer) scopeExprs(n sqlparser.SQLNode) (bool, error) {
	current := a.peek()
	log(n, "%d scopeExprs %T", current.i, n)
	switch expr := n.(type) {
	case *sqlparser.Subquery:
		a.exprScope[expr] = current
		a.push(newScope(current))
		if err := a.analyze(expr.Select); err != nil {
			return false, err
		}
		a.pop()
		return false, nil
	case sqlparser.Expr:
		a.exprScope[expr] = current
	}
	return true, nil
}

func (a *analyzer) analyzeTableExprs(tablExprs sqlparser.TableExprs) error {
	for _, tableExpr := range tablExprs {
		if err := a.analyzeTableExpr(tableExpr); err != nil {
			return err
		}
	}
	return nil
}

func (a *analyzer) analyzeTableExpr(tableExpr sqlparser.TableExpr) error {
	log(tableExpr, "analyzeTableExpr %T", tableExpr)
	switch table := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return a.bindTable(table, table.Expr)
	case *sqlparser.JoinTableExpr:
		if err := a.analyzeTableExpr(table.LeftExpr); err != nil {
			return err
		}
		if err := a.analyzeTableExpr(table.RightExpr); err != nil {
			return err
		}
	case *sqlparser.ParenTableExpr:
		return a.analyzeTableExprs(table.Exprs)
	}
	return nil
}
