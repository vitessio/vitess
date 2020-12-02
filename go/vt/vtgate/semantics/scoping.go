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
		parent *scope
		tables map[string]*sqlparser.AliasedTableExpr
	}
)

func newScope(parent *scope) *scope {
	return &scope{tables: map[string]*sqlparser.AliasedTableExpr{}, parent: parent}
}

func (s *scope) addTable(name string, table *sqlparser.AliasedTableExpr) error {
	_, found := s.tables[name]
	if found {
		return mysql.NewSQLError(mysql.ERNonUniqTable, mysql.SSSyntaxErrorOrAccessViolation, "Not unique table/alias: '%s'", name)
	}
	s.tables[name] = table
	return nil
}

func (a *analyzer) scopeUp(n sqlparser.SQLNode) {
	switch n.(type) {
	case *sqlparser.Subquery, *sqlparser.Select:
		a.popScope()
	}
}

func (a *analyzer) scopeDown(n sqlparser.SQLNode) (bool, error) {
	current := a.currentScope()
	log(n, "%p scopeDown %T", current, n)
	switch node := n.(type) {
	case *sqlparser.Select:
		a.push(newScope(current))
		for _, tableExpr := range node.From {
			if err := a.analyzeTableExpr(tableExpr); err != nil {
				return false, err
			}
		}
	case *sqlparser.TableExprs:
		// this has already been visited when we encountered the SELECT struct
		return false, nil

	case *sqlparser.Subquery:
		a.exprScope[node] = current
		a.push(newScope(current))

	case sqlparser.Expr:
		a.exprScope[node] = current
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
