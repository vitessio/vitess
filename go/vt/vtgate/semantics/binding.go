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

func (a *analyzer) bindExpr(cursor *sqlparser.Cursor) bool {
	n := cursor.Node()
	current := a.peek()
	log(n, "%d bindExpr %T", current.i, n)
	switch expr := n.(type) {
	case *sqlparser.ColName:
		if expr.Qualifier.IsEmpty() {
			// try to guess which table this column belongs to
			a.resolveUnQualifiedColumn(current, expr)
			return true
		}
		a.err = a.resolveQualifiedColumn(current, expr)
	case *sqlparser.BinaryExpr:
		deps := a.exprDeps[expr.Left]
		for _, t1 := range a.exprDeps[expr.Right] {
			found := false
			for _, t2 := range deps {
				if t1 == t2 {
					found = true
					break
				}
			}
			if !found {
				deps = append(deps, t1)
			}
		}
		a.exprDeps[expr] = deps
	}

	return a.err == nil
}

func (a *analyzer) resolveQualifiedColumn(current *scope, expr *sqlparser.ColName) error {
	qualifier := expr.Qualifier.Name.String()

	for current != nil {
		tableExpr, found := current.tables[qualifier]
		if found {
			a.exprDeps[expr] = []*sqlparser.AliasedTableExpr{tableExpr}
			return nil
		}
		current = current.parent
	}

	return mysql.NewSQLError(mysql.ERBadFieldError, mysql.SSBadFieldError, "Unknown column '%s'", sqlparser.String(expr))
}

func (a *analyzer) resolveUnQualifiedColumn(current *scope, expr *sqlparser.ColName) {
	if len(current.tables) == 1 {
		for _, tableExpr := range current.tables {
			a.exprDeps[expr] = []*sqlparser.AliasedTableExpr{tableExpr}
		}
	}
}

func (a *analyzer) bindTable(alias *sqlparser.AliasedTableExpr, expr sqlparser.SimpleTableExpr) error {
	switch t := expr.(type) {
	case *sqlparser.DerivedTable:
		a.push(newScope(nil))
		a.analyze(t.Select)
		a.pop()
		scope := a.peek()
		return scope.addTable(alias.As.String(), alias)
	case sqlparser.TableName:
		scope := a.peek()
		if alias.As.IsEmpty() {
			return scope.addTable(t.Name.String(), alias)
		}

		return scope.addTable(alias.As.String(), alias)
	}
	return nil
}
