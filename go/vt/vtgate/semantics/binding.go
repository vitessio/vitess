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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

func (a *analyzer) bindUp(n sqlparser.SQLNode, childrenState []interface{}) (interface{}, error) {
	current := a.currentScope()
	log(n, "%p bindUp %T", current, n)

	deps := collectChildrenDeps(childrenState)

	switch expr := n.(type) {
	case *sqlparser.ColName:
		var err error
		var t table
		if expr.Qualifier.IsEmpty() {
			t, err = a.resolveUnQualifiedColumn(current, expr)
		} else {
			t, err = a.resolveQualifiedColumn(current, expr)
		}
		if err != nil {
			return nil, err
		}
		deps = append(deps, t)
		a.exprDeps[expr] = deps
	case sqlparser.Expr:
		a.exprDeps[expr] = deps
	}

	return deps, nil
}

func collectChildrenDeps(childrenState []interface{}) []table {
	// if we have a single child, it's dependencies is all we need. we can cut short here
	if len(childrenState) == 1 {
		return childrenState[0].([]table)
	}

	type Void struct{}
	var void Void
	resultMap := map[table]Void{}
	// adding dependencies through a map to make them unique
	for _, d := range childrenState {
		dependencies := d.([]table)
		for _, table := range dependencies {
			resultMap[table] = void
		}
	}
	var deps []table
	for t := range resultMap {
		deps = append(deps, t)
	}
	return deps
}

// resolveQualifiedColumn handles `tabl.col` expressions
func (a *analyzer) resolveQualifiedColumn(current *scope, expr *sqlparser.ColName) (table, error) {
	qualifier := expr.Qualifier.Name.String()

	for current != nil {
		tableExpr, found := current.tables[qualifier]
		if found {
			return tableExpr, nil
		}
		current = current.parent
	}

	return nil, mysql.NewSQLError(mysql.ERBadFieldError, mysql.SSBadFieldError, "Unknown column '%s'", sqlparser.String(expr))
}

// resolveUnQualifiedColumn
func (a *analyzer) resolveUnQualifiedColumn(current *scope, expr *sqlparser.ColName) (table, error) {
	if len(current.tables) == 1 {
		for _, tableExpr := range current.tables {
			return tableExpr, nil
		}
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "todo - figure out which table this column belongs to")
}

func (a *analyzer) bindTable(alias *sqlparser.AliasedTableExpr, expr sqlparser.SimpleTableExpr) error {
	switch t := expr.(type) {
	case *sqlparser.DerivedTable:
		a.push(newScope(nil))
		if _, err := a.analyze(t.Select); err != nil {
			return err
		}
		a.popScope()
		scope := a.currentScope()
		return scope.addTable(alias.As.String(), alias)
	case sqlparser.TableName:
		scope := a.currentScope()
		if alias.As.IsEmpty() {
			return scope.addTable(t.Name.String(), alias)
		}

		return scope.addTable(alias.As.String(), alias)
	}
	return nil
}
