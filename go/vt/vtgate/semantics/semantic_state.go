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
	"fmt"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	// SemTable contains semantic analysis information about the query.
	SemTable struct {
		exprScope        map[sqlparser.Expr]*scope
		exprDependencies map[sqlparser.Expr][]*sqlparser.AliasedTableExpr
	}
	scope struct {
		i      int
		tables map[string]*sqlparser.AliasedTableExpr
	}
)

var i = 0

func newScope() *scope {
	i++
	return &scope{i: i, tables: map[string]*sqlparser.AliasedTableExpr{}}
}

func (t *SemTable) scope(expr sqlparser.Expr) *scope {
	return t.exprScope[expr]
}

func (t *SemTable) dependencies(expr sqlparser.Expr) []*sqlparser.AliasedTableExpr {
	return t.exprDependencies[expr]
}

// analyzer is a struct to work with analyzing the query.
type analyzer struct {
	scopes    []*scope
	exprScope map[sqlparser.Expr]*scope
	exprDeps  map[sqlparser.Expr][]*sqlparser.AliasedTableExpr
	si        schemaInformation
	err       error
}

type schemaInformation interface {
	FindTable(tablename sqlparser.TableName) (*vindexes.Table, error)
}

// newAnalyzer create the semantic analyzer
func newAnalyzer(si schemaInformation) *analyzer {
	return &analyzer{
		exprScope: map[sqlparser.Expr]*scope{},
		exprDeps:  map[sqlparser.Expr][]*sqlparser.AliasedTableExpr{},
		si:        si,
	}
}

// Analyse analyzes the parsed query.
func Analyse(statement sqlparser.Statement, si schemaInformation) (*SemTable, error) {
	analyzer := newAnalyzer(si)
	// Initial scope
	analyzer.push(newScope())
	analyzer.analyze(statement)
	if analyzer.err != nil {
		return nil, analyzer.err
	}
	return &SemTable{exprScope: analyzer.exprScope, exprDependencies: analyzer.exprDeps}, nil
}

var debug = false

func log(node sqlparser.SQLNode, format string, args ...interface{}) {
	if debug {
		fmt.Printf(format, args...)
		if node == nil {
			fmt.Println()
		} else {
			fmt.Println(" - " + sqlparser.String(node))
		}
	}
}
func (a *analyzer) analyze(statement sqlparser.Statement) {
	log(statement, "analyse %T", statement)
	switch stmt := statement.(type) {
	case *sqlparser.Select:
		for _, tableExpr := range stmt.From {
			a.analyzeTableExpr(tableExpr)
		}
		sqlparser.Rewrite(stmt.SelectExprs, a.scopeExprs, a.bindExprs)
		sqlparser.Rewrite(stmt.Where, a.scopeExprs, a.bindExprs)
		sqlparser.Rewrite(stmt.OrderBy, a.scopeExprs, a.bindExprs)
		sqlparser.Rewrite(stmt.GroupBy, a.scopeExprs, a.bindExprs)
		sqlparser.Rewrite(stmt.Having, a.scopeExprs, a.bindExprs)
		sqlparser.Rewrite(stmt.Limit, a.scopeExprs, a.bindExprs)
	}
}

func (a *analyzer) scopeExprs(cursor *sqlparser.Cursor) bool {
	n := cursor.Node()
	current := a.peek()
	log(n, "%d scopeExprs %T", current.i, n)
	switch expr := n.(type) {
	case *sqlparser.Subquery:
		a.exprScope[expr] = current
		a.push(newScope())
		a.analyze(expr.Select)
		a.pop()
		return false
	case sqlparser.Expr:
		a.exprScope[expr] = current
	}
	return true
}

func (a *analyzer) bindExprs(cursor *sqlparser.Cursor) bool {
	n := cursor.Node()
	current := a.peek()
	log(n, "%d bindExprs %T", current.i, n)
	switch expr := n.(type) {
	case *sqlparser.ColName:
		if expr.Qualifier.IsEmpty() {
			// try to guess which table this column belongs to
			a.resolveUnQualifiedColumn(current, expr)
			return true
		}

		a.err = a.resolveQualifiedColumn(expr, current)
	case *sqlparser.BinaryExpr:
		a.exprDeps[expr] = append(a.exprDeps[expr.Left], a.exprDeps[expr.Right]...)
	}

	return a.err == nil
}

func (a *analyzer) resolveQualifiedColumn(expr *sqlparser.ColName, current *scope) error {
	qualifier := expr.Qualifier.Name.String()
	tableExpr, found := current.tables[qualifier]
	if found {
		a.exprDeps[expr] = []*sqlparser.AliasedTableExpr{tableExpr}
		return nil
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

			a.push(newScope())
			a.analyze(t.Select)
			a.pop()
			scope := a.peek()
			scope.tables[table.As.String()] = table
		case sqlparser.TableName:
			scope := a.peek()
			if table.As.IsEmpty() {
				scope.tables[t.Name.String()] = table
			} else {
				scope.tables[table.As.String()] = table
			}
		}
	case *sqlparser.JoinTableExpr:
		a.analyzeTableExpr(table.LeftExpr)
		a.analyzeTableExpr(table.RightExpr)
	case *sqlparser.ParenTableExpr:
		a.analyzeTableExprs(table.Exprs)
	}
	return true
}

func (a *analyzer) push(s *scope) {
	a.scopes = append(a.scopes, s)
}

func (a *analyzer) pop() {
	l := len(a.scopes) - 1
	a.scopes = a.scopes[:l]
}

func (a *analyzer) peek() *scope {
	return a.scopes[len(a.scopes)-1]
}
