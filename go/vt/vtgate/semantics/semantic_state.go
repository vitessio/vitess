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

// Analyzer is a struct to work with analyzing the query.
type Analyzer struct {
	scopes           []*scope
	exprScope        map[sqlparser.Expr]*scope
	exprDependencies map[sqlparser.Expr][]*sqlparser.AliasedTableExpr
	err              error
}

// NewAnalyzer create the semantic Analyzer
func NewAnalyzer() *Analyzer {
	return &Analyzer{
		exprScope:        map[sqlparser.Expr]*scope{},
		exprDependencies: map[sqlparser.Expr][]*sqlparser.AliasedTableExpr{},
	}
}

// Analyse analyzes the parsed query.
func Analyse(statement sqlparser.Statement) (*SemTable, error) {
	analyzer := NewAnalyzer()
	// Initial scope
	analyzer.push(newScope())
	analyzer.analyze(statement)
	if analyzer.err != nil {
		return nil, analyzer.err
	}
	return &SemTable{exprScope: analyzer.exprScope, exprDependencies: analyzer.exprDependencies}, nil
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
func (a *Analyzer) analyze(statement sqlparser.Statement) {
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

func (a *Analyzer) scopeExprs(cursor *sqlparser.Cursor) bool {
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

func (a *Analyzer) bindExprs(cursor *sqlparser.Cursor) bool {
	n := cursor.Node()
	current := a.peek()
	log(n, "%d bindExprs %T", current.i, n)
	switch expr := n.(type) {
	case *sqlparser.ColName:
		qualifier := expr.Qualifier.Name.String()
		tableExpr := current.tables[qualifier]
		a.exprDependencies[expr] = []*sqlparser.AliasedTableExpr{tableExpr}
	case *sqlparser.BinaryExpr:
		a.exprDependencies[expr] = append(a.exprDependencies[expr.Left], a.exprDependencies[expr.Right]...)
	}
	return a.err == nil
}
func (a *Analyzer) analyzeTableExprs(tablExprs sqlparser.TableExprs) {
	for _, tableExpr := range tablExprs {
		a.analyzeTableExpr(tableExpr)
	}
}

func (a *Analyzer) analyzeTableExpr(tableExpr sqlparser.TableExpr) bool {
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

func (a *Analyzer) push(s *scope) {
	a.scopes = append(a.scopes, s)
}

func (a *Analyzer) pop() {
	l := len(a.scopes) - 1
	a.scopes = a.scopes[:l]
}

func (a *Analyzer) peek() *scope {
	return a.scopes[len(a.scopes)-1]
}
