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
	SemTable struct {
		exprScope  map[sqlparser.Expr]*scope
		outerScope *scope
	}
	scope struct {
		i int
	}
)

var i = 0

func newScope() *scope {
	i++
	return &scope{i: i}
}

func (t *SemTable) scope(expr sqlparser.Expr) *scope {
	return t.exprScope[expr]
}

type analyzer struct {
	exprScope map[sqlparser.Expr]*scope
	scopes    []*scope
}

func NewAnalyzer() *analyzer {
	return &analyzer{
		exprScope: map[sqlparser.Expr]*scope{},
	}
}

func Analyse(statement sqlparser.Statement) (*SemTable, error) {
	analyzer := NewAnalyzer()
	// Initial scope
	analyzer.push(newScope())
	err := analyzer.analyze(statement)
	if err != nil {
		return nil, err
	}
	return &SemTable{outerScope: analyzer.peek(), exprScope: analyzer.exprScope}, nil
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
func (a *analyzer) analyze(statement sqlparser.Statement) error {
	log(statement, "analyse %T", statement)
	switch stmt := statement.(type) {
	case *sqlparser.Select:
		sqlparser.Rewrite(stmt.SelectExprs, a.analyzeExprs, nil)
		sqlparser.Rewrite(stmt.Where, a.analyzeExprs, nil)
		sqlparser.Rewrite(stmt.OrderBy, a.analyzeExprs, nil)
		sqlparser.Rewrite(stmt.GroupBy, a.analyzeExprs, nil)
		sqlparser.Rewrite(stmt.Having, a.analyzeExprs, nil)
		sqlparser.Rewrite(stmt.Limit, a.analyzeExprs, nil)
		for _, tableExpr := range stmt.From {
			a.analyzeTableExpr(tableExpr)
		}
	}
	return nil
}

func (a *analyzer) analyzeExprs(cursor *sqlparser.Cursor) bool {
	n := cursor.Node()
	current := a.peek()
	log(n, "%d analyzeExprs %T", current.i, n)
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

func (a *analyzer) analyzeTableExprs(tablExprs sqlparser.TableExprs) {
	for _, tableExpr := range tablExprs {
		a.analyzeTableExpr(tableExpr)
	}
}

func (a *analyzer) analyzeTableExpr(tableExpr sqlparser.TableExpr) bool {
	log(tableExpr, "analyzeTableExpr %T", tableExpr)
	switch table := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		a.analyzeSimpleTableExpr(table.Expr)
	case *sqlparser.JoinTableExpr:
		a.analyzeTableExpr(table.LeftExpr)
		a.analyzeTableExpr(table.RightExpr)
	case *sqlparser.ParenTableExpr:
		a.analyzeTableExprs(table.Exprs)
	}
	return true
}

func (a *analyzer) analyzeSimpleTableExpr(expr sqlparser.SimpleTableExpr) {
	log(expr, "analyzeSimpleTableExpr %T", expr)
	dt, derived := expr.(*sqlparser.DerivedTable)
	if derived {
		a.push(newScope())
		a.analyze(dt.Select)
		a.pop()
	}
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
