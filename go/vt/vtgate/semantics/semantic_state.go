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

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	table = *sqlparser.AliasedTableExpr
	// SemTable contains semantic analysis information about the query.
	SemTable struct {
		exprScope        map[sqlparser.Expr]*scope
		exprDependencies map[sqlparser.Expr][]table
	}
	// analyzer is a struct to work with analyzing the query.
	analyzer struct {
		scopes    []*scope
		exprScope map[sqlparser.Expr]*scope
		exprDeps  map[sqlparser.Expr][]table
		si        schemaInformation
		err       error
	}
	schemaInformation interface {
		FindTable(tablename sqlparser.TableName) (*vindexes.Table, error)
	}
)

func (t *SemTable) scope(expr sqlparser.Expr) *scope {
	return t.exprScope[expr]
}

func (t *SemTable) dependencies(expr sqlparser.Expr) []table {
	return t.exprDependencies[expr]
}

// newAnalyzer create the semantic analyzer
func newAnalyzer(si schemaInformation) *analyzer {
	return &analyzer{
		exprScope: map[sqlparser.Expr]*scope{},
		exprDeps:  map[sqlparser.Expr][]table{},
		si:        si,
	}
}

// Analyse analyzes the parsed query.
func Analyse(statement sqlparser.Statement, si schemaInformation) (*SemTable, error) {
	analyzer := newAnalyzer(si)
	// Initial scope
	analyzer.push(newScope(nil))
	analyzer.analyze(statement)
	if analyzer.err != nil {
		return nil, analyzer.err
	}
	return &SemTable{exprScope: analyzer.exprScope, exprDependencies: analyzer.exprDeps}, nil
}

var debug = true

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
		sqlparser.Rewrite(stmt.SelectExprs, a.scopeExprs, a.bindExpr)
		sqlparser.Rewrite(stmt.Where, a.scopeExprs, a.bindExpr)
		sqlparser.Rewrite(stmt.OrderBy, a.scopeExprs, a.bindExpr)
		sqlparser.Rewrite(stmt.GroupBy, a.scopeExprs, a.bindExpr)
		sqlparser.Rewrite(stmt.Having, a.scopeExprs, a.bindExpr)
		sqlparser.Rewrite(stmt.Limit, a.scopeExprs, a.bindExpr)
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
