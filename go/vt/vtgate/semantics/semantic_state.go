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

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

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
	}
	schemaInformation interface {
		FindTable(tablename sqlparser.TableName) (*vindexes.Table, error)
	}
)

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
	//analyzer.push(newScope(nil))
	_, err := analyzer.analyze(statement)
	if err != nil {
		return nil, err
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

func (a *analyzer) analyze(statement sqlparser.Statement) ([]table, error) {
	log(statement, "analyse %T", statement)
	deps, err := sqlparser.VisitWithState(statement, a.scopeDown, a.analyzeUp)
	if err != nil {
		return nil, err
	}
	tables, ok := deps.([]table)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "bug: got unknown content from AST traversal: %T", deps)
	}

	return tables, nil
}

func (a *analyzer) analyzeUp(n sqlparser.SQLNode, childrenState []interface{}) (interface{}, error) {
	a.scopeUp(n)
	return a.bindUp(n, childrenState)
}

func (a *analyzer) push(s *scope) {
	log(nil, "enter new scope")
	a.scopes = append(a.scopes, s)
}

func (a *analyzer) popScope() {
	log(nil, "exit scope")
	l := len(a.scopes) - 1
	a.scopes = a.scopes[:l]
}

func (a *analyzer) currentScope() *scope {
	size := len(a.scopes)
	if size == 0 {
		return nil
	}
	return a.scopes[size-1]
}
