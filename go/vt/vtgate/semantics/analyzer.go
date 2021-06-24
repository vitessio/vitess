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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// analyzer is a struct to work with analyzing the query.
	analyzer struct {
		si SchemaInformation

		Tables    []*TableInfo
		scopes    []*scope
		exprDeps  map[sqlparser.Expr]TableSet
		err       error
		currentDb string

		selectScope map[*sqlparser.Select]*scope
	}
)

// newAnalyzer create the semantic analyzer
func newAnalyzer(dbName string, si SchemaInformation) *analyzer {
	return &analyzer{
		exprDeps:    map[sqlparser.Expr]TableSet{},
		selectScope: map[*sqlparser.Select]*scope{},
		currentDb:   dbName,
		si:          si,
	}
}

// Analyze analyzes the parsed query.
func Analyze(statement sqlparser.Statement, currentDb string, si SchemaInformation) (*SemTable, error) {
	analyzer := newAnalyzer(currentDb, si)
	// Initial scope
	err := analyzer.analyze(statement)
	if err != nil {
		return nil, err
	}
	return &SemTable{exprDependencies: analyzer.exprDeps, Tables: analyzer.Tables, selectScope: analyzer.selectScope}, nil
}

// analyzeDown pushes new scopes when we encounter sub queries,
// and resolves the table a column is using
func (a *analyzer) analyzeDown(cursor *sqlparser.Cursor) bool {
	// If we have an error we keep on going down the tree without checking for anything else
	// this way we can abort when we come back up.
	if !a.shouldContinue() {
		return true
	}
	current := a.currentScope()
	n := cursor.Node()
	switch node := n.(type) {
	case *sqlparser.Select:
		if node.Having != nil {
			a.err = Gen4NotSupportedF("HAVING")
		}
		a.push(newScope(current))
		a.selectScope[node] = a.currentScope()
	case *sqlparser.DerivedTable:
		a.err = Gen4NotSupportedF("derived tables")
	case *sqlparser.Subquery:
		a.err = Gen4NotSupportedF("subquery")
	case sqlparser.TableExpr:
		_, isSelect := cursor.Parent().(*sqlparser.Select)
		if isSelect {
			a.push(newScope(nil))
		}
		switch node := node.(type) {
		case *sqlparser.AliasedTableExpr:
			a.err = a.bindTable(node, node.Expr)
		case *sqlparser.JoinTableExpr:
			if node.Condition.Using != nil {
				a.err = vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: join with USING(column_list) clause for complex queries")
			}
			if node.Join == sqlparser.NaturalJoinType || node.Join == sqlparser.NaturalRightJoinType || node.Join == sqlparser.NaturalLeftJoinType {
				a.err = vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: "+node.Join.ToString())
			}

		}
	case *sqlparser.Union:
		a.push(newScope(current))
	case *sqlparser.ColName:
		t, err := a.resolveColumn(node, current)
		if err != nil {
			a.err = err
		} else {
			a.exprDeps[node] = t
		}
	case *sqlparser.FuncExpr:
		if node.Distinct {
			err := vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "syntax error: %s", sqlparser.String(node))
			if len(node.Exprs) != 1 {
				a.err = err
			} else if _, ok := node.Exprs[0].(*sqlparser.AliasedExpr); !ok {
				a.err = err
			}
		}
		if sqlparser.IsLockingFunc(node) {
			a.err = Gen4NotSupportedF("locking functions")
		}
	}

	// this is the visitor going down the tree. Returning false here would just not visit the children
	// to the current node, but that is not what we want if we have encountered an error.
	// In order to abort the whole visitation, we have to return true here and then return false in the `analyzeUp` method
	return true
}

func (a *analyzer) resolveColumn(colName *sqlparser.ColName, current *scope) (TableSet, error) {
	var t *TableInfo
	var err error
	if colName.Qualifier.IsEmpty() {
		t, err = a.resolveUnQualifiedColumn(current, colName)
	} else {
		t, err = a.resolveQualifiedColumn(current, colName)
	}
	if err != nil {
		return 0, err
	}
	return a.tableSetFor(t.ASTNode), nil
}

// resolveQualifiedColumn handles `tabl.col` expressions
func (a *analyzer) resolveQualifiedColumn(current *scope, expr *sqlparser.ColName) (*TableInfo, error) {
	// search up the scope stack until we find a match
	for current != nil {
		dbName := expr.Qualifier.Qualifier.String()
		tableName := expr.Qualifier.Name.String()
		for _, table := range current.tables {
			if tableName == table.tableName &&
				(dbName == table.dbName || (dbName == "" && (table.dbName == a.currentDb || a.currentDb == ""))) {
				return table, nil
			}
		}
		current = current.parent
	}
	return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "symbol %s not found", sqlparser.String(expr))
}

// resolveUnQualifiedColumn
func (a *analyzer) resolveUnQualifiedColumn(current *scope, expr *sqlparser.ColName) (*TableInfo, error) {
	if len(current.tables) == 1 {
		for _, tableExpr := range current.tables {
			return tableExpr, nil
		}
	}

	var tblInfo *TableInfo
	for _, tbl := range current.tables {
		if tbl.Table == nil {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUniqError, fmt.Sprintf("Column '%s' in field list is ambiguous", sqlparser.String(expr)))
		}
		for _, col := range tbl.Table.Columns {
			if expr.Name.Equal(col.Name) {
				if tblInfo != nil {
					return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUniqError, fmt.Sprintf("Column '%s' in field list is ambiguous", sqlparser.String(expr)))
				}
				tblInfo = tbl
			}
		}
	}
	return tblInfo, nil
}

func (a *analyzer) tableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	for i, t2 := range a.Tables {
		if t == t2.ASTNode {
			return TableSet(1 << i)
		}
	}
	panic("unknown table")
}

func (a *analyzer) bindTable(alias *sqlparser.AliasedTableExpr, expr sqlparser.SimpleTableExpr) error {
	switch t := expr.(type) {
	case *sqlparser.DerivedTable:
		return Gen4NotSupportedF("derived table")
	case sqlparser.TableName:
		if sqlparser.SystemSchema(t.Qualifier.String()) {
			return Gen4NotSupportedF("system tables")
		}
		tbl, vdx, _, _, _, err := a.si.FindTableOrVindex(t)
		if err != nil {
			return err
		}
		if tbl == nil && vdx != nil {
			return Gen4NotSupportedF("vindex in FROM")
		}
		scope := a.currentScope()
		dbName := t.Qualifier.String()
		if dbName == "" {
			dbName = a.currentDb
		}
		var tableName string
		if alias.As.IsEmpty() {
			tableName = t.Name.String()
		} else {
			tableName = alias.As.String()
		}
		table := &TableInfo{
			dbName:    dbName,
			tableName: tableName,
			ASTNode:   alias,
			Table:     tbl,
		}
		a.Tables = append(a.Tables, table)
		return scope.addTable(table)
	}
	return nil
}

func (a *analyzer) analyze(statement sqlparser.Statement) error {
	_ = sqlparser.Rewrite(statement, a.analyzeDown, a.analyzeUp)
	return a.err
}

func (a *analyzer) analyzeUp(cursor *sqlparser.Cursor) bool {
	if !a.shouldContinue() {
		return false
	}
	switch cursor.Node().(type) {
	case *sqlparser.Union, *sqlparser.Select:
		a.popScope()
	case sqlparser.TableExpr:
		_, isSelect := cursor.Parent().(*sqlparser.Select)
		if isSelect {
			curScope := a.currentScope()
			a.popScope()
			earlierScope := a.currentScope()
			// copy curScope into the earlierScope
			for _, table := range curScope.tables {
				err := earlierScope.addTable(table)
				if err != nil {
					a.err = err
					break
				}
			}
		}
	}
	return a.shouldContinue()
}

func (a *analyzer) shouldContinue() bool {
	return a.err == nil
}

func (a *analyzer) push(s *scope) {
	a.scopes = append(a.scopes, s)
}

func (a *analyzer) popScope() {
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

// Gen4NotSupportedF returns a common error for shortcomings in the gen4 planner
func Gen4NotSupportedF(format string, args ...interface{}) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "gen4 does not yet support: "+format, args...)
}
