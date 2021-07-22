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
	"runtime/debug"
	"strconv"
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// analyzer is a struct to work with analyzing the query.
	analyzer struct {
		si SchemaInformation

		Tables       []TableInfo
		scopes       []*scope
		exprDeps     ExprDependencies
		exprTypes    map[sqlparser.Expr]querypb.Type
		err          error
		currentDb    string
		inProjection []bool

		rScope  map[*sqlparser.Select]*scope
		wScope  map[*sqlparser.Select]*scope
		projErr error
	}
)

// newAnalyzer create the semantic analyzer
func newAnalyzer(dbName string, si SchemaInformation) *analyzer {
	return &analyzer{
		exprDeps:  map[sqlparser.Expr]TableSet{},
		exprTypes: map[sqlparser.Expr]querypb.Type{},
		rScope:    map[*sqlparser.Select]*scope{},
		wScope:    map[*sqlparser.Select]*scope{},
		currentDb: dbName,
		si:        si,
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
	return &SemTable{
		exprDependencies: analyzer.exprDeps,
		exprTypes:        analyzer.exprTypes,
		Tables:           analyzer.Tables,
		selectScope:      analyzer.rScope,
		ProjectionErr:    analyzer.projErr,
	}, nil
}

func (a *analyzer) setError(err error) {
	if len(a.inProjection) > 0 && vterrors.ErrState(err) == vterrors.NonUniqError {
		a.projErr = err
	} else {
		a.err = err
	}
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
			a.setError(Gen4NotSupportedF("HAVING"))
		}

		currScope := newScope(current)
		a.push(currScope)

		// Needed for order by with Literal to find the Expression.
		currScope.selectExprs = node.SelectExprs

		a.rScope[node] = currScope
		a.wScope[node] = newScope(nil)
	case *sqlparser.Subquery:
		a.setError(Gen4NotSupportedF("subquery"))
	case sqlparser.TableExpr:
		if isParentSelect(cursor) {
			a.push(newScope(nil))
		}
	case *sqlparser.Union:
		a.push(newScope(current))
	case sqlparser.SelectExprs:
		sel, ok := cursor.Parent().(*sqlparser.Select)
		if !ok {
			break
		}

		a.inProjection = append(a.inProjection, true)
		wScope, exists := a.wScope[sel]
		if !exists {
			break
		}

		wScope.tables = append(wScope.tables, a.createVTableInfoForExpressions(node))
	case sqlparser.OrderBy:
		a.changeScopeForOrderBy(cursor)
	case *sqlparser.Order:
		a.analyzeOrderByGroupByExprForLiteral(node.Expr, "order clause")
	case sqlparser.GroupBy:
		a.changeScopeForOrderBy(cursor)
		for _, grpExpr := range node {
			a.analyzeOrderByGroupByExprForLiteral(grpExpr, "group statement")
		}
	case *sqlparser.ColName:
		ts, qt, err := a.resolveColumn(node, current)
		if err != nil {
			a.setError(err)
		} else {
			a.exprDeps[node] = ts
			if qt != nil {
				a.exprTypes[node] = *qt
			}
		}
	case *sqlparser.FuncExpr:
		if node.Distinct {
			err := vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "syntax error: %s", sqlparser.String(node))
			if len(node.Exprs) != 1 {
				a.setError(err)
			} else if _, ok := node.Exprs[0].(*sqlparser.AliasedExpr); !ok {
				a.setError(err)
			}
		}
		if sqlparser.IsLockingFunc(node) {
			a.setError(Gen4NotSupportedF("locking functions"))
		}
	}

	// this is the visitor going down the tree. Returning false here would just not visit the children
	// to the current node, but that is not what we want if we have encountered an error.
	// In order to abort the whole visitation, we have to return true here and then return false in the `analyzeUp` method
	return true
}

func (a *analyzer) analyzeOrderByGroupByExprForLiteral(input sqlparser.Expr, caller string) {
	l, ok := input.(*sqlparser.Literal)
	if !ok {
		return
	}
	if l.Type != sqlparser.IntVal {
		return
	}
	currScope := a.currentScope()
	num, err := strconv.Atoi(l.Val)
	if err != nil {
		a.err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "error parsing column number: %s", l.Val)
		return
	}
	if num < 1 || num > len(currScope.selectExprs) {
		a.err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "Unknown column '%d' in '%s'", num, caller)
		return
	}

	expr, ok := currScope.selectExprs[num-1].(*sqlparser.AliasedExpr)
	if !ok {
		return
	}

	var deps TableSet
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		expr, ok := node.(sqlparser.Expr)
		if ok {
			deps = deps.Merge(a.exprDeps[expr])
		}
		return true, nil
	}, expr.Expr)

	a.exprDeps[input] = deps
}

func (a *analyzer) changeScopeForOrderBy(cursor *sqlparser.Cursor) {
	sel, ok := cursor.Parent().(*sqlparser.Select)
	if !ok {
		return
	}
	// In ORDER BY, we can see both the scope in the FROM part of the query, and the SELECT columns created
	// so before walking the rest of the tree, we change the scope to match this behaviour
	incomingScope := a.currentScope()
	nScope := newScope(incomingScope)
	a.push(nScope)
	wScope := a.wScope[sel]
	nScope.tables = append(nScope.tables, wScope.tables...)
	nScope.selectExprs = incomingScope.selectExprs

	if a.rScope[sel] != incomingScope {
		panic("BUG: scope counts did not match")
	}
}

func isParentSelect(cursor *sqlparser.Cursor) bool {
	_, isSelect := cursor.Parent().(*sqlparser.Select)
	return isSelect
}

func (a *analyzer) resolveColumn(colName *sqlparser.ColName, current *scope) (TableSet, *querypb.Type, error) {
	if colName.Qualifier.IsEmpty() {
		return a.resolveUnQualifiedColumn(current, colName)
	}
	t, err := a.resolveQualifiedColumn(current, colName)
	if t == nil || err != nil {
		return 0, nil, err
	}
	tableSet := a.tableSetFor(t.GetExpr())
	for _, colInfo := range t.GetColumns() {
		if colName.Name.EqualString(colInfo.Name) {
			// A column can't be of type NULL, that is the default value indicating that we dont know the actual type
			// But expressions can be of NULL type, so we use nil to represent an unknown type
			if colInfo.Type == querypb.Type_NULL_TYPE {
				return tableSet, nil, nil
			}
			return tableSet, &colInfo.Type, nil
		}
	}
	return tableSet, nil, nil
}

// tableInfoFor returns the table info for the table set. It should contains only single table.
func (a *analyzer) tableInfoFor(id TableSet) (TableInfo, error) {
	numberOfTables := id.NumberOfTables()
	if numberOfTables == 0 {
		return nil, nil
	}
	if numberOfTables > 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should only be used for single tables")
	}
	return a.Tables[id.TableOffset()], nil
}

// resolveQualifiedColumn handles `tabl.col` expressions
func (a *analyzer) resolveQualifiedColumn(current *scope, expr *sqlparser.ColName) (TableInfo, error) {
	// search up the scope stack until we find a match
	for current != nil {
		for _, table := range current.tables {
			if table.Matches(expr.Qualifier) {
				if table.IsActualTable() {
					return table, nil
				}
				newExpr := *expr
				newExpr.Qualifier = sqlparser.TableName{}
				ts, _, err := table.DepsFor(&newExpr, a, len(current.tables) == 1)
				if err != nil {
					return nil, err
				}
				if ts == nil {
					return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "symbol %s not found", sqlparser.String(expr))
				}
				return a.tableInfoFor(*ts)
			}
		}
		current = current.parent
	}
	return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "symbol %s not found", sqlparser.String(expr))
}

type originable interface {
	tableSetFor(t *sqlparser.AliasedTableExpr) TableSet
	depsForExpr(expr sqlparser.Expr) (TableSet, *querypb.Type)
}

func (a *analyzer) depsForExpr(expr sqlparser.Expr) (TableSet, *querypb.Type) {
	ts := a.exprDeps.Dependencies(expr)
	qt, isFound := a.exprTypes[expr]
	if !isFound {
		return ts, nil
	}
	return ts, &qt
}

// resolveUnQualifiedColumn
func (a *analyzer) resolveUnQualifiedColumn(current *scope, expr *sqlparser.ColName) (TableSet, *querypb.Type, error) {
	var tsp *TableSet
	var typp *querypb.Type

	for current != nil && tsp == nil {
		for _, tbl := range current.tables {
			ts, typ, err := tbl.DepsFor(expr, a, len(current.tables) == 1)
			if err != nil {
				return 0, nil, err
			}
			if ts != nil && tsp != nil {
				return 0, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUniqError, fmt.Sprintf("Column '%s' in field list is ambiguous", sqlparser.String(expr)))
			}
			if ts != nil {
				tsp = ts
				typp = typ
			}
		}

		current = current.parent
	}

	if tsp == nil {
		return 0, nil, nil
	}
	return *tsp, typp, nil
}

func (a *analyzer) tableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	for i, t2 := range a.Tables {
		if t == t2.GetExpr() {
			return TableSet(1 << i)
		}
	}
	panic("unknown table")
}

func (a *analyzer) createTable(t sqlparser.TableName, alias *sqlparser.AliasedTableExpr, tbl *vindexes.Table, isInfSchema bool) TableInfo {
	dbName := t.Qualifier.String()
	if dbName == "" {
		dbName = a.currentDb
	}
	if alias.As.IsEmpty() {
		return &RealTable{
			dbName:      dbName,
			tableName:   t.Name.String(),
			ASTNode:     alias,
			Table:       tbl,
			isInfSchema: isInfSchema,
		}
	}
	return &AliasedTable{
		tableName:   alias.As.String(),
		ASTNode:     alias,
		Table:       tbl,
		isInfSchema: isInfSchema,
	}
}

func (a *analyzer) bindTable(alias *sqlparser.AliasedTableExpr, expr sqlparser.SimpleTableExpr) error {
	switch t := expr.(type) {
	case *sqlparser.DerivedTable:
		sel, isSelect := t.Select.(*sqlparser.Select)
		if !isSelect {
			return Gen4NotSupportedF("union in derived table")
		}

		tableInfo := a.createVTableInfoForExpressions(sel.SelectExprs)
		tableInfo.ASTNode = alias
		tableInfo.tableName = alias.As.String()

		a.Tables = append(a.Tables, tableInfo)
		scope := a.currentScope()
		return scope.addTable(tableInfo)
	case sqlparser.TableName:
		var tbl *vindexes.Table
		var isInfSchema bool
		if sqlparser.SystemSchema(t.Qualifier.String()) {
			isInfSchema = true
		} else {
			table, vdx, _, _, _, err := a.si.FindTableOrVindex(t)
			if err != nil {
				return err
			}
			tbl = table
			if tbl == nil && vdx != nil {
				return Gen4NotSupportedF("vindex in FROM")
			}
		}
		scope := a.currentScope()
		tableInfo := a.createTable(t, alias, tbl, isInfSchema)

		a.Tables = append(a.Tables, tableInfo)
		return scope.addTable(tableInfo)
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
	switch node := cursor.Node().(type) {
	case sqlparser.SelectExprs:
		if isParentSelect(cursor) {
			a.popProjection()
		}
	case *sqlparser.Union, *sqlparser.Select, sqlparser.OrderBy, sqlparser.GroupBy:
		a.popScope()
	case sqlparser.TableExpr:
		if isParentSelect(cursor) {
			curScope := a.currentScope()
			a.popScope()
			earlierScope := a.currentScope()
			// copy curScope into the earlierScope
			for _, table := range curScope.tables {
				err := earlierScope.addTable(table)
				if err != nil {
					a.setError(err)
					break
				}
			}
		}
		switch node := node.(type) {
		case *sqlparser.AliasedTableExpr:
			a.setError(a.bindTable(node, node.Expr))
		case *sqlparser.JoinTableExpr:
			if node.Condition.Using != nil {
				a.setError(vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: join with USING(column_list) clause for complex queries"))
			}
			if node.Join == sqlparser.NaturalJoinType || node.Join == sqlparser.NaturalRightJoinType || node.Join == sqlparser.NaturalLeftJoinType {
				a.setError(vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: "+node.Join.ToString()))
			}
		}
	}

	return a.shouldContinue()
}

func (a *analyzer) createVTableInfoForExpressions(expressions sqlparser.SelectExprs) *vTableInfo {
	vTbl := &vTableInfo{}
	for _, selectExpr := range expressions {
		expr, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		vTbl.cols = append(vTbl.cols, expr.Expr)
		if expr.As.IsEmpty() {
			switch expr := expr.Expr.(type) {
			case *sqlparser.ColName:
				// for projections, we strip out the qualifier and keep only the column name
				vTbl.columnNames = append(vTbl.columnNames, expr.Name.String())
			default:
				vTbl.columnNames = append(vTbl.columnNames, sqlparser.String(expr))
			}
		} else {
			vTbl.columnNames = append(vTbl.columnNames, expr.As.String())
		}
	}
	return vTbl
}

func (a *analyzer) popProjection() {
	a.inProjection = a.inProjection[:len(a.inProjection)-1]
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
	message := fmt.Sprintf("gen4 does not yet support: "+format, args...)

	// add the line that this happens in so it is easy to find it
	stack := string(debug.Stack())
	lines := strings.Split(stack, "\n")
	message += "\n" + lines[6]
	return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, message)
}
