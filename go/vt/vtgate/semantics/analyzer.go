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
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// analyzer is a struct to work with analyzing the query.
	analyzer struct {
		scoper *scoper
		tables *tableCollector
		binder *binder

		exprTypes    map[sqlparser.Expr]querypb.Type
		err          error
		inProjection []bool

		projErr error
	}
)

// newAnalyzer create the semantic analyzer
func newAnalyzer(dbName string, si SchemaInformation) *analyzer {
	s := newScoper()
	a := &analyzer{
		exprTypes: map[sqlparser.Expr]querypb.Type{},
		scoper:    s,
		tables:    newTableCollector(s, si, dbName),
	}

	a.binder = newBinder(s, a, a.tables)

	return a
}

// Analyze analyzes the parsed query.
func Analyze(statement sqlparser.SelectStatement, currentDb string, si SchemaInformation) (*SemTable, error) {
	analyzer := newAnalyzer(currentDb, si)
	// Initial scope
	err := analyzer.analyze(statement)
	if err != nil {
		return nil, err
	}
	return &SemTable{
		ExprBaseTableDeps: analyzer.binder.exprRecursiveDeps,
		ExprDeps:          analyzer.binder.exprDeps,
		exprTypes:         analyzer.exprTypes,
		Tables:            analyzer.tables.Tables,
		selectScope:       analyzer.scoper.rScope,
		ProjectionErr:     analyzer.projErr,
		Comments:          statement.GetComments(),
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

	if err := checkForInvalidConstructs(cursor); err != nil {
		a.setError(err)
		return true
	}

	a.scoper.down(cursor)

	if err := a.binder.down(cursor); err != nil {
		a.setError(err)
		return true
	}

	n := cursor.Node()
	switch node := n.(type) {
	case sqlparser.SelectExprs:
		if !isParentSelect(cursor) {
			break
		}

		a.inProjection = append(a.inProjection, true)
	case *sqlparser.ColName:
		tsRecursive, ts, qt, err := a.resolveColumn(node, a.scoper.currentScope())
		if err != nil {
			a.setError(err)
		} else {
			a.binder.exprRecursiveDeps[node] = tsRecursive
			a.binder.exprDeps[node] = ts
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
	}

	// this is the visitor going down the tree. Returning false here would just not visit the children
	// to the current node, but that is not what we want if we have encountered an error.
	// In order to abort the whole visitation, we have to return true here and then return false in the `analyzeUp` method
	return true
}

func isParentSelect(cursor *sqlparser.Cursor) bool {
	_, isSelect := cursor.Parent().(*sqlparser.Select)
	return isSelect
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
	return a.tables.Tables[id.TableOffset()], nil
}

type originable interface {
	tableSetFor(t *sqlparser.AliasedTableExpr) TableSet
	depsForExpr(expr sqlparser.Expr) (TableSet, *querypb.Type)
}

func (a *analyzer) depsForExpr(expr sqlparser.Expr) (TableSet, *querypb.Type) {
	ts := a.binder.exprRecursiveDeps.Dependencies(expr)
	qt, isFound := a.exprTypes[expr]
	if !isFound {
		return ts, nil
	}
	return ts, &qt
}

func (v *vTableInfo) checkForDuplicates() error {
	for i, name := range v.columnNames {
		for j, name2 := range v.columnNames {
			if i == j {
				continue
			}
			if name == name2 {
				return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DupFieldName, "Duplicate column name '%s'", name)
			}
		}
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

	if err := a.tables.up(cursor); err != nil {
		a.setError(err)
		return false
	}

	if err := a.scoper.up(cursor); err != nil {
		a.setError(err)
		return false
	}

	switch cursor.Node().(type) {
	case sqlparser.SelectExprs:
		if isParentSelect(cursor) {
			a.popProjection()
		}
	}

	return a.shouldContinue()
}

func checkForInvalidConstructs(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.JoinTableExpr:
		if node.Condition.Using != nil {
			return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: join with USING(column_list) clause for complex queries")
		}
		if node.Join == sqlparser.NaturalJoinType || node.Join == sqlparser.NaturalRightJoinType || node.Join == sqlparser.NaturalLeftJoinType {
			return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: "+node.Join.ToString())
		}
	case *sqlparser.Select:
		if node.Having != nil {
			return Gen4NotSupportedF("HAVING")
		}
	case *sqlparser.Subquery:
		return Gen4NotSupportedF("subquery")
	case *sqlparser.FuncExpr:
		if sqlparser.IsLockingFunc(node) {
			return Gen4NotSupportedF("locking functions")
		}
	}

	return nil
}

func createVTableInfoForExpressions(expressions sqlparser.SelectExprs) *vTableInfo {
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

func (a *analyzer) resolveColumn(colName *sqlparser.ColName, current *scope) (TableSet, TableSet, *querypb.Type, error) {
	if colName.Qualifier.IsEmpty() {
		return a.resolveUnQualifiedColumn(current, colName)
	}
	return a.resolveQualifiedColumn(current, colName)
}

// resolveQualifiedColumn handles column expressions where the table is explicitly stated
func (a *analyzer) resolveQualifiedColumn(current *scope, expr *sqlparser.ColName) (TableSet, TableSet, *querypb.Type, error) {
	// search up the scope stack until we find a match
	for current != nil {
		for _, table := range current.tables {
			if !table.Matches(expr.Qualifier) {
				continue
			}
			if table.IsActualTable() {
				actualTable, ts, typ := a.resolveQualifiedColumnOnActualTable(table, expr)
				return actualTable, ts, typ, nil
			}
			recursiveTs, typ, err := table.RecursiveDepsFor(expr, a, len(current.tables) == 1)
			if err != nil {
				return 0, 0, nil, err
			}
			if recursiveTs == nil {
				return 0, 0, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "symbol %s not found", sqlparser.String(expr))
			}

			ts, err := table.DepsFor(expr, a, len(current.tables) == 1)
			if err != nil {
				return 0, 0, nil, err
			}
			return *recursiveTs, *ts, typ, nil
		}
		current = current.parent
	}
	return 0, 0, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "symbol %s not found", sqlparser.String(expr))
}

// resolveUnQualifiedColumn handles column that do not specify which table they belong to
func (a *analyzer) resolveUnQualifiedColumn(current *scope, expr *sqlparser.ColName) (TableSet, TableSet, *querypb.Type, error) {
	var tspRecursive, tsp *TableSet
	var typp *querypb.Type

	for current != nil && tspRecursive == nil {
		for _, tbl := range current.tables {
			recursiveTs, typ, err := tbl.RecursiveDepsFor(expr, a, len(current.tables) == 1)
			if err != nil {
				return 0, 0, nil, err
			}
			if recursiveTs != nil && tspRecursive != nil {
				return 0, 0, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUniqError, fmt.Sprintf("Column '%s' in field list is ambiguous", sqlparser.String(expr)))
			}
			if recursiveTs != nil {
				tspRecursive = recursiveTs
				typp = typ
			}
			if tbl.IsActualTable() {
				continue
			}
			ts, err := tbl.DepsFor(expr, a, len(current.tables) == 1)
			if err != nil {
				return 0, 0, nil, err
			}
			if ts != nil {
				tsp = ts
			}
		}

		current = current.parent
	}

	if tspRecursive == nil {
		return 0, 0, nil, nil
	}
	if tsp == nil {
		return *tspRecursive, 0, typp, nil
	}
	return *tspRecursive, *tsp, typp, nil
}

func (a *analyzer) resolveQualifiedColumnOnActualTable(table TableInfo, expr *sqlparser.ColName) (TableSet, TableSet, *querypb.Type) {
	ts := a.tableSetFor(table.GetExpr())
	for _, colInfo := range table.GetColumns() {
		if expr.Name.EqualString(colInfo.Name) {
			// A column can't be of type NULL, that is the default value indicating that we dont know the actual type
			// But expressions can be of NULL type, so we use nil to represent an unknown type
			if colInfo.Type == querypb.Type_NULL_TYPE {
				return ts, ts, nil
			}
			return ts, ts, &colInfo.Type
		}
	}
	return ts, ts, nil
}

func (a *analyzer) tableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	return a.tables.tableSetFor(t)
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
