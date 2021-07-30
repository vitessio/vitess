/*
Copyright 2021 The Vitess Authors.

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
	"strconv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type binder struct {
	exprRecursiveDeps ExprDependencies
	exprDeps          ExprDependencies
	scoper            *scoper
}

func newBinder(scoper *scoper) *binder {
	return &binder{
		exprRecursiveDeps: map[sqlparser.Expr]TableSet{},
		exprDeps:          map[sqlparser.Expr]TableSet{},
		scoper:            scoper,
	}
}

func (b *binder) down(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.Order:
		return b.analyzeOrderByGroupByExprForLiteral(node.Expr, "order clause")
	case sqlparser.GroupBy:
		for _, grpExpr := range node {
			err := b.analyzeOrderByGroupByExprForLiteral(grpExpr, "group statement")
			if err != nil {
				return err
			}
		}
	}
	return nil
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

func (b *binder) analyzeOrderByGroupByExprForLiteral(input sqlparser.Expr, caller string) error {
	l, ok := input.(*sqlparser.Literal)
	if !ok {
		return nil
	}
	if l.Type != sqlparser.IntVal {
		return nil
	}
	currScope := b.scoper.currentScope()
	num, err := strconv.Atoi(l.Val)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "error parsing column number: %s", l.Val)
	}
	if num < 1 || num > len(currScope.selectExprs) {
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "Unknown column '%d' in '%s'", num, caller)
	}

	expr, ok := currScope.selectExprs[num-1].(*sqlparser.AliasedExpr)
	if !ok {
		return nil
	}

	b.exprRecursiveDeps[input] = b.exprRecursiveDeps.Dependencies(expr.Expr)
	return nil
}
