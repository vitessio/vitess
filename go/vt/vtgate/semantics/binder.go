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

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type binder struct {
	exprRecursiveDeps ExprDependencies
	exprDeps          ExprDependencies
	scoper            *scoper
	tc                *tableCollector
	org               originable
}

func newBinder(scoper *scoper, org originable, tc *tableCollector) *binder {
	return &binder{
		exprRecursiveDeps: map[sqlparser.Expr]TableSet{},
		exprDeps:          map[sqlparser.Expr]TableSet{},
		scoper:            scoper,
		org:               org,
		tc:                tc,
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

func (b *binder) resolveColumn(colName *sqlparser.ColName, current *scope) (TableSet, TableSet, error) {
	if colName.Qualifier.IsEmpty() {
		return b.resolveUnQualifiedColumn(current, colName)
	}
	return b.resolveQualifiedColumn(current, colName)
}

// resolveQualifiedColumn handles column expressions where the table is explicitly stated
func (b *binder) resolveQualifiedColumn(current *scope, expr *sqlparser.ColName) (TableSet, TableSet, error) {
	// search up the scope stack until we find a match
	for current != nil {
		for _, table := range current.tables {
			if !table.Matches(expr.Qualifier) {
				continue
			}
			if table.IsActualTable() {
				ts := b.tc.tableSetFor(table.GetExpr())
				return ts, ts, nil
			}
			recursiveTs, _, err := table.RecursiveDepsFor(expr, b.org, len(current.tables) == 1)
			if err != nil {
				return 0, 0, err
			}
			if recursiveTs == nil {
				return 0, 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "symbol %s not found", sqlparser.String(expr))
			}

			ts, err := table.DepsFor(expr, b.org, len(current.tables) == 1)
			if err != nil {
				return 0, 0, err
			}
			return *recursiveTs, *ts, nil
		}
		current = current.parent
	}
	return 0, 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "symbol %s not found", sqlparser.String(expr))
}

// resolveUnQualifiedColumn handles column that do not specify which table they belong to
func (b *binder) resolveUnQualifiedColumn(current *scope, expr *sqlparser.ColName) (TableSet, TableSet, error) {
	var tspRecursive, tsp *TableSet

	for current != nil && tspRecursive == nil {
		for _, tbl := range current.tables {
			recursiveTs, _, err := tbl.RecursiveDepsFor(expr, b.org, len(current.tables) == 1)
			if err != nil {
				return 0, 0, err
			}
			if recursiveTs != nil && tspRecursive != nil {
				return 0, 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUniqError, fmt.Sprintf("Column '%s' in field list is ambiguous", sqlparser.String(expr)))
			}
			if recursiveTs != nil {
				tspRecursive = recursiveTs
			}
			if tbl.IsActualTable() {
				continue
			}
			ts, err := tbl.DepsFor(expr, b.org, len(current.tables) == 1)
			if err != nil {
				return 0, 0, err
			}
			if ts != nil {
				tsp = ts
			}
		}

		current = current.parent
	}

	if tspRecursive == nil {
		return 0, 0, nil
	}
	if tsp == nil {
		return *tspRecursive, 0, nil
	}
	return *tspRecursive, *tsp, nil
}
