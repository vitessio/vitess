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

	"vitess.io/vitess/go/vt/vtgate/engine"

	querypb "vitess.io/vitess/go/vt/proto/query"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// binder is responsible for finding all the column references in
// the query and bind them to the table that they belong to.
// While doing this, it will also find the types for columns and
// store these in the typer:s expression map
type binder struct {
	exprRecursiveDeps ExprDependencies
	exprDeps          ExprDependencies
	scoper            *scoper
	tc                *tableCollector
	org               originable
	typer             *typer
	subqueryMap       map[*sqlparser.Select][]*subquery
	subqueryRef       map[*sqlparser.Subquery]*subquery
}

func newBinder(scoper *scoper, org originable, tc *tableCollector, typer *typer) *binder {
	return &binder{
		exprRecursiveDeps: map[sqlparser.Expr]TableSet{},
		exprDeps:          map[sqlparser.Expr]TableSet{},
		scoper:            scoper,
		org:               org,
		tc:                tc,
		typer:             typer,
		subqueryMap:       map[*sqlparser.Select][]*subquery{},
		subqueryRef:       map[*sqlparser.Subquery]*subquery{},
	}
}

func (b *binder) down(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.Subquery:
		currScope := b.scoper.currentScope()
		if currScope.selectStmt == nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unable to bind subquery to select statement")
		}
		opcode := engine.PulloutValue
		switch par := cursor.Parent().(type) {
		case *sqlparser.ComparisonExpr:
			switch par.Operator {
			case sqlparser.InOp:
				opcode = engine.PulloutIn
			case sqlparser.NotInOp:
				opcode = engine.PulloutNotIn
			}
		case *sqlparser.ExistsExpr:
			opcode = engine.PulloutExists
		}
		sq := &subquery{
			SubQuery: node,
			OpCode:   opcode,
		}
		b.subqueryMap[currScope.selectStmt] = append(b.subqueryMap[currScope.selectStmt], sq)
		b.subqueryRef[node] = sq
	case *sqlparser.ColName:
		baseTableTS, ts, typ, err := b.resolveColumn(node, b.scoper.currentScope())
		if err != nil {
			return err
		}
		b.exprRecursiveDeps[node] = baseTableTS
		b.exprDeps[node] = ts
		if typ != nil {
			b.typer.setTypeFor(node, *typ)
		}
	case *sqlparser.FuncExpr:
		// need special handling so that any lingering `*` expressions are bound to all local tables
		if len(node.Exprs) != 1 {
			break
		}
		if _, isStar := node.Exprs[0].(*sqlparser.StarExpr); !isStar {
			break
		}
		scope := b.scoper.currentScope()
		ts := TableSet(0)
		for _, table := range scope.tables {
			if !table.IsActualTable() {
				continue
			}
			ts |= b.tc.tableSetFor(table.GetExpr())
		}
		b.exprRecursiveDeps[node] = ts
		b.exprDeps[node] = ts
	}
	return nil
}

func (b *binder) resolveColumn(colName *sqlparser.ColName, current *scope) (TableSet, TableSet, *querypb.Type, error) {
	if colName.Qualifier.IsEmpty() {
		return b.resolveUnQualifiedColumn(current, colName)
	}
	return b.resolveQualifiedColumn(current, colName)
}

// resolveQualifiedColumn handles column expressions where the table is explicitly stated
func (b *binder) resolveQualifiedColumn(current *scope, expr *sqlparser.ColName) (TableSet, TableSet, *querypb.Type, error) {
	// search up the scope stack until we find a match
	for current != nil {
		for _, table := range current.tables {
			if !table.Matches(expr.Qualifier) {
				continue
			}
			if table.IsActualTable() {
				actualTable, ts, typ := b.resolveQualifiedColumnOnActualTable(table, expr)
				return actualTable, ts, typ, nil
			}
			recursiveTs, typ, err := table.RecursiveDepsFor(expr, b.org, len(current.tables) == 1)
			if err != nil {
				return 0, 0, nil, err
			}
			if recursiveTs == nil {
				return 0, 0, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "symbol %s not found", sqlparser.String(expr))
			}

			ts, err := table.DepsFor(expr, b.org, len(current.tables) == 1)
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
func (b *binder) resolveUnQualifiedColumn(current *scope, expr *sqlparser.ColName) (TableSet, TableSet, *querypb.Type, error) {
	var tspRecursive, tsp *TableSet
	var typp *querypb.Type

	for current != nil && tspRecursive == nil {
		for _, tbl := range current.tables {
			recursiveTs, typ, err := tbl.RecursiveDepsFor(expr, b.org, len(current.tables) == 1)
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
				tsp = tspRecursive
				continue
			}
			ts, err := tbl.DepsFor(expr, b.org, len(current.tables) == 1)
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
		return 0, 0, nil, ProjError{vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUniqError, fmt.Sprintf("Column '%s' in field list is ambiguous", sqlparser.String(expr)))}
	}

	if tsp == nil {
		return *tspRecursive, 0, typp, nil
	}
	return *tspRecursive, *tsp, typp, nil
}

func (b *binder) resolveQualifiedColumnOnActualTable(table TableInfo, expr *sqlparser.ColName) (TableSet, TableSet, *querypb.Type) {
	ts := b.tc.tableSetFor(table.GetExpr())
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
