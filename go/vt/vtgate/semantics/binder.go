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
	"vitess.io/vitess/go/vt/vtgate/engine"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// binder is responsible for finding all the column references in
// the query and bind them to the table that they belong to.
// While doing this, it will also find the types for columns and
// store these in the typer:s expression map
type binder struct {
	recursive   ExprDependencies
	direct      ExprDependencies
	scoper      *scoper
	tc          *tableCollector
	org         originable
	typer       *typer
	subqueryMap map[*sqlparser.Select][]*subquery
	subqueryRef map[*sqlparser.Subquery]*subquery
}

func newBinder(scoper *scoper, org originable, tc *tableCollector, typer *typer) *binder {
	return &binder{
		recursive:   map[sqlparser.Expr]TableSet{},
		direct:      map[sqlparser.Expr]TableSet{},
		scoper:      scoper,
		org:         org,
		tc:          tc,
		typer:       typer,
		subqueryMap: map[*sqlparser.Select][]*subquery{},
		subqueryRef: map[*sqlparser.Subquery]*subquery{},
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
		deps, err := b.resolveColumn(node, b.scoper.currentScope())
		if err != nil {
			return err
		}
		b.recursive[node] = deps.recursive
		b.direct[node] = deps.direct
		if deps.typ != nil {
			b.typer.setTypeFor(node, *deps.typ)
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
		b.recursive[node] = ts
		b.direct[node] = ts
	}
	return nil
}

func (b *binder) resolveColumn(colName *sqlparser.ColName, current *scope) (deps dependency, err error) {
	var thisDeps dependencies
	for current != nil {
		thisDeps, err = b.resolveColumnInScope(current, colName)
		if err != nil {
			err = makeAmbiguousError(colName, err)
			return dependency{}, err
		}
		if !thisDeps.Empty() {
			deps, err = thisDeps.Get()
			err = makeAmbiguousError(colName, err)
			return deps, err
		}
		current = current.parent
	}
	return dependency{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "symbol %s not found", sqlparser.String(colName))
}

func (b *binder) resolveColumnInScope(current *scope, expr *sqlparser.ColName) (dependencies, error) {
	var deps dependencies = &nothing{}
	for _, table := range current.tables {
		if !expr.Qualifier.IsEmpty() && !table.Matches(expr.Qualifier) {
			continue
		}
		thisDeps, err := table.Dependencies(expr.Name.String(), b.org)
		if err != nil {
			return nil, err
		}
		deps, err = thisDeps.Merge(deps)
		if err != nil {
			return nil, err
		}
	}
	if deps, isUncertain := deps.(*uncertain); isUncertain && deps.fail {
		// if we have a failure from uncertain, we matched the column to multiple non-authoritative tables
		return nil, ProjError{
			Inner: vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Column '%s' in field list is ambiguous", sqlparser.String(expr)),
		}
	}
	return deps, nil
}

func makeAmbiguousError(colName *sqlparser.ColName, err error) error {
	if err == ambigousErr {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Column '%s' in field list is ambiguous", sqlparser.String(colName))
	}
	return err
}
