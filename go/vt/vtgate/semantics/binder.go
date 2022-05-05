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
	recursive    ExprDependencies
	direct       ExprDependencies
	scoper       *scoper
	tc           *tableCollector
	org          originable
	typer        *typer
	subqueryMap  map[sqlparser.Statement][]*sqlparser.ExtractedSubquery
	subqueryRef  map[*sqlparser.Subquery]*sqlparser.ExtractedSubquery
	colidentDeps map[string]TableSet
}

func newBinder(scoper *scoper, org originable, tc *tableCollector, typer *typer) *binder {
	return &binder{
		recursive:    map[sqlparser.Expr]TableSet{},
		direct:       map[sqlparser.Expr]TableSet{},
		scoper:       scoper,
		org:          org,
		tc:           tc,
		typer:        typer,
		subqueryMap:  map[sqlparser.Statement][]*sqlparser.ExtractedSubquery{},
		subqueryRef:  map[*sqlparser.Subquery]*sqlparser.ExtractedSubquery{},
		colidentDeps: map[string]TableSet{},
	}
}

func (b *binder) up(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.Subquery:
		currScope := b.scoper.currentScope()
		sq, err := b.createExtractedSubquery(cursor, currScope, node)
		if err != nil {
			return err
		}

		b.subqueryMap[currScope.stmt] = append(b.subqueryMap[currScope.stmt], sq)
		b.subqueryRef[node] = sq

		b.setSubQueryDependencies(node, currScope)
	case *sqlparser.JoinCondition:
		for _, ident := range node.Using {
			name := sqlparser.NewColName(ident.String())
			s := b.scoper.currentScope()
			deps, err := b.resolveColumn(name, s, true)
			if err != nil {
				return err
			}
			b.colidentDeps[ident.Lowered()] = deps.direct
		}
	case *sqlparser.ColName:
		currentScope := b.scoper.currentScope()
		deps, err := b.resolveColumn(node, currentScope, false)
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
		var ts TableSet
		for _, table := range scope.tables {
			expr := table.getExpr()
			if expr != nil {
				ts.MergeInPlace(b.tc.tableSetFor(expr))
			}
		}
		b.recursive[node] = ts
		b.direct[node] = ts
	}
	return nil
}

// setSubQueryDependencies sets the correct dependencies for the subquery
// the binder usually only sets the dependencies of ColNames, but we need to
// handle the subquery dependencies differently, so they are set manually here
// this method will only keep dependencies to tables outside the subquery
func (b *binder) setSubQueryDependencies(subq *sqlparser.Subquery, currScope *scope) {
	subqRecursiveDeps := b.recursive.dependencies(subq)
	subqDirectDeps := b.direct.dependencies(subq)

	tablesToKeep := EmptyTableSet()
	sco := currScope
	for sco != nil {
		for _, table := range sco.tables {
			tablesToKeep.MergeInPlace(table.getTableSet(b.org))
		}
		sco = sco.parent
	}

	subqDirectDeps.KeepOnly(tablesToKeep)
	subqRecursiveDeps.KeepOnly(tablesToKeep)
	b.recursive[subq] = subqRecursiveDeps
	b.direct[subq] = subqDirectDeps
}

func (b *binder) createExtractedSubquery(cursor *sqlparser.Cursor, currScope *scope, subq *sqlparser.Subquery) (*sqlparser.ExtractedSubquery, error) {
	if currScope.stmt == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unable to bind subquery to select statement")
	}

	sq := &sqlparser.ExtractedSubquery{
		Subquery: subq,
		Original: subq,
		OpCode:   int(engine.PulloutValue),
	}

	switch par := cursor.Parent().(type) {
	case *sqlparser.ComparisonExpr:
		switch par.Operator {
		case sqlparser.InOp:
			sq.OpCode = int(engine.PulloutIn)
		case sqlparser.NotInOp:
			sq.OpCode = int(engine.PulloutNotIn)
		}
		subq, exp := GetSubqueryAndOtherSide(par)
		sq.Original = &sqlparser.ComparisonExpr{
			Left:     exp,
			Operator: par.Operator,
			Right:    subq,
		}
		sq.OtherSide = exp
	case *sqlparser.ExistsExpr:
		sq.OpCode = int(engine.PulloutExists)
		sq.Original = par
	}
	return sq, nil
}

func (b *binder) resolveColumn(colName *sqlparser.ColName, current *scope, allowMulti bool) (deps dependency, err error) {
	var thisDeps dependencies
	for current != nil {
		thisDeps, err = b.resolveColumnInScope(current, colName, allowMulti)
		if err != nil {
			err = makeAmbiguousError(colName, err)
			return dependency{}, err
		}
		if !thisDeps.empty() {
			deps, err = thisDeps.get()
			err = makeAmbiguousError(colName, err)
			return deps, err
		}
		current = current.parent
	}
	return dependency{}, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "symbol %s not found", sqlparser.String(colName))
}

func (b *binder) resolveColumnInScope(current *scope, expr *sqlparser.ColName, allowMulti bool) (dependencies, error) {
	var deps dependencies = &nothing{}
	for _, table := range current.tables {
		if !expr.Qualifier.IsEmpty() && !table.matches(expr.Qualifier) {
			continue
		}
		thisDeps, err := table.dependencies(expr.Name.String(), b.org)
		if err != nil {
			return nil, err
		}
		deps, err = thisDeps.merge(deps, allowMulti)
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

// GetSubqueryAndOtherSide returns the subquery and other side of a comparison, iff one of the sides is a SubQuery
func GetSubqueryAndOtherSide(node *sqlparser.ComparisonExpr) (*sqlparser.Subquery, sqlparser.Expr) {
	var subq *sqlparser.Subquery
	var exp sqlparser.Expr
	if lSubq, lIsSubq := node.Left.(*sqlparser.Subquery); lIsSubq {
		subq = lSubq
		exp = node.Right
	} else if rSubq, rIsSubq := node.Right.(*sqlparser.Subquery); rIsSubq {
		subq = rSubq
		exp = node.Left
	}
	return subq, exp
}
