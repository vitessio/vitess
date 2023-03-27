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
	"strings"

	"vitess.io/vitess/go/vt/vtgate/engine"

	"vitess.io/vitess/go/vt/sqlparser"
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
	subqueryMap map[sqlparser.Statement][]*sqlparser.ExtractedSubquery
	subqueryRef map[*sqlparser.Subquery]*sqlparser.ExtractedSubquery

	// every table will have an entry in the outer map. it will point to a map with all the columns
	// that this map is joined with using USING.
	// This information is used to expand `*` correctly, and is not available post-analysis
	usingJoinInfo map[TableSet]map[string]TableSet
}

func newBinder(scoper *scoper, org originable, tc *tableCollector, typer *typer) *binder {
	return &binder{
		recursive:     map[sqlparser.Expr]TableSet{},
		direct:        map[sqlparser.Expr]TableSet{},
		scoper:        scoper,
		org:           org,
		tc:            tc,
		typer:         typer,
		subqueryMap:   map[sqlparser.Statement][]*sqlparser.ExtractedSubquery{},
		subqueryRef:   map[*sqlparser.Subquery]*sqlparser.ExtractedSubquery{},
		usingJoinInfo: map[TableSet]map[string]TableSet{},
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
		currScope := b.scoper.currentScope()
		for _, ident := range node.Using {
			name := sqlparser.NewColName(ident.String())
			deps, err := b.resolveColumn(name, currScope, true)
			if err != nil {
				return err
			}
			currScope.joinUsing[ident.Lowered()] = deps.direct
		}
		if len(node.Using) > 0 {
			err := rewriteJoinUsing(currScope, node.Using, b.org)
			if err != nil {
				return err
			}
			node.Using = nil
		}
	case *sqlparser.ColName:
		currentScope := b.scoper.currentScope()
		deps, err := b.resolveColumn(node, currentScope, false)
		if err != nil {
			if deps.direct.IsEmpty() ||
				!strings.HasSuffix(err.Error(), "is ambiguous") ||
				!b.canRewriteUsingJoin(deps, node) {
				return err
			}

			// if we got here it means we are dealing with a ColName that is involved in a JOIN USING.
			// we do the rewriting of these ColName structs here because it would be difficult to copy all the
			// needed state over to the earlyRewriter
			deps, err = b.rewriteJoinUsingColName(deps, node, currentScope)
			if err != nil {
				return err
			}
		}
		b.recursive[node] = deps.recursive
		b.direct[node] = deps.direct
		if deps.typ != nil {
			b.typer.setTypeFor(node, *deps.typ)
		}
	case *sqlparser.CountStar:
		b.bindCountStar(node)
	}
	return nil
}

func (b *binder) bindCountStar(node *sqlparser.CountStar) {
	scope := b.scoper.currentScope()
	var ts TableSet
	for _, tbl := range scope.tables {
		switch tbl := tbl.(type) {
		case *vTableInfo:
			for _, col := range tbl.cols {
				if sqlparser.Equals.Expr(node, col) {
					ts = ts.Merge(b.recursive[col])
				}
			}
		default:
			expr := tbl.getExpr()
			if expr != nil {
				setFor := b.tc.tableSetFor(expr)
				ts = ts.Merge(setFor)
			}
		}
	}
	b.recursive[node] = ts
	b.direct[node] = ts
}

func (b *binder) rewriteJoinUsingColName(deps dependency, node *sqlparser.ColName, currentScope *scope) (dependency, error) {
	constituents := deps.recursive.Constituents()
	if len(constituents) < 1 {
		return dependency{}, NewError(Buggy, "we should not have a *ColName that depends on nothing")
	}
	newTbl := constituents[0]
	infoFor, err := b.tc.tableInfoFor(newTbl)
	if err != nil {
		return dependency{}, err
	}
	alias := infoFor.getExpr().As
	if alias.IsEmpty() {
		name, err := infoFor.Name()
		if err != nil {
			return dependency{}, err
		}
		node.Qualifier = name
	} else {
		node.Qualifier = sqlparser.TableName{
			Name: sqlparser.NewIdentifierCS(alias.String()),
		}
	}
	deps, err = b.resolveColumn(node, currentScope, false)
	if err != nil {
		return dependency{}, err
	}
	return deps, nil
}

// canRewriteUsingJoin will return true when this ColName is safe to rewrite since it can only belong to a USING JOIN
func (b *binder) canRewriteUsingJoin(deps dependency, node *sqlparser.ColName) bool {
	tbls := deps.direct.Constituents()
	colName := node.Name.Lowered()
	for _, tbl := range tbls {
		m := b.usingJoinInfo[tbl]
		if _, found := m[colName]; !found {
			return false
		}
	}
	return true
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
			tablesToKeep = tablesToKeep.Merge(table.getTableSet(b.org))
		}
		sco = sco.parent
	}

	b.recursive[subq] = subqRecursiveDeps.KeepOnly(tablesToKeep)
	b.direct[subq] = subqDirectDeps.KeepOnly(tablesToKeep)
}

func (b *binder) createExtractedSubquery(cursor *sqlparser.Cursor, currScope *scope, subq *sqlparser.Subquery) (*sqlparser.ExtractedSubquery, error) {
	if currScope.stmt == nil {
		return nil, NewError(Buggy, "unable to bind subquery to select statement")
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

func (b *binder) resolveColumn(colName *sqlparser.ColName, current *scope, allowMulti bool) (dependency, error) {
	var thisDeps dependencies
	for current != nil {
		var err error
		thisDeps, err = b.resolveColumnInScope(current, colName, allowMulti)
		if err != nil {
			err = makeAmbiguousError(colName, err)
			if thisDeps == nil {
				return dependency{}, err
			}
		}
		if !thisDeps.empty() {
			deps, thisErr := thisDeps.get()
			if thisErr != nil {
				err = makeAmbiguousError(colName, thisErr)
			}
			return deps, err
		} else if err != nil {
			return dependency{}, err
		}
		current = current.parent
	}
	return dependency{}, ShardedError{Inner: NewError(ColumnNotFound, colName)}
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
		deps = thisDeps.merge(deps, allowMulti)
	}
	if deps, isUncertain := deps.(*uncertain); isUncertain && deps.fail {
		// if we have a failure from uncertain, we matched the column to multiple non-authoritative tables
		return nil, ProjError{Inner: NewError(AmbiguousColumn, expr)}
	}
	return deps, nil
}

func makeAmbiguousError(colName *sqlparser.ColName, err error) error {
	if err == ambigousErr {
		err = NewError(AmbiguousColumn, colName)
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
