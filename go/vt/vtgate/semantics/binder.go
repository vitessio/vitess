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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// binder is responsible for finding all the column references in
// the query and bind them to the table that they belong to.
// While doing this, it will also find the types for columns and
// store these in the typer:s expression map
type binder struct {
	recursive ExprDependencies
	direct    ExprDependencies
	targets   TableSet
	scoper    *scoper
	tc        *tableCollector
	org       originable
	typer     *typer

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
		usingJoinInfo: map[TableSet]map[string]TableSet{},
	}
}

func (b *binder) up(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.Subquery:
		return b.setSubQueryDependencies(node)
	case *sqlparser.JoinCondition:
		return b.bindJoinCondition(node)
	case *sqlparser.ColName:
		return b.bindColName(node)
	case *sqlparser.CountStar:
		return b.bindCountStar(node)
	case *sqlparser.Union:
		return b.bindUnion(node)
	case sqlparser.TableNames:
		return b.bindTableNames(cursor, node)
	case *sqlparser.UpdateExpr:
		return b.bindUpdateExpr(node)
	default:
		return nil
	}
}

func (b *binder) bindUpdateExpr(ue *sqlparser.UpdateExpr) error {
	ts, ok := b.direct[ue.Name]
	if !ok {
		return nil
	}
	b.targets = b.targets.Merge(ts)
	return nil
}

func (b *binder) bindTableNames(cursor *sqlparser.Cursor, tables sqlparser.TableNames) error {
	_, isDelete := cursor.Parent().(*sqlparser.Delete)
	if !isDelete {
		return nil
	}
	current := b.scoper.currentScope()
	for _, target := range tables {
		finalDep, err := b.findDependentTableSet(current, target)
		if err != nil {
			return err
		}
		b.targets = b.targets.Merge(finalDep.direct)
	}
	return nil
}

func (b *binder) bindUnion(union *sqlparser.Union) error {
	info := b.tc.unionInfo[union]
	// TODO: this check can be removed and available type information should be used.
	if !info.isAuthoritative {
		return nil
	}

	for i, expr := range info.exprs {
		ae := expr.(*sqlparser.AliasedExpr)
		b.recursive[ae.Expr] = info.recursive[i]
		if t := info.types[i]; t.Valid() {
			b.typer.m[ae.Expr] = t
		}
	}
	return nil
}

func (b *binder) bindColName(col *sqlparser.ColName) error {
	currentScope := b.scoper.currentScope()
	deps, err := b.resolveColumn(col, currentScope, false, true)
	if err != nil {
		s := err.Error()
		if deps.direct.IsEmpty() ||
			!strings.HasSuffix(s, "is ambiguous") ||
			!b.canRewriteUsingJoin(deps, col) {
			return err
		}

		// if we got here it means we are dealing with a ColName that is involved in a JOIN USING.
		// we do the rewriting of these ColName structs here because it would be difficult to copy all the
		// needed state over to the earlyRewriter
		deps, err = b.rewriteJoinUsingColName(deps, col, currentScope)
		if err != nil {
			return err
		}
	}
	b.recursive[col] = deps.recursive
	b.direct[col] = deps.direct
	if deps.typ.Valid() {
		b.typer.setTypeFor(col, deps.typ)
	}
	return nil
}

func (b *binder) bindJoinCondition(condition *sqlparser.JoinCondition) error {
	currScope := b.scoper.currentScope()
	for _, ident := range condition.Using {
		name := sqlparser.NewColName(ident.String())
		deps, err := b.resolveColumn(name, currScope, true, true)
		if err != nil {
			return err
		}
		currScope.joinUsing[ident.Lowered()] = deps.direct
	}
	return nil
}

func (b *binder) findDependentTableSet(current *scope, target sqlparser.TableName) (dependency, error) {
	var deps dependencies = &nothing{}
	for _, table := range current.tables {
		tblName, err := table.Name()
		if err != nil {
			continue
		}
		if tblName.Name.String() != target.Name.String() {
			continue
		}
		ts := b.org.tableSetFor(table.GetAliasedTableExpr())
		c := createCertain(ts, ts, evalengine.Type{})
		deps = deps.merge(c, false)
	}
	finalDep, err := deps.get(nil)
	if err != nil {
		return dependency{}, err
	}
	if finalDep.direct != finalDep.recursive {
		return dependency{}, vterrors.VT03004(target.Name.String())
	}
	return finalDep, nil
}

func (b *binder) bindCountStar(node *sqlparser.CountStar) error {
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
			ts = ts.Merge(tbl.getTableSet(b.org))
		}
	}
	b.recursive[node] = ts
	b.direct[node] = ts
	return nil
}

func (b *binder) rewriteJoinUsingColName(deps dependency, node *sqlparser.ColName, currentScope *scope) (dependency, error) {
	constituents := deps.recursive.Constituents()
	if len(constituents) < 1 {
		return dependency{}, &BuggyError{Msg: "we should not have a *ColName that depends on nothing"}
	}
	newTbl := constituents[0]
	infoFor, err := b.tc.tableInfoFor(newTbl)
	if err != nil {
		return dependency{}, err
	}
	name, err := infoFor.Name()
	if err != nil {
		return dependency{}, err
	}
	node.Qualifier = name
	deps, err = b.resolveColumn(node, currentScope, false, true)
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
func (b *binder) setSubQueryDependencies(subq *sqlparser.Subquery) error {
	currScope := b.scoper.currentScope()
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
	return nil
}

func (b *binder) resolveColumn(colName *sqlparser.ColName, current *scope, allowMulti, singleTableFallBack bool) (dependency, error) {
	if !current.stmtScope && current.inGroupBy {
		return b.resolveColInGroupBy(colName, current, allowMulti)
	}
	if !current.stmtScope && current.inHaving && !current.inHavingAggr {
		return b.resolveColumnInHaving(colName, current, allowMulti)
	}

	var thisDeps dependencies
	first := true
	var tableName *sqlparser.TableName

	for current != nil {
		var err error
		thisDeps, err = b.resolveColumnInScope(current, colName, allowMulti)
		if err != nil {
			return dependency{}, err
		}
		if !thisDeps.empty() {
			return thisDeps.get(colName)
		}
		if current.parent == nil &&
			len(current.tables) == 1 &&
			first &&
			colName.Qualifier.IsEmpty() &&
			singleTableFallBack {
			// if this is the top scope, and we still haven't been able to find a match, we know we are about to fail
			// we can check this last scope and see if there is a single table. if there is just one table in the scope
			// we assume that the column is meant to come from this table.
			// we also check that this is the first scope we are looking in.
			// If there are more scopes the column could come from, we can't assume anything
			// This is just used for a clearer error message
			name, err := current.tables[0].Name()
			if err == nil {
				tableName = &name
			}
		}
		first = false
		current = current.parent
	}
	return dependency{}, ShardedError{ColumnNotFoundError{Column: colName, Table: tableName}}
}

func isColumnNotFound(err error) bool {
	switch err := err.(type) {
	case ColumnNotFoundError:
		return true
	case ShardedError:
		return isColumnNotFound(err.Inner)
	default:
		return false
	}
}

func (b *binder) resolveColumnInHaving(colName *sqlparser.ColName, current *scope, allowMulti bool) (dependency, error) {
	if current.inHavingAggr {
		// when inside an aggregation, we'll search the FROM clause before the SELECT expressions
		deps, err := b.resolveColumn(colName, current.parent, allowMulti, true)
		if deps.direct.NotEmpty() || (err != nil && !isColumnNotFound(err)) {
			return deps, err
		}
	}

	// Here we are searching among the SELECT expressions for a match
	thisDeps, err := b.resolveColumnInScope(current, colName, allowMulti)
	if err != nil {
		return dependency{}, err
	}

	if !thisDeps.empty() {
		// we found something! let's return it
		return thisDeps.get(colName)
	}

	notFoundErr := &ColumnNotFoundClauseError{Column: colName.Name.String(), Clause: "having clause"}
	if current.inHavingAggr {
		// if we are inside an aggregation, we've already looked everywhere. now it's time to give up
		return dependency{}, notFoundErr
	}

	// Now we'll search the FROM clause, but with a twist. If we find it in the FROM clause, the column must also
	// exist as a standalone expression in the SELECT list
	deps, err := b.resolveColumn(colName, current.parent, allowMulti, true)
	if deps.direct.IsEmpty() {
		return dependency{}, notFoundErr
	}

	sel := current.stmt.(*sqlparser.Select) // we can be sure of this, since HAVING doesn't exist on UNION
	if selDeps := b.searchInSelectExpressions(colName, deps, sel); selDeps.direct.NotEmpty() {
		return selDeps, nil
	}

	if !current.inHavingAggr && len(sel.GroupBy) == 0 {
		// if we are not inside an aggregation, and there is no GROUP BY, we consider the FROM clause before failing
		if deps.direct.NotEmpty() || (err != nil && !isColumnNotFound(err)) {
			return deps, err
		}
	}

	return dependency{}, notFoundErr
}

// searchInSelectExpressions searches for the ColName among the SELECT and GROUP BY expressions
// It used dependency information to match the columns
func (b *binder) searchInSelectExpressions(colName *sqlparser.ColName, deps dependency, stmt *sqlparser.Select) dependency {
	for _, selectExpr := range stmt.SelectExprs {
		ae, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		selectCol, ok := ae.Expr.(*sqlparser.ColName)
		if !ok || !selectCol.Name.Equal(colName.Name) {
			continue
		}

		_, direct, _ := b.org.depsForExpr(selectCol)
		if deps.direct == direct {
			// we have found the ColName in the SELECT expressions, so it's safe to use here
			direct, recursive, typ := b.org.depsForExpr(ae.Expr)
			return dependency{certain: true, direct: direct, recursive: recursive, typ: typ}
		}
	}

	for _, gb := range stmt.GroupBy {
		selectCol, ok := gb.(*sqlparser.ColName)
		if !ok || !selectCol.Name.Equal(colName.Name) {
			continue
		}

		_, direct, _ := b.org.depsForExpr(selectCol)
		if deps.direct == direct {
			// we have found the ColName in the GROUP BY expressions, so it's safe to use here
			direct, recursive, typ := b.org.depsForExpr(gb)
			return dependency{certain: true, direct: direct, recursive: recursive, typ: typ}
		}
	}
	return dependency{}
}

// resolveColInGroupBy handles the special rules we have when binding on the GROUP BY column
func (b *binder) resolveColInGroupBy(
	colName *sqlparser.ColName,
	current *scope,
	allowMulti bool,
) (dependency, error) {
	if current.parent == nil {
		return dependency{}, vterrors.VT13001("did not expect this to be the last scope")
	}
	// if we are in GROUP BY, we have to search the FROM clause before we search the SELECT expressions
	deps, firstErr := b.resolveColumn(colName, current.parent, allowMulti, false)
	if firstErr == nil {
		return deps, nil
	}

	// either we didn't find the column on a table, or it was ambiguous.
	// in either case, next step is to search the SELECT expressions
	if colName.Qualifier.NonEmpty() {
		// if the col name has a qualifier, none of the SELECT expressions are going to match
		return dependency{}, nil
	}
	vtbl, ok := current.tables[0].(*vTableInfo)
	if !ok {
		return dependency{}, vterrors.VT13001("expected the table info to be a *vTableInfo")
	}

	dependencies, err := vtbl.dependenciesInGroupBy(colName.Name.String(), b.org)
	if err != nil {
		return dependency{}, err
	}
	if dependencies.empty() {
		if isColumnNotFound(firstErr) {
			return dependency{}, &ColumnNotFoundClauseError{Column: colName.Name.String(), Clause: "group statement"}
		}
		return deps, firstErr
	}
	return dependencies.get(colName)
}

func (b *binder) resolveColumnInScope(current *scope, expr *sqlparser.ColName, allowMulti bool) (dependencies, error) {
	var deps dependencies = &nothing{}
	for _, table := range current.tables {
		if !expr.Qualifier.IsEmpty() && !table.matches(expr.Qualifier) && !current.isUnion {
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
		return nil, ProjError{Inner: newAmbiguousColumnError(expr)}
	}
	return deps, nil
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
