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
	"vitess.io/vitess/go/mysql/collations"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// analyzer controls the flow of the analysis.
// It starts the tree walking and controls which part of the analysis sees which parts of the tree
type analyzer struct {
	scoper      *scoper
	earlyTables *earlyTableCollector
	tables      *tableCollector
	binder      *binder
	typer       *typer
	rewriter    *earlyRewriter
	fk          *fkManager
	sig         QuerySignature
	si          SchemaInformation
	currentDb   string
	recheck     bool

	err          error
	inProjection int

	projErr                 error
	unshardedErr            error
	warning                 string
	singleUnshardedKeyspace bool
	fullAnalysis            bool
}

// newAnalyzer create the semantic analyzer
func newAnalyzer(dbName string, si SchemaInformation, fullAnalysis bool) *analyzer {
	// TODO  dependencies between these components are a little tangled. We should try to clean up
	s := newScoper()
	a := &analyzer{
		scoper:       s,
		earlyTables:  newEarlyTableCollector(si, dbName),
		typer:        newTyper(si.Environment().CollationEnv()),
		si:           si,
		currentDb:    dbName,
		fullAnalysis: fullAnalysis,
	}
	s.org = a
	return a
}

func (a *analyzer) lateInit() {
	a.tables = a.earlyTables.newTableCollector(a.scoper, a)
	a.binder = newBinder(a.scoper, a, a.tables, a.typer)
	a.scoper.binder = a.binder
	a.rewriter = &earlyRewriter{
		binder:          a.binder,
		scoper:          a.scoper,
		expandedColumns: map[sqlparser.TableName][]*sqlparser.ColName{},
		env:             a.si.Environment(),
		aliasMapCache:   map[*sqlparser.Select]map[string]exprContainer{},
		reAnalyze:       a.reAnalyze,
		tables:          a.tables,
	}
	a.fk = &fkManager{
		binder:   a.binder,
		tables:   a.tables,
		si:       a.si,
		getError: a.getError,
	}
}

// Analyze analyzes the parsed query.
func Analyze(statement sqlparser.Statement, currentDb string, si SchemaInformation) (*SemTable, error) {
	return analyseAndGetSemTable(statement, currentDb, si, false)
}

func analyseAndGetSemTable(statement sqlparser.Statement, currentDb string, si SchemaInformation, fullAnalysis bool) (*SemTable, error) {
	analyzer := newAnalyzer(currentDb, newSchemaInfo(si), fullAnalysis)

	// Analysis for initial scope
	err := analyzer.analyze(statement)
	if err != nil {
		return nil, err
	}

	// Creation of the semantic table
	return analyzer.newSemTable(statement, si.ConnCollation(), si.GetForeignKeyChecksState(), si.Environment().CollationEnv())
}

// AnalyzeStrict analyzes the parsed query, and fails the analysis for any possible errors
func AnalyzeStrict(statement sqlparser.Statement, currentDb string, si SchemaInformation) (*SemTable, error) {
	st, err := analyseAndGetSemTable(statement, currentDb, si, true)
	if err != nil {
		return nil, err
	}

	if st.NotUnshardedErr != nil {
		return nil, st.NotUnshardedErr
	}
	if st.NotSingleRouteErr != nil {
		return nil, st.NotSingleRouteErr
	}

	return st, nil
}

func (a *analyzer) newSemTable(
	statement sqlparser.Statement,
	coll collations.ID,
	fkChecksState *bool,
	env *collations.Environment,
) (*SemTable, error) {
	var comments *sqlparser.ParsedComments
	commentedStmt, isCommented := statement.(sqlparser.Commented)
	if isCommented {
		comments = commentedStmt.GetParsedComments()
	}

	if a.singleUnshardedKeyspace {
		return &SemTable{
			Tables:                    a.earlyTables.Tables,
			Comments:                  comments,
			Warning:                   a.warning,
			Collation:                 coll,
			ExprTypes:                 map[sqlparser.Expr]evalengine.Type{},
			NotSingleRouteErr:         a.projErr,
			NotUnshardedErr:           a.unshardedErr,
			Recursive:                 ExprDependencies{},
			Direct:                    ExprDependencies{},
			ColumnEqualities:          map[columnName][]sqlparser.Expr{},
			ExpandedColumns:           map[sqlparser.TableName][]*sqlparser.ColName{},
			columns:                   map[*sqlparser.Union]sqlparser.SelectExprs{},
			StatementIDs:              a.scoper.statementIDs,
			QuerySignature:            QuerySignature{},
			childForeignKeysInvolved:  map[TableSet][]vindexes.ChildFKInfo{},
			parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{},
			childFkToUpdExprs:         map[string]sqlparser.UpdateExprs{},
			collEnv:                   env,
		}, nil
	}

	columns := map[*sqlparser.Union]sqlparser.SelectExprs{}
	for union, info := range a.tables.unionInfo {
		columns[union] = info.exprs
	}

	childFks, parentFks, childFkToUpdExprs, err := a.fk.getInvolvedForeignKeys(statement, fkChecksState)
	if err != nil {
		return nil, err
	}

	return &SemTable{
		Recursive:                 a.binder.recursive,
		Direct:                    a.binder.direct,
		ExprTypes:                 a.typer.m,
		Tables:                    a.tables.Tables,
		Targets:                   a.binder.targets,
		NotSingleRouteErr:         a.projErr,
		NotUnshardedErr:           a.unshardedErr,
		Warning:                   a.warning,
		Comments:                  comments,
		ColumnEqualities:          map[columnName][]sqlparser.Expr{},
		Collation:                 coll,
		ExpandedColumns:           a.rewriter.expandedColumns,
		columns:                   columns,
		StatementIDs:              a.scoper.statementIDs,
		QuerySignature:            a.sig,
		childForeignKeysInvolved:  childFks,
		parentForeignKeysInvolved: parentFks,
		childFkToUpdExprs:         childFkToUpdExprs,
		collEnv:                   env,
	}, nil
}

func (a *analyzer) setError(err error) {
	switch err := err.(type) {
	case ProjError:
		a.projErr = err.Inner
	case ShardedError:
		a.unshardedErr = err.Inner
	default:
		if a.inProjection > 0 && vterrors.ErrState(err) == vterrors.NonUniqError {
			a.projErr = err
		} else {
			a.err = err
		}
	}
}

func (a *analyzer) analyzeDown(cursor *sqlparser.Cursor) bool {
	// If we have an error we keep on going down the tree without checking for anything else
	// this way we can abort when we come back up.
	if !a.shouldContinue() {
		return true
	}

	if err := a.scoper.down(cursor); err != nil {
		a.setError(err)
		return true
	}
	if err := a.checkForInvalidConstructs(cursor); err != nil {
		a.setError(err)
		return true
	}
	if err := a.rewriter.down(cursor); err != nil {
		a.setError(err)
		return true
	}
	// log any warn in rewriting.
	a.warning = a.rewriter.warning

	a.noteQuerySignature(cursor.Node())

	a.enterProjection(cursor)
	// this is the visitor going down the tree. Returning false here would just not visit the children
	// to the current node, but that is not what we want if we have encountered an error.
	// In order to abort the whole visitation, we have to return true here and then return false in the `analyzeUp` method
	return true
}

func (a *analyzer) analyzeUp(cursor *sqlparser.Cursor) bool {
	if !a.shouldContinue() {
		return false
	}

	if err := a.tables.up(cursor); err != nil {
		a.setError(err)
		return false
	}

	if err := a.binder.up(cursor); err != nil {
		a.setError(err)
		return true
	}

	if err := a.typer.up(cursor); err != nil {
		a.setError(err)
		return false
	}

	if !a.recheck {
		// no need to run the rewriter on rechecking
		if err := a.rewriter.up(cursor); err != nil {
			a.setError(err)
			return true
		}
	}

	if err := a.scoper.up(cursor); err != nil {
		a.setError(err)
		return false
	}

	a.leaveProjection(cursor)
	return a.shouldContinue()
}

func containsStar(s sqlparser.SelectExprs) bool {
	for _, expr := range s {
		_, isStar := expr.(*sqlparser.StarExpr)
		if isStar {
			return true
		}
	}
	return false
}

func checkUnionColumns(union *sqlparser.Union) error {
	firstProj := sqlparser.GetFirstSelect(union).SelectExprs
	if containsStar(firstProj) {
		// if we still have *, we can't figure out if the query is invalid or not
		// we'll fail it at run time instead
		return nil
	}

	secondProj := sqlparser.GetFirstSelect(union.Right).SelectExprs
	if containsStar(secondProj) {
		return nil
	}

	if len(secondProj) != len(firstProj) {
		return &UnionColumnsDoNotMatchError{FirstProj: len(firstProj), SecondProj: len(secondProj)}
	}

	return nil
}

/*
errors that happen when we are evaluating SELECT expressions are saved until we know
if we can merge everything into a single route or not
*/
func (a *analyzer) enterProjection(cursor *sqlparser.Cursor) {
	_, ok := cursor.Node().(sqlparser.SelectExprs)
	if ok && isParentSelect(cursor) {
		a.inProjection++
	}
}

func (a *analyzer) leaveProjection(cursor *sqlparser.Cursor) {
	_, ok := cursor.Node().(sqlparser.SelectExprs)
	if ok && isParentSelect(cursor) {
		a.inProjection--
	}
}

func isParentSelect(cursor *sqlparser.Cursor) bool {
	_, isSelect := cursor.Parent().(*sqlparser.Select)
	return isSelect
}

func isParentDeleteOrUpdate(cursor *sqlparser.Cursor) bool {
	_, isDelete := cursor.Parent().(*sqlparser.Delete)
	_, isUpdate := cursor.Parent().(*sqlparser.Update)
	return isDelete || isUpdate
}

func isParentSelectStatement(cursor *sqlparser.Cursor) bool {
	_, isSelect := cursor.Parent().(sqlparser.SelectStatement)
	return isSelect
}

type originable interface {
	tableSetFor(t *sqlparser.AliasedTableExpr) TableSet
	depsForExpr(expr sqlparser.Expr) (direct, recursive TableSet, typ evalengine.Type)
	collationEnv() *collations.Environment
}

func (a *analyzer) depsForExpr(expr sqlparser.Expr) (direct, recursive TableSet, typ evalengine.Type) {
	recursive = a.binder.recursive.dependencies(expr)
	direct = a.binder.direct.dependencies(expr)
	typ = a.typer.exprType(expr)
	return
}

func (a *analyzer) collationEnv() *collations.Environment {
	return a.typer.collationEnv
}

func (a *analyzer) analyze(statement sqlparser.Statement) error {
	_ = sqlparser.Rewrite(statement, nil, a.earlyUp)
	if a.err != nil {
		return a.err
	}

	if a.canShortCut(statement) {
		return nil
	}

	a.lateInit()

	return a.lateAnalyze(statement)
}

func (a *analyzer) lateAnalyze(statement sqlparser.SQLNode) error {
	_ = sqlparser.Rewrite(statement, a.analyzeDown, a.analyzeUp)
	return a.err
}

func (a *analyzer) reAnalyze(statement sqlparser.SQLNode) error {
	a.recheck = true
	defer func() {
		a.recheck = false
	}()
	return a.lateAnalyze(statement)
}

// canShortCut checks if we are dealing with a single unsharded keyspace and no tables that have managed foreign keys
// if so, we can stop the analyzer early
func (a *analyzer) canShortCut(statement sqlparser.Statement) (canShortCut bool) {
	if a.fullAnalysis {
		return false
	}
	ks, _ := singleUnshardedKeyspace(a.earlyTables.Tables)
	if ks == nil {
		return false
	}

	defer func() {
		a.singleUnshardedKeyspace = canShortCut
	}()

	if !sqlparser.IsDMLStatement(statement) {
		return true
	}

	fkMode, err := a.si.ForeignKeyMode(ks.Name)
	if err != nil {
		a.err = err
		return false
	}
	if fkMode != vschemapb.Keyspace_managed {
		return true
	}

	for _, table := range a.earlyTables.Tables {
		vtbl := table.GetVindexTable()
		if len(vtbl.ChildForeignKeys) > 0 || len(vtbl.ParentForeignKeys) > 0 {
			return false
		}
	}

	return true
}

// earlyUp collects tables in the query, so we can check
// if this a single unsharded query we are dealing with
func (a *analyzer) earlyUp(cursor *sqlparser.Cursor) bool {
	a.earlyTables.up(cursor)
	return true
}

func (a *analyzer) shouldContinue() bool {
	return a.err == nil
}

func (a *analyzer) tableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	return a.tables.tableSetFor(t)
}

func (a *analyzer) noteQuerySignature(node sqlparser.SQLNode) {
	switch node := node.(type) {
	case *sqlparser.Union:
		a.sig.Union = true
		if node.Distinct {
			a.sig.Distinct = true
		}
	case *sqlparser.Subquery:
		a.sig.SubQueries = true
	case *sqlparser.Select:
		if node.Distinct {
			a.sig.Distinct = true
		}
		if node.GroupBy != nil {
			a.sig.Aggregation = true
		}
	case sqlparser.AggrFunc:
		a.sig.Aggregation = true
	case *sqlparser.Delete, *sqlparser.Update, *sqlparser.Insert:
		a.sig.DML = true
	}
}

// getError gets the error stored in the analyzer during previous phases.
func (a *analyzer) getError() error {
	if a.projErr != nil {
		return a.projErr
	}
	if a.unshardedErr != nil {
		return a.unshardedErr
	}
	return a.err
}

// ProjError is used to mark an error as something that should only be returned
// if the planner fails to merge everything down to a single route
type ProjError struct {
	Inner error
}

func (p ProjError) Error() string {
	return p.Inner.Error()
}

// ShardedError is used to mark an error as something that should only be returned
// if the query is not unsharded
type ShardedError struct {
	Inner error
}

func (p ShardedError) Unwrap() error {
	return p.Inner
}

func (p ShardedError) Error() string {
	return p.Inner.Error()
}
