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
	scoper   *scoper
	tables   *tableCollector
	binder   *binder
	typer    *typer
	rewriter *earlyRewriter
	sig      QuerySignature

	err          error
	inProjection int

	projErr      error
	unshardedErr error
	warning      string
}

// newAnalyzer create the semantic analyzer
func newAnalyzer(dbName string, si SchemaInformation) *analyzer {
	// TODO  dependencies between these components are a little tangled. We should try to clean up
	s := newScoper()
	a := &analyzer{
		scoper: s,
		tables: newTableCollector(s, si, dbName),
		typer:  newTyper(si.CollationEnv()),
	}
	s.org = a
	a.tables.org = a

	b := newBinder(s, a, a.tables, a.typer)
	a.binder = b
	a.rewriter = &earlyRewriter{
		scoper:          s,
		binder:          b,
		expandedColumns: map[sqlparser.TableName][]*sqlparser.ColName{},
		collationEnv:    si.CollationEnv(),
	}
	s.binder = b
	return a
}

// Analyze analyzes the parsed query.
func Analyze(statement sqlparser.Statement, currentDb string, si SchemaInformation) (*SemTable, error) {
	analyzer := newAnalyzer(currentDb, newSchemaInfo(si))

	// Analysis for initial scope
	err := analyzer.analyze(statement)
	if err != nil {
		return nil, err
	}

	// Creation of the semantic table
	return analyzer.newSemTable(statement, si.ConnCollation(), si.GetForeignKeyChecksState())
}

// AnalyzeStrict analyzes the parsed query, and fails the analysis for any possible errors
func AnalyzeStrict(statement sqlparser.Statement, currentDb string, si SchemaInformation) (*SemTable, error) {
	st, err := Analyze(statement, currentDb, si)
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

func (a *analyzer) newSemTable(statement sqlparser.Statement, coll collations.ID, fkChecksState *bool) (*SemTable, error) {
	var comments *sqlparser.ParsedComments
	commentedStmt, isCommented := statement.(sqlparser.Commented)
	if isCommented {
		comments = commentedStmt.GetParsedComments()
	}
	columns := map[*sqlparser.Union]sqlparser.SelectExprs{}
	for union, info := range a.tables.unionInfo {
		columns[union] = info.exprs
	}

	childFks, parentFks, childFkToUpdExprs, err := a.getInvolvedForeignKeys(statement, fkChecksState)
	if err != nil {
		return nil, err
	}

	return &SemTable{
		Recursive:                 a.binder.recursive,
		Direct:                    a.binder.direct,
		ExprTypes:                 a.typer.m,
		Tables:                    a.tables.Tables,
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

	if err := a.scoper.up(cursor); err != nil {
		a.setError(err)
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

	if err := a.rewriter.up(cursor); err != nil {
		a.setError(err)
		return true
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

func isParentSelectStatement(cursor *sqlparser.Cursor) bool {
	_, isSelect := cursor.Parent().(sqlparser.SelectStatement)
	return isSelect
}

type originable interface {
	tableSetFor(t *sqlparser.AliasedTableExpr) TableSet
	depsForExpr(expr sqlparser.Expr) (direct, recursive TableSet, typ evalengine.Type)
}

func (a *analyzer) depsForExpr(expr sqlparser.Expr) (direct, recursive TableSet, typ evalengine.Type) {
	recursive = a.binder.recursive.dependencies(expr)
	direct = a.binder.direct.dependencies(expr)
	typ = a.typer.exprType(expr)
	return
}

func (a *analyzer) analyze(statement sqlparser.Statement) error {
	_ = sqlparser.Rewrite(statement, a.analyzeDown, a.analyzeUp)
	return a.err
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
	}
}

// getInvolvedForeignKeys gets the foreign keys that might require taking care off when executing the given statement.
func (a *analyzer) getInvolvedForeignKeys(statement sqlparser.Statement, fkChecksState *bool) (map[TableSet][]vindexes.ChildFKInfo, map[TableSet][]vindexes.ParentFKInfo, map[string]sqlparser.UpdateExprs, error) {
	if fkChecksState != nil && !*fkChecksState {
		return nil, nil, nil, nil
	}
	// There are only the DML statements that require any foreign keys handling.
	switch stmt := statement.(type) {
	case *sqlparser.Delete:
		// For DELETE statements, none of the parent foreign keys require handling.
		// So we collect all the child foreign keys.
		allChildFks, _, err := a.getAllManagedForeignKeys()
		return allChildFks, nil, nil, err
	case *sqlparser.Insert:
		// For INSERT statements, we have 3 different cases:
		// 1. REPLACE statement: REPLACE statements are essentially DELETEs and INSERTs rolled into one.
		// 	  So we need to the parent foreign keys to ensure we are inserting the correct values, and the child foreign keys
		//	  to ensure we don't change a row that breaks the constraint or cascade any operations on the child tables.
		// 2. Normal INSERT statement: We don't need to check anything on the child foreign keys, so we just get all the parent foreign keys.
		// 3. INSERT with ON DUPLICATE KEY UPDATE: This might trigger an update on the columns specified in the ON DUPLICATE KEY UPDATE clause.
		allChildFks, allParentFKs, err := a.getAllManagedForeignKeys()
		if err != nil {
			return nil, nil, nil, err
		}
		if stmt.Action == sqlparser.ReplaceAct {
			return allChildFks, allParentFKs, nil, nil
		}
		if len(stmt.OnDup) == 0 {
			return nil, allParentFKs, nil, nil
		}
		// If only a certain set of columns are being updated, then there might be some child foreign keys that don't need any consideration since their columns aren't being updated.
		// So, we filter these child foreign keys out. We can't filter any parent foreign keys because the statement will INSERT a row too, which requires validating all the parent foreign keys.
		updatedChildFks, _, childFkToUpdExprs := a.filterForeignKeysUsingUpdateExpressions(allChildFks, nil, sqlparser.UpdateExprs(stmt.OnDup))
		return updatedChildFks, allParentFKs, childFkToUpdExprs, nil
	case *sqlparser.Update:
		// For UPDATE queries we get all the parent and child foreign keys, but we can filter some of them out if the columns that they consist off aren't being updated or are set to NULLs.
		allChildFks, allParentFks, err := a.getAllManagedForeignKeys()
		if err != nil {
			return nil, nil, nil, err
		}
		childFks, parentFks, childFkToUpdExprs := a.filterForeignKeysUsingUpdateExpressions(allChildFks, allParentFks, stmt.Exprs)
		return childFks, parentFks, childFkToUpdExprs, nil
	default:
		return nil, nil, nil, nil
	}
}

// filterForeignKeysUsingUpdateExpressions filters the child and parent foreign key constraints that don't require any validations/cascades given the updated expressions.
func (a *analyzer) filterForeignKeysUsingUpdateExpressions(allChildFks map[TableSet][]vindexes.ChildFKInfo, allParentFks map[TableSet][]vindexes.ParentFKInfo, updExprs sqlparser.UpdateExprs) (map[TableSet][]vindexes.ChildFKInfo, map[TableSet][]vindexes.ParentFKInfo, map[string]sqlparser.UpdateExprs) {
	if len(allChildFks) == 0 && len(allParentFks) == 0 {
		return nil, nil, nil
	}

	pFksRequired := make(map[TableSet][]bool, len(allParentFks))
	cFksRequired := make(map[TableSet][]bool, len(allChildFks))
	for ts, fks := range allParentFks {
		pFksRequired[ts] = make([]bool, len(fks))
	}
	for ts, fks := range allChildFks {
		cFksRequired[ts] = make([]bool, len(fks))
	}

	// updExprToTableSet stores the tables that the updated expressions are from.
	updExprToTableSet := make(map[*sqlparser.ColName]TableSet)

	// childFKToUpdExprs stores child foreign key to update expressions mapping.
	childFKToUpdExprs := map[string]sqlparser.UpdateExprs{}

	// Go over all the update expressions
	for _, updateExpr := range updExprs {
		deps := a.binder.direct.dependencies(updateExpr.Name)
		if deps.NumberOfTables() != 1 {
			panic("expected to have single table dependency")
		}
		updExprToTableSet[updateExpr.Name] = deps
		// Get all the child and parent foreign keys for the given table that the update expression belongs to.
		childFks := allChildFks[deps]
		parentFKs := allParentFks[deps]

		// Any foreign key to a child table for a column that has been updated
		// will require the cascade operations or restrict verification to happen, so we include all such foreign keys.
		for idx, childFk := range childFks {
			if childFk.ParentColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				cFksRequired[deps][idx] = true
				tbl, _ := a.tables.tableInfoFor(deps)
				ue := childFKToUpdExprs[childFk.String(tbl.GetVindexTable())]
				ue = append(ue, updateExpr)
				childFKToUpdExprs[childFk.String(tbl.GetVindexTable())] = ue
			}
		}
		// If we are setting a column to NULL, then we don't need to verify the existence of an
		// equivalent row in the parent table, even if this column was part of a foreign key to a parent table.
		if sqlparser.IsNull(updateExpr.Expr) {
			continue
		}
		// We add all the possible parent foreign key constraints that need verification that an equivalent row
		// exists, given that this column has changed.
		for idx, parentFk := range parentFKs {
			if parentFk.ChildColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				pFksRequired[deps][idx] = true
			}
		}
	}
	// For the parent foreign keys, if any of the columns part of the fk is set to NULL,
	// then, we don't care for the existence of an equivalent row in the parent table.
	for _, updateExpr := range updExprs {
		if !sqlparser.IsNull(updateExpr.Expr) {
			continue
		}
		ts := updExprToTableSet[updateExpr.Name]
		parentFKs := allParentFks[ts]
		for idx, parentFk := range parentFKs {
			if parentFk.ChildColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				pFksRequired[ts][idx] = false
			}
		}
	}

	// Create new maps with only the required foreign keys.
	pFksNeedsHandling := map[TableSet][]vindexes.ParentFKInfo{}
	cFksNeedsHandling := map[TableSet][]vindexes.ChildFKInfo{}
	for ts, parentFks := range allParentFks {
		var pFKNeeded []vindexes.ParentFKInfo
		for idx, fk := range parentFks {
			if pFksRequired[ts][idx] {
				pFKNeeded = append(pFKNeeded, fk)
			}
		}
		pFksNeedsHandling[ts] = pFKNeeded
	}
	for ts, childFks := range allChildFks {
		var cFKNeeded []vindexes.ChildFKInfo
		for idx, fk := range childFks {
			if cFksRequired[ts][idx] {
				cFKNeeded = append(cFKNeeded, fk)
			}
		}
		cFksNeedsHandling[ts] = cFKNeeded
	}
	return cFksNeedsHandling, pFksNeedsHandling, childFKToUpdExprs
}

// getAllManagedForeignKeys gets all the foreign keys for the query we are analyzing that Vitess is responsible for managing.
func (a *analyzer) getAllManagedForeignKeys() (map[TableSet][]vindexes.ChildFKInfo, map[TableSet][]vindexes.ParentFKInfo, error) {
	allChildFKs := make(map[TableSet][]vindexes.ChildFKInfo)
	allParentFKs := make(map[TableSet][]vindexes.ParentFKInfo)

	// Go over all the tables and collect the foreign keys.
	for idx, table := range a.tables.Tables {
		vi := table.GetVindexTable()
		if vi == nil || vi.Keyspace == nil {
			// If is not a real table, so should be skipped.
			continue
		}
		// Check whether Vitess needs to manage the foreign keys in this keyspace or not.
		fkMode, err := a.tables.si.ForeignKeyMode(vi.Keyspace.Name)
		if err != nil {
			return nil, nil, err
		}
		if fkMode != vschemapb.Keyspace_managed {
			continue
		}
		// Cyclic foreign key constraints error is stored in the keyspace.
		ksErr := a.tables.si.KeyspaceError(vi.Keyspace.Name)
		if ksErr != nil {
			return nil, nil, ksErr
		}

		// Add all the child and parent foreign keys to our map.
		ts := SingleTableSet(idx)
		allChildFKs[ts] = vi.ChildForeignKeys
		allParentFKs[ts] = vi.ParentForeignKeys
	}
	return allChildFKs, allParentFKs, nil
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

func (p ShardedError) Error() string {
	return p.Inner.Error()
}
