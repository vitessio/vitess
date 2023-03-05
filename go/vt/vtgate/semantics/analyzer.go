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
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// analyzer controls the flow of the analysis.
// It starts the tree walking and controls which part of the analysis sees which parts of the tree
type analyzer struct {
	scoper   *scoper
	tables   *tableCollector
	binder   *binder
	typer    *typer
	rewriter *earlyRewriter

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
		typer:  newTyper(),
	}
	s.org = a
	a.tables.org = a

	b := newBinder(s, a, a.tables, a.typer)
	a.binder = b
	a.rewriter = &earlyRewriter{scoper: s, binder: b}
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
	semTable := analyzer.newSemTable(statement, si.ConnCollation())

	return semTable, nil
}

func (a analyzer) newSemTable(statement sqlparser.Statement, coll collations.ID) *SemTable {
	var comments *sqlparser.ParsedComments
	commentedStmt, isCommented := statement.(sqlparser.Commented)
	if isCommented {
		comments = commentedStmt.GetParsedComments()
	}

	return &SemTable{
		Recursive:         a.binder.recursive,
		Direct:            a.binder.direct,
		ExprTypes:         a.typer.exprTypes,
		Tables:            a.tables.Tables,
		selectScope:       a.scoper.rScope,
		NotSingleRouteErr: a.projErr,
		NotUnshardedErr:   a.unshardedErr,
		Warning:           a.warning,
		Comments:          comments,
		SubqueryMap:       a.binder.subqueryMap,
		SubqueryRef:       a.binder.subqueryRef,
		ColumnEqualities:  map[columnName][]sqlparser.Expr{},
		Collation:         coll,
	}
}

func (a *analyzer) setError(err error) {
	switch err := err.(type) {
	case ProjError:
		a.projErr = err.Inner
	case UnshardedError:
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

	if err := a.binder.up(cursor); err != nil {
		a.setError(err)
		return true
	}

	if err := a.scoper.up(cursor); err != nil {
		a.setError(err)
		return false
	}
	if err := a.tables.up(cursor); err != nil {
		a.setError(err)
		return false
	}
	if err := a.typer.up(cursor); err != nil {
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
	count := len(firstProj)

	secondProj := sqlparser.GetFirstSelect(union.Right).SelectExprs
	if containsStar(secondProj) {
		return nil
	}

	if len(secondProj) != count {
		return vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.WrongNumberOfColumnsInSelect, "The used SELECT statements have a different number of columns")
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
	depsForExpr(expr sqlparser.Expr) (direct, recursive TableSet, typ *Type)
}

func (a *analyzer) depsForExpr(expr sqlparser.Expr) (direct, recursive TableSet, typ *Type) {
	recursive = a.binder.recursive.dependencies(expr)
	direct = a.binder.direct.dependencies(expr)
	qt, isFound := a.typer.exprTypes[expr]
	if !isFound {
		return
	}
	typ = &qt
	return
}

func (a *analyzer) analyze(statement sqlparser.Statement) error {
	_ = sqlparser.Rewrite(statement, a.analyzeDown, a.analyzeUp)
	return a.err
}

func (a *analyzer) checkForInvalidConstructs(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.Update:
		if len(node.TableExprs) != 1 {
			return UnshardedError{Inner: vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: multiple tables in update")}
		}
		alias, isAlias := node.TableExprs[0].(*sqlparser.AliasedTableExpr)
		if !isAlias {
			return UnshardedError{Inner: vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: multiple tables in update")}
		}
		_, isDerived := alias.Expr.(*sqlparser.DerivedTable)
		if isDerived {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUpdateableTable, "The target table %s of the UPDATE is not updatable", alias.As.String())
		}
	case *sqlparser.Select:
		parent := cursor.Parent()
		if _, isUnion := parent.(*sqlparser.Union); isUnion && node.SQLCalcFoundRows {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "SQL_CALC_FOUND_ROWS not supported with union")
		}
		if _, isRoot := parent.(*sqlparser.RootNode); !isRoot && node.SQLCalcFoundRows {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'")
		}
		errMsg := "INTO"
		nextVal := false
		if len(node.SelectExprs) == 1 {
			if _, isNextVal := node.SelectExprs[0].(*sqlparser.Nextval); isNextVal {
				nextVal = true
				errMsg = "NEXT"
			}
		}
		if !nextVal && node.Into == nil {
			return nil
		}
		if a.scoper.currentScope().parent != nil {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.CantUseOptionHere, "Incorrect usage/placement of '%s'", errMsg)
		}
	case *sqlparser.Nextval:
		currScope := a.scoper.currentScope()
		if currScope.parent != nil {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.CantUseOptionHere, "Incorrect usage/placement of 'INTO'")
		}
		if len(currScope.tables) != 1 {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] Next statement should not contain multiple tables")
		}
		vindexTbl := currScope.tables[0].GetVindexTable()
		if vindexTbl == nil {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Table information is not provided in vschema")
		}
		if vindexTbl.Type != vindexes.TypeSequence {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "NEXT used on a non-sequence table")
		}
	case *sqlparser.JoinTableExpr:
		if node.Join == sqlparser.NaturalJoinType || node.Join == sqlparser.NaturalRightJoinType || node.Join == sqlparser.NaturalLeftJoinType {
			return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: "+node.Join.ToString())
		}
	case *sqlparser.LockingFunc:
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%v allowed only with dual", sqlparser.String(node))
	case *sqlparser.Union:
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				if !node.Qualifier.IsEmpty() {
					return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Table '%s' from one of the SELECTs cannot be used in global ORDER clause", node.Qualifier.Name)
				}
			case *sqlparser.Subquery:
				return false, nil
			}
			return true, nil
		}, node.OrderBy)
		if err != nil {
			return err
		}
		err = checkUnionColumns(node)
		if err != nil {
			return err
		}
	case *sqlparser.JSONTableExpr:
		return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: json_table expressions")
	}

	return nil
}

func (a *analyzer) shouldContinue() bool {
	return a.err == nil
}

func (a *analyzer) tableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	return a.tables.tableSetFor(t)
}

// ProjError is used to mark an error as something that should only be returned
// if the planner fails to merge everything down to a single route
type ProjError struct {
	Inner error
}

func (p ProjError) Error() string {
	return p.Inner.Error()
}

// UnshardedError is used to mark an error as something that should only be returned
// if the query is not unsharded
type UnshardedError struct {
	Inner error
}

func (p UnshardedError) Error() string {
	return p.Inner.Error()
}
