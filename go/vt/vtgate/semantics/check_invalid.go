/*
Copyright 2023 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func (a *analyzer) checkForInvalidConstructs(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.Select:
		return a.checkSelect(cursor, node)
	case *sqlparser.Nextval:
		return a.checkNextVal()
	case *sqlparser.AliasedTableExpr:
		return checkAliasedTableExpr(node)
	case *sqlparser.JoinTableExpr:
		return a.checkJoin(node)
	case *sqlparser.LockingFunc:
		return &LockOnlyWithDualError{Node: node}
	case *sqlparser.Union:
		return checkUnion(node)
	case *sqlparser.JSONTableExpr:
		return &JSONTablesError{}
	case *sqlparser.DerivedTable:
		return checkDerived(node)
	case *sqlparser.AssignmentExpr:
		return vterrors.VT12001("Assignment expression")
	case *sqlparser.Subquery:
		return a.checkSubqueryColumns(cursor.Parent(), node)
	case *sqlparser.With:
		if node.Recursive {
			return vterrors.VT12001("recursive common table expression")
		}
	case *sqlparser.Insert:
		if node.Action == sqlparser.ReplaceAct {
			return ShardedError{Inner: &UnsupportedConstruct{errString: "REPLACE INTO with sharded keyspace"}}
		}
	case *sqlparser.OverClause:
		return ShardedError{Inner: &UnsupportedConstruct{errString: "OVER CLAUSE with sharded keyspace"}}
	}

	return nil
}

// checkSubqueryColumns checks that subqueries used in comparisons have the correct number of columns
func (a *analyzer) checkSubqueryColumns(parent sqlparser.SQLNode, subq *sqlparser.Subquery) error {
	cmp, ok := parent.(*sqlparser.ComparisonExpr)
	if !ok {
		return nil
	}
	var otherSide sqlparser.Expr
	if cmp.Left == subq {
		otherSide = cmp.Right
	} else {
		otherSide = cmp.Left
	}

	cols := 1
	if tuple, ok := otherSide.(sqlparser.ValTuple); ok {
		cols = len(tuple)
	}
	columns := subq.Select.GetColumns()
	for _, expr := range columns {
		_, ok := expr.(*sqlparser.StarExpr)
		if ok {
			// we can't check these queries properly. if we are able to push it down to mysql,
			// it will be checked there. if not, we'll fail because we are missing the column
			// information when we get to offset planning
			return nil
		}
	}
	if len(columns) != cols {
		return &SubqueryColumnCountError{Expected: cols}
	}
	return nil
}

func checkDerived(node *sqlparser.DerivedTable) error {
	if node.Lateral {
		return vterrors.VT12001("lateral derived tables")
	}
	return nil
}

func checkUnion(node *sqlparser.Union) error {
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			if !node.Qualifier.IsEmpty() {
				return false, &QualifiedOrderInUnionError{Table: node.Qualifier.Name.String()}
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
	return nil
}

func (a *analyzer) checkJoin(j *sqlparser.JoinTableExpr) error {
	if j.Join == sqlparser.NaturalJoinType || j.Join == sqlparser.NaturalRightJoinType || j.Join == sqlparser.NaturalLeftJoinType {
		return &UnsupportedNaturalJoinError{JoinExpr: j}
	}
	return nil
}
func (a *analyzer) checkNextVal() error {
	currScope := a.scoper.currentScope()
	if currScope.parent != nil {
		// This is defensively checking that we are not inside a subquery or derived table
		// Will probably already have been checked on the SELECT level
		return &CantUseOptionHereError{Msg: "INTO"}
	}
	if len(currScope.tables) != 1 {
		// This is defensively checking that we don't have too many tables.
		// Hard to check this with unit tests, since the parser does not accept these queries
		return &NextWithMultipleTablesError{CountTables: len(currScope.tables)}
	}
	vindexTbl := currScope.tables[0].GetVindexTable()
	if vindexTbl == nil {
		return &MissingInVSchemaError{
			Table: currScope.tables[0],
		}
	}
	if vindexTbl.Type != vindexes.TypeSequence {
		return &NotSequenceTableError{Table: vindexTbl.Name.String()}
	}
	return nil
}

func (a *analyzer) checkSelect(cursor *sqlparser.Cursor, node *sqlparser.Select) error {
	parent := cursor.Parent()
	if _, isUnion := parent.(*sqlparser.Union); isUnion && node.SQLCalcFoundRows {
		return &UnionWithSQLCalcFoundRowsError{}
	}
	if _, isRoot := parent.(*sqlparser.RootNode); !isRoot && node.SQLCalcFoundRows {
		return &SQLCalcFoundRowsUsageError{}
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
		return &CantUseOptionHereError{Msg: errMsg}
	}
	if node.Into != nil {
		return ShardedError{Inner: &UnsupportedConstruct{errString: "INTO on sharded keyspace"}}
	}
	return nil
}

// checkAliasedTableExpr checks the validity of AliasedTableExpr.
func checkAliasedTableExpr(node *sqlparser.AliasedTableExpr) error {
	if len(node.Hints) == 0 {
		return nil
	}
	alreadySeenVindexHint := false
	for _, hint := range node.Hints {
		if hint.Type.IsVindexHint() {
			if alreadySeenVindexHint {
				// TableName is safe to call, because only TableExpr can have hints.
				// And we already checked for hints being empty.
				tableName, err := node.TableName()
				if err != nil {
					return err
				}
				return &CantUseMultipleVindexHints{Table: sqlparser.String(tableName)}
			}
			alreadySeenVindexHint = true
		}
	}
	return nil
}
