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
	case *sqlparser.Update:
		return checkUpdate(node)
	case *sqlparser.Select:
		return a.checkSelect(cursor, node)
	case *sqlparser.Nextval:
		return a.checkNextVal()
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
	return nil
}

func checkUpdate(node *sqlparser.Update) error {
	if len(node.TableExprs) != 1 {
		return ShardedError{Inner: &UnsupportedMultiTablesInUpdateError{ExprCount: len(node.TableExprs)}}
	}
	alias, isAlias := node.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !isAlias {
		return ShardedError{Inner: &UnsupportedMultiTablesInUpdateError{NotAlias: true}}
	}
	_, isDerived := alias.Expr.(*sqlparser.DerivedTable)
	if isDerived {
		return &TableNotUpdatableError{Table: alias.As.String()}
	}
	return nil
}
