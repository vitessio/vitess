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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// tableCollector is responsible for gathering information about the tables listed in the FROM clause,
// and adding them to the current scope, plus keeping the global list of tables used in the query
type tableCollector struct {
	Tables    []TableInfo
	scoper    *scoper
	si        SchemaInformation
	currentDb string
	org       originable
}

func newTableCollector(scoper *scoper, si SchemaInformation, currentDb string) *tableCollector {
	return &tableCollector{
		scoper:    scoper,
		si:        si,
		currentDb: currentDb,
	}
}

func (tc *tableCollector) up(cursor *sqlparser.Cursor) error {
	node, ok := cursor.Node().(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil
	}

	switch t := node.Expr.(type) {
	case *sqlparser.DerivedTable:
		switch sel := t.Select.(type) {
		case *sqlparser.Select:
			tables := tc.scoper.wScope[sel]
			tableInfo := createDerivedTableForExpressions(sqlparser.GetFirstSelect(sel).SelectExprs, tables.tables, tc.org)
			if err := tableInfo.checkForDuplicates(); err != nil {
				return err
			}

			tableInfo.ASTNode = node
			tableInfo.tableName = node.As.String()

			tc.Tables = append(tc.Tables, tableInfo)
			scope := tc.scoper.currentScope()
			return scope.addTable(tableInfo)

		case *sqlparser.Union:
			tables := tc.scoper.wScope[sel.FirstStatement.(*sqlparser.Select)]
			tableInfo := createDerivedTableForExpressions(sqlparser.GetFirstSelect(sel).SelectExprs, tables.tables, tc.org)
			if err := tableInfo.checkForDuplicates(); err != nil {
				return err
			}
			tableInfo.ASTNode = node
			tableInfo.tableName = node.As.String()

			tc.Tables = append(tc.Tables, tableInfo)
			scope := tc.scoper.currentScope()
			return scope.addTable(tableInfo)

		default:
			return Gen4NotSupportedF("union in derived table")
		}

	case sqlparser.TableName:
		var tbl *vindexes.Table
		var vindex vindexes.Vindex
		var isInfSchema bool
		if sqlparser.SystemSchema(t.Qualifier.String()) {
			isInfSchema = true
		} else {
			var err error
			tbl, vindex, _, _, _, err = tc.si.FindTableOrVindex(t)
			if err != nil {
				return err
			}
			if tbl == nil && vindex != nil {
				tbl = newVindexTable(t.Name)
			}
		}
		scope := tc.scoper.currentScope()
		tableInfo := tc.createTable(t, node, tbl, isInfSchema, vindex)

		tc.Tables = append(tc.Tables, tableInfo)
		return scope.addTable(tableInfo)
	}
	return nil
}

func newVindexTable(t sqlparser.TableIdent) *vindexes.Table {
	vindexCols := []vindexes.Column{
		{Name: sqlparser.NewColIdent("id")},
		{Name: sqlparser.NewColIdent("keyspace_id")},
		{Name: sqlparser.NewColIdent("range_start")},
		{Name: sqlparser.NewColIdent("range_end")},
		{Name: sqlparser.NewColIdent("hex_keyspace_id")},
		{Name: sqlparser.NewColIdent("shard")},
	}

	return &vindexes.Table{
		Name:                    t,
		Columns:                 vindexCols,
		ColumnListAuthoritative: true,
	}
}

// tabletSetFor implements the originable interface, and that is why it lives on the analyser struct.
// The code lives in this file since it is only touching tableCollector data
func (tc *tableCollector) tableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	for i, t2 := range tc.Tables {
		if t == t2.getExpr() {
			return TableSet(1 << i)
		}
	}
	panic("unknown table")
}

func (tc *tableCollector) createTable(
	t sqlparser.TableName,
	alias *sqlparser.AliasedTableExpr,
	tbl *vindexes.Table,
	isInfSchema bool,
	vindex vindexes.Vindex,
) TableInfo {
	dbName := t.Qualifier.String()
	if dbName == "" {
		dbName = tc.currentDb
	}
	var table TableInfo
	if alias.As.IsEmpty() {
		table = &RealTable{
			dbName:      dbName,
			tableName:   t.Name.String(),
			ASTNode:     alias,
			Table:       tbl,
			isInfSchema: isInfSchema,
		}
	} else {
		table = &AliasedTable{
			tableName:   alias.As.String(),
			ASTNode:     alias,
			Table:       tbl,
			isInfSchema: isInfSchema,
		}
	}

	if vindex != nil {
		return &VindexTable{
			Table:  table,
			Vindex: vindex,
		}
	}
	return table
}
