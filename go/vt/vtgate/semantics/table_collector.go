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
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
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
	unionInfo map[*sqlparser.Union]unionInfo
}

func newTableCollector(scoper *scoper, si SchemaInformation, currentDb string) *tableCollector {
	return &tableCollector{
		scoper:    scoper,
		si:        si,
		currentDb: currentDb,
		unionInfo: map[*sqlparser.Union]unionInfo{},
	}
}

func (tc *tableCollector) up(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.AliasedTableExpr:
		return tc.visitAliasedTableExpr(node)
	case *sqlparser.Union:
		firstSelect := sqlparser.GetFirstSelect(node)
		expanded, selectExprs := getColumnNames(firstSelect.SelectExprs)
		info := unionInfo{
			isAuthoritative: expanded,
			exprs:           selectExprs,
		}
		tc.unionInfo[node] = info
		if !expanded {
			return nil
		}

		size := len(firstSelect.SelectExprs)
		info.recursive = make([]TableSet, size)
		info.types = make([]*Type, size)

		_ = sqlparser.VisitAllSelects(node, func(s *sqlparser.Select, idx int) error {
			for i, expr := range s.SelectExprs {
				ae, ok := expr.(*sqlparser.AliasedExpr)
				if !ok {
					continue
				}
				_, recursiveDeps, qt := tc.org.depsForExpr(ae.Expr)
				info.recursive[i] = info.recursive[i].Merge(recursiveDeps)
				if idx == 0 {
					// TODO: we probably should coerce these types together somehow, but I'm not sure how
					info.types[i] = qt
				}
			}
			return nil
		})
		tc.unionInfo[node] = info
	}

	return nil
}

func (tc *tableCollector) visitAliasedTableExpr(node *sqlparser.AliasedTableExpr) error {
	switch t := node.Expr.(type) {
	case *sqlparser.DerivedTable:
		switch sel := t.Select.(type) {
		case *sqlparser.Select:
			return tc.addSelectDerivedTable(sel, node)

		case *sqlparser.Union:
			return tc.addUnionDerivedTable(sel, node)

		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] %T in a derived table", sel)
		}

	case sqlparser.TableName:
		var tbl *vindexes.Table
		var vindex vindexes.Vindex
		isInfSchema := sqlparser.SystemSchema(t.Qualifier.String())
		var err error
		tbl, vindex, _, _, _, err = tc.si.FindTableOrVindex(t)
		if err != nil && !isInfSchema {
			// if we are dealing with a system table, it might not be available in the vschema, but that is OK
			return err
		}
		if tbl == nil && vindex != nil {
			tbl = newVindexTable(t.Name)
		}

		scope := tc.scoper.currentScope()
		tableInfo := tc.createTable(t, node, tbl, isInfSchema, vindex)

		tc.Tables = append(tc.Tables, tableInfo)
		return scope.addTable(tableInfo)
	}
	return nil
}

func (tc *tableCollector) addSelectDerivedTable(sel *sqlparser.Select, node *sqlparser.AliasedTableExpr) error {
	tables := tc.scoper.wScope[sel]
	size := len(sel.SelectExprs)
	deps := make([]TableSet, size)
	types := make([]*Type, size)
	expanded := true
	for i, expr := range sel.SelectExprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			expanded = false
			continue
		}
		_, deps[i], types[i] = tc.org.depsForExpr(ae.Expr)
	}

	tableInfo := createDerivedTableForExpressions(sel.SelectExprs, node.Columns, tables.tables, tc.org, expanded, deps, types)
	if err := tableInfo.checkForDuplicates(); err != nil {
		return err
	}

	tableInfo.ASTNode = node
	tableInfo.tableName = node.As.String()

	tc.Tables = append(tc.Tables, tableInfo)
	scope := tc.scoper.currentScope()
	return scope.addTable(tableInfo)
}

func (tc *tableCollector) addUnionDerivedTable(union *sqlparser.Union, node *sqlparser.AliasedTableExpr) error {
	firstSelect := sqlparser.GetFirstSelect(union)
	tables := tc.scoper.wScope[firstSelect]
	info, found := tc.unionInfo[union]
	if !found {
		return vterrors.VT13001("information about union is not available")
	}

	tableInfo := createDerivedTableForExpressions(info.exprs, node.Columns, tables.tables, tc.org, info.isAuthoritative, info.recursive, info.types)
	if err := tableInfo.checkForDuplicates(); err != nil {
		return err
	}
	tableInfo.ASTNode = node
	tableInfo.tableName = node.As.String()

	tc.Tables = append(tc.Tables, tableInfo)
	scope := tc.scoper.currentScope()
	return scope.addTable(tableInfo)
}

func newVindexTable(t sqlparser.IdentifierCS) *vindexes.Table {
	vindexCols := []vindexes.Column{
		{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_VARBINARY},
		{Name: sqlparser.NewIdentifierCI("keyspace_id"), Type: querypb.Type_VARBINARY},
		{Name: sqlparser.NewIdentifierCI("range_start"), Type: querypb.Type_VARBINARY},
		{Name: sqlparser.NewIdentifierCI("range_end"), Type: querypb.Type_VARBINARY},
		{Name: sqlparser.NewIdentifierCI("hex_keyspace_id"), Type: querypb.Type_VARBINARY},
		{Name: sqlparser.NewIdentifierCI("shard"), Type: querypb.Type_VARBINARY},
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
		if t == t2.GetExpr() {
			return SingleTableSet(i)
		}
	}
	panic("unknown table")
}

// tableInfoFor returns the table info for the table set. It should contains only single table.
func (tc *tableCollector) tableInfoFor(id TableSet) (TableInfo, error) {
	offset := id.TableOffset()
	if offset < 0 {
		return nil, ErrNotSingleTable
	}
	return tc.Tables[offset], nil
}

func (tc *tableCollector) createTable(
	t sqlparser.TableName,
	alias *sqlparser.AliasedTableExpr,
	tbl *vindexes.Table,
	isInfSchema bool,
	vindex vindexes.Vindex,
) TableInfo {
	table := &RealTable{
		tableName:   alias.As.String(),
		ASTNode:     alias,
		Table:       tbl,
		isInfSchema: isInfSchema,
	}

	if alias.As.IsEmpty() {
		dbName := t.Qualifier.String()
		if dbName == "" {
			dbName = tc.currentDb
		}

		table.dbName = dbName
		table.tableName = t.Name.String()
	}

	if vindex != nil {
		return &VindexTable{
			Table:  table,
			Vindex: vindex,
		}
	}
	return table
}
