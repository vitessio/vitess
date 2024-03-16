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
	"fmt"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
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
	done      map[*sqlparser.AliasedTableExpr]TableInfo
}

type earlyTableCollector struct {
	si         SchemaInformation
	currentDb  string
	Tables     []TableInfo
	done       map[*sqlparser.AliasedTableExpr]TableInfo
	withTables map[sqlparser.IdentifierCS]any
}

func newEarlyTableCollector(si SchemaInformation, currentDb string) *earlyTableCollector {
	return &earlyTableCollector{
		si:         si,
		currentDb:  currentDb,
		done:       map[*sqlparser.AliasedTableExpr]TableInfo{},
		withTables: map[sqlparser.IdentifierCS]any{},
	}
}

func (etc *earlyTableCollector) up(cursor *sqlparser.Cursor) {
	switch node := cursor.Node().(type) {
	case *sqlparser.AliasedTableExpr:
		etc.visitAliasedTableExpr(node)
	case *sqlparser.With:
		for _, cte := range node.CTEs {
			etc.withTables[cte.ID] = nil
		}
	}
}

func (etc *earlyTableCollector) visitAliasedTableExpr(aet *sqlparser.AliasedTableExpr) {
	tbl, ok := aet.Expr.(sqlparser.TableName)
	if !ok {
		return
	}
	etc.handleTableName(tbl, aet)
}

func (etc *earlyTableCollector) newTableCollector(scoper *scoper, org originable) *tableCollector {
	return &tableCollector{
		Tables:    etc.Tables,
		scoper:    scoper,
		si:        etc.si,
		currentDb: etc.currentDb,
		unionInfo: map[*sqlparser.Union]unionInfo{},
		done:      etc.done,
		org:       org,
	}
}

func (etc *earlyTableCollector) handleTableName(tbl sqlparser.TableName, aet *sqlparser.AliasedTableExpr) {
	if tbl.Qualifier.IsEmpty() {
		_, isCTE := etc.withTables[tbl.Name]
		if isCTE {
			// no need to handle these tables here, we wait for the late phase instead
			return
		}
	}
	tableInfo, err := getTableInfo(aet, tbl, etc.si, etc.currentDb)
	if err != nil {
		// this could just be a CTE that we haven't processed, so we'll give it the benefit of the doubt for now
		return
	}

	etc.done[aet] = tableInfo
	etc.Tables = append(etc.Tables, tableInfo)
}

func (tc *tableCollector) up(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.AliasedTableExpr:
		return tc.visitAliasedTableExpr(node)
	case *sqlparser.Union:
		return tc.visitUnion(node)
	default:
		return nil
	}
}

func (tc *tableCollector) visitUnion(union *sqlparser.Union) error {
	firstSelect := sqlparser.GetFirstSelect(union)
	expanded, selectExprs := getColumnNames(firstSelect.SelectExprs)
	info := unionInfo{
		isAuthoritative: expanded,
		exprs:           selectExprs,
	}
	tc.unionInfo[union] = info
	if !expanded {
		return nil
	}

	size := len(firstSelect.SelectExprs)
	info.recursive = make([]TableSet, size)
	typers := make([]evalengine.TypeAggregator, size)
	collations := tc.org.collationEnv()

	err := sqlparser.VisitAllSelects(union, func(s *sqlparser.Select, idx int) error {
		for i, expr := range s.SelectExprs {
			ae, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			_, recursiveDeps, qt := tc.org.depsForExpr(ae.Expr)
			info.recursive[i] = info.recursive[i].Merge(recursiveDeps)
			if err := typers[i].Add(qt, collations); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, ts := range typers {
		info.types = append(info.types, ts.Type())
	}
	tc.unionInfo[union] = info
	return nil
}

func (tc *tableCollector) visitAliasedTableExpr(node *sqlparser.AliasedTableExpr) error {
	switch t := node.Expr.(type) {
	case *sqlparser.DerivedTable:
		return tc.handleDerivedTable(node, t)

	case sqlparser.TableName:
		return tc.handleTableName(node, t)
	}
	return nil
}

func (tc *tableCollector) handleTableName(node *sqlparser.AliasedTableExpr, t sqlparser.TableName) (err error) {
	var tableInfo TableInfo
	var found bool

	tableInfo, found = tc.done[node]
	if !found {
		tableInfo, err = getTableInfo(node, t, tc.si, tc.currentDb)
		if err != nil {
			return err
		}
		tc.Tables = append(tc.Tables, tableInfo)
	}

	scope := tc.scoper.currentScope()
	return scope.addTable(tableInfo)
}

func getTableInfo(node *sqlparser.AliasedTableExpr, t sqlparser.TableName, si SchemaInformation, currentDb string) (TableInfo, error) {
	var tbl *vindexes.Table
	var vindex vindexes.Vindex
	isInfSchema := sqlparser.SystemSchema(t.Qualifier.String())
	var err error
	tbl, vindex, _, _, _, err = si.FindTableOrVindex(t)
	if err != nil && !isInfSchema {
		// if we are dealing with a system table, it might not be available in the vschema, but that is OK
		return nil, err
	}
	if tbl == nil && vindex != nil {
		tbl = newVindexTable(t.Name)
	}

	tableInfo, err := createTable(t, node, tbl, isInfSchema, vindex, si, currentDb)
	if err != nil {
		return nil, err
	}
	return tableInfo, nil
}

func (tc *tableCollector) handleDerivedTable(node *sqlparser.AliasedTableExpr, t *sqlparser.DerivedTable) error {
	switch sel := t.Select.(type) {
	case *sqlparser.Select:
		return tc.addSelectDerivedTable(sel, node, node.Columns, node.As)
	case *sqlparser.Union:
		return tc.addUnionDerivedTable(sel, node, node.Columns, node.As)
	default:
		return vterrors.VT13001("[BUG] %T in a derived table", sel)
	}
}

func (tc *tableCollector) addSelectDerivedTable(
	sel *sqlparser.Select,
	tableExpr *sqlparser.AliasedTableExpr,
	columns sqlparser.Columns,
	alias sqlparser.IdentifierCS,
) error {
	tables := tc.scoper.wScope[sel]
	size := len(sel.SelectExprs)
	deps := make([]TableSet, size)
	types := make([]evalengine.Type, size)
	expanded := true
	for i, expr := range sel.SelectExprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			expanded = false
			continue
		}
		_, deps[i], types[i] = tc.org.depsForExpr(ae.Expr)
	}

	tableInfo := createDerivedTableForExpressions(sel.SelectExprs, columns, tables.tables, tc.org, expanded, deps, types)
	if err := tableInfo.checkForDuplicates(); err != nil {
		return err
	}

	tableInfo.ASTNode = tableExpr
	tableInfo.tableName = alias.String()

	tc.Tables = append(tc.Tables, tableInfo)
	scope := tc.scoper.currentScope()
	return scope.addTable(tableInfo)
}

func (tc *tableCollector) addUnionDerivedTable(union *sqlparser.Union, node *sqlparser.AliasedTableExpr, columns sqlparser.Columns, alias sqlparser.IdentifierCS) error {
	firstSelect := sqlparser.GetFirstSelect(union)
	tables := tc.scoper.wScope[firstSelect]
	info, found := tc.unionInfo[union]
	if !found {
		return vterrors.VT13001("information about union is not available")
	}

	tableInfo := createDerivedTableForExpressions(info.exprs, columns, tables.tables, tc.org, info.isAuthoritative, info.recursive, info.types)
	if err := tableInfo.checkForDuplicates(); err != nil {
		return err
	}
	tableInfo.ASTNode = node
	tableInfo.tableName = alias.String()

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
		if t == t2.GetAliasedTableExpr() {
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

func createTable(
	t sqlparser.TableName,
	alias *sqlparser.AliasedTableExpr,
	tbl *vindexes.Table,
	isInfSchema bool,
	vindex vindexes.Vindex,
	si SchemaInformation,
	currentDb string,
) (TableInfo, error) {
	hint := getVindexHint(alias.Hints)

	if err := checkValidVindexHints(hint, tbl); err != nil {
		return nil, err
	}

	table := &RealTable{
		tableName:    alias.As.String(),
		ASTNode:      alias,
		Table:        tbl,
		VindexHint:   hint,
		isInfSchema:  isInfSchema,
		collationEnv: si.Environment().CollationEnv(),
	}

	if alias.As.IsEmpty() {
		dbName := t.Qualifier.String()
		if dbName == "" {
			dbName = currentDb
		}

		table.dbName = dbName
		table.tableName = t.Name.String()
	}

	if vindex != nil {
		return &VindexTable{
			Table:  table,
			Vindex: vindex,
		}, nil
	}
	return table, nil
}

func checkValidVindexHints(hint *sqlparser.IndexHint, tbl *vindexes.Table) error {
	if hint == nil {
		return nil
	}
outer:
	for _, index := range hint.Indexes {
		for _, columnVindex := range tbl.ColumnVindexes {
			if index.EqualString(columnVindex.Name) {
				continue outer
			}
		}
		// we found a hint on a non-existing vindex
		return &NoSuchVindexFound{
			Table:      fmt.Sprintf("%s.%s", tbl.Keyspace.Name, tbl.Name.String()),
			VindexName: index.String(),
		}
	}
	return nil
}

// getVindexHint gets the vindex hint from the list of IndexHints.
func getVindexHint(hints sqlparser.IndexHints) *sqlparser.IndexHint {
	for _, hint := range hints {
		if hint.Type.IsVindexHint() {
			return hint
		}
	}
	return nil
}
