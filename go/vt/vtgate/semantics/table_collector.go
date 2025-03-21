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

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	// tableCollector is responsible for gathering information about the tables listed in the FROM clause,
	// and adding them to the current scope, plus keeping the global list of tables used in the query
	tableCollector struct {
		earlyTableCollector
		scoper    *scoper
		org       originable
		unionInfo map[*sqlparser.Union]unionInfo
	}

	earlyTableCollector struct {
		si        SchemaInformation
		currentDb string
		Tables    []TableInfo
		done      map[*sqlparser.AliasedTableExpr]TableInfo

		// cte is a map of CTE definitions that are used in the query
		cte map[string]*CTE

		// lastInsertIdWithArgument is used to signal to later stages that we
		// need to do special handling of the engine primitive
		lastInsertIdWithArgument bool
	}
)

func newEarlyTableCollector(si SchemaInformation, currentDb string) *earlyTableCollector {
	return &earlyTableCollector{
		si:        si,
		currentDb: currentDb,
		done:      map[*sqlparser.AliasedTableExpr]TableInfo{},
		cte:       map[string]*CTE{},
	}
}

func (etc *earlyTableCollector) down(cursor *sqlparser.Cursor) bool {
	switch node := cursor.Node().(type) {
	case *sqlparser.With:
		for _, cte := range node.CTEs {
			etc.cte[cte.ID.String()] = &CTE{
				Name:      cte.ID.String(),
				Query:     cte.Subquery,
				Columns:   cte.Columns,
				Recursive: node.Recursive,
			}
		}
	case *sqlparser.FuncExpr:
		if node.Name.EqualString("last_insert_id") && len(node.Exprs) == 1 {
			etc.lastInsertIdWithArgument = true
		}
	}

	return true
}

func (etc *earlyTableCollector) up(cursor *sqlparser.Cursor) bool {
	ate, ok := cursor.Node().(*sqlparser.AliasedTableExpr)
	if !ok {
		return true
	}
	etc.visitAliasedTableExpr(ate)
	return true
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
		earlyTableCollector: *etc,
		scoper:              scoper,
		unionInfo:           map[*sqlparser.Union]unionInfo{},
		org:                 org,
	}
}

func (etc *earlyTableCollector) handleTableName(tbl sqlparser.TableName, aet *sqlparser.AliasedTableExpr) {
	if tbl.Qualifier.IsEmpty() {
		_, isCTE := etc.cte[tbl.Name.String()]
		if isCTE {
			// no need to handle these tables here, we wait for the late phase instead
			return
		}
	}
	tableInfo, err := etc.getTableInfo(aet, tbl, nil)
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
	case *sqlparser.RowAlias:
		ins, ok := cursor.Parent().(*sqlparser.Insert)
		if !ok {
			return vterrors.VT13001("RowAlias is expected to hang off an Insert statement")
		}
		return tc.visitRowAlias(ins, node)
	default:
		return nil
	}
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

func (tc *tableCollector) visitUnion(union *sqlparser.Union) error {
	firstSelect, err := sqlparser.GetFirstSelect(union)
	if err != nil {
		return err
	}
	expanded, selectExprs := getColumnNames(firstSelect.GetColumns())
	info := unionInfo{
		isAuthoritative: expanded,
		exprs:           selectExprs,
	}
	tc.unionInfo[union] = info
	if !expanded {
		return nil
	}

	size := firstSelect.GetColumnCount()
	info.recursive = make([]TableSet, size)
	typers := make([]evalengine.TypeAggregator, size)
	collations := tc.org.collationEnv()

	err = sqlparser.VisitAllSelects(union, func(s *sqlparser.Select, idx int) error {
		for i, expr := range s.GetColumns() {
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

func (tc *tableCollector) visitRowAlias(ins *sqlparser.Insert, rowAlias *sqlparser.RowAlias) error {
	origTableInfo := tc.Tables[0]

	colNames, types, err := tc.getColumnNamesAndTypes(ins, rowAlias, origTableInfo)
	if err != nil {
		return err
	}

	derivedTable := buildDerivedTable(colNames, rowAlias, types)
	tc.Tables = append(tc.Tables, derivedTable)
	current := tc.scoper.currentScope()
	return current.addTable(derivedTable)
}

func (tc *tableCollector) getColumnNamesAndTypes(ins *sqlparser.Insert, rowAlias *sqlparser.RowAlias, origTableInfo TableInfo) (colNames []string, types []evalengine.Type, err error) {
	switch {
	case len(rowAlias.Columns) > 0 && len(ins.Columns) > 0:
		return tc.handleExplicitColumns(ins, rowAlias, origTableInfo)
	case len(rowAlias.Columns) > 0:
		return tc.handleRowAliasColumns(origTableInfo, rowAlias)
	case len(ins.Columns) > 0:
		colNames, types = tc.handleInsertColumns(ins, origTableInfo)
		return colNames, types, nil
	default:
		return tc.handleDefaultColumns(origTableInfo)
	}
}

// handleDefaultColumns have no explicit column list on the insert statement and no column list on the row alias
func (tc *tableCollector) handleDefaultColumns(origTableInfo TableInfo) ([]string, []evalengine.Type, error) {
	if !origTableInfo.authoritative() {
		return nil, nil, vterrors.VT09015()
	}
	var colNames []string
	var types []evalengine.Type
	for _, column := range origTableInfo.getColumns(true /* ignoreInvisibleCol */) {
		colNames = append(colNames, column.Name)
		types = append(types, column.Type)
	}
	return colNames, types, nil
}

// handleInsertColumns have explicit column list on the insert statement and no column list on the row alias
func (tc *tableCollector) handleInsertColumns(ins *sqlparser.Insert, origTableInfo TableInfo) ([]string, []evalengine.Type) {
	var colNames []string
	var types []evalengine.Type
	origCols := origTableInfo.getColumns(false /* ignoreInvisbleCol */)
for2:
	for _, column := range ins.Columns {
		colNames = append(colNames, column.String())
		for _, origCol := range origCols {
			if column.EqualString(origCol.Name) {
				types = append(types, origCol.Type)
				continue for2
			}
		}
		types = append(types, evalengine.NewUnknownType())
	}
	return colNames, types
}

// handleRowAliasColumns have explicit column list on the row alias and no column list on the insert statement
func (tc *tableCollector) handleRowAliasColumns(origTableInfo TableInfo, rowAlias *sqlparser.RowAlias) ([]string, []evalengine.Type, error) {
	if !origTableInfo.authoritative() {
		return nil, nil, vterrors.VT09015()
	}
	origCols := origTableInfo.getColumns(true /* ignoreInvisibleCol */)
	if len(rowAlias.Columns) != len(origCols) {
		return nil, nil, vterrors.VT03033()
	}
	var colNames []string
	var types []evalengine.Type
	for idx, column := range rowAlias.Columns {
		colNames = append(colNames, column.String())
		types = append(types, origCols[idx].Type)
	}
	return colNames, types, nil
}

// handleExplicitColumns have explicit column list on the row alias and the insert statement
func (tc *tableCollector) handleExplicitColumns(ins *sqlparser.Insert, rowAlias *sqlparser.RowAlias, origTableInfo TableInfo) ([]string, []evalengine.Type, error) {
	if len(rowAlias.Columns) != len(ins.Columns) {
		return nil, nil, vterrors.VT03033()
	}
	var colNames []string
	var types []evalengine.Type
	origCols := origTableInfo.getColumns(false /* ignoreInvisbleCol */)
for1:
	for idx, column := range rowAlias.Columns {
		colNames = append(colNames, column.String())
		col := ins.Columns[idx]
		for _, origCol := range origCols {
			if col.EqualString(origCol.Name) {
				types = append(types, origCol.Type)
				continue for1
			}
		}
		return nil, nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadFieldError, "Unknown column '%s' in 'field list'", col)
	}
	return colNames, types, nil
}

func buildDerivedTable(colNames []string, rowAlias *sqlparser.RowAlias, types []evalengine.Type) *DerivedTable {
	deps := make([]TableSet, len(colNames))
	for i := range colNames {
		deps[i] = SingleTableSet(0)
	}

	derivedTable := &DerivedTable{
		tableName: rowAlias.TableName.String(),
		ASTNode: &sqlparser.AliasedTableExpr{
			Expr: sqlparser.NewTableName(rowAlias.TableName.String()),
		},
		columnNames:     colNames,
		tables:          SingleTableSet(0),
		recursive:       deps,
		isAuthoritative: true,
		types:           types,
	}
	return derivedTable
}

func (tc *tableCollector) handleTableName(node *sqlparser.AliasedTableExpr, t sqlparser.TableName) (err error) {
	var tableInfo TableInfo
	var found bool

	tableInfo, found = tc.done[node]
	if !found {
		tableInfo, err = tc.earlyTableCollector.getTableInfo(node, t, tc.scoper)
		if err != nil {
			return err
		}
		tc.Tables = append(tc.Tables, tableInfo)
	}

	scope := tc.scoper.currentScope()
	return scope.addTable(tableInfo)
}

func (etc *earlyTableCollector) getCTE(t sqlparser.TableName) *CTE {
	if t.Qualifier.NotEmpty() {
		return nil
	}

	return etc.cte[t.Name.String()]
}

func (etc *earlyTableCollector) getTableInfo(node *sqlparser.AliasedTableExpr, t sqlparser.TableName, sc *scoper) (TableInfo, error) {
	var tbl *vindexes.BaseTable
	var vindex vindexes.Vindex
	if cteDef := etc.getCTE(t); cteDef != nil {
		cte, err := etc.buildRecursiveCTE(node, t, sc, cteDef)
		if err != nil {
			return nil, err
		}
		if cte != nil {
			// if we didn't get a table, it means we can't build a recursive CTE,
			// so we need to look for a regular table instead
			return cte, nil
		}
	}

	isInfSchema := sqlparser.SystemSchema(t.Qualifier.String())
	var err error
	tbl, vindex, _, _, _, err = etc.si.FindTableOrVindex(t)
	if err != nil && !isInfSchema {
		// if we are dealing with a system table, it might not be available in the vschema, but that is OK
		return nil, err
	}
	if tbl == nil && vindex != nil {
		tbl = newVindexTable(t.Name)
	}

	tableInfo, err := etc.createTable(t, node, tbl, isInfSchema, vindex)
	if err != nil {
		return nil, err
	}
	return tableInfo, nil
}

func (etc *earlyTableCollector) buildRecursiveCTE(node *sqlparser.AliasedTableExpr, t sqlparser.TableName, sc *scoper, cteDef *CTE) (TableInfo, error) {
	// If sc is nil, then we are in the early table collector.
	// In early table collector, we don't go over the CTE definitions, so we must be seeing a usage of the CTE.
	if sc != nil && len(sc.commonTableExprScopes) > 0 {
		cte := sc.commonTableExprScopes[len(sc.commonTableExprScopes)-1]
		if cte.ID.String() == t.Name.String() {

			if err := checkValidRecursiveCTE(cteDef); err != nil {
				return nil, err
			}

			cteTable := newCTETable(node, t, cteDef)
			cteTableSet := SingleTableSet(len(etc.Tables))
			cteDef.IDForRecurse = &cteTableSet
			if !cteDef.Recursive {
				return nil, nil
			}
			return cteTable, nil
		}
	}
	return &RealTable{
		tableName:    node.TableNameString(),
		ASTNode:      node,
		CTE:          cteDef,
		collationEnv: etc.si.Environment().CollationEnv(),
	}, nil
}

func checkValidRecursiveCTE(cteDef *CTE) error {
	if cteDef.IDForRecurse != nil {
		return vterrors.VT09029(cteDef.Name)
	}

	union, isUnion := cteDef.Query.(*sqlparser.Union)
	if !isUnion {
		return vterrors.VT09026(cteDef.Name)
	}

	firstSelect, err := sqlparser.GetFirstSelect(union.Right)
	if err != nil {
		return err
	}
	if firstSelect.GroupBy != nil {
		return vterrors.VT09027(cteDef.Name)
	}

	for _, expr := range firstSelect.GetColumns() {
		if sqlparser.ContainsAggregation(expr) {
			return vterrors.VT09027(cteDef.Name)
		}
	}
	return nil
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
	size := sel.GetColumnCount()
	deps := make([]TableSet, size)
	types := make([]evalengine.Type, size)
	expanded := true
	for i, expr := range sel.GetColumns() {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			expanded = false
			continue
		}
		_, deps[i], types[i] = tc.org.depsForExpr(ae.Expr)
	}

	tableInfo := createDerivedTableForExpressions(sel.GetColumns(), columns, tables.tables, tc.org, expanded, deps, types)
	if err := tableInfo.checkForDuplicates(); err != nil {
		return err
	}

	tableInfo.ASTNode = tableExpr
	tableInfo.tableName = alias.String()

	tc.Tables = append(tc.Tables, tableInfo)
	scope := tc.scoper.currentScope()
	return scope.addTable(tableInfo)
}

func (tc *tableCollector) addUnionDerivedTable(
	union *sqlparser.Union,
	node *sqlparser.AliasedTableExpr,
	columns sqlparser.Columns,
	alias sqlparser.IdentifierCS,
) error {
	firstSelect, err := sqlparser.GetFirstSelect(union)
	if err != nil {
		return err
	}
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

func newVindexTable(t sqlparser.IdentifierCS) *vindexes.BaseTable {
	vindexCols := []vindexes.Column{
		{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_VARBINARY},
		{Name: sqlparser.NewIdentifierCI("keyspace_id"), Type: querypb.Type_VARBINARY},
		{Name: sqlparser.NewIdentifierCI("range_start"), Type: querypb.Type_VARBINARY},
		{Name: sqlparser.NewIdentifierCI("range_end"), Type: querypb.Type_VARBINARY},
		{Name: sqlparser.NewIdentifierCI("hex_keyspace_id"), Type: querypb.Type_VARBINARY},
		{Name: sqlparser.NewIdentifierCI("shard"), Type: querypb.Type_VARBINARY},
	}

	return &vindexes.BaseTable{
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

func (etc *earlyTableCollector) createTable(
	t sqlparser.TableName,
	alias *sqlparser.AliasedTableExpr,
	tbl *vindexes.BaseTable,
	isInfSchema bool,
	vindex vindexes.Vindex,
) (TableInfo, error) {
	hint := getVindexHint(alias.Hints)

	if err := checkValidVindexHints(hint, tbl); err != nil {
		return nil, err
	}

	tblName := t
	if tbl != nil && tbl.Keyspace != nil {
		tblName = tbl.GetTableName()
	}
	mr, err := etc.si.FindMirrorRule(tblName)
	if err != nil {
		// Mirroring is best effort. If we get an error while mirroring, keep going
		// as if mirroring was disabled. We don't want to interrupt production work
		// because of an issue with mirroring.
		mr = nil
	}

	table := &RealTable{
		tableName:    alias.As.String(),
		ASTNode:      alias,
		Table:        tbl,
		VindexHint:   hint,
		MirrorRule:   mr,
		isInfSchema:  isInfSchema,
		collationEnv: etc.si.Environment().CollationEnv(),
	}

	if alias.As.IsEmpty() {
		dbName := t.Qualifier.String()
		if dbName == "" {
			dbName = etc.currentDb
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

func checkValidVindexHints(hint *sqlparser.IndexHint, tbl *vindexes.BaseTable) error {
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
