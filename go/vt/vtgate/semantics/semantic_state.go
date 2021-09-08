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
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	// TableInfo contains information about tables
	TableInfo interface {
		Matches(name sqlparser.TableName) bool
		Authoritative() bool
		Name() (sqlparser.TableName, error)
		GetExpr() *sqlparser.AliasedTableExpr
		GetVindexTable() *vindexes.Table
		GetColumns() []ColumnInfo
		IsActualTable() bool

		// RecursiveDepsFor returns a pointer to the table set for the table that this column belongs to, if it can be found
		// if the column is not found, nil will be returned instead. If the column is a derived table column, this method
		// will recursively find the dependencies of the expression inside the derived table
		RecursiveDepsFor(col *sqlparser.ColName, org originable, single bool) (*TableSet, *querypb.Type, error)

		// DepsFor finds the table that a column depends on. No recursing is done on derived tables
		DepsFor(col *sqlparser.ColName, org originable, single bool) (*TableSet, error)
		IsInfSchema() bool
		GetExprFor(s string) (sqlparser.Expr, error)
		GetTables() []TableInfo
	}

	// ColumnInfo contains information about columns
	ColumnInfo struct {
		Name string
		Type querypb.Type
	}

	// RealTable contains the alias table expr and vindex table
	RealTable struct {
		dbName, tableName string
		ASTNode           *sqlparser.AliasedTableExpr
		Table             *vindexes.Table
		isInfSchema       bool
	}

	// AliasedTable contains the alias table expr and vindex table
	AliasedTable struct {
		tableName   string
		ASTNode     *sqlparser.AliasedTableExpr
		Table       *vindexes.Table
		isInfSchema bool
	}

	// vTableInfo is used to represent projected results, not real tables. It is used for
	// ORDER BY, GROUP BY and HAVING that need to access result columns, and also for derived tables.
	vTableInfo struct {
		tableName   string
		ASTNode     *sqlparser.AliasedTableExpr
		columnNames []string
		cols        []sqlparser.Expr
		tables      []TableInfo
	}

	// VindexTable contains a vindexes.Vindex and a TableInfo. The former represents the vindex
	// we are keeping information about, and the latter represents the additional table information
	// (usually a RealTable or an AliasedTable) of our vindex.
	VindexTable struct {
		Table  TableInfo
		Vindex vindexes.Vindex
	}

	// TableSet is how a set of tables is expressed.
	// Tables get unique bits assigned in the order that they are encountered during semantic analysis
	TableSet uint64 // we can only join 64 tables with this underlying data type
	// TODO : change uint64 to struct to support arbitrary number of tables.

	// ExprDependencies stores the tables that an expression depends on as a map
	ExprDependencies map[sqlparser.Expr]TableSet

	// SemTable contains semantic analysis information about the query.
	SemTable struct {
		Tables []TableInfo
		// ProjectionErr stores the error that we got during the semantic analysis of the SelectExprs.
		// This is only a real error if we are unable to plan the query as a single route
		ProjectionErr error

		// ExprBaseTableDeps contains the dependencies from the expression to the actual tables
		// in the query (i.e. not including derived tables). If an expression is a column on a derived table,
		// this map will contain the accumulated dependencies for the column expression inside the derived table
		ExprBaseTableDeps ExprDependencies

		// ExprDeps keeps information about dependencies for expressions, no matter if they are
		// against real tables or derived tables
		ExprDeps ExprDependencies

		exprTypes   map[sqlparser.Expr]querypb.Type
		selectScope map[*sqlparser.Select]*scope
		Comments    sqlparser.Comments
		SubqueryMap map[*sqlparser.Select][]*subquery
		SubqueryRef map[*sqlparser.Subquery]*subquery

		// ColumnEqualities is used to enable transitive closures
		// if a == b and b == c then a == c
		ColumnEqualities map[columnName][]sqlparser.Expr
	}

	columnName struct {
		Table      TableSet
		ColumnName string
	}

	subquery struct {
		ArgName  string
		SubQuery *sqlparser.Subquery
		OpCode   engine.PulloutOpcode
	}

	scope struct {
		parent     *scope
		selectStmt *sqlparser.Select
		tables     []TableInfo
	}

	// SchemaInformation is used tp provide table information from Vschema.
	SchemaInformation interface {
		FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error)
	}
)

// GetTables implements the TableInfo interface
func (v *VindexTable) GetTables() []TableInfo {
	return v.Table.GetTables()
}

// GetTables implements the TableInfo interface
func (v *vTableInfo) GetTables() []TableInfo {
	return v.tables
}

// GetTables implements the TableInfo interface
func (a *AliasedTable) GetTables() []TableInfo {
	return []TableInfo{a}
}

// GetTables implements the TableInfo interface
func (r *RealTable) GetTables() []TableInfo {
	return []TableInfo{r}
}

// GetExprFor implements the TableInfo interface
func (v *VindexTable) GetExprFor(_ string) (sqlparser.Expr, error) {
	panic("implement me")
}

// CopyDependencies copies the dependencies from one expression into the other
func (st *SemTable) CopyDependencies(from, to sqlparser.Expr) {
	st.ExprBaseTableDeps[to] = st.BaseTableDependencies(from)
	st.ExprDeps[to] = st.Dependencies(from)
}

// GetExprFor implements the TableInfo interface
func (v *vTableInfo) GetExprFor(s string) (sqlparser.Expr, error) {
	for i, colName := range v.columnNames {
		if colName == s {
			return v.cols[i], nil
		}
	}
	return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadFieldError, "Unknown column '%s' in 'field list'", s)
}

// GetExprFor implements the TableInfo interface
func (a *AliasedTable) GetExprFor(s string) (sqlparser.Expr, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Unknown column '%s' in 'field list'", s)
}

// GetExprFor implements the TableInfo interface
func (r *RealTable) GetExprFor(s string) (sqlparser.Expr, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Unknown column '%s' in 'field list'", s)
}

// RecursiveDepsFor implements the TableInfo interface
func (v *vTableInfo) RecursiveDepsFor(col *sqlparser.ColName, org originable, _ bool) (*TableSet, *querypb.Type, error) {
	if !col.Qualifier.IsEmpty() && (v.ASTNode == nil || v.tableName != col.Qualifier.Name.String()) {
		// if we have a table qualifier in the expression, we know that it is not referencing an aliased table
		return nil, nil, nil
	}
	var tsF TableSet
	var qtF *querypb.Type
	found := false
	for i, colName := range v.columnNames {
		if col.Name.String() == colName {
			ts, qt := org.depsForExpr(v.cols[i])
			if !found {
				tsF = ts
				qtF = qt
			} else if tsF != ts {
				// the column does not resolve to the same TableSet. Therefore, it is an ambiguous column reference.
				return nil, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUniqError, "Column '%s' is ambiguous", colName)
			}
			found = true
		}
	}
	if found {
		return &tsF, qtF, nil
	}
	if len(v.tables) == 0 {
		return nil, nil, nil
	}
	for _, table := range v.tables {
		tsF |= org.tableSetFor(table.GetExpr())
	}
	return &tsF, nil, nil
}

// DepsFor implements the TableInfo interface
func (v *vTableInfo) DepsFor(col *sqlparser.ColName, org originable, _ bool) (*TableSet, error) {
	if v.ASTNode == nil {
		return nil, nil
	}
	if !col.Qualifier.IsEmpty() && (v.ASTNode == nil || v.tableName != col.Qualifier.Name.String()) {
		// if we have a table qualifier in the expression, we know that it is not referencing an aliased table
		return nil, nil
	}
	for _, colName := range v.columnNames {
		if col.Name.String() == colName {
			ts := org.tableSetFor(v.ASTNode)
			return &ts, nil
		}
	}
	if len(v.tables) == 0 {
		return nil, nil
	}
	var ts TableSet
	for range v.tables {
		ts |= org.tableSetFor(v.ASTNode)
	}
	return &ts, nil
}

// RecursiveDepsFor implements the TableInfo interface
func (a *AliasedTable) RecursiveDepsFor(col *sqlparser.ColName, org originable, single bool) (*TableSet, *querypb.Type, error) {
	return depsFor(col, org, single, a.ASTNode, a.GetColumns(), a.Authoritative())
}

// DepsFor implements the TableInfo interface
func (a *AliasedTable) DepsFor(col *sqlparser.ColName, org originable, single bool) (*TableSet, error) {
	ts, _, err := a.RecursiveDepsFor(col, org, single)
	return ts, err
}

// RecursiveDepsFor implements the TableInfo interface
func (r *RealTable) RecursiveDepsFor(col *sqlparser.ColName, org originable, single bool) (*TableSet, *querypb.Type, error) {
	return depsFor(col, org, single, r.ASTNode, r.GetColumns(), r.Authoritative())
}

// DepsFor implements the TableInfo interface
func (r *RealTable) DepsFor(col *sqlparser.ColName, org originable, single bool) (*TableSet, error) {
	ts, _, err := r.RecursiveDepsFor(col, org, single)
	return ts, err
}

// depsFor implements the TableInfo interface for RealTable and AliasedTable
func depsFor(
	col *sqlparser.ColName,
	org originable,
	single bool,
	astNode *sqlparser.AliasedTableExpr,
	cols []ColumnInfo,
	authoritative bool,
) (*TableSet, *querypb.Type, error) {
	// if we know that we are the only table in the scope, there is no doubt - the column must belong to the table
	if single {
		ts := org.tableSetFor(astNode)

		for _, info := range cols {
			if col.Name.EqualString(info.Name) {
				return &ts, &info.Type, nil
			}
		}

		if authoritative {
			// if we are authoritative and we can't find the column, we should fail
			return nil, nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadFieldError, "Unknown column '%s' in 'field list'", col.Name.String())
		}

		// it's probably the correct table, but we don't have enough info to be sure or figure out the type of the column
		return &ts, nil, nil
	}

	for _, info := range cols {
		if col.Name.EqualString(info.Name) {
			ts := org.tableSetFor(astNode)
			return &ts, &info.Type, nil
		}
	}
	return nil, nil, nil
}

// IsInfSchema implements the TableInfo interface
func (v *vTableInfo) IsInfSchema() bool {
	return false
}

// IsInfSchema implements the TableInfo interface
func (a *AliasedTable) IsInfSchema() bool {
	return a.isInfSchema
}

// IsInfSchema implements the TableInfo interface
func (r *RealTable) IsInfSchema() bool {
	return r.isInfSchema
}

// IsActualTable implements the TableInfo interface
func (v *vTableInfo) IsActualTable() bool {
	return false
}

// IsActualTable implements the TableInfo interface
func (a *AliasedTable) IsActualTable() bool {
	return true
}

// IsActualTable implements the TableInfo interface
func (r *RealTable) IsActualTable() bool {
	return true
}

var _ TableInfo = (*RealTable)(nil)
var _ TableInfo = (*AliasedTable)(nil)
var _ TableInfo = (*vTableInfo)(nil)
var _ TableInfo = (*VindexTable)(nil)

func (v *vTableInfo) Matches(name sqlparser.TableName) bool {
	return v.tableName == name.Name.String() && name.Qualifier.IsEmpty()
}

func (v *vTableInfo) Authoritative() bool {
	return true
}

func (v *vTableInfo) Name() (sqlparser.TableName, error) {
	return v.ASTNode.TableName()
}

func (v *vTableInfo) GetExpr() *sqlparser.AliasedTableExpr {
	return v.ASTNode
}

// GetVindexTable implements the TableInfo interface
func (v *vTableInfo) GetVindexTable() *vindexes.Table {
	return nil
}

// GetVindexTable implements the TableInfo interface
func (v *VindexTable) GetVindexTable() *vindexes.Table {
	return v.Table.GetVindexTable()
}

func (v *vTableInfo) GetColumns() []ColumnInfo {
	cols := make([]ColumnInfo, 0, len(v.columnNames))
	for _, col := range v.columnNames {
		cols = append(cols, ColumnInfo{
			Name: col,
		})
	}
	return cols
}

func vindexTableToColumnInfo(tbl *vindexes.Table) []ColumnInfo {
	if tbl == nil {
		return nil
	}
	nameMap := map[string]interface{}{}
	cols := make([]ColumnInfo, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		cols = append(cols, ColumnInfo{
			Name: col.Name.String(),
			Type: col.Type,
		})
		nameMap[col.Name.String()] = nil
	}
	// If table is authoritative, we do not need ColumnVindexes to help in resolving the unqualified columns.
	if tbl.ColumnListAuthoritative {
		return cols
	}
	for _, vindex := range tbl.ColumnVindexes {
		for _, column := range vindex.Columns {
			name := column.String()
			if _, exists := nameMap[name]; exists {
				continue
			}
			cols = append(cols, ColumnInfo{
				Name: name,
			})
			nameMap[name] = nil
		}
	}
	return cols
}

// GetColumns implements the TableInfo interface
func (a *AliasedTable) GetColumns() []ColumnInfo {
	return vindexTableToColumnInfo(a.Table)
}

// GetExpr implements the TableInfo interface
func (a *AliasedTable) GetExpr() *sqlparser.AliasedTableExpr {
	return a.ASTNode
}

// GetVindexTable implements the TableInfo interface
func (a *AliasedTable) GetVindexTable() *vindexes.Table {
	return a.Table
}

// Name implements the TableInfo interface
func (a *AliasedTable) Name() (sqlparser.TableName, error) {
	return a.ASTNode.TableName()
}

// Authoritative implements the TableInfo interface
func (a *AliasedTable) Authoritative() bool {
	return a.Table != nil && a.Table.ColumnListAuthoritative
}

// Matches implements the TableInfo interface
func (a *AliasedTable) Matches(name sqlparser.TableName) bool {
	return a.tableName == name.Name.String() && name.Qualifier.IsEmpty()
}

// GetColumns implements the TableInfo interface
func (r *RealTable) GetColumns() []ColumnInfo {
	return vindexTableToColumnInfo(r.Table)
}

// GetExpr implements the TableInfo interface
func (r *RealTable) GetExpr() *sqlparser.AliasedTableExpr {
	return r.ASTNode
}

// GetVindexTable implements the TableInfo interface
func (r *RealTable) GetVindexTable() *vindexes.Table {
	return r.Table
}

// Name implements the TableInfo interface
func (r *RealTable) Name() (sqlparser.TableName, error) {
	return r.ASTNode.TableName()
}

// Authoritative implements the TableInfo interface
func (r *RealTable) Authoritative() bool {
	return r.Table != nil && r.Table.ColumnListAuthoritative
}

// Matches implements the TableInfo interface
func (r *RealTable) Matches(name sqlparser.TableName) bool {
	if !name.Qualifier.IsEmpty() {
		if r.dbName != name.Qualifier.String() {
			return false
		}
	}
	return r.tableName == name.Name.String()
}

// Matches implements the TableInfo interface
func (v *VindexTable) Matches(name sqlparser.TableName) bool {
	return v.Table.Matches(name)
}

// Authoritative implements the TableInfo interface
func (v *VindexTable) Authoritative() bool {
	return true
}

// Name implements the TableInfo interface
func (v *VindexTable) Name() (sqlparser.TableName, error) {
	return v.Table.Name()
}

// GetExpr implements the TableInfo interface
func (v *VindexTable) GetExpr() *sqlparser.AliasedTableExpr {
	return v.Table.GetExpr()
}

// GetColumns implements the TableInfo interface
func (v *VindexTable) GetColumns() []ColumnInfo {
	return v.Table.GetColumns()
}

// IsActualTable implements the TableInfo interface
func (v *VindexTable) IsActualTable() bool {
	return true
}

// RecursiveDepsFor implements the TableInfo interface
func (v *VindexTable) RecursiveDepsFor(col *sqlparser.ColName, org originable, single bool) (*TableSet, *querypb.Type, error) {
	return v.Table.RecursiveDepsFor(col, org, single)
}

// DepsFor implements the TableInfo interface
func (v *VindexTable) DepsFor(col *sqlparser.ColName, org originable, single bool) (*TableSet, error) {
	return v.Table.DepsFor(col, org, single)
}

// IsInfSchema implements the TableInfo interface
func (v *VindexTable) IsInfSchema() bool {
	return v.Table.IsInfSchema()
}

// NewSemTable creates a new empty SemTable
func NewSemTable() *SemTable {
	return &SemTable{ExprBaseTableDeps: map[sqlparser.Expr]TableSet{}, ColumnEqualities: map[columnName][]sqlparser.Expr{}}
}

// TableSetFor returns the bitmask for this particular table
func (st *SemTable) TableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	for idx, t2 := range st.Tables {
		if t == t2.GetExpr() {
			return 1 << idx
		}
	}
	return 0
}

// TableInfoFor returns the table info for the table set. It should contains only single table.
func (st *SemTable) TableInfoFor(id TableSet) (TableInfo, error) {
	if id.NumberOfTables() > 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should only be used for single tables")
	}
	return st.Tables[id.TableOffset()], nil
}

// BaseTableDependencies return the table dependencies of the expression.
func (st *SemTable) BaseTableDependencies(expr sqlparser.Expr) TableSet {
	return st.ExprBaseTableDeps.Dependencies(expr)
}

// Dependencies return the table dependencies of the expression.
func (st *SemTable) Dependencies(expr sqlparser.Expr) TableSet {
	return st.ExprDeps.Dependencies(expr)
}

// AddColumnEquality adds a relation of the given colName to the ColumnEqualities map
func (st *SemTable) AddColumnEquality(colName *sqlparser.ColName, expr sqlparser.Expr) {
	ts := st.ExprDeps.Dependencies(colName)
	columnName := columnName{
		Table:      ts,
		ColumnName: colName.Name.String(),
	}
	elem := st.ColumnEqualities[columnName]
	elem = append(elem, expr)
	st.ColumnEqualities[columnName] = elem
}

// GetExprAndEqualities returns a slice containing the given expression, and it's known equalities if any
func (st *SemTable) GetExprAndEqualities(expr sqlparser.Expr) []sqlparser.Expr {
	result := []sqlparser.Expr{expr}
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		table := st.Dependencies(expr)
		k := columnName{Table: table, ColumnName: expr.Name.String()}
		result = append(result, st.ColumnEqualities[k]...)
	}
	return result
}

// TableInfoForExpr returns the table info of the table that this expression depends on.
// Careful: this only works for expressions that have a single table dependency
func (st *SemTable) TableInfoForExpr(expr sqlparser.Expr) (TableInfo, error) {
	return st.TableInfoFor(st.ExprDeps.Dependencies(expr))
}

// GetSelectTables returns the table in the select.
func (st *SemTable) GetSelectTables(node *sqlparser.Select) []TableInfo {
	scope := st.selectScope[node]
	return scope.tables
}

// AddExprs adds new select exprs to the SemTable.
func (st *SemTable) AddExprs(tbl *sqlparser.AliasedTableExpr, cols sqlparser.SelectExprs) {
	tableSet := st.TableSetFor(tbl)
	for _, col := range cols {
		st.ExprBaseTableDeps[col.(*sqlparser.AliasedExpr).Expr] = tableSet
	}
}

// TypeFor returns the type of expressions in the query
func (st *SemTable) TypeFor(e sqlparser.Expr) *querypb.Type {
	typ, found := st.exprTypes[e]
	if found {
		return &typ
	}
	return nil
}

// Dependencies return the table dependencies of the expression. This method finds table dependencies recursively
func (d ExprDependencies) Dependencies(expr sqlparser.Expr) TableSet {
	deps, found := d[expr]
	if found {
		return deps
	}

	// During the original semantic analysis, all ColName:s were found and bound the the corresponding tables
	// Here, we'll walk the expression tree and look to see if we can found any sub-expressions
	// that have already set dependencies.
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		expr, ok := node.(sqlparser.Expr)
		if !ok || !validAsMapKey(expr) {
			// if this is not an expression, or it is an expression we can't use as a map-key,
			// just carry on down the tree
			return true, nil
		}

		set, found := d[expr]
		if found {
			deps |= set
		}

		// if we found a cached value, there is no need to continue down to visit children
		return !found, nil
	}, expr)

	d[expr] = deps
	return deps
}

func newScope(parent *scope) *scope {
	return &scope{parent: parent}
}

func (s *scope) addTable(info TableInfo) error {
	for _, scopeTable := range s.tables {
		scopeTableName, err := scopeTable.Name()
		if err != nil {
			return err
		}
		if info.Matches(scopeTableName) {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUniqTable, "Not unique table/alias: '%s'", scopeTableName.Name.String())
		}
	}
	s.tables = append(s.tables, info)
	return nil
}

// IsOverlapping returns true if at least one table exists in both sets
func (ts TableSet) IsOverlapping(b TableSet) bool { return ts&b != 0 }

// IsSolvedBy returns true if all of `ts` is contained in `b`
func (ts TableSet) IsSolvedBy(b TableSet) bool { return ts&b == ts }

// NumberOfTables returns the number of bits set
func (ts TableSet) NumberOfTables() int {
	// Brian Kernighan’s Algorithm
	count := 0
	for ts > 0 {
		ts &= ts - 1
		count++
	}
	return count
}

// TableOffset returns the offset in the Tables array from TableSet
func (ts TableSet) TableOffset() int {
	offset := 0
	for ts > 1 {
		ts = ts >> 1
		offset++
	}
	return offset
}

// Constituents returns an slice with all the
// individual tables in their own TableSet identifier
func (ts TableSet) Constituents() (result []TableSet) {
	mask := ts

	for mask > 0 {
		maskLeft := mask & (mask - 1)
		constituent := mask ^ maskLeft
		mask = maskLeft
		result = append(result, constituent)
	}
	return
}

// Merge creates a TableSet that contains both inputs
func (ts TableSet) Merge(other TableSet) TableSet {
	return ts | other
}

// RewriteDerivedExpression rewrites all the ColName instances in the supplied expression with
// the expressions behind the column definition of the derived table
// SELECT foo FROM (SELECT id+42 as foo FROM user) as t
// We need `foo` to be translated to `id+42` on the inside of the derived table
func RewriteDerivedExpression(expr sqlparser.Expr, vt TableInfo) (sqlparser.Expr, error) {
	newExpr := sqlparser.CloneExpr(expr)
	sqlparser.Rewrite(newExpr, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.ColName:
			exp, err := vt.GetExprFor(node.Name.String())
			if err != nil {
				return false
			}
			cursor.Replace(exp)
			return false
		}
		return true
	}, nil)
	return newExpr, nil
}
