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
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	TableInfo interface {
		Matches(name sqlparser.TableName) bool
		Authoritative() bool
		Name() (sqlparser.TableName, error)
		GetExpr() *sqlparser.AliasedTableExpr
		GetColumns() []ColumnInfo
	}

	ColumnInfo struct {
		Name string
	}

	// RealTable contains the alias table expr and vindex table
	RealTable struct {
		dbName, tableName string
		ASTNode           *sqlparser.AliasedTableExpr
		Table             *vindexes.Table
	}

	// AliasedTable contains the alias table expr and vindex table
	AliasedTable struct {
		tableName string
		ASTNode   *sqlparser.AliasedTableExpr
		Table     *vindexes.Table
	}

	// TableSet is how a set of tables is expressed.
	// Tables get unique bits assigned in the order that they are encountered during semantic analysis
	TableSet uint64 // we can only join 64 tables with this underlying data type
	// TODO : change uint64 to struct to support arbitrary number of tables.

	// SemTable contains semantic analysis information about the query.
	SemTable struct {
		Tables           []*RealTable
		ITables          []TableInfo
		exprDependencies map[sqlparser.Expr]TableSet
		selectScope      map[*sqlparser.Select]*scope
	}

	scope struct {
		parent  *scope
		tables  []*RealTable
		itables []TableInfo
	}

	// SchemaInformation is used tp provide table information from Vschema.
	SchemaInformation interface {
		FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error)
	}
)

var _ TableInfo = (*RealTable)(nil)
var _ TableInfo = (*AliasedTable)(nil)

func vindexTableToColumnInfo(tbl *vindexes.Table) []ColumnInfo {
	cols := make([]ColumnInfo, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		cols = append(cols, ColumnInfo{
			Name: col.Name.String(),
		})
	}
	return cols
}

func (a *AliasedTable) GetColumns() []ColumnInfo {
	return vindexTableToColumnInfo(a.Table)
}

func (a *AliasedTable) GetExpr() *sqlparser.AliasedTableExpr {
	return a.ASTNode
}

func (a *AliasedTable) Name() (sqlparser.TableName, error) {
	return a.ASTNode.TableName()
}

func (a *AliasedTable) Authoritative() bool {
	return a.Table != nil && a.Table.ColumnListAuthoritative
}

func (a *AliasedTable) Matches(name sqlparser.TableName) bool {
	return a.tableName == name.Name.String() && name.Qualifier.IsEmpty()
}

func (r *RealTable) GetColumns() []ColumnInfo {
	return vindexTableToColumnInfo(r.Table)
}

func (r *RealTable) GetExpr() *sqlparser.AliasedTableExpr {
	return r.ASTNode
}

func (r *RealTable) Name() (sqlparser.TableName, error) {
	return r.ASTNode.TableName()
}

func (r *RealTable) Authoritative() bool {
	return r.Table != nil && r.Table.ColumnListAuthoritative
}

func (r *RealTable) Matches(name sqlparser.TableName) bool {
	if !name.Qualifier.IsEmpty() {
		if r.dbName != name.Qualifier.String() {
			return false
		}
	}
	return r.tableName == name.Name.String()
}

// NewSemTable creates a new empty SemTable
func NewSemTable() *SemTable {
	return &SemTable{exprDependencies: map[sqlparser.Expr]TableSet{}}
}

// TableSetFor returns the bitmask for this particular tableshoe
func (st *SemTable) TableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	for idx, t2 := range st.Tables {
		if t == t2.ASTNode {
			return 1 << idx
		}
	}
	return 0
}

// TableInfoFor returns the table info for the table set. It should contains only single table.
func (st *SemTable) TableInfoFor(id TableSet) (*RealTable, error) {
	if id.NumberOfTables() > 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should only be used for single tables")
	}
	return st.Tables[id.TableOffset()], nil
}

// Dependencies return the table dependencies of the expression.
func (st *SemTable) Dependencies(expr sqlparser.Expr) TableSet {
	deps, found := st.exprDependencies[expr]
	if found {
		return deps
	}

	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		colName, ok := node.(*sqlparser.ColName)
		if ok {
			set := st.exprDependencies[colName]
			deps |= set
		}
		return true, nil
	}, expr)

	st.exprDependencies[expr] = deps

	return deps
}

func (st *SemTable) GetSelectTables(node *sqlparser.Select) []TableInfo {
	scope := st.selectScope[node]
	return scope.itables
}

// AddExprs adds new select exprs to the SemTable.
func (st *SemTable) AddExprs(tbl *sqlparser.AliasedTableExpr, cols sqlparser.SelectExprs) {
	tableSet := st.TableSetFor(tbl)
	for _, col := range cols {
		st.exprDependencies[col.(*sqlparser.AliasedExpr).Expr] = tableSet
	}
}

func newScope(parent *scope) *scope {
	return &scope{parent: parent}
}

func (s *scope) addTable(info TableInfo, table *RealTable) error {
	for _, scopeTable := range s.tables {
		b := scopeTable.tableName == table.tableName
		b2 := scopeTable.dbName == table.dbName
		if b && b2 {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUniqTable, "Not unique table/alias: '%s'", table.tableName)
		}
	}

	s.tables = append(s.tables, table)
	s.itables = append(s.itables, info)
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
