package semantics

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// VindexTable contains a vindexes.Vindex and a TableInfo. The former represents the vindex
// we are keeping information about, and the latter represents the additional table information
// (usually a RealTable or an AliasedTable) of our vindex.
type VindexTable struct {
	Table  TableInfo
	Vindex vindexes.Vindex
}

var _ TableInfo = (*VindexTable)(nil)

// dependencies implements the TableInfo interface
func (v *VindexTable) dependencies(colName string, org originable) (dependencies, error) {
	return v.Table.dependencies(colName, org)
}

// GetTables implements the TableInfo interface
func (v *VindexTable) getTableSet(org originable) TableSet {
	return v.Table.getTableSet(org)
}

// GetExprFor implements the TableInfo interface
func (v *VindexTable) getExprFor(_ string) (sqlparser.Expr, error) {
	panic("implement me")
}

// GetVindexTable implements the TableInfo interface
func (v *VindexTable) GetVindexTable() *vindexes.Table {
	return v.Table.GetVindexTable()
}

// Matches implements the TableInfo interface
func (v *VindexTable) matches(name sqlparser.TableName) bool {
	return v.Table.matches(name)
}

// Authoritative implements the TableInfo interface
func (v *VindexTable) authoritative() bool {
	return true
}

// Name implements the TableInfo interface
func (v *VindexTable) Name() (sqlparser.TableName, error) {
	return v.Table.Name()
}

// GetExpr implements the TableInfo interface
func (v *VindexTable) getExpr() *sqlparser.AliasedTableExpr {
	return v.Table.getExpr()
}

// GetColumns implements the TableInfo interface
func (v *VindexTable) getColumns() []ColumnInfo {
	return v.Table.getColumns()
}

// IsInfSchema implements the TableInfo interface
func (v *VindexTable) IsInfSchema() bool {
	return v.Table.IsInfSchema()
}
