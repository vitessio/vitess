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
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// RealTable contains the alias table expr and vindex table
type RealTable struct {
	dbName, tableName string
	ASTNode           *sqlparser.AliasedTableExpr
	Table             *vindexes.Table
	VindexHint        *sqlparser.IndexHint
	isInfSchema       bool
	collationEnv      *collations.Environment
}

var _ TableInfo = (*RealTable)(nil)

// dependencies implements the TableInfo interface
func (r *RealTable) dependencies(colName string, org originable) (dependencies, error) {
	ts := org.tableSetFor(r.ASTNode)
	for _, info := range r.getColumns() {
		if strings.EqualFold(info.Name, colName) {
			return createCertain(ts, ts, info.Type), nil
		}
	}

	if r.authoritative() {
		return &nothing{}, nil
	}
	return createUncertain(ts, ts), nil
}

// GetTables implements the TableInfo interface
func (r *RealTable) getTableSet(org originable) TableSet {
	return org.tableSetFor(r.ASTNode)
}

// GetExprFor implements the TableInfo interface
func (r *RealTable) getExprFor(s string) (sqlparser.Expr, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Unknown column '%s' in 'field list'", s)
}

// IsInfSchema implements the TableInfo interface
func (r *RealTable) IsInfSchema() bool {
	return r.isInfSchema
}

// GetColumns implements the TableInfo interface
func (r *RealTable) getColumns() []ColumnInfo {
	return vindexTableToColumnInfo(r.Table, r.collationEnv)
}

// GetExpr implements the TableInfo interface
func (r *RealTable) GetAliasedTableExpr() *sqlparser.AliasedTableExpr {
	return r.ASTNode
}

func (r *RealTable) canShortCut() shortCut {
	if r.Table == nil {
		return cannotShortCut
	}
	if r.Table.Type != "" {
		// A reference table is not an issue when seeing if a query is going to an unsharded keyspace
		if r.Table.Type == vindexes.TypeReference {
			return canShortCut
		}
		return cannotShortCut
	}

	name, ok := r.ASTNode.Expr.(sqlparser.TableName)
	if !ok || name.Name.String() != r.Table.Name.String() {
		return cannotShortCut
	}

	return dependsOnKeyspace
}

// GetVindexTable implements the TableInfo interface
func (r *RealTable) GetVindexTable() *vindexes.Table {
	return r.Table
}

// GetVindexHint implements the TableInfo interface
func (r *RealTable) GetVindexHint() *sqlparser.IndexHint {
	return r.VindexHint
}

// Name implements the TableInfo interface
func (r *RealTable) Name() (sqlparser.TableName, error) {
	return r.ASTNode.TableName()
}

// Authoritative implements the TableInfo interface
func (r *RealTable) authoritative() bool {
	return r.Table != nil && r.Table.ColumnListAuthoritative
}

// Matches implements the TableInfo interface
func (r *RealTable) matches(name sqlparser.TableName) bool {
	return (name.Qualifier.IsEmpty() || name.Qualifier.String() == r.dbName) && r.tableName == name.Name.String()
}

func vindexTableToColumnInfo(tbl *vindexes.Table, collationEnv *collations.Environment) []ColumnInfo {
	if tbl == nil {
		return nil
	}
	nameMap := map[string]any{}
	cols := make([]ColumnInfo, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		cols = append(cols, ColumnInfo{
			Name:      col.Name.String(),
			Type:      col.ToEvalengineType(collationEnv),
			Invisible: col.Invisible,
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
