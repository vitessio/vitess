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
	"strings"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// DerivedTable contains the information about the projection, tables involved in derived table.
type DerivedTable struct {
	tableName       string
	ASTNode         *sqlparser.AliasedTableExpr
	columnNames     []string
	cols            []sqlparser.Expr
	tables          TableSet
	isAuthoritative bool
}

var _ TableInfo = (*DerivedTable)(nil)

func createDerivedTableForExpressions(expressions sqlparser.SelectExprs, cols sqlparser.Columns, tables []TableInfo, org originable) *DerivedTable {
	vTbl := &DerivedTable{isAuthoritative: true}
	for i, selectExpr := range expressions {
		switch expr := selectExpr.(type) {
		case *sqlparser.AliasedExpr:
			vTbl.cols = append(vTbl.cols, expr.Expr)
			if len(cols) > 0 {
				vTbl.columnNames = append(vTbl.columnNames, cols[i].String())
			} else if expr.As.IsEmpty() {
				switch expr := expr.Expr.(type) {
				case *sqlparser.ColName:
					// for projections, we strip out the qualifier and keep only the column name
					vTbl.columnNames = append(vTbl.columnNames, expr.Name.String())
				default:
					vTbl.columnNames = append(vTbl.columnNames, sqlparser.String(expr))
				}
			} else {
				vTbl.columnNames = append(vTbl.columnNames, expr.As.String())
			}
		case *sqlparser.StarExpr:
			for _, table := range tables {
				vTbl.tables = vTbl.tables.Merge(table.getTableSet(org))
				if !table.authoritative() {
					vTbl.isAuthoritative = false
				}
			}
		}
	}
	return vTbl
}

// dependencies implements the TableInfo interface
func (dt *DerivedTable) dependencies(colName string, org originable) (dependencies, error) {
	directDeps := org.tableSetFor(dt.ASTNode)
	for i, name := range dt.columnNames {
		if !strings.EqualFold(name, colName) {
			continue
		}
		_, recursiveDeps, qt := org.depsForExpr(dt.cols[i])

		return createCertain(directDeps, recursiveDeps, qt), nil
	}

	if !dt.hasStar() {
		return &nothing{}, nil
	}

	return createUncertain(directDeps, dt.tables), nil
}

// IsInfSchema implements the TableInfo interface
func (dt *DerivedTable) IsInfSchema() bool {
	return false
}

func (dt *DerivedTable) matches(name sqlparser.TableName) bool {
	return dt.tableName == name.Name.String() && name.Qualifier.IsEmpty()
}

func (dt *DerivedTable) authoritative() bool {
	return dt.isAuthoritative
}

// Name implements the TableInfo interface
func (dt *DerivedTable) Name() (sqlparser.TableName, error) {
	return dt.ASTNode.TableName()
}

func (dt *DerivedTable) GetExpr() *sqlparser.AliasedTableExpr {
	return dt.ASTNode
}

// GetVindexTable implements the TableInfo interface
func (dt *DerivedTable) GetVindexTable() *vindexes.Table {
	return nil
}

func (dt *DerivedTable) getColumns() []ColumnInfo {
	cols := make([]ColumnInfo, 0, len(dt.columnNames))
	for _, col := range dt.columnNames {
		cols = append(cols, ColumnInfo{
			Name: col,
		})
	}
	return cols
}

func (dt *DerivedTable) hasStar() bool {
	return dt.tables.NonEmpty()
}

// GetTables implements the TableInfo interface
func (dt *DerivedTable) getTableSet(_ originable) TableSet {
	return dt.tables
}

// GetExprFor implements the TableInfo interface
func (dt *DerivedTable) getExprFor(s string) (sqlparser.Expr, error) {
	for i, colName := range dt.columnNames {
		if colName == s {
			return dt.cols[i], nil
		}
	}
	return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadFieldError, "Unknown column '%s' in 'field list'", s)
}

func (dt *DerivedTable) checkForDuplicates() error {
	for i, name := range dt.columnNames {
		for j, name2 := range dt.columnNames {
			if i == j {
				continue
			}
			if name == name2 {
				return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.DupFieldName, "Duplicate column name '%s'", name)
			}
		}
	}
	return nil
}
