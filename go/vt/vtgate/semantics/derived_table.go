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
	"vitess.io/vitess/go/vt/vtgate/evalengine"
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

	recursive []TableSet
	types     []evalengine.Type
}

type unionInfo struct {
	isAuthoritative bool
	recursive       []TableSet
	types           []evalengine.Type
	exprs           sqlparser.SelectExprs
}

var _ TableInfo = (*DerivedTable)(nil)

func createDerivedTableForExpressions(
	expressions sqlparser.SelectExprs,
	cols sqlparser.Columns,
	tables []TableInfo,
	org originable,
	expanded bool,
	recursiveDeps []TableSet,
	types []evalengine.Type,
) *DerivedTable {
	vTbl := &DerivedTable{isAuthoritative: expanded, recursive: recursiveDeps, types: types}
	for i, selectExpr := range expressions {
		switch expr := selectExpr.(type) {
		case *sqlparser.AliasedExpr:
			handleAliasedExpr(vTbl, expr, cols, i)
		case *sqlparser.StarExpr:
			handleUnexpandedStarExpression(tables, vTbl, org)
		}
	}
	return vTbl
}

func handleAliasedExpr(vTbl *DerivedTable, expr *sqlparser.AliasedExpr, cols sqlparser.Columns, i int) {
	vTbl.cols = append(vTbl.cols, expr.Expr)

	if len(cols) > 0 {
		vTbl.columnNames = append(vTbl.columnNames, cols[i].String())
		return
	}

	if expr.As.NotEmpty() {
		vTbl.columnNames = append(vTbl.columnNames, expr.As.String())
		return
	}

	switch expr := expr.Expr.(type) {
	case *sqlparser.ColName:
		// for projections, we strip out the qualifier and keep only the column name
		vTbl.columnNames = append(vTbl.columnNames, expr.Name.String())
	default:
		vTbl.columnNames = append(vTbl.columnNames, sqlparser.String(expr))
	}
}

func handleUnexpandedStarExpression(tables []TableInfo, vTbl *DerivedTable, org originable) {
	for _, table := range tables {
		vTbl.tables = vTbl.tables.Merge(table.getTableSet(org))
		if !table.authoritative() {
			vTbl.isAuthoritative = false
		}
	}
}

// dependencies implements the TableInfo interface
func (dt *DerivedTable) dependencies(colName string, org originable) (dependencies, error) {
	directDeps := org.tableSetFor(dt.ASTNode)
	for i, name := range dt.columnNames {
		if !strings.EqualFold(name, colName) {
			continue
		}
		if len(dt.recursive) == 0 {
			// we have unexpanded columns and can't figure this out
			return nil, ShardedError{Inner: vterrors.VT09015()}
		}
		recursiveDeps, qt := dt.recursive[i], dt.types[i]

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

func (dt *DerivedTable) GetAliasedTableExpr() *sqlparser.AliasedTableExpr {
	return dt.ASTNode
}

func (dt *DerivedTable) canShortCut() shortCut {
	panic(vterrors.VT12001("should not be called"))
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
	return dt.tables.NotEmpty()
}

// GetTables implements the TableInfo interface
func (dt *DerivedTable) getTableSet(_ originable) TableSet {
	return dt.tables
}

// GetExprFor implements the TableInfo interface
func (dt *DerivedTable) getExprFor(s string) (sqlparser.Expr, error) {
	if !dt.isAuthoritative {
		return nil, vterrors.VT09015()
	}
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
