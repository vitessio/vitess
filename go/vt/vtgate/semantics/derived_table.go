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

type derivedTable struct {
	tableName   string
	ASTNode     *sqlparser.AliasedTableExpr
	columnNames []string
	cols        []sqlparser.Expr
	tables      []TableInfo
}

var _ TableInfo = (*derivedTable)(nil)

func createDerivedTableForExpressions(expressions sqlparser.SelectExprs, tables []TableInfo) *derivedTable {
	vTbl := &derivedTable{}
	for _, selectExpr := range expressions {
		switch expr := selectExpr.(type) {
		case *sqlparser.AliasedExpr:
			vTbl.cols = append(vTbl.cols, expr.Expr)
			if expr.As.IsEmpty() {
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
				vTbl.tables = append(vTbl.tables, table.GetTables()...)
			}
		}
	}
	return vTbl
}

func (dt *derivedTable) Dependencies(colName string, org originable) (dependencies, error) {
	for i, name := range dt.columnNames {
		if name != colName {
			continue
		}
		recursiveDeps, qt := org.depsForExpr(dt.cols[i])

		// TODO: deal with nil ASTNodes (group/order by) better
		directDeps := recursiveDeps
		if dt.ASTNode != nil {
			directDeps = org.tableSetFor(dt.ASTNode)
		}
		return createCertain(directDeps, recursiveDeps, qt), nil
	}

	if !dt.hasStar() {
		return &nothing{}, nil
	}

	var ts TableSet
	for _, table := range dt.tables {
		ts |= org.tableSetFor(table.GetExpr())
	}

	d := dependency{
		direct:    ts,
		recursive: ts,
	}
	if dt.ASTNode != nil {
		d.direct = org.tableSetFor(dt.ASTNode)
	}
	return &uncertain{
		dependency: d,
	}, nil
}

// IsInfSchema implements the TableInfo interface
func (dt *derivedTable) IsInfSchema() bool {
	return false
}

// IsActualTable implements the TableInfo interface
func (dt *derivedTable) IsActualTable() bool {
	return false
}

func (dt *derivedTable) Matches(name sqlparser.TableName) bool {
	return dt.tableName == name.Name.String() && name.Qualifier.IsEmpty()
}

func (dt *derivedTable) Authoritative() bool {
	return true
}

func (dt *derivedTable) Name() (sqlparser.TableName, error) {
	return dt.ASTNode.TableName()
}

func (dt *derivedTable) GetExpr() *sqlparser.AliasedTableExpr {
	return dt.ASTNode
}

// GetVindexTable implements the TableInfo interface
func (dt *derivedTable) GetVindexTable() *vindexes.Table {
	return nil
}

func (dt *derivedTable) GetColumns() []ColumnInfo {
	cols := make([]ColumnInfo, 0, len(dt.columnNames))
	for _, col := range dt.columnNames {
		cols = append(cols, ColumnInfo{
			Name: col,
		})
	}
	return cols
}

func (dt *derivedTable) hasStar() bool {
	return len(dt.tables) > 0
}

// GetTables implements the TableInfo interface
func (dt *derivedTable) GetTables() []TableInfo {
	return dt.tables
}

// GetExprFor implements the TableInfo interface
func (dt *derivedTable) GetExprFor(s string) (sqlparser.Expr, error) {
	for i, colName := range dt.columnNames {
		if colName == s {
			return dt.cols[i], nil
		}
	}
	return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadFieldError, "Unknown column '%s' in 'field list'", s)
}

// RecursiveDepsFor implements the TableInfo interface
func (dt *derivedTable) RecursiveDepsFor(col *sqlparser.ColName, org originable, _ bool) (*TableSet, *querypb.Type, error) {
	if !col.Qualifier.IsEmpty() && (dt.ASTNode == nil || dt.tableName != col.Qualifier.Name.String()) {
		// if we have a table qualifier in the expression, we know that it is not referencing an aliased table
		return nil, nil, nil
	}
	var tsF TableSet
	var qtF *querypb.Type
	found := false
	for i, colName := range dt.columnNames {
		if col.Name.String() == colName {
			ts, qt := org.depsForExpr(dt.cols[i])
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
	if len(dt.tables) == 0 {
		return nil, nil, nil
	}
	for _, table := range dt.tables {
		tsF |= org.tableSetFor(table.GetExpr())
	}
	return &tsF, nil, nil
}

// DepsFor implements the TableInfo interface
func (dt *derivedTable) DepsFor(col *sqlparser.ColName, org originable, _ bool) (*TableSet, error) {
	if dt.ASTNode == nil {
		return nil, nil
	}
	if !col.Qualifier.IsEmpty() && (dt.ASTNode == nil || dt.tableName != col.Qualifier.Name.String()) {
		// if we have a table qualifier in the expression, we know that it is not referencing an aliased table
		return nil, nil
	}
	for _, colName := range dt.columnNames {
		if col.Name.String() == colName {
			ts := org.tableSetFor(dt.ASTNode)
			return &ts, nil
		}
	}
	if len(dt.tables) == 0 {
		return nil, nil
	}
	var ts TableSet
	for range dt.tables {
		ts |= org.tableSetFor(dt.ASTNode)
	}
	return &ts, nil
}

func (dt *derivedTable) checkForDuplicates() error {
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
