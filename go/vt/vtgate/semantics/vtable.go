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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// vTableInfo is used to represent projected results, not real tables. It is used for
// ORDER BY, GROUP BY and HAVING that need to access result columns
type vTableInfo struct {
	tableName   string
	columnNames []string
	cols        []sqlparser.Expr
	tables      TableSet
}

var _ TableInfo = (*vTableInfo)(nil)

// Dependencies implements the TableInfo interface
func (v *vTableInfo) Dependencies(colName string, org originable) (dependencies, error) {
	var deps dependencies = &nothing{}
	var err error
	for i, name := range v.columnNames {
		if name != colName {
			continue
		}
		recursiveDeps, qt := org.depsForExpr(v.cols[i])

		var directDeps TableSet
		/*
				If we find a match, it means the query looks something like:
				SELECT 1 as x FROM t1 ORDER BY/GROUP BY x - d/r: 0/0
				SELECT t1.x as x FROM t1 ORDER BY/GROUP BY x - d/r: 0/1
				SELECT x FROM t1 ORDER BY/GROUP BY x - d/r: 1/1

			    Now, after figuring out the recursive deps
		*/
		if recursiveDeps.NumberOfTables() > 0 {
			directDeps = recursiveDeps
		}

		newDeps := createCertain(directDeps, recursiveDeps, qt)
		deps, err = deps.Merge(newDeps)
		if err != nil {
			return nil, err
		}
	}
	if deps.Empty() && v.hasStar() {
		return createUncertain(v.tables, v.tables), nil
	}
	return deps, nil
}

// IsInfSchema implements the TableInfo interface
func (v *vTableInfo) IsInfSchema() bool {
	return false
}

// IsActualTable implements the TableInfo interface
func (v *vTableInfo) IsActualTable() bool {
	return false
}

func (v *vTableInfo) Matches(name sqlparser.TableName) bool {
	return v.tableName == name.Name.String() && name.Qualifier.IsEmpty()
}

func (v *vTableInfo) Authoritative() bool {
	return true
}

func (v *vTableInfo) Name() (sqlparser.TableName, error) {
	return sqlparser.TableName{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "oh noes")
}

func (v *vTableInfo) GetExpr() *sqlparser.AliasedTableExpr {
	return nil
}

// GetVindexTable implements the TableInfo interface
func (v *vTableInfo) GetVindexTable() *vindexes.Table {
	return nil
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

func (v *vTableInfo) hasStar() bool {
	return v.tables > 0
}

// GetTables implements the TableInfo interface
func (v *vTableInfo) GetTables(org originable) TableSet {
	return v.tables
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

func createVTableInfoForExpressions(expressions sqlparser.SelectExprs, tables []TableInfo, org originable) *vTableInfo {
	cols, colNames, ts := selectExprsToInfos(expressions, tables, org)
	return &vTableInfo{
		columnNames: colNames,
		cols:        cols,
		tables:      ts,
	}
}

func selectExprsToInfos(
	expressions sqlparser.SelectExprs,
	tables []TableInfo,
	org originable,
) (cols []sqlparser.Expr, colNames []string, ts TableSet) {
	for _, selectExpr := range expressions {
		switch expr := selectExpr.(type) {
		case *sqlparser.AliasedExpr:
			cols = append(cols, expr.Expr)
			if expr.As.IsEmpty() {
				switch expr := expr.Expr.(type) {
				case *sqlparser.ColName:
					// for projections, we strip out the qualifier and keep only the column name
					colNames = append(colNames, expr.Name.String())
				default:
					colNames = append(colNames, sqlparser.String(expr))
				}
			} else {
				colNames = append(colNames, expr.As.String())
			}
		case *sqlparser.StarExpr:
			for _, table := range tables {
				ts |= table.GetTables(org)
			}
		}
	}
	return
}
