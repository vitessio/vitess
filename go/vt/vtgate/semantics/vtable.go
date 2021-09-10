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

// vTableInfo is used to represent projected results, not real tables. It is used for
// ORDER BY, GROUP BY and HAVING that need to access result columns, and also for derived tables.
type vTableInfo struct {
	tableName   string
	ASTNode     *sqlparser.AliasedTableExpr
	columnNames []string
	cols        []sqlparser.Expr
	tables      []TableInfo
}

var _ TableInfo = (*vTableInfo)(nil)

func (v *vTableInfo) Dependencies(colName string, org originable) (dependencies, error) {
	for i, name := range v.columnNames {
		if name != colName {
			continue
		}
		recursiveDeps, qt := org.depsForExpr(v.cols[i])

		// TODO: deal with nil ASTNodes (group/order by) better
		directDeps := recursiveDeps
		if v.ASTNode != nil {
			directDeps = org.tableSetFor(v.ASTNode)
		}
		return &certain{
			dependency: dependency{
				direct:    directDeps,
				recursive: recursiveDeps,
				typ:       qt,
			},
		}, nil
	}

	if !v.hasStar() {
		return &nothing{}, nil
	}

	var ts TableSet
	for _, table := range v.tables {
		ts |= org.tableSetFor(table.GetExpr())
	}

	d := dependency{
		direct:    ts,
		recursive: ts,
	}
	if v.ASTNode != nil {
		d.direct = org.tableSetFor(v.ASTNode)
	}
	return &uncertain{
		dependency: d,
	}, nil
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
	return v.ASTNode.TableName()
}

func (v *vTableInfo) GetExpr() *sqlparser.AliasedTableExpr {
	return v.ASTNode
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
	return len(v.tables) > 0
}

// GetTables implements the TableInfo interface
func (v *vTableInfo) GetTables() []TableInfo {
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
