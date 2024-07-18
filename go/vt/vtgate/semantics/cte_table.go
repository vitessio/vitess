/*
Copyright 2024 The Vitess Authors.

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

// CTETable contains the information about the CTE table.
type CTETable struct {
	tableName string
	ASTNode   *sqlparser.AliasedTableExpr
	CTEDef
}

var _ TableInfo = (*CTETable)(nil)

func newCTETable(node *sqlparser.AliasedTableExpr, t sqlparser.TableName, cteDef CTEDef) *CTETable {
	var name string
	if node.As.IsEmpty() {
		name = t.Name.String()
	} else {
		name = node.As.String()
	}
	return &CTETable{
		tableName: name,
		ASTNode:   node,
		CTEDef:    cteDef,
	}
}

func (cte *CTETable) Name() (sqlparser.TableName, error) {
	return sqlparser.NewTableName(cte.tableName), nil
}

func (cte *CTETable) GetVindexTable() *vindexes.Table {
	return nil
}

func (cte *CTETable) IsInfSchema() bool {
	return false
}

func (cte *CTETable) matches(name sqlparser.TableName) bool {
	return cte.tableName == name.Name.String() && name.Qualifier.IsEmpty()
}

func (cte *CTETable) authoritative() bool {
	return cte.isAuthoritative
}

func (cte *CTETable) GetAliasedTableExpr() *sqlparser.AliasedTableExpr {
	return cte.ASTNode
}

func (cte *CTETable) canShortCut() shortCut {
	return canShortCut
}

func (cte *CTETable) getColumns(bool) []ColumnInfo {
	selExprs := cte.definition.GetColumns()
	cols := make([]ColumnInfo, 0, len(selExprs))
	for _, selExpr := range selExprs {
		ae, isAe := selExpr.(*sqlparser.AliasedExpr)
		if !isAe {
			panic(vterrors.VT12001("should not be called"))
		}
		cols = append(cols, ColumnInfo{
			Name: ae.ColumnName(),
		})
	}
	return cols
}

func (cte *CTETable) dependencies(colName string, org originable) (dependencies, error) {
	directDeps := org.tableSetFor(cte.ASTNode)
	for _, columnInfo := range cte.getColumns(false) {
		if strings.EqualFold(columnInfo.Name, colName) {
			return createCertain(directDeps, cte.recursive(org), evalengine.NewUnknownType()), nil
		}
	}

	if cte.authoritative() {
		return &nothing{}, nil
	}

	return createUncertain(directDeps, cte.recursive(org)), nil
}

func (cte *CTETable) getExprFor(s string) (sqlparser.Expr, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Unknown column '%s' in 'field list'", s)
}

func (cte *CTETable) getTableSet(org originable) TableSet {
	return org.tableSetFor(cte.ASTNode)
}
