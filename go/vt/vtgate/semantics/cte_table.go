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
// This is a special TableInfo that is used to represent the recursive table inside a CTE. For the query:
// WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT * FROM cte as C1) SELECT * FROM cte as C2
// The CTE table C1 is represented by a CTETable.
type CTETable struct {
	TableName string
	ASTNode   *sqlparser.AliasedTableExpr
	*CTE
}

var _ TableInfo = (*CTETable)(nil)

func newCTETable(node *sqlparser.AliasedTableExpr, t sqlparser.TableName, cteDef *CTE) *CTETable {
	var name string
	if node.As.IsEmpty() {
		name = t.Name.String()
	} else {
		name = node.As.String()
	}

	authoritative := true
	for _, expr := range cteDef.Query.GetColumns() {
		_, isStar := expr.(*sqlparser.StarExpr)
		if isStar {
			authoritative = false
			break
		}
	}
	cteDef.isAuthoritative = authoritative

	return &CTETable{
		TableName: name,
		ASTNode:   node,
		CTE:       cteDef,
	}
}

func (cte *CTETable) Name() (sqlparser.TableName, error) {
	return sqlparser.NewTableName(cte.TableName), nil
}

func (cte *CTETable) GetVindexTable() *vindexes.Table {
	return nil
}

func (cte *CTETable) IsInfSchema() bool {
	return false
}

func (cte *CTETable) matches(name sqlparser.TableName) bool {
	return cte.TableName == name.Name.String() && name.Qualifier.IsEmpty()
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
	selExprs := cte.Query.GetColumns()
	cols := make([]ColumnInfo, 0, len(selExprs))
	for i, selExpr := range selExprs {
		ae, isAe := selExpr.(*sqlparser.AliasedExpr)
		if !isAe {
			panic(vterrors.VT12001("should not be called"))
		}
		if len(cte.Columns) == 0 {
			cols = append(cols, ColumnInfo{Name: ae.ColumnName()})
			continue
		}

		// We have column aliases defined on the CTE
		cols = append(cols, ColumnInfo{Name: cte.Columns[i].String()})
	}
	return cols
}

func (cte *CTETable) dependencies(colName string, org originable) (dependencies, error) {
	directDeps := org.tableSetFor(cte.ASTNode)
	columns := cte.getColumns(false)
	for _, columnInfo := range columns {
		if strings.EqualFold(columnInfo.Name, colName) {
			return createCertain(directDeps, directDeps, evalengine.NewUnknownType()), nil
		}
	}

	if cte.authoritative() {
		return &nothing{}, nil
	}

	return createUncertain(directDeps, directDeps), nil
}

func (cte *CTETable) getExprFor(s string) (sqlparser.Expr, error) {
	for _, se := range cte.Query.GetColumns() {
		ae, ok := se.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, vterrors.VT09015()
		}
		if ae.ColumnName() == s {
			return ae.Expr, nil
		}
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Unknown column '%s' in 'field list'", s)
}

func (cte *CTETable) getTableSet(org originable) TableSet {
	return org.tableSetFor(cte.ASTNode)
}

// GetMirrorRule implements TableInfo.
func (cte *CTETable) GetMirrorRule() *vindexes.MirrorRule {
	return nil
}

type CTE struct {
	Name            string
	Query           sqlparser.SelectStatement
	isAuthoritative bool
	recursiveDeps   *TableSet
	Columns         sqlparser.Columns
	IDForRecurse    *TableSet

	// Was this CTE marked for being recursive?
	Recursive bool

	// The CTE had the seed and term parts merged
	Merged bool
}

func (cte *CTE) recursive(org originable) (id TableSet) {
	if cte.recursiveDeps != nil {
		return *cte.recursiveDeps
	}

	// We need to find the recursive dependencies of the CTE
	// We'll do this by walking the inner query and finding all the tables
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		ate, ok := node.(*sqlparser.AliasedTableExpr)
		if !ok {
			return true, nil
		}
		id = id.Merge(org.tableSetFor(ate))
		return true, nil
	}, cte.Query)
	return
}
