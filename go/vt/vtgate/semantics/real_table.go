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
	"vitess.io/vitess/go/slice"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// RealTable contains the alias table expr and vindex table
type RealTable struct {
	dbName, tableName string
	ASTNode           *sqlparser.AliasedTableExpr
	Table             *vindexes.Table
	CTE               *CTE
	VindexHint        *sqlparser.IndexHint
	MirrorRule        *vindexes.MirrorRule
	isInfSchema       bool
	collationEnv      *collations.Environment
	cache             map[string]dependencies
}

var _ TableInfo = (*RealTable)(nil)

// dependencies implements the TableInfo interface
func (r *RealTable) dependencies(colName string, org originable) (deps dependencies, err error) {
	var myID *TableSet
	if r.cache == nil {
		r.cache = make(map[string]dependencies)
		ts := org.tableSetFor(r.ASTNode)
		myID = &ts
		for _, info := range r.getColumns(false /* ignoreInvisbleCol */) {
			r.cache[strings.ToLower(info.Name)] = createCertain(ts, ts, info.Type)
		}
	}

	if deps, ok := r.cache[strings.ToLower(colName)]; ok {
		return deps, nil
	}

	if r.authoritative() {
		return &nothing{}, nil
	}

	if myID == nil {
		ts := org.tableSetFor(r.ASTNode)
		myID = &ts
	}
	return createUncertain(*myID, *myID), nil
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
func (r *RealTable) getColumns(ignoreInvisbleCol bool) []ColumnInfo {
	switch {
	case r.CTE != nil:
		return r.getCTEColumns()
	case r.Table == nil:
		return nil
	default:
		return r.getVindexTableColumns(ignoreInvisbleCol)
	}
}

func (r *RealTable) getVindexTableColumns(ignoreInvisbleCol bool) []ColumnInfo {
	nameMap := map[string]any{}
	cols := make([]ColumnInfo, 0, len(r.Table.Columns))
	for _, col := range r.Table.Columns {
		if col.Invisible && ignoreInvisbleCol {
			continue
		}
		cols = append(cols, ColumnInfo{
			Name:      col.Name.String(),
			Type:      col.ToEvalengineType(r.collationEnv),
			Invisible: col.Invisible,
		})
		nameMap[col.Name.String()] = nil
	}
	// If table is authoritative, we do not need ColumnVindexes to help in resolving the unqualified columns.
	if r.Table.ColumnListAuthoritative {
		return cols
	}
	for _, vindex := range r.Table.ColumnVindexes {
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

func (r *RealTable) getCTEColumns() []ColumnInfo {
	selectExprs := r.CTE.Query.GetColumns()
	ci := extractColumnsFromCTE(r.CTE.Columns, selectExprs)
	if ci != nil {
		return ci
	}
	return extractSelectExprsFromCTE(selectExprs)
}

// Authoritative implements the TableInfo interface
func (r *RealTable) authoritative() bool {
	switch {
	case r.Table != nil:
		return r.Table.ColumnListAuthoritative
	case r.CTE != nil:
		return r.CTE.isAuthoritative
	default:
		return false
	}
}

func extractSelectExprsFromCTE(selectExprs sqlparser.SelectExprs) []ColumnInfo {
	var ci []ColumnInfo
	for _, expr := range selectExprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil
		}
		ci = append(ci, ColumnInfo{
			Name: ae.ColumnName(),
			Type: evalengine.NewUnknownType(), // TODO: set the proper type
		})
	}
	return ci
}

func extractColumnsFromCTE(columns sqlparser.Columns, selectExprs sqlparser.SelectExprs) []ColumnInfo {
	if len(columns) == 0 {
		return nil
	}
	if len(selectExprs) != len(columns) {
		panic("mismatch of columns")
	}
	return slice.Map(columns, func(from sqlparser.IdentifierCI) ColumnInfo {
		return ColumnInfo{
			Name: from.String(),
			Type: evalengine.NewUnknownType(),
		}
	})
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

// Matches implements the TableInfo interface
func (r *RealTable) matches(name sqlparser.TableName) bool {
	return (name.Qualifier.IsEmpty() || name.Qualifier.String() == r.dbName) && r.tableName == name.Name.String()
}

// GetMirrorRule implements TableInfo.
func (r *RealTable) GetMirrorRule() *vindexes.MirrorRule {
	return r.MirrorRule
}
