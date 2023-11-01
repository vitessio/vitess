/*
Copyright 2023 The Vitess Authors.

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

package vindexes

import (
	"encoding/json"
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// ParentFKInfo contains the parent foreign key info for the table.
type ParentFKInfo struct {
	Table         *Table
	ParentColumns sqlparser.Columns
	ChildColumns  sqlparser.Columns
}

// MarshalJSON returns a JSON representation of ParentFKInfo.
func (fk *ParentFKInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name          string            `json:"parent_table"`
		ParentColumns sqlparser.Columns `json:"parent_columns"`
		ChildColumns  sqlparser.Columns `json:"child_columns"`
	}{
		Name:          fk.Table.Name.String(),
		ChildColumns:  fk.ChildColumns,
		ParentColumns: fk.ParentColumns,
	})
}

func (fk *ParentFKInfo) String(childTable *Table) string {
	var str strings.Builder
	str.WriteString(sqlparser.String(childTable.GetTableName()))
	for _, column := range fk.ChildColumns {
		str.WriteString("|" + sqlparser.String(column))
	}
	str.WriteString("||" + sqlparser.String(fk.Table.GetTableName()))
	for _, column := range fk.ParentColumns {
		str.WriteString("|" + sqlparser.String(column))
	}
	return str.String()
}

// NewParentFkInfo creates a new ParentFKInfo.
func NewParentFkInfo(parentTbl *Table, fkDef *sqlparser.ForeignKeyDefinition) ParentFKInfo {
	return ParentFKInfo{
		Table:         parentTbl,
		ChildColumns:  fkDef.Source,
		ParentColumns: fkDef.ReferenceDefinition.ReferencedColumns,
	}
}

// ChildFKInfo contains the child foreign key info for the table.
type ChildFKInfo struct {
	Table         *Table
	ChildColumns  sqlparser.Columns
	ParentColumns sqlparser.Columns
	Match         sqlparser.MatchAction
	OnDelete      sqlparser.ReferenceAction
	OnUpdate      sqlparser.ReferenceAction
}

// MarshalJSON returns a JSON representation of ChildFKInfo.
func (fk *ChildFKInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name          string            `json:"child_table"`
		ChildColumns  sqlparser.Columns `json:"child_columns"`
		ParentColumns sqlparser.Columns `json:"parent_columns"`
	}{
		Name:          fk.Table.Name.String(),
		ChildColumns:  fk.ChildColumns,
		ParentColumns: fk.ParentColumns,
	})
}

func (fk *ChildFKInfo) String(parentTable *Table) string {
	var str strings.Builder
	str.WriteString(sqlparser.String(fk.Table.GetTableName()))
	for _, column := range fk.ChildColumns {
		str.WriteString("|" + sqlparser.String(column))
	}
	str.WriteString("||" + sqlparser.String(parentTable.GetTableName()))
	for _, column := range fk.ParentColumns {
		str.WriteString("|" + sqlparser.String(column))
	}
	return str.String()
}

// NewChildFkInfo creates a new ChildFKInfo.
func NewChildFkInfo(childTbl *Table, fkDef *sqlparser.ForeignKeyDefinition) ChildFKInfo {
	return ChildFKInfo{
		Table:         childTbl,
		ChildColumns:  fkDef.Source,
		ParentColumns: fkDef.ReferenceDefinition.ReferencedColumns,
		Match:         fkDef.ReferenceDefinition.Match,
		OnDelete:      fkDef.ReferenceDefinition.OnDelete,
		OnUpdate:      fkDef.ReferenceDefinition.OnUpdate,
	}
}

func UpdateAction(fk ChildFKInfo) sqlparser.ReferenceAction { return fk.OnUpdate }
func DeleteAction(fk ChildFKInfo) sqlparser.ReferenceAction { return fk.OnDelete }

// AddForeignKey is for testing only.
func (vschema *VSchema) AddForeignKey(ksname, childTableName string, fkConstraint *sqlparser.ForeignKeyDefinition) error {
	ks, ok := vschema.Keyspaces[ksname]
	if !ok {
		return fmt.Errorf("keyspace %s not found in vschema", ksname)
	}
	cTbl, ok := ks.Tables[childTableName]
	if !ok {
		return fmt.Errorf("child table %s not found in keyspace %s", childTableName, ksname)
	}
	pKsName := fkConstraint.ReferenceDefinition.ReferencedTable.Qualifier.String()
	if pKsName != "" {
		ks, ok = vschema.Keyspaces[pKsName]
		if !ok {
			return fmt.Errorf("keyspace %s not found in vschema", pKsName)
		}
		ksname = pKsName
	}
	parentTableName := fkConstraint.ReferenceDefinition.ReferencedTable.Name.String()
	pTbl, ok := ks.Tables[parentTableName]
	if !ok {
		return fmt.Errorf("parent table %s not found in keyspace %s", parentTableName, ksname)
	}
	pTbl.ChildForeignKeys = append(pTbl.ChildForeignKeys, NewChildFkInfo(cTbl, fkConstraint))
	cTbl.ParentForeignKeys = append(cTbl.ParentForeignKeys, NewParentFkInfo(pTbl, fkConstraint))
	return nil
}

// AddPrimaryKey is for testing only.
func (vschema *VSchema) AddPrimaryKey(ksname, tblName string, cols []string) error {
	ks, ok := vschema.Keyspaces[ksname]
	if !ok {
		return fmt.Errorf("keyspace %s not found in vschema", ksname)
	}
	tbl, ok := ks.Tables[tblName]
	if !ok {
		return fmt.Errorf("table %s not found in keyspace %s", tblName, ksname)
	}
	for _, col := range cols {
		tbl.PrimaryKey = append(tbl.PrimaryKey, sqlparser.NewIdentifierCI(col))
	}
	return nil
}

// AddUniqueKey is for testing only.
func (vschema *VSchema) AddUniqueKey(ksname, tblName string, exprs sqlparser.Exprs) error {
	ks, ok := vschema.Keyspaces[ksname]
	if !ok {
		return fmt.Errorf("keyspace %s not found in vschema", ksname)
	}
	tbl, ok := ks.Tables[tblName]
	if !ok {
		return fmt.Errorf("table %s not found in keyspace %s", tblName, ksname)
	}
	tbl.UniqueKeys = append(tbl.UniqueKeys, exprs)
	return nil
}
