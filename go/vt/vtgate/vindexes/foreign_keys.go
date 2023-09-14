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
	str.WriteString(childTable.String())
	for _, column := range fk.ChildColumns {
		str.WriteString(column.String())
	}
	str.WriteString(fk.Table.String())
	for _, column := range fk.ParentColumns {
		str.WriteString(column.String())
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
	str.WriteString(fk.Table.String())
	for _, column := range fk.ChildColumns {
		str.WriteString(column.String())
	}
	str.WriteString(parentTable.String())
	for _, column := range fk.ParentColumns {
		str.WriteString(column.String())
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

// ParentFKsNeedsHandling returns all the parent fk constraints on this table that are not shard scoped.
func (t *Table) ParentFKsNeedsHandling(verifyAllFKs bool, fkToIgnore string) (fks []ParentFKInfo) {
	for _, fk := range t.ParentForeignKeys {
		// Check if we need to specifically ignore this foreign key
		if fkToIgnore != "" && fk.String(t) == fkToIgnore {
			continue
		}

		// If we require all the foreign keys, add them all.
		if verifyAllFKs {
			fks = append(fks, fk)
			continue
		}

		// If the keyspaces are different, then the fk definition
		// is going to go across shards.
		if fk.Table.Keyspace.Name != t.Keyspace.Name {
			fks = append(fks, fk)
			continue
		}
		// If the keyspaces match and they are unsharded, then the fk defintion
		// is shard-scoped.
		if !t.Keyspace.Sharded {
			continue
		}

		if !isShardScoped(fk.Table, t, fk.ParentColumns, fk.ChildColumns) {
			fks = append(fks, fk)
		}
	}
	return
}

// ChildFKsNeedsHandling retuns the child foreign keys that needs to be handled by the vtgate.
// This can be either the foreign key is not shard scoped or the child tables needs cascading.
func (t *Table) ChildFKsNeedsHandling(verifyAllFKs bool, getAction func(fk ChildFKInfo) sqlparser.ReferenceAction) (fks []ChildFKInfo) {
	// If we require all the foreign keys, return the entire list.
	if verifyAllFKs {
		return t.ChildForeignKeys
	}
	for _, fk := range t.ChildForeignKeys {
		// If the keyspaces are different, then the fk definition
		// is going to go across shards.
		if fk.Table.Keyspace.Name != t.Keyspace.Name {
			fks = append(fks, fk)
			continue
		}
		// If the action is not Restrict, then it needs a cascade.
		switch getAction(fk) {
		case sqlparser.Cascade, sqlparser.SetNull, sqlparser.SetDefault:
			fks = append(fks, fk)
			continue
		}
		// sqlparser.Restrict, sqlparser.NoAction, sqlparser.DefaultAction
		// all the actions means the same thing i.e. Restrict
		// do not allow modification if there is a child row.
		// Check if the restrict is shard scoped.
		if !isShardScoped(t, fk.Table, fk.ParentColumns, fk.ChildColumns) {
			fks = append(fks, fk)
		}
	}
	return
}

func UpdateAction(fk ChildFKInfo) sqlparser.ReferenceAction { return fk.OnUpdate }
func DeleteAction(fk ChildFKInfo) sqlparser.ReferenceAction { return fk.OnDelete }

func isShardScoped(pTable *Table, cTable *Table, pCols sqlparser.Columns, cCols sqlparser.Columns) bool {
	if !pTable.Keyspace.Sharded {
		return true
	}

	pPrimaryVdx := pTable.ColumnVindexes[0]
	cPrimaryVdx := cTable.ColumnVindexes[0]

	// If the primary vindexes don't match between the parent and child table,
	// we cannot infer that the fk constraint in shard scoped.
	if cPrimaryVdx.Vindex != pPrimaryVdx.Vindex {
		return false
	}

	childFkContatined, childFkIndexes := cCols.Indexes(cPrimaryVdx.Columns)
	if !childFkContatined {
		// PrimaryVindex is not part of the foreign key constraint on the children side.
		// So it is a cross-shard foreign key.
		return false
	}

	// We need to run the same check for the parent columns.
	parentFkContatined, parentFkIndexes := pCols.Indexes(pPrimaryVdx.Columns)
	if !parentFkContatined {
		return false
	}

	// Both the child and parent table contain the foreign key and that the vindexes are the same,
	// now we need to make sure, that the indexes of both match.
	// For example, consider the following tables,
	//	t1 (primary vindex (x,y))
	//	t2 (primary vindex (a,b))
	//	If we have a foreign key constraint from t1(x,y) to t2(b,a), then they are not shard scoped.
	//	Let's say in t1, (1,3) will be in -80 and (3,1) will be in 80-, then in t2 (1,3) will end up in 80-.
	for i := range parentFkIndexes {
		if parentFkIndexes[i] != childFkIndexes[i] {
			return false
		}
	}
	return true
}

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
