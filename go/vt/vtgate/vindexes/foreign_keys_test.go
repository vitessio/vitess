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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	uks  = &Keyspace{Name: "uks"}
	uks2 = &Keyspace{Name: "uks2"}
	sks  = &Keyspace{Name: "sks", Sharded: true}
)

// TestTable_CrossShardParentFKs tests the functionality of the method CrossShardParentFKs.
func TestTable_CrossShardParentFKs(t *testing.T) {
	col1Vindex := &ColumnVindex{
		Name:    "v1",
		Vindex:  binVindex,
		Columns: sqlparser.MakeColumns("col1"),
	}
	col4DiffVindex := &ColumnVindex{
		Name:    "v2",
		Vindex:  binOnlyVindex,
		Columns: sqlparser.MakeColumns("col4"),
	}
	col123Vindex := &ColumnVindex{
		Name:    "v2",
		Vindex:  binVindex,
		Columns: sqlparser.MakeColumns("col1", "col2", "col3"),
	}
	col456Vindex := &ColumnVindex{
		Name:    "v2",
		Vindex:  binVindex,
		Columns: sqlparser.MakeColumns("col4", "col5", "col6"),
	}

	unshardedTbl := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: uks2,
	}
	shardedSingleColTblWithDiffVindex := &Table{
		Name:           sqlparser.NewIdentifierCS("t1"),
		Keyspace:       sks,
		ColumnVindexes: []*ColumnVindex{col4DiffVindex},
	}
	shardedMultiColTbl := &Table{
		Name:           sqlparser.NewIdentifierCS("t1"),
		Keyspace:       sks,
		ColumnVindexes: []*ColumnVindex{col456Vindex},
	}

	tests := []struct {
		name                   string
		table                  *Table
		wantCrossShardFKTables []string
		verifyAllFKs           bool
		fkToIgnore             string
	}{{
		name: "No Parent FKs",
		table: &Table{
			ColumnVindexes: []*ColumnVindex{col1Vindex},
			Keyspace:       sks,
		},
		wantCrossShardFKTables: []string{},
	}, {
		name: "Unsharded keyspace",
		table: &Table{
			ColumnVindexes:    []*ColumnVindex{col1Vindex},
			Keyspace:          uks2,
			ParentForeignKeys: []ParentFKInfo{pkInfo(unshardedTbl, []string{"col4"}, []string{"col1"})},
		},
		wantCrossShardFKTables: []string{},
	}, {
		name:         "Unsharded keyspace with verify all FKs",
		verifyAllFKs: true,
		table: &Table{
			ColumnVindexes:    []*ColumnVindex{col1Vindex},
			Keyspace:          uks2,
			ParentForeignKeys: []ParentFKInfo{pkInfo(unshardedTbl, []string{"col4"}, []string{"col1"})},
		},
		wantCrossShardFKTables: []string{"t1"},
	}, {
		name: "Keyspaces don't match", // parent table is on uks2
		table: &Table{
			Keyspace:          uks,
			ParentForeignKeys: []ParentFKInfo{pkInfo(unshardedTbl, []string{"col4"}, []string{"col1"})},
		},
		wantCrossShardFKTables: []string{"t1"},
	}, {
		name:       "Keyspaces don't match with ignore fk", // parent table is on uks2
		fkToIgnore: "uks.col1uks2.t1col4",
		table: &Table{
			Keyspace:          uks,
			ParentForeignKeys: []ParentFKInfo{pkInfo(unshardedTbl, []string{"col4"}, []string{"col1"})},
		},
		wantCrossShardFKTables: []string{},
	}, {
		name:         "Unsharded keyspace with verify all FKs and fk to ignore",
		verifyAllFKs: true,
		fkToIgnore:   "uks2.col1uks2.t1col4",
		table: &Table{
			ColumnVindexes:    []*ColumnVindex{col1Vindex},
			Keyspace:          uks2,
			ParentForeignKeys: []ParentFKInfo{pkInfo(unshardedTbl, []string{"col4"}, []string{"col1"})},
		},
		wantCrossShardFKTables: []string{},
	}, {
		name: "Column Vindexes don't match", // primary vindexes on different vindex type
		table: &Table{
			Keyspace:          sks,
			ColumnVindexes:    []*ColumnVindex{col1Vindex},
			ParentForeignKeys: []ParentFKInfo{pkInfo(shardedSingleColTblWithDiffVindex, []string{"col4"}, []string{"col1"})},
		},
		wantCrossShardFKTables: []string{"t1"},
	}, {
		name: "child table foreign key does not contain primary vindex columns",
		table: &Table{
			Keyspace:          sks,
			ColumnVindexes:    []*ColumnVindex{col123Vindex},
			ParentForeignKeys: []ParentFKInfo{pkInfo(shardedMultiColTbl, []string{"col4", "col5", "col6"}, []string{"col3", "col9", "col1"})},
		},
		wantCrossShardFKTables: []string{"t1"},
	}, {
		name: "Parent FK doesn't contain primary vindex",
		table: &Table{
			Keyspace:          sks,
			ColumnVindexes:    []*ColumnVindex{col123Vindex},
			ParentForeignKeys: []ParentFKInfo{pkInfo(shardedMultiColTbl, []string{"col4", "col9", "col6"}, []string{"col1", "col2", "col3"})},
		},
		wantCrossShardFKTables: []string{"t1"},
	}, {
		name: "Indexes of the two FKs with column vindexes don't line up",
		table: &Table{
			Keyspace:          sks,
			ColumnVindexes:    []*ColumnVindex{col123Vindex},
			ParentForeignKeys: []ParentFKInfo{pkInfo(shardedMultiColTbl, []string{"col4", "col9", "col5", "col6"}, []string{"col1", "col2", "col3", "col9"})},
		},
		wantCrossShardFKTables: []string{"t1"},
	}, {
		name: "Shard scoped foreign key constraint",
		table: &Table{
			Keyspace:          sks,
			ColumnVindexes:    []*ColumnVindex{col123Vindex},
			ParentForeignKeys: []ParentFKInfo{pkInfo(shardedMultiColTbl, []string{"col4", "col9", "col5", "col6", "colc"}, []string{"col1", "cola", "col2", "col3", "colb"})},
		},
		wantCrossShardFKTables: []string{},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crossShardFks := tt.table.ParentFKsNeedsHandling(tt.verifyAllFKs, tt.fkToIgnore)
			var crossShardFkTables []string
			for _, fk := range crossShardFks {
				crossShardFkTables = append(crossShardFkTables, fk.Table.Name.String())
			}
			require.ElementsMatch(t, tt.wantCrossShardFKTables, crossShardFkTables)
		})
	}
}

func pkInfo(parentTable *Table, pCols []string, cCols []string) ParentFKInfo {
	return ParentFKInfo{
		Table:         parentTable,
		ParentColumns: sqlparser.MakeColumns(pCols...),
		ChildColumns:  sqlparser.MakeColumns(cCols...),
	}
}

// TestChildFKs tests the ChildFKsNeedsHandling method is provides the child foreign key table whose
// rows needs to be managed by vitess.
func TestChildFKs(t *testing.T) {
	col1Vindex := &ColumnVindex{
		Name:    "v1",
		Vindex:  binVindex,
		Columns: sqlparser.MakeColumns("col1"),
	}
	col4DiffVindex := &ColumnVindex{
		Name:    "v2",
		Vindex:  binOnlyVindex,
		Columns: sqlparser.MakeColumns("col4"),
	}

	unshardedTbl := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: uks2,
	}
	shardedSingleColTbl := &Table{
		Name:           sqlparser.NewIdentifierCS("t1"),
		Keyspace:       sks,
		ColumnVindexes: []*ColumnVindex{col1Vindex},
	}
	shardedSingleColTblWithDiffVindex := &Table{
		Name:           sqlparser.NewIdentifierCS("t1"),
		Keyspace:       sks,
		ColumnVindexes: []*ColumnVindex{col4DiffVindex},
	}

	tests := []struct {
		verifyAllFKs bool
		name         string
		table        *Table
		expChildTbls []string
	}{{
		name: "No Parent FKs",
		table: &Table{
			ColumnVindexes: []*ColumnVindex{col1Vindex},
			Keyspace:       sks,
		},
		expChildTbls: []string{},
	}, {
		name: "restrict unsharded",
		table: &Table{
			ColumnVindexes:   []*ColumnVindex{col1Vindex},
			Keyspace:         uks2,
			ChildForeignKeys: []ChildFKInfo{ckInfo(unshardedTbl, []string{"col4"}, []string{"col1"}, sqlparser.Restrict)},
		},
		expChildTbls: []string{},
	}, {
		name:         "restrict unsharded with verify all fks",
		verifyAllFKs: true,
		table: &Table{
			ColumnVindexes:   []*ColumnVindex{col1Vindex},
			Keyspace:         uks2,
			ChildForeignKeys: []ChildFKInfo{ckInfo(unshardedTbl, []string{"col4"}, []string{"col1"}, sqlparser.Restrict)},
		},
		expChildTbls: []string{"t1"},
	}, {
		name: "restrict shard scoped",
		table: &Table{
			ColumnVindexes:   []*ColumnVindex{col1Vindex},
			Keyspace:         sks,
			ChildForeignKeys: []ChildFKInfo{ckInfo(shardedSingleColTbl, []string{"col1"}, []string{"col1"}, sqlparser.Restrict)},
		},
		expChildTbls: []string{},
	}, {
		name:         "restrict shard scoped with verify all fks",
		verifyAllFKs: true,
		table: &Table{
			ColumnVindexes:   []*ColumnVindex{col1Vindex},
			Keyspace:         sks,
			ChildForeignKeys: []ChildFKInfo{ckInfo(shardedSingleColTbl, []string{"col1"}, []string{"col1"}, sqlparser.Restrict)},
		},
		expChildTbls: []string{"t1"},
	}, {
		name: "restrict Keyspaces don't match",
		table: &Table{
			Keyspace:         uks,
			ChildForeignKeys: []ChildFKInfo{ckInfo(shardedSingleColTbl, []string{"col1"}, []string{"col1"}, sqlparser.Restrict)},
		},
		expChildTbls: []string{"t1"},
	}, {
		name: "restrict cross shard",
		table: &Table{
			Keyspace:         sks,
			ColumnVindexes:   []*ColumnVindex{col1Vindex},
			ChildForeignKeys: []ChildFKInfo{ckInfo(shardedSingleColTblWithDiffVindex, []string{"col4"}, []string{"col1"}, sqlparser.Restrict)},
		},
		expChildTbls: []string{"t1"},
	}, {
		name: "cascade unsharded",
		table: &Table{
			Keyspace:         uks2,
			ColumnVindexes:   []*ColumnVindex{col1Vindex},
			ChildForeignKeys: []ChildFKInfo{ckInfo(unshardedTbl, []string{"col4"}, []string{"col1"}, sqlparser.Cascade)},
		},
		expChildTbls: []string{"t1"},
	}, {
		name: "cascade cross shard",
		table: &Table{
			Keyspace:         sks,
			ColumnVindexes:   []*ColumnVindex{col1Vindex},
			ChildForeignKeys: []ChildFKInfo{ckInfo(shardedSingleColTblWithDiffVindex, []string{"col4"}, []string{"col1"}, sqlparser.Cascade)},
		},
		expChildTbls: []string{"t1"},
	}}
	deleteAction := func(fk ChildFKInfo) sqlparser.ReferenceAction { return fk.OnDelete }
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			childFks := tt.table.ChildFKsNeedsHandling(tt.verifyAllFKs, deleteAction)
			var actualChildTbls []string
			for _, fk := range childFks {
				actualChildTbls = append(actualChildTbls, fk.Table.Name.String())
			}
			require.ElementsMatch(t, tt.expChildTbls, actualChildTbls)
		})
	}
}

func ckInfo(cTable *Table, pCols []string, cCols []string, refAction sqlparser.ReferenceAction) ChildFKInfo {
	return ChildFKInfo{
		Table:         cTable,
		ParentColumns: sqlparser.MakeColumns(pCols...),
		ChildColumns:  sqlparser.MakeColumns(cCols...),
		OnDelete:      refAction,
	}
}
