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

package semantics

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var parentTbl = &vindexes.Table{
	Name: sqlparser.NewIdentifierCS("parentt"),
	Keyspace: &vindexes.Keyspace{
		Name: "ks",
	},
}

var tbl = map[string]TableInfo{
	"t0": &RealTable{
		Table: &vindexes.Table{
			Name:     sqlparser.NewIdentifierCS("t0"),
			Keyspace: &vindexes.Keyspace{Name: "ks"},
			ChildForeignKeys: []vindexes.ChildFKInfo{
				ckInfo(parentTbl, []string{"col"}, []string{"col"}, sqlparser.Restrict),
				ckInfo(parentTbl, []string{"col1", "col2"}, []string{"ccol1", "ccol2"}, sqlparser.SetNull),
			},
			ParentForeignKeys: []vindexes.ParentFKInfo{
				pkInfo(parentTbl, []string{"colb"}, []string{"colb"}),
				pkInfo(parentTbl, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
			},
		},
	},
	"t1": &RealTable{
		Table: &vindexes.Table{
			Name:     sqlparser.NewIdentifierCS("t1"),
			Keyspace: &vindexes.Keyspace{Name: "ks_unmanaged", Sharded: true},
			ChildForeignKeys: []vindexes.ChildFKInfo{
				ckInfo(parentTbl, []string{"cola"}, []string{"cola"}, sqlparser.Restrict),
				ckInfo(parentTbl, []string{"cola1", "cola2"}, []string{"ccola1", "ccola2"}, sqlparser.SetNull),
			},
		},
	},
	"t2": &RealTable{
		Table: &vindexes.Table{
			Name:     sqlparser.NewIdentifierCS("t2"),
			Keyspace: &vindexes.Keyspace{Name: "ks"},
		},
	},
	"t3": &RealTable{
		Table: &vindexes.Table{
			Name:     sqlparser.NewIdentifierCS("t3"),
			Keyspace: &vindexes.Keyspace{Name: "undefined_ks", Sharded: true},
		},
	},
	"t4": &RealTable{
		Table: &vindexes.Table{
			Name:     sqlparser.NewIdentifierCS("t4"),
			Keyspace: &vindexes.Keyspace{Name: "ks"},
			ChildForeignKeys: []vindexes.ChildFKInfo{
				ckInfo(parentTbl, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
				ckInfo(parentTbl, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
				ckInfo(parentTbl, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
				ckInfo(parentTbl, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
			},
			ParentForeignKeys: []vindexes.ParentFKInfo{
				pkInfo(parentTbl, []string{"pcola", "pcolx"}, []string{"cola", "colx"}),
				pkInfo(parentTbl, []string{"pcolc"}, []string{"colc"}),
				pkInfo(parentTbl, []string{"pcolb", "pcola"}, []string{"colb", "cola"}),
				pkInfo(parentTbl, []string{"pcolb"}, []string{"colb"}),
				pkInfo(parentTbl, []string{"pcola"}, []string{"cola"}),
				pkInfo(parentTbl, []string{"pcolb", "pcolx"}, []string{"colb", "colx"}),
			},
		},
	},
	"t5": &RealTable{
		Table: &vindexes.Table{
			Name:     sqlparser.NewIdentifierCS("t5"),
			Keyspace: &vindexes.Keyspace{Name: "ks"},
			ChildForeignKeys: []vindexes.ChildFKInfo{
				ckInfo(parentTbl, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
				ckInfo(parentTbl, []string{"colc", "colx"}, []string{"child_colc", "child_colx"}, sqlparser.SetNull),
				ckInfo(parentTbl, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
			},
			ParentForeignKeys: []vindexes.ParentFKInfo{
				pkInfo(parentTbl, []string{"pcolc", "pcolx"}, []string{"colc", "colx"}),
				pkInfo(parentTbl, []string{"pcola"}, []string{"cola"}),
				pkInfo(parentTbl, []string{"pcold", "pcolc"}, []string{"cold", "colc"}),
				pkInfo(parentTbl, []string{"pcold"}, []string{"cold"}),
				pkInfo(parentTbl, []string{"pcold", "pcolx"}, []string{"cold", "colx"}),
			},
		},
	},
	"t6": &RealTable{
		Table: &vindexes.Table{
			Name:     sqlparser.NewIdentifierCS("t6"),
			Keyspace: &vindexes.Keyspace{Name: "ks"},
			ChildForeignKeys: []vindexes.ChildFKInfo{
				ckInfo(parentTbl, []string{"col"}, []string{"col"}, sqlparser.Restrict),
				ckInfo(parentTbl, []string{"col1", "col2"}, []string{"ccol1", "ccol2"}, sqlparser.SetNull),
				ckInfo(parentTbl, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
				ckInfo(parentTbl, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
				ckInfo(parentTbl, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
				ckInfo(parentTbl, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
			},
			ParentForeignKeys: []vindexes.ParentFKInfo{
				pkInfo(parentTbl, []string{"colb"}, []string{"colb"}),
				pkInfo(parentTbl, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
			},
		},
	},
}

// TestGetAllManagedForeignKeys tests the functionality of getAllManagedForeignKeys.
func TestGetAllManagedForeignKeys(t *testing.T) {
	tests := []struct {
		name           string
		fkManager      *fkManager
		childFkWanted  map[TableSet][]vindexes.ChildFKInfo
		parentFkWanted map[TableSet][]vindexes.ParentFKInfo
		expectedErr    string
	}{
		{
			name: "Collect all foreign key constraints",
			fkManager: &fkManager{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t0"],
						tbl["t1"],
						&DerivedTable{},
					},
				},
				si: &FakeSI{
					KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
						"ks":           vschemapb.Keyspace_managed,
						"ks_unmanaged": vschemapb.Keyspace_unmanaged,
					},
				},
			},
			childFkWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(parentTbl, []string{"col"}, []string{"col"}, sqlparser.Restrict),
					ckInfo(parentTbl, []string{"col1", "col2"}, []string{"ccol1", "ccol2"}, sqlparser.SetNull),
				},
			},
			parentFkWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(parentTbl, []string{"colb"}, []string{"colb"}),
					pkInfo(parentTbl, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
				},
			},
		},
		{
			name: "keyspace not found in schema information",
			fkManager: &fkManager{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t2"],
						tbl["t3"],
					},
				},
				si: &FakeSI{
					KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
						"ks": vschemapb.Keyspace_managed,
					},
				},
			},
			expectedErr: "undefined_ks keyspace not found",
		},
		{
			name: "Cyclic fk constraints error",
			fkManager: &fkManager{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t0"], tbl["t1"],
						&DerivedTable{},
					},
				},
				si: &FakeSI{
					KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
						"ks":           vschemapb.Keyspace_managed,
						"ks_unmanaged": vschemapb.Keyspace_unmanaged,
					},
					KsError: map[string]error{
						"ks": fmt.Errorf("VT09019: keyspace 'ks' has cyclic foreign keys"),
					},
				},
			},
			expectedErr: "VT09019: keyspace 'ks' has cyclic foreign keys",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			childFk, parentFk, err := tt.fkManager.getAllManagedForeignKeys()
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.EqualValues(t, tt.childFkWanted, childFk)
			require.EqualValues(t, tt.parentFkWanted, parentFk)
		})
	}
}

// TestFilterForeignKeysUsingUpdateExpressions tests the functionality of filterForeignKeysUsingUpdateExpressions.
func TestFilterForeignKeysUsingUpdateExpressions(t *testing.T) {
	cola := sqlparser.NewColName("cola")
	colb := sqlparser.NewColName("colb")
	colc := sqlparser.NewColName("colc")
	cold := sqlparser.NewColName("cold")
	a := &fkManager{
		binder: &binder{
			direct: map[sqlparser.Expr]TableSet{
				cola: SingleTableSet(0),
				colb: SingleTableSet(0),
				colc: SingleTableSet(1),
				cold: SingleTableSet(1),
			},
		},
		getError: func() error { return fmt.Errorf("ambiguous test error") },
		tables: &tableCollector{
			Tables: []TableInfo{
				tbl["t4"],
				tbl["t5"],
			},
			si: &FakeSI{
				KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
					"ks": vschemapb.Keyspace_managed,
				},
			},
		},
	}
	updateExprs := sqlparser.UpdateExprs{
		&sqlparser.UpdateExpr{Name: cola, Expr: sqlparser.NewIntLiteral("1")},
		&sqlparser.UpdateExpr{Name: colb, Expr: &sqlparser.NullVal{}},
		&sqlparser.UpdateExpr{Name: colc, Expr: sqlparser.NewIntLiteral("1")},
		&sqlparser.UpdateExpr{Name: cold, Expr: &sqlparser.NullVal{}},
	}
	tests := []struct {
		name            string
		fkManager       *fkManager
		allChildFks     map[TableSet][]vindexes.ChildFKInfo
		allParentFks    map[TableSet][]vindexes.ParentFKInfo
		updExprs        sqlparser.UpdateExprs
		childFksWanted  map[TableSet][]vindexes.ChildFKInfo
		parentFksWanted map[TableSet][]vindexes.ParentFKInfo
		errWanted       string
	}{
		{
			name:         "Child Foreign Keys Filtering",
			fkManager:    a,
			allParentFks: nil,
			allChildFks: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): tbl["t4"].(*RealTable).Table.ChildForeignKeys,
				SingleTableSet(1): tbl["t5"].(*RealTable).Table.ChildForeignKeys,
			},
			updExprs: updateExprs,
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(parentTbl, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
					ckInfo(parentTbl, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
				},
				SingleTableSet(1): {
					ckInfo(parentTbl, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
					ckInfo(parentTbl, []string{"colc", "colx"}, []string{"child_colc", "child_colx"}, sqlparser.SetNull),
				},
			},
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{},
		}, {
			name:      "Parent Foreign Keys Filtering",
			fkManager: a,
			allParentFks: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): tbl["t4"].(*RealTable).Table.ParentForeignKeys,
				SingleTableSet(1): tbl["t5"].(*RealTable).Table.ParentForeignKeys,
			},
			allChildFks:    nil,
			updExprs:       updateExprs,
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{},
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(parentTbl, []string{"pcola", "pcolx"}, []string{"cola", "colx"}),
					pkInfo(parentTbl, []string{"pcola"}, []string{"cola"}),
				},
				SingleTableSet(1): {
					pkInfo(parentTbl, []string{"pcolc", "pcolx"}, []string{"colc", "colx"}),
				},
			},
		}, {
			name:      "Unknown column",
			fkManager: a,
			allParentFks: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): tbl["t4"].(*RealTable).Table.ParentForeignKeys,
				SingleTableSet(1): tbl["t5"].(*RealTable).Table.ParentForeignKeys,
			},
			allChildFks: nil,
			updExprs: sqlparser.UpdateExprs{
				&sqlparser.UpdateExpr{Name: sqlparser.NewColName("unknownCol"), Expr: sqlparser.NewIntLiteral("1")},
			},
			errWanted: "ambiguous test error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			childFks, parentFks, _, err := tt.fkManager.filterForeignKeysUsingUpdateExpressions(tt.allChildFks, tt.allParentFks, tt.updExprs)
			require.EqualValues(t, tt.childFksWanted, childFks)
			require.EqualValues(t, tt.parentFksWanted, parentFks)
			if tt.errWanted != "" {
				require.EqualError(t, err, tt.errWanted)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestGetInvolvedForeignKeys tests the functionality of getInvolvedForeignKeys.
func TestGetInvolvedForeignKeys(t *testing.T) {
	cola := sqlparser.NewColName("cola")
	colb := sqlparser.NewColName("colb")
	colc := sqlparser.NewColName("colc")
	cold := sqlparser.NewColName("cold")
	tests := []struct {
		name                     string
		stmt                     sqlparser.Statement
		fkManager                *fkManager
		childFksWanted           map[TableSet][]vindexes.ChildFKInfo
		parentFksWanted          map[TableSet][]vindexes.ParentFKInfo
		childFkUpdateExprsWanted map[string]sqlparser.UpdateExprs
		expectedErr              string
	}{
		{
			name: "Delete Query",
			stmt: &sqlparser.Delete{},
			fkManager: &fkManager{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t0"],
						tbl["t1"],
					},
				},
				si: &FakeSI{
					KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
						"ks":           vschemapb.Keyspace_managed,
						"ks_unmanaged": vschemapb.Keyspace_unmanaged,
					},
				},
			},
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(parentTbl, []string{"col"}, []string{"col"}, sqlparser.Restrict),
					ckInfo(parentTbl, []string{"col1", "col2"}, []string{"ccol1", "ccol2"}, sqlparser.SetNull),
				},
			},
		},
		{
			name: "Update statement",
			stmt: &sqlparser.Update{
				Exprs: sqlparser.UpdateExprs{
					&sqlparser.UpdateExpr{Name: cola, Expr: sqlparser.NewIntLiteral("1")},
					&sqlparser.UpdateExpr{Name: colb, Expr: &sqlparser.NullVal{}},
					&sqlparser.UpdateExpr{Name: colc, Expr: sqlparser.NewIntLiteral("1")},
					&sqlparser.UpdateExpr{Name: cold, Expr: &sqlparser.NullVal{}},
				},
			},
			fkManager: &fkManager{
				binder: &binder{
					direct: map[sqlparser.Expr]TableSet{
						cola: SingleTableSet(0),
						colb: SingleTableSet(0),
						colc: SingleTableSet(1),
						cold: SingleTableSet(1),
					},
				},
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t4"],
						tbl["t5"],
					},
				},
				si: &FakeSI{
					KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
						"ks": vschemapb.Keyspace_managed,
					},
				},
			},
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(parentTbl, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
					ckInfo(parentTbl, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
				},
				SingleTableSet(1): {
					ckInfo(parentTbl, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
					ckInfo(parentTbl, []string{"colc", "colx"}, []string{"child_colc", "child_colx"}, sqlparser.SetNull),
				},
			},
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(parentTbl, []string{"pcola", "pcolx"}, []string{"cola", "colx"}),
					pkInfo(parentTbl, []string{"pcola"}, []string{"cola"}),
				},
				SingleTableSet(1): {
					pkInfo(parentTbl, []string{"pcolc", "pcolx"}, []string{"colc", "colx"}),
				},
			},
			childFkUpdateExprsWanted: map[string]sqlparser.UpdateExprs{
				"ks.parentt|child_cola|child_colx||ks.t4|cola|colx": {&sqlparser.UpdateExpr{Name: cola, Expr: sqlparser.NewIntLiteral("1")}},
				"ks.parentt|child_colb||ks.t4|colb":                 {&sqlparser.UpdateExpr{Name: colb, Expr: &sqlparser.NullVal{}}},
				"ks.parentt|child_colc|child_colx||ks.t5|colc|colx": {&sqlparser.UpdateExpr{Name: colc, Expr: sqlparser.NewIntLiteral("1")}},
				"ks.parentt|child_cold||ks.t5|cold":                 {&sqlparser.UpdateExpr{Name: cold, Expr: &sqlparser.NullVal{}}},
			},
		},
		{
			name: "Replace Query",
			stmt: &sqlparser.Insert{
				Action: sqlparser.ReplaceAct,
			},
			fkManager: &fkManager{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t0"],
						tbl["t1"],
					},
				},
				si: &FakeSI{
					KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
						"ks":           vschemapb.Keyspace_managed,
						"ks_unmanaged": vschemapb.Keyspace_unmanaged,
					},
				},
			},
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(parentTbl, []string{"col"}, []string{"col"}, sqlparser.Restrict),
					ckInfo(parentTbl, []string{"col1", "col2"}, []string{"ccol1", "ccol2"}, sqlparser.SetNull),
				},
			},
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(parentTbl, []string{"colb"}, []string{"colb"}),
					pkInfo(parentTbl, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
				},
			},
		},
		{
			name: "Insert Query",
			stmt: &sqlparser.Insert{
				Action: sqlparser.InsertAct,
			},
			fkManager: &fkManager{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t0"],
						tbl["t1"],
					},
				},
				si: &FakeSI{
					KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
						"ks":           vschemapb.Keyspace_managed,
						"ks_unmanaged": vschemapb.Keyspace_unmanaged,
					},
				},
			},
			childFksWanted: nil,
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(parentTbl, []string{"colb"}, []string{"colb"}),
					pkInfo(parentTbl, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
				},
			},
		},
		{
			name: "Insert Query with On Duplicate",
			stmt: &sqlparser.Insert{
				Action: sqlparser.InsertAct,
				OnDup: sqlparser.OnDup{
					&sqlparser.UpdateExpr{Name: cola, Expr: sqlparser.NewIntLiteral("1")},
					&sqlparser.UpdateExpr{Name: colb, Expr: &sqlparser.NullVal{}},
				},
			},
			fkManager: &fkManager{
				binder: &binder{
					direct: map[sqlparser.Expr]TableSet{
						cola: SingleTableSet(0),
						colb: SingleTableSet(0),
					},
				},
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t6"],
						tbl["t1"],
					},
				},
				si: &FakeSI{
					KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
						"ks":           vschemapb.Keyspace_managed,
						"ks_unmanaged": vschemapb.Keyspace_unmanaged,
					},
				},
			},
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(parentTbl, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
					ckInfo(parentTbl, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
				},
			},
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(parentTbl, []string{"colb"}, []string{"colb"}),
					pkInfo(parentTbl, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
				},
			},
			childFkUpdateExprsWanted: map[string]sqlparser.UpdateExprs{
				"ks.parentt|child_cola|child_colx||ks.t6|cola|colx": {&sqlparser.UpdateExpr{Name: cola, Expr: sqlparser.NewIntLiteral("1")}},
				"ks.parentt|child_colb||ks.t6|colb":                 {&sqlparser.UpdateExpr{Name: colb, Expr: &sqlparser.NullVal{}}},
			},
		},
		{
			name: "Insert error",
			stmt: &sqlparser.Insert{},
			fkManager: &fkManager{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t2"],
						tbl["t3"],
					},
				},
				si: &FakeSI{
					KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
						"ks": vschemapb.Keyspace_managed,
					},
				},
			},
			expectedErr: "undefined_ks keyspace not found",
		},
		{
			name: "Update error",
			stmt: &sqlparser.Update{},
			fkManager: &fkManager{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t2"],
						tbl["t3"],
					},
				},
				si: &FakeSI{
					KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
						"ks": vschemapb.Keyspace_managed,
					},
				},
			},
			expectedErr: "undefined_ks keyspace not found",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fkState := true
			childFks, parentFks, childFkUpdateExprs, err := tt.fkManager.getInvolvedForeignKeys(tt.stmt, &fkState)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.EqualValues(t, tt.childFksWanted, childFks)
			require.EqualValues(t, tt.childFkUpdateExprsWanted, childFkUpdateExprs)
			require.EqualValues(t, tt.parentFksWanted, parentFks)
		})
	}
}

func ckInfo(cTable *vindexes.Table, pCols []string, cCols []string, refAction sqlparser.ReferenceAction) vindexes.ChildFKInfo {
	return vindexes.ChildFKInfo{
		Table:         cTable,
		ParentColumns: sqlparser.MakeColumns(pCols...),
		ChildColumns:  sqlparser.MakeColumns(cCols...),
		OnDelete:      refAction,
	}
}

func pkInfo(parentTable *vindexes.Table, pCols []string, cCols []string) vindexes.ParentFKInfo {
	return vindexes.ParentFKInfo{
		Table:         parentTable,
		ParentColumns: sqlparser.MakeColumns(pCols...),
		ChildColumns:  sqlparser.MakeColumns(cCols...),
	}
}
