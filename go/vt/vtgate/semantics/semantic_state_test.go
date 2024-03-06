/*
Copyright 2022 The Vitess Authors.

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestBindingAndExprEquality(t *testing.T) {
	tests := []struct {
		expressions string
		equal       bool
	}{{
		expressions: "t1_id+1, t1.t1_id+1",
		equal:       true,
	}, {
		expressions: "t2_id+1, t1_id+1",
		equal:       false,
	}, {
		expressions: "(t1_id+1)+1, t1.t1_id+1+1",
		equal:       true,
	}}

	for _, test := range tests {
		t.Run(test.expressions, func(t *testing.T) {
			parse, err := sqlparser.NewTestParser().Parse(fmt.Sprintf("select %s from t1, t2", test.expressions))
			require.NoError(t, err)
			st, err := Analyze(parse, "db", fakeSchemaInfoTest())
			require.NoError(t, err)
			exprs := parse.(*sqlparser.Select).SelectExprs
			a := exprs[0].(*sqlparser.AliasedExpr).Expr
			b := exprs[1].(*sqlparser.AliasedExpr).Expr
			assert.Equal(t, st.EqualsExpr(a, b), test.equal)
		})
	}
}

func fakeSchemaInfoTest() *FakeSI {
	cols1 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("t1_id"),
		Type: querypb.Type_INT64,
	}}
	cols2 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("t2_id"),
		Type: querypb.Type_INT64,
	}}

	si := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t1": {Name: sqlparser.NewIdentifierCS("t1"), Columns: cols1, ColumnListAuthoritative: true, Keyspace: ks2},
			"t2": {Name: sqlparser.NewIdentifierCS("t2"), Columns: cols2, ColumnListAuthoritative: true, Keyspace: ks3},
		},
	}
	return si
}

// TestForeignKeysPresent tests the functionality of ForeignKeysPresent.
func TestForeignKeysPresent(t *testing.T) {
	tests := []struct {
		name string
		st   *SemTable
		want bool
	}{
		{
			name: "Nil maps",
			st:   &SemTable{},
			want: false,
		}, {
			name: "Empty lists in the maps",
			st: &SemTable{
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{
					SingleTableSet(1): {},
				},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(1): {},
				},
			},
			want: false,
		}, {
			name: "Parent foriegn key exists",
			st: &SemTable{
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{
					SingleTableSet(1): {},
				},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(1): {
						vindexes.ParentFKInfo{},
					},
				},
			},
			want: true,
		}, {
			name: "Child foriegn key exists",
			st: &SemTable{
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{
					SingleTableSet(1): {
						vindexes.ChildFKInfo{},
					},
				},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(1): {},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.st.ForeignKeysPresent())
		})
	}
}

// TestIsShardScoped tests the functionality of isShardScoped.
func TestIsShardScoped(t *testing.T) {
	hashVindex := &vindexes.Hash{}
	xxhashVindex := &vindexes.XXHash{}

	tests := []struct {
		name              string
		pTable            *vindexes.Table
		cTable            *vindexes.Table
		pCols             sqlparser.Columns
		cCols             sqlparser.Columns
		wantedShardScoped bool
	}{
		{
			name: "unsharded keyspace",
			pTable: &vindexes.Table{
				Keyspace: &vindexes.Keyspace{Name: "uks", Sharded: false},
			},
			wantedShardScoped: true,
		},
		{
			name: "Primary vindexes don't match",
			pTable: &vindexes.Table{
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				ColumnVindexes: []*vindexes.ColumnVindex{
					{
						Vindex: hashVindex,
					},
				},
			},
			cTable: &vindexes.Table{
				ColumnVindexes: []*vindexes.ColumnVindex{
					{
						Vindex: xxhashVindex,
					},
				},
			},
			wantedShardScoped: false,
		},
		{
			name: "Child primary vindex not part of the foreign key",
			pTable: &vindexes.Table{
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				ColumnVindexes: []*vindexes.ColumnVindex{
					{
						Vindex: hashVindex,
					},
				},
			},
			cTable: &vindexes.Table{
				ColumnVindexes: []*vindexes.ColumnVindex{
					{
						Vindex:  hashVindex,
						Columns: sqlparser.MakeColumns("cola", "colb", "colc"),
					},
				},
			},
			cCols:             sqlparser.MakeColumns("colc", "colx", "cola"),
			wantedShardScoped: false,
		},
		{
			name: "Parent primary vindex not part of the foreign key",
			pTable: &vindexes.Table{
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				ColumnVindexes: []*vindexes.ColumnVindex{
					{
						Vindex:  hashVindex,
						Columns: sqlparser.MakeColumns("pcola", "pcolb", "pcolc"),
					},
				},
			},
			cTable: &vindexes.Table{
				ColumnVindexes: []*vindexes.ColumnVindex{
					{
						Vindex:  hashVindex,
						Columns: sqlparser.MakeColumns("cola", "colb", "colc"),
					},
				},
			},
			cCols:             sqlparser.MakeColumns("colc", "colb", "cola"),
			pCols:             sqlparser.MakeColumns("pcolc", "pcolx", "pcola"),
			wantedShardScoped: false,
		},
		{
			name: "Indexes order doesn't match",
			pTable: &vindexes.Table{
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				ColumnVindexes: []*vindexes.ColumnVindex{
					{
						Vindex:  hashVindex,
						Columns: sqlparser.MakeColumns("pcola", "pcolb", "pcolc"),
					},
				},
			},
			cTable: &vindexes.Table{
				ColumnVindexes: []*vindexes.ColumnVindex{
					{
						Vindex:  hashVindex,
						Columns: sqlparser.MakeColumns("cola", "colb", "colc"),
					},
				},
			},
			cCols:             sqlparser.MakeColumns("colc", "colb", "cola"),
			pCols:             sqlparser.MakeColumns("pcolc", "pcola", "pcolb"),
			wantedShardScoped: false,
		},
		{
			name: "Is shard scoped",
			pTable: &vindexes.Table{
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				ColumnVindexes: []*vindexes.ColumnVindex{
					{
						Vindex:  hashVindex,
						Columns: sqlparser.MakeColumns("pcola", "pcolb", "pcolc"),
					},
				},
			},
			cTable: &vindexes.Table{
				ColumnVindexes: []*vindexes.ColumnVindex{
					{
						Vindex:  hashVindex,
						Columns: sqlparser.MakeColumns("cola", "colb", "colc"),
					},
				},
			},
			cCols:             sqlparser.MakeColumns("colc", "colb", "cola"),
			pCols:             sqlparser.MakeColumns("pcolc", "pcolb", "pcola"),
			wantedShardScoped: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantedShardScoped, isShardScoped(tt.pTable, tt.cTable, tt.pCols, tt.cCols))
		})
	}
}

// TestGetChildForeignKeysList tests the function GetChildForeignKeysList
func TestGetChildForeignKeysList(t *testing.T) {
	tests := []struct {
		name           string
		semTable       *SemTable
		childFksWanted []vindexes.ChildFKInfo
	}{
		{
			name: "Collect all FKs",
			semTable: &SemTable{
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{
					SingleTableSet(0): {
						ckInfo(nil, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
						ckInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
					},
					SingleTableSet(1): {
						ckInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
						ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
					},
				},
			},
			childFksWanted: []vindexes.ChildFKInfo{
				ckInfo(nil, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
				ckInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
				ckInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
				ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
			},
		},
		{
			name: "Nil Map",
			semTable: &SemTable{
				childForeignKeysInvolved: nil,
			},
			childFksWanted: nil,
		},
		{
			name: "Empty Map",
			semTable: &SemTable{
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{
					SingleTableSet(0): {},
					SingleTableSet(1): nil,
				},
			},
			childFksWanted: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ElementsMatch(t, tt.childFksWanted, tt.semTable.GetChildForeignKeysList())
		})
	}
}

// TestGetParentForeignKeysList tests the function GetParentForeignKeysList
func TestGetParentForeignKeysList(t *testing.T) {
	tests := []struct {
		name            string
		semTable        *SemTable
		parentFksWanted []vindexes.ParentFKInfo
	}{
		{
			name: "Collect all FKs",
			semTable: &SemTable{
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(0): {
						pkInfo(nil, []string{"colb"}, []string{"child_colb"}),
						pkInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}),
					},
					SingleTableSet(1): {
						pkInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}),
						pkInfo(nil, []string{"cold"}, []string{"child_cold"}),
					},
				},
			},
			parentFksWanted: []vindexes.ParentFKInfo{
				pkInfo(nil, []string{"colb"}, []string{"child_colb"}),
				pkInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}),
				pkInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}),
				pkInfo(nil, []string{"cold"}, []string{"child_cold"}),
			},
		},
		{
			name: "Nil Map",
			semTable: &SemTable{
				parentForeignKeysInvolved: nil,
			},
			parentFksWanted: nil,
		},
		{
			name: "Empty Map",
			semTable: &SemTable{
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(0): {},
					SingleTableSet(1): nil,
				},
			},
			parentFksWanted: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ElementsMatch(t, tt.parentFksWanted, tt.semTable.GetParentForeignKeysList())
		})
	}
}

// TestRemoveParentForeignKey tests the functionality of RemoveParentForeignKey
func TestRemoveParentForeignKey(t *testing.T) {
	t1Table := &vindexes.Table{
		Keyspace: &vindexes.Keyspace{Name: "ks"},
		Name:     sqlparser.NewIdentifierCS("t1"),
	}
	t2Table := &vindexes.Table{
		Keyspace: &vindexes.Keyspace{Name: "ks"},
		Name:     sqlparser.NewIdentifierCS("t2"),
	}
	t3Table := &vindexes.Table{
		Keyspace: &vindexes.Keyspace{Name: "ks"},
		Name:     sqlparser.NewIdentifierCS("t3"),
	}
	tests := []struct {
		name            string
		semTable        *SemTable
		fkToIgnore      string
		parentFksWanted []vindexes.ParentFKInfo
		expectedErr     string
	}{
		{
			name: "Sucess",
			semTable: &SemTable{
				Tables: []TableInfo{
					&RealTable{
						Table: t1Table,
					}, &RealTable{
						Table: t2Table,
					},
				},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(0): {
						pkInfo(t3Table, []string{"colb"}, []string{"child_colb"}),
						pkInfo(t3Table, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}),
					},
					SingleTableSet(1): {
						pkInfo(t3Table, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}),
						pkInfo(t3Table, []string{"cold"}, []string{"child_cold"}),
					},
				},
			},
			fkToIgnore: "ks.t2|child_cold||ks.t3|cold",
			parentFksWanted: []vindexes.ParentFKInfo{
				pkInfo(t3Table, []string{"colb"}, []string{"child_colb"}),
				pkInfo(t3Table, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}),
				pkInfo(t3Table, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}),
			},
		}, {
			name: "Foreign to ignore doesn't match any fk",
			semTable: &SemTable{
				Tables: []TableInfo{
					&RealTable{
						Table: t1Table,
					}, &RealTable{
						Table: t2Table,
					},
				},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(0): {
						pkInfo(t3Table, []string{"colb"}, []string{"child_colb"}),
						pkInfo(t3Table, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}),
					},
					SingleTableSet(1): {
						pkInfo(t3Table, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}),
						pkInfo(t3Table, []string{"cold"}, []string{"child_cold"}),
					},
				},
			},
			fkToIgnore: "incorrect name",
			parentFksWanted: []vindexes.ParentFKInfo{
				pkInfo(t3Table, []string{"colb"}, []string{"child_colb"}),
				pkInfo(t3Table, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}),
				pkInfo(t3Table, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}),
				pkInfo(t3Table, []string{"cold"}, []string{"child_cold"}),
			},
		}, {
			name: "Table information not found",
			semTable: &SemTable{
				Tables: []TableInfo{},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(0).Merge(SingleTableSet(1)): {
						pkInfo(t3Table, []string{"colb"}, []string{"child_colb"}),
						pkInfo(t3Table, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}),
					},
				},
			},
			expectedErr: "[BUG] should only be used for single tables",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.semTable.RemoveParentForeignKey(tt.fkToIgnore)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.ElementsMatch(t, tt.parentFksWanted, tt.semTable.GetParentForeignKeysList())
		})
	}
}

// TestRemoveNonRequiredForeignKeys tests the functionality of RemoveNonRequiredForeignKeys.
func TestRemoveNonRequiredForeignKeys(t *testing.T) {
	hashVindex := &vindexes.Hash{}
	xxhashVindex := &vindexes.XXHash{}
	t1Table := &vindexes.Table{
		Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
		Name:     sqlparser.NewIdentifierCS("t1"),
		ColumnVindexes: []*vindexes.ColumnVindex{
			{
				Vindex:  hashVindex,
				Columns: sqlparser.MakeColumns("cola"),
			},
		},
	}
	t2Table := &vindexes.Table{
		Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
		Name:     sqlparser.NewIdentifierCS("t2"),
		ColumnVindexes: []*vindexes.ColumnVindex{
			{
				Vindex:  xxhashVindex,
				Columns: sqlparser.MakeColumns("cola", "colb", "colc"),
			},
		},
	}
	t4Table := &vindexes.Table{
		Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
		Name:     sqlparser.NewIdentifierCS("t4"),
		ColumnVindexes: []*vindexes.ColumnVindex{
			{
				Vindex:  hashVindex,
				Columns: sqlparser.MakeColumns("cola"),
			},
		},
	}
	t3Table := &vindexes.Table{
		Keyspace: &vindexes.Keyspace{Name: "ks2"},
		Name:     sqlparser.NewIdentifierCS("t3"),
	}
	tests := []struct {
		name           string
		verifyAllFks   bool
		semTable       *SemTable
		expectedErr    string
		childFkWanted  map[TableSet][]vindexes.ChildFKInfo
		parentFkWanted map[TableSet][]vindexes.ParentFKInfo
	}{
		{
			name:         "VerifyAllFks specified",
			verifyAllFks: true,
			semTable: &SemTable{
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{
					SingleTableSet(0): {
						ckInfo(nil, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
						ckInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
						ckInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
						ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
					},
					SingleTableSet(1): {
						ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
						ckInfo(nil, []string{"colc", "colx"}, []string{"child_colc", "child_colx"}, sqlparser.SetNull),
						ckInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
					},
				},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(0): {
						pkInfo(nil, []string{"pcola", "pcolx"}, []string{"cola", "colx"}),
						pkInfo(nil, []string{"pcolc"}, []string{"colc"}),
						pkInfo(nil, []string{"pcolb", "pcola"}, []string{"colb", "cola"}),
						pkInfo(nil, []string{"pcolb"}, []string{"colb"}),
						pkInfo(nil, []string{"pcola"}, []string{"cola"}),
						pkInfo(nil, []string{"pcolb", "pcolx"}, []string{"colb", "colx"}),
					},
				},
			},
			childFkWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(nil, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
					ckInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
					ckInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
					ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
				},
				SingleTableSet(1): {
					ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
					ckInfo(nil, []string{"colc", "colx"}, []string{"child_colc", "child_colx"}, sqlparser.SetNull),
					ckInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
				},
			},
			parentFkWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(nil, []string{"pcola", "pcolx"}, []string{"cola", "colx"}),
					pkInfo(nil, []string{"pcolc"}, []string{"colc"}),
					pkInfo(nil, []string{"pcolb", "pcola"}, []string{"colb", "cola"}),
					pkInfo(nil, []string{"pcolb"}, []string{"colb"}),
					pkInfo(nil, []string{"pcola"}, []string{"cola"}),
					pkInfo(nil, []string{"pcolb", "pcolx"}, []string{"colb", "colx"}),
				},
			},
		},
		{
			name: "Filtering - Keep cross keyspace parent foreign key",
			semTable: &SemTable{
				Tables: []TableInfo{
					&RealTable{
						Table: t1Table,
					},
					&RealTable{
						Table: t2Table,
					},
				},
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(0): {
						pkInfo(t3Table, []string{"pcola", "pcolx"}, []string{"cola", "colx"}),
					},
					SingleTableSet(1): {
						pkInfo(t3Table, []string{"pcolc", "pcolx"}, []string{"colc", "colx"}),
					},
				},
			},
			childFkWanted: map[TableSet][]vindexes.ChildFKInfo{},
			parentFkWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(t3Table, []string{"pcola", "pcolx"}, []string{"cola", "colx"}),
				},
				SingleTableSet(1): {
					pkInfo(t3Table, []string{"pcolc", "pcolx"}, []string{"colc", "colx"}),
				},
			},
		},
		{
			name: "Filtering - Shard scoped parent foreign keys",
			semTable: &SemTable{
				Tables: []TableInfo{
					&RealTable{
						Table: t1Table,
					},
					&RealTable{
						Table: t2Table,
					},
				},
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(0): {
						pkInfo(t4Table, []string{"cola", "colx"}, []string{"cola", "colx"}),
					},
					SingleTableSet(1): {
						pkInfo(t4Table, []string{"colc", "colx"}, []string{"colc", "colx"}),
					},
				},
			},
			childFkWanted: map[TableSet][]vindexes.ChildFKInfo{},
			parentFkWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): nil,
				SingleTableSet(1): {
					pkInfo(t4Table, []string{"colc", "colx"}, []string{"colc", "colx"}),
				},
			},
		},
		{
			name: "Filtering - Keep cross keyspace child foreign key",
			semTable: &SemTable{
				Tables: []TableInfo{
					&RealTable{
						Table: t1Table,
					},
					&RealTable{
						Table: t2Table,
					},
				},
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{
					SingleTableSet(0): {
						ckInfo(t3Table, []string{"cola", "colx"}, []string{"cola", "colx"}, sqlparser.Restrict),
					},
					SingleTableSet(1): {
						ckInfo(t3Table, []string{"colc", "colx"}, []string{"colc", "colx"}, sqlparser.Cascade),
					},
				},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{},
			},
			childFkWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(t3Table, []string{"cola", "colx"}, []string{"cola", "colx"}, sqlparser.Restrict),
				},
				SingleTableSet(1): {
					ckInfo(t3Table, []string{"colc", "colx"}, []string{"colc", "colx"}, sqlparser.Cascade),
				},
			},
			parentFkWanted: map[TableSet][]vindexes.ParentFKInfo{},
		},
		{
			name: "Filtering - Remove Restrict shard scoped foreign keys",
			semTable: &SemTable{
				Tables: []TableInfo{
					&RealTable{
						Table: t1Table,
					},
					&RealTable{
						Table: t2Table,
					},
				},
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{
					SingleTableSet(0): {
						ckInfo(t4Table, []string{"cola", "colx"}, []string{"cola", "colx"}, sqlparser.Restrict),
						ckInfo(t4Table, []string{"cola", "coly"}, []string{"cola", "coly"}, sqlparser.Cascade),
					},
					SingleTableSet(1): {
						ckInfo(t4Table, []string{"colc", "colx"}, []string{"colc", "colx"}, sqlparser.Restrict),
					},
				},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{},
			},
			childFkWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(t4Table, []string{"cola", "coly"}, []string{"cola", "coly"}, sqlparser.Cascade),
				},
				SingleTableSet(1): {
					ckInfo(t4Table, []string{"colc", "colx"}, []string{"colc", "colx"}, sqlparser.Restrict),
				},
			},
			parentFkWanted: map[TableSet][]vindexes.ParentFKInfo{},
		},
		{
			name: "Error - Reading table info for parent foreign keys",
			semTable: &SemTable{
				Tables: []TableInfo{
					&RealTable{
						Table: t1Table,
					},
					&RealTable{
						Table: t2Table,
					},
				},
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{
					SingleTableSet(0).Merge(SingleTableSet(1)): {},
				},
			},
			expectedErr: "[BUG] should only be used for single tables",
		},
		{
			name: "Error - Reading table info for child foreign keys",
			semTable: &SemTable{
				Tables: []TableInfo{
					&RealTable{
						Table: t1Table,
					},
					&RealTable{
						Table: t2Table,
					},
				},
				childForeignKeysInvolved: map[TableSet][]vindexes.ChildFKInfo{
					SingleTableSet(0).Merge(SingleTableSet(1)): {},
				},
				parentForeignKeysInvolved: map[TableSet][]vindexes.ParentFKInfo{},
			},
			expectedErr: "[BUG] should only be used for single tables",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.semTable.RemoveNonRequiredForeignKeys(tt.verifyAllFks, vindexes.DeleteAction)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.EqualValues(t, tt.childFkWanted, tt.semTable.childForeignKeysInvolved)
			require.EqualValues(t, tt.parentFkWanted, tt.semTable.parentForeignKeysInvolved)
		})
	}
}

func TestIsFkDependentColumnUpdated(t *testing.T) {
	keyspaceName := "ks"
	t3Table := &vindexes.Table{
		Keyspace: &vindexes.Keyspace{Name: keyspaceName},
		Name:     sqlparser.NewIdentifierCS("t3"),
	}
	tests := []struct {
		name       string
		query      string
		fakeSi     *FakeSI
		updatedErr string
	}{
		{
			name:  "updated child foreign key column is dependent on another updated column",
			query: "update t1 set col = id + 1, id = 6 where foo = 3",
			fakeSi: &FakeSI{
				KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
					keyspaceName: vschemapb.Keyspace_managed,
				},
				Tables: map[string]*vindexes.Table{
					"t1": {
						Name:     sqlparser.NewIdentifierCS("t1"),
						Keyspace: &vindexes.Keyspace{Name: keyspaceName},
						ChildForeignKeys: []vindexes.ChildFKInfo{
							ckInfo(t3Table, []string{"col"}, []string{"col"}, sqlparser.Cascade),
						},
					},
				},
			},
			updatedErr: "VT12001: unsupported: id column referenced in foreign key column col is itself updated",
		}, {
			name:  "updated parent foreign key column is dependent on another updated column",
			query: "update t1 set col = id + 1, id = 6 where foo = 3",
			fakeSi: &FakeSI{
				KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
					keyspaceName: vschemapb.Keyspace_managed,
				},
				Tables: map[string]*vindexes.Table{
					"t1": {
						Name:     sqlparser.NewIdentifierCS("t1"),
						Keyspace: &vindexes.Keyspace{Name: keyspaceName},
						ParentForeignKeys: []vindexes.ParentFKInfo{
							pkInfo(t3Table, []string{"col"}, []string{"col"}),
						},
					},
				},
			},
			updatedErr: "VT12001: unsupported: id column referenced in foreign key column col is itself updated",
		}, {
			name:  "no foreign key column is dependent on a updated value",
			query: "update t1 set col = id + 1 where foo = 3",
			fakeSi: &FakeSI{
				KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
					keyspaceName: vschemapb.Keyspace_managed,
				},
				Tables: map[string]*vindexes.Table{
					"t1": {
						Name:     sqlparser.NewIdentifierCS("t1"),
						Keyspace: &vindexes.Keyspace{Name: keyspaceName},
						ParentForeignKeys: []vindexes.ParentFKInfo{
							pkInfo(t3Table, []string{"col"}, []string{"col"}),
						},
					},
				},
			},
			updatedErr: "",
		}, {
			name:  "self-referenced foreign key",
			query: "update t1 set col = col + 1 where foo = 3",
			fakeSi: &FakeSI{
				KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
					keyspaceName: vschemapb.Keyspace_managed,
				},
				Tables: map[string]*vindexes.Table{
					"t1": {
						Name:     sqlparser.NewIdentifierCS("t1"),
						Keyspace: &vindexes.Keyspace{Name: keyspaceName},
						ParentForeignKeys: []vindexes.ParentFKInfo{
							pkInfo(t3Table, []string{"col"}, []string{"col"}),
						},
					},
				},
			},
			updatedErr: "",
		}, {
			name:  "no foreign keys",
			query: "update t1 set col = id + 1, id = 6 where foo = 3",
			fakeSi: &FakeSI{
				KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
					keyspaceName: vschemapb.Keyspace_managed,
				},
				Tables: map[string]*vindexes.Table{
					"t1": {
						Name:     sqlparser.NewIdentifierCS("t1"),
						Keyspace: &vindexes.Keyspace{Name: keyspaceName, Sharded: true},
					},
				},
			},
			updatedErr: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.query)
			require.NoError(t, err)
			semTable, err := Analyze(stmt, keyspaceName, tt.fakeSi)
			require.NoError(t, err)
			got := semTable.ErrIfFkDependentColumnUpdated(stmt.(*sqlparser.Update).Exprs)
			if tt.updatedErr == "" {
				require.NoError(t, got)
			} else {
				require.EqualError(t, got, tt.updatedErr)
			}
		})
	}
}

func TestHasNonLiteralForeignKeyUpdate(t *testing.T) {
	keyspaceName := "ks"
	t3Table := &vindexes.Table{
		Keyspace: &vindexes.Keyspace{Name: keyspaceName},
		Name:     sqlparser.NewIdentifierCS("t3"),
	}
	tests := []struct {
		name          string
		query         string
		fakeSi        *FakeSI
		hasNonLiteral bool
	}{
		{
			name:  "non literal child foreign key update",
			query: "update t1 set col = id + 1 where foo = 3",
			fakeSi: &FakeSI{
				KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
					keyspaceName: vschemapb.Keyspace_managed,
				},
				Tables: map[string]*vindexes.Table{
					"t1": {
						Name:     sqlparser.NewIdentifierCS("t1"),
						Keyspace: &vindexes.Keyspace{Name: keyspaceName},
						ChildForeignKeys: []vindexes.ChildFKInfo{
							ckInfo(t3Table, []string{"col"}, []string{"col"}, sqlparser.Cascade),
						},
					},
				},
			},
			hasNonLiteral: true,
		}, {
			name:  "non literal parent foreign key update",
			query: "update t1 set col = id + 1 where foo = 3",
			fakeSi: &FakeSI{
				KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
					keyspaceName: vschemapb.Keyspace_managed,
				},
				Tables: map[string]*vindexes.Table{
					"t1": {
						Name:     sqlparser.NewIdentifierCS("t1"),
						Keyspace: &vindexes.Keyspace{Name: keyspaceName},
						ParentForeignKeys: []vindexes.ParentFKInfo{
							pkInfo(t3Table, []string{"col"}, []string{"col"}),
						},
					},
				},
			},
			hasNonLiteral: true,
		}, {
			name:  "literal updates only",
			query: "update t1 set col = 1 where foo = 3",
			fakeSi: &FakeSI{
				KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
					keyspaceName: vschemapb.Keyspace_managed,
				},
				Tables: map[string]*vindexes.Table{
					"t1": {
						Name:     sqlparser.NewIdentifierCS("t1"),
						Keyspace: &vindexes.Keyspace{Name: keyspaceName},
						ParentForeignKeys: []vindexes.ParentFKInfo{
							pkInfo(t3Table, []string{"col"}, []string{"col"}),
						},
					},
				},
			},
			hasNonLiteral: false,
		}, {
			name:  "self-referenced foreign key",
			query: "update t1 set col = col + 1 where foo = 3",
			fakeSi: &FakeSI{
				KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
					keyspaceName: vschemapb.Keyspace_managed,
				},
				Tables: map[string]*vindexes.Table{
					"t1": {
						Name:     sqlparser.NewIdentifierCS("t1"),
						Keyspace: &vindexes.Keyspace{Name: keyspaceName},
						ParentForeignKeys: []vindexes.ParentFKInfo{
							pkInfo(t3Table, []string{"col"}, []string{"col"}),
						},
					},
				},
			},
			hasNonLiteral: true,
		}, {
			name:  "no foreign keys",
			query: "update t1 set col = id + 1 where foo = 3",
			fakeSi: &FakeSI{
				KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
					keyspaceName: vschemapb.Keyspace_managed,
				},
				Tables: map[string]*vindexes.Table{
					"t1": {
						Name:     sqlparser.NewIdentifierCS("t1"),
						Keyspace: &vindexes.Keyspace{Name: keyspaceName},
					},
				},
			},
			hasNonLiteral: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.query)
			require.NoError(t, err)
			semTable, err := Analyze(stmt, keyspaceName, tt.fakeSi)
			require.NoError(t, err)
			got := semTable.HasNonLiteralForeignKeyUpdate(stmt.(*sqlparser.Update).Exprs)
			require.EqualValues(t, tt.hasNonLiteral, got)
		})
	}
}
