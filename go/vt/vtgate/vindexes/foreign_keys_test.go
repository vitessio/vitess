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

// TestTable_CrossShardParentFKs tests the functionality of the method CrossShardParentFKs.
func TestTable_CrossShardParentFKs(t *testing.T) {
	tests := []struct {
		name                   string
		table                  *Table
		wantCrossShardFKTables []string
	}{
		{
			name: "Unsharded keyspace",
			table: &Table{
				ColumnVindexes: []*ColumnVindex{
					{
						Name:   "v1",
						Vindex: binVindex,
						Columns: []sqlparser.IdentifierCI{
							sqlparser.NewIdentifierCI("col1"),
						},
					},
				},
				Keyspace: &Keyspace{
					Name:    "ks",
					Sharded: false,
				},
				ParentForeignKeys: []ParentFKInfo{
					{
						Table: &Table{
							Name: sqlparser.NewIdentifierCS("t1"),
							Keyspace: &Keyspace{
								Name:    "ks",
								Sharded: false,
							},
							ColumnVindexes: []*ColumnVindex{
								{
									Name:   "v2",
									Vindex: binVindex,
									Columns: []sqlparser.IdentifierCI{
										sqlparser.NewIdentifierCI("col4"),
									},
								},
							},
						},
						ChildColumns:  sqlparser.MakeColumns("col1"),
						ParentColumns: sqlparser.MakeColumns("col3"),
					},
				},
			},
			wantCrossShardFKTables: []string{},
		},
		{
			name: "No Parent FKs",
			table: &Table{
				ColumnVindexes: []*ColumnVindex{
					{
						Name:   "v1",
						Vindex: binVindex,
						Columns: []sqlparser.IdentifierCI{
							sqlparser.NewIdentifierCI("col1"),
						},
					},
				},
				Keyspace: &Keyspace{
					Name:    "ks",
					Sharded: true,
				},
			},
			wantCrossShardFKTables: []string{},
		}, {
			name: "Keyspaces don't match",
			table: &Table{
				ColumnVindexes: []*ColumnVindex{
					{
						Name:   "v1",
						Vindex: binVindex,
						Columns: []sqlparser.IdentifierCI{
							sqlparser.NewIdentifierCI("col1"),
						},
					},
				},
				Keyspace: &Keyspace{
					Name:    "ks",
					Sharded: false,
				},
				ParentForeignKeys: []ParentFKInfo{
					{
						Table: &Table{
							Name: sqlparser.NewIdentifierCS("t1"),
							Keyspace: &Keyspace{
								Name:    "ks2",
								Sharded: false,
							},
							ColumnVindexes: []*ColumnVindex{
								{
									Name:   "v2",
									Vindex: binVindex,
									Columns: []sqlparser.IdentifierCI{
										sqlparser.NewIdentifierCI("col4"),
									},
								},
							},
						},
						ChildColumns:  sqlparser.MakeColumns("col1"),
						ParentColumns: sqlparser.MakeColumns("col4"),
					},
				},
			},
			wantCrossShardFKTables: []string{"t1"},
		}, {
			name: "Column Vindexes don't match",
			table: &Table{
				ColumnVindexes: []*ColumnVindex{
					{
						Name:   "v1",
						Vindex: binVindex,
						Columns: []sqlparser.IdentifierCI{
							sqlparser.NewIdentifierCI("col1"),
						},
					},
				},
				Keyspace: &Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				ParentForeignKeys: []ParentFKInfo{
					{
						Table: &Table{
							Name: sqlparser.NewIdentifierCS("t1"),
							Keyspace: &Keyspace{
								Name:    "ks",
								Sharded: true,
							},
							ColumnVindexes: []*ColumnVindex{
								{
									Name:   "v2",
									Vindex: binOnlyVindex,
									Columns: []sqlparser.IdentifierCI{
										sqlparser.NewIdentifierCI("col4"),
									},
								},
							},
						},
						ChildColumns:  sqlparser.MakeColumns("col1"),
						ParentColumns: sqlparser.MakeColumns("col4"),
					},
				},
			},
			wantCrossShardFKTables: []string{"t1"},
		}, {
			name: "Child FK doesn't contain primary vindex",
			table: &Table{
				ColumnVindexes: []*ColumnVindex{
					{
						Name:   "v1",
						Vindex: binVindex,
						Columns: []sqlparser.IdentifierCI{
							sqlparser.NewIdentifierCI("col1"),
							sqlparser.NewIdentifierCI("col2"),
							sqlparser.NewIdentifierCI("col3"),
						},
					},
				},
				Keyspace: &Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				ParentForeignKeys: []ParentFKInfo{
					{
						Table: &Table{
							Name: sqlparser.NewIdentifierCS("t1"),
							Keyspace: &Keyspace{
								Name:    "ks",
								Sharded: true,
							},
							ColumnVindexes: []*ColumnVindex{
								{
									Name:   "v2",
									Vindex: binVindex,
									Columns: []sqlparser.IdentifierCI{
										sqlparser.NewIdentifierCI("col4"),
										sqlparser.NewIdentifierCI("col5"),
										sqlparser.NewIdentifierCI("col6"),
									},
								},
							},
						},
						ChildColumns:  sqlparser.MakeColumns("col3", "col9", "col1"),
						ParentColumns: sqlparser.MakeColumns("col4", "col5", "col6"),
					},
				},
			},
			wantCrossShardFKTables: []string{"t1"},
		}, {
			name: "Parent FK doesn't contain primary vindex",
			table: &Table{
				ColumnVindexes: []*ColumnVindex{
					{
						Name:   "v1",
						Vindex: binVindex,
						Columns: []sqlparser.IdentifierCI{
							sqlparser.NewIdentifierCI("col1"),
							sqlparser.NewIdentifierCI("col2"),
							sqlparser.NewIdentifierCI("col3"),
						},
					},
				},
				Keyspace: &Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				ParentForeignKeys: []ParentFKInfo{
					{
						Table: &Table{
							Name: sqlparser.NewIdentifierCS("t1"),
							Keyspace: &Keyspace{
								Name:    "ks",
								Sharded: true,
							},
							ColumnVindexes: []*ColumnVindex{
								{
									Name:   "v2",
									Vindex: binVindex,
									Columns: []sqlparser.IdentifierCI{
										sqlparser.NewIdentifierCI("col4"),
										sqlparser.NewIdentifierCI("col5"),
										sqlparser.NewIdentifierCI("col6"),
									},
								},
							},
						},
						ChildColumns:  sqlparser.MakeColumns("col1", "col2", "col3"),
						ParentColumns: sqlparser.MakeColumns("col4", "col9", "col6"),
					},
				},
			},
			wantCrossShardFKTables: []string{"t1"},
		}, {
			name: "Indexes of the two FKs with column vindexes don't line up",
			table: &Table{
				ColumnVindexes: []*ColumnVindex{
					{
						Name:   "v1",
						Vindex: binVindex,
						Columns: []sqlparser.IdentifierCI{
							sqlparser.NewIdentifierCI("col1"),
							sqlparser.NewIdentifierCI("col2"),
							sqlparser.NewIdentifierCI("col3"),
						},
					},
				},
				Keyspace: &Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				ParentForeignKeys: []ParentFKInfo{
					{
						Table: &Table{
							Name: sqlparser.NewIdentifierCS("t1"),
							Keyspace: &Keyspace{
								Name:    "ks",
								Sharded: true,
							},
							ColumnVindexes: []*ColumnVindex{
								{
									Name:   "v2",
									Vindex: binVindex,
									Columns: []sqlparser.IdentifierCI{
										sqlparser.NewIdentifierCI("col4"),
										sqlparser.NewIdentifierCI("col5"),
										sqlparser.NewIdentifierCI("col6"),
									},
								},
							},
						},
						ChildColumns:  sqlparser.MakeColumns("col1", "col2", "col3", "col9"),
						ParentColumns: sqlparser.MakeColumns("col4", "col9", "col5", "col6"),
					},
				},
			},
			wantCrossShardFKTables: []string{"t1"},
		}, {
			name: "Shard scoped foreign key constraint",
			table: &Table{
				ColumnVindexes: []*ColumnVindex{
					{
						Name:   "v1",
						Vindex: binVindex,
						Columns: []sqlparser.IdentifierCI{
							sqlparser.NewIdentifierCI("col1"),
							sqlparser.NewIdentifierCI("col2"),
							sqlparser.NewIdentifierCI("col3"),
						},
					},
				},
				Keyspace: &Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				ParentForeignKeys: []ParentFKInfo{
					{
						Table: &Table{
							Name: sqlparser.NewIdentifierCS("t1"),
							Keyspace: &Keyspace{
								Name:    "ks",
								Sharded: true,
							},
							ColumnVindexes: []*ColumnVindex{
								{
									Name:   "v2",
									Vindex: binVindex,
									Columns: []sqlparser.IdentifierCI{
										sqlparser.NewIdentifierCI("col4"),
										sqlparser.NewIdentifierCI("col5"),
										sqlparser.NewIdentifierCI("col6"),
									},
								},
							},
						},
						ChildColumns:  sqlparser.MakeColumns("col1", "cola", "col2", "col3", "colb"),
						ParentColumns: sqlparser.MakeColumns("col4", "col9", "col5", "col6", "colc"),
					},
				},
			},
			wantCrossShardFKTables: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crossShardFks := tt.table.CrossShardParentFKs()
			var crossShardFkTables []string
			for _, fk := range crossShardFks {
				crossShardFkTables = append(crossShardFkTables, fk.Table.Name.String())
			}
			require.ElementsMatch(t, tt.wantCrossShardFKTables, crossShardFkTables)
		})
	}
}
