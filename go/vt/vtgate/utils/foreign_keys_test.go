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

package utils

import (
	"testing"

	"github.com/stretchr/testify/require"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestMarkErrorIfCyclesInFk(t *testing.T) {
	ksName := "ks"
	keyspace := &vindexes.Keyspace{
		Name: ksName,
	}
	tests := []struct {
		name              string
		getVschema        func() *vindexes.VSchema
		errWanted         string
		tablesOrderWanted []string
	}{
		{
			name: "Has a direct cycle",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.Table{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
								},
								"t2": {
									Name:     sqlparser.NewIdentifierCS("t2"),
									Keyspace: keyspace,
								},
								"t3": {
									Name:     sqlparser.NewIdentifierCS("t3"),
									Keyspace: keyspace,
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t2", createFkDefinition([]string{"col"}, "t1", []string{"col"}, sqlparser.SetNull, sqlparser.SetNull))
				_ = vschema.AddForeignKey("ks", "t3", createFkDefinition([]string{"col"}, "t2", []string{"col"}, sqlparser.SetNull, sqlparser.SetNull))
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"col"}, "t3", []string{"col"}, sqlparser.SetNull, sqlparser.SetNull))
				return vschema
			},
			errWanted: "VT09019: keyspace 'ks' has cyclic foreign keys",
		},
		{
			name: "Has a direct cycle but there is a restrict constraint in between",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.Table{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
								},
								"t2": {
									Name:     sqlparser.NewIdentifierCS("t2"),
									Keyspace: keyspace,
								},
								"t3": {
									Name:     sqlparser.NewIdentifierCS("t3"),
									Keyspace: keyspace,
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t2", createFkDefinition([]string{"col"}, "t1", []string{"col"}, sqlparser.SetNull, sqlparser.SetNull))
				_ = vschema.AddForeignKey("ks", "t3", createFkDefinition([]string{"col"}, "t2", []string{"col"}, sqlparser.Restrict, sqlparser.Restrict))
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"col"}, "t3", []string{"col"}, sqlparser.SetNull, sqlparser.SetNull))
				return vschema
			},
			errWanted:         "",
			tablesOrderWanted: []string{"t3", "t1", "t2"},
		},
		{
			name: "No cycle",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.Table{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
								},
								"t2": {
									Name:     sqlparser.NewIdentifierCS("t2"),
									Keyspace: keyspace,
								},
								"t3": {
									Name:     sqlparser.NewIdentifierCS("t3"),
									Keyspace: keyspace,
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t2", createFkDefinition([]string{"col"}, "t1", []string{"col"}, sqlparser.Cascade, sqlparser.Cascade))
				_ = vschema.AddForeignKey("ks", "t3", createFkDefinition([]string{"col"}, "t2", []string{"col"}, sqlparser.Cascade, sqlparser.Cascade))
				return vschema
			},
			errWanted: "",
		}, {
			name: "Self-referencing foreign key with delete cascade",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.Table{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
									Columns: []vindexes.Column{
										{
											Name: sqlparser.NewIdentifierCI("id"),
										},
										{
											Name: sqlparser.NewIdentifierCI("manager_id"),
										},
									},
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"manager_id"}, "t1", []string{"id"}, sqlparser.SetNull, sqlparser.Cascade))
				return vschema
			},
			errWanted: "VT09019: keyspace 'ks' has cyclic foreign keys. Cycle exists between [ks.t1.id ks.t1.id]",
		}, {
			name: "Self-referencing foreign key without delete cascade",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.Table{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
									Columns: []vindexes.Column{
										{
											Name: sqlparser.NewIdentifierCI("id"),
										},
										{
											Name: sqlparser.NewIdentifierCI("manager_id"),
										},
									},
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"manager_id"}, "t1", []string{"id"}, sqlparser.SetNull, sqlparser.SetNull))
				return vschema
			},
			errWanted:         "",
			tablesOrderWanted: []string{"t1"},
		}, {
			name: "Has an indirect cycle because of cascades",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.Table{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
									Columns: []vindexes.Column{
										{
											Name: sqlparser.NewIdentifierCI("a"),
										},
										{
											Name: sqlparser.NewIdentifierCI("b"),
										},
										{
											Name: sqlparser.NewIdentifierCI("c"),
										},
									},
								},
								"t2": {
									Name:     sqlparser.NewIdentifierCS("t2"),
									Keyspace: keyspace,
									Columns: []vindexes.Column{
										{
											Name: sqlparser.NewIdentifierCI("d"),
										},
										{
											Name: sqlparser.NewIdentifierCI("e"),
										},
										{
											Name: sqlparser.NewIdentifierCI("f"),
										},
									},
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t2", createFkDefinition([]string{"f"}, "t1", []string{"a"}, sqlparser.SetNull, sqlparser.Cascade))
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"b"}, "t2", []string{"e"}, sqlparser.SetNull, sqlparser.Cascade))
				return vschema
			},
			errWanted: "VT09019: keyspace 'ks' has cyclic foreign keys",
		}, {
			name: "Cycle part of a multi-column foreign key",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.Table{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
								},
								"t2": {
									Name:     sqlparser.NewIdentifierCS("t2"),
									Keyspace: keyspace,
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t2", createFkDefinition([]string{"e", "f"}, "t1", []string{"a", "b"}, sqlparser.SetNull, sqlparser.SetNull))
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"b"}, "t2", []string{"e"}, sqlparser.SetNull, sqlparser.SetNull))
				return vschema
			},
			errWanted: "VT09019: keyspace 'ks' has cyclic foreign keys",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vschema := tt.getVschema()
			MarkErrorIfCyclesInFkAndOrderTables(vschema)
			if tt.errWanted != "" {
				require.ErrorContains(t, vschema.Keyspaces[ksName].Error, tt.errWanted)
				return
			}
			require.NoError(t, vschema.Keyspaces[ksName].Error)
			require.EqualValues(t, tt.tablesOrderWanted, getTableOrder(vschema.Keyspaces[ksName].Tables))
		})
	}
}

func getTableOrder(tables map[string]*vindexes.Table) []string {
	var orderedTables []string
	for i := 1; i <= len(tables); i++ {
		for _, table := range tables {
			if table.FkOrder == i {
				orderedTables = append(orderedTables, table.GetTableName().Name.String())
			}
		}
	}
	return orderedTables
}

// createFkDefinition is a helper function to create a Foreign key definition struct from the columns used in it provided as list of strings.
func createFkDefinition(childCols []string, parentTableName string, parentCols []string, onUpdate, onDelete sqlparser.ReferenceAction) *sqlparser.ForeignKeyDefinition {
	pKs, pTbl, _ := sqlparser.NewTestParser().ParseTable(parentTableName)
	return &sqlparser.ForeignKeyDefinition{
		Source: sqlparser.MakeColumns(childCols...),
		ReferenceDefinition: &sqlparser.ReferenceDefinition{
			ReferencedTable:   sqlparser.NewTableNameWithQualifier(pTbl, pKs),
			ReferencedColumns: sqlparser.MakeColumns(parentCols...),
			OnUpdate:          onUpdate,
			OnDelete:          onDelete,
		},
	}
}
