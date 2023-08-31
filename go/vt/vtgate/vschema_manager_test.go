package vtgate

import (
	"testing"

	"vitess.io/vitess/go/test/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestVSchemaUpdate(t *testing.T) {
	cols1 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("id"),
		Type: querypb.Type_INT64,
	}}
	cols2 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("uid"),
		Type: querypb.Type_INT64,
	}, {
		Name: sqlparser.NewIdentifierCI("name"),
		Type: querypb.Type_VARCHAR,
	}}
	ks := &vindexes.Keyspace{Name: "ks"}
	tblNoCol := &vindexes.Table{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, ColumnListAuthoritative: true}
	tblCol1 := &vindexes.Table{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols1, ColumnListAuthoritative: true}
	tblCol2 := &vindexes.Table{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols2, ColumnListAuthoritative: true}
	tblCol2NA := &vindexes.Table{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols2}

	vindexTable_multicol_t1 := &vindexes.Table{
		Name:                    sqlparser.NewIdentifierCS("multicol_t1"),
		Keyspace:                ks,
		Columns:                 cols2,
		ColumnListAuthoritative: true,
	}
	vindexTable_multicol_t2 := &vindexes.Table{
		Name:                    sqlparser.NewIdentifierCS("multicol_t2"),
		Keyspace:                ks,
		Columns:                 cols2,
		ColumnListAuthoritative: true,
	}
	vindexTable_t1 := &vindexes.Table{
		Name:                    sqlparser.NewIdentifierCS("t1"),
		Keyspace:                ks,
		Columns:                 cols1,
		ColumnListAuthoritative: true,
	}
	vindexTable_t2 := &vindexes.Table{
		Name:                    sqlparser.NewIdentifierCS("t2"),
		Keyspace:                ks,
		Columns:                 cols1,
		ColumnListAuthoritative: true,
	}
	sqlparserCols1 := sqlparser.MakeColumns("id")
	sqlparserCols2 := sqlparser.MakeColumns("uid", "name")

	vindexTable_multicol_t1.ChildForeignKeys = append(vindexTable_multicol_t1.ChildForeignKeys, vindexes.ChildFKInfo{
		Table:         vindexTable_multicol_t2,
		ChildColumns:  sqlparserCols2,
		ParentColumns: sqlparserCols2,
		OnDelete:      sqlparser.NoAction,
		OnUpdate:      sqlparser.Restrict,
	})
	vindexTable_multicol_t2.ParentForeignKeys = append(vindexTable_multicol_t2.ParentForeignKeys, vindexes.ParentFKInfo{
		Table:         vindexTable_multicol_t1,
		ChildColumns:  sqlparserCols2,
		ParentColumns: sqlparserCols2,
	})
	vindexTable_t1.ChildForeignKeys = append(vindexTable_t1.ChildForeignKeys, vindexes.ChildFKInfo{
		Table:         vindexTable_t2,
		ChildColumns:  sqlparserCols1,
		ParentColumns: sqlparserCols1,
		OnDelete:      sqlparser.SetNull,
		OnUpdate:      sqlparser.Cascade,
	})
	vindexTable_t2.ParentForeignKeys = append(vindexTable_t2.ParentForeignKeys, vindexes.ParentFKInfo{
		Table:         vindexTable_t1,
		ChildColumns:  sqlparserCols1,
		ParentColumns: sqlparserCols1,
	})

	tcases := []struct {
		name           string
		srvVschema     *vschemapb.SrvVSchema
		currentVSchema *vindexes.VSchema
		schema         map[string]*vindexes.TableInfo
		expected       *vindexes.VSchema
	}{{
		name: "0 Schematracking- 1 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblCol2NA}),
	}, {
		name:       "1 Schematracking- 0 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, nil),
		schema:     map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		expected:   makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblCol1}),
	}, {
		name:       "1 Schematracking - 1 srvVSchema (no columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {}}),
		schema:     map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (no columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {
			ColumnListAuthoritative: true,
		}}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblNoCol}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: true,
			},
		}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema tracker will be ignored for authoritative tables.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblCol2}),
	}, {
		name:     "srvVschema received as nil",
		schema:   map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		expected: makeTestEmptyVSchema(),
	}, {
		name:           "srvVschema received as nil - have existing vschema",
		currentVSchema: &vindexes.VSchema{},
		schema:         map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		expected:       &vindexes.VSchema{},
	}, {
		name:           "foreign keys in schema",
		currentVSchema: &vindexes.VSchema{},
		schema: map[string]*vindexes.TableInfo{
			"t1": {
				Columns: cols1,
			},
			"t2": {
				Columns: cols1,
				ForeignKeys: []*sqlparser.ForeignKeyDefinition{
					{
						Source: sqlparser.MakeColumns("id"),
						ReferenceDefinition: &sqlparser.ReferenceDefinition{
							ReferencedTable:   sqlparser.NewTableName("t1"),
							ReferencedColumns: sqlparserCols1,
							OnUpdate:          sqlparser.Cascade,
							OnDelete:          sqlparser.SetNull,
						},
					},
				},
			},
			"multicol_t1": {
				Columns: cols2,
			},
			"multicol_t2": {
				Columns: cols2,
				ForeignKeys: []*sqlparser.ForeignKeyDefinition{
					{
						Source: sqlparser.MakeColumns("uid", "name"),
						ReferenceDefinition: &sqlparser.ReferenceDefinition{
							ReferencedTable:   sqlparser.NewTableName("multicol_t1"),
							ReferencedColumns: sqlparserCols2,
							OnUpdate:          sqlparser.Restrict,
							OnDelete:          sqlparser.NoAction,
						},
					},
				},
			},
		},
		srvVschema: &vschemapb.SrvVSchema{
			Keyspaces: map[string]*vschemapb.Keyspace{
				"ks": {
					Sharded:        false,
					ForeignKeyMode: vschemapb.Keyspace_FK_MANAGED,
					Tables: map[string]*vschemapb.Table{
						"t1": {
							Columns: []*vschemapb.Column{
								{
									Name: "id",
									Type: querypb.Type_INT64,
								},
							},
						},
						"t2": {
							Columns: []*vschemapb.Column{
								{
									Name: "id",
									Type: querypb.Type_INT64,
								},
							},
						},
						"multicol_t1": {
							Columns: []*vschemapb.Column{
								{
									Name: "uid",
									Type: querypb.Type_INT64,
								}, {
									Name: "name",
									Type: querypb.Type_VARCHAR,
								},
							},
						}, "multicol_t2": {
							Columns: []*vschemapb.Column{
								{
									Name: "uid",
									Type: querypb.Type_INT64,
								}, {
									Name: "name",
									Type: querypb.Type_VARCHAR,
								},
							},
						},
					},
				},
			},
		},
		expected: &vindexes.VSchema{
			RoutingRules: map[string]*vindexes.RoutingRule{},
			Keyspaces: map[string]*vindexes.KeyspaceSchema{
				"ks": {
					Keyspace:       ks,
					ForeignKeyMode: vschemapb.Keyspace_FK_MANAGED,
					Vindexes:       map[string]vindexes.Vindex{},
					Tables: map[string]*vindexes.Table{
						"t1":          vindexTable_t1,
						"t2":          vindexTable_t2,
						"multicol_t1": vindexTable_multicol_t1,
						"multicol_t2": vindexTable_multicol_t2,
					},
				},
			},
		},
	}}

	vm := &VSchemaManager{}
	var vs *vindexes.VSchema
	vm.subscriber = func(vschema *vindexes.VSchema, _ *VSchemaStats) {
		vs = vschema
		vs.ResetCreated()
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			vs = nil
			vm.schema = &fakeSchema{t: tcase.schema}
			vm.currentSrvVschema = nil
			vm.currentVschema = tcase.currentVSchema
			vm.VSchemaUpdate(tcase.srvVschema, nil)

			utils.MustMatchFn(".globalTables", ".uniqueVindexes")(t, tcase.expected, vs)
			if tcase.srvVschema != nil {
				utils.MustMatch(t, vs, vm.currentVschema, "currentVschema should have same reference as Vschema")
			}
		})
	}
}

func TestRebuildVSchema(t *testing.T) {
	cols1 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("id"),
		Type: querypb.Type_INT64,
	}}
	cols2 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("uid"),
		Type: querypb.Type_INT64,
	}, {
		Name: sqlparser.NewIdentifierCI("name"),
		Type: querypb.Type_VARCHAR,
	}}
	ks := &vindexes.Keyspace{Name: "ks"}
	tblNoCol := &vindexes.Table{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, ColumnListAuthoritative: true}
	tblCol1 := &vindexes.Table{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols1, ColumnListAuthoritative: true}
	tblCol2 := &vindexes.Table{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols2, ColumnListAuthoritative: true}
	tblCol2NA := &vindexes.Table{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols2}

	tcases := []struct {
		name       string
		srvVschema *vschemapb.SrvVSchema
		schema     map[string]*vindexes.TableInfo
		expected   *vindexes.VSchema
	}{{
		name: "0 Schematracking- 1 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblCol2NA}),
	}, {
		name:       "1 Schematracking- 0 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, nil),
		schema:     map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		expected:   makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblCol1}),
	}, {
		name:       "1 Schematracking - 1 srvVSchema (no columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {}}),
		schema:     map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (no columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {
			ColumnListAuthoritative: true,
		}}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblNoCol}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: true,
			},
		}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema tracker will be ignored for authoritative tables.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"tbl": tblCol2}),
	}, {
		name:   "srvVschema received as nil",
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
	}}

	vm := &VSchemaManager{}
	var vs *vindexes.VSchema
	vm.subscriber = func(vschema *vindexes.VSchema, _ *VSchemaStats) {
		vs = vschema
		vs.ResetCreated()
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			vs = nil
			vm.schema = &fakeSchema{t: tcase.schema}
			vm.currentSrvVschema = tcase.srvVschema
			vm.currentVschema = nil
			vm.Rebuild()

			utils.MustMatchFn(".globalTables", ".uniqueVindexes")(t, tcase.expected, vs)
			if vs != nil {
				utils.MustMatch(t, vs, vm.currentVschema, "currentVschema should have same reference as Vschema")
			}
		})
	}
}

func makeTestVSchema(ks string, sharded bool, tbls map[string]*vindexes.Table) *vindexes.VSchema {
	keyspaceSchema := &vindexes.KeyspaceSchema{
		Keyspace: &vindexes.Keyspace{
			Name:    ks,
			Sharded: sharded,
		},
		// Default foreign key mode
		ForeignKeyMode: vschemapb.Keyspace_FK_UNMANAGED,
		Tables:         tbls,
		Vindexes:       map[string]vindexes.Vindex{},
	}
	vs := makeTestEmptyVSchema()
	vs.Keyspaces[ks] = keyspaceSchema
	vs.ResetCreated()
	return vs
}

func makeTestEmptyVSchema() *vindexes.VSchema {
	return &vindexes.VSchema{
		RoutingRules: map[string]*vindexes.RoutingRule{},
		Keyspaces:    map[string]*vindexes.KeyspaceSchema{},
	}
}

func makeTestSrvVSchema(ks string, sharded bool, tbls map[string]*vschemapb.Table) *vschemapb.SrvVSchema {
	keyspaceSchema := &vschemapb.Keyspace{
		Sharded: sharded,
		Tables:  tbls,
		// Default foreign key mode
		ForeignKeyMode: vschemapb.Keyspace_FK_UNMANAGED,
	}
	return &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{ks: keyspaceSchema},
	}
}

type fakeSchema struct {
	t map[string]*vindexes.TableInfo
}

func (f *fakeSchema) Tables(string) map[string]*vindexes.TableInfo {
	return f.t
}

func (f *fakeSchema) Views(string) map[string]sqlparser.SelectStatement {
	return nil
}

var _ SchemaInfo = (*fakeSchema)(nil)
