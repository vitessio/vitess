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
		Name: sqlparser.NewColIdent("id"),
		Type: querypb.Type_INT64,
	}}
	cols2 := []vindexes.Column{{
		Name: sqlparser.NewColIdent("uid"),
		Type: querypb.Type_INT64,
	}, {
		Name: sqlparser.NewColIdent("name"),
		Type: querypb.Type_VARCHAR,
	}}
	ks := &vindexes.Keyspace{Name: "ks"}
	dual := &vindexes.Table{Type: vindexes.TypeReference, Name: sqlparser.NewTableIdent("dual"), Keyspace: ks}
	tblNoCol := &vindexes.Table{Name: sqlparser.NewTableIdent("tbl"), Keyspace: ks, ColumnListAuthoritative: true}
	tblCol1 := &vindexes.Table{Name: sqlparser.NewTableIdent("tbl"), Keyspace: ks, Columns: cols1, ColumnListAuthoritative: true}
	tblCol2 := &vindexes.Table{Name: sqlparser.NewTableIdent("tbl"), Keyspace: ks, Columns: cols2, ColumnListAuthoritative: true}
	tblCol2NA := &vindexes.Table{Name: sqlparser.NewTableIdent("tbl"), Keyspace: ks, Columns: cols2}

	tcases := []struct {
		name           string
		srvVschema     *vschemapb.SrvVSchema
		currentVSchema *vindexes.VSchema
		schema         map[string][]vindexes.Column
		expected       *vindexes.VSchema
	}{{
		name: "0 Schematracking- 1 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblCol2NA}),
	}, {
		name:       "1 Schematracking- 0 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, nil),
		schema:     map[string][]vindexes.Column{"tbl": cols1},
		expected:   makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblCol1}),
	}, {
		name:       "1 Schematracking - 1 srvVSchema (no columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {}}),
		schema:     map[string][]vindexes.Column{"tbl": cols1},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		schema: map[string][]vindexes.Column{"tbl": cols1},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (no columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {
			ColumnListAuthoritative: true,
		}}),
		schema: map[string][]vindexes.Column{"tbl": cols1},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblNoCol}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: true,
			},
		}),
		schema: map[string][]vindexes.Column{"tbl": cols1},
		// schema tracker will be ignored for authoritative tables.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblCol2}),
	}, {
		name:     "srvVschema received as nil",
		schema:   map[string][]vindexes.Column{"tbl": cols1},
		expected: makeTestEmptyVSchema(),
	}, {
		name:           "srvVschema received as nil - have existing vschema",
		currentVSchema: &vindexes.VSchema{},
		schema:         map[string][]vindexes.Column{"tbl": cols1},
		expected:       &vindexes.VSchema{},
	}}

	vm := &VSchemaManager{}
	var vs *vindexes.VSchema
	vm.subscriber = func(vschema *vindexes.VSchema, _ *VSchemaStats) {
		vs = vschema
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			vs = nil
			vm.schema = &fakeSchema{t: tcase.schema}
			vm.currentSrvVschema = nil
			vm.currentVschema = tcase.currentVSchema
			vm.VSchemaUpdate(tcase.srvVschema, nil)

			utils.MustMatchFn(".uniqueTables", ".uniqueVindexes")(t, tcase.expected, vs)
			if tcase.srvVschema != nil {
				utils.MustMatch(t, vs, vm.currentVschema, "currentVschema should have same reference as Vschema")
			}
		})
	}
}

func TestRebuildVSchema(t *testing.T) {
	cols1 := []vindexes.Column{{
		Name: sqlparser.NewColIdent("id"),
		Type: querypb.Type_INT64,
	}}
	cols2 := []vindexes.Column{{
		Name: sqlparser.NewColIdent("uid"),
		Type: querypb.Type_INT64,
	}, {
		Name: sqlparser.NewColIdent("name"),
		Type: querypb.Type_VARCHAR,
	}}
	ks := &vindexes.Keyspace{Name: "ks"}
	dual := &vindexes.Table{Type: vindexes.TypeReference, Name: sqlparser.NewTableIdent("dual"), Keyspace: ks}
	tblNoCol := &vindexes.Table{Name: sqlparser.NewTableIdent("tbl"), Keyspace: ks, ColumnListAuthoritative: true}
	tblCol1 := &vindexes.Table{Name: sqlparser.NewTableIdent("tbl"), Keyspace: ks, Columns: cols1, ColumnListAuthoritative: true}
	tblCol2 := &vindexes.Table{Name: sqlparser.NewTableIdent("tbl"), Keyspace: ks, Columns: cols2, ColumnListAuthoritative: true}
	tblCol2NA := &vindexes.Table{Name: sqlparser.NewTableIdent("tbl"), Keyspace: ks, Columns: cols2}

	tcases := []struct {
		name       string
		srvVschema *vschemapb.SrvVSchema
		schema     map[string][]vindexes.Column
		expected   *vindexes.VSchema
	}{{
		name: "0 Schematracking- 1 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblCol2NA}),
	}, {
		name:       "1 Schematracking- 0 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, nil),
		schema:     map[string][]vindexes.Column{"tbl": cols1},
		expected:   makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblCol1}),
	}, {
		name:       "1 Schematracking - 1 srvVSchema (no columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {}}),
		schema:     map[string][]vindexes.Column{"tbl": cols1},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		schema: map[string][]vindexes.Column{"tbl": cols1},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (no columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {
			ColumnListAuthoritative: true,
		}}),
		schema: map[string][]vindexes.Column{"tbl": cols1},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblNoCol}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: true,
			},
		}),
		schema: map[string][]vindexes.Column{"tbl": cols1},
		// schema tracker will be ignored for authoritative tables.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.Table{"dual": dual, "tbl": tblCol2}),
	}, {
		name:   "srvVschema received as nil",
		schema: map[string][]vindexes.Column{"tbl": cols1},
	}}

	vm := &VSchemaManager{}
	var vs *vindexes.VSchema
	vm.subscriber = func(vschema *vindexes.VSchema, _ *VSchemaStats) {
		vs = vschema
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			vs = nil
			vm.schema = &fakeSchema{t: tcase.schema}
			vm.currentSrvVschema = tcase.srvVschema
			vm.currentVschema = nil
			vm.Rebuild()

			utils.MustMatchFn(".uniqueTables", ".uniqueVindexes")(t, tcase.expected, vs)
			if vs != nil {
				utils.MustMatch(t, vs, vm.currentVschema, "currentVschema should have same reference as Vschema")
			}
		})
	}
}

func makeTestVSchema(ks string, sharded bool, tbls map[string]*vindexes.Table) *vindexes.VSchema {
	kSchema := &vindexes.KeyspaceSchema{
		Keyspace: &vindexes.Keyspace{
			Name:    ks,
			Sharded: sharded,
		},
		Tables:   tbls,
		Vindexes: map[string]vindexes.Vindex{},
	}
	vs := makeTestEmptyVSchema()
	vs.Keyspaces[ks] = kSchema
	return vs
}

func makeTestEmptyVSchema() *vindexes.VSchema {
	return &vindexes.VSchema{
		RoutingRules: map[string]*vindexes.RoutingRule{},
		Keyspaces:    map[string]*vindexes.KeyspaceSchema{},
	}
}

func makeTestSrvVSchema(ks string, sharded bool, tbls map[string]*vschemapb.Table) *vschemapb.SrvVSchema {
	kSchema := &vschemapb.Keyspace{
		Sharded: sharded,
		Tables:  tbls,
	}
	return &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{ks: kSchema},
	}
}

type fakeSchema struct {
	t map[string][]vindexes.Column
}

var _ SchemaInfo = (*fakeSchema)(nil)

func (f *fakeSchema) Tables(string) map[string][]vindexes.Column {
	return f.t
}
