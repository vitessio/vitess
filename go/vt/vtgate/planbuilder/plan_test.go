/*
Copyright 2019 The Vitess Authors.

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

package planbuilder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	oprewriters "vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func makeTestOutput(t *testing.T) string {
	testOutputTempDir := utils.MakeTestOutput(t, "testdata", "plan_test")

	return testOutputTempDir
}

func TestPlan(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "vschemas/schema.json", true),
		tabletType:    topodatapb.TabletType_PRIMARY,
		sysVarEnabled: true,
	}
	testOutputTempDir := makeTestOutput(t)

	// You will notice that some tests expect user.Id instead of user.id.
	// This is because we now pre-create vindex columns in the symbol
	// table, which come from vschema. In the test vschema,
	// the column is named as Id. This is to make sure that
	// column names are case-preserved, but treated as
	// case-insensitive even if they come from the vschema.
	testFile(t, "aggr_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "dml_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "from_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "filter_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "postprocess_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "select_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "symtab_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "unsupported_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "vindex_func_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "wireup_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "memory_sort_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "use_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "set_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "union_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "large_union_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "transaction_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "lock_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "large_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "ddl_cases_no_default_keyspace.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "flush_cases_no_default_keyspace.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "show_cases_no_default_keyspace.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "stream_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "info_schema80_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "reference_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "vexplain_cases.json", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "misc_cases.json", testOutputTempDir, vschemaWrapper, false)
}

// TestForeignKeyPlanning tests the planning of foreign keys in a managed mode by Vitess.
func TestForeignKeyPlanning(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
		// Set the keyspace with foreign keys enabled as the default.
		keyspace: &vindexes.Keyspace{
			Name:    "user_fk_allow",
			Sharded: true,
		},
	}
	testOutputTempDir := makeTestOutput(t)

	testFile(t, "foreignkey_cases.json", testOutputTempDir, vschemaWrapper, false)
}

func TestSystemTables57(t *testing.T) {
	// first we move everything to use 5.7 logic
	servenv.SetMySQLServerVersionForTest("5.7")
	defer servenv.SetMySQLServerVersionForTest("")
	vschemaWrapper := &vschemaWrapper{v: loadSchema(t, "vschemas/schema.json", true)}
	testOutputTempDir := makeTestOutput(t)
	testFile(t, "info_schema57_cases.json", testOutputTempDir, vschemaWrapper, false)
}

func TestSysVarSetDisabled(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "vschemas/schema.json", true),
		sysVarEnabled: false,
	}

	testFile(t, "set_sysvar_disabled_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestViews(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:           loadSchema(t, "vschemas/schema.json", true),
		enableViews: true,
	}

	testFile(t, "view_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestOne(t *testing.T) {
	reset := oprewriters.EnableDebugPrinting()
	defer reset()

	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneTPCC(t *testing.T) {
	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/tpcc_schema.json", true),
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithMainAsDefault(t *testing.T) {
	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithSecondUserAsDefault(t *testing.T) {
	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "second_user",
			Sharded: true,
		},
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithUserAsDefault(t *testing.T) {
	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "user",
			Sharded: true,
		},
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithTPCHVSchema(t *testing.T) {
	vschema := &vschemaWrapper{
		v:             loadSchema(t, "vschemas/tpch_schema.json", true),
		sysVarEnabled: true,
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWith57Version(t *testing.T) {
	// first we move everything to use 5.7 logic
	servenv.SetMySQLServerVersionForTest("5.7")
	defer servenv.SetMySQLServerVersionForTest("")
	vschema := &vschemaWrapper{v: loadSchema(t, "vschemas/schema.json", true)}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestRubyOnRailsQueries(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "vschemas/rails_schema.json", true),
		sysVarEnabled: true,
	}

	testFile(t, "rails_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestOLTP(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "vschemas/oltp_schema.json", true),
		sysVarEnabled: true,
	}

	testFile(t, "oltp_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestTPCC(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "vschemas/tpcc_schema.json", true),
		sysVarEnabled: true,
	}

	testFile(t, "tpcc_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestTPCH(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "vschemas/tpch_schema.json", true),
		sysVarEnabled: true,
	}

	testFile(t, "tpch_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func BenchmarkOLTP(b *testing.B) {
	benchmarkWorkload(b, "oltp")
}

func BenchmarkTPCC(b *testing.B) {
	benchmarkWorkload(b, "tpcc")
}

func BenchmarkTPCH(b *testing.B) {
	benchmarkWorkload(b, "tpch")
}

func benchmarkWorkload(b *testing.B, name string) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(b, "vschemas/"+name+"_schema.json", true),
		sysVarEnabled: true,
	}

	testCases := readJSONTests(name + "_cases.json")
	b.ResetTimer()
	for _, version := range plannerVersions {
		b.Run(version.String(), func(b *testing.B) {
			benchmarkPlanner(b, version, testCases, vschemaWrapper)
		})
	}
}

func TestBypassPlanningShardTargetFromFile(t *testing.T) {
	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
		dest:       key.DestinationShard("-80")}

	testFile(t, "bypass_shard_cases.json", makeTestOutput(t), vschema, false)
}

func TestBypassPlanningKeyrangeTargetFromFile(t *testing.T) {
	keyRange, _ := key.ParseShardingSpec("-")

	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
		dest:       key.DestinationExactKeyRange{KeyRange: keyRange[0]},
	}

	testFile(t, "bypass_keyrange_cases.json", makeTestOutput(t), vschema, false)
}

func TestWithDefaultKeyspaceFromFile(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
	}
	ts := memorytopo.NewServer("cell1")
	ts.CreateKeyspace(context.Background(), "main", &topodatapb.Keyspace{})
	ts.CreateKeyspace(context.Background(), "user", &topodatapb.Keyspace{})
	// Create a cache to use for lookups of the sidecar database identifier
	// in use by each keyspace.
	_, created := sidecardb.NewIdentifierCache(func(ctx context.Context, keyspace string) (string, error) {
		ki, err := ts.GetKeyspace(ctx, keyspace)
		if err != nil {
			return "", err
		}
		return ki.SidecarDbName, nil
	})
	require.True(t, created)

	testOutputTempDir := makeTestOutput(t)
	testFile(t, "alterVschema_cases.json", testOutputTempDir, vschema, false)
	testFile(t, "ddl_cases.json", testOutputTempDir, vschema, false)
	testFile(t, "migration_cases.json", testOutputTempDir, vschema, false)
	testFile(t, "flush_cases.json", testOutputTempDir, vschema, false)
	testFile(t, "show_cases.json", testOutputTempDir, vschema, false)
	testFile(t, "call_cases.json", testOutputTempDir, vschema, false)
}

func TestWithDefaultKeyspaceFromFileSharded(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "second_user",
			Sharded: true,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
	}

	testOutputTempDir := makeTestOutput(t)
	testFile(t, "select_cases_with_default.json", testOutputTempDir, vschema, false)
}

func TestWithUserDefaultKeyspaceFromFileSharded(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "user",
			Sharded: true,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
	}

	testOutputTempDir := makeTestOutput(t)
	testFile(t, "select_cases_with_user_as_default.json", testOutputTempDir, vschema, false)
}

func TestWithSystemSchemaAsDefaultKeyspace(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemaWrapper{
		v:          loadSchema(t, "vschemas/schema.json", true),
		keyspace:   &vindexes.Keyspace{Name: "information_schema"},
		tabletType: topodatapb.TabletType_PRIMARY,
	}

	testFile(t, "sysschema_default.json", makeTestOutput(t), vschema, false)
}

func TestOtherPlanningFromFile(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
	}

	testOutputTempDir := makeTestOutput(t)
	testFile(t, "other_read_cases.json", testOutputTempDir, vschema, false)
	testFile(t, "other_admin_cases.json", testOutputTempDir, vschema, false)
}

func loadSchema(t testing.TB, filename string, setCollation bool) *vindexes.VSchema {
	formal, err := vindexes.LoadFormal(locateFile(filename))
	if err != nil {
		t.Fatal(err)
	}
	vschema := vindexes.BuildVSchema(formal)
	if err != nil {
		t.Fatal(err)
	}
	for _, ks := range vschema.Keyspaces {
		if ks.Error != nil {
			t.Fatal(ks.Error)
		}

		// adding view in user keyspace
		if ks.Keyspace.Name == "user" {
			if err = vschema.AddView(ks.Keyspace.Name,
				"user_details_view",
				"select user.id, user_extra.col from user join user_extra on user.id = user_extra.user_id"); err != nil {
				t.Fatal(err)
			}
		}

		// setting a default value to all the text columns in the tables of this keyspace
		// so that we can "simulate" a real case scenario where the vschema is aware of
		// columns' collations.
		if setCollation {
			for _, table := range ks.Tables {
				for i, col := range table.Columns {
					if sqltypes.IsText(col.Type) {
						table.Columns[i].CollationName = "latin1_swedish_ci"
					}
				}
			}
		}
	}
	if vschema.Keyspaces["user_fk_allow"] != nil {
		// FK from multicol_tbl2 referencing multicol_tbl1 that is shard scoped.
		err = vschema.AddForeignKey("user_fk_allow", "multicol_tbl2", createFkDefinition([]string{"colb", "cola", "x", "colc", "y"}, "multicol_tbl1", []string{"colb", "cola", "y", "colc", "x"}))
		require.NoError(t, err)
		// FK from tbl2 referencing tbl1 that is shard scoped.
		err = vschema.AddForeignKey("user_fk_allow", "tbl2", createFkDefinition([]string{"col2"}, "tbl1", []string{"col1"}))
		require.NoError(t, err)
		// FK from tbl3 referencing tbl1 that is not shard scoped.
		err = vschema.AddForeignKey("user_fk_allow", "tbl3", createFkDefinition([]string{"coly"}, "tbl1", []string{"col1"}))
		require.NoError(t, err)
	}
	return vschema
}

// createFkDefinition is a helper function to create a Foreign key definition struct from the columns used in it provided as list of strings.
func createFkDefinition(childCols []string, parentTableName string, parentCols []string) *sqlparser.ForeignKeyDefinition {
	return &sqlparser.ForeignKeyDefinition{
		Source: sqlparser.MakeColumns(childCols...),
		ReferenceDefinition: &sqlparser.ReferenceDefinition{
			ReferencedTable:   sqlparser.NewTableName(parentTableName),
			ReferencedColumns: sqlparser.MakeColumns(parentCols...),
		},
	}
}

var _ plancontext.VSchema = (*vschemaWrapper)(nil)

type vschemaWrapper struct {
	v             *vindexes.VSchema
	keyspace      *vindexes.Keyspace
	tabletType    topodatapb.TabletType
	dest          key.Destination
	sysVarEnabled bool
	version       plancontext.PlannerVersion
	enableViews   bool
}

func (vw *vschemaWrapper) GetPrepareData(stmtName string) *vtgatepb.PrepareData {
	switch stmtName {
	case "prep_one_param":
		return &vtgatepb.PrepareData{
			PrepareStatement: "select 1 from user where id = :v1",
			ParamsCount:      1,
		}
	case "prep_in_param":
		return &vtgatepb.PrepareData{
			PrepareStatement: "select 1 from user where id in (:v1, :v2)",
			ParamsCount:      2,
		}
	case "prep_no_param":
		return &vtgatepb.PrepareData{
			PrepareStatement: "select 1 from user",
			ParamsCount:      0,
		}
	}
	return nil
}

func (vw *vschemaWrapper) PlanPrepareStatement(ctx context.Context, query string) (*engine.Plan, sqlparser.Statement, error) {
	plan, err := TestBuilder(query, vw, vw.currentDb())
	if err != nil {
		return nil, nil, err
	}
	stmt, _, err := sqlparser.Parse2(query)
	if err != nil {
		return nil, nil, err
	}
	return plan, stmt, nil
}

func (vw *vschemaWrapper) ClearPrepareData(lowered string) {
}

func (vw *vschemaWrapper) StorePrepareData(string, *vtgatepb.PrepareData) {}

func (vw *vschemaWrapper) GetUDV(name string) *querypb.BindVariable {
	if strings.EqualFold(name, "prep_stmt") {
		return sqltypes.StringBindVariable("select * from user where id in (?, ?, ?)")
	}
	return nil
}

func (vw *vschemaWrapper) IsShardRoutingEnabled() bool {
	return false
}

func (vw *vschemaWrapper) GetVSchema() *vindexes.VSchema {
	return vw.v
}

func (vw *vschemaWrapper) GetSrvVschema() *vschemapb.SrvVSchema {
	return &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"user": {
				Sharded:  true,
				Vindexes: map[string]*vschemapb.Vindex{},
				Tables: map[string]*vschemapb.Table{
					"user": {},
				},
			},
		},
	}
}

func (vw *vschemaWrapper) ConnCollation() collations.ID {
	return collations.Default()
}

func (vw *vschemaWrapper) PlannerWarning(_ string) {
}

func (vw *vschemaWrapper) ForeignKeyMode(keyspace string) (vschemapb.Keyspace_ForeignKeyMode, error) {
	defaultFkMode := vschemapb.Keyspace_FK_UNMANAGED
	if vw.v.Keyspaces[keyspace] != nil && vw.v.Keyspaces[keyspace].ForeignKeyMode != vschemapb.Keyspace_FK_DEFAULT {
		return vw.v.Keyspaces[keyspace].ForeignKeyMode, nil
	}
	return defaultFkMode, nil
}

func (vw *vschemaWrapper) AllKeyspace() ([]*vindexes.Keyspace, error) {
	if vw.keyspace == nil {
		return nil, vterrors.VT13001("keyspace not available")
	}
	return []*vindexes.Keyspace{vw.keyspace}, nil
}

// FindKeyspace implements the VSchema interface
func (vw *vschemaWrapper) FindKeyspace(keyspace string) (*vindexes.Keyspace, error) {
	if vw.keyspace == nil {
		return nil, vterrors.VT13001("keyspace not available")
	}
	if vw.keyspace.Name == keyspace {
		return vw.keyspace, nil
	}
	return nil, nil
}

func (vw *vschemaWrapper) Planner() plancontext.PlannerVersion {
	return vw.version
}

// SetPlannerVersion implements the ContextVSchema interface
func (vw *vschemaWrapper) SetPlannerVersion(v plancontext.PlannerVersion) {
	vw.version = v
}

func (vw *vschemaWrapper) GetSemTable() *semantics.SemTable {
	return nil
}

func (vw *vschemaWrapper) KeyspaceExists(keyspace string) bool {
	if vw.keyspace != nil {
		return vw.keyspace.Name == keyspace
	}
	return false
}

func (vw *vschemaWrapper) SysVarSetEnabled() bool {
	return vw.sysVarEnabled
}

func (vw *vschemaWrapper) TargetDestination(qualifier string) (key.Destination, *vindexes.Keyspace, topodatapb.TabletType, error) {
	var keyspaceName string
	if vw.keyspace != nil {
		keyspaceName = vw.keyspace.Name
	}
	if vw.dest == nil && qualifier != "" {
		keyspaceName = qualifier
	}
	if keyspaceName == "" {
		return nil, nil, 0, vterrors.VT03007()
	}
	keyspace := vw.v.Keyspaces[keyspaceName]
	if keyspace == nil {
		return nil, nil, 0, vterrors.VT05003(keyspaceName)
	}
	return vw.dest, keyspace.Keyspace, vw.tabletType, nil

}

func (vw *vschemaWrapper) TabletType() topodatapb.TabletType {
	return vw.tabletType
}

func (vw *vschemaWrapper) Destination() key.Destination {
	return vw.dest
}

func (vw *vschemaWrapper) FindTable(tab sqlparser.TableName) (*vindexes.Table, string, topodatapb.TabletType, key.Destination, error) {
	destKeyspace, destTabletType, destTarget, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_PRIMARY)
	if err != nil {
		return nil, destKeyspace, destTabletType, destTarget, err
	}
	table, err := vw.v.FindTable(destKeyspace, tab.Name.String())
	if err != nil {
		return nil, destKeyspace, destTabletType, destTarget, err
	}
	return table, destKeyspace, destTabletType, destTarget, nil
}

func (vw *vschemaWrapper) FindView(tab sqlparser.TableName) sqlparser.SelectStatement {
	destKeyspace, _, _, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_PRIMARY)
	if err != nil {
		return nil
	}
	return vw.v.FindView(destKeyspace, tab.Name.String())
}

func (vw *vschemaWrapper) FindTableOrVindex(tab sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	if tab.Qualifier.IsEmpty() && tab.Name.String() == "dual" {
		ksName := vw.getActualKeyspace()
		var ks *vindexes.Keyspace
		if ksName == "" {
			ks = vw.getfirstKeyspace()
			ksName = ks.Name
		} else {
			ks = vw.v.Keyspaces[ksName].Keyspace
		}
		tbl := &vindexes.Table{
			Name:     sqlparser.NewIdentifierCS("dual"),
			Keyspace: ks,
			Type:     vindexes.TypeReference,
		}
		return tbl, nil, ksName, topodatapb.TabletType_PRIMARY, nil, nil
	}
	destKeyspace, destTabletType, destTarget, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_PRIMARY)
	if err != nil {
		return nil, nil, destKeyspace, destTabletType, destTarget, err
	}
	if destKeyspace == "" {
		destKeyspace = vw.getActualKeyspace()
	}
	table, vindex, err := vw.v.FindTableOrVindex(destKeyspace, tab.Name.String(), topodatapb.TabletType_PRIMARY)
	if err != nil {
		return nil, nil, destKeyspace, destTabletType, destTarget, err
	}
	return table, vindex, destKeyspace, destTabletType, destTarget, nil
}

func (vw *vschemaWrapper) getfirstKeyspace() (ks *vindexes.Keyspace) {
	var f string
	for name, schema := range vw.v.Keyspaces {
		if f == "" || f > name {
			f = name
			ks = schema.Keyspace
		}
	}
	return
}

func (vw *vschemaWrapper) getActualKeyspace() string {
	if vw.keyspace == nil {
		return ""
	}
	if !sqlparser.SystemSchema(vw.keyspace.Name) {
		return vw.keyspace.Name
	}
	ks, err := vw.AnyKeyspace()
	if err != nil {
		return ""
	}
	return ks.Name
}

func (vw *vschemaWrapper) DefaultKeyspace() (*vindexes.Keyspace, error) {
	return vw.v.Keyspaces["main"].Keyspace, nil
}

func (vw *vschemaWrapper) AnyKeyspace() (*vindexes.Keyspace, error) {
	return vw.DefaultKeyspace()
}

func (vw *vschemaWrapper) FirstSortedKeyspace() (*vindexes.Keyspace, error) {
	return vw.v.Keyspaces["main"].Keyspace, nil
}

func (vw *vschemaWrapper) TargetString() string {
	return "targetString"
}

func (vw *vschemaWrapper) WarnUnshardedOnly(_ string, _ ...any) {

}

func (vw *vschemaWrapper) ErrorIfShardedF(keyspace *vindexes.Keyspace, _, errFmt string, params ...any) error {
	if keyspace.Sharded {
		return fmt.Errorf(errFmt, params...)
	}
	return nil
}

func (vw *vschemaWrapper) currentDb() string {
	ksName := ""
	if vw.keyspace != nil {
		ksName = vw.keyspace.Name
	}
	return ksName
}

func (vw *vschemaWrapper) FindRoutedShard(keyspace, shard string) (string, error) {
	return "", nil
}

func (vw *vschemaWrapper) IsViewsEnabled() bool {
	return vw.enableViews
}

type (
	planTest struct {
		Comment string          `json:"comment,omitempty"`
		Query   string          `json:"query,omitempty"`
		Plan    json.RawMessage `json:"plan,omitempty"`
	}
)

func testFile(t *testing.T, filename, tempDir string, vschema *vschemaWrapper, render bool) {
	opts := jsondiff.DefaultConsoleOptions()

	t.Run(filename, func(t *testing.T) {
		var expected []planTest
		for _, tcase := range readJSONTests(filename) {
			testName := tcase.Comment
			if testName == "" {
				testName = tcase.Query
			}
			if tcase.Query == "" {
				continue
			}
			current := planTest{
				Comment: testName,
				Query:   tcase.Query,
			}
			vschema.version = Gen4
			out := getPlanOutput(tcase, vschema, render)

			// our expectation for the planner on the query is one of three
			// - produces same plan as expected
			// - produces a different plan than expected
			// - fails to produce a plan
			t.Run(testName, func(t *testing.T) {
				compare, s := jsondiff.Compare(tcase.Plan, []byte(out), &opts)
				if compare != jsondiff.FullMatch {
					t.Errorf("%s\nDiff:\n%s\n[%s] \n[%s]", filename, s, tcase.Plan, out)
				}
				current.Plan = []byte(out)
			})
			expected = append(expected, current)
		}
		if tempDir != "" {
			name := strings.TrimSuffix(filename, filepath.Ext(filename))
			name = filepath.Join(tempDir, name+".json")
			file, err := os.Create(name)
			require.NoError(t, err)
			enc := json.NewEncoder(file)
			enc.SetEscapeHTML(false)
			enc.SetIndent("", "  ")
			err = enc.Encode(expected)
			if err != nil {
				require.NoError(t, err)
			}
		}
	})
}

func readJSONTests(filename string) []planTest {
	var output []planTest
	file, err := os.Open(locateFile(filename))
	if err != nil {
		panic(err)
	}
	dec := json.NewDecoder(file)
	err = dec.Decode(&output)
	if err != nil {
		panic(err)
	}
	return output
}

func getPlanOutput(tcase planTest, vschema *vschemaWrapper, render bool) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("panicked: %v\n%s", r, string(debug.Stack()))
		}
	}()
	plan, err := TestBuilder(tcase.Query, vschema, vschema.currentDb())
	if render && plan != nil {
		viz, err := engine.GraphViz(plan.Instructions)
		if err == nil {
			_ = viz.Render()
		}
	}
	return getPlanOrErrorOutput(err, plan)
}

func getPlanOrErrorOutput(err error, plan *engine.Plan) string {
	if err != nil {
		return "\"" + err.Error() + "\""
	}
	b := new(bytes.Buffer)
	enc := json.NewEncoder(b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	err = enc.Encode(plan)
	if err != nil {
		panic(err)
	}
	return b.String()
}

func locateFile(name string) string {
	return "testdata/" + name
}

var benchMarkFiles = []string{"from_cases.json", "filter_cases.json", "large_cases.json", "aggr_cases.json", "select_cases.json", "union_cases.json"}

func BenchmarkPlanner(b *testing.B) {
	vschema := &vschemaWrapper{
		v:             loadSchema(b, "vschemas/schema.json", true),
		sysVarEnabled: true,
	}
	for _, filename := range benchMarkFiles {
		testCases := readJSONTests(filename)
		b.Run(filename+"-gen4", func(b *testing.B) {
			benchmarkPlanner(b, Gen4, testCases, vschema)
		})
		b.Run(filename+"-gen4left2right", func(b *testing.B) {
			benchmarkPlanner(b, Gen4Left2Right, testCases, vschema)
		})
	}
}

func BenchmarkSemAnalysis(b *testing.B) {
	vschema := &vschemaWrapper{
		v:             loadSchema(b, "vschemas/schema.json", true),
		sysVarEnabled: true,
	}

	for i := 0; i < b.N; i++ {
		for _, filename := range benchMarkFiles {
			for _, tc := range readJSONTests(filename) {
				exerciseAnalyzer(tc.Query, vschema.currentDb(), vschema)
			}
		}
	}
}

func exerciseAnalyzer(query, database string, s semantics.SchemaInformation) {
	defer func() {
		// if analysis panics, let's just continue. this is just a benchmark
		recover()
	}()

	ast, err := sqlparser.Parse(query)
	if err != nil {
		return
	}
	sel, ok := ast.(sqlparser.SelectStatement)
	if !ok {
		return
	}

	_, _ = semantics.Analyze(sel, database, s)
}

func BenchmarkSelectVsDML(b *testing.B) {
	vschema := &vschemaWrapper{
		v:             loadSchema(b, "vschemas/schema.json", true),
		sysVarEnabled: true,
		version:       Gen4,
	}

	dmlCases := readJSONTests("dml_cases.json")
	selectCases := readJSONTests("select_cases.json")

	rand.Shuffle(len(dmlCases), func(i, j int) {
		dmlCases[i], dmlCases[j] = dmlCases[j], dmlCases[i]
	})

	rand.Shuffle(len(selectCases), func(i, j int) {
		selectCases[i], selectCases[j] = selectCases[j], selectCases[i]
	})

	b.Run("DML (random sample, N=32)", func(b *testing.B) {
		benchmarkPlanner(b, Gen4, dmlCases[:32], vschema)
	})

	b.Run("Select (random sample, N=32)", func(b *testing.B) {
		benchmarkPlanner(b, Gen4, selectCases[:32], vschema)
	})
}

func benchmarkPlanner(b *testing.B, version plancontext.PlannerVersion, testCases []planTest, vschema *vschemaWrapper) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		for _, tcase := range testCases {
			if len(tcase.Plan) > 0 {
				vschema.version = version
				_, _ = TestBuilder(tcase.Query, vschema, vschema.currentDb())
			}
		}
	}
}
