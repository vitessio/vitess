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
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func makeTestOutput(t *testing.T) string {
	testOutputTempDir := utils.MakeTestOutput(t, "testdata", "plan_test")

	return testOutputTempDir
}

func TestPlan(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/schema.json", true),
		TabletType_:   topodatapb.TabletType_PRIMARY,
		SysVarEnabled: true,
		TestBuilder:   TestBuilder,
		Env:           vtenv.NewTestEnv(),
	}
	testOutputTempDir := makeTestOutput(t)
	addPKs(t, vschemaWrapper.V, "user", []string{"user", "music"})
	addPKsProvided(t, vschemaWrapper.V, "user", []string{"user_extra"}, []string{"id", "user_id"})
	addPKsProvided(t, vschemaWrapper.V, "ordering", []string{"order"}, []string{"oid", "region_id"})
	addPKsProvided(t, vschemaWrapper.V, "ordering", []string{"order_event"}, []string{"oid", "ename"})

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
	testFile(t, "unknown_schema_cases.json", testOutputTempDir, vschemaWrapper, false)
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
	testFile(t, "cte_cases.json", testOutputTempDir, vschemaWrapper, false)
}

// TestForeignKeyPlanning tests the planning of foreign keys in a managed mode by Vitess.
func TestForeignKeyPlanning(t *testing.T) {
	vschema := loadSchema(t, "vschemas/schema.json", true)
	setFks(t, vschema)
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:           vschema,
		TestBuilder: TestBuilder,
		Env:         vtenv.NewTestEnv(),
	}

	testOutputTempDir := makeTestOutput(t)

	testFile(t, "foreignkey_cases.json", testOutputTempDir, vschemaWrapper, false)
}

// TestForeignKeyChecksOn tests the planning when the session variable for foreign_key_checks is set to ON.
func TestForeignKeyChecksOn(t *testing.T) {
	vschema := loadSchema(t, "vschemas/schema.json", true)
	setFks(t, vschema)
	fkChecksState := true
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:                     vschema,
		TestBuilder:           TestBuilder,
		ForeignKeyChecksState: &fkChecksState,
		Env:                   vtenv.NewTestEnv(),
	}

	testOutputTempDir := makeTestOutput(t)

	testFile(t, "foreignkey_checks_on_cases.json", testOutputTempDir, vschemaWrapper, false)
}

// TestForeignKeyChecksOff tests the planning when the session variable for foreign_key_checks is set to OFF.
func TestForeignKeyChecksOff(t *testing.T) {
	vschema := loadSchema(t, "vschemas/schema.json", true)
	setFks(t, vschema)
	fkChecksState := false
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:                     vschema,
		TestBuilder:           TestBuilder,
		ForeignKeyChecksState: &fkChecksState,
		Env:                   vtenv.NewTestEnv(),
	}

	testOutputTempDir := makeTestOutput(t)

	testFile(t, "foreignkey_checks_off_cases.json", testOutputTempDir, vschemaWrapper, false)
}

func setFks(t *testing.T, vschema *vindexes.VSchema) {
	if vschema.Keyspaces["sharded_fk_allow"] != nil {
		// FK from multicol_tbl2 referencing multicol_tbl1 that is shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "multicol_tbl2", createFkDefinition([]string{"colb", "cola", "x", "colc", "y"}, "multicol_tbl1", []string{"colb", "cola", "y", "colc", "x"}, sqlparser.Cascade, sqlparser.Cascade))

		// FK from tbl2 referencing tbl1 that is shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl2", createFkDefinition([]string{"col2"}, "tbl1", []string{"col1"}, sqlparser.Restrict, sqlparser.Restrict))
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl2", createFkDefinition([]string{"col2", "col"}, "tbl1", []string{"col1", "col"}, sqlparser.Restrict, sqlparser.Restrict))
		// FK from tbl3 referencing tbl1 that is not shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl3", createFkDefinition([]string{"coly"}, "tbl1", []string{"t1col1"}, sqlparser.DefaultAction, sqlparser.DefaultAction))
		// FK from tbl10 referencing tbl2 that is shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl10", createFkDefinition([]string{"sk", "col"}, "tbl2", []string{"col2", "col"}, sqlparser.Restrict, sqlparser.Restrict))
		// FK from tbl10 referencing tbl3 that is not shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl10", createFkDefinition([]string{"col"}, "tbl3", []string{"col"}, sqlparser.Restrict, sqlparser.Restrict))

		// FK from tbl4 referencing tbl5 that is shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl4", createFkDefinition([]string{"col4"}, "tbl5", []string{"col5"}, sqlparser.SetNull, sqlparser.Cascade))
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl4", createFkDefinition([]string{"t4col4"}, "tbl5", []string{"t5col5"}, sqlparser.SetNull, sqlparser.Cascade))

		// FK from tbl5 referencing tbl8 that is shard scoped of SET-NULL types.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl5", createFkDefinition([]string{"col5"}, "tbl8", []string{"col8"}, sqlparser.SetNull, sqlparser.SetNull))

		// FK from tbl4 referencing tbl9 that is not shard scoped of SET-NULL types.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl4", createFkDefinition([]string{"col_ref"}, "tbl9", []string{"col9"}, sqlparser.SetNull, sqlparser.SetNull))

		// FK from tbl6 referencing tbl7 that is shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl6", createFkDefinition([]string{"col6"}, "tbl7", []string{"col7"}, sqlparser.NoAction, sqlparser.NoAction))
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl6", createFkDefinition([]string{"t6col6"}, "tbl7", []string{"t7col7"}, sqlparser.NoAction, sqlparser.NoAction))
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl6", createFkDefinition([]string{"t6col62"}, "tbl7", []string{"t7col72"}, sqlparser.NoAction, sqlparser.NoAction))

		// FK from tblrefDef referencing tbl20 that is shard scoped of SET-Default types.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tblrefDef", createFkDefinition([]string{"ref"}, "tbl20", []string{"col2"}, sqlparser.SetDefault, sqlparser.SetDefault))

		// FK from tbl_auth referencing tbl20 that is shard scoped of CASCADE types.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl_auth", createFkDefinition([]string{"id"}, "tbl20", []string{"col2"}, sqlparser.Cascade, sqlparser.Cascade))
		addPKs(t, vschema, "sharded_fk_allow", []string{"tbl1", "tbl2", "tbl3", "tbl4", "tbl5", "tbl6", "tbl7", "tbl9", "tbl10",
			"multicol_tbl1", "multicol_tbl2", "tbl_auth", "tblrefDef", "tbl20"})
	}
	if vschema.Keyspaces["unsharded_fk_allow"] != nil {
		// u_tbl2(col2)  -> u_tbl1(col1)  Cascade.
		// u_tbl4(col41) -> u_tbl1(col14) Restrict.
		// u_tbl9(col9)  -> u_tbl1(col1)  Cascade Null.
		// u_tbl3(col2)  -> u_tbl2(col2)  Cascade Null.
		// u_tbl4(col4)  -> u_tbl3(col3)  Restrict.
		// u_tbl6(col6)  -> u_tbl5(col5)  Restrict.
		// u_tbl8(col8)  -> u_tbl9(col9)  Null Null.
		// u_tbl8(col8)  -> u_tbl6(col6)  Cascade Null.
		// u_tbl4(col4)  -> u_tbl7(col7)  Cascade Cascade.
		// u_tbl9(col9)  -> u_tbl4(col4)  Restrict Restrict.
		// u_multicol_tbl2(cola, colb)  -> u_multicol_tbl1(cola, colb)  Null Null.
		// u_multicol_tbl3(cola, colb)  -> u_multicol_tbl2(cola, colb)  Cascade Cascade.

		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl2", createFkDefinition([]string{"col2"}, "u_tbl1", []string{"col1"}, sqlparser.Cascade, sqlparser.Cascade))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl9", createFkDefinition([]string{"col9"}, "u_tbl1", []string{"col1"}, sqlparser.SetNull, sqlparser.NoAction))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl4", createFkDefinition([]string{"col41"}, "u_tbl1", []string{"col14"}, sqlparser.NoAction, sqlparser.NoAction))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl3", createFkDefinition([]string{"col3"}, "u_tbl2", []string{"col2"}, sqlparser.SetNull, sqlparser.SetNull))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl4", createFkDefinition([]string{"col4"}, "u_tbl3", []string{"col3"}, sqlparser.Restrict, sqlparser.Restrict))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl6", createFkDefinition([]string{"col6"}, "u_tbl5", []string{"col5"}, sqlparser.DefaultAction, sqlparser.DefaultAction))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl8", createFkDefinition([]string{"col8"}, "u_tbl9", []string{"col9"}, sqlparser.SetNull, sqlparser.SetNull))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl8", createFkDefinition([]string{"col8"}, "u_tbl6", []string{"col6"}, sqlparser.Cascade, sqlparser.Cascade))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl4", createFkDefinition([]string{"col4"}, "u_tbl7", []string{"col7"}, sqlparser.Cascade, sqlparser.Cascade))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl9", createFkDefinition([]string{"col9"}, "u_tbl4", []string{"col4"}, sqlparser.Restrict, sqlparser.Restrict))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl11", createFkDefinition([]string{"col"}, "u_tbl10", []string{"col"}, sqlparser.Cascade, sqlparser.Cascade))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl", createFkDefinition([]string{"col"}, "sharded_fk_allow.s_tbl", []string{"col"}, sqlparser.Restrict, sqlparser.Restrict))

		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_multicol_tbl2", createFkDefinition([]string{"cola", "colb"}, "u_multicol_tbl1", []string{"cola", "colb"}, sqlparser.SetNull, sqlparser.SetNull))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_multicol_tbl3", createFkDefinition([]string{"cola", "colb"}, "u_multicol_tbl2", []string{"cola", "colb"}, sqlparser.Cascade, sqlparser.Cascade))

		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl9", sqlparser.Exprs{sqlparser.NewColName("col9")})
		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl9", sqlparser.Exprs{&sqlparser.BinaryExpr{Operator: sqlparser.MultOp, Left: sqlparser.NewColName("col9"), Right: sqlparser.NewColName("foo")}})
		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl9", sqlparser.Exprs{sqlparser.NewColName("col9"), sqlparser.NewColName("foo")})
		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl9", sqlparser.Exprs{sqlparser.NewColName("foo"), sqlparser.NewColName("bar")})
		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl9", sqlparser.Exprs{sqlparser.NewColName("bar"), sqlparser.NewColName("col9")})
		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl8", sqlparser.Exprs{sqlparser.NewColName("col8")})

		addPKs(t, vschema, "unsharded_fk_allow", []string{"u_tbl1", "u_tbl2", "u_tbl3", "u_tbl4", "u_tbl5", "u_tbl6", "u_tbl7", "u_tbl8", "u_tbl9", "u_tbl10", "u_tbl11",
			"u_multicol_tbl1", "u_multicol_tbl2", "u_multicol_tbl3"})
	}

}

func addPKs(t *testing.T, vschema *vindexes.VSchema, ks string, tbls []string) {
	for _, tbl := range tbls {
		require.NoError(t,
			vschema.AddPrimaryKey(ks, tbl, []string{"id"}))
	}
}

func addPKsProvided(t *testing.T, vschema *vindexes.VSchema, ks string, tbls []string, pks []string) {
	for _, tbl := range tbls {
		require.NoError(t,
			vschema.AddPrimaryKey(ks, tbl, pks))
	}
}

func TestSystemTables57(t *testing.T) {
	// first we move everything to use 5.7 logic
	env, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: "5.7.9",
	})
	require.NoError(t, err)
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:   loadSchema(t, "vschemas/schema.json", true),
		Env: env,
	}
	testOutputTempDir := makeTestOutput(t)
	testFile(t, "info_schema57_cases.json", testOutputTempDir, vschemaWrapper, false)
}

func TestSysVarSetDisabled(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/schema.json", true),
		SysVarEnabled: false,
		Env:           vtenv.NewTestEnv(),
	}

	testFile(t, "set_sysvar_disabled_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestViews(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:           loadSchema(t, "vschemas/schema.json", true),
		EnableViews: true,
		Env:         vtenv.NewTestEnv(),
	}

	testFile(t, "view_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestOne(t *testing.T) {
	reset := operators.EnableDebugPrinting()
	defer reset()

	lv := loadSchema(t, "vschemas/schema.json", true)
	setFks(t, lv)
	addPKs(t, lv, "user", []string{"user", "music"})
	addPKs(t, lv, "main", []string{"unsharded"})
	addPKsProvided(t, lv, "user", []string{"user_extra"}, []string{"id", "user_id"})
	addPKsProvided(t, lv, "ordering", []string{"order"}, []string{"oid", "region_id"})
	addPKsProvided(t, lv, "ordering", []string{"order_event"}, []string{"oid", "ename"})
	vschema := &vschemawrapper.VSchemaWrapper{
		V:           lv,
		TestBuilder: TestBuilder,
		Env:         vtenv.NewTestEnv(),
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneTPCC(t *testing.T) {
	reset := operators.EnableDebugPrinting()
	defer reset()

	vschema := &vschemawrapper.VSchemaWrapper{
		V:   loadSchema(t, "vschemas/tpcc_schema.json", true),
		Env: vtenv.NewTestEnv(),
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithMainAsDefault(t *testing.T) {
	reset := operators.EnableDebugPrinting()
	defer reset()
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		Env: vtenv.NewTestEnv(),
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithSecondUserAsDefault(t *testing.T) {
	reset := operators.EnableDebugPrinting()
	defer reset()
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "second_user",
			Sharded: true,
		},
		Env: vtenv.NewTestEnv(),
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithUserAsDefault(t *testing.T) {
	reset := operators.EnableDebugPrinting()
	defer reset()
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "user",
			Sharded: true,
		},
		Env: vtenv.NewTestEnv(),
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithTPCHVSchema(t *testing.T) {
	reset := operators.EnableDebugPrinting()
	defer reset()
	vschema := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/tpch_schema.json", true),
		SysVarEnabled: true,
		Env:           vtenv.NewTestEnv(),
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWith57Version(t *testing.T) {
	reset := operators.EnableDebugPrinting()
	defer reset()
	// first we move everything to use 5.7 logic
	env, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: "5.7.9",
	})
	require.NoError(t, err)
	vschema := &vschemawrapper.VSchemaWrapper{
		V:   loadSchema(t, "vschemas/schema.json", true),
		Env: env,
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestRubyOnRailsQueries(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/rails_schema.json", true),
		SysVarEnabled: true,
		Env:           vtenv.NewTestEnv(),
	}

	testFile(t, "rails_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestOLTP(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/oltp_schema.json", true),
		SysVarEnabled: true,
		Env:           vtenv.NewTestEnv(),
	}

	testFile(t, "oltp_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestTPCC(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/tpcc_schema.json", true),
		SysVarEnabled: true,
		Env:           vtenv.NewTestEnv(),
	}

	testFile(t, "tpcc_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestTPCH(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/tpch_schema.json", true),
		SysVarEnabled: true,
		Env:           vtenv.NewTestEnv(),
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
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(b, "vschemas/"+name+"_schema.json", true),
		SysVarEnabled: true,
		Env:           vtenv.NewTestEnv(),
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
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		TabletType_: topodatapb.TabletType_PRIMARY,
		Dest:        key.DestinationShard("-80"),
		Env:         vtenv.NewTestEnv(),
	}

	testFile(t, "bypass_shard_cases.json", makeTestOutput(t), vschema, false)
}

func TestBypassPlanningKeyrangeTargetFromFile(t *testing.T) {
	keyRange, _ := key.ParseShardingSpec("-")

	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		TabletType_: topodatapb.TabletType_PRIMARY,
		Dest:        key.DestinationExactKeyRange{KeyRange: keyRange[0]},
		Env:         vtenv.NewTestEnv(),
	}

	testFile(t, "bypass_keyrange_cases.json", makeTestOutput(t), vschema, false)
}

func TestWithDefaultKeyspaceFromFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		TabletType_: topodatapb.TabletType_PRIMARY,
		Env:         vtenv.NewTestEnv(),
	}
	ts := memorytopo.NewServer(ctx, "cell1")
	ts.CreateKeyspace(ctx, "main", &topodatapb.Keyspace{})
	ts.CreateKeyspace(ctx, "user", &topodatapb.Keyspace{})
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
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "second_user",
			Sharded: true,
		},
		TabletType_: topodatapb.TabletType_PRIMARY,
		Env:         vtenv.NewTestEnv(),
	}

	testOutputTempDir := makeTestOutput(t)
	testFile(t, "select_cases_with_default.json", testOutputTempDir, vschema, false)
}

func TestWithUserDefaultKeyspaceFromFileSharded(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "user",
			Sharded: true,
		},
		TabletType_: topodatapb.TabletType_PRIMARY,
		Env:         vtenv.NewTestEnv(),
	}

	testOutputTempDir := makeTestOutput(t)
	testFile(t, "select_cases_with_user_as_default.json", testOutputTempDir, vschema, false)
}

func TestWithSystemSchemaAsDefaultKeyspace(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemawrapper.VSchemaWrapper{
		V:           loadSchema(t, "vschemas/schema.json", true),
		Keyspace:    &vindexes.Keyspace{Name: "information_schema"},
		TabletType_: topodatapb.TabletType_PRIMARY,
		Env:         vtenv.NewTestEnv(),
	}

	testFile(t, "sysschema_default.json", makeTestOutput(t), vschema, false)
}

func TestOtherPlanningFromFile(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		TabletType_: topodatapb.TabletType_PRIMARY,
		Env:         vtenv.NewTestEnv(),
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
	vschema := vindexes.BuildVSchema(formal, sqlparser.NewTestParser())
	if err != nil {
		t.Fatal(err)
	}
	for _, ks := range vschema.Keyspaces {
		if ks.Error != nil {
			t.Fatal(ks.Error)
		}

		// adding view in user keyspace
		if ks.Keyspace.Name == "user" {
			if err = vschema.AddView(ks.Keyspace.Name, "user_details_view", "select user.id, user_extra.col from user join user_extra on user.id = user_extra.user_id", sqlparser.NewTestParser()); err != nil {
				t.Fatal(err)
			}
		}

		// setting a default value to all the text columns in the tables of this keyspace
		// so that we can "simulate" a real case scenario where the vschema is aware of
		// columns' collations.
		if setCollation {
			for _, table := range ks.Tables {
				for i, col := range table.Columns {
					if sqltypes.IsText(col.Type) && col.CollationName == "" {
						table.Columns[i].CollationName = "latin1_swedish_ci"
					}
				}
			}
		}
	}
	return vschema
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

type (
	planTest struct {
		Comment string          `json:"comment,omitempty"`
		Query   string          `json:"query,omitempty"`
		Plan    json.RawMessage `json:"plan,omitempty"`
		Skip    bool            `json:"skip,omitempty"`
	}
)

func testFile(t *testing.T, filename, tempDir string, vschema *vschemawrapper.VSchemaWrapper, render bool) {
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
			vschema.Version = Gen4
			out := getPlanOutput(tcase, vschema, render)

			// our expectation for the planner on the query is one of three
			// - produces same plan as expected
			// - produces a different plan than expected
			// - fails to produce a plan
			t.Run(testName, func(t *testing.T) {
				compare, s := jsondiff.Compare(tcase.Plan, []byte(out), &opts)
				if compare != jsondiff.FullMatch {
					message := fmt.Sprintf("%s\nDiff:\n%s\n[%s] \n[%s]", filename, s, tcase.Plan, out)
					if tcase.Skip {
						t.Skip(message)
					} else {
						t.Errorf(message)
					}
				} else if tcase.Skip {
					t.Errorf("query is correct even though it is skipped:\n %s", tcase.Query)
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
	dec.DisallowUnknownFields()
	err = dec.Decode(&output)
	if err != nil {
		panic(err)
	}
	return output
}

func getPlanOutput(tcase planTest, vschema *vschemawrapper.VSchemaWrapper, render bool) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("panicked: %v\n%s", r, string(debug.Stack()))
		}
	}()
	plan, err := TestBuilder(tcase.Query, vschema, vschema.CurrentDb())
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
	vschema := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(b, "vschemas/schema.json", true),
		SysVarEnabled: true,
		Env:           vtenv.NewTestEnv(),
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
	vschema := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(b, "vschemas/schema.json", true),
		SysVarEnabled: true,
		Env:           vtenv.NewTestEnv(),
	}

	for i := 0; i < b.N; i++ {
		for _, filename := range benchMarkFiles {
			for _, tc := range readJSONTests(filename) {
				exerciseAnalyzer(tc.Query, vschema.CurrentDb(), vschema)
			}
		}
	}
}

func exerciseAnalyzer(query, database string, s semantics.SchemaInformation) {
	defer func() {
		// if analysis panics, let's just continue. this is just a benchmark
		recover()
	}()

	ast, err := sqlparser.NewTestParser().Parse(query)
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
	vschema := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(b, "vschemas/schema.json", true),
		SysVarEnabled: true,
		Version:       Gen4,
		Env:           vtenv.NewTestEnv(),
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

func benchmarkPlanner(b *testing.B, version plancontext.PlannerVersion, testCases []planTest, vschema *vschemawrapper.VSchemaWrapper) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		for _, tcase := range testCases {
			if len(tcase.Plan) > 0 {
				vschema.Version = version
				_, _ = TestBuilder(tcase.Query, vschema, vschema.CurrentDb())
			}
		}
	}
}
