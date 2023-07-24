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

	"vitess.io/vitess/go/test/vschemawrapper"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"
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
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/schema.json", true),
		TabletType_:   topodatapb.TabletType_PRIMARY,
		SysVarEnabled: true,
		TestBuilder:   TestBuilder,
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

func TestSystemTables57(t *testing.T) {
	// first we move everything to use 5.7 logic
	servenv.SetMySQLServerVersionForTest("5.7")
	defer servenv.SetMySQLServerVersionForTest("")
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{V: loadSchema(t, "vschemas/schema.json", true)}
	testOutputTempDir := makeTestOutput(t)
	testFile(t, "info_schema57_cases.json", testOutputTempDir, vschemaWrapper, false)
}

func TestSysVarSetDisabled(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/schema.json", true),
		SysVarEnabled: false,
	}

	testFile(t, "set_sysvar_disabled_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestViews(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:           loadSchema(t, "vschemas/schema.json", true),
		EnableViews: true,
	}

	testFile(t, "view_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestOne(t *testing.T) {
	reset := oprewriters.EnableDebugPrinting()
	defer reset()

	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneTPCC(t *testing.T) {
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/tpcc_schema.json", true),
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithMainAsDefault(t *testing.T) {
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithSecondUserAsDefault(t *testing.T) {
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "second_user",
			Sharded: true,
		},
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithUserAsDefault(t *testing.T) {
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "user",
			Sharded: true,
		},
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWithTPCHVSchema(t *testing.T) {
	vschema := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/tpch_schema.json", true),
		SysVarEnabled: true,
	}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestOneWith57Version(t *testing.T) {
	// first we move everything to use 5.7 logic
	servenv.SetMySQLServerVersionForTest("5.7")
	defer servenv.SetMySQLServerVersionForTest("")
	vschema := &vschemawrapper.VSchemaWrapper{V: loadSchema(t, "vschemas/schema.json", true)}

	testFile(t, "onecase.json", "", vschema, false)
}

func TestRubyOnRailsQueries(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/rails_schema.json", true),
		SysVarEnabled: true,
	}

	testFile(t, "rails_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestOLTP(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/oltp_schema.json", true),
		SysVarEnabled: true,
	}

	testFile(t, "oltp_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestTPCC(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/tpcc_schema.json", true),
		SysVarEnabled: true,
	}

	testFile(t, "tpcc_cases.json", makeTestOutput(t), vschemaWrapper, false)
}

func TestTPCH(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/tpch_schema.json", true),
		SysVarEnabled: true,
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
		Dest:        key.DestinationShard("-80")}

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
	}

	testFile(t, "bypass_keyrange_cases.json", makeTestOutput(t), vschema, false)
}

func TestWithDefaultKeyspaceFromFile(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		TabletType_: topodatapb.TabletType_PRIMARY,
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
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(t, "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "second_user",
			Sharded: true,
		},
		TabletType_: topodatapb.TabletType_PRIMARY,
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
	return vschema
}

type (
	planTest struct {
		Comment string          `json:"comment,omitempty"`
		Query   string          `json:"query,omitempty"`
		Plan    json.RawMessage `json:"plan,omitempty"`
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
	vschema := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(b, "vschemas/schema.json", true),
		SysVarEnabled: true,
		Version:       Gen4,
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
