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

package vtexplain

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv/tabletenvtest"
)

func defaultTestOpts() *Options {
	return &Options{
		ReplicationMode: "ROW",
		NumShards:       4,
		Normalize:       true,
		StrictDDL:       true,
	}
}

type testopts struct {
	shardmap map[string]map[string]*topo.ShardInfo
}

func initTest(ctx context.Context, ts *topo.Server, mode string, opts *Options, topts *testopts, t *testing.T) *VTExplain {
	schema, err := os.ReadFile("testdata/test-schema.sql")
	require.NoError(t, err)

	vSchema, err := os.ReadFile("testdata/test-vschema.json")
	require.NoError(t, err)

	shardmap := ""
	if topts.shardmap != nil {
		shardmapBytes, err := json.Marshal(topts.shardmap)
		require.NoError(t, err)

		shardmap = string(shardmapBytes)
	}

	opts.ExecutionMode = mode
	srvTopoCounts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	vte, err := Init(ctx, vtenv.NewTestEnv(), ts, string(vSchema), string(schema), shardmap, opts, srvTopoCounts)
	require.NoError(t, err, "vtexplain Init error\n%s", string(schema))
	return vte
}

func testExplain(testcase string, opts *Options, t *testing.T) {
	modes := []string{
		ModeMulti,

		// TwoPC mode is functional, but the output isn't stable for
		// tests since there are timestamps in the value rows
		// ModeTwoPC,
	}

	for _, mode := range modes {
		runTestCase(testcase, mode, opts, &testopts{}, t)
	}
}

func runTestCase(testcase, mode string, opts *Options, topts *testopts, t *testing.T) {
	t.Run(testcase, func(t *testing.T) {
		ctx := utils.LeakCheckContext(t)

		ts := memorytopo.NewServer(ctx, Cell)
		vte := initTest(ctx, ts, mode, opts, topts, t)
		defer vte.Stop()

		sqlFile := fmt.Sprintf("testdata/%s-queries.sql", testcase)
		sql, err := os.ReadFile(sqlFile)
		require.NoError(t, err, "vtexplain error")

		textOutFile := fmt.Sprintf("testdata/%s-output/%s-output.txt", mode, testcase)
		expected, _ := os.ReadFile(textOutFile)

		explains, err := vte.Run(string(sql))
		require.NoError(t, err, "vtexplain error")
		require.NotNil(t, explains, "vtexplain error running %s: no explain", string(sql))

		// We want to remove the additional `set collation_connection` queries that happen
		// when the tablet connects to MySQL to set the default collation.
		// Removing them lets us keep simpler expected output files.
		for _, e := range explains {
			for i, action := range e.TabletActions {
				var mysqlQueries []*MysqlQuery
				for _, query := range action.MysqlQueries {
					if !strings.Contains(strings.ToLower(query.SQL), "set collation_connection") {
						mysqlQueries = append(mysqlQueries, query)
					}
				}
				e.TabletActions[i].MysqlQueries = mysqlQueries
			}
		}

		explainText, err := vte.ExplainsAsText(explains)
		require.NoError(t, err, "vtexplain error")

		if diff := cmp.Diff(strings.TrimSpace(string(expected)), strings.TrimSpace(explainText)); diff != "" {
			// Print the Text that was actually returned and also dump to a
			// temp file to be able to diff the results.
			t.Errorf("Text output did not match (-want +got):\n%s", diff)

			testOutputTempDir, err := os.MkdirTemp("testdata", "plan_test")
			require.NoError(t, err)
			gotFile := fmt.Sprintf("%s/%s-output.txt", testOutputTempDir, testcase)
			os.WriteFile(gotFile, []byte(explainText), 0644)

			t.Logf("run the following command to update the expected output:")
			t.Logf("cp %s/* %s", testOutputTempDir, path.Dir(textOutFile))
		}
	})
}

func TestExplain(t *testing.T) {
	tabletenvtest.LoadTabletEnvFlags()

	type test struct {
		name string
		opts *Options
	}
	tests := []test{
		{"unsharded", defaultTestOpts()},
	}

	for _, tst := range tests {
		testExplain(tst.name, tst.opts, t)
	}
}

func TestErrors(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	ts := memorytopo.NewServer(ctx, Cell)
	vte := initTest(ctx, ts, ModeMulti, defaultTestOpts(), &testopts{}, t)
	defer vte.Stop()

	tests := []struct {
		SQL string
		Err string
	}{
		{
			SQL: "INVALID SQL",
			Err: "vtexplain execute error in 'INVALID SQL': syntax error at position 8 near 'INVALID'",
		},

		{
			SQL: "SELECT * FROM THIS IS NOT SQL",
			Err: "vtexplain execute error in 'SELECT * FROM THIS IS NOT SQL': syntax error at position 22 near 'IS'",
		},

		{
			SQL: "SELECT * FROM table_not_in_vschema",
			Err: "vtexplain execute error in 'SELECT * FROM table_not_in_vschema': table table_not_in_vschema not found",
		},

		{
			SQL: "SELECT * FROM table_not_in_schema",
			Err: "unknown error: unable to resolve table name table_not_in_schema",
		},
	}

	for _, test := range tests {
		t.Run(test.SQL, func(t *testing.T) {
			_, err := vte.Run(test.SQL)
			require.Error(t, err)
			require.Contains(t, err.Error(), test.Err)
		})
	}
}

func TestJSONOutput(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	ts := memorytopo.NewServer(ctx, Cell)
	vte := initTest(ctx, ts, ModeMulti, defaultTestOpts(), &testopts{}, t)
	defer vte.Stop()
	sql := "select 1 from user where id = 1"
	explains, err := vte.Run(sql)
	require.NoError(t, err, "vtexplain error")
	require.NotNil(t, explains, "vtexplain error running %s: no explain", string(sql))

	for _, e := range explains {
		for i, action := range e.TabletActions {
			var mysqlQueries []*MysqlQuery
			for _, query := range action.MysqlQueries {
				if !strings.Contains(strings.ToLower(query.SQL), "set collation_connection") {
					mysqlQueries = append(mysqlQueries, query)
				}
			}
			e.TabletActions[i].MysqlQueries = mysqlQueries
		}
	}
	explainJSON := ExplainsAsJSON(explains)

	var data any
	err = json.Unmarshal([]byte(explainJSON), &data)
	require.NoError(t, err, "error unmarshaling json")

	array, ok := data.([]any)
	if !ok || len(array) != 1 {
		t.Errorf("expected single-element top-level array, got:\n%s", explainJSON)
	}

	explain, ok := array[0].(map[string]any)
	if !ok {
		t.Errorf("expected explain map, got:\n%s", explainJSON)
	}

	if explain["SQL"] != sql {
		t.Errorf("expected SQL, got:\n%s", explainJSON)
	}

	plans, ok := explain["Plans"].([]any)
	if !ok || len(plans) != 1 {
		t.Errorf("expected single-element plans array, got:\n%s", explainJSON)
	}

	actions, ok := explain["TabletActions"].(map[string]any)
	if !ok {
		t.Errorf("expected TabletActions map, got:\n%s", explainJSON)
	}

	actionsJSON, err := json.MarshalIndent(actions, "", "    ")
	require.NoError(t, err, "error in json marshal")
	wantJSON := `{
    "ks_sharded/-40": {
        "MysqlQueries": [
            {
                "SQL": "select 1 from ` + "`user`" + ` where id = 1 limit 10001 /* INT64 */",
                "Time": 1
            }
        ],
        "TabletQueries": [
            {
                "BindVars": {
                    "#maxLimit": "10001",
                    "vtg1": "1"
                },
                "SQL": "select :vtg1 /* INT64 */ from ` + "`user`" + ` where id = :vtg1 /* INT64 */",
                "Time": 1
            }
        ]
    }
}`
	diff := cmp.Diff(wantJSON, string(actionsJSON))
	if diff != "" {
		t.Errorf(diff)
	}
}

func testShardInfo(ks, start, end string, primaryServing bool, t *testing.T) *topo.ShardInfo {
	kr, err := key.ParseKeyRangeParts(start, end)
	require.NoError(t, err)

	return topo.NewShardInfo(
		ks,
		fmt.Sprintf("%s-%s", start, end),
		&topodata.Shard{KeyRange: kr, IsPrimaryServing: primaryServing},
		&vtexplainTestTopoVersion{},
	)
}

func TestUsingKeyspaceShardMap(t *testing.T) {
	tests := []struct {
		testcase      string
		ShardRangeMap map[string]map[string]*topo.ShardInfo
	}{
		{
			testcase: "select-sharded-8",
			ShardRangeMap: map[string]map[string]*topo.ShardInfo{
				"ks_sharded": {
					"-20":   testShardInfo("ks_sharded", "", "20", true, t),
					"20-40": testShardInfo("ks_sharded", "20", "40", true, t),
					"40-60": testShardInfo("ks_sharded", "40", "60", true, t),
					"60-80": testShardInfo("ks_sharded", "60", "80", true, t),
					"80-a0": testShardInfo("ks_sharded", "80", "a0", true, t),
					"a0-c0": testShardInfo("ks_sharded", "a0", "c0", true, t),
					"c0-e0": testShardInfo("ks_sharded", "c0", "e0", true, t),
					"e0-":   testShardInfo("ks_sharded", "e0", "", true, t),
					// Some non-serving shards below - these should never be in the output of vtexplain
					"-80": testShardInfo("ks_sharded", "", "80", false, t),
					"80-": testShardInfo("ks_sharded", "80", "", false, t),
				},
			},
		},
		{
			testcase: "uneven-keyspace",
			ShardRangeMap: map[string]map[string]*topo.ShardInfo{
				// Have mercy on the poor soul that has this keyspace sharding.
				// But, hey, vtexplain still works so they have that going for them.
				"ks_sharded": {
					"-80":   testShardInfo("ks_sharded", "", "80", true, t),
					"80-90": testShardInfo("ks_sharded", "80", "90", true, t),
					"90-a0": testShardInfo("ks_sharded", "90", "a0", true, t),
					"a0-e8": testShardInfo("ks_sharded", "a0", "e8", true, t),
					"e8-":   testShardInfo("ks_sharded", "e8", "", true, t),
					// Plus some un-even shards that are not serving and which should never be in the output of vtexplain
					"80-a0": testShardInfo("ks_sharded", "80", "a0", false, t),
					"a0-a5": testShardInfo("ks_sharded", "a0", "a5", false, t),
					"a5-":   testShardInfo("ks_sharded", "a5", "", false, t),
				},
			},
		},
	}

	for _, test := range tests {
		runTestCase(test.testcase, ModeMulti, defaultTestOpts(), &testopts{test.ShardRangeMap}, t)
	}
}

func TestInit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vschema := `{
  "ks1": {
    "sharded": true,
    "tables": {
      "table_missing_primary_vindex": {}
    }
  }
}`
	schema := "create table table_missing_primary_vindex (id int primary key)"
	ts := memorytopo.NewServer(ctx, Cell)
	srvTopoCounts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	_, err := Init(ctx, vtenv.NewTestEnv(), ts, vschema, schema, "", defaultTestOpts(), srvTopoCounts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing primary col vindex")
}

type vtexplainTestTopoVersion struct{}

func (vtexplain *vtexplainTestTopoVersion) String() string { return "vtexplain-test-topo" }
