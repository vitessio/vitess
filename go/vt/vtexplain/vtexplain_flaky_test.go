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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

var testOutputTempDir string

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

func initTest(mode string, opts *Options, topts *testopts, t *testing.T) {
	schema, err := ioutil.ReadFile("testdata/test-schema.sql")
	require.NoError(t, err)

	vSchema, err := ioutil.ReadFile("testdata/test-vschema.json")
	require.NoError(t, err)

	shardmap := ""
	if topts.shardmap != nil {
		shardmapBytes, err := json.Marshal(topts.shardmap)
		require.NoError(t, err)

		shardmap = string(shardmapBytes)
	}

	opts.ExecutionMode = mode
	err = Init(string(vSchema), string(schema), shardmap, opts)
	require.NoError(t, err, "vtexplain Init error\n%s", string(schema))
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
		initTest(mode, opts, topts, t)

		sqlFile := fmt.Sprintf("testdata/%s-queries.sql", testcase)
		sql, err := ioutil.ReadFile(sqlFile)
		require.NoError(t, err, "vtexplain error")

		textOutFile := fmt.Sprintf("testdata/%s-output/%s-output.txt", mode, testcase)
		expected, _ := ioutil.ReadFile(textOutFile)

		explains, err := Run(string(sql))
		require.NoError(t, err, "vtexplain error")
		require.NotNil(t, explains, "vtexplain error running %s: no explain", string(sql))

		explainText := ExplainsAsText(explains)
		if diff := cmp.Diff(strings.TrimSpace(string(expected)), strings.TrimSpace(explainText)); diff != "" {
			// Print the Text that was actually returned and also dump to a
			// temp file to be able to diff the results.
			t.Errorf("Text output did not match (-want +got):\n%s", diff)

			if testOutputTempDir == "" {
				testOutputTempDir, err = ioutil.TempDir("", "vtexplain_output")
				require.NoError(t, err, "error getting tempdir")
			}
			gotFile := fmt.Sprintf("%s/%s-output.txt", testOutputTempDir, testcase)
			ioutil.WriteFile(gotFile, []byte(explainText), 0644)

			t.Logf("run the following command to update the expected output:")
			t.Logf("cp %s/* %s", testOutputTempDir, path.Dir(textOutFile))
		}
	})
}

func TestExplain(t *testing.T) {
	type test struct {
		name string
		opts *Options
	}
	tests := []test{
		{"unsharded", defaultTestOpts()},
		{"selectsharded", defaultTestOpts()},
		{"insertsharded", defaultTestOpts()},
		{"updatesharded", defaultTestOpts()},
		{"deletesharded", defaultTestOpts()},
		{"comments", defaultTestOpts()},
		{"options", &Options{
			ReplicationMode: "STATEMENT",
			NumShards:       4,
			Normalize:       false,
		}},
		{"target", &Options{
			ReplicationMode: "ROW",
			NumShards:       4,
			Normalize:       false,
			Target:          "ks_sharded/40-80",
		}},
	}

	for _, tst := range tests {
		testExplain(tst.name, tst.opts, t)
	}
}

func TestErrors(t *testing.T) {
	initTest(ModeMulti, defaultTestOpts(), &testopts{}, t)

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
			_, err := Run(test.SQL)
			require.Error(t, err)
			require.Contains(t, err.Error(), test.Err)
		})
	}
}

func TestJSONOutput(t *testing.T) {
	sql := "select 1 from user where id = 1"
	explains, err := Run(sql)
	require.NoError(t, err, "vtexplain error")
	require.NotNil(t, explains, "vtexplain error running %s: no explain", string(sql))

	explainJSON := ExplainsAsJSON(explains)

	var data interface{}
	err = json.Unmarshal([]byte(explainJSON), &data)
	require.NoError(t, err, "error unmarshaling json")

	array, ok := data.([]interface{})
	if !ok || len(array) != 1 {
		t.Errorf("expected single-element top-level array, got:\n%s", explainJSON)
	}

	explain, ok := array[0].(map[string]interface{})
	if !ok {
		t.Errorf("expected explain map, got:\n%s", explainJSON)
	}

	if explain["SQL"] != sql {
		t.Errorf("expected SQL, got:\n%s", explainJSON)
	}

	plans, ok := explain["Plans"].([]interface{})
	if !ok || len(plans) != 1 {
		t.Errorf("expected single-element plans array, got:\n%s", explainJSON)
	}

	actions, ok := explain["TabletActions"].(map[string]interface{})
	if !ok {
		t.Errorf("expected TabletActions map, got:\n%s", explainJSON)
	}

	actionsJSON, err := json.MarshalIndent(actions, "", "    ")
	require.NoError(t, err, "error in json marshal")
	wantJSON := `{
    "ks_sharded/-40": {
        "MysqlQueries": [
            {
                "SQL": "select 1 from ` + "`user`" + ` where id = 1 limit 10001",
                "Time": 1
            }
        ],
        "TabletQueries": [
            {
                "BindVars": {
                    "#maxLimit": "10001",
                    "vtg1": "1"
                },
                "SQL": "select :vtg1 from ` + "`user`" + ` where id = :vtg1",
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

func testShardInfo(ks, start, end string, t *testing.T) *topo.ShardInfo {
	kr, err := key.ParseKeyRangeParts(start, end)
	require.NoError(t, err)

	return topo.NewShardInfo(
		ks,
		fmt.Sprintf("%s-%s", start, end),
		&topodata.Shard{KeyRange: kr},
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
					"-20":   testShardInfo("ks_sharded", "", "20", t),
					"20-40": testShardInfo("ks_sharded", "20", "40", t),
					"40-60": testShardInfo("ks_sharded", "40", "60", t),
					"60-80": testShardInfo("ks_sharded", "60", "80", t),
					"80-a0": testShardInfo("ks_sharded", "80", "a0", t),
					"a0-c0": testShardInfo("ks_sharded", "a0", "c0", t),
					"c0-e0": testShardInfo("ks_sharded", "c0", "e0", t),
					"e0-":   testShardInfo("ks_sharded", "e0", "", t),
				},
			},
		},
		{
			testcase: "uneven-keyspace",
			ShardRangeMap: map[string]map[string]*topo.ShardInfo{
				// Have mercy on the poor soul that has this keyspace sharding.
				// But, hey, vtexplain still works so they have that going for them.
				"ks_sharded": {
					"-80":   testShardInfo("ks_sharded", "", "80", t),
					"80-90": testShardInfo("ks_sharded", "80", "90", t),
					"90-a0": testShardInfo("ks_sharded", "90", "a0", t),
					"a0-e8": testShardInfo("ks_sharded", "a0", "e8", t),
					"e8-":   testShardInfo("ks_sharded", "e8", "", t),
				},
			},
		},
	}

	for _, test := range tests {
		runTestCase(test.testcase, ModeMulti, defaultTestOpts(), &testopts{test.ShardRangeMap}, t)
	}
}

type vtexplainTestTopoVersion struct{}

func (vtexplain *vtexplainTestTopoVersion) String() string { return "vtexplain-test-topo" }
