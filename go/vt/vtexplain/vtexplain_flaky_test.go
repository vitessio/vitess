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

func initTest(mode string, opts *Options, t *testing.T) {
	schema, err := ioutil.ReadFile("testdata/test-schema.sql")
	require.NoError(t, err)

	vSchema, err := ioutil.ReadFile("testdata/test-vschema.json")
	require.NoError(t, err)

	opts.ExecutionMode = mode
	err = Init(string(vSchema), string(schema), opts)
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
		runTestCase(testcase, mode, opts, t)
	}
}

func runTestCase(testcase, mode string, opts *Options, t *testing.T) {
	t.Run(testcase, func(t *testing.T) {
		initTest(mode, opts, t)

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
	initTest(ModeMulti, defaultTestOpts(), t)

	tests := []struct {
		SQL string
		Err string
	}{
		{
			SQL: "INVALID SQL",
			Err: "vtexplain execute error in 'INVALID SQL': unrecognized statement: INVALID SQL",
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
			Err: "target: ks_unsharded.-.master, used tablet: explainCell-0 (ks_unsharded/-): unknown error: unable to resolve table name table_not_in_schema",
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
                "SQL": "select 1 from user where id = 1 limit 10001",
                "Time": 1
            }
        ],
        "TabletQueries": [
            {
                "BindVars": {
                    "#maxLimit": "10001",
                    "vtg1": "1"
                },
                "SQL": "select :vtg1 from user where id = :vtg1",
                "Time": 1
            }
        ]
    }
}`
	require.Equal(t, wantJSON, string(actionsJSON))
}
