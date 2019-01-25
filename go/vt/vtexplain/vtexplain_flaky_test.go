/*
Copyright 2017 Google Inc.

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
	"os/exec"
	"path"
	"strings"
	"testing"
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
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	vSchema, err := ioutil.ReadFile("testdata/test-vschema.json")
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	opts.ExecutionMode = mode
	err = Init(string(vSchema), string(schema), opts)
	if err != nil {
		t.Fatalf("vtexplain Init error: %v", err)
	}
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
	t.Logf("vtexplain test: %s mode: %s", testcase, mode)
	initTest(mode, opts, t)

	sqlFile := fmt.Sprintf("testdata/%s-queries.sql", testcase)
	sql, err := ioutil.ReadFile(sqlFile)
	if err != nil {
		t.Fatalf("vtexplain error: %v", err)
	}

	textOutFile := fmt.Sprintf("testdata/%s-output/%s-output.txt", mode, testcase)
	textOut, _ := ioutil.ReadFile(textOutFile)

	explains, err := Run(string(sql))
	if err != nil {
		t.Fatalf("vtexplain error: %v", err)
	}
	if explains == nil {
		t.Fatalf("vtexplain error running %s: no explain", string(sql))
	}

	explainText := ExplainsAsText(explains)
	if strings.TrimSpace(string(explainText)) != strings.TrimSpace(string(textOut)) {
		// Print the Text that was actually returned and also dump to a
		// temp file to be able to diff the results.
		t.Errorf("Text output did not match")

		if testOutputTempDir == "" {
			testOutputTempDir, err = ioutil.TempDir("", "vtexplain_output")
			if err != nil {
				t.Fatalf("error getting tempdir: %v", err)
			}
		}
		gotFile := fmt.Sprintf("%s/%s-output.txt", testOutputTempDir, testcase)
		ioutil.WriteFile(gotFile, []byte(explainText), 0644)

		command := exec.Command("diff", "-u", textOutFile, gotFile)
		out, _ := command.CombinedOutput()
		t.Logf("diff:\n%s\n", out)
		t.Logf("run the following command to update the expected output:")
		t.Logf("cp %s/* %s", testOutputTempDir, path.Dir(textOutFile))
	}
}

func TestUnsharded(t *testing.T) {
	testExplain("unsharded", defaultTestOpts(), t)
}

func TestSelectSharded(t *testing.T) {
	testExplain("selectsharded", defaultTestOpts(), t)
}

func TestInsertSharded(t *testing.T) {
	testExplain("insertsharded", defaultTestOpts(), t)
}

func TestUpdateSharded(t *testing.T) {
	testExplain("updatesharded", defaultTestOpts(), t)
}

func TestDeleteSharded(t *testing.T) {
	testExplain("deletesharded", defaultTestOpts(), t)
}

func TestOptions(t *testing.T) {
	opts := &Options{
		ReplicationMode: "STATEMENT",
		NumShards:       4,
		Normalize:       false,
	}

	testExplain("options", opts, t)
}
func TestTarget(t *testing.T) {
	opts := &Options{
		ReplicationMode: "ROW",
		NumShards:       4,
		Normalize:       false,
		Target:          "ks_sharded/40-80",
	}

	testExplain("target", opts, t)
}

func TestComments(t *testing.T) {
	testExplain("comments", defaultTestOpts(), t)
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
			Err: "vtexplain execute error in 'SELECT * FROM THIS IS NOT SQL': syntax error at position 22 near 'is'",
		},

		{
			SQL: "SELECT * FROM table_not_in_vschema",
			Err: "vtexplain execute error in 'SELECT * FROM table_not_in_vschema': table table_not_in_vschema not found",
		},

		{
			SQL: "SELECT * FROM table_not_in_schema",
			Err: "vtexplain execute error in 'SELECT * FROM table_not_in_schema': target: ks_unsharded.-.master, used tablet: explainCell-0 (ks_unsharded/-), table table_not_in_schema not found in schema",
		},
	}

	for _, test := range tests {
		_, err := Run(test.SQL)
		if err == nil || err.Error() != test.Err {
			t.Errorf("Run(%s): %v, want %s", test.SQL, err, test.Err)
		}
	}
}

func TestJSONOutput(t *testing.T) {
	sql := "select 1 from user where id = 1"
	explains, err := Run(sql)
	if err != nil {
		t.Fatalf("vtexplain error: %v", err)
	}
	if explains == nil {
		t.Fatalf("vtexplain error running %s: no explain", string(sql))
	}

	explainJSON := ExplainsAsJSON(explains)

	var data interface{}
	err = json.Unmarshal([]byte(explainJSON), &data)
	if err != nil {
		t.Errorf("error unmarshaling json: %v", err)
	}

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
	if err != nil {
		t.Errorf("error in json marshal: %v", err)
	}
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
	if string(actionsJSON) != wantJSON {
		t.Errorf("TabletActions mismatch: got:\n%v\nwant:\n%v\n", string(actionsJSON), wantJSON)
	}
}
