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
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
)

func defaultTestOpts() *Options {
	return &Options{
		ReplicationMode: "ROW",
		NumShards:       2,
		Normalize:       false,
	}
}

func initTest(opts *Options, t *testing.T) {
	schema, err := ioutil.ReadFile(testfiles.Locate("vtexplain/test-schema.sql"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	vSchema, err := ioutil.ReadFile(testfiles.Locate("vtexplain/test-vschema.json"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	err = Init(string(vSchema), string(schema), opts)
	if err != nil {
		t.Fatalf("vtexplain Init error: %v", err)
	}

}

func testExplain(testcase string, opts *Options, t *testing.T) {
	initTest(opts, t)

	sqlFile := testfiles.Locate(fmt.Sprintf("vtexplain/%s-queries.sql", testcase))
	sql, err := ioutil.ReadFile(sqlFile)

	jsonOutFile := testfiles.Locate(fmt.Sprintf("vtexplain/%s-output.json", testcase))
	jsonOut, err := ioutil.ReadFile(jsonOutFile)

	textOutFile := testfiles.Locate(fmt.Sprintf("vtexplain/%s-output.txt", testcase))
	textOut, err := ioutil.ReadFile(textOutFile)

	explains, err := Run(string(sql))
	if err != nil {
		t.Fatalf("vtexplain error: %v", err)
	}
	if explains == nil {
		t.Fatalf("vtexplain error running %s: no explain", string(sql))
	}

	explainJSON := ExplainsAsJSON(explains)
	if strings.TrimSpace(string(explainJSON)) != strings.TrimSpace(string(jsonOut)) {
		// Print the json that was actually returned and also dump to a
		// temp file to be able to diff the results.
		t.Errorf("json output did not match")
		t.Logf("got:\n%s\n", string(explainJSON))

		tempDir, err := ioutil.TempDir("", "vtexplain_output")
		if err != nil {
			t.Fatalf("error getting tempdir: %v", err)
		}
		gotFile := fmt.Sprintf("%s/%s-output.json", tempDir, testcase)
		ioutil.WriteFile(gotFile, []byte(explainJSON), 0644)

		command := exec.Command("diff", "-u", jsonOutFile, gotFile)
		out, _ := command.CombinedOutput()
		t.Logf("diff:\n%s\n", out)
	}

	explainText := ExplainsAsText(explains)
	if strings.TrimSpace(string(explainText)) != strings.TrimSpace(string(textOut)) {
		// Print the Text that was actually returned and also dump to a
		// temp file to be able to diff the results.
		t.Errorf("Text output did not match")
		t.Logf("got:\n%s\n", string(explainText))

		tempDir, err := ioutil.TempDir("", "vtexplain_output")
		if err != nil {
			t.Fatalf("error getting tempdir: %v", err)
		}
		gotFile := fmt.Sprintf("%s/%s-output.txt", tempDir, testcase)
		ioutil.WriteFile(gotFile, []byte(explainText), 0644)

		command := exec.Command("diff", "-u", textOutFile, gotFile)
		out, _ := command.CombinedOutput()
		t.Logf("diff:\n%s\n", out)
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

func TestOptions(t *testing.T) {
	opts := &Options{
		ReplicationMode: "STATEMENT",
		NumShards:       4,
		Normalize:       true,
	}

	testExplain("options", opts, t)
}

func TestComments(t *testing.T) {
	testExplain("comments", defaultTestOpts(), t)
}

func TestErrors(t *testing.T) {
	initTest(defaultTestOpts(), t)

	tests := []struct {
		SQL string
		Err string
	}{
		{
			SQL: "INVALID SQL",
			Err: "vtexplain execute error: unrecognized statement: INVALID SQL in INVALID SQL",
		},

		{
			SQL: "SELECT * FROM THIS IS NOT SQL",
			Err: "vtexplain execute error: syntax error at position 22 near 'is' in SELECT * FROM THIS IS NOT SQL",
		},

		{
			SQL: "SELECT * FROM table_not_in_vschema",
			Err: "vtexplain execute error: table table_not_in_vschema not found in SELECT * FROM table_not_in_vschema",
		},

		{
			SQL: "SELECT * FROM table_not_in_schema",
			Err: "vtexplain execute error: target: ks_unsharded.-.master, used tablet: explainCell-0 (ks_unsharded/-), table table_not_in_schema not found in schema in SELECT * FROM table_not_in_schema",
		},
	}

	for _, test := range tests {
		_, err := Run(test.SQL)
		if err == nil || err.Error() != test.Err {
			t.Errorf("Run(%s): %v, want %s", test.SQL, err, test.Err)
		}
	}
}
