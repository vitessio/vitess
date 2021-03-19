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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

// MarshalJSON returns a JSON of the given Plan.
// This is only for testing.
func (p *Plan) MarshalJSON() ([]byte, error) {
	mplan := struct {
		PlanID      PlanType
		TableName   sqlparser.TableIdent   `json:",omitempty"`
		Permissions []Permission           `json:",omitempty"`
		FieldQuery  *sqlparser.ParsedQuery `json:",omitempty"`
		FullQuery   *sqlparser.ParsedQuery `json:",omitempty"`
		NextCount   string                 `json:",omitempty"`
		WhereClause *sqlparser.ParsedQuery `json:",omitempty"`
	}{
		PlanID:      p.PlanID,
		TableName:   p.TableName(),
		Permissions: p.Permissions,
		FieldQuery:  p.FieldQuery,
		FullQuery:   p.FullQuery,
		WhereClause: p.WhereClause,
	}
	if !p.NextCount.IsNull() {
		b, _ := p.NextCount.MarshalJSON()
		mplan.NextCount = string(b)
	}
	return json.Marshal(&mplan)
}

func TestPlan(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	for tcase := range iterateExecFile("exec_cases.txt") {
		t.Run(tcase.input, func(t *testing.T) {
			if strings.Contains(tcase.options, "PassthroughDMLs") {
				PassthroughDMLs = true
			}
			var plan *Plan
			var err error
			statement, err := sqlparser.Parse(tcase.input)
			if err == nil {
				plan, err = Build(statement, testSchema, false, "dbName")
			}
			PassthroughDMLs = false

			var out string
			if err != nil {
				out = err.Error()
			} else {
				bout, err := json.Marshal(plan)
				if err != nil {
					t.Fatalf("Error marshalling %v: %v", plan, err)
				}
				out = string(bout)
			}
			if out != tcase.output {
				t.Errorf("Line:%v\ngot  = %s\nwant = %s", tcase.lineno, out, tcase.output)
				if err != nil {
					out = fmt.Sprintf("\"%s\"", out)
				} else {
					bout, _ := json.MarshalIndent(plan, "", "  ")
					out = string(bout)
				}
				fmt.Printf("\"in> %s\"\nout>%s\nexpected: %s\n\n", tcase.input, out, tcase.output)
			}
		})
	}
}

func TestPlanPoolUnsafe(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	for tcase := range iterateExecFile("pool_unsafe_cases.txt") {
		t.Run(tcase.input, func(t *testing.T) {
			var plan *Plan
			var err error
			statement, err := sqlparser.Parse(tcase.input)
			require.NoError(t, err)
			// In Pooled Connection, plan building will fail.
			plan, err = Build(statement, testSchema, false /* isReservedConn */, "dbName")
			require.Error(t, err)
			out := err.Error()
			if out != tcase.output {
				t.Errorf("Line:%v\ngot  = %s\nwant = %s", tcase.lineno, out, tcase.output)
				if err != nil {
					out = fmt.Sprintf("\"%s\"", out)
				} else {
					bout, _ := json.MarshalIndent(plan, "", "  ")
					out = string(bout)
				}
				fmt.Printf("\"%s\"\n%s\n\n", tcase.input, out)
			}
			// In Reserved Connection, plan will be built.
			plan, err = Build(statement, testSchema, true /* isReservedConn */, "dbName")
			require.NoError(t, err)
			require.NotEmpty(t, plan)
		})
	}
}

func TestPlanInReservedConn(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	for tcase := range iterateExecFile("exec_cases.txt") {
		t.Run(tcase.input, func(t *testing.T) {
			if strings.Contains(tcase.options, "PassthroughDMLs") {
				PassthroughDMLs = true
			}
			var plan *Plan
			var err error
			statement, err := sqlparser.Parse(tcase.input)
			if err == nil {
				plan, err = Build(statement, testSchema, true, "dbName")
			}
			PassthroughDMLs = false

			var out string
			if err != nil {
				out = err.Error()
			} else {
				bout, err := json.Marshal(plan)
				if err != nil {
					t.Fatalf("Error marshalling %v: %v", plan, err)
				}
				out = string(bout)
			}
			if out != tcase.output {
				t.Errorf("Line:%v\ngot  = %s\nwant = %s", tcase.lineno, out, tcase.output)
				if err != nil {
					out = fmt.Sprintf("\"%s\"", out)
				} else {
					bout, _ := json.MarshalIndent(plan, "", "  ")
					out = string(bout)
				}
				fmt.Printf("\"%s\"\n%s\n\n", tcase.input, out)
			}
		})
	}
}

func TestCustom(t *testing.T) {
	testSchemas, _ := filepath.Glob("testdata/*_schema.json")
	if len(testSchemas) == 0 {
		t.Log("No schemas to test")
		return
	}
	for _, schemFile := range testSchemas {
		schem := loadSchema(schemFile)
		t.Logf("Testing schema %s", schemFile)
		files, err := filepath.Glob(strings.Replace(schemFile, "schema.json", "*.txt", -1))
		if err != nil {
			log.Fatal(err)
		}
		if len(files) == 0 {
			t.Fatalf("No test files for %s", schemFile)
		}
		for _, file := range files {
			t.Logf("Testing file %s", file)
			for tcase := range iterateExecFile(file) {
				statement, err := sqlparser.Parse(tcase.input)
				if err != nil {
					t.Fatalf("Got error: %v, parsing sql: %v", err.Error(), tcase.input)
				}
				plan, err := Build(statement, schem, false, "dbName")
				var out string
				if err != nil {
					out = err.Error()
				} else {
					bout, err := json.Marshal(plan)
					if err != nil {
						t.Fatalf("Error marshalling %v: %v", plan, err)
					}
					out = string(bout)
				}
				if out != tcase.output {
					t.Errorf("File: %s: Line:%v\ngot  = %s\nwant = %s", file, tcase.lineno, out, tcase.output)
				}
			}
		}
	}
}

func TestStreamPlan(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	for tcase := range iterateExecFile("stream_cases.txt") {
		plan, err := BuildStreaming(tcase.input, testSchema, false)
		var out string
		if err != nil {
			out = err.Error()
		} else {
			bout, err := json.Marshal(plan)
			if err != nil {
				t.Fatalf("Error marshalling %v: %v", plan, err)
			}
			out = string(bout)
		}
		if out != tcase.output {
			t.Errorf("Line:%v\ngot  = %s\nwant = %s", tcase.lineno, out, tcase.output)
		}
		//fmt.Printf("%s\n%s\n\n", tcase.input, out)
	}
}

func TestMessageStreamingPlan(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	plan, err := BuildMessageStreaming("msg", testSchema)
	require.NoError(t, err)
	bout, _ := json.Marshal(plan)
	planJSON := string(bout)

	wantPlan := &Plan{
		PlanID: PlanMessageStream,
		Table:  testSchema["msg"],
		Permissions: []Permission{{
			TableName: "msg",
			Role:      tableacl.WRITER,
		}},
	}
	bout, _ = json.Marshal(wantPlan)
	wantJSON := string(bout)

	if planJSON != wantJSON {
		t.Errorf("BuildMessageStreaming: \n%s, want\n%s", planJSON, wantJSON)
	}

	_, err = BuildMessageStreaming("absent", testSchema)
	want := "table absent not found in schema"
	if err == nil || err.Error() != want {
		t.Errorf("BuildMessageStreaming(absent) error: %v, want %s", err, want)
	}

	_, err = BuildMessageStreaming("a", testSchema)
	want = "'a' is not a message table"
	if err == nil || err.Error() != want {
		t.Errorf("BuildMessageStreaming(absent) error: %v, want %s", err, want)
	}
}

func loadSchema(name string) map[string]*schema.Table {
	b, err := ioutil.ReadFile(locateFile(name))
	if err != nil {
		panic(err)
	}
	tables := make([]*schema.Table, 0, 10)
	err = json.Unmarshal(b, &tables)
	if err != nil {
		panic(err)
	}
	s := make(map[string]*schema.Table)
	for _, t := range tables {
		s[t.Name.String()] = t
	}
	return s
}

type testCase struct {
	file    string
	lineno  int
	options string
	input   string
	output  string
}

func iterateExecFile(name string) (testCaseIterator chan testCase) {
	name = locateFile(name)
	fd, err := os.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		panic(fmt.Sprintf("Could not open file %s", name))
	}
	testCaseIterator = make(chan testCase)
	go func() {
		defer close(testCaseIterator)

		r := bufio.NewReader(fd)
		lineno := 0
		options := ""
		for {
			binput, err := r.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Printf("Line: %d\n", lineno)
					panic(fmt.Errorf("Error reading file %s: %s", name, err.Error()))
				}
				break
			}
			lineno++
			input := string(binput)
			if input == "" || input == "\n" || input[0] == '#' || strings.HasPrefix(input, "Length:") {
				//fmt.Printf("%s\n", input)
				continue
			}

			if strings.HasPrefix(input, "options:") {
				options = input[8:]
				continue
			}
			err = json.Unmarshal(binput, &input)
			if err != nil {
				fmt.Printf("Line: %d, input: %s\n", lineno, binput)
				panic(err)
			}
			input = strings.Trim(input, "\"")
			var output []byte
			for {
				l, err := r.ReadBytes('\n')
				lineno++
				if err != nil {
					fmt.Printf("Line: %d\n", lineno)
					panic(fmt.Errorf("Error reading file %s: %s", name, err.Error()))
				}
				output = append(output, l...)
				if l[0] == '}' {
					output = output[:len(output)-1]
					b := bytes.NewBuffer(make([]byte, 0, 64))
					if err := json.Compact(b, output); err == nil {
						output = b.Bytes()
					}
					break
				}
				if l[0] == '"' {
					output = output[1 : len(output)-2]
					break
				}
			}
			testCaseIterator <- testCase{name, lineno, options, input, string(output)}
			options = ""
		}
	}()
	return testCaseIterator
}

func locateFile(name string) string {
	return "testdata/" + name
}
