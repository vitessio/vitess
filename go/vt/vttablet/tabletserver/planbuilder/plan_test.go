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
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

// MarshalJSON returns a JSON of the given Plan.
// This is only for testing.
func (p *Plan) MarshalJSON() ([]byte, error) {
	mplan := struct {
		PlanID            PlanType
		TableName         sqlparser.IdentifierCS
		Permissions       []Permission           `json:",omitempty"`
		FieldQuery        *sqlparser.ParsedQuery `json:",omitempty"`
		FullQuery         *sqlparser.ParsedQuery `json:",omitempty"`
		NextCount         string                 `json:",omitempty"`
		WhereClause       *sqlparser.ParsedQuery `json:",omitempty"`
		NeedsReservedConn bool                   `json:",omitempty"`
	}{
		PlanID:      p.PlanID,
		TableName:   p.TableName(),
		Permissions: p.Permissions,
		FullQuery:   p.FullQuery,
		WhereClause: p.WhereClause,
	}
	if p.NextCount != nil {
		mplan.NextCount = sqlparser.String(p.NextCount)
	}
	if p.NeedsReservedConn {
		mplan.NeedsReservedConn = true
	}
	return json.Marshal(&mplan)
}

func TestPlan(t *testing.T) {
	testPlan(t, "exec_cases.txt")
}

func TestDDLPlan(t *testing.T) {
	testPlan(t, "ddl_cases.txt")
}

func testPlan(t *testing.T, fileName string) {
	t.Helper()
	parser := sqlparser.NewTestParser()
	testSchema := loadSchema("schema_test.json")
	for tcase := range iterateExecFile(fileName) {
		t.Run(tcase.input, func(t *testing.T) {
			if strings.Contains(tcase.options, "PassthroughDMLs") {
				PassthroughDMLs = true
			}
			var plan *Plan
			var err error
			statement, err := parser.Parse(tcase.input)
			if err == nil {
				plan, err = Build(vtenv.NewTestEnv(), statement, testSchema, "dbName", false)
			}
			PassthroughDMLs = false

			var out string
			if err != nil {
				out = err.Error()
			} else {
				bout, err := json.Marshal(plan)
				require.NoError(t, err, "Error marshalling %v: %v", plan, err)
				out = string(bout)
			}
			if out != tcase.output {
				if err != nil {
					out = fmt.Sprintf("\"%s\"", out)
				} else {
					bout, _ := json.MarshalIndent(plan, "", "  ")
					out = string(bout)
				}
				fmt.Printf("\"in> %s\"\nout>%s\nexpected: %s\n\n", tcase.input, out, tcase.output)
				assert.Failf(t, "plan mismatch", "Line:%v\ngot  = %s\nwant = %s", tcase.lineno, out, tcase.output)
			}
		})
	}
}

func TestPlanInReservedConn(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	parser := sqlparser.NewTestParser()
	for tcase := range iterateExecFile("exec_cases.txt") {
		t.Run(tcase.input, func(t *testing.T) {
			if strings.Contains(tcase.options, "PassthroughDMLs") {
				PassthroughDMLs = true
			}
			var plan *Plan
			var err error
			statement, err := parser.Parse(tcase.input)
			if err == nil {
				plan, err = Build(vtenv.NewTestEnv(), statement, testSchema, "dbName", false)
			}
			PassthroughDMLs = false

			var out string
			if err != nil {
				out = err.Error()
			} else {
				bout, mErr := json.Marshal(plan)
				require.NoErrorf(t, mErr, "Error marshalling %v", plan)
				out = string(bout)
			}
			if out != tcase.output {
				if err != nil {
					out = fmt.Sprintf("\"%s\"", out)
				} else {
					bout, _ := json.MarshalIndent(plan, "", "  ")
					out = string(bout)
				}
				fmt.Printf("\"%s\"\n%s\n\n", tcase.input, out)
				assert.Failf(t, "plan mismatch", "Line:%v\ngot  = %s\nwant = %s", tcase.lineno, out, tcase.output)
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
	parser := sqlparser.NewTestParser()
	for _, schemFile := range testSchemas {
		schem := loadSchema(schemFile)
		t.Logf("Testing schema %s", schemFile)
		files, err := filepath.Glob(strings.ReplaceAll(schemFile, "schema.json", "*.txt"))
		if err != nil {
			log.Fatal(err)
		}
		require.NotEmptyf(t, files, "No test files for %s", schemFile)
		for _, file := range files {
			t.Logf("Testing file %s", file)
			for tcase := range iterateExecFile(file) {
				statement, err := parser.Parse(tcase.input)
				require.NoErrorf(t, err, "Got error parsing sql: %v", tcase.input)
				plan, err := Build(vtenv.NewTestEnv(), statement, schem, "dbName", false)
				var out string
				if err != nil {
					out = err.Error()
				} else {
					bout, mErr := json.Marshal(plan)
					require.NoErrorf(t, mErr, "Error marshalling %v", plan)
					out = string(bout)
				}
				assert.Equalf(t, tcase.output, out, "File: %s: Line:%v", file, tcase.lineno)
			}
		}
	}
}

func TestStreamPlan(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	parser := sqlparser.NewTestParser()
	for tcase := range iterateExecFile("stream_cases.txt") {
		var plan *Plan
		var err error
		statement, err := parser.Parse(tcase.input)
		if err == nil {
			plan, err = BuildStreaming(vtenv.NewTestEnv(), statement, testSchema, "dbName")
		}
		var out string
		if err != nil {
			out = err.Error()
		} else {
			bout, mErr := json.Marshal(plan)
			require.NoErrorf(t, mErr, "Error marshalling %v", plan)
			out = string(bout)
		}
		assert.Equalf(t, tcase.output, out, "Line:%v", tcase.lineno)
	}
}

// Build must set Plan.StatementType from the parsed AST. The CTE cases are the
// regression: a textual scan calls anything starting with WITH unknown.
func TestBuildStatementType(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	parser := sqlparser.NewTestParser()

	tcases := []struct {
		input string
		want  sqlparser.StatementType
	}{
		{"select * from a", sqlparser.StmtSelect},
		{"select * from a union select * from b", sqlparser.StmtSelect},
		{"insert into a(eid, id) values (1, 2)", sqlparser.StmtInsert},
		{"replace into a(eid, id) values (1, 2)", sqlparser.StmtReplace},
		{"update a set name='foo' where id=1", sqlparser.StmtUpdate},
		{"delete from a where id=1", sqlparser.StmtDelete},
		{"with cte as (select id from a) select * from cte", sqlparser.StmtSelect},
		{"with cte as (select id from a) update a set name='foo' where id in (select id from cte)", sqlparser.StmtUpdate},
		{"with cte as (select id from a) delete from a where id in (select id from cte)", sqlparser.StmtDelete},
	}

	for _, tcase := range tcases {
		t.Run(tcase.input, func(t *testing.T) {
			statement, err := parser.Parse(tcase.input)
			require.NoError(t, err)

			plan, err := Build(vtenv.NewTestEnv(), statement, testSchema, "dbName", false)
			require.NoError(t, err)
			require.Equal(t, tcase.want, plan.StatementType)
		})
	}
}

// Why the statement type comes from the AST: sqlparser.Preview calls a CTE SELECT
// UNKNOWN, which fails open in the query throttler. Build and BuildStreaming must both
// say SELECT.
func TestBuildStatementType_CTERegression(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	parser := sqlparser.NewTestParser()

	const cteSelect = "with cte as (select id from a) select * from cte"

	// Premise guard: the textual classifier gets this wrong.
	require.Equal(t, sqlparser.StmtUnknown, sqlparser.Preview(cteSelect))

	statement, err := parser.Parse(cteSelect)
	require.NoError(t, err)

	plan, err := Build(vtenv.NewTestEnv(), statement, testSchema, "dbName", false)
	require.NoError(t, err)
	require.Equal(t, sqlparser.StmtSelect, plan.StatementType)

	streamPlan, err := BuildStreaming(vtenv.NewTestEnv(), statement, testSchema, "dbName")
	require.NoError(t, err)
	require.Equal(t, sqlparser.StmtSelect, streamPlan.StatementType)
}

// TestBuildStatementType_ReplaceRegression pins REPLACE to its own statement type.
// Reporting it as INSERT makes a query throttler REPLACE rule unmatchable, and wrongly
// applies an INSERT rule instead.
func TestBuildStatementType_ReplaceRegression(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	parser := sqlparser.NewTestParser()

	const replaceStmt = "replace into a(eid, id) values (1, 2)"

	statement, err := parser.Parse(replaceStmt)
	require.NoError(t, err)

	// Premise: REPLACE is represented as an *Insert with ReplaceAct.
	ins, ok := statement.(*sqlparser.Insert)
	require.True(t, ok, "REPLACE should parse to *sqlparser.Insert")
	require.Equal(t, sqlparser.ReplaceAct, ins.Action)

	plan, err := Build(vtenv.NewTestEnv(), statement, testSchema, "dbName", false)
	require.NoError(t, err)
	require.Equal(t, sqlparser.StmtReplace, plan.StatementType)

	streamPlan, err := BuildStreaming(vtenv.NewTestEnv(), statement, testSchema, "dbName")
	require.NoError(t, err)
	require.Equal(t, sqlparser.StmtReplace, streamPlan.StatementType)

	// An INSERT must stay an INSERT.
	insertStatement, err := parser.Parse("insert into a(eid, id) values (1, 2)")
	require.NoError(t, err)
	insertPlan, err := Build(vtenv.NewTestEnv(), insertStatement, testSchema, "dbName", false)
	require.NoError(t, err)
	require.Equal(t, sqlparser.StmtInsert, insertPlan.StatementType)
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

	assert.Equalf(t, wantJSON, planJSON, "BuildMessageStreaming")

	_, err = BuildMessageStreaming("absent", testSchema)
	require.EqualError(t, err, "table absent not found in schema", "BuildMessageStreaming(absent)")

	_, err = BuildMessageStreaming("a", testSchema)
	assert.EqualError(t, err, "'a' is not a message table", "BuildMessageStreaming(absent)")
}

func TestLockPlan(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	parser := sqlparser.NewTestParser()
	for tcase := range iterateExecFile("lock_cases.txt") {
		t.Run(tcase.input, func(t *testing.T) {
			var plan *Plan
			var err error
			statement, err := parser.Parse(tcase.input)
			if err == nil {
				plan, err = Build(vtenv.NewTestEnv(), statement, testSchema, "dbName", false)
			}

			var out string
			if err != nil {
				out = err.Error()
			} else {
				bout, mErr := json.Marshal(plan)
				require.NoErrorf(t, mErr, "Error marshalling %v", plan)
				out = string(bout)
			}
			if out != tcase.output {
				if err != nil {
					out = fmt.Sprintf("\"%s\"", out)
				} else {
					bout, _ := json.MarshalIndent(plan, "", "  ")
					out = string(bout)
				}
				fmt.Printf("\"in> %s\"\nout>%s\nexpected: %s\n\n", tcase.input, out, tcase.output)
				assert.Failf(t, "plan mismatch", "Line:%v\ngot  = %s\nwant = %s", tcase.lineno, out, tcase.output)
			}
		})
	}
}

func loadSchema(name string) map[string]*schema.Table {
	b, err := os.ReadFile(locateFile(name))
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
		panic("Could not open file " + name)
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
				// fmt.Printf("%s\n", input)
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
