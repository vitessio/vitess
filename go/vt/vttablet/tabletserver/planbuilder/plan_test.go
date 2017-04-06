// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
)

// toJSON returns a JSON of the given Plan.
// Except for "TableName", it's a 1:1 copy of the fields of "Plan".
// (The JSON output is used in the tests to compare it against the data in the
// golden files e.g. data/test/tabletserver/exec_cases.txt.)
func toJSON(p *Plan) ([]byte, error) {
	mplan := struct {
		PlanID               PlanType
		Reason               ReasonType             `json:",omitempty"`
		TableName            sqlparser.TableIdent   `json:",omitempty"`
		FieldQuery           *sqlparser.ParsedQuery `json:",omitempty"`
		FullQuery            *sqlparser.ParsedQuery `json:",omitempty"`
		OuterQuery           *sqlparser.ParsedQuery `json:",omitempty"`
		Subquery             *sqlparser.ParsedQuery `json:",omitempty"`
		UpsertQuery          *sqlparser.ParsedQuery `json:",omitempty"`
		ColumnNumbers        []int                  `json:",omitempty"`
		PKValues             []interface{}          `json:",omitempty"`
		SecondaryPKValues    []interface{}          `json:",omitempty"`
		WhereClause          *sqlparser.ParsedQuery `json:",omitempty"`
		SubqueryPKColumns    []int                  `json:",omitempty"`
		MessageReloaderQuery *sqlparser.ParsedQuery `json:",omitempty"`
	}{
		PlanID:               p.PlanID,
		Reason:               p.Reason,
		TableName:            p.TableName(),
		FieldQuery:           p.FieldQuery,
		FullQuery:            p.FullQuery,
		OuterQuery:           p.OuterQuery,
		Subquery:             p.Subquery,
		UpsertQuery:          p.UpsertQuery,
		ColumnNumbers:        p.ColumnNumbers,
		PKValues:             p.PKValues,
		SecondaryPKValues:    p.SecondaryPKValues,
		WhereClause:          p.WhereClause,
		SubqueryPKColumns:    p.SubqueryPKColumns,
		MessageReloaderQuery: p.MessageReloaderQuery,
	}
	return json.Marshal(&mplan)
}

func TestPlan(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	for tcase := range iterateExecFile("exec_cases.txt") {
		plan, err := Build(tcase.input, testSchema)
		var out string
		if err != nil {
			out = err.Error()
		} else {
			bout, err := toJSON(plan)
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
	}
}

func TestCustom(t *testing.T) {
	testSchemas := testfiles.Glob("tabletserver/*_schema.json")
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
				plan, err := Build(tcase.input, schem)
				var out string
				if err != nil {
					out = err.Error()
				} else {
					bout, err := toJSON(plan)
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
		plan, err := BuildStreaming(tcase.input, testSchema)
		var out string
		if err != nil {
			out = err.Error()
		} else {
			bout, err := toJSON(plan)
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

func TestDDLPlan(t *testing.T) {
	for tcase := range iterateExecFile("ddl_cases.txt") {
		plan := DDLParse(tcase.input)
		expected := make(map[string]interface{})
		err := json.Unmarshal([]byte(tcase.output), &expected)
		if err != nil {
			t.Fatalf("Error marshalling %v", plan)
		}
		matchString(t, tcase.lineno, expected["Action"], plan.Action)
		matchString(t, tcase.lineno, expected["TableName"], sqlparser.String(plan.TableName))
		matchString(t, tcase.lineno, expected["NewName"], sqlparser.String(plan.NewName))
	}
}

func matchString(t *testing.T, line int, expected interface{}, actual string) {
	if expected != nil {
		if expected.(string) != actual {
			t.Errorf("Line %d: expected: %v, received %s", line, expected, actual)
		}
	}
}

func loadSchema(name string) map[string]*schema.Table {
	b, err := ioutil.ReadFile(locateFile(name))
	if err != nil {
		panic(err)
	}
	tables := make([]*schema.Table, 0, 8)
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
	file   string
	lineno int
	input  string
	output string
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
			testCaseIterator <- testCase{name, lineno, input, string(output)}
		}
	}()
	return testCaseIterator
}

func locateFile(name string) string {
	if path.IsAbs(name) {
		return name
	}
	return testfiles.Locate("tabletserver/" + name)
}
