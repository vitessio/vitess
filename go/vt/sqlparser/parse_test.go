/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package sqlparser

import (
	"bufio"
	"bytes"
	"code.google.com/p/vitess/go/vt/schema"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"
)

func TestGen(t *testing.T) {
	_, err := Parse("select :1 from a where a in (:1)")
	if err != nil {
		t.Error(err)
	}
}

var schem map[string]*schema.Table

func initTables() {
	schem = make(map[string]*schema.Table)

	a := schema.NewTable("a")
	a.Version = 0
	a.Columns = append(a.Columns, "eid", "id", "name", "foo")
	a.ColumnCategory = append(a.ColumnCategory, schema.CAT_NUMBER, schema.CAT_NUMBER, schema.CAT_OTHER, schema.CAT_OTHER)
	a.Indexes = append(a.Indexes, &schema.Index{"PRIMARY", []string{"eid", "id"}, []uint64{1, 1}, a.Columns})
	a.Indexes = append(a.Indexes, &schema.Index{"a_name", []string{"eid", "name"}, []uint64{1, 1}, a.Indexes[0].Columns})
	a.Indexes = append(a.Indexes, &schema.Index{"b_name", []string{"name"}, []uint64{3}, a.Indexes[0].Columns})
	a.Indexes = append(a.Indexes, &schema.Index{"c_name", []string{"name"}, []uint64{2}, a.Indexes[0].Columns})
	a.PKColumns = append(a.PKColumns, 0, 1)
	a.CacheType = 1
	schem["a"] = a

	b := schema.NewTable("b")
	b.Version = 0
	b.Columns = append(a.Columns, "eid", "id")
	b.ColumnCategory = append(a.ColumnCategory, schema.CAT_NUMBER, schema.CAT_NUMBER)
	b.Indexes = append(a.Indexes, &schema.Index{"PRIMARY", []string{"eid", "id"}, []uint64{1, 1}, b.Columns})
	b.PKColumns = append(a.PKColumns, 0, 1)
	b.CacheType = 0
	schem["b"] = b

	c := schema.NewTable("c")
	c.Version = 0
	c.Columns = append(a.Columns, "eid", "id")
	c.ColumnCategory = append(a.ColumnCategory, schema.CAT_NUMBER, schema.CAT_NUMBER)
	c.CacheType = 0
	schem["c"] = c
}

func tableGetter(name string) (*schema.Table, bool) {
	r, ok := schem[name]
	return r, ok
}

func TestExec(t *testing.T) {
	initTables()
	for tcase := range iterateJSONFile("test/exec_cases.txt") {
		plan, err := ExecParse(tcase.input, tableGetter)
		var out string
		if err != nil {
			out = err.Error()
		} else {
			bout, err := json.Marshal(plan)
			if err != nil {
				panic(fmt.Sprintf("Error marshalling %v: %v", plan, err))
			}
			out = string(bout)
		}
		if out != tcase.output {
			t.Error(fmt.Sprintf("Line:%v\n%s\n%s", tcase.lineno, tcase.output, out))
		}
		//fmt.Printf("%s\n%s\n\n", tcase.input, out)
	}
}

var actionToString = map[int]string{
	CREATE: "CREATE",
	ALTER:  "ALTER",
	DROP:   "DROP",
	RENAME: "RENAME",
	0:      "NONE",
}

func TestDDL(t *testing.T) {
	for tcase := range iterateFile("test/ddl_cases.txt") {
		plan := DDLParse(tcase.input)
		expected := make(map[string]interface{})
		err := json.Unmarshal([]byte(tcase.output), &expected)
		if err != nil {
			panic(fmt.Sprintf("Error marshalling %v", plan))
		}
		matchString(t, tcase.lineno, expected["Action"], actionToString[plan.Action])
		matchString(t, tcase.lineno, expected["TableName"], plan.TableName)
		matchString(t, tcase.lineno, expected["NewName"], plan.NewName)
	}
}

func matchString(t *testing.T, line int, expected interface{}, actual string) {
	if expected != nil {
		if expected.(string) != actual {
			t.Error(fmt.Sprintf("Line %d: expected: %v, received %s", line, expected, actual))
		}
	}
}

func TestParse(t *testing.T) {
	for tcase := range iterateFile("test/parse_pass.sql") {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Error(fmt.Sprintf("Line:%v\n%s\n%s", tcase.lineno, tcase.input, err))
		} else {
			out := tree.String()
			if out != tcase.output {
				t.Error(fmt.Sprintf("Line:%v\n%s\n%s", tcase.lineno, tcase.output, out))
			}
		}
	}
}

func TestRouting(t *testing.T) {
	tabletkeys := []string{
		"\x00\x00\x00\x00\x00\x00\x00\x00",
		"\x00\x00\x00\x00\x00\x00\x00\x02",
		"\x00\x00\x00\x00\x00\x00\x00\x04",
		"\x00\x00\x00\x00\x00\x00\x00\x06",
		"a",
		"b",
		"d",
	}
	bindVariables := make(map[string]interface{})
	bindVariables["id0"] = 0
	bindVariables["id2"] = 2
	bindVariables["id3"] = 3
	bindVariables["id4"] = 4
	bindVariables["id6"] = 6
	bindVariables["id8"] = 8
	bindVariables["ids"] = []interface{}{1, 4}
	bindVariables["a"] = "a"
	bindVariables["b"] = "b"
	bindVariables["c"] = "c"
	bindVariables["d"] = "d"
	bindVariables["e"] = "e"
	for tcase := range iterateFile("test/routing_cases.txt") {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		out, err := GetShardList(tcase.input, bindVariables, tabletkeys)
		if err != nil {
			if err.Error() != tcase.output {
				t.Error(fmt.Sprintf("Line:%v\n%s\n%s", tcase.lineno, tcase.input, err))
			}
			continue
		}
		sort.Ints(out)
		outstr := fmt.Sprintf("%v", out)
		if outstr != tcase.output {
			t.Error(fmt.Sprintf("Line:%v\n%s\n%s", tcase.lineno, tcase.output, outstr))
		}
	}
}

type testCase struct {
	lineno int
	input  string
	output string
}

func iterateFile(name string) (testCaseIterator chan testCase) {
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
			line, err := r.ReadString('\n')
			lines := strings.Split(strings.TrimRight(line, "\n"), "#")
			lineno++
			if err != nil {
				if err != io.EOF {
					panic(fmt.Sprintf("Error reading file %s: %s", name, err.Error()))
				}
				break
			}
			input := lines[0]
			output := ""
			if len(lines) > 1 {
				output = lines[1]
			}
			if input == "" {
				continue
			}
			testCaseIterator <- testCase{lineno, input, output}
		}
	}()
	return testCaseIterator
}

func iterateJSONFile(name string) (testCaseIterator chan testCase) {
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
			binput, _, err := r.ReadLine()
			input := string(binput)
			lineno++
			if err != nil {
				if err != io.EOF {
					panic(fmt.Sprintf("Error reading file %s: %s", name, err.Error()))
				}
				break
			}
			if input == "" || input[0] == '#' {
				//fmt.Printf("%s\n", input)
				continue
			}
			var output []byte
			for {
				l, err := r.ReadBytes('\n')
				lineno++
				if err != nil {
					panic(fmt.Sprintf("Error reading file %s: %s", name, err.Error()))
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
			testCaseIterator <- testCase{lineno, input, string(output)}
		}
	}()
	return testCaseIterator
}
