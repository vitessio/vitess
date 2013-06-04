// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	"code.google.com/p/vitess/go/sqltypes"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/schema"
)

func TestGen(t *testing.T) {
	_, err := Parse("select :1 from a where a in (:1)")
	if err != nil {
		t.Error(err)
	}
}

var (
	SQLZERO = sqltypes.MakeString([]byte("0"))
)

var schem map[string]*schema.Table

func initTables() {
	schem = make(map[string]*schema.Table)

	a := schema.NewTable("a")
	a.AddColumn("eid", "int", SQLZERO, "")
	a.AddColumn("id", "int", SQLZERO, "")
	a.AddColumn("name", "varchar(10)", SQLZERO, "")
	a.AddColumn("foo", "varchar(10)", SQLZERO, "")
	acolumns := []string{"eid", "id", "name", "foo"}
	a.Indexes = append(a.Indexes, &schema.Index{"PRIMARY", []string{"eid", "id"}, []uint64{1, 1}, acolumns})
	a.Indexes = append(a.Indexes, &schema.Index{"a_name", []string{"eid", "name"}, []uint64{1, 1}, a.Indexes[0].Columns})
	a.Indexes = append(a.Indexes, &schema.Index{"b_name", []string{"name"}, []uint64{3}, a.Indexes[0].Columns})
	a.Indexes = append(a.Indexes, &schema.Index{"c_name", []string{"name"}, []uint64{2}, a.Indexes[0].Columns})
	a.PKColumns = append(a.PKColumns, 0, 1)
	a.CacheType = schema.CACHE_RW
	schem["a"] = a

	b := schema.NewTable("b")
	b.AddColumn("eid", "int", SQLZERO, "")
	b.AddColumn("id", "int", SQLZERO, "")
	bcolumns := []string{"eid", "id"}
	b.Indexes = append(a.Indexes, &schema.Index{"PRIMARY", []string{"eid", "id"}, []uint64{1, 1}, bcolumns})
	b.PKColumns = append(a.PKColumns, 0, 1)
	b.CacheType = schema.CACHE_NONE
	schem["b"] = b

	c := schema.NewTable("c")
	c.AddColumn("eid", "int", SQLZERO, "")
	c.AddColumn("id", "int", SQLZERO, "")
	c.CacheType = schema.CACHE_NONE
	schem["c"] = c

	d := schema.NewTable("d")
	d.AddColumn("name", "varbinary(10)", SQLZERO, "")
	d.AddColumn("id", "int", SQLZERO, "")
	dcolumns := []string{"name"}
	d.Indexes = append(d.Indexes, &schema.Index{"PRIMARY", []string{"name"}, []uint64{1}, dcolumns})
	d.PKColumns = append(d.PKColumns, 0)
	d.CacheType = schema.CACHE_RW
	schem["d"] = d

	e := schema.NewTable("e")
	e.AddColumn("eid", "int", SQLZERO, "")
	e.AddColumn("id", "int", SQLZERO, "")
	ecolumns := []string{"eid", "id"}
	e.Indexes = append(a.Indexes, &schema.Index{"PRIMARY", []string{"eid", "id"}, []uint64{1, 1}, ecolumns})
	e.PKColumns = append(a.PKColumns, 0, 1)
	e.CacheType = schema.CACHE_W
	schem["e"] = e
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
	tabletkeys := []key.KeyspaceId{
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
