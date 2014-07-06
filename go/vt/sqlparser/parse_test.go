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
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/schema"
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

func TestExec(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	for tcase := range iterateExecFile("exec_cases.txt") {
		plan, err := ExecParse(tcase.input, func(name string) (*schema.Table, bool) {
			r, ok := testSchema[name]
			return r, ok
		}, true)
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

func TestCustomExec(t *testing.T) {
	testSchemas := testfiles.Glob("sqlparser_test/*_schema.json")
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
		getter := func(name string) (*schema.Table, bool) {
			r, ok := schem[name]
			return r, ok
		}
		for _, file := range files {
			t.Logf("Testing file %s", file)
			for tcase := range iterateExecFile(file) {
				plan, err := ExecParse(tcase.input, getter, false)
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
					t.Errorf("File: %s: Line:%v\n%s\n%s", file, tcase.lineno, tcase.output, out)
				}
			}
		}
	}
}

func TestStreamExec(t *testing.T) {
	for tcase := range iterateExecFile("stream_cases.txt") {
		plan, err := StreamExecParse(tcase.input, true)
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

func TestDDL(t *testing.T) {
	for tcase := range iterateFiles("sqlparser_test/ddl_cases.txt") {
		plan := DDLParse(tcase.input)
		expected := make(map[string]interface{})
		err := json.Unmarshal([]byte(tcase.output), &expected)
		if err != nil {
			panic(fmt.Sprintf("Error marshalling %v", plan))
		}
		matchString(t, tcase.lineno, expected["Action"], plan.Action)
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
	for tcase := range iterateFiles("sqlparser_test/*.sql") {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		var out string
		if err != nil {
			out = err.Error()
		} else {
			out = String(tree)
		}
		if out != tcase.output {
			t.Error(fmt.Sprintf("File:%s Line:%v\n%q\n%q", tcase.file, tcase.lineno, tcase.output, out))
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
	for tcase := range iterateFiles("sqlparser_test/routing_cases.txt") {
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

func TestAnonymizer(t *testing.T) {
	testSchema := loadSchema("schema_test.json")
	sql := "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
	plan, err := ExecParse(sql, func(name string) (*schema.Table, bool) {
		r, ok := testSchema[name]
		return r, ok
	}, true)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	want := "select ?, ?, ?, eid from a where ? = eid and name = ?"
	if plan.DisplayQuery != want {
		t.Errorf("got %q, want %q", plan.DisplayQuery, want)
	}

	plan, err = ExecParse(sql, func(name string) (*schema.Table, bool) {
		r, ok := testSchema[name]
		return r, ok
	}, false)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	want = "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
	if plan.DisplayQuery != want {
		t.Errorf("got %q, want %q", plan.DisplayQuery, want)
	}

	splan, err := StreamExecParse(sql, true)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	want = "select ?, ?, ?, eid from a where ? = eid and name = ?"
	if splan.DisplayQuery != want {
		t.Errorf("got %q, want %q", splan.DisplayQuery, want)
	}

	splan, err = StreamExecParse(sql, false)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	want = "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
	if splan.DisplayQuery != want {
		t.Errorf("got %q, want %q", splan.DisplayQuery, want)
	}
}

func BenchmarkParse1(b *testing.B) {
	sql := "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
	for i := 0; i < b.N; i++ {
		_, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParse2(b *testing.B) {
	sql := "select aaaa, bbb, ccc, ddd, eeee, ffff, gggg, hhhh, iiii from tttt, ttt1, ttt3 where aaaa = bbbb and bbbb = cccc and dddd+1 = eeee group by fff, gggg having hhhh = iiii and iiii = jjjj order by kkkk, llll limit 3, 4"
	for i := 0; i < b.N; i++ {
		_, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
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
		s[t.Name] = t
	}
	return s
}

type testCase struct {
	file   string
	lineno int
	input  string
	output string
}

func iterateFiles(pattern string) (testCaseIterator chan testCase) {
	names := testfiles.Glob(pattern)
	testCaseIterator = make(chan testCase)
	go func() {
		defer close(testCaseIterator)
		for _, name := range names {
			fd, err := os.OpenFile(name, os.O_RDONLY, 0)
			if err != nil {
				panic(fmt.Sprintf("Could not open file %s", name))
			}

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
				testCaseIterator <- testCase{name, lineno, input, output}
			}
		}
	}()
	return testCaseIterator
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
	return testfiles.Locate("sqlparser_test/" + name)
}
