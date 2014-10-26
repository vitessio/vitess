// Copyright 2014, Google Inc. All rights reserved.
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
	"os"
	"path"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

func TestPlan(t *testing.T) {
	gateSchema = loadSchema("schema_test.json")
	for tcase := range iterateExecFile("select_cases.txt") {
		var out string
		sel, err := sqlparser.Parse(tcase.input)
		if err != nil {
			out = err.Error()
		} else {
			plan := buildSelectPlan(sel.(*sqlparser.Select))
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

func loadSchema(name string) map[string]*VTGateTable {
	b, err := ioutil.ReadFile(locateFile(name))
	if err != nil {
		panic(err)
	}
	tables := make(map[string]*VTGateTable)
	err = json.Unmarshal(b, &tables)
	if err != nil {
		panic(err)
	}
	return tables
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
	return testfiles.Locate("vtgate/" + name)
}
