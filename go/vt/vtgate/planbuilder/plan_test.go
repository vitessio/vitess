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
	"github.com/youtube/vitess/go/vt/key"
)

// hashIndex satisfies Functional, Unique.
type hashIndex struct{}

func (_ *hashIndex) Cost() int                                            { return 1 }
func (_ *hashIndex) Verify(_ interface{}, _ key.KeyspaceId) (bool, error) { return false, nil }
func (_ *hashIndex) Map(_ []interface{}) ([]key.KeyspaceId, error)        { return nil, nil }
func (_ *hashIndex) Create(_ interface{}) error                           { return nil }
func (_ *hashIndex) Delete(_ interface{}, _ key.KeyspaceId) error         { return nil }

func newHashIndex(_ map[string]interface{}) (Vindex, error) { return &hashIndex{}, nil }

// lookupIndex satisfies Lookup, Unique.
type lookupIndex struct{}

func (_ *lookupIndex) Cost() int                                            { return 2 }
func (_ *lookupIndex) Verify(_ interface{}, _ key.KeyspaceId) (bool, error) { return false, nil }
func (_ *lookupIndex) Map(_ []interface{}) ([]key.KeyspaceId, error)        { return nil, nil }
func (_ *lookupIndex) Create(_ interface{}, _ key.KeyspaceId) error         { return nil }
func (_ *lookupIndex) Delete(_ interface{}, _ key.KeyspaceId) error         { return nil }

func newLookupIndex(_ map[string]interface{}) (Vindex, error) { return &lookupIndex{}, nil }

// multiIndex satisfies Lookup, NonUnique.
type multiIndex struct{}

func (_ *multiIndex) Cost() int                                            { return 3 }
func (_ *multiIndex) Verify(_ interface{}, _ key.KeyspaceId) (bool, error) { return false, nil }
func (_ *multiIndex) Map(_ []interface{}) ([][]key.KeyspaceId, error)      { return nil, nil }
func (_ *multiIndex) Create(_ interface{}, _ key.KeyspaceId) error         { return nil }
func (_ *multiIndex) Delete(_ interface{}, _ key.KeyspaceId) error         { return nil }

func newMultiIndex(_ map[string]interface{}) (Vindex, error) { return &multiIndex{}, nil }

func init() {
	Register("hash", newHashIndex)
	Register("lookup", newLookupIndex)
	Register("multi", newMultiIndex)
}

func TestPlanName(t *testing.T) {
	id, ok := PlanByName("SelectUnsharded")
	if !ok {
		t.Errorf("got false, want true")
	}
	if id != SelectUnsharded {
		t.Errorf("got %d, want SelectUnsharded", id)
	}
	id, ok = PlanByName("NonExistent")
	if ok {
		t.Errorf("got true, want false")
	}
	fakeName := NumPlans.String()
	if fakeName != "" {
		t.Errorf("got %s, want \"\"", fakeName)
	}
}

func TestPlan(t *testing.T) {
	schema, err := LoadSchemaJSON(locateFile("schema_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	testFile(t, "select_cases.txt", schema)
	//testFile(t, "dml_cases.txt", schema)
	//testFile(t, "insert_cases.txt", schema)
}

func testFile(t *testing.T, filename string, schema *Schema) {
	for tcase := range iterateExecFile(filename) {
		plan := BuildPlan(tcase.input, schema)
		if plan.ID == NoPlan {
			plan.Rewritten = ""
			plan.ColVindex = nil
			plan.Values = nil
		}
		bout, err := json.Marshal(plan)
		if err != nil {
			panic(fmt.Sprintf("Error marshalling %v: %v", plan, err))
		}
		out := string(bout)
		if out != tcase.output {
			t.Error(fmt.Sprintf("File: %s, Line:%v\n%s\n%s", filename, tcase.lineno, tcase.output, out))
		}
		//fmt.Printf("%s\n%s\n\n", tcase.input, out)
	}
}

func loadSchema(name string) *Schema {
	b, err := ioutil.ReadFile(locateFile(name))
	if err != nil {
		panic(err)
	}
	var schema Schema
	err = json.Unmarshal(b, &schema)
	if err != nil {
		panic(err)
	}
	return &schema
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
