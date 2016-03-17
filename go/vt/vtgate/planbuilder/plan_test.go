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
	"os"
	"path"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
)

// hashIndex satisfies Functional, Unique.
type hashIndex struct{ name string }

func (v *hashIndex) String() string { return v.name }
func (*hashIndex) Cost() int        { return 1 }
func (*hashIndex) Verify(VCursor, interface{}, []byte) (bool, error) {
	return false, nil
}
func (*hashIndex) Map(VCursor, []interface{}) ([][]byte, error) { return nil, nil }
func (*hashIndex) Create(VCursor, interface{}) error            { return nil }
func (*hashIndex) Delete(VCursor, []interface{}, []byte) error  { return nil }

func newHashIndex(name string, _ map[string]interface{}) (Vindex, error) {
	return &hashIndex{name: name}, nil
}

// lookupIndex satisfies Lookup, Unique.
type lookupIndex struct{ name string }

func (v *lookupIndex) String() string { return v.name }
func (*lookupIndex) Cost() int        { return 2 }
func (*lookupIndex) Verify(VCursor, interface{}, []byte) (bool, error) {
	return false, nil
}
func (*lookupIndex) Map(VCursor, []interface{}) ([][]byte, error) { return nil, nil }
func (*lookupIndex) Create(VCursor, interface{}, []byte) error    { return nil }
func (*lookupIndex) Delete(VCursor, []interface{}, []byte) error  { return nil }

func newLookupIndex(name string, _ map[string]interface{}) (Vindex, error) {
	return &lookupIndex{name: name}, nil
}

// multiIndex satisfies Lookup, NonUnique.
type multiIndex struct{ name string }

func (v *multiIndex) String() string { return v.name }
func (*multiIndex) Cost() int        { return 3 }
func (*multiIndex) Verify(VCursor, interface{}, []byte) (bool, error) {
	return false, nil
}
func (*multiIndex) Map(VCursor, []interface{}) ([][][]byte, error) { return nil, nil }
func (*multiIndex) Create(VCursor, interface{}, []byte) error      { return nil }
func (*multiIndex) Delete(VCursor, []interface{}, []byte) error    { return nil }

func newMultiIndex(name string, _ map[string]interface{}) (Vindex, error) {
	return &multiIndex{name: name}, nil
}

// costlyIndex satisfies Lookup, NonUnique.
type costlyIndex struct{ name string }

func (v *costlyIndex) String() string { return v.name }
func (*costlyIndex) Cost() int        { return 10 }
func (*costlyIndex) Verify(VCursor, interface{}, []byte) (bool, error) {
	return false, nil
}
func (*costlyIndex) Map(VCursor, []interface{}) ([][][]byte, error) { return nil, nil }
func (*costlyIndex) Create(VCursor, interface{}, []byte) error      { return nil }
func (*costlyIndex) Delete(VCursor, []interface{}, []byte) error    { return nil }

func newCostlyIndex(name string, _ map[string]interface{}) (Vindex, error) {
	return &costlyIndex{name: name}, nil
}

func init() {
	Register("hash", newHashIndex)
	Register("lookup", newLookupIndex)
	Register("multi", newMultiIndex)
	Register("costly", newCostlyIndex)
}

func TestPlan(t *testing.T) {
	vschema, err := LoadFile(locateFile("schema_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	testFile(t, "from_cases.txt", vschema)
	testFile(t, "filter_cases.txt", vschema)
	testFile(t, "select_cases.txt", vschema)
	testFile(t, "postprocess_cases.txt", vschema)
	testFile(t, "generator_cases.txt", vschema)
	testFile(t, "dml_cases.txt", vschema)
	testFile(t, "unsupported_cases.txt", vschema)
}

func TestOne(t *testing.T) {
	vschema, err := LoadFile(locateFile("schema_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	testFile(t, "onecase.txt", vschema)
}

func testFile(t *testing.T, filename string, vschema *VSchema) {
	for tcase := range iterateExecFile(filename) {
		plan, err := BuildPlan(tcase.input, vschema)
		var out string
		if err != nil {
			out = err.Error()
		} else {
			bout, _ := json.Marshal(plan)
			out = string(bout)
		}
		if out != tcase.output {
			t.Errorf("File: %s, Line:%v\n%s\n%s", filename, tcase.lineno, tcase.output, out)
			// Uncomment these lines to re-generate input files
			if err != nil {
				out = fmt.Sprintf("\"%s\"", out)
			} else {
				bout, _ := json.MarshalIndent(plan, "", "  ")
				out = string(bout)
			}
			fmt.Printf("%s\"%s\"\n%s\n\n", tcase.comments, tcase.input, out)
		}
	}
}

type testCase struct {
	file     string
	lineno   int
	input    string
	output   string
	comments string
}

func iterateExecFile(name string) (testCaseIterator chan testCase) {
	name = locateFile(name)
	fd, err := os.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		panic(fmt.Sprintf("Could not open file %s", name))
	}
	testCaseIterator = make(chan testCase)
	var comments string
	go func() {
		defer close(testCaseIterator)

		r := bufio.NewReader(fd)
		lineno := 0
		for {
			binput, err := r.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Printf("Line: %d\n", lineno)
					panic(fmt.Errorf("error reading file %s: %s", name, err.Error()))
				}
				break
			}
			lineno++
			input := string(binput)
			if input == "" || input == "\n" || strings.HasPrefix(input, "Length:") {
				continue
			}
			if input[0] == '#' {
				comments = comments + input
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
					panic(fmt.Errorf("error reading file %s: %s", name, err.Error()))
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
			testCaseIterator <- testCase{
				file:     name,
				lineno:   lineno,
				input:    input,
				output:   string(output),
				comments: comments,
			}
			comments = ""
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
