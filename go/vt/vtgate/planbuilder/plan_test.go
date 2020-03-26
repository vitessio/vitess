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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// hashIndex is a functional, unique Vindex.
type hashIndex struct{ name string }

func (v *hashIndex) String() string   { return v.name }
func (*hashIndex) Cost() int          { return 1 }
func (*hashIndex) IsUnique() bool     { return true }
func (*hashIndex) NeedsVCursor() bool { return false }
func (*hashIndex) Verify(vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*hashIndex) Map(cursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}

func newHashIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &hashIndex{name: name}, nil
}

// lookupIndex is a unique Vindex, and satisfies Lookup.
type lookupIndex struct{ name string }

func (v *lookupIndex) String() string   { return v.name }
func (*lookupIndex) Cost() int          { return 2 }
func (*lookupIndex) IsUnique() bool     { return true }
func (*lookupIndex) NeedsVCursor() bool { return false }
func (*lookupIndex) Verify(vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*lookupIndex) Map(cursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*lookupIndex) Create(vindexes.VCursor, [][]sqltypes.Value, [][]byte, bool) error { return nil }
func (*lookupIndex) Delete(vindexes.VCursor, [][]sqltypes.Value, []byte) error         { return nil }
func (*lookupIndex) Update(vindexes.VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}

func newLookupIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &lookupIndex{name: name}, nil
}

var _ vindexes.Lookup = (*lookupIndex)(nil)

// multiIndex satisfies Lookup, NonUnique.
type multiIndex struct{ name string }

func (v *multiIndex) String() string   { return v.name }
func (*multiIndex) Cost() int          { return 3 }
func (*multiIndex) IsUnique() bool     { return false }
func (*multiIndex) NeedsVCursor() bool { return false }
func (*multiIndex) Verify(vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*multiIndex) Map(cursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*multiIndex) Create(vindexes.VCursor, [][]sqltypes.Value, [][]byte, bool) error { return nil }
func (*multiIndex) Delete(vindexes.VCursor, [][]sqltypes.Value, []byte) error         { return nil }
func (*multiIndex) Update(vindexes.VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}

func newMultiIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &multiIndex{name: name}, nil
}

var _ vindexes.Vindex = (*multiIndex)(nil)
var _ vindexes.Lookup = (*multiIndex)(nil)

// costlyIndex satisfies Lookup, NonUnique.
type costlyIndex struct{ name string }

func (v *costlyIndex) String() string   { return v.name }
func (*costlyIndex) Cost() int          { return 10 }
func (*costlyIndex) IsUnique() bool     { return false }
func (*costlyIndex) NeedsVCursor() bool { return false }
func (*costlyIndex) Verify(vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*costlyIndex) Map(cursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*costlyIndex) Create(vindexes.VCursor, [][]sqltypes.Value, [][]byte, bool) error { return nil }
func (*costlyIndex) Delete(vindexes.VCursor, [][]sqltypes.Value, []byte) error         { return nil }
func (*costlyIndex) Update(vindexes.VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}

func newCostlyIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &costlyIndex{name: name}, nil
}

var _ vindexes.Vindex = (*costlyIndex)(nil)
var _ vindexes.Lookup = (*costlyIndex)(nil)

func init() {
	vindexes.Register("hash_test", newHashIndex)
	vindexes.Register("lookup_test", newLookupIndex)
	vindexes.Register("multi", newMultiIndex)
	vindexes.Register("costly", newCostlyIndex)
}

func TestPlan(t *testing.T) {
	vschema := loadSchema(t, "schema_test.json")
	testOutputTempDir, err := ioutil.TempDir("", "plan_test")
	require.NoError(t, err)
	// You will notice that some tests expect user.Id instead of user.id.
	// This is because we now pre-create vindex columns in the symbol
	// table, which come from vschema. In the test vschema,
	// the column is named as Id. This is to make sure that
	// column names are case-preserved, but treated as
	// case-insensitive even if they come from the vschema.
	testFile(t, "aggr_cases.txt", testOutputTempDir, vschema)
	testFile(t, "dml_cases.txt", testOutputTempDir, vschema)
	testFile(t, "from_cases.txt", testOutputTempDir, vschema)
	testFile(t, "filter_cases.txt", testOutputTempDir, vschema)
	testFile(t, "postprocess_cases.txt", testOutputTempDir, vschema)
	testFile(t, "select_cases.txt", testOutputTempDir, vschema)
	testFile(t, "symtab_cases.txt", testOutputTempDir, vschema)
	testFile(t, "unsupported_cases.txt", testOutputTempDir, vschema)
	testFile(t, "vindex_func_cases.txt", testOutputTempDir, vschema)
	testFile(t, "wireup_cases.txt", testOutputTempDir, vschema)
	testFile(t, "memory_sort_cases.txt", testOutputTempDir, vschema)
}

func TestOne(t *testing.T) {
	vschema := loadSchema(t, "schema_test.json")
	testFile(t, "onecase.txt", "", vschema)
}

func loadSchema(t *testing.T, filename string) *vindexes.VSchema {
	formal, err := vindexes.LoadFormal(locateFile(filename))
	if err != nil {
		t.Fatal(err)
	}
	vschema, err := vindexes.BuildVSchema(formal)
	if err != nil {
		t.Fatal(err)
	}
	for _, ks := range vschema.Keyspaces {
		if ks.Error != nil {
			t.Fatal(ks.Error)
		}
	}
	return vschema
}

var _ ContextVSchema = (*vschemaWrapper)(nil)

type vschemaWrapper struct {
	v *vindexes.VSchema
}

func (vw *vschemaWrapper) Destination() key.Destination {
	panic("implement me")
}

func (vw *vschemaWrapper) FindTable(tab sqlparser.TableName) (*vindexes.Table, string, topodatapb.TabletType, key.Destination, error) {
	destKeyspace, destTabletType, destTarget, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_MASTER)
	if err != nil {
		return nil, destKeyspace, destTabletType, destTarget, err
	}
	table, err := vw.v.FindTable(destKeyspace, tab.Name.String())
	if err != nil {
		return nil, destKeyspace, destTabletType, destTarget, err
	}
	return table, destKeyspace, destTabletType, destTarget, nil
}

func (vw *vschemaWrapper) FindTablesOrVindex(tab sqlparser.TableName) ([]*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	destKeyspace, destTabletType, destTarget, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_MASTER)
	if err != nil {
		return nil, nil, destKeyspace, destTabletType, destTarget, err
	}
	tables, vindex, err := vw.v.FindTablesOrVindex(destKeyspace, tab.Name.String(), topodatapb.TabletType_MASTER)
	if err != nil {
		return nil, nil, destKeyspace, destTabletType, destTarget, err
	}
	return tables, vindex, destKeyspace, destTabletType, destTarget, nil
}

func (vw *vschemaWrapper) DefaultKeyspace() (*vindexes.Keyspace, error) {
	return vw.v.Keyspaces["main"].Keyspace, nil
}

func (vw *vschemaWrapper) TargetString() string {
	return "targetString"
}

// For the purposes of this set of tests, just compare the actual plan
// and ignore all the metrics.
type testPlan struct {
	Original     string           `json:",omitempty"`
	Instructions engine.Primitive `json:",omitempty"`
}

func testFile(t *testing.T, filename, tempDir string, vschema *vindexes.VSchema) {
	t.Run(filename, func(t *testing.T) {
		expected := &strings.Builder{}
		fail := false
		for tcase := range iterateExecFile(filename) {
			t.Run(tcase.comments, func(t *testing.T) {
				plan, err := Build(tcase.input, &vschemaWrapper{
					v: vschema,
				})

				out := getPlanOrErrorOutput(err, plan)

				if out != tcase.output {
					fail = true
					t.Errorf("File: %s, Line: %v\n %s", filename, tcase.lineno, cmp.Diff(tcase.output, out))
				}

				if err != nil {
					out = `"` + out + `"`
				}

				expected.WriteString(fmt.Sprintf("%s\"%s\"\n%s\n\n", tcase.comments, tcase.input, out))

			})
		}
		if fail && tempDir != "" {
			gotFile := fmt.Sprintf("%s/%s", tempDir, filename)
			ioutil.WriteFile(gotFile, []byte(strings.TrimSpace(expected.String())+"\n"), 0644)
			fmt.Println(fmt.Sprintf("Errors found in plantests. If the output is correct, run `cp %s/* testdata/` to update test expectations", tempDir))
		}
	})
}

func getPlanOrErrorOutput(err error, plan *engine.Plan) string {
	if err != nil {
		return err.Error()
	}
	bout, _ := json.MarshalIndent(testPlan{
		Original:     plan.Original,
		Instructions: plan.Instructions,
	}, "", "  ")
	return string(bout)
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
					panic(fmt.Errorf("error reading file %s: line %d: %s", name, lineno, err.Error()))
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
				panic(fmt.Sprintf("Line: %d, input: %s, error: %v\n", lineno, binput, err))
			}
			input = strings.Trim(input, "\"")
			var output []byte
			for {
				l, err := r.ReadBytes('\n')
				lineno++
				if err != nil {
					panic(fmt.Sprintf("error reading file %s line# %d: %s", name, lineno, err.Error()))
				}
				output = append(output, l...)
				if l[0] == '}' {
					output = output[:len(output)-1]
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
	return "testdata/" + name
}
