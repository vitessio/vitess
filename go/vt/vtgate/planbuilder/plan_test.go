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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"github.com/google/go-cmp/cmp"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

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

const samePlanMarker = "Gen4 plan same as above\n"

func TestPlan(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "schema_test.json"),
		sysVarEnabled: true,
	}

	testOutputTempDir, err := ioutil.TempDir("", "plan_test")
	require.NoError(t, err)
	defer func() {
		if !t.Failed() {
			os.RemoveAll(testOutputTempDir)
		}
	}()
	// You will notice that some tests expect user.Id instead of user.id.
	// This is because we now pre-create vindex columns in the symbol
	// table, which come from vschema. In the test vschema,
	// the column is named as Id. This is to make sure that
	// column names are case-preserved, but treated as
	// case-insensitive even if they come from the vschema.
	testFile(t, "aggr_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "dml_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "from_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "filter_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "postprocess_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "select_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "symtab_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "unsupported_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "vindex_func_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "wireup_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "memory_sort_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "use_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "set_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "union_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "transaction_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "lock_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "large_cases.txt", testOutputTempDir, vschemaWrapper, true)
	testFile(t, "ddl_cases_no_default_keyspace.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "flush_cases_no_default_keyspace.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "show_cases_no_default_keyspace.txt", testOutputTempDir, vschemaWrapper, false)
}

func TestSysVarSetDisabled(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "schema_test.json"),
		sysVarEnabled: false,
	}

	testOutputTempDir, err := ioutil.TempDir("", "plan_test")
	require.NoError(t, err)
	defer os.RemoveAll(testOutputTempDir)
	testFile(t, "set_sysvar_disabled_cases.txt", testOutputTempDir, vschemaWrapper, false)
}

func TestOne(t *testing.T) {
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json"),
	}

	testFile(t, "onecase.txt", "", vschema, true)
}

func TestBypassPlanningFromFile(t *testing.T) {
	testOutputTempDir, err := ioutil.TempDir("", "plan_test")
	require.NoError(t, err)
	defer os.RemoveAll(testOutputTempDir)
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json"),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_MASTER,
		dest:       key.DestinationShard("-80"),
	}

	testFile(t, "bypass_cases.txt", testOutputTempDir, vschema, true)
}

func TestWithDefaultKeyspaceFromFile(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	testOutputTempDir, err := ioutil.TempDir("", "plan_test")
	require.NoError(t, err)
	defer os.RemoveAll(testOutputTempDir)
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json"),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_MASTER,
	}

	testFile(t, "alterVschema_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "ddl_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "migration_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "flush_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "show_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "call_cases.txt", testOutputTempDir, vschema, false)
}

func TestWithSystemSchemaAsDefaultKeyspace(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	testOutputTempDir, err := ioutil.TempDir("", "plan_test")
	require.NoError(t, err)
	defer os.RemoveAll(testOutputTempDir)
	vschema := &vschemaWrapper{
		v:          loadSchema(t, "schema_test.json"),
		keyspace:   &vindexes.Keyspace{Name: "mysql"},
		tabletType: topodatapb.TabletType_MASTER,
	}

	testFile(t, "sysschema_default.txt", testOutputTempDir, vschema, false)
}

func TestOtherPlanningFromFile(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	testOutputTempDir, err := ioutil.TempDir("", "plan_test")
	defer os.RemoveAll(testOutputTempDir)
	require.NoError(t, err)
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json"),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_MASTER,
	}

	testFile(t, "other_read_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "other_admin_cases.txt", testOutputTempDir, vschema, false)
}

func loadSchema(t testing.TB, filename string) *vindexes.VSchema {
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
	v             *vindexes.VSchema
	keyspace      *vindexes.Keyspace
	tabletType    topodatapb.TabletType
	dest          key.Destination
	sysVarEnabled bool
	version       PlannerVersion
}

func (vw *vschemaWrapper) AllKeyspace() ([]*vindexes.Keyspace, error) {
	if vw.keyspace == nil {
		return nil, errors.New("keyspace not available")
	}
	return []*vindexes.Keyspace{vw.keyspace}, nil
}

func (vw *vschemaWrapper) Planner() PlannerVersion {
	return vw.version
}
func (vw *vschemaWrapper) GetSemTable() *semantics.SemTable {
	return nil
}

func (vw *vschemaWrapper) KeyspaceExists(keyspace string) bool {
	if vw.keyspace != nil {
		return vw.keyspace.Name == keyspace
	}
	return false
}

func (vw *vschemaWrapper) SysVarSetEnabled() bool {
	return vw.sysVarEnabled
}

func (vw *vschemaWrapper) TargetDestination(qualifier string) (key.Destination, *vindexes.Keyspace, topodatapb.TabletType, error) {
	var keyspaceName string
	if vw.keyspace != nil {
		keyspaceName = vw.keyspace.Name
	}
	if vw.dest == nil && qualifier != "" {
		keyspaceName = qualifier
	}
	if keyspaceName == "" {
		return nil, nil, 0, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace not specified")
	}
	keyspace := vw.v.Keyspaces[keyspaceName]
	if keyspace == nil {
		return nil, nil, 0, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadDb, "Unknown database '%s' in vschema", keyspaceName)
	}
	return vw.dest, keyspace.Keyspace, vw.tabletType, nil

}

func (vw *vschemaWrapper) TabletType() topodatapb.TabletType {
	return vw.tabletType
}

func (vw *vschemaWrapper) Destination() key.Destination {
	return vw.dest
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

func (vw *vschemaWrapper) FindTableOrVindex(tab sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	destKeyspace, destTabletType, destTarget, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_MASTER)
	if err != nil {
		return nil, nil, destKeyspace, destTabletType, destTarget, err
	}
	if destKeyspace == "" {
		destKeyspace = vw.getActualKeyspace()
	}
	table, vindex, err := vw.v.FindTableOrVindex(destKeyspace, tab.Name.String(), topodatapb.TabletType_MASTER)
	if err != nil {
		return nil, nil, destKeyspace, destTabletType, destTarget, err
	}
	return table, vindex, destKeyspace, destTabletType, destTarget, nil
}

func (vw *vschemaWrapper) getActualKeyspace() string {
	if vw.keyspace == nil {
		return ""
	}
	if !sqlparser.SystemSchema(vw.keyspace.Name) {
		return vw.keyspace.Name
	}
	ks, err := vw.AnyKeyspace()
	if err != nil {
		return ""
	}
	return ks.Name
}

func (vw *vschemaWrapper) DefaultKeyspace() (*vindexes.Keyspace, error) {
	return vw.v.Keyspaces["main"].Keyspace, nil
}

func (vw *vschemaWrapper) AnyKeyspace() (*vindexes.Keyspace, error) {
	return vw.DefaultKeyspace()
}

func (vw *vschemaWrapper) FirstSortedKeyspace() (*vindexes.Keyspace, error) {
	return vw.v.Keyspaces["main"].Keyspace, nil
}

func (vw *vschemaWrapper) TargetString() string {
	return "targetString"
}

func (vw *vschemaWrapper) WarnUnshardedOnly(_ string, _ ...interface{}) {

}

func (vw *vschemaWrapper) ErrorIfShardedF(keyspace *vindexes.Keyspace, _, errFmt string, params ...interface{}) error {
	if keyspace.Sharded {
		return fmt.Errorf(errFmt, params...)
	}
	return nil
}

func escapeNewLines(in string) string {
	return strings.ReplaceAll(in, "\n", "\\n")
}

func testFile(t *testing.T, filename, tempDir string, vschema *vschemaWrapper, checkV4equalPlan bool) {
	var checkAllTests = false
	t.Run(filename, func(t *testing.T) {
		expected := &strings.Builder{}
		fail := checkAllTests
		var outFirstPlanner string
		for tcase := range iterateExecFile(filename) {
			t.Run(fmt.Sprintf("%d V3: %s", tcase.lineno, tcase.comments), func(t *testing.T) {
				vschema.version = V3
				plan, err := TestBuilder(tcase.input, vschema)
				out := getPlanOrErrorOutput(err, plan)

				if out != tcase.output {
					fail = true
					t.Errorf("V3 - File: %s, Line: %d\nDiff:\n%s\n[%s] \n[%s]", filename, tcase.lineno, cmp.Diff(tcase.output, out), tcase.output, out)
				}
				if err != nil {
					out = `"` + out + `"`
				}
				outFirstPlanner = out

				expected.WriteString(fmt.Sprintf("%s\"%s\"\n%s\n", tcase.comments, escapeNewLines(tcase.input), out))
			})

			empty := false
			if tcase.output2ndPlanner == "" {
				empty = true
			}

			vschema.version = Gen4
			out, err := getPlanOutput(tcase, vschema)

			// our expectation for the new planner on this query is one of three
			//  - it produces the same plan as V3 - this is shown using empty brackets: {\n}
			//  - it produces a different but accepted plan - this is shown using the accepted plan
			//  - or it produces a different plan that has not yet been accepted, or it fails to produce a plan
			//       this is shown by not having any info at all after the result for the V3 planner
			//       with this last expectation, it is an error if the V4 planner
			//       produces the same plan as the V3 planner does
			testName := fmt.Sprintf("%d V4: %s", tcase.lineno, tcase.comments)
			if !empty || checkAllTests {
				t.Run(testName, func(t *testing.T) {
					if out != tcase.output2ndPlanner {
						fail = true
						t.Errorf("V4 - %s:%d\nDiff:\n%s\n[%s] \n[%s]", filename, tcase.lineno, cmp.Diff(tcase.output2ndPlanner, out), tcase.output, out)

					}
					if err != nil {
						out = `"` + out + `"`
					}

					if outFirstPlanner == out {
						expected.WriteString(samePlanMarker)
					} else {
						expected.WriteString(fmt.Sprintf("%s\n", out))
					}
				})
			} else {
				if out == tcase.output && checkV4equalPlan {
					t.Run(testName, func(t *testing.T) {
						t.Errorf("V4 - %s:%d\nplanner produces same output as V3", filename, tcase.lineno)
					})
				}
			}

			expected.WriteString("\n")
		}

		if fail && tempDir != "" {
			gotFile := fmt.Sprintf("%s/%s", tempDir, filename)
			_ = ioutil.WriteFile(gotFile, []byte(strings.TrimSpace(expected.String())+"\n"), 0644)
			fmt.Println(fmt.Sprintf("Errors found in plantests. If the output is correct, run `cp %s/* testdata/` to update test expectations", tempDir)) //nolint
		}
	})
}

func getPlanOutput(tcase testCase, vschema *vschemaWrapper) (out string, err error) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("panicked: %v\n%s", r, string(debug.Stack()))
		}
	}()
	plan, err := TestBuilder(tcase.input, vschema)
	out = getPlanOrErrorOutput(err, plan)
	return out, err
}

func getPlanOrErrorOutput(err error, plan *engine.Plan) string {
	if err != nil {
		return err.Error()
	}
	bout, _ := json.MarshalIndent(plan, "", "  ")
	return string(bout)
}

type testCase struct {
	file             string
	lineno           int
	input            string
	output           string
	output2ndPlanner string
	comments         string
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

			binput, err = r.ReadBytes('\n')
			lineno++
			var output2Planner []byte
			if err != nil && err != io.EOF {
				panic(fmt.Sprintf("error reading file %s line# %d: %s", name, lineno, err.Error()))
			}
			if len(binput) > 0 && string(binput) == samePlanMarker {
				output2Planner = output
			} else if len(binput) > 0 && (binput[0] == '"' || binput[0] == '{') {
				output2Planner = append(output2Planner, binput...)
				for {
					l, err := r.ReadBytes('\n')
					lineno++
					if err != nil {
						panic(fmt.Sprintf("error reading file %s line# %d: %s", name, lineno, err.Error()))
					}
					output2Planner = append(output2Planner, l...)
					if l[0] == '}' {
						output2Planner = output2Planner[:len(output2Planner)-1]
						break
					}
					if l[0] == '"' {
						output2Planner = output2Planner[1 : len(output2Planner)-2]
						break
					}
				}
			}

			testCaseIterator <- testCase{
				file:             name,
				lineno:           lineno,
				input:            input,
				output:           string(output),
				output2ndPlanner: string(output2Planner),
				comments:         comments,
			}
			comments = ""
		}
	}()
	return testCaseIterator
}

func locateFile(name string) string {
	return "testdata/" + name
}

func BenchmarkPlanner(b *testing.B) {
	filenames := []string{"from_cases.txt", "filter_cases.txt", "large_cases.txt", "aggr_cases.txt", "select_cases.txt", "union_cases.txt"}
	vschema := &vschemaWrapper{
		v:             loadSchema(b, "schema_test.json"),
		sysVarEnabled: true,
	}
	for _, filename := range filenames {
		var testCases []testCase
		for tc := range iterateExecFile(filename) {
			testCases = append(testCases, tc)
		}
		b.Run(filename+"-v3", func(b *testing.B) {
			benchmarkPlanner(b, V3, testCases, vschema)
		})
		b.Run(filename+"-v4", func(b *testing.B) {
			benchmarkPlanner(b, Gen4, testCases, vschema)
		})
		b.Run(filename+"-v4left2right", func(b *testing.B) {
			benchmarkPlanner(b, Gen4Left2Right, testCases, vschema)
		})
	}
}

func BenchmarkSelectVsDML(b *testing.B) {
	vschema := &vschemaWrapper{
		v:             loadSchema(b, "schema_test.json"),
		sysVarEnabled: true,
		version:       V3,
	}

	var dmlCases []testCase
	var selectCases []testCase

	for tc := range iterateExecFile("dml_cases.txt") {
		dmlCases = append(dmlCases, tc)
	}

	for tc := range iterateExecFile("select_cases.txt") {
		if tc.output2ndPlanner != "" {
			selectCases = append(selectCases, tc)
		}
	}

	rand.Shuffle(len(dmlCases), func(i, j int) {
		dmlCases[i], dmlCases[j] = dmlCases[j], dmlCases[i]
	})

	rand.Shuffle(len(selectCases), func(i, j int) {
		selectCases[i], selectCases[j] = selectCases[j], selectCases[i]
	})

	b.Run("DML (random sample, N=32)", func(b *testing.B) {
		benchmarkPlanner(b, V3, dmlCases[:32], vschema)
	})

	b.Run("Select (random sample, N=32)", func(b *testing.B) {
		benchmarkPlanner(b, V3, selectCases[:32], vschema)
	})
}

func benchmarkPlanner(b *testing.B, version PlannerVersion, testCases []testCase, vschema *vschemaWrapper) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		for _, tcase := range testCases {
			if tcase.output2ndPlanner != "" {
				vschema.version = version
				_, _ = TestBuilder(tcase.input, vschema)
			}
		}
	}
}
