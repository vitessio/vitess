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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	"vitess.io/vitess/go/test/utils"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vtgate/semantics"

	"github.com/google/go-cmp/cmp"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

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
func (*hashIndex) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*hashIndex) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
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
func (*lookupIndex) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*lookupIndex) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*lookupIndex) Create(context.Context, vindexes.VCursor, [][]sqltypes.Value, [][]byte, bool) error {
	return nil
}
func (*lookupIndex) Delete(context.Context, vindexes.VCursor, [][]sqltypes.Value, []byte) error {
	return nil
}
func (*lookupIndex) Update(context.Context, vindexes.VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}

func newLookupIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &lookupIndex{name: name}, nil
}

var _ vindexes.Lookup = (*lookupIndex)(nil)

// nameLkpIndex satisfies Lookup, NonUnique.
type nameLkpIndex struct{ name string }

func (v *nameLkpIndex) String() string                     { return v.name }
func (*nameLkpIndex) Cost() int                            { return 3 }
func (*nameLkpIndex) IsUnique() bool                       { return false }
func (*nameLkpIndex) NeedsVCursor() bool                   { return false }
func (*nameLkpIndex) AllowBatch() bool                     { return true }
func (*nameLkpIndex) GetCommitOrder() vtgatepb.CommitOrder { return vtgatepb.CommitOrder_NORMAL }
func (*nameLkpIndex) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*nameLkpIndex) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*nameLkpIndex) Create(context.Context, vindexes.VCursor, [][]sqltypes.Value, [][]byte, bool) error {
	return nil
}
func (*nameLkpIndex) Delete(context.Context, vindexes.VCursor, [][]sqltypes.Value, []byte) error {
	return nil
}
func (*nameLkpIndex) Update(context.Context, vindexes.VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}
func (v *nameLkpIndex) Query() (string, []string) {
	return "select name, keyspace_id from name_user_vdx where name in ::name", []string{"name"}
}
func (*nameLkpIndex) MapResult([]sqltypes.Value, []*sqltypes.Result) ([]key.Destination, error) {
	return nil, nil
}

func newNameLkpIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &nameLkpIndex{name: name}, nil
}

var _ vindexes.Vindex = (*nameLkpIndex)(nil)
var _ vindexes.Lookup = (*nameLkpIndex)(nil)
var _ vindexes.LookupPlanable = (*nameLkpIndex)(nil)

// costlyIndex satisfies Lookup, NonUnique.
type costlyIndex struct{ name string }

func (v *costlyIndex) String() string   { return v.name }
func (*costlyIndex) Cost() int          { return 10 }
func (*costlyIndex) IsUnique() bool     { return false }
func (*costlyIndex) NeedsVCursor() bool { return false }
func (*costlyIndex) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*costlyIndex) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*costlyIndex) Create(context.Context, vindexes.VCursor, [][]sqltypes.Value, [][]byte, bool) error {
	return nil
}
func (*costlyIndex) Delete(context.Context, vindexes.VCursor, [][]sqltypes.Value, []byte) error {
	return nil
}
func (*costlyIndex) Update(context.Context, vindexes.VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}

func newCostlyIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &costlyIndex{name: name}, nil
}

var _ vindexes.Vindex = (*costlyIndex)(nil)
var _ vindexes.Lookup = (*costlyIndex)(nil)

// multiColIndex satisfies multi column vindex.
type multiColIndex struct {
	name string
}

func newMultiColIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &multiColIndex{name: name}, nil
}

var _ vindexes.MultiColumn = (*multiColIndex)(nil)

func (m *multiColIndex) String() string { return m.name }

func (m *multiColIndex) Cost() int { return 1 }

func (m *multiColIndex) IsUnique() bool { return true }

func (m *multiColIndex) NeedsVCursor() bool { return false }

func (m *multiColIndex) Map(ctx context.Context, vcursor vindexes.VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}

func (m *multiColIndex) Verify(ctx context.Context, vcursor vindexes.VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	return []bool{}, nil
}

func (m *multiColIndex) PartialVindex() bool {
	return true
}

func init() {
	vindexes.Register("hash_test", newHashIndex)
	vindexes.Register("lookup_test", newLookupIndex)
	vindexes.Register("name_lkp_test", newNameLkpIndex)
	vindexes.Register("costly", newCostlyIndex)
	vindexes.Register("multiCol_test", newMultiColIndex)
}

const (
	samePlanMarker  = "Gen4 plan same as above\n"
	gen4ErrorPrefix = "Gen4 error: "
)

func makeTestOutput(t *testing.T) string {
	testOutputTempDir := utils.MakeTestOutput(t, "testdata", "plan_test")

	t.Cleanup(func() {
		if !t.Failed() {
			_ = os.RemoveAll(testOutputTempDir)
		} else {
			t.Logf("Errors found in plantests. If the output is correct, run `cp %s/* testdata/` to update test expectations", testOutputTempDir)
		}
	})

	return testOutputTempDir
}

func TestPlan(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "schema_test.json", true),
		sysVarEnabled: true,
	}
	testOutputTempDir := makeTestOutput(t)

	// You will notice that some tests expect user.Id instead of user.id.
	// This is because we now pre-create vindex columns in the symbol
	// table, which come from vschema. In the test vschema,
	// the column is named as Id. This is to make sure that
	// column names are case-preserved, but treated as
	// case-insensitive even if they come from the vschema.
	testFile(t, "aggr_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "dml_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "from_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "filter_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "postprocess_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "select_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "symtab_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "unsupported_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "vindex_func_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "wireup_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "memory_sort_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "use_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "set_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "union_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "transaction_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "lock_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "large_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "ddl_cases_no_default_keyspace.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "flush_cases_no_default_keyspace.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "show_cases_no_default_keyspace.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "stream_cases.txt", testOutputTempDir, vschemaWrapper, false)
	testFile(t, "systemtables_cases.txt", testOutputTempDir, vschemaWrapper, false)
}

func TestSysVarSetDisabled(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "schema_test.json", true),
		sysVarEnabled: false,
	}

	testFile(t, "set_sysvar_disabled_cases.txt", makeTestOutput(t), vschemaWrapper, false)
}

func TestOne(t *testing.T) {
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json", true),
	}

	testFile(t, "onecase.txt", "", vschema, true)
}

func TestOneWithMainAsDefault(t *testing.T) {
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
	}

	testFile(t, "onecase.txt", "", vschema, false)
}

func TestOneWithSecondUserAsDefault(t *testing.T) {
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "second_user",
			Sharded: true,
		},
	}

	testFile(t, "onecase.txt", "", vschema, false)
}

func TestOneWithUserAsDefault(t *testing.T) {
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "user",
			Sharded: true,
		},
	}

	testFile(t, "onecase.txt", "", vschema, false)
}

func TestOneWithTPCHVSchema(t *testing.T) {
	vschema := &vschemaWrapper{
		v:             loadSchema(t, "tpch_schema_test.json", true),
		sysVarEnabled: true,
	}

	testFile(t, "onecase.txt", "", vschema, false)
}

func TestRubyOnRailsQueries(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "rails_schema_test.json", true),
		sysVarEnabled: true,
	}

	testFile(t, "rails_cases.txt", makeTestOutput(t), vschemaWrapper, false)
}

func TestOLTP(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "oltp_schema_test.json", true),
		sysVarEnabled: true,
	}

	testFile(t, "oltp_cases.txt", makeTestOutput(t), vschemaWrapper, false)
}

func TestTPCC(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "tpcc_schema_test.json", true),
		sysVarEnabled: true,
	}

	testFile(t, "tpcc_cases.txt", makeTestOutput(t), vschemaWrapper, false)
}

func TestTPCH(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "tpch_schema_test.json", true),
		sysVarEnabled: true,
	}

	testFile(t, "tpch_cases.txt", makeTestOutput(t), vschemaWrapper, false)
}

func BenchmarkOLTP(b *testing.B) {
	benchmarkWorkload(b, "oltp")
}

func BenchmarkTPCC(b *testing.B) {
	benchmarkWorkload(b, "tpcc")
}

func BenchmarkTPCH(b *testing.B) {
	benchmarkWorkload(b, "tpch")
}

func benchmarkWorkload(b *testing.B, name string) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(b, name+"_schema_test.json", true),
		sysVarEnabled: true,
	}

	var testCases []testCase
	for tc := range iterateExecFile(name + "_cases.txt") {
		testCases = append(testCases, tc)
	}
	b.ResetTimer()
	for _, version := range plannerVersions {
		b.Run(version.String(), func(b *testing.B) {
			benchmarkPlanner(b, version, testCases, vschemaWrapper)
		})
	}
}

func TestBypassPlanningShardTargetFromFile(t *testing.T) {
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
		dest:       key.DestinationShard("-80")}

	testFile(t, "bypass_shard_cases.txt", makeTestOutput(t), vschema, false)
}
func TestBypassPlanningKeyrangeTargetFromFile(t *testing.T) {
	keyRange, _ := key.ParseShardingSpec("-")

	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
		dest:       key.DestinationExactKeyRange{KeyRange: keyRange[0]},
	}

	testFile(t, "bypass_keyrange_cases.txt", makeTestOutput(t), vschema, false)
}

func TestWithDefaultKeyspaceFromFile(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
	}

	testOutputTempDir := makeTestOutput(t)
	testFile(t, "alterVschema_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "ddl_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "migration_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "flush_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "show_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "call_cases.txt", testOutputTempDir, vschema, false)
}

func TestWithDefaultKeyspaceFromFileSharded(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "second_user",
			Sharded: true,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
	}

	testOutputTempDir := makeTestOutput(t)
	testFile(t, "select_cases_with_default.txt", testOutputTempDir, vschema, false)
}

func TestWithUserDefaultKeyspaceFromFileSharded(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "user",
			Sharded: true,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
	}

	testOutputTempDir := makeTestOutput(t)
	testFile(t, "select_cases_with_user_as_default.txt", testOutputTempDir, vschema, false)
}

func TestWithSystemSchemaAsDefaultKeyspace(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemaWrapper{
		v:          loadSchema(t, "schema_test.json", true),
		keyspace:   &vindexes.Keyspace{Name: "information_schema"},
		tabletType: topodatapb.TabletType_PRIMARY,
	}

	testFile(t, "sysschema_default.txt", makeTestOutput(t), vschema, false)
}

func TestOtherPlanningFromFile(t *testing.T) {
	// We are testing this separately so we can set a default keyspace
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
	}

	testOutputTempDir := makeTestOutput(t)
	testFile(t, "other_read_cases.txt", testOutputTempDir, vschema, false)
	testFile(t, "other_admin_cases.txt", testOutputTempDir, vschema, false)
}

func loadSchema(t testing.TB, filename string, setCollation bool) *vindexes.VSchema {
	formal, err := vindexes.LoadFormal(locateFile(filename))
	if err != nil {
		t.Fatal(err)
	}
	vschema := vindexes.BuildVSchema(formal)
	if err != nil {
		t.Fatal(err)
	}
	for _, ks := range vschema.Keyspaces {
		if ks.Error != nil {
			t.Fatal(ks.Error)
		}

		// setting a default value to all the text columns in the tables of this keyspace
		// so that we can "simulate" a real case scenario where the vschema is aware of
		// columns' collations.
		if setCollation {
			for _, table := range ks.Tables {
				for i, col := range table.Columns {
					if sqltypes.IsText(col.Type) {
						table.Columns[i].CollationName = "latin1_swedish_ci"
					}
				}
			}
		}
	}
	return vschema
}

var _ plancontext.VSchema = (*vschemaWrapper)(nil)

type vschemaWrapper struct {
	v             *vindexes.VSchema
	keyspace      *vindexes.Keyspace
	tabletType    topodatapb.TabletType
	dest          key.Destination
	sysVarEnabled bool
	version       plancontext.PlannerVersion
}

func (vw *vschemaWrapper) IsShardRoutingEnabled() bool {
	return false
}

func (vw *vschemaWrapper) GetVSchema() *vindexes.VSchema {
	return vw.v
}

func (vw *vschemaWrapper) GetSrvVschema() *vschemapb.SrvVSchema {
	return &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"user": {
				Sharded:  true,
				Vindexes: map[string]*vschemapb.Vindex{},
				Tables: map[string]*vschemapb.Table{
					"user": {},
				},
			},
		},
	}
}

func (vw *vschemaWrapper) ConnCollation() collations.ID {
	return collations.Unknown
}

func (vw *vschemaWrapper) PlannerWarning(_ string) {
}

func (vw *vschemaWrapper) ForeignKeyMode() string {
	return "allow"
}

func (vw *vschemaWrapper) AllKeyspace() ([]*vindexes.Keyspace, error) {
	if vw.keyspace == nil {
		return nil, errors.New("keyspace not available")
	}
	return []*vindexes.Keyspace{vw.keyspace}, nil
}

// FindKeyspace implements the VSchema interface
func (vw *vschemaWrapper) FindKeyspace(keyspace string) (*vindexes.Keyspace, error) {
	if vw.keyspace == nil {
		return nil, errors.New("keyspace not available")
	}
	if vw.keyspace.Name == keyspace {
		return vw.keyspace, nil
	}
	return nil, nil
}

func (vw *vschemaWrapper) Planner() plancontext.PlannerVersion {
	return vw.version
}

// SetPlannerVersion implements the ContextVSchema interface
func (vw *vschemaWrapper) SetPlannerVersion(v plancontext.PlannerVersion) {
	vw.version = v
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
	destKeyspace, destTabletType, destTarget, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_PRIMARY)
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
	destKeyspace, destTabletType, destTarget, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_PRIMARY)
	if err != nil {
		return nil, nil, destKeyspace, destTabletType, destTarget, err
	}
	if destKeyspace == "" {
		destKeyspace = vw.getActualKeyspace()
	}
	table, vindex, err := vw.v.FindTableOrVindex(destKeyspace, tab.Name.String(), topodatapb.TabletType_PRIMARY)
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

func (vw *vschemaWrapper) WarnUnshardedOnly(_ string, _ ...any) {

}

func (vw *vschemaWrapper) ErrorIfShardedF(keyspace *vindexes.Keyspace, _, errFmt string, params ...any) error {
	if keyspace.Sharded {
		return fmt.Errorf(errFmt, params...)
	}
	return nil
}

func (vw *vschemaWrapper) currentDb() string {
	ksName := ""
	if vw.keyspace != nil {
		ksName = vw.keyspace.Name
	}
	return ksName
}

func (vw *vschemaWrapper) FindRoutedShard(keyspace, shard string) (string, error) {
	return "", nil
}

func testFile(t *testing.T, filename, tempDir string, vschema *vschemaWrapper, render bool) {
	t.Run(filename, func(t *testing.T) {
		expected := &strings.Builder{}
		var outFirstPlanner string
		for tcase := range iterateExecFile(filename) {
			t.Run(fmt.Sprintf("%d V3: %s", tcase.lineno, tcase.comments), func(t *testing.T) {
				vschema.version = V3
				plan, err := TestBuilder(tcase.input, vschema, vschema.currentDb())
				if render && plan != nil {
					viz, err := engine.GraphViz(plan.Instructions)
					if err == nil {
						_ = viz.Render()
					}
				}
				out := getPlanOrErrorOutput(err, plan)

				if out != tcase.output {
					t.Errorf("V3 - %s:%d\nDiff:\n%s\n[%s] \n[%s]", filename, tcase.lineno, cmp.Diff(tcase.output, out), tcase.output, out)
				}
				if err != nil {
					out = `"` + out + `"`
				}
				outFirstPlanner = out

				expected.WriteString(tcase.comments)
				encoder := json.NewEncoder(expected)
				encoder.Encode(tcase.input)
				expected.WriteString(fmt.Sprintf("%s\n", out))
			})

			vschema.version = Gen4
			out, err := getPlanOutput(tcase, vschema, render)
			if err != nil && tcase.output2ndPlanner == "" && strings.HasPrefix(err.Error(), "gen4 does not yet support") {
				expected.WriteString("\n")
				continue
			}

			// our expectation for the new planner on this query is one of three
			//  - it produces the same plan as V3 - this is shown using empty brackets: {\n}
			//  - it produces a different but accepted plan - this is shown using the accepted plan
			//  - or it produces a different plan that has not yet been accepted, or it fails to produce a plan
			//       this is shown by not having any info at all after the result for the V3 planner
			//       with this last expectation, it is an error if the Gen4 planner
			//       produces the same plan as the V3 planner does
			testName := fmt.Sprintf("%d Gen4: %s", tcase.lineno, tcase.comments)
			t.Run(testName, func(t *testing.T) {
				if out != tcase.output2ndPlanner {
					t.Errorf("Gen4 - %s:%d\nDiff:\n%s\n[%s] \n[%s]", filename, tcase.lineno, cmp.Diff(tcase.output2ndPlanner, out), tcase.output2ndPlanner, out)
				}
				if err != nil {
					out = `"` + out + `"`
				}

				if outFirstPlanner == out {
					expected.WriteString(samePlanMarker)
				} else {
					if err != nil {
						out = out[1 : len(out)-1] // remove the double quotes
						expected.WriteString(fmt.Sprintf("Gen4 error: %s\n", out))
					} else {
						expected.WriteString(fmt.Sprintf("%s\n", out))
					}
				}
			})
			expected.WriteString("\n")
		}

		if tempDir != "" {
			gotFile := fmt.Sprintf("%s/%s", tempDir, filename)
			_ = os.WriteFile(gotFile, []byte(strings.TrimSpace(expected.String())+"\n"), 0644)
		}
	})
}

func getPlanOutput(tcase testCase, vschema *vschemaWrapper, render bool) (out string, err error) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("panicked: %v\n%s", r, string(debug.Stack()))
		}
	}()
	plan, err := TestBuilder(tcase.input, vschema, vschema.currentDb())
	if render && plan != nil {
		viz, err := engine.GraphViz(plan.Instructions)
		if err == nil {
			_ = viz.Render()
		}
	}
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
			nextLine := string(binput)
			switch {
			case nextLine == samePlanMarker:
				output2Planner = output
			case strings.HasPrefix(nextLine, "{"):
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
			case strings.HasPrefix(nextLine, gen4ErrorPrefix):
				output2Planner = []byte(nextLine[len(gen4ErrorPrefix) : len(nextLine)-1])
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

var benchMarkFiles = []string{"from_cases.txt", "filter_cases.txt", "large_cases.txt", "aggr_cases.txt", "select_cases.txt", "union_cases.txt"}

func BenchmarkPlanner(b *testing.B) {
	vschema := &vschemaWrapper{
		v:             loadSchema(b, "schema_test.json", true),
		sysVarEnabled: true,
	}
	for _, filename := range benchMarkFiles {
		var testCases []testCase
		for tc := range iterateExecFile(filename) {
			testCases = append(testCases, tc)
		}
		b.Run(filename+"-v3", func(b *testing.B) {
			benchmarkPlanner(b, V3, testCases, vschema)
		})
		b.Run(filename+"-gen4", func(b *testing.B) {
			benchmarkPlanner(b, Gen4, testCases, vschema)
		})
		b.Run(filename+"-gen4left2right", func(b *testing.B) {
			benchmarkPlanner(b, Gen4Left2Right, testCases, vschema)
		})
	}
}

func BenchmarkSemAnalysis(b *testing.B) {
	vschema := &vschemaWrapper{
		v:             loadSchema(b, "schema_test.json", true),
		sysVarEnabled: true,
	}

	for i := 0; i < b.N; i++ {
		for _, filename := range benchMarkFiles {
			for tc := range iterateExecFile(filename) {
				exerciseAnalyzer(tc.input, vschema.currentDb(), vschema)
			}
		}
	}
}

func exerciseAnalyzer(query, database string, s semantics.SchemaInformation) {
	defer func() {
		// if analysis panics, let's just continue. this is just a benchmark
		recover()
	}()

	ast, err := sqlparser.Parse(query)
	if err != nil {
		return
	}
	sel, ok := ast.(sqlparser.SelectStatement)
	if !ok {
		return
	}

	_, _ = semantics.Analyze(sel, database, s)
}

func BenchmarkSelectVsDML(b *testing.B) {
	vschema := &vschemaWrapper{
		v:             loadSchema(b, "schema_test.json", true),
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

func benchmarkPlanner(b *testing.B, version plancontext.PlannerVersion, testCases []testCase, vschema *vschemaWrapper) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		for _, tcase := range testCases {
			if tcase.output2ndPlanner != "" {
				vschema.version = version
				_, _ = TestBuilder(tcase.input, vschema, vschema.currentDb())
			}
		}
	}
}
