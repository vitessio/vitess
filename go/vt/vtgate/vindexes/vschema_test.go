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

package vindexes

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"vitess.io/vitess/go/test/utils"

	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// cheapVindex is a Functional, Unique Vindex.
type cheapVindex struct {
	name   string
	Params map[string]string
}

func (v *cheapVindex) String() string   { return v.name }
func (*cheapVindex) Cost() int          { return 0 }
func (*cheapVindex) IsUnique() bool     { return true }
func (*cheapVindex) NeedsVCursor() bool { return false }
func (*cheapVindex) Verify(context.Context, VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*cheapVindex) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}

func NewCheapVindex(name string, params map[string]string) (Vindex, error) {
	return &cheapVindex{name: name, Params: params}, nil
}

var _ SingleColumn = (*stFU)(nil)

// stFU is a Functional, Unique Vindex.
type stFU struct {
	name   string
	Params map[string]string
}

func (v *stFU) String() string   { return v.name }
func (*stFU) Cost() int          { return 1 }
func (*stFU) IsUnique() bool     { return true }
func (*stFU) NeedsVCursor() bool { return false }
func (*stFU) Verify(context.Context, VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*stFU) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}

func NewSTFU(name string, params map[string]string) (Vindex, error) {
	return &stFU{name: name, Params: params}, nil
}

var _ SingleColumn = (*stFU)(nil)

// stFN is a Functional, NonUnique Vindex.
type stFN struct {
	name   string
	Params map[string]string
}

func (v *stFN) String() string   { return v.name }
func (*stFN) Cost() int          { return 1 }
func (*stFN) IsUnique() bool     { return false }
func (*stFN) NeedsVCursor() bool { return false }
func (*stFN) Verify(context.Context, VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*stFN) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}

func NewSTFN(name string, params map[string]string) (Vindex, error) {
	return &stFN{name: name, Params: params}, nil
}

var _ SingleColumn = (*stFN)(nil)

// stLN is a Lookup, NonUnique Vindex.
type stLN struct {
	name   string
	Params map[string]string
}

func (v *stLN) String() string   { return v.name }
func (*stLN) Cost() int          { return 0 }
func (*stLN) IsUnique() bool     { return false }
func (*stLN) NeedsVCursor() bool { return true }
func (*stLN) Verify(context.Context, VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*stLN) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*stLN) Create(context.Context, VCursor, [][]sqltypes.Value, [][]byte, bool) error { return nil }
func (*stLN) Delete(context.Context, VCursor, [][]sqltypes.Value, []byte) error         { return nil }
func (*stLN) Update(context.Context, VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}

func NewSTLN(name string, params map[string]string) (Vindex, error) {
	return &stLN{name: name, Params: params}, nil
}

var _ SingleColumn = (*stLN)(nil)
var _ Lookup = (*stLN)(nil)

// stLU is a Lookup, Unique Vindex.
type stLU struct {
	name   string
	Params map[string]string
}

func (v *stLU) String() string   { return v.name }
func (*stLU) Cost() int          { return 2 }
func (*stLU) IsUnique() bool     { return true }
func (*stLU) NeedsVCursor() bool { return true }
func (*stLU) Verify(context.Context, VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*stLU) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*stLU) Create(context.Context, VCursor, [][]sqltypes.Value, [][]byte, bool) error { return nil }
func (*stLU) Delete(context.Context, VCursor, [][]sqltypes.Value, []byte) error         { return nil }
func (*stLU) Update(context.Context, VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}

func NewSTLU(name string, params map[string]string) (Vindex, error) {
	return &stLU{name: name, Params: params}, nil
}

var _ SingleColumn = (*stLO)(nil)
var _ Lookup = (*stLO)(nil)
var _ WantOwnerInfo = (*stLO)(nil)

// stLO is a Lookup Vindex that wants owner columns.
type stLO struct {
	keyspace string
	name     string
	table    string
	cols     []sqlparser.IdentifierCI
}

func (v *stLO) String() string   { return v.name }
func (*stLO) Cost() int          { return 2 }
func (*stLO) IsUnique() bool     { return true }
func (*stLO) NeedsVCursor() bool { return true }
func (*stLO) Verify(context.Context, VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*stLO) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*stLO) Create(context.Context, VCursor, [][]sqltypes.Value, [][]byte, bool) error { return nil }
func (*stLO) Delete(context.Context, VCursor, [][]sqltypes.Value, []byte) error         { return nil }
func (*stLO) Update(context.Context, VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}
func (v *stLO) SetOwnerInfo(keyspace, table string, cols []sqlparser.IdentifierCI) error {
	v.keyspace = keyspace
	v.table = table
	v.cols = cols
	return nil
}

func NewSTLO(name string, _ map[string]string) (Vindex, error) {
	return &stLO{name: name}, nil
}

var _ SingleColumn = (*stLO)(nil)
var _ Lookup = (*stLO)(nil)

// mcFU is a multi-column Functional, Unique Vindex.
type mcFU struct {
	name   string
	Params map[string]string
}

func (v *mcFU) String() string   { return v.name }
func (*mcFU) Cost() int          { return 1 }
func (*mcFU) IsUnique() bool     { return true }
func (*mcFU) NeedsVCursor() bool { return false }
func (*mcFU) Verify(context.Context, VCursor, [][]sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*mcFU) Map(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*mcFU) PartialVindex() bool { return false }

func NewMCFU(name string, params map[string]string) (Vindex, error) {
	return &mcFU{name: name, Params: params}, nil
}

var _ MultiColumn = (*mcFU)(nil)

func init() {
	Register("cheap", NewCheapVindex)
	Register("stfu", NewSTFU)
	Register("stfn", NewSTFN)
	Register("stln", NewSTLN)
	Register("stlu", NewSTLU)
	Register("stlo", NewSTLO)
	Register("region_experimental_test", NewRegionExperimental)
	Register("mcfu", NewMCFU)
}

func TestUnshardedVSchemaValid(t *testing.T) {
	err := ValidateKeyspace(&vschemapb.Keyspace{
		Sharded:  false,
		Vindexes: make(map[string]*vschemapb.Vindex),
		Tables:   make(map[string]*vschemapb.Table),
	})
	require.NoError(t, err)
}

func TestUnshardedVSchema(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"t1": {}}}}}

	got := BuildVSchema(&good)
	require.NoError(t, got.Keyspaces["unsharded"].Error)

	table, err := got.FindTable("unsharded", "t1")
	require.NoError(t, err)
	assert.NotNil(t, table)

	table, err = got.FindTable("", "t1")
	require.NoError(t, err)
	assert.NotNil(t, table)

	table, err = got.FindTable("", "dual")
	require.NoError(t, err)
	assert.Equal(t, TypeReference, table.Type)
}

func TestVSchemaColumns(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Columns: []*vschemapb.Column{{
							Name: "c1"}, {
							Name: "c2",
							Type: sqltypes.VarChar}}}}}}}

	got := BuildVSchema(&good)
	require.NoError(t, got.Keyspaces["unsharded"].Error)

	t1, err := got.FindTable("unsharded", "t1")
	require.NoError(t, err)
	assertColumn(t, t1.Columns[0], "c1", sqltypes.Null)
	assertColumn(t, t1.Columns[1], "c2", sqltypes.VarChar)
}

func TestVSchemaViews(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Columns: []*vschemapb.Column{{
							Name: "c1",
						}, {
							Name: "c2",
							Type: sqltypes.VarChar}}}}},
			"main": {
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Columns: []*vschemapb.Column{{
							Name: "c1",
						}, {
							Name: "c2",
							Type: sqltypes.VarChar}}}}}}}
	vschema := BuildVSchema(&good)
	require.NoError(t, vschema.Keyspaces["unsharded"].Error)

	// add view to unsharded keyspace.
	vschema.AddView("unsharded", "v1", "SELECT c1+c2 AS added FROM t1")

	view := vschema.FindView("unsharded", "v1")
	assert.Equal(t, "select c1 + c2 as added from t1", sqlparser.String(view))

	view = vschema.FindView("", "v1")
	assert.Equal(t, "select c1 + c2 as added from t1", sqlparser.String(view))
}

func TestVSchemaColumnListAuthoritative(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Columns: []*vschemapb.Column{{
							Name: "c1"}, {
							Name: "c2",
							Type: sqltypes.VarChar}},
						ColumnListAuthoritative: true}}}}}

	got := BuildVSchema(&good)

	t1, err := got.FindTable("unsharded", "t1")
	require.NoError(t, err)
	assert.True(t, t1.ColumnListAuthoritative)
	assertColumn(t, t1.Columns[0], "c1", sqltypes.Null)
	assertColumn(t, t1.Columns[1], "c2", sqltypes.VarChar)
}

func TestVSchemaColumnsFail(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Columns: []*vschemapb.Column{{
							Name: "c1"}, {
							Name: "c1"}}}}}}}

	got := BuildVSchema(&good)
	require.EqualError(t, got.Keyspaces["unsharded"].Error, "duplicate column name 'c1' for table: t1")
}

func TestVSchemaPinned(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Pinned: "80"}}}}}

	got := BuildVSchema(&good)

	err := got.Keyspaces["sharded"].Error
	require.NoError(t, err)

	t1, err := got.FindTable("sharded", "t1")
	require.NoError(t, err)
	assert.Equal(t, "\x80", string(t1.Pinned))
}

func TestShardedVSchemaOwned(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu1": {
						Type: "stfu",
						Params: map[string]string{
							"stfu1": "1"},
						Owner: "t1"},
					"stln1": {
						Type:  "stln",
						Owner: "t1"}},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1"}, {
								Column: "c2",
								Name:   "stln1"}}}}}}}

	got := BuildVSchema(&good)
	err := got.Keyspaces["sharded"].Error
	require.NoError(t, err)

	t1, err := got.FindTable("sharded", "t1")
	require.NoError(t, err)

	vindex1 := &stFU{
		name: "stfu1",
		Params: map[string]string{
			"stfu1": "1"}}
	assertVindexMatches(t, t1.ColumnVindexes[0], vindex1, "stfu1", false)

	vindex2 := &stLN{name: "stln1"}
	assertVindexMatches(t, t1.ColumnVindexes[1], vindex2, "stln1", true)

	assert.Equal(t, []string{"stln1", "stfu1"}, vindexNames(t1.Ordered))
}

func TestShardedVSchemaOwnerInfo(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu": {
						Type: "stfu",
					},
					"stlo1": {
						Type:  "stlo",
						Owner: "t1",
					},
					"stlo2": {
						Type:  "stlo",
						Owner: "t2",
					},
					"stlo3": {
						Type:  "stlo",
						Owner: "none",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Column: "id",
							Name:   "stfu",
						}, {
							Column: "c1",
							Name:   "stlo1",
						}},
					},
					"t2": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Column: "id",
							Name:   "stfu",
						}, {
							Columns: []string{"c1", "c2"},
							Name:    "stlo2",
						}},
					},
					"t3": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Column: "id",
							Name:   "stfu",
						}, {
							Columns: []string{"c1", "c2"},
							Name:    "stlo3",
						}},
					},
				},
			},
		},
	}
	got := BuildVSchema(&good)
	err := got.Keyspaces["sharded"].Error
	require.NoError(t, err)
	results := []struct {
		name     string
		keyspace string
		table    string
		cols     []string
	}{{
		name:     "stlo1",
		keyspace: "sharded",
		table:    "t1",
		cols:     []string{"c1"},
	}, {
		name:     "stlo2",
		keyspace: "sharded",
		table:    "t2",
		cols:     []string{"c1", "c2"},
	}, {
		name:     "stlo3",
		keyspace: "",
		table:    "",
		cols:     nil,
	}}
	for _, want := range results {
		var gotcols []string
		vdx := got.Keyspaces["sharded"].Vindexes[want.name].(*stLO)
		if vdx.table != want.table {
			t.Errorf("Table(%s): %v, want %v", want.name, vdx.table, want.table)
		}
		if vdx.keyspace != want.keyspace {
			t.Errorf("Keyspace(%s): %v, want %v", want.name, vdx.table, want.keyspace)
		}
		for _, col := range vdx.cols {
			gotcols = append(gotcols, col.String())
		}
		if !reflect.DeepEqual(gotcols, want.cols) {
			t.Errorf("Cols(%s): %v, want %v", want.name, gotcols, want.cols)
		}
	}
}

func TestVSchemaRoutingRules(t *testing.T) {
	input := vschemapb.SrvVSchema{
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{{
				FromTable: "rt1",
				ToTables:  []string{"ks1.t1", "ks2.t2"},
			}, {
				FromTable: "rt2",
				ToTables:  []string{"ks2.t2"},
			}, {
				FromTable: "escaped",
				ToTables:  []string{"`ks2`.`t2`"},
			}, {
				FromTable: "dup",
				ToTables:  []string{"ks1.t1"},
			}, {
				FromTable: "dup",
				ToTables:  []string{"ks1.t1"},
			}, {
				FromTable: "badname",
				ToTables:  []string{"t1.t2.t3"},
			}, {
				FromTable: "unqualified",
				ToTables:  []string{"t1"},
			}, {
				FromTable: "badkeyspace",
				ToTables:  []string{"ks3.t1"},
			}, {
				FromTable: "notfound",
				ToTables:  []string{"ks1.t2"},
			}},
		},
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks1": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu1": {
						Type: "stfu",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
					},
				},
			},
			"ks2": {
				Tables: map[string]*vschemapb.Table{
					"t2": {},
				},
			},
		},
	}
	got := BuildVSchema(&input)
	ks1 := &Keyspace{
		Name:    "ks1",
		Sharded: true,
	}
	ks2 := &Keyspace{
		Name: "ks2",
	}
	vindex1 := &stFU{
		name: "stfu1",
	}
	t1 := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ks1,
		ColumnVindexes: []*ColumnVindex{{
			Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c1")},
			Type:     "stfu",
			Name:     "stfu1",
			Vindex:   vindex1,
			isUnique: vindex1.IsUnique(),
			cost:     vindex1.Cost(),
		}},
	}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[0],
	}
	t2 := &Table{
		Name:     sqlparser.NewIdentifierCS("t2"),
		Keyspace: ks2,
	}
	dual1 := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ks1,
		Type:     TypeReference,
	}
	dual2 := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ks2,
		Type:     TypeReference,
	}
	want := &VSchema{
		global: globalSchema{
			keyspaceNames: map[string]any{},
			tables: map[string]*Table{
				"t1":   t1,
				"t2":   t2,
				"dual": dual1,
			},
			vindexes: map[string]Vindex{
				"stfu1": vindex1,
			},
		},
		RoutingRules: map[string]*RoutingRule{
			"rt1": {
				Error: errors.New("table rt1 has more than one target: [ks1.t1 ks2.t2]"),
			},
			"rt2": {
				Tables: []*Table{t2},
			},
			"escaped": {
				Tables: []*Table{t2},
			},
			"dup": {
				Error: errors.New("duplicate rule for entry dup"),
			},
			"badname": {
				Error: errors.New("invalid table name: t1.t2.t3, it must be of the qualified form <keyspace_name>.<table_name> (dots are not allowed in either name)"),
			},
			"unqualified": {
				Error: errors.New("invalid table name: t1, it must be of the qualified form <keyspace_name>.<table_name> (dots are not allowed in either name)"),
			},
			"badkeyspace": {
				Error: errors.New("Unknown database 'ks3' in vschema"),
			},
			"notfound": {
				Error: errors.New("table t2 not found"),
			},
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"ks1": {
				Keyspace: ks1,
				Tables: map[string]*Table{
					"t1":   t1,
					"dual": dual1,
				},
				Vindexes: map[string]Vindex{
					"stfu1": vindex1,
				},
			},
			"ks2": {
				Keyspace: ks2,
				Tables: map[string]*Table{
					"t2":   t2,
					"dual": dual2,
				},
				Vindexes: map[string]Vindex{},
			},
		},
	}
	gotb, _ := json.MarshalIndent(got, "", "  ")
	wantb, _ := json.MarshalIndent(want, "", "  ")
	assert.Equal(t, string(wantb), string(gotb), string(gotb))
}

func TestChooseVindexForType(t *testing.T) {
	testcases := []struct {
		in  querypb.Type
		out string
	}{{
		in:  sqltypes.Null,
		out: "",
	}, {
		in:  sqltypes.Int8,
		out: "hash",
	}, {
		in:  sqltypes.Uint8,
		out: "hash",
	}, {
		in:  sqltypes.Int16,
		out: "hash",
	}, {
		in:  sqltypes.Uint16,
		out: "hash",
	}, {
		in:  sqltypes.Int24,
		out: "hash",
	}, {
		in:  sqltypes.Uint24,
		out: "hash",
	}, {
		in:  sqltypes.Int32,
		out: "hash",
	}, {
		in:  sqltypes.Uint32,
		out: "hash",
	}, {
		in:  sqltypes.Int64,
		out: "hash",
	}, {
		in:  sqltypes.Uint64,
		out: "hash",
	}, {
		in:  sqltypes.Float32,
		out: "hash",
	}, {
		in:  sqltypes.Float64,
		out: "",
	}, {
		in:  sqltypes.Timestamp,
		out: "",
	}, {
		in:  sqltypes.Date,
		out: "",
	}, {
		in:  sqltypes.Time,
		out: "",
	}, {
		in:  sqltypes.Datetime,
		out: "",
	}, {
		in:  sqltypes.Year,
		out: "hash",
	}, {
		in:  sqltypes.Decimal,
		out: "",
	}, {
		in:  sqltypes.Text,
		out: "unicode_loose_md5",
	}, {
		in:  sqltypes.Blob,
		out: "binary_md5",
	}, {
		in:  sqltypes.VarChar,
		out: "unicode_loose_md5",
	}, {
		in:  sqltypes.VarBinary,
		out: "binary_md5",
	}, {
		in:  sqltypes.Char,
		out: "unicode_loose_md5",
	}, {
		in:  sqltypes.Binary,
		out: "binary_md5",
	}, {
		in:  sqltypes.Bit,
		out: "",
	}, {
		in:  sqltypes.Enum,
		out: "",
	}, {
		in:  sqltypes.Set,
		out: "",
	}, {
		in:  sqltypes.Geometry,
		out: "",
	}, {
		in:  sqltypes.TypeJSON,
		out: "",
	}, {
		in:  sqltypes.Expression,
		out: "",
	}}

	for _, tcase := range testcases {
		out, err := ChooseVindexForType(tcase.in)
		if out == "" {
			assert.Error(t, err, tcase.in)
			continue
		}
		assert.Equal(t, out, tcase.out, tcase.in)
	}
}

func TestFindBestColVindex(t *testing.T) {
	testSrvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks1": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu": {
						Type: "stfu"},
					"stfn": {
						Type: "stfn"},
					"stlu": {
						Type: "stlu"},
					"stln": {
						Type: "stln"},
					"cheap": {
						Type: "cheap"},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "stfu",
							Columns: []string{"id"}}}},
					"nogoodvindex": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "stlu",
							Columns: []string{"id"}}}},
					"thirdvindexgood": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "stlu",
							Columns: []string{"id"}}, {
							Name:    "stfn",
							Columns: []string{"id"}}, {
							Name:    "stfu",
							Columns: []string{"id"}}}},
					"cheapest": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "stfu",
							Columns: []string{"id"}}, {
							Name:    "cheap",
							Columns: []string{"id"}}}}}},
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"t2": {}}}}}

	vs := BuildVSchema(testSrvVSchema)

	testcases := []struct {
		tablename  string
		vindexname string
		err        string
	}{{
		tablename:  "t1",
		vindexname: "stfu",
	}, {
		tablename: "nogoodvindex",
		err:       "could not find a vindex to compute keyspace id for table nogoodvindex",
	}, {
		tablename:  "thirdvindexgood",
		vindexname: "stfu",
	}, {
		tablename:  "cheapest",
		vindexname: "cheap",
	}, {
		tablename: "t2",
		err:       "table t2 has no vindex",
	}}
	for _, tcase := range testcases {
		table, err := vs.FindTable("", tcase.tablename)
		require.NoError(t, err)
		cv, err := FindBestColVindex(table)
		if err != nil {
			assert.EqualError(t, err, tcase.err, tcase.tablename)
			continue
		}
		assert.NoError(t, err, tcase.tablename)
		assert.Equal(t, cv.Name, tcase.vindexname, tcase.tablename)
	}
}

func TestFindVindexForSharding(t *testing.T) {
	ks := &Keyspace{
		Name:    "sharded",
		Sharded: true,
	}
	vindex1 := &stFU{
		name: "stfu1",
		Params: map[string]string{
			"stfu1": "1",
		},
	}
	vindex2 := &stLN{name: "stln1"}
	t1 := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ks,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c1")},
				Type:     "stfu",
				Name:     "stfu1",
				Vindex:   vindex1,
				isUnique: vindex1.IsUnique(),
				cost:     vindex1.Cost(),
			},
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c2")},
				Type:     "stln",
				Name:     "stln1",
				Owned:    true,
				Vindex:   vindex2,
				isUnique: vindex2.IsUnique(),
				cost:     vindex2.Cost(),
			},
		},
	}
	res, err := FindVindexForSharding(t1.Name.String(), t1.ColumnVindexes)
	require.NoError(t, err)
	if !reflect.DeepEqual(res, t1.ColumnVindexes[0]) {
		t.Errorf("FindVindexForSharding:\n got\n%v, want\n%v", res, t1.ColumnVindexes[0])
	}
}

func TestFindVindexForShardingError(t *testing.T) {
	ks := &Keyspace{
		Name:    "sharded",
		Sharded: true,
	}
	vindex1 := &stLU{name: "stlu1"}
	vindex2 := &stLN{name: "stln1"}
	t1 := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ks,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c1")},
				Type:     "stlu",
				Name:     "stlu1",
				Vindex:   vindex1,
				isUnique: vindex1.IsUnique(),
				cost:     vindex1.Cost(),
			},
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c2")},
				Type:     "stln",
				Name:     "stln1",
				Owned:    true,
				Vindex:   vindex2,
				isUnique: vindex2.IsUnique(),
				cost:     vindex2.Cost(),
			},
		},
	}
	res, err := FindVindexForSharding(t1.Name.String(), t1.ColumnVindexes)
	want := `could not find a vindex to use for sharding table t1`
	if err == nil || err.Error() != want {
		t.Errorf("FindVindexForSharding: %v, want %v", err, want)
	}
	if res != nil {
		t.Errorf("FindVindexForSharding:\n got\n%v, want\n%v", res, nil)
	}
}

func TestFindVindexForSharding2(t *testing.T) {
	ks := &Keyspace{
		Name:    "sharded",
		Sharded: true,
	}
	vindex1 := &stLU{name: "stlu1"}
	vindex2 := &stFU{
		name: "stfu1",
		Params: map[string]string{
			"stfu1": "1",
		},
	}
	t1 := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ks,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c1")},
				Type:     "stlu",
				Name:     "stlu1",
				Vindex:   vindex1,
				isUnique: vindex1.IsUnique(),
				cost:     vindex1.Cost(),
			},
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c2")},
				Type:     "stfu",
				Name:     "stfu1",
				Owned:    true,
				Vindex:   vindex2,
				isUnique: vindex2.IsUnique(),
				cost:     vindex2.Cost(),
			},
		},
	}
	res, err := FindVindexForSharding(t1.Name.String(), t1.ColumnVindexes)
	require.NoError(t, err)
	if !reflect.DeepEqual(res, t1.ColumnVindexes[1]) {
		t.Errorf("FindVindexForSharding:\n got\n%v, want\n%v", res, t1.ColumnVindexes[1])
	}
}

func TestShardedVSchemaMultiColumnVindex(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu1": {
						Type: "stfu",
						Params: map[string]string{
							"stfu1": "1"},
						Owner: "t1"}},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Columns: []string{"c1", "c2"},
							Name:    "stfu1"}}}}}}}

	got := BuildVSchema(&good)
	err := got.Keyspaces["sharded"].Error
	require.NoError(t, err)
	ks := &Keyspace{
		Name:    "sharded",
		Sharded: true,
	}
	vindex1 := &stFU{
		name: "stfu1",
		Params: map[string]string{
			"stfu1": "1",
		},
	}
	t1 := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ks,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c1"), sqlparser.NewIdentifierCI("c2")},
				Type:     "stfu",
				Name:     "stfu1",
				Vindex:   vindex1,
				isUnique: vindex1.IsUnique(),
				cost:     vindex1.Cost(),
			},
		},
	}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[0],
	}
	dual := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ks,
		Type:     TypeReference,
	}
	want := &VSchema{
		global: globalSchema{
			keyspaceNames: map[string]any{},
			tables: map[string]*Table{
				"t1":   t1,
				"dual": dual,
			},
			vindexes: map[string]Vindex{
				"stfu1": vindex1,
			},
		},
		RoutingRules: map[string]*RoutingRule{},
		Keyspaces: map[string]*KeyspaceSchema{
			"sharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1":   t1,
					"dual": dual},
				Vindexes: map[string]Vindex{
					"stfu1": vindex1},
			}}}
	if !reflect.DeepEqual(got, want) {
		gotjson, _ := json.Marshal(got)
		wantjson, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:\n%s, want\n%s", gotjson, wantjson)
	}
}

func TestShardedVSchemaNotOwned(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stlu1": {
						Type:  "stlu",
						Owner: ""},
					"stfu1": {
						Type:  "stfu",
						Owner: ""}},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Column: "c1",
							Name:   "stlu1"}, {
							Column: "c2",
							Name:   "stfu1"}}}}}}}
	got := BuildVSchema(&good)
	err := got.Keyspaces["sharded"].Error
	require.NoError(t, err)
	ks := &Keyspace{
		Name:    "sharded",
		Sharded: true,
	}
	vindex1 := &stLU{name: "stlu1"}
	vindex2 := &stFU{name: "stfu1"}
	t1 := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ks,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c1")},
				Type:     "stlu",
				Name:     "stlu1",
				Owned:    false,
				Vindex:   vindex1,
				isUnique: vindex1.IsUnique(),
				cost:     vindex1.Cost(),
			}, {
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c2")},
				Type:     "stfu",
				Name:     "stfu1",
				Owned:    false,
				Vindex:   vindex2,
				isUnique: vindex2.IsUnique(),
				cost:     vindex2.Cost()}}}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[1],
		t1.ColumnVindexes[0]}
	dual := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ks,
		Type:     TypeReference}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		global: globalSchema{
			keyspaceNames: map[string]any{},
			tables: map[string]*Table{
				"t1":   t1,
				"dual": dual,
			},
			vindexes: map[string]Vindex{
				"stlu1": vindex1,
				"stfu1": vindex2,
			},
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"sharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1":   t1,
					"dual": dual},
				Vindexes: map[string]Vindex{
					"stlu1": vindex1,
					"stfu1": vindex2},
			}}}
	if !reflect.DeepEqual(got, want) {
		gotjson, _ := json.Marshal(got)
		wantjson, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:s\n%s, want\n%s", gotjson, wantjson)
	}
}

func TestBuildVSchemaVindexNotFoundFail(t *testing.T) {
	bad := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"noexist": {
						Type: "noexist",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "noexist",
							},
						},
					},
				},
			},
		},
	}
	got := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := `vindexType "noexist" not found`
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaNoColumnVindexFail(t *testing.T) {
	bad := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu": {
						Type: "stfu",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {},
				},
			},
		},
	}
	got := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := "missing primary col vindex for table: t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaDupSeq(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ksa": {
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: "sequence"}}},
			"ksb": {
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: "sequence"}}}}}
	ksa := &Keyspace{
		Name: "ksa"}
	ksb := &Keyspace{
		Name: "ksb"}
	got := BuildVSchema(&good)
	t1a := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ksa,
		Type:     "sequence"}
	t1b := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ksb,
		Type:     "sequence"}
	duala := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ksa,
		Type:     TypeReference}
	dualb := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ksb,
		Type:     TypeReference}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		Keyspaces: map[string]*KeyspaceSchema{
			"ksa": {
				Keyspace: ksa,
				Tables: map[string]*Table{
					"t1":   t1a,
					"dual": duala},
				Vindexes: map[string]Vindex{},
			},
			"ksb": {
				Keyspace: ksb,
				Tables: map[string]*Table{
					"t1":   t1b,
					"dual": dualb},
				Vindexes: map[string]Vindex{}}},
		global: globalSchema{
			keyspaceNames: map[string]any{},
			tables: map[string]*Table{
				"t1":   nil,
				"dual": duala,
			},
			vindexes: map[string]Vindex{},
		}}
	if !reflect.DeepEqual(got, want) {
		gotjson, _ := json.Marshal(got)
		wantjson, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:\n%s, want\n%s", gotjson, wantjson)
	}
}

func TestBuildVSchemaDupTable(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ksa": {
				Tables: map[string]*vschemapb.Table{
					"t1": {},
				},
			},
			"ksb": {
				Tables: map[string]*vschemapb.Table{
					"t1": {},
				},
			},
		},
	}
	got := BuildVSchema(&good)
	ksa := &Keyspace{
		Name: "ksa",
	}
	t1a := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ksa,
	}
	ksb := &Keyspace{
		Name: "ksb",
	}
	t1b := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ksb,
	}
	duala := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ksa,
		Type:     TypeReference,
	}
	dualb := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ksb,
		Type:     TypeReference,
	}
	want := &VSchema{
		global: globalSchema{
			keyspaceNames: map[string]any{},
			tables: map[string]*Table{
				"t1":   nil,
				"dual": duala,
			},
			vindexes: map[string]Vindex{},
		},
		RoutingRules: map[string]*RoutingRule{},
		Keyspaces: map[string]*KeyspaceSchema{
			"ksa": {
				Keyspace: ksa,
				Tables: map[string]*Table{
					"t1":   t1a,
					"dual": duala,
				},
				Vindexes: map[string]Vindex{},
			},
			"ksb": {
				Keyspace: ksb,
				Tables: map[string]*Table{
					"t1":   t1b,
					"dual": dualb,
				},
				Vindexes: map[string]Vindex{},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		gotjson, _ := json.Marshal(got)
		wantjson, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:\n%s, want\n%s", gotjson, wantjson)
	}
}

func TestBuildVSchemaDupVindex(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ksa": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stlu1": {
						Type:  "stlu",
						Owner: "",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stlu1",
							},
						},
					},
				},
			},
			"ksb": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stlu1": {
						Type:  "stlu",
						Owner: "",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stlu1",
							},
						},
					},
				},
			},
		},
	}
	got := BuildVSchema(&good)
	err := got.Keyspaces["ksa"].Error
	err1 := got.Keyspaces["ksb"].Error
	require.NoError(t, err)
	if err1 != nil {
		t.Error(err1)
	}
	ksa := &Keyspace{
		Name:    "ksa",
		Sharded: true,
	}
	ksb := &Keyspace{
		Name:    "ksb",
		Sharded: true,
	}
	vindex1 := &stLU{name: "stlu1"}
	t1 := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ksa,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c1")},
				Type:     "stlu",
				Name:     "stlu1",
				Owned:    false,
				Vindex:   vindex1,
				isUnique: vindex1.IsUnique(),
				cost:     vindex1.Cost(),
			},
		},
	}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[0],
	}
	t2 := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ksb,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c1")},
				Type:     "stlu",
				Name:     "stlu1",
				Owned:    false,
				Vindex:   vindex1,
				isUnique: vindex1.IsUnique(),
				cost:     vindex1.Cost(),
			},
		},
	}
	t2.Ordered = []*ColumnVindex{
		t2.ColumnVindexes[0],
	}
	duala := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ksa,
		Type:     TypeReference,
	}
	dualb := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ksb,
		Type:     TypeReference,
	}
	want := &VSchema{
		global: globalSchema{
			keyspaceNames: map[string]any{},
			tables: map[string]*Table{
				"t1":   nil,
				"dual": duala,
			},
			vindexes: map[string]Vindex{
				"stlu1": nil,
			},
		},
		RoutingRules: map[string]*RoutingRule{},
		Keyspaces: map[string]*KeyspaceSchema{
			"ksa": {
				Keyspace: ksa,
				Tables: map[string]*Table{
					"t1":   t1,
					"dual": duala,
				},
				Vindexes: map[string]Vindex{
					"stlu1": vindex1,
				},
			},
			"ksb": {
				Keyspace: ksb,
				Tables: map[string]*Table{
					"t1":   t2,
					"dual": dualb,
				},
				Vindexes: map[string]Vindex{
					"stlu1": vindex1,
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		gotjson, _ := json.Marshal(got)
		wantjson, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:\n%s, want\n%s", gotjson, wantjson)
	}
}

func TestBuildVSchemaNoindexFail(t *testing.T) {
	bad := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu": {
						Type: "stfu",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "notexist",
							},
						},
					},
				},
			},
		},
	}
	got := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := "vindex notexist not found for table t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaColumnAndColumnsFail(t *testing.T) {
	bad := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu": {
						Type: "stfu",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column:  "c1",
								Columns: []string{"c2", "c3"},
								Name:    "stfu",
							},
						},
					},
				},
			},
		},
	}
	got := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := `can't use column and columns at the same time in vindex (stfu) and table (t1)`
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaNoColumnsFail(t *testing.T) {
	bad := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu": {
						Type: "stfu",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Name: "stfu",
							},
						},
					},
				},
			},
		},
	}
	got := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := `must specify at least one column for vindex (stfu) and table (t1)`
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaNotUniqueFail(t *testing.T) {
	bad := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stln": {
						Type: "stln",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stln",
							},
						},
					},
				},
			},
		},
	}
	got := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := "primary vindex stln is not Unique for table t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaPrimaryCannotBeOwned(t *testing.T) {
	bad := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stlu": {
						Type:  "stlu",
						Owner: "t1",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stlu",
							},
						},
					},
				},
			},
		},
	}
	got := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := "primary vindex stlu cannot be owned for table t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaReferenceTableSourceMustBeQualified(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"src": {},
				},
			},
			"sharded": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"ref": {
						Type:   "reference",
						Source: "src",
					},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	require.NoError(t, vschema.Keyspaces["unsharded"].Error)
	require.Error(t, vschema.Keyspaces["sharded"].Error)
	require.EqualError(t, vschema.Keyspaces["sharded"].Error,
		"invalid source \"src\" for reference table: ref; invalid table name: src, it must be of the qualified form <keyspace_name>.<table_name> (dots are not allowed in either name)")
}

func TestBuildVSchemaReferenceTableSourceMustBeInDifferentKeyspace(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "binary_md5",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"ref": {
						Type:   "reference",
						Source: "sharded.src",
					},
					"src": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "hash",
							},
						},
					},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	require.Error(t, vschema.Keyspaces["sharded"].Error)
	require.EqualError(t, vschema.Keyspaces["sharded"].Error,
		"source \"sharded.src\" may not reference a table in the same keyspace as table: ref")
}

func TestBuildVSchemaReferenceTableSourceKeyspaceMustExist(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"ref": {
						Type:   "reference",
						Source: "unsharded.src",
					},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	require.Error(t, vschema.Keyspaces["sharded"].Error)
	require.EqualError(t, vschema.Keyspaces["sharded"].Error,
		"source \"unsharded.src\" references a non-existent keyspace \"unsharded\"")
}

func TestBuildVSchemaReferenceTableSourceTableMustExist(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"foo": {},
				},
			},
			"sharded": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"ref": {
						Type:   "reference",
						Source: "unsharded.src",
					},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	require.Error(t, vschema.Keyspaces["sharded"].Error)
	require.EqualError(t, vschema.Keyspaces["sharded"].Error,
		"source \"unsharded.src\" references a table \"src\" that is not present in the VSchema of keyspace \"unsharded\"")
}

func TestBuildVSchemaReferenceTableSourceMayUseShardedKeyspace(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded1": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "binary_md5",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"src": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "hash",
							},
						},
					},
				},
			},
			"sharded2": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"ref": {
						Type:   "reference",
						Source: "sharded1.src",
					},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	require.NoError(t, vschema.Keyspaces["sharded1"].Error)
	require.NoError(t, vschema.Keyspaces["sharded2"].Error)
}

func TestBuildVSchemaReferenceTableSourceTableMustBeBasicOrReference(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded1": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"src1": {
						Type: "sequence",
					},
				},
			},
			"unsharded2": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"src2": {
						Type:   "reference",
						Source: "unsharded1.src1",
					},
				},
			},
			"unsharded3": {
				Tables: map[string]*vschemapb.Table{
					"src3": {
						Type: "reference",
					},
				},
			},
			"sharded1": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"ref1": {
						Type:   "reference",
						Source: "unsharded1.src1",
					},
				},
			},
			"sharded2": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"ref2": {
						Type:   "reference",
						Source: "unsharded3.src3",
					},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	require.Error(t, vschema.Keyspaces["sharded1"].Error)
	require.EqualError(t, vschema.Keyspaces["sharded1"].Error,
		"source \"unsharded1.src1\" may not reference a table of type \"sequence\": ref1")
	require.NoError(t, vschema.Keyspaces["sharded2"].Error)
}

func TestBuildVSchemaSourceMayBeReferencedAtMostOncePerKeyspace(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"src": {},
				},
			},
			"sharded": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"ref2": {
						Type:   "reference",
						Source: "unsharded.src",
					},
					"ref1": {
						Type:   "reference",
						Source: "unsharded.src",
					},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	require.Error(t, vschema.Keyspaces["sharded"].Error)
	require.EqualError(t, vschema.Keyspaces["sharded"].Error,
		"source \"unsharded.src\" may not be referenced more than once per keyspace: ref1, ref2")
}

func TestBuildVSchemaMayNotChainReferences(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded1": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"ref": {
						Type:   TypeReference,
						Source: "unsharded2.ref",
					},
				},
			},
			"unsharded2": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"ref": {
						Type:   TypeReference,
						Source: "unsharded3.ref",
					},
				},
			},
			"unsharded3": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"ref": {
						Type:   "reference",
						Source: "unsharded1.ref",
					},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	require.Error(t, vschema.Keyspaces["unsharded1"].Error)
	require.EqualError(t, vschema.Keyspaces["unsharded1"].Error,
		"reference chaining is not allowed ref => unsharded2.ref => unsharded3.ref: ref")
}

func TestSequence(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"seq": {
						Type: "sequence",
					},
				},
			},
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu1": {
						Type: "stfu",
						Params: map[string]string{
							"stfu1": "1",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
						AutoIncrement: &vschemapb.AutoIncrement{
							Column:   "c1",
							Sequence: "seq",
						},
					},
					"t2": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
						AutoIncrement: &vschemapb.AutoIncrement{
							Column:   "c2",
							Sequence: "`unsharded`.`seq`",
						},
					},
				},
			},
		},
	}
	got := BuildVSchema(&good)
	err := got.Keyspaces["sharded"].Error
	require.NoError(t, err)
	err1 := got.Keyspaces["unsharded"].Error
	if err1 != nil {
		t.Error(err1)
	}
	ksu := &Keyspace{
		Name: "unsharded",
	}
	kss := &Keyspace{
		Name:    "sharded",
		Sharded: true,
	}
	seq := &Table{
		Name:     sqlparser.NewIdentifierCS("seq"),
		Keyspace: ksu,
		Type:     "sequence",
	}
	vindex1 := &stFU{
		name: "stfu1",
		Params: map[string]string{
			"stfu1": "1",
		},
	}
	t1 := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: kss,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c1")},
				Type:     "stfu",
				Name:     "stfu1",
				Vindex:   vindex1,
				isUnique: vindex1.IsUnique(),
				cost:     vindex1.Cost(),
			},
		},
		AutoIncrement: &AutoIncrement{
			Column:   sqlparser.NewIdentifierCI("c1"),
			Sequence: seq,
		},
	}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[0],
	}
	t2 := &Table{
		Name:     sqlparser.NewIdentifierCS("t2"),
		Keyspace: kss,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns:  []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("c1")},
				Type:     "stfu",
				Name:     "stfu1",
				Vindex:   vindex1,
				isUnique: vindex1.IsUnique(),
				cost:     vindex1.Cost(),
			},
		},
		AutoIncrement: &AutoIncrement{
			Column:   sqlparser.NewIdentifierCI("c2"),
			Sequence: seq,
		},
	}
	t2.Ordered = []*ColumnVindex{
		t2.ColumnVindexes[0],
	}
	duala := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ksu,
		Type:     TypeReference,
	}
	dualb := &Table{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: kss,
		Type:     TypeReference,
	}
	want := &VSchema{
		global: globalSchema{
			keyspaceNames: map[string]any{},
			tables: map[string]*Table{
				"seq":  seq,
				"t1":   t1,
				"t2":   t2,
				"dual": dualb,
			},
			vindexes: map[string]Vindex{
				"stfu1": vindex1,
			},
		},
		RoutingRules: map[string]*RoutingRule{},
		Keyspaces: map[string]*KeyspaceSchema{
			"unsharded": {
				Keyspace: ksu,
				Tables: map[string]*Table{
					"seq":  seq,
					"dual": duala,
				},
				Vindexes: map[string]Vindex{},
			},
			"sharded": {
				Keyspace: kss,
				Tables: map[string]*Table{
					"t1":   t1,
					"t2":   t2,
					"dual": dualb,
				},
				Vindexes: map[string]Vindex{
					"stfu1": vindex1,
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		gotjson, _ := json.Marshal(got)
		wantjson, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:\n%s, want\n%s", gotjson, wantjson)
	}
}

func TestBadSequence(t *testing.T) {
	bad := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"valid_seq": {
						Type: "sequence",
					},
				},
			},
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu1": {
						Type: "stfu",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
						AutoIncrement: &vschemapb.AutoIncrement{
							Column:   "c1",
							Sequence: "invalid_seq",
						},
					},
					"t2": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
						AutoIncrement: &vschemapb.AutoIncrement{
							Column:   "c1",
							Sequence: "valid_seq",
						},
					},
				},
			},
		},
	}
	got := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := "cannot resolve sequence invalid_seq: table invalid_seq not found"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
	if t1 := got.Keyspaces["sharded"].Tables["t1"]; t1 != nil {
		t.Errorf("BuildVSchema: table t1 must not be present in the keyspace: %v", t1)
	}

	// Verify that a failure to set up a sequence for t1 doesn't prevent setting up
	// a sequence for t2.
	t2Seq := got.Keyspaces["sharded"].Tables["t2"].AutoIncrement.Sequence
	if t2Seq.Name.String() != "valid_seq" {
		t.Errorf("BuildVSchema: unexpected t2 sequence name. Got: %v. Want: %v",
			t2Seq.AutoIncrement.Sequence.Name,
			"valid_seq",
		)
	}
}

func TestBadSequenceName(t *testing.T) {
	bad := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu1": {
						Type: "stfu",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
						AutoIncrement: &vschemapb.AutoIncrement{
							Column:   "c1",
							Sequence: "a.b.seq",
						},
					},
				},
			},
		},
	}
	got := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := "invalid table name: a.b.seq"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("BuildVSchema: %v, must contain %v", err, want)
	}
	if t1 := got.Keyspaces["sharded"].Tables["t1"]; t1 != nil {
		t.Errorf("BuildVSchema: table t1 must not be present in the keyspace: %v", t1)
	}
}

func TestBadShardedSequence(t *testing.T) {
	bad := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: "sequence",
					},
				},
			},
		},
	}
	got := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := "sequence table has to be in an unsharded keyspace or must be pinned: t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestFindTable(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ksa": {
				Tables: map[string]*vschemapb.Table{
					"ta": {},
					"t1": {},
					"t2": {},
				},
			},
			"ksb": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu1": {
						Type: "stfu",
						Params: map[string]string{
							"stfu1": "1",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"tb": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
					},
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
					},
					"t2": {
						Type:   "reference",
						Source: "ksa.t2",
					},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	_, err := vschema.FindTable("", "t1")
	require.EqualError(t, err, "ambiguous table reference: t1")

	_, err = vschema.FindTable("", "none")
	require.EqualError(t, err, "table none not found")

	ta := &Table{
		Name: sqlparser.NewIdentifierCS("ta"),
		Keyspace: &Keyspace{
			Name: "ksa",
		},
	}
	got, err := vschema.FindTable("", "ta")
	require.NoError(t, err)
	require.Equal(t, ta, got)

	t2 := &Table{
		Name: sqlparser.NewIdentifierCS("t2"),
		Keyspace: &Keyspace{
			Name: "ksa",
		},
		ReferencedBy: map[string]*Table{
			"ksb": {
				Type: "reference",
				Name: sqlparser.NewIdentifierCS("t2"),
				Keyspace: &Keyspace{
					Sharded: true,
					Name:    "ksb",
				},
				Source: &Source{
					sqlparser.TableName{
						Qualifier: sqlparser.NewIdentifierCS("ksa"),
						Name:      sqlparser.NewIdentifierCS("t2"),
					},
				},
			},
		},
	}
	got, err = vschema.FindTable("", "t2")
	require.NoError(t, err)
	require.Equal(t, t2, got)

	got, _ = vschema.FindTable("ksa", "ta")
	require.Equal(t, ta, got)

	none := &Table{
		Name: sqlparser.NewIdentifierCS("none"),
		Keyspace: &Keyspace{
			Name: "ksa",
		},
	}
	got, _ = vschema.FindTable("ksa", "none")
	require.Equal(t, none, got)

	_, err = vschema.FindTable("ksb", "none")
	require.EqualError(t, err, "table none not found")

	_, err = vschema.FindTable("none", "aa")
	require.EqualError(t, err, "Unknown database 'none' in vschema")
}

func TestFindTableOrVindex(t *testing.T) {
	input := vschemapb.SrvVSchema{
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{{
				FromTable: "unqualified",
				ToTables:  []string{"ksa.ta"},
			}, {
				FromTable: "unqualified@replica",
				ToTables:  []string{"ksb.t1"},
			}, {
				FromTable: "newks.qualified",
				ToTables:  []string{"ksa.ta"},
			}, {
				FromTable: "newks.qualified@replica",
				ToTables:  []string{"ksb.t1"},
			}, {
				FromTable: "notarget",
				ToTables:  []string{},
			}},
		},
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ksa": {
				Tables: map[string]*vschemapb.Table{
					"ta": {},
					"t1": {},
				},
			},
			"ksb": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu1": {
						Type: "stfu",
					},
					"dup": {
						Type: "stfu",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
					},
				},
			},
			"ksc": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"dup": {
						Type: "stfu",
					},
					"ta": {
						Type: "stfu",
					},
				},
				Tables: map[string]*vschemapb.Table{},
			},
		},
	}
	vschema := BuildVSchema(&input)
	ta := vschema.Keyspaces["ksa"].Tables["ta"]
	t1 := vschema.Keyspaces["ksb"].Tables["t1"]

	_, _, err := vschema.FindTableOrVindex("", "t1", topodatapb.TabletType_PRIMARY)
	wantErr := "ambiguous table reference: t1"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTableOrVindex(\"\"): %v, want %s", err, wantErr)
	}

	_, _, err = vschema.FindTableOrVindex("", "none", topodatapb.TabletType_PRIMARY)
	wantErr = "table none not found"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTableOrVindex(\"\"): %v, want %s", err, wantErr)
	}

	got, _, err := vschema.FindTableOrVindex("", "ta", topodatapb.TabletType_PRIMARY)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, ta) {
		t.Errorf("FindTableOrVindex(\"t1a\"): %+v, want %+v", got, ta)
	}

	_, vindex, err := vschema.FindTableOrVindex("", "stfu1", topodatapb.TabletType_PRIMARY)
	if err != nil {
		t.Fatal(err)
	}
	wantVindex := &stFU{
		name: "stfu1",
	}
	if !reflect.DeepEqual(vindex, wantVindex) {
		t.Errorf("FindTableOrVindex(\"stfu1\"): %+v, want %+v", vindex, wantVindex)
	}

	_, vindex, err = vschema.FindTableOrVindex("ksc", "ta", topodatapb.TabletType_PRIMARY)
	if err != nil {
		t.Fatal(err)
	}
	wantVindex = &stFU{
		name: "ta",
	}
	if !reflect.DeepEqual(vindex, wantVindex) {
		t.Errorf("FindTableOrVindex(\"stfu1\"): %+v, want %+v", vindex, wantVindex)
	}

	_, _, err = vschema.FindTableOrVindex("", "dup", topodatapb.TabletType_PRIMARY)
	wantErr = "ambiguous vindex reference: dup"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTableOrVindex(\"\"): %v, want %s", err, wantErr)
	}

	got, _, err = vschema.FindTableOrVindex("", "unqualified", topodatapb.TabletType_PRIMARY)
	if err != nil {
		t.Fatal(err)
	}
	if want := ta; !reflect.DeepEqual(got, want) {
		t.Errorf("FindTableOrVindex(unqualified): %+v, want %+v", got, want)
	}

	got, _, err = vschema.FindTableOrVindex("", "unqualified", topodatapb.TabletType_REPLICA)
	if err != nil {
		t.Fatal(err)
	}
	if want := t1; !reflect.DeepEqual(got, want) {
		t.Errorf("FindTableOrVindex(unqualified): %+v, want %+v", got, want)
	}

	got, _, err = vschema.FindTableOrVindex("newks", "qualified", topodatapb.TabletType_PRIMARY)
	if err != nil {
		t.Fatal(err)
	}
	if want := ta; !reflect.DeepEqual(got, want) {
		t.Errorf("FindTableOrVindex(unqualified): %+v, want %+v", got, want)
	}

	got, _, err = vschema.FindTableOrVindex("newks", "qualified", topodatapb.TabletType_REPLICA)
	if err != nil {
		t.Fatal(err)
	}
	if want := t1; !reflect.DeepEqual(got, want) {
		t.Errorf("FindTableOrVindex(unqualified): %+v, want %+v", got, want)
	}

	_, _, err = vschema.FindTableOrVindex("", "notarget", topodatapb.TabletType_PRIMARY)
	wantErr = "table notarget has been disabled"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTableOrVindex(\"\"): %v, want %s", err, wantErr)
	}
}

func TestBuildKeyspaceSchema(t *testing.T) {
	good := &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"t1": {
				AutoIncrement: &vschemapb.AutoIncrement{
					Column:   "col",
					Sequence: "outside",
				},
			},
			"t2": {},
		},
	}
	got, _ := BuildKeyspaceSchema(good, "ks")
	err := got.Error
	require.NoError(t, err)
	ks := &Keyspace{
		Name: "ks",
	}
	t1 := &Table{
		Name:     sqlparser.NewIdentifierCS("t1"),
		Keyspace: ks,
	}
	t2 := &Table{
		Name:     sqlparser.NewIdentifierCS("t2"),
		Keyspace: ks,
	}
	want := &KeyspaceSchema{
		Keyspace: ks,
		Tables: map[string]*Table{
			"t1": t1,
			"t2": t2,
		},
		Vindexes: map[string]Vindex{},
	}
	if !reflect.DeepEqual(got, want) {
		gs, _ := json.Marshal(got)
		ws, _ := json.Marshal(want)
		t.Errorf("BuildKeyspaceSchema:\n%s, want\n%s", gs, ws)
	}
}

func TestValidate(t *testing.T) {
	good := &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"t1": {
				AutoIncrement: &vschemapb.AutoIncrement{
					Column:   "col",
					Sequence: "outside",
				},
			},
			"t2": {},
		},
	}
	err := ValidateKeyspace(good)
	require.NoError(t, err)
	bad := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "absent",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t2": {},
		},
	}
	err = ValidateKeyspace(bad)
	want := `vindexType "absent" not found`
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Validate: %v, must start with %s", err, want)
	}
}

func TestVSchemaPBJSON(t *testing.T) {
	in := `
	{
		"sharded": true,
		"tables": {
			"t1": {
				"column_vindexes":[{
						"column":"c1",
						"name":"stfu1"
					},{
						"column":"c2",
						"name":"stln1"
					}],
				"auto_increment": {
					"column": "col",
					"sequence": "outside"
				}
			},
			"t2": {
				"columns":[{
					"name": "c1",
					"type": "VARCHAR"
				}]
			}
		}
	}
`
	var got vschemapb.Keyspace
	if err := json2.Unmarshal([]byte(in), &got); err != nil {
		t.Error(err)
	}
	want := vschemapb.Keyspace{
		Sharded: true,
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{
					{
						Column: "c1",
						Name:   "stfu1",
					}, {
						Column: "c2",
						Name:   "stln1",
					},
				},
				AutoIncrement: &vschemapb.AutoIncrement{
					Column:   "col",
					Sequence: "outside",
				},
			},
			"t2": {
				Columns: []*vschemapb.Column{{
					Name: "c1",
					Type: querypb.Type_VARCHAR,
				}},
			},
		},
	}
	if !proto.Equal(&got, &want) {
		gs, _ := json2.MarshalPB(&got)
		ws, _ := json2.MarshalPB(&want)
		t.Errorf("vschemapb.SrvVSchemaForKeyspace():\n%s, want\n%s", gs, ws)
	}
}

func TestVSchemaJSON(t *testing.T) {
	lkp, _ := NewLookupHash("n2", map[string]string{
		"from":  "f",
		"table": "t",
		"to":    "2",
	})
	in := map[string]*KeyspaceSchema{
		"unsharded": {
			Keyspace: &Keyspace{
				Name: "k1",
			},
			Tables: map[string]*Table{
				"t1": {
					Name: sqlparser.NewIdentifierCS("n1"),
					Columns: []Column{{
						Name: sqlparser.NewIdentifierCI("c1"),
					}, {
						Name: sqlparser.NewIdentifierCI("c2"),
						Type: sqltypes.VarChar,
					}},
				},
				"t2": {
					Type: "sequence",
					Name: sqlparser.NewIdentifierCS("n2"),
				},
			},
		},
		"sharded": {
			Keyspace: &Keyspace{
				Name:    "k2",
				Sharded: true,
			},
			Tables: map[string]*Table{
				"t3": {
					Name: sqlparser.NewIdentifierCS("n3"),
					ColumnVindexes: []*ColumnVindex{{
						Columns: []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("aa")},
						Type:    "vtype",
						Name:    "vname",
						Owned:   true,
						Vindex:  lkp,
					}},
				},
			},
		},
	}
	out, err := json.MarshalIndent(in, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)
	want := `{
  "sharded": {
    "sharded": true,
    "tables": {
      "t3": {
        "name": "n3",
        "column_vindexes": [
          {
            "columns": [
              "aa"
            ],
            "type": "vtype",
            "name": "vname",
            "owned": true,
            "vindex": {
              "table": "t",
              "from_columns": [
                "f"
              ],
              "to": "2"
            }
          }
        ]
      }
    }
  },
  "unsharded": {
    "tables": {
      "t1": {
        "name": "n1",
        "columns": [
          {
            "name": "c1",
            "type": "NULL_TYPE"
          },
          {
            "name": "c2",
            "type": "VARCHAR"
          }
        ]
      },
      "t2": {
        "type": "sequence",
        "name": "n2"
      }
    }
  }
}`
	if got != want {
		t.Errorf("json.Marshal():\n%s, want\n%s", got, want)
	}
}

func TestFindSingleKeyspace(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ksa": {
				Tables: map[string]*vschemapb.Table{
					"ta": {},
					"t1": {},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	none := &Table{
		Name: sqlparser.NewIdentifierCS("none"),
		Keyspace: &Keyspace{
			Name: "ksa",
		},
	}
	got, _ := vschema.FindTable("", "none")
	if !reflect.DeepEqual(got, none) {
		t.Errorf("FindTable(\"t1a\"): %+v, want %+v", got, none)
	}
	input = vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ksb": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu1": {
						Type: "stfu",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"tb": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
					},
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stfu1",
							},
						},
					},
				},
			},
		},
	}
	vschema = BuildVSchema(&input)
	_, err := vschema.FindTable("", "none")
	wantErr := "table none not found"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTable(\"\"): %v, want %s", err, wantErr)
	}
}

func TestMultiColVindexPartialAllowed(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ksa": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"regional_vdx": {
						Type: "region_experimental_test",
						Params: map[string]string{
							"region_bytes": "1",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"user_region": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Columns: []string{"cola", "colb"},
								Name:    "regional_vdx",
							},
						},
					},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	table, err := vschema.FindTable("ksa", "user_region")
	require.NoError(t, err)
	require.Len(t, table.ColumnVindexes, 2)
	require.True(t, table.ColumnVindexes[0].IsUnique())
	require.False(t, table.ColumnVindexes[1].IsUnique())
	require.EqualValues(t, 1, table.ColumnVindexes[0].Cost())
	require.EqualValues(t, 2, table.ColumnVindexes[1].Cost())
}

func TestMultiColVindexPartialNotAllowed(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ksa": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"multicol_vdx": {
						Type: "mcfu",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"multiColTbl": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Columns: []string{"cola", "colb", "colc"},
								Name:    "multicol_vdx",
							},
						},
					},
				},
			},
		},
	}
	vschema := BuildVSchema(&input)
	table, err := vschema.FindTable("ksa", "multiColTbl")
	require.NoError(t, err)
	require.Len(t, table.ColumnVindexes, 1)
	require.True(t, table.ColumnVindexes[0].IsUnique())
	require.EqualValues(t, 1, table.ColumnVindexes[0].Cost())
}

func TestSourceTableHasReferencedBy(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"src": {},
				},
			},
			"sharded1": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"ref": {
						Type:   "reference",
						Source: "unsharded.src",
					},
				},
			},
			"sharded2": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"ref": {
						Type:   "reference",
						Source: "unsharded.src",
					},
				},
			},
		},
	}
	vs := BuildVSchema(&input)
	ref1, err := vs.FindTable("sharded1", "ref")
	require.NoError(t, err)
	ref2, err := vs.FindTable("sharded2", "ref")
	require.NoError(t, err)
	src, err := vs.FindTable("unsharded", "src")
	require.NoError(t, err)
	require.Equal(t, src.ReferencedBy, map[string]*Table{
		"sharded1": ref1,
		"sharded2": ref2,
	})
}

func TestReferenceTableAndSourceAreGloballyRoutable(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"t1": {},
				},
			},
			"sharded": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type:   "reference",
						Source: "unsharded.t1",
					},
				},
			},
		},
	}
	vs := BuildVSchema(&input)
	t1, err := vs.FindTable("unsharded", "t1")
	require.NoError(t, err)
	globalT1, err := vs.FindTable("", "t1")
	require.NoError(t, err)
	require.Equal(t, t1, globalT1)

	input.Keyspaces["unsharded"].RequireExplicitRouting = true
	vs = BuildVSchema(&input)
	t1, err = vs.FindTable("sharded", "t1")
	require.NoError(t, err)
	globalT1, err = vs.FindTable("", "t1")
	require.NoError(t, err)
	require.Equal(t, t1, globalT1)
}

func TestOtherTablesMakeReferenceTableAndSourceAmbiguous(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded1": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"t1": {},
				},
			},
			"unsharded2": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"t1": {},
				},
			},
			"sharded": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type:   "reference",
						Source: "unsharded1.t1",
					},
				},
			},
		},
	}
	vs := BuildVSchema(&input)
	_, err := vs.FindTable("", "t1")
	require.Error(t, err)
}

func TestUseGlobalKeyspaceNameToFindTables(t *testing.T) {
	input := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Sharded: false,
				Tables: map[string]*vschemapb.Table{
					"t1": {},
					"t2": {},
				},
			},
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "binary_md5",
					},
					"stfu1": {
						Type: "stfu",
						Params: map[string]string{
							"stfu1": "1",
						},
						Owner: "t3",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type:   "reference",
						Source: "unsharded.t1",
					},
					"t3": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "hash",
							},
							{
								Column: "c2",
								Name:   "stfu1",
							},
						},
					},
				},
			},
		},
	}

	builder := NewVSchemaBuilder([]string{"unsharded", "vt_global", "sharded"})
	vs := builder.BuildVSchema(&input)

	for _, ks := range vs.Keyspaces {
		require.NoError(t, ks.Error)
	}

	// Verify conflicting global keyspace name `unsharded` cannot be used to
	// globally route to keyspaces.
	t1, err := vs.FindTable("unsharded", "t1")
	require.NoError(t, err)
	require.Equal(t, "t1", t1.Name.String())
	require.Equal(t, "unsharded", t1.Keyspace.Name)
	require.Nil(t, t1.Source)
	require.Equal(t, "", t1.Type)

	t2, err := vs.FindTable("unsharded", "t2")
	require.NoError(t, err)
	require.Equal(t, "t2", t2.Name.String())
	require.Equal(t, "unsharded", t2.Keyspace.Name)

	// This isn't global routing, it's expected behavior for unsharded
	// keyspaces.
	t3, err := vs.FindTable("unsharded", "t3")
	require.NoError(t, err)
	require.Equal(t, "t3", t3.Name.String())
	require.Equal(t, "unsharded", t3.Keyspace.Name)

	// Verify `vt_global` can be used to find all globally routable keyspaces.
	t1, err = vs.FindTable("vt_global", "t1")
	require.NoError(t, err)
	require.Equal(t, "t1", t1.Name.String())
	require.Equal(t, "unsharded", t1.Keyspace.Name)

	t2, err = vs.FindTable("vt_global", "t2")
	require.NoError(t, err)
	require.Equal(t, "t2", t2.Name.String())
	require.Equal(t, "unsharded", t2.Keyspace.Name)

	t3, err = vs.FindTable("vt_global", "t3")
	require.NoError(t, err)
	require.Equal(t, "t3", t3.Name.String())
	require.Equal(t, "sharded", t3.Keyspace.Name)

	// Verify conflicting global keyspace name `sharded` cannot be used to
	// globally route to keyspaces.
	t1, err = vs.FindTable("sharded", "t1")
	require.NoError(t, err)
	require.Equal(t, "t1", t1.Name.String())
	require.Equal(t, "sharded", t1.Keyspace.Name)
	require.NotNil(t, t1.Source)
	require.Equal(t, "unsharded.t1", t1.Source.String())
	require.Equal(t, TypeReference, t1.Type)

	t2, err = vs.FindTable("sharded", "t2")
	require.Error(t, err)
	require.Nil(t, t2)

	t3, err = vs.FindTable("vt_global", "t3")
	require.NoError(t, err)
	require.Equal(t, "t3", t3.Name.String())
	require.Equal(t, "sharded", t3.Keyspace.Name)

	// Verify FindTableOrVindex works as expected.
	t1, v1, err := vs.FindTableOrVindex("vt_global", "t1", topodatapb.TabletType_PRIMARY)
	require.Nil(t, err)
	require.Nil(t, v1)
	require.Equal(t, "t1", t1.Name.String())
	require.Equal(t, "unsharded", t1.Keyspace.Name)

	t1, v1, err = vs.FindTableOrVindex("sharded", "t1", topodatapb.TabletType_PRIMARY)
	require.Nil(t, err)
	require.Nil(t, v1)
	require.Equal(t, "t1", t1.Name.String())
	require.Equal(t, "sharded", t1.Keyspace.Name)

	t2, v2, err := vs.FindTableOrVindex("vt_global", "t2", topodatapb.TabletType_PRIMARY)
	require.Nil(t, err)
	require.Nil(t, v2)
	require.Equal(t, "t2", t2.Name.String())
	require.Equal(t, "unsharded", t2.Keyspace.Name)

	t3, v3, err := vs.FindTableOrVindex("vt_global", "t3", topodatapb.TabletType_PRIMARY)
	require.Nil(t, err)
	require.Nil(t, v3)
	require.Equal(t, "t3", t3.Name.String())
	require.Equal(t, "sharded", t3.Keyspace.Name)

	// This isn't global routing, it's expected behavior for unsharded
	// keyspaces.
	t3, v3, err = vs.FindTableOrVindex("unsharded", "t3", topodatapb.TabletType_PRIMARY)
	require.NoError(t, err)
	require.Nil(t, v3)
	require.Equal(t, "t3", t3.Name.String())
	require.Equal(t, "unsharded", t3.Keyspace.Name)

	tstfu1, vstfu1, err := vs.FindTableOrVindex("vt_global", "stfu1", topodatapb.TabletType_PRIMARY)
	require.Nil(t, err)
	require.Nil(t, tstfu1)
	require.NotNil(t, vstfu1)
}

func vindexNames(vindexes []*ColumnVindex) (result []string) {
	for _, vindex := range vindexes {
		result = append(result, vindex.Name)
	}
	return
}

func assertVindexMatches(t *testing.T, cv *ColumnVindex, v Vindex, name string, owned bool) {
	utils.MustMatch(t, v, cv.Vindex)
	assert.Equal(t, name, cv.Name)
	assert.Equal(t, v.Cost(), cv.Cost(), "cost not correct")
	assert.Equal(t, v.IsUnique(), cv.IsUnique(), "isUnique not correct")
	assert.Equal(t, owned, cv.Owned, "owned is not correct")
}

func assertColumn(t *testing.T, col Column, expectedName string, expectedType querypb.Type) {
	assert.True(t, col.Name.EqualString(expectedName), "column name does not match")
	assert.Equal(t, expectedType, col.Type, "column type does not match")

}
