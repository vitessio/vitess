/*
Copyright 2017 Google Inc.

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
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// stFU is a Functional, Unique Vindex.
type stFU struct {
	name   string
	Params map[string]string
}

func (v *stFU) String() string                                                    { return v.name }
func (*stFU) Cost() int                                                           { return 1 }
func (*stFU) IsUnique() bool                                                      { return true }
func (*stFU) IsFunctional() bool                                                  { return true }
func (*stFU) Verify(VCursor, []sqltypes.Value, [][]byte) ([]bool, error)          { return []bool{}, nil }
func (*stFU) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) { return nil, nil }

func NewSTFU(name string, params map[string]string) (Vindex, error) {
	return &stFU{name: name, Params: params}, nil
}

var _ Vindex = (*stFU)(nil)

// stLN is a Lookup, NonUnique Vindex.
type stLN struct {
	name   string
	Params map[string]string
}

func (v *stLN) String() string                                                    { return v.name }
func (*stLN) Cost() int                                                           { return 0 }
func (*stLN) IsUnique() bool                                                      { return false }
func (*stLN) IsFunctional() bool                                                  { return false }
func (*stLN) Verify(VCursor, []sqltypes.Value, [][]byte) ([]bool, error)          { return []bool{}, nil }
func (*stLN) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) { return nil, nil }
func (*stLN) Create(VCursor, [][]sqltypes.Value, [][]byte, bool) error            { return nil }
func (*stLN) Delete(VCursor, [][]sqltypes.Value, []byte) error                    { return nil }
func (*stLN) Update(VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error    { return nil }

func NewSTLN(name string, params map[string]string) (Vindex, error) {
	return &stLN{name: name, Params: params}, nil
}

var _ Vindex = (*stLN)(nil)
var _ Lookup = (*stLN)(nil)

// stLU is a Lookup, Unique Vindex.
type stLU struct {
	name   string
	Params map[string]string
}

func (v *stLU) String() string                                                    { return v.name }
func (*stLU) Cost() int                                                           { return 2 }
func (*stLU) IsUnique() bool                                                      { return true }
func (*stLU) IsFunctional() bool                                                  { return false }
func (*stLU) Verify(VCursor, []sqltypes.Value, [][]byte) ([]bool, error)          { return []bool{}, nil }
func (*stLU) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) { return nil, nil }
func (*stLU) Create(VCursor, [][]sqltypes.Value, [][]byte, bool) error            { return nil }
func (*stLU) Delete(VCursor, [][]sqltypes.Value, []byte) error                    { return nil }
func (*stLU) Update(VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error    { return nil }

func NewSTLU(name string, params map[string]string) (Vindex, error) {
	return &stLU{name: name, Params: params}, nil
}

var _ Vindex = (*stLU)(nil)
var _ Lookup = (*stLU)(nil)

func init() {
	Register("stfu", NewSTFU)
	Register("stln", NewSTLN)
	Register("stlu", NewSTLU)
}

func TestUnshardedVSchema(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"t1": {},
				},
			},
		},
	}
	got, _ := BuildVSchema(&good)
	err := got.Keyspaces["unsharded"].Error
	if err != nil {
		t.Error(err)
	}
	ks := &Keyspace{
		Name: "unsharded",
	}
	t1 := &Table{
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks,
	}
	dual := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ks,
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		uniqueTables: map[string]*Table{
			"t1":   t1,
			"dual": dual,
		},
		uniqueVindexes: map[string]Vindex{},
		Keyspaces: map[string]*KeyspaceSchema{
			"unsharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1":   t1,
					"dual": dual,
				},
				Vindexes: map[string]Vindex{},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildVSchema:\n%v, want\n%v", got, want)
	}
}

func TestVSchemaColumns(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Columns: []*vschemapb.Column{{
							Name: "c1",
						}, {
							Name: "c2",
							Type: sqltypes.VarChar,
						}},
					},
				},
			},
		},
	}
	got, _ := BuildVSchema(&good)
	err := got.Keyspaces["unsharded"].Error
	if err != nil {
		t.Error(err)
	}
	ks := &Keyspace{
		Name: "unsharded",
	}
	t1 := &Table{
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks,
		Columns: []Column{{
			Name: sqlparser.NewColIdent("c1"),
			Type: sqltypes.Null,
		}, {
			Name: sqlparser.NewColIdent("c2"),
			Type: sqltypes.VarChar,
		}},
	}
	dual := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ks,
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		uniqueTables: map[string]*Table{
			"t1":   t1,
			"dual": dual,
		},
		uniqueVindexes: map[string]Vindex{},
		Keyspaces: map[string]*KeyspaceSchema{
			"unsharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1":   t1,
					"dual": dual,
				},
				Vindexes: map[string]Vindex{},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		gotb, _ := json.Marshal(got)
		wantb, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:\n%s, want\n%s", gotb, wantb)
	}
}

func TestVSchemaColumnListAuthoritative(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Columns: []*vschemapb.Column{{
							Name: "c1",
						}, {
							Name: "c2",
							Type: sqltypes.VarChar,
						}},
						ColumnListAuthoritative: true,
					},
				},
			},
		},
	}
	got, err := BuildVSchema(&good)
	if err != nil {
		t.Error(err)
	}
	ks := &Keyspace{
		Name: "unsharded",
	}
	t1 := &Table{
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks,
		Columns: []Column{{
			Name: sqlparser.NewColIdent("c1"),
			Type: sqltypes.Null,
		}, {
			Name: sqlparser.NewColIdent("c2"),
			Type: sqltypes.VarChar,
		}},
		ColumnListAuthoritative: true,
	}
	dual := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ks,
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		uniqueTables: map[string]*Table{
			"t1":   t1,
			"dual": dual,
		},
		uniqueVindexes: map[string]Vindex{},
		Keyspaces: map[string]*KeyspaceSchema{
			"unsharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1":   t1,
					"dual": dual,
				},
				Vindexes: map[string]Vindex{},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		gotb, _ := json.Marshal(got)
		wantb, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:\n%s, want\n%s", gotb, wantb)
	}
}

func TestVSchemaColumnsFail(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"unsharded": {
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Columns: []*vschemapb.Column{{
							Name: "c1",
						}, {
							Name: "c1",
						}},
					},
				},
			},
		},
	}
	got, _ := BuildVSchema(&good)
	want := "duplicate column name 'c1' for table: t1"
	err := got.Keyspaces["unsharded"].Error
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema(dup col): %v, want %v", err, want)
	}
}

func TestVSchemaPinned(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Pinned: "80",
					},
				},
			},
		},
	}
	got, _ := BuildVSchema(&good)
	err := got.Keyspaces["sharded"].Error
	if err != nil {
		t.Error(err)
	}
	ks := &Keyspace{
		Name:    "sharded",
		Sharded: true,
	}
	t1 := &Table{
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks,
		Pinned:   []byte{0x80},
	}
	dual := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ks,
		Pinned:   []byte{0},
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		uniqueTables: map[string]*Table{
			"t1":   t1,
			"dual": dual,
		},
		uniqueVindexes: map[string]Vindex{},
		Keyspaces: map[string]*KeyspaceSchema{
			"sharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1":   t1,
					"dual": dual,
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

func TestShardedVSchemaOwned(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stfu1": {
						Type: "stfu",
						Params: map[string]string{
							"stfu1": "1",
						},
						Owner: "t1",
					},
					"stln1": {
						Type:  "stln",
						Owner: "t1",
					},
				},
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
					},
				},
			},
		},
	}
	got, _ := BuildVSchema(&good)
	err := got.Keyspaces["sharded"].Error
	if err != nil {
		t.Error(err)
	}
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
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c1")},
				Type:    "stfu",
				Name:    "stfu1",
				Vindex:  vindex1,
			},
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c2")},
				Type:    "stln",
				Name:    "stln1",
				Owned:   true,
				Vindex:  vindex2,
			},
		},
	}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[1],
		t1.ColumnVindexes[0],
	}
	t1.Owned = t1.ColumnVindexes[1:]
	dual := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ks,
		Pinned:   []byte{0},
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		uniqueTables: map[string]*Table{
			"t1":   t1,
			"dual": dual,
		},
		uniqueVindexes: map[string]Vindex{
			"stfu1": vindex1,
			"stln1": vindex2,
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"sharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1":   t1,
					"dual": dual,
				},
				Vindexes: map[string]Vindex{
					"stfu1": vindex1,
					"stln1": vindex2,
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
				FromTable: "dup",
				ToTables:  []string{"ks1.t1"},
			}, {
				FromTable: "dup",
				ToTables:  []string{"ks1.t1"},
			}, {
				FromTable: "unqualified",
				ToTables:  []string{"t1"},
			}, {
				FromTable: "badkeyspace",
				ToTables:  []string{"ks3.t1"},
			}, {
				FromTable: "notfound",
				ToTables:  []string{"ks1.t2"},
			}, {
				FromTable: "doubletable",
				ToTables:  []string{"ks1.t1", "ks1.t1"},
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
	got, _ := BuildVSchema(&input)
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
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks1,
		ColumnVindexes: []*ColumnVindex{{
			Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c1")},
			Type:    "stfu",
			Name:    "stfu1",
			Vindex:  vindex1,
		}},
	}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[0],
	}
	t2 := &Table{
		Name:     sqlparser.NewTableIdent("t2"),
		Keyspace: ks2,
	}
	dual1 := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ks1,
		Pinned:   []byte{0},
	}
	dual2 := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ks2,
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{
			"rt1": {
				Tables: []*Table{t1, t2},
			},
			"rt2": {
				Tables: []*Table{t2},
			},
			"dup": {
				Error: errors.New("duplicate rule for entry dup"),
			},
			"unqualified": {
				Error: errors.New("table t1 must be qualified"),
			},
			"badkeyspace": {
				Error: errors.New("keyspace ks3 not found in vschema"),
			},
			"notfound": {
				Error: errors.New("table t2 not found"),
			},
			"doubletable": {
				Error: errors.New("table ks1.t1 specified more than once"),
			},
		},
		uniqueTables: map[string]*Table{
			"t1":   t1,
			"t2":   t2,
			"dual": dual1,
		},
		uniqueVindexes: map[string]Vindex{
			"stfu1": vindex1,
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
	if !reflect.DeepEqual(got, want) {
		gotb, _ := json.Marshal(got)
		wantb, _ := json.Marshal(want)
		t.Errorf("BuildVSchema:\n%s, want\n%s", gotb, wantb)
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
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c1")},
				Type:    "stfu",
				Name:    "stfu1",
				Vindex:  vindex1,
			},
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c2")},
				Type:    "stln",
				Name:    "stln1",
				Owned:   true,
				Vindex:  vindex2,
			},
		},
	}
	res, err := FindVindexForSharding(t1.Name.String(), t1.ColumnVindexes)
	if err != nil {
		t.Error(err)
	}
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
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c1")},
				Type:    "stlu",
				Name:    "stlu1",
				Vindex:  vindex1,
			},
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c2")},
				Type:    "stln",
				Name:    "stln1",
				Owned:   true,
				Vindex:  vindex2,
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
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c1")},
				Type:    "stlu",
				Name:    "stlu1",
				Vindex:  vindex1,
			},
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c2")},
				Type:    "stfu",
				Name:    "stfu1",
				Owned:   true,
				Vindex:  vindex2,
			},
		},
	}
	res, err := FindVindexForSharding(t1.Name.String(), t1.ColumnVindexes)
	if err != nil {
		t.Error(err)
	}
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
							"stfu1": "1",
						},
						Owner: "t1",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Columns: []string{"c1", "c2"},
								Name:    "stfu1",
							},
						},
					},
				},
			},
		},
	}
	got, _ := BuildVSchema(&good)
	err := got.Keyspaces["sharded"].Error
	if err != nil {
		t.Error(err)
	}
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
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c1"), sqlparser.NewColIdent("c2")},
				Type:    "stfu",
				Name:    "stfu1",
				Vindex:  vindex1,
			},
		},
	}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[0],
	}
	dual := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ks,
		Pinned:   []byte{0},
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		uniqueTables: map[string]*Table{
			"t1":   t1,
			"dual": dual,
		},
		uniqueVindexes: map[string]Vindex{
			"stfu1": vindex1,
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"sharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1":   t1,
					"dual": dual,
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

func TestShardedVSchemaNotOwned(t *testing.T) {
	good := vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"stlu1": {
						Type:  "stlu",
						Owner: "",
					},
					"stfu1": {
						Type:  "stfu",
						Owner: "",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "stlu1",
							}, {
								Column: "c2",
								Name:   "stfu1",
							},
						},
					},
				},
			},
		},
	}
	got, _ := BuildVSchema(&good)
	err := got.Keyspaces["sharded"].Error
	if err != nil {
		t.Error(err)
	}
	ks := &Keyspace{
		Name:    "sharded",
		Sharded: true,
	}
	vindex1 := &stLU{name: "stlu1"}
	vindex2 := &stFU{name: "stfu1"}
	t1 := &Table{
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c1")},
				Type:    "stlu",
				Name:    "stlu1",
				Owned:   false,
				Vindex:  vindex1,
			},
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c2")},
				Type:    "stfu",
				Name:    "stfu1",
				Owned:   false,
				Vindex:  vindex2,
			},
		},
	}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[1],
		t1.ColumnVindexes[0],
	}
	dual := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ks,
		Pinned:   []byte{0},
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		uniqueTables: map[string]*Table{
			"t1":   t1,
			"dual": dual,
		},
		uniqueVindexes: map[string]Vindex{
			"stlu1": vindex1,
			"stfu1": vindex2,
		},
		Keyspaces: map[string]*KeyspaceSchema{
			"sharded": {
				Keyspace: ks,
				Tables: map[string]*Table{
					"t1":   t1,
					"dual": dual,
				},
				Vindexes: map[string]Vindex{
					"stlu1": vindex1,
					"stfu1": vindex2,
				},
			},
		},
	}
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
	got, _ := BuildVSchema(&bad)
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
	got, _ := BuildVSchema(&bad)
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
						Type: "sequence",
					},
				},
			},
			"ksb": {
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: "sequence",
					},
				},
			},
		},
	}
	ksa := &Keyspace{
		Name: "ksa",
	}
	ksb := &Keyspace{
		Name: "ksb",
	}
	got, _ := BuildVSchema(&good)
	t1a := &Table{
		Name:       sqlparser.NewTableIdent("t1"),
		Keyspace:   ksa,
		IsSequence: true,
	}
	t1b := &Table{
		Name:       sqlparser.NewTableIdent("t1"),
		Keyspace:   ksb,
		IsSequence: true,
	}
	duala := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ksa,
	}
	dualb := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ksb,
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		uniqueTables: map[string]*Table{
			"t1":   nil,
			"dual": duala,
		},
		uniqueVindexes: map[string]Vindex{},
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
	got, _ := BuildVSchema(&good)
	ksa := &Keyspace{
		Name: "ksa",
	}
	t1a := &Table{
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ksa,
	}
	ksb := &Keyspace{
		Name: "ksb",
	}
	t1b := &Table{
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ksb,
	}
	duala := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ksa,
	}
	dualb := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ksb,
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		uniqueTables: map[string]*Table{
			"t1":   nil,
			"dual": duala,
		},
		uniqueVindexes: map[string]Vindex{},
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
	got, _ := BuildVSchema(&good)
	err := got.Keyspaces["ksa"].Error
	err1 := got.Keyspaces["ksb"].Error
	if err != nil {
		t.Error(err)
	}
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
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ksa,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c1")},
				Type:    "stlu",
				Name:    "stlu1",
				Owned:   false,
				Vindex:  vindex1,
			},
		},
	}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[0],
	}
	t2 := &Table{
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ksb,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c1")},
				Type:    "stlu",
				Name:    "stlu1",
				Owned:   false,
				Vindex:  vindex1,
			},
		},
	}
	t2.Ordered = []*ColumnVindex{
		t2.ColumnVindexes[0],
	}
	duala := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ksa,
		Pinned:   []byte{0},
	}
	dualb := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ksb,
		Pinned:   []byte{0},
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		uniqueTables: map[string]*Table{
			"t1":   nil,
			"dual": duala,
		},
		uniqueVindexes: map[string]Vindex{
			"stlu1": nil,
		},
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
	got, _ := BuildVSchema(&bad)
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
	got, _ := BuildVSchema(&bad)
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
	got, _ := BuildVSchema(&bad)
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
	got, _ := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := "primary vindex stln is not Unique for table t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
}

func TestBuildVSchemaPrimaryNonFunctionalFail(t *testing.T) {
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
	got, _ := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := "primary vindex stlu cannot be owned for table t1"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}
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
							Sequence: "unsharded.seq",
						},
					},
				},
			},
		},
	}
	got, _ := BuildVSchema(&good)
	err := got.Keyspaces["sharded"].Error
	if err != nil {
		t.Error(err)
	}
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
		Name:       sqlparser.NewTableIdent("seq"),
		Keyspace:   ksu,
		IsSequence: true,
	}
	vindex1 := &stFU{
		name: "stfu1",
		Params: map[string]string{
			"stfu1": "1",
		},
	}
	t1 := &Table{
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: kss,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c1")},
				Type:    "stfu",
				Name:    "stfu1",
				Vindex:  vindex1,
			},
		},
		AutoIncrement: &AutoIncrement{
			Column:   sqlparser.NewColIdent("c1"),
			Sequence: seq,
		},
	}
	t1.Ordered = []*ColumnVindex{
		t1.ColumnVindexes[0],
	}
	t2 := &Table{
		Name:     sqlparser.NewTableIdent("t2"),
		Keyspace: kss,
		ColumnVindexes: []*ColumnVindex{
			{
				Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("c1")},
				Type:    "stfu",
				Name:    "stfu1",
				Vindex:  vindex1,
			},
		},
		AutoIncrement: &AutoIncrement{
			Column:   sqlparser.NewColIdent("c2"),
			Sequence: seq,
		},
	}
	t2.Ordered = []*ColumnVindex{
		t2.ColumnVindexes[0],
	}
	duala := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: ksu,
	}
	dualb := &Table{
		Name:     sqlparser.NewTableIdent("dual"),
		Keyspace: kss,
		Pinned:   []byte{0},
	}
	want := &VSchema{
		RoutingRules: map[string]*RoutingRule{},
		uniqueTables: map[string]*Table{
			"seq":  seq,
			"t1":   t1,
			"t2":   t2,
			"dual": dualb,
		},
		uniqueVindexes: map[string]Vindex{
			"stfu1": vindex1,
		},
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
	got, _ := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := "cannot resolve sequence invalid_seq: table invalid_seq not found"
	if err == nil || err.Error() != want {
		t.Errorf("BuildVSchema: %v, want %v", err, want)
	}

	t1Seq := got.Keyspaces["sharded"].Tables["t1"].AutoIncrement.Sequence
	if t1Seq != nil {
		t.Errorf("BuildVSchema: unexpected sequence for table t1: %v", t1Seq)
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
	got, _ := BuildVSchema(&bad)
	err := got.Keyspaces["sharded"].Error
	want := "cannot resolve sequence a.b.seq: table a.b.seq not found"
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
				},
			},
		},
	}
	vschema, _ := BuildVSchema(&input)
	_, err := vschema.FindTable("", "t1")
	wantErr := "ambiguous table reference: t1"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTable(\"\"): %v, want %s", err, wantErr)
	}
	_, err = vschema.FindTable("", "none")
	wantErr = "table none not found"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTable(\"\"): %v, want %s", err, wantErr)
	}
	got, err := vschema.FindTable("", "ta")
	if err != nil {
		t.Error(err)
		return
	}
	ta := &Table{
		Name: sqlparser.NewTableIdent("ta"),
		Keyspace: &Keyspace{
			Name: "ksa",
		},
	}
	if !reflect.DeepEqual(got, ta) {
		t.Errorf("FindTable(\"t1a\"): %+v, want %+v", got, ta)
	}
	got, _ = vschema.FindTable("ksa", "ta")
	if !reflect.DeepEqual(got, ta) {
		t.Errorf("FindTable(\"t1a\"): %+v, want %+v", got, ta)
	}
	none := &Table{
		Name: sqlparser.NewTableIdent("none"),
		Keyspace: &Keyspace{
			Name: "ksa",
		},
	}
	got, _ = vschema.FindTable("ksa", "none")
	if !reflect.DeepEqual(got, none) {
		t.Errorf("FindTable(\"t1a\"): %+v, want %+v", got, none)
	}
	_, err = vschema.FindTable("ksb", "none")
	wantErr = "table none not found"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTable(\"\"): %v, want %s", err, wantErr)
	}
	_, err = vschema.FindTable("none", "aa")
	wantErr = "keyspace none not found in vschema"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTable(\"\"): %v, want %s", err, wantErr)
	}
}

func TestFindTablesOrVindex(t *testing.T) {
	input := vschemapb.SrvVSchema{
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{{
				FromTable: "unqualified",
				ToTables:  []string{"ksa.ta", "ksb.t1"},
			}, {
				FromTable: "newks.qualified",
				ToTables:  []string{"ksa.ta"},
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
	vschema, _ := BuildVSchema(&input)
	ta := vschema.Keyspaces["ksa"].Tables["ta"]
	t1 := vschema.Keyspaces["ksb"].Tables["t1"]

	_, _, err := vschema.FindTablesOrVindex("", "t1")
	wantErr := "ambiguous table reference: t1"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTablesOrVindex(\"\"): %v, want %s", err, wantErr)
	}

	_, _, err = vschema.FindTablesOrVindex("", "none")
	wantErr = "table none not found"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTablesOrVindex(\"\"): %v, want %s", err, wantErr)
	}

	got, _, err := vschema.FindTablesOrVindex("", "ta")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []*Table{ta}) {
		t.Errorf("FindTablesOrVindex(\"t1a\"): %+v, want %+v", got, ta)
	}

	_, vindex, err := vschema.FindTablesOrVindex("", "stfu1")
	if err != nil {
		t.Fatal(err)
	}
	wantVindex := &stFU{
		name: "stfu1",
	}
	if !reflect.DeepEqual(vindex, wantVindex) {
		t.Errorf("FindTablesOrVindex(\"stfu1\"): %+v, want %+v", vindex, wantVindex)
	}

	_, vindex, err = vschema.FindTablesOrVindex("ksc", "ta")
	if err != nil {
		t.Fatal(err)
	}
	wantVindex = &stFU{
		name: "ta",
	}
	if !reflect.DeepEqual(vindex, wantVindex) {
		t.Errorf("FindTablesOrVindex(\"stfu1\"): %+v, want %+v", vindex, wantVindex)
	}

	_, _, err = vschema.FindTablesOrVindex("", "dup")
	wantErr = "ambiguous vindex reference: dup"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTablesOrVindex(\"\"): %v, want %s", err, wantErr)
	}

	got, _, err = vschema.FindTablesOrVindex("", "unqualified")
	if err != nil {
		t.Fatal(err)
	}
	if want := []*Table{ta, t1}; !reflect.DeepEqual(got, want) {
		t.Errorf("FindTablesOrVindex(unqualified): %+v, want %+v", got, want)
	}

	got, _, err = vschema.FindTablesOrVindex("newks", "qualified")
	if err != nil {
		t.Fatal(err)
	}
	if want := []*Table{ta}; !reflect.DeepEqual(got, want) {
		t.Errorf("FindTablesOrVindex(unqualified): %+v, want %+v", got, want)
	}

	_, _, err = vschema.FindTablesOrVindex("", "notarget")
	wantErr = "table notarget has been disabled"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTablesOrVindex(\"\"): %v, want %s", err, wantErr)
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
	if err != nil {
		t.Error(err)
	}
	ks := &Keyspace{
		Name: "ks",
	}
	t1 := &Table{
		Name:     sqlparser.NewTableIdent("t1"),
		Keyspace: ks,
	}
	t2 := &Table{
		Name:     sqlparser.NewTableIdent("t2"),
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
	if err != nil {
		t.Error(err)
	}
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
					Name: sqlparser.NewTableIdent("n1"),
					Columns: []Column{{
						Name: sqlparser.NewColIdent("c1"),
					}, {
						Name: sqlparser.NewColIdent("c2"),
						Type: sqltypes.VarChar,
					}},
				},
				"t2": {
					IsSequence: true,
					Name:       sqlparser.NewTableIdent("n2"),
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
					Name: sqlparser.NewTableIdent("n3"),
					ColumnVindexes: []*ColumnVindex{{
						Columns: []sqlparser.ColIdent{sqlparser.NewColIdent("aa")},
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
        "is_sequence": true,
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
	vschema, _ := BuildVSchema(&input)
	none := &Table{
		Name: sqlparser.NewTableIdent("none"),
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
	vschema, _ = BuildVSchema(&input)
	_, err := vschema.FindTable("", "none")
	wantErr := "table none not found"
	if err == nil || err.Error() != wantErr {
		t.Errorf("FindTable(\"\"): %v, want %s", err, wantErr)
	}
}
