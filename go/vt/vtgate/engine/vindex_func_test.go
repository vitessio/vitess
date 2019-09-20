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

package engine

import (
	"reflect"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// uvindex is Unique.
type uvindex struct{ matchid, matchkr bool }

func (*uvindex) String() string     { return "uvindex" }
func (*uvindex) Cost() int          { return 1 }
func (*uvindex) IsUnique() bool     { return true }
func (*uvindex) IsFunctional() bool { return false }
func (*uvindex) Verify(vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	panic("unimplemented")
}

func (v *uvindex) Map(cursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	if v.matchkr {
		return []key.Destination{
			key.DestinationKeyRange{
				KeyRange: &topodatapb.KeyRange{
					Start: []byte{0x40},
					End:   []byte{0x60},
				},
			},
		}, nil
	}
	if v.matchid {
		return []key.Destination{
			key.DestinationKeyspaceID([]byte("foo")),
		}, nil
	}
	return []key.Destination{key.DestinationNone{}}, nil
}

// nvindex is NonUnique.
type nvindex struct{ matchid, matchkr bool }

func (*nvindex) String() string     { return "nvindex" }
func (*nvindex) Cost() int          { return 1 }
func (*nvindex) IsUnique() bool     { return false }
func (*nvindex) IsFunctional() bool { return false }
func (*nvindex) Verify(vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	panic("unimplemented")
}

func (v *nvindex) Map(cursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	if v.matchid {
		return []key.Destination{
			key.DestinationKeyspaceIDs([][]byte{
				[]byte("foo"),
				[]byte("bar"),
			}),
		}, nil
	}
	if v.matchkr {
		return []key.Destination{
			key.DestinationKeyRange{
				KeyRange: &topodatapb.KeyRange{
					Start: []byte{0x40},
					End:   []byte{0x60},
				},
			},
		}, nil
	}
	return []key.Destination{key.DestinationNone{}}, nil
}

func TestVindexFuncMap(t *testing.T) {
	// Unique Vindex returning 0 rows.
	vf := testVindexFunc(&uvindex{})
	got, err := vf.Execute(nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want := &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|range_start|range_end", "varbinary|varbinary|varbinary|varbinary"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}

	// Unique Vindex returning 1 row.
	vf = testVindexFunc(&uvindex{matchid: true})
	got, err = vf.Execute(nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|keyspace_id|range_start|range_end", "varbinary|varbinary|varbinary|varbinary"),
		"1|foo",
	)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}

	// Unique Vindex returning keyrange.
	vf = testVindexFunc(&uvindex{matchkr: true})
	got, err = vf.Execute(nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want = &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|range_start|range_end", "varbinary|varbinary|varbinary|varbinary"),
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("1"),
			sqltypes.NULL,
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{0x40}),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{0x60}),
		}},
		RowsAffected: 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}

	// NonUnique Vindex returning 0 rows.
	vf = testVindexFunc(&nvindex{})
	got, err = vf.Execute(nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want = &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|range_start|range_end", "varbinary|varbinary|varbinary|varbinary"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}

	// NonUnique Vindex returning 2 rows.
	vf = testVindexFunc(&nvindex{matchid: true})
	got, err = vf.Execute(nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|keyspace_id|range_start|range_end", "varbinary|varbinary|varbinary|varbinary"),
		"1|foo||",
		"1|bar||",
	)
	// Massage the rows because MakeTestResult doesn't do NULL values.
	for _, row := range want.Rows {
		row[2] = sqltypes.NULL
		row[3] = sqltypes.NULL
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}

	// NonUnique Vindex returning keyrange
	vf = testVindexFunc(&nvindex{matchkr: true})
	got, err = vf.Execute(nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want = &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|range_start|range_end", "varbinary|varbinary|varbinary|varbinary"),
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("1"),
			sqltypes.NULL,
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{0x40}),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{0x60}),
		}},
		RowsAffected: 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}
}

func TestVindexFuncStreamExecute(t *testing.T) {
	vf := testVindexFunc(&nvindex{matchid: true})
	want := []*sqltypes.Result{{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|range_start|range_end", "varbinary|varbinary|varbinary|varbinary"),
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("1"), sqltypes.NewVarBinary("foo"), sqltypes.NULL, sqltypes.NULL,
		}, {
			sqltypes.NewVarBinary("1"), sqltypes.NewVarBinary("bar"), sqltypes.NULL, sqltypes.NULL,
		}},
	}}
	i := 0
	err := vf.StreamExecute(nil, nil, false, func(qr *sqltypes.Result) error {
		if !reflect.DeepEqual(qr, want[i]) {
			t.Errorf("callback(%d):\n%v, want\n%v", i, qr, want[i])
		}
		i++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestVindexFuncGetFields(t *testing.T) {
	vf := testVindexFunc(&uvindex{matchid: true})
	got, err := vf.GetFields(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|range_start|range_end", "varbinary|varbinary|varbinary|varbinary"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}
}

func TestFieldOrder(t *testing.T) {
	vf := testVindexFunc(&nvindex{matchid: true})
	vf.Fields = sqltypes.MakeTestFields("keyspace_id|id|keyspace_id", "varbinary|varbinary|varbinary")
	vf.Cols = []int{1, 0, 1}
	got, err := vf.Execute(nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	want := sqltypes.MakeTestResult(
		vf.Fields,
		"foo|1|foo",
		"bar|1|bar",
	)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}
}

func testVindexFunc(v vindexes.Vindex) *VindexFunc {
	return &VindexFunc{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|range_start|range_end", "varbinary|varbinary|varbinary|varbinary"),
		Cols:   []int{0, 1, 2, 3},
		Opcode: VindexMap,
		Vindex: v,
		Value:  int64PlanValue(1),
	}
}
