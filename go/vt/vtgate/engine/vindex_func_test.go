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
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

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
func (*uvindex) NeedsVCursor() bool { return false }
func (*uvindex) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	panic("unimplemented")
}

func (v *uvindex) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	destinations := make([]key.Destination, 0, len(ids))
	dkid := []byte("foo")
	for i := 0; i < len(ids); i++ {
		if v.matchkr {
			destinations = append(destinations,
				key.DestinationKeyRange{
					KeyRange: &topodatapb.KeyRange{
						Start: []byte{0x40},
						End:   []byte{0x60},
					},
				})
		} else if v.matchid {
			destinations = append(destinations, key.DestinationKeyspaceID(dkid))
		} else {
			destinations = append(destinations, key.DestinationNone{})
		}
	}
	return destinations, nil
}

// nvindex is NonUnique.
type nvindex struct{ matchid, matchkr bool }

func (*nvindex) String() string     { return "nvindex" }
func (*nvindex) Cost() int          { return 1 }
func (*nvindex) IsUnique() bool     { return false }
func (*nvindex) NeedsVCursor() bool { return false }
func (*nvindex) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	panic("unimplemented")
}

func (v *nvindex) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	destinations := make([]key.Destination, 0)
	for i := 0; i < len(ids); i++ {
		if v.matchid {
			destinations = append(destinations,
				[]key.Destination{
					key.DestinationKeyspaceIDs([][]byte{
						[]byte("foo"),
						[]byte("bar"),
					}),
				}...)
		} else if v.matchkr {
			destinations = append(destinations,
				[]key.Destination{
					key.DestinationKeyRange{
						KeyRange: &topodatapb.KeyRange{
							Start: []byte{0x40},
							End:   []byte{0x60},
						},
					},
				}...)
		} else {
			destinations = append(destinations, []key.Destination{key.DestinationNone{}}...)
		}
	}
	return destinations, nil
}

func TestVindexFuncMap(t *testing.T) {
	// Unique Vindex returning 0 rows.
	vf := testVindexFunc(&uvindex{})
	got, err := vf.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	want := &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|hex(keyspace_id)|range_start|range_end", "varbinary|varbinary|varbinary|varbinary|varbinary"),
	}
	require.Equal(t, got, want)

	// Unique Vindex returning 1 row.
	vf = testVindexFunc(&uvindex{matchid: true})
	got, err = vf.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	want = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|keyspace_id|hex(keyspace_id)|range_start|range_end", "varbinary|varbinary|varbinary|varbinary|varbinary"),
		"1|foo|||666f6f",
	)
	for _, row := range want.Rows {
		row[2] = sqltypes.NULL
		row[3] = sqltypes.NULL
	}
	require.Equal(t, got, want)

	// Unique Vindex returning 3 rows
	vf = &VindexFunc{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|hex(keyspace_id)|range_start|range_end", "varbinary|varbinary|varbinary|varbinary|varbinary"),
		Cols:   []int{0, 1, 2, 3, 4},
		Opcode: VindexMap,
		Vindex: &uvindex{matchid: true},
		Value:  evalengine.TupleExpr{evalengine.NewLiteralInt(1), evalengine.NewLiteralInt(2), evalengine.NewLiteralInt(3)},
	}
	got, err = vf.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	want = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|keyspace_id|hex(keyspace_id)|range_start|range_end", "varbinary|varbinary|varbinary|varbinary|varbinary"),
		"1|foo|||666f6f",
		"2|foo|||666f6f",
		"3|foo|||666f6f",
	)
	for _, row := range want.Rows {
		row[2] = sqltypes.NULL
		row[3] = sqltypes.NULL
	}
	require.Equal(t, got, want)

	// Unique Vindex returning keyrange.
	vf = testVindexFunc(&uvindex{matchkr: true})
	got, err = vf.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	want = &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|hex(keyspace_id)|range_start|range_end", "varbinary|varbinary|varbinary|varbinary|varbinary"),
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("1"),
				sqltypes.NULL,
				sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{0x40}),
				sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{0x60}),
				sqltypes.NULL,
			},
		},
		RowsAffected: 0,
	}
	require.Equal(t, got, want)

	// NonUnique Vindex returning 0 rows.
	vf = testVindexFunc(&nvindex{})
	got, err = vf.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	want = &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|hex(keyspace_id)|range_start|range_end", "varbinary|varbinary|varbinary|varbinary|varbinary"),
	}
	require.Equal(t, got, want)

	// NonUnique Vindex returning 2 rows.
	vf = testVindexFunc(&nvindex{matchid: true})
	got, err = vf.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	want = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|keyspace_id|hex(keyspace_id)|range_start|range_end", "varbinary|varbinary|varbinary|varbinary|varbinary"),
		"1|foo|||666f6f",
		"1|bar|||626172",
	)
	// Massage the rows because MakeTestResult doesn't do NULL values.
	for _, row := range want.Rows {
		row[2] = sqltypes.NULL
		row[3] = sqltypes.NULL
	}
	require.Equal(t, got, want)

	// NonUnique Vindex returning keyrange
	vf = testVindexFunc(&nvindex{matchkr: true})
	got, err = vf.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	want = &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|hex(keyspace_id)|range_start|range_end", "varbinary|varbinary|varbinary|varbinary|varbinary"),
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("1"),
			sqltypes.NULL,
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{0x40}),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{0x60}),
			sqltypes.NULL,
		}},
		RowsAffected: 0,
	}
	require.Equal(t, got, want)
}

func TestVindexFuncStreamExecute(t *testing.T) {
	vf := testVindexFunc(&nvindex{matchid: true})
	want := []*sqltypes.Result{{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|hex(keyspace_id)|range_start|range_end", "varbinary|varbinary|varbinary|varbinary|varbinary"),
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("1"), sqltypes.NewVarBinary("foo"), sqltypes.NULL, sqltypes.NULL, sqltypes.NewVarBinary("666f6f"),
		}, {
			sqltypes.NewVarBinary("1"), sqltypes.NewVarBinary("bar"), sqltypes.NULL, sqltypes.NULL, sqltypes.NewVarBinary("626172"),
		}},
	}}
	i := 0
	err := vf.TryStreamExecute(context.Background(), &noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
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
	got, err := vf.GetFields(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|hex(keyspace_id)|range_start|range_end", "varbinary|varbinary|varbinary|varbinary|varbinary"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}
}

func TestFieldOrder(t *testing.T) {
	vf := testVindexFunc(&nvindex{matchid: true})
	vf.Fields = sqltypes.MakeTestFields("keyspace_id|id|keyspace_id", "varbinary|varbinary|varbinary")
	vf.Cols = []int{1, 0, 1}
	got, err := vf.TryExecute(context.Background(), &noopVCursor{}, nil, true)
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

func testVindexFunc(v vindexes.SingleColumn) *VindexFunc {
	return &VindexFunc{
		Fields: sqltypes.MakeTestFields("id|keyspace_id|hex(keyspace_id)|range_start|range_end", "varbinary|varbinary|varbinary|varbinary|varbinary"),
		Cols:   []int{0, 1, 2, 3, 4},
		Opcode: VindexMap,
		Vindex: v,
		Value:  evalengine.NewLiteralInt(1),
	}
}
