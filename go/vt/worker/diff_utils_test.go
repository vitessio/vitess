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

package worker

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

func TestOrderedColumns(t *testing.T) {
	input := &tabletmanagerdatapb.TableDefinition{
		PrimaryKeyColumns: []string{"pk1", "pk2"},
		Columns:           []string{"pk1", "col1", "pk2", "col2"},
	}
	want := []string{"pk1", "pk2", "col1", "col2"}
	if got := orderedColumns(input); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestUint64FromKeyspaceId(t *testing.T) {
	table := map[string]uint64{
		"10":       0x1000000000000000,
		"fe":       0xfe00000000000000,
		"1234cafe": 0x1234cafe00000000,
	}
	for input, want := range table {
		keyspaceID, err := hex.DecodeString(input)
		if err != nil {
			t.Errorf("hex.DecodeString error: %v", err)
			continue
		}
		if got := uint64FromKeyspaceID(keyspaceID); got != want {
			t.Errorf("uint64FromKeyspaceID(%v) = %q, want %q", input, got, want)
		}
	}
}

func TestCompareRows(t *testing.T) {
	table := []struct {
		fields      []*querypb.Field
		left, right []sqltypes.Value
		want        int
	}{
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Int32}},
			left:   []sqltypes.Value{sqltypes.NewInt32(123)},
			right:  []sqltypes.Value{sqltypes.NewInt32(14)},
			want:   1,
		},
		{
			fields: []*querypb.Field{
				{Name: "a", Type: sqltypes.Int32},
				{Name: "b", Type: sqltypes.Int32},
			},
			left: []sqltypes.Value{
				sqltypes.NewInt32(555),
				sqltypes.NewInt32(12),
			},
			right: []sqltypes.Value{
				sqltypes.NewInt32(555),
				sqltypes.NewInt32(144),
			},
			want: -1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Int32}},
			left:   []sqltypes.Value{sqltypes.NewInt32(144)},
			right:  []sqltypes.Value{sqltypes.NewInt32(144)},
			want:   0,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Uint64}},
			left:   []sqltypes.Value{sqltypes.NewUint64(9223372036854775809)},
			right:  []sqltypes.Value{sqltypes.NewUint64(9223372036854775810)},
			want:   -1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Uint64}},
			left:   []sqltypes.Value{sqltypes.NewUint64(9223372036854775819)},
			right:  []sqltypes.Value{sqltypes.NewUint64(9223372036854775810)},
			want:   1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Float64}},
			left:   []sqltypes.Value{sqltypes.NewFloat64(3.14)},
			right:  []sqltypes.Value{sqltypes.NewFloat64(3.2)},
			want:   -1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Float64}},
			left:   []sqltypes.Value{sqltypes.NewFloat64(123.4)},
			right:  []sqltypes.Value{sqltypes.NewFloat64(123.2)},
			want:   1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Char}},
			left:   []sqltypes.Value{sqltypes.NewVarBinary("abc")},
			right:  []sqltypes.Value{sqltypes.NewVarBinary("abb")},
			want:   1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Char}},
			left:   []sqltypes.Value{sqltypes.NewVarBinary("abc")},
			right:  []sqltypes.Value{sqltypes.NewVarBinary("abd")},
			want:   -1,
		},
	}
	for _, tc := range table {
		got, err := CompareRows(tc.fields, len(tc.fields), tc.left, tc.right)
		if err != nil {
			t.Errorf("CompareRows error: %v", err)
			continue
		}
		if got != tc.want {
			t.Errorf("CompareRows(%v, %v, %v) = %v, want %v", tc.fields, tc.left, tc.right, got, tc.want)
		}
	}
}
