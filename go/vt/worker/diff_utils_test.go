// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
			left:   []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Int32, []byte("123"))},
			right:  []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Int32, []byte("14"))},
			want:   1,
		},
		{
			fields: []*querypb.Field{
				{Name: "a", Type: sqltypes.Int32},
				{Name: "b", Type: sqltypes.Int32},
			},
			left: []sqltypes.Value{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("555")),
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("12")),
			},
			right: []sqltypes.Value{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("555")),
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("144")),
			},
			want: -1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Int32}},
			left:   []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Int32, []byte("144"))},
			right:  []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Int32, []byte("144"))},
			want:   0,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Uint64}},
			left:   []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Uint64, []byte("9223372036854775809"))},
			right:  []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Uint64, []byte("9223372036854775810"))},
			want:   -1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Uint64}},
			left:   []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Uint64, []byte("9223372036854775819"))},
			right:  []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Uint64, []byte("9223372036854775810"))},
			want:   1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Float64}},
			left:   []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Float64, []byte("3.14"))},
			right:  []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Float64, []byte("3.2"))},
			want:   -1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Float64}},
			left:   []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Float64, []byte("123.4"))},
			right:  []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Float64, []byte("123.2"))},
			want:   1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Char}},
			left:   []sqltypes.Value{sqltypes.MakeString([]byte("abc"))},
			right:  []sqltypes.Value{sqltypes.MakeString([]byte("abb"))},
			want:   1,
		},
		{
			fields: []*querypb.Field{{Name: "a", Type: sqltypes.Char}},
			left:   []sqltypes.Value{sqltypes.MakeString([]byte("abc"))},
			right:  []sqltypes.Value{sqltypes.MakeString([]byte("abd"))},
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
