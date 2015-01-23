// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"reflect"
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

func TestOrderedColumns(t *testing.T) {
	input := &myproto.TableDefinition{
		PrimaryKeyColumns: []string{"pk1", "pk2"},
		Columns:           []string{"pk1", "col1", "pk2", "col2"},
	}
	want := []string{"pk1", "pk2", "col1", "col2"}
	if got := orderedColumns(input); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestUint64FromKeyspaceId(t *testing.T) {
	table := map[string]string{
		"10":       "0x1000000000000000",
		"fe":       "0xfe00000000000000",
		"1234cafe": "0x1234cafe00000000",
	}
	for input, want := range table {
		keyspaceID, err := key.HexKeyspaceId(input).Unhex()
		if err != nil {
			t.Errorf("Unhex error: %v", err)
			continue
		}
		if got := uint64FromKeyspaceId(keyspaceID); got != want {
			t.Errorf("uint64FromKeyspaceId(%v) = %q, want %q", input, got, want)
		}
	}
}

func TestCompareRows(t *testing.T) {
	table := []struct {
		fields      []mproto.Field
		left, right []sqltypes.Value
		want        int
	}{
		{
			fields: []mproto.Field{{Name: "a", Type: mproto.VT_LONG}},
			left:   []sqltypes.Value{{sqltypes.Numeric("123")}},
			right:  []sqltypes.Value{{sqltypes.Numeric("14")}},
			want:   1,
		},
		{
			fields: []mproto.Field{
				{Name: "a", Type: mproto.VT_LONG},
				{Name: "b", Type: mproto.VT_LONG},
			},
			left: []sqltypes.Value{
				{sqltypes.Numeric("555")},
				{sqltypes.Numeric("12")},
			},
			right: []sqltypes.Value{
				{sqltypes.Numeric("555")},
				{sqltypes.Numeric("144")},
			},
			want: -1,
		},
		{
			fields: []mproto.Field{{Name: "a", Type: mproto.VT_LONG}},
			left:   []sqltypes.Value{{sqltypes.Numeric("144")}},
			right:  []sqltypes.Value{{sqltypes.Numeric("144")}},
			want:   0,
		},
		{
			fields: []mproto.Field{{Name: "a", Type: mproto.VT_LONGLONG}},
			left:   []sqltypes.Value{{sqltypes.Numeric("9223372036854775809")}},
			right:  []sqltypes.Value{{sqltypes.Numeric("9223372036854775810")}},
			want:   -1,
		},
		{
			fields: []mproto.Field{{Name: "a", Type: mproto.VT_LONGLONG}},
			left:   []sqltypes.Value{{sqltypes.Numeric("9223372036854775819")}},
			right:  []sqltypes.Value{{sqltypes.Numeric("9223372036854775810")}},
			want:   1,
		},
		{
			fields: []mproto.Field{{Name: "a", Type: mproto.VT_DOUBLE}},
			left:   []sqltypes.Value{{sqltypes.Fractional("3.14")}},
			right:  []sqltypes.Value{{sqltypes.Fractional("3.2")}},
			want:   -1,
		},
		{
			fields: []mproto.Field{{Name: "a", Type: mproto.VT_DOUBLE}},
			left:   []sqltypes.Value{{sqltypes.Fractional("123.4")}},
			right:  []sqltypes.Value{{sqltypes.Fractional("123.2")}},
			want:   1,
		},
		{
			fields: []mproto.Field{{Name: "a", Type: mproto.VT_STRING}},
			left:   []sqltypes.Value{{sqltypes.String("abc")}},
			right:  []sqltypes.Value{{sqltypes.String("abb")}},
			want:   1,
		},
		{
			fields: []mproto.Field{{Name: "a", Type: mproto.VT_STRING}},
			left:   []sqltypes.Value{{sqltypes.String("abc")}},
			right:  []sqltypes.Value{{sqltypes.String("abd")}},
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
