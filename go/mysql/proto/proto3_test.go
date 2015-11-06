// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/proto/query"
)

func TestFields(t *testing.T) {
	fields := []Field{{
		Name:  "aa",
		Type:  1,
		Flags: 32,
	}, {
		Name: "bb",
		Type: 2,
	}}
	p3 := FieldsToProto3(fields)
	wantp3 := []*query.Field{
		&query.Field{
			Name: "aa",
			Type: sqltypes.Uint8,
		},
		&query.Field{
			Name: "bb",
			Type: sqltypes.Int16,
		},
	}
	if !reflect.DeepEqual(p3, wantp3) {
		t.Errorf("p3: %v, want %v", p3, wantp3)
	}

	reverse := Proto3ToFields(p3)
	if !reflect.DeepEqual(reverse, fields) {
		t.Errorf("reverse: %v, want %v", reverse, fields)
	}
}

func TestRowsToProto3(t *testing.T) {
	rows := [][]sqltypes.Value{{
		sqltypes.MakeString([]byte("aa")),
		sqltypes.NULL,
		sqltypes.MakeString([]byte("12")),
	}, {
		sqltypes.MakeString([]byte("bb")),
		sqltypes.NULL,
		sqltypes.NULL,
	}}
	p3 := RowsToProto3(rows)
	want := []*query.Row{
		&query.Row{
			Lengths: []int64{2, -1, 2},
			Values:  []byte("aa12"),
		},
		&query.Row{
			Lengths: []int64{2, -1, -1},
			Values:  []byte("bb"),
		},
	}
	if !reflect.DeepEqual(p3, want) {
		t.Errorf("P3: %v, want %v", p3, want)
	}

	reverse := Proto3ToRows(p3)
	if !reflect.DeepEqual(reverse, rows) {
		t.Errorf("reverse: \n%#v, want \n%#v", reverse, rows)
	}
}

func TestInvalidRowsProto(t *testing.T) {
	p3 := []*query.Row{
		&query.Row{
			Lengths: []int64{3, 5, -1, 6},
			Values:  []byte("aa12"),
		},
	}
	rows := Proto3ToRows(p3)
	want := [][]sqltypes.Value{{
		sqltypes.MakeString([]byte("aa1")),
		sqltypes.NULL,
		sqltypes.NULL,
		sqltypes.NULL,
	}}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("reverse: \n%#v, want \n%#v", rows, want)
	}

	p3 = []*query.Row{
		&query.Row{
			Lengths: []int64{2, -2, 2},
			Values:  []byte("aa12"),
		},
	}
	rows = Proto3ToRows(p3)
	want = [][]sqltypes.Value{{
		sqltypes.MakeString([]byte("aa")),
		sqltypes.NULL,
		sqltypes.MakeString([]byte("12")),
	}}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("reverse: \n%#v, want \n%#v", rows, want)
	}
}
