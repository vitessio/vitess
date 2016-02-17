// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"reflect"
	"testing"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestRepair(t *testing.T) {
	fields := []*querypb.Field{{
		Type: Int64,
	}, {
		Type: VarChar,
	}}
	in := Result{
		Rows: [][]Value{
			{testVal(VarBinary, "1"), testVal(VarBinary, "aa")},
			{testVal(VarBinary, "2"), testVal(VarBinary, "bb")},
		},
	}
	want := Result{
		Rows: [][]Value{
			{testVal(Int64, "1"), testVal(VarChar, "aa")},
			{testVal(Int64, "2"), testVal(VarChar, "bb")},
		},
	}
	in.Repair(fields)
	if !reflect.DeepEqual(in, want) {
		t.Errorf("Repair:\n%#v, want\n%#v", in, want)
	}
}

func TestCopy(t *testing.T) {
	in := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}, {
			Type: VarChar,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{testVal(Int64, "1"), MakeTrusted(Null, nil)},
			{testVal(Int64, "2"), MakeTrusted(VarChar, nil)},
			{testVal(Int64, "3"), testVal(VarChar, "")},
		},
	}
	want := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}, {
			Type: VarChar,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{testVal(Int64, "1"), MakeTrusted(Null, nil)},
			{testVal(Int64, "2"), testVal(VarChar, "")},
			{testVal(Int64, "3"), testVal(VarChar, "")},
		},
	}
	out := in.Copy()
	// Change in so we're sure out got actually copied
	in.Fields[0].Type = VarChar
	in.Rows[0][0] = testVal(VarChar, "aa")
	if !reflect.DeepEqual(out, want) {
		t.Errorf("Copy:\n%#v, want\n%#v", out, want)
	}
}
