// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"reflect"
	"testing"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestResult(t *testing.T) {
	fields := []*querypb.Field{{
		Name: "col1",
		Type: VarChar,
	}, {
		Name: "col2",
		Type: Int64,
	}, {
		Name: "col3",
		Type: Float64,
	}}
	sqlResult := &Result{
		Fields:       fields,
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{{
			testVal(VarChar, "aa"),
			testVal(Int64, "1"),
			testVal(Float64, "2"),
		}, {
			MakeTrusted(VarChar, []byte("bb")),
			NULL,
			NULL,
		}},
	}
	p3Result := &querypb.QueryResult{
		Fields:       fields,
		InsertId:     1,
		RowsAffected: 2,
		Rows: []*querypb.Row{{
			Lengths: []int64{2, 1, 1},
			Values:  []byte("aa12"),
		}, {
			Lengths: []int64{2, -1, -1},
			Values:  []byte("bb"),
		}},
	}
	p3converted := ResultToProto3(sqlResult)
	if !reflect.DeepEqual(p3converted, p3Result) {
		t.Errorf("P3:\n%v, want\n%v", p3converted, p3Result)
	}

	reverse := Proto3ToResult(p3Result)
	if !reflect.DeepEqual(reverse, sqlResult) {
		t.Errorf("reverse:\n%#v, want\n%#v", reverse, sqlResult)
	}

	// Test custom fields.
	fields[1].Type = VarBinary
	sqlResult.Rows[0][1] = testVal(VarBinary, "1")
	reverse = CustomProto3ToResult(fields, p3Result)
	if !reflect.DeepEqual(reverse, sqlResult) {
		t.Errorf("reverse:\n%#v, want\n%#v", reverse, sqlResult)
	}
}

func TestResults(t *testing.T) {
	fields1 := []*querypb.Field{{
		Name: "col1",
		Type: VarChar,
	}, {
		Name: "col2",
		Type: Int64,
	}, {
		Name: "col3",
		Type: Float64,
	}}
	fields2 := []*querypb.Field{{
		Name: "col11",
		Type: VarChar,
	}, {
		Name: "col12",
		Type: Int64,
	}, {
		Name: "col13",
		Type: Float64,
	}}
	sqlResults := []Result{{
		Fields:       fields1,
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{{
			testVal(VarChar, "aa"),
			testVal(Int64, "1"),
			testVal(Float64, "2"),
		}},
	}, {
		Fields:       fields2,
		InsertID:     3,
		RowsAffected: 4,
		Rows: [][]Value{{
			testVal(VarChar, "bb"),
			testVal(Int64, "3"),
			testVal(Float64, "4"),
		}},
	}}
	p3Results := []*querypb.QueryResult{{
		Fields:       fields1,
		InsertId:     1,
		RowsAffected: 2,
		Rows: []*querypb.Row{{
			Lengths: []int64{2, 1, 1},
			Values:  []byte("aa12"),
		}},
	}, {
		Fields:       fields2,
		InsertId:     3,
		RowsAffected: 4,
		Rows: []*querypb.Row{{
			Lengths: []int64{2, 1, 1},
			Values:  []byte("bb34"),
		}},
	}}
	p3converted := ResultsToProto3(sqlResults)
	if !reflect.DeepEqual(p3converted, p3Results) {
		t.Errorf("P3:\n%v, want\n%v", p3converted, p3Results)
	}

	reverse := Proto3ToResults(p3Results)
	if !reflect.DeepEqual(reverse, sqlResults) {
		t.Errorf("reverse:\n%#v, want\n%#v", reverse, sqlResults)
	}
}
