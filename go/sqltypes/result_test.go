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
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 123,
				Shard:     "sh",
				Position:  "po",
			},
			Fresher: true,
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
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 123,
				Shard:     "sh",
				Position:  "po",
			},
			Fresher: true,
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

func TestStripMetaData(t *testing.T) {
	testcases := []struct {
		name           string
		in             *Result
		expected       *Result
		includedFields querypb.ExecuteOptions_IncludedFields
	}{{
		name:     "no fields",
		in:       &Result{},
		expected: &Result{},
	}, {
		name: "empty fields",
		in: &Result{
			Fields: []*querypb.Field{},
		},
		expected: &Result{
			Fields: []*querypb.Field{},
		},
	}, {
		name:           "no name",
		includedFields: querypb.ExecuteOptions_TYPE_ONLY,
		in: &Result{
			Fields: []*querypb.Field{{
				Type: Int64,
			}, {
				Type: VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Type: Int64,
			}, {
				Type: VarChar,
			}},
		},
	}, {
		name:           "names",
		includedFields: querypb.ExecuteOptions_TYPE_ONLY,
		in: &Result{
			Fields: []*querypb.Field{{
				Name: "field1",
				Type: Int64,
			}, {
				Name: "field2",
				Type: VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Type: Int64,
			}, {
				Type: VarChar,
			}},
		},
	}, {
		name:           "all fields - strip to type",
		includedFields: querypb.ExecuteOptions_TYPE_ONLY,
		in: &Result{
			Fields: []*querypb.Field{{
				Name:         "field1",
				Table:        "table1",
				OrgTable:     "orgtable1",
				OrgName:      "orgname1",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         Int64,
			}, {
				Name:         "field2",
				Table:        "table2",
				OrgTable:     "orgtable2",
				OrgName:      "orgname2",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Type: Int64,
			}, {
				Type: VarChar,
			}},
		},
	}, {
		name:           "all fields - not stripped",
		includedFields: querypb.ExecuteOptions_ALL,
		in: &Result{
			Fields: []*querypb.Field{{
				Name:         "field1",
				Table:        "table1",
				OrgTable:     "orgtable1",
				OrgName:      "orgname1",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         Int64,
			}, {
				Name:         "field2",
				Table:        "table2",
				OrgTable:     "orgtable2",
				OrgName:      "orgname2",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Name:         "field1",
				Table:        "table1",
				OrgTable:     "orgtable1",
				OrgName:      "orgname1",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         Int64,
			}, {
				Name:         "field2",
				Table:        "table2",
				OrgTable:     "orgtable2",
				OrgName:      "orgname2",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         VarChar,
			}},
		},
	}, {
		name: "all fields - strip to type and name",
		in: &Result{
			Fields: []*querypb.Field{{
				Name:         "field1",
				Table:        "table1",
				OrgTable:     "orgtable1",
				OrgName:      "orgname1",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         Int64,
			}, {
				Name:         "field2",
				Table:        "table2",
				OrgTable:     "orgtable2",
				OrgName:      "orgname2",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Name: "field1",
				Type: Int64,
			}, {
				Name: "field2",
				Type: VarChar,
			}},
		},
	}}
	for _, tcase := range testcases {
		inCopy := tcase.in.Copy()
		out := inCopy.StripMetadata(tcase.includedFields)
		if !reflect.DeepEqual(out, tcase.expected) {
			t.Errorf("StripMetaData unexpected result for %v: %v", tcase.name, out)
		}
		if len(tcase.in.Fields) > 0 {
			// check the out array is different than the in array.
			if out.Fields[0] == inCopy.Fields[0] && tcase.includedFields != querypb.ExecuteOptions_ALL {
				t.Errorf("StripMetaData modified original Field for %v", tcase.name)
			}
		}
		// check we didn't change the original result.
		if !reflect.DeepEqual(tcase.in, inCopy) {
			t.Error("StripMetaData modified original result")
		}
	}
}
