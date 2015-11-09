// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/proto/query"
)

type TestCase struct {
	qr           QueryResult
	encoded      string
	unmarshalled QueryResult
}

var testcases = []TestCase{
	// Empty
	{
		qr:      QueryResult{},
		encoded: "J\x00\x00\x00\x04Fields\x00\x05\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x00\x05\x00\x00\x00\x00\nErr\x00\x00",
		unmarshalled: QueryResult{
			Fields: []*query.Field{},
			Rows:   [][]sqltypes.Value{},
		},
	},
	// Only fields set
	{
		qr: QueryResult{
			Fields: []*query.Field{
				{Name: "foo", Type: sqltypes.Int8},
			},
		},
		encoded: "}\x00\x00\x00\x04Fields\x008\x00\x00\x00\x030\x000\x00\x00\x00\x05Name\x00\x03\x00\x00\x00\x00foo\x12Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x12Flags\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x00\x05\x00\x00\x00\x00\nErr\x00\x00",
		unmarshalled: QueryResult{
			Fields: []*query.Field{
				{Name: "foo", Type: sqltypes.Int8},
			},
			Rows: [][]sqltypes.Value{},
		},
	},
	// Only rows, no fields
	{
		qr: QueryResult{
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("abcd")), sqltypes.MakeNumeric([]byte("1234")), sqltypes.MakeFractional([]byte("1.234"))},
			},
		},
		encoded: "w\x00\x00\x00\x04Fields\x00\x05\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x002\x00\x00\x00\x040\x00*\x00\x00\x00\x050\x00\x04\x00\x00\x00\x00abcd\x051\x00\x04\x00\x00\x00\x001234\x052\x00\x05\x00\x00\x00\x001.234\x00\x00\nErr\x00\x00",
		unmarshalled: QueryResult{
			Fields: []*query.Field{},
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("abcd")), sqltypes.MakeString([]byte("1234")), sqltypes.MakeString([]byte("1.234"))},
			},
		},
	},
	// one row and one field
	{
		qr: QueryResult{
			Fields: []*query.Field{
				{Name: "foo", Type: sqltypes.Int8},
			},
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("abcd")), sqltypes.MakeNumeric([]byte("1234")), sqltypes.MakeFractional([]byte("1.234")), sqltypes.Value{}},
			},
		},
		encoded: "",
		unmarshalled: QueryResult{
			Fields: []*query.Field{
				{Name: "foo", Type: sqltypes.Int8},
			},
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("abcd")), sqltypes.MakeString([]byte("1234")), sqltypes.MakeString([]byte("1.234")), sqltypes.Value{}},
			},
		},
	},
	// two rows and two fields
	{
		qr: QueryResult{
			Fields: []*query.Field{
				{Name: "foo", Type: sqltypes.Int8},
				{Name: "bar", Type: sqltypes.Int16},
			},
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("abcd")), sqltypes.MakeNumeric([]byte("1234")), sqltypes.MakeFractional([]byte("1.234")), sqltypes.Value{}},
			},
		},
		encoded: "",
		unmarshalled: QueryResult{
			Fields: []*query.Field{
				{Name: "foo", Type: sqltypes.Int8},
				{Name: "bar", Type: sqltypes.Int16},
			},
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("abcd")), sqltypes.MakeString([]byte("1234")), sqltypes.MakeString([]byte("1.234")), sqltypes.Value{}},
			},
		},
	},
	// With an error
	{
		qr: QueryResult{
			Err: &RPCError{1000, "generic error"},
		},
		encoded: "x\x00\x00\x00\x04Fields\x00\x05\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x00\x05\x00\x00\x00\x00\x03Err\x00.\x00\x00\x00\x12Code\x00\xe8\x03\x00\x00\x00\x00\x00\x00\x05Message\x00\r\x00\x00\x00\x00generic error\x00\x00",
		unmarshalled: QueryResult{
			Fields: []*query.Field{},
			Rows:   [][]sqltypes.Value{},
			Err:    &RPCError{1000, "generic error"},
		},
	},
}

func TestRun(t *testing.T) {
	for caseno, tcase := range testcases {
		actual, err := bson.Marshal(&tcase.qr)
		if err != nil {
			t.Errorf("Error on %d: %v", caseno, err)
		}
		if tcase.encoded != "" && string(actual) != tcase.encoded {
			t.Errorf("Expecting vs actual for %d:\n%#v\n%#v", caseno, tcase.encoded, string(actual))
		}
		var newqr QueryResult
		err = bson.Unmarshal(actual, &newqr)
		if err != nil {
			t.Errorf("Error on %d: %v", caseno, err)
		}
		if !reflect.DeepEqual(newqr, tcase.unmarshalled) {
			t.Errorf("Case: %d,\n%#v, want\n%#v", caseno, newqr, tcase.unmarshalled)
		}
	}
}
