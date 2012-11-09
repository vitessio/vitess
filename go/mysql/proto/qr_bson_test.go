// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"testing"

	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/sqltypes"
)

type TestCase struct {
	qr      QueryResult
	encoded string
}

var testcases = []TestCase{
	// Empty
	{
		qr:      QueryResult{},
		encoded: "E\x00\x00\x00\x04Fields\x00\x05\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x00\x05\x00\x00\x00\x00\x00",
	},
	// Only fields set
	{
		qr: QueryResult{
			Fields: []Field{
				{Name: "foo", Type: 1},
			},
		},
		encoded: "i\x00\x00\x00\x04Fields\x00)\x00\x00\x00\x030\x00!\x00\x00\x00\x05Name\x00\x03\x00\x00\x00\x00foo\x12Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x00\x05\x00\x00\x00\x00\x00",
	},
	// Only rows, no fields
	{
		qr: QueryResult{
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("abcd")), sqltypes.MakeNumeric([]byte("1234")), sqltypes.MakeFractional([]byte("1.234"))},
			},
		},
		encoded: "r\x00\x00\x00\x04Fields\x00\x05\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x002\x00\x00\x00\x040\x00*\x00\x00\x00\x050\x00\x04\x00\x00\x00\x00abcd\x051\x00\x04\x00\x00\x00\x001234\x052\x00\x05\x00\x00\x00\x001.234\x00\x00\x00",
	},
	// one row and one field
	{
		qr: QueryResult{
			Fields: []Field{
				{Name: "foo", Type: 1},
			},
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("abcd")), sqltypes.MakeNumeric([]byte("1234")), sqltypes.MakeFractional([]byte("1.234")), sqltypes.Value{}},
			},
		},
		encoded: "",
	},
	// two rows and two fields
	{
		qr: QueryResult{
			Fields: []Field{
				{Name: "foo", Type: 1},
				{Name: "bar", Type: 2},
			},
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("abcd")), sqltypes.MakeNumeric([]byte("1234")), sqltypes.MakeFractional([]byte("1.234")), sqltypes.Value{}},
				{sqltypes.MakeNumeric([]byte("123")), sqltypes.MakeString([]byte("abc")), sqltypes.MakeFractional([]byte("1.234"))},
			},
		},
		encoded: "",
	},
}

func TestRun(t *testing.T) {
	for i, tcase := range testcases {
		test(t, i, tcase)
	}
}

func test(t *testing.T, caseno int, tcase TestCase) {
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
	compare(t, caseno, tcase.qr, newqr)
}

func compare(t *testing.T, caseno int, original, newqr QueryResult) {
	if len(original.Fields) != len(newqr.Fields) {
		goto mismatch
	}
	for i, field := range original.Fields {
		if field != newqr.Fields[i] {
			goto mismatch
		}
	}
	if original.RowsAffected != newqr.RowsAffected {
		goto mismatch
	}
	if original.RowsAffected != newqr.RowsAffected {
		goto mismatch
	}
	if len(original.Rows) != len(newqr.Rows) {
		goto mismatch
	}
	for i, row := range original.Rows {
		if len(row) != len(newqr.Rows[i]) {
			goto mismatch
		}
		for j, v := range row {
			if v.IsNull() && newqr.Rows[i][j].IsNull() {
				continue
			}
			if v.String() != newqr.Rows[i][j].String() {
				goto mismatch
			}
		}
	}
	return

mismatch:
	t.Errorf("mismatch on %d:\n%v\n%v", caseno, original, newqr)
}
