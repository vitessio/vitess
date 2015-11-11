// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/vt/proto/query"
)

type TestCase struct {
	qr           Result
	encoded      string
	unmarshalled Result
}

var testcases = []TestCase{
	// Empty
	{
		qr:      Result{},
		encoded: "E\x00\x00\x00\x04Fields\x00\x05\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x00\x05\x00\x00\x00\x00\x00",
		unmarshalled: Result{
			Fields: []*query.Field{},
			Rows:   [][]Value{},
		},
	},
	// Only fields set
	{
		qr: Result{
			Fields: []*query.Field{
				{Name: "foo", Type: Int8},
			},
		},
		encoded: "x\x00\x00\x00\x04Fields\x008\x00\x00\x00\x030\x000\x00\x00\x00\x05Name\x00\x03\x00\x00\x00\x00foo\x12Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x12Flags\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x00\x05\x00\x00\x00\x00\x00",
		unmarshalled: Result{
			Fields: []*query.Field{
				{Name: "foo", Type: Int8},
			},
			Rows: [][]Value{},
		},
	},
	// Only rows, no fields
	{
		qr: Result{
			Rows: [][]Value{
				{MakeString([]byte("abcd")), MakeNumeric([]byte("1234")), MakeFractional([]byte("1.234"))},
			},
		},
		encoded: "r\x00\x00\x00\x04Fields\x00\x05\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x002\x00\x00\x00\x040\x00*\x00\x00\x00\x050\x00\x04\x00\x00\x00\x00abcd\x051\x00\x04\x00\x00\x00\x001234\x052\x00\x05\x00\x00\x00\x001.234\x00\x00\x00",
		unmarshalled: Result{
			Fields: []*query.Field{},
			Rows: [][]Value{
				{MakeString([]byte("abcd")), MakeString([]byte("1234")), MakeString([]byte("1.234"))},
			},
		},
	},
	// one row and one field
	{
		qr: Result{
			Fields: []*query.Field{
				{Name: "foo", Type: Int8},
			},
			Rows: [][]Value{
				{MakeString([]byte("abcd")), MakeNumeric([]byte("1234")), MakeFractional([]byte("1.234")), Value{}},
			},
		},
		encoded: "",
		unmarshalled: Result{
			Fields: []*query.Field{
				{Name: "foo", Type: Int8},
			},
			Rows: [][]Value{
				{MakeString([]byte("abcd")), MakeString([]byte("1234")), MakeString([]byte("1.234")), Value{}},
			},
		},
	},
	// two rows and two fields
	{
		qr: Result{
			Fields: []*query.Field{
				{Name: "foo", Type: Int8},
				{Name: "bar", Type: Int16},
			},
			Rows: [][]Value{
				{MakeString([]byte("abcd")), MakeNumeric([]byte("1234")), MakeFractional([]byte("1.234")), Value{}},
			},
		},
		encoded: "",
		unmarshalled: Result{
			Fields: []*query.Field{
				{Name: "foo", Type: Int8},
				{Name: "bar", Type: Int16},
			},
			Rows: [][]Value{
				{MakeString([]byte("abcd")), MakeString([]byte("1234")), MakeString([]byte("1.234")), Value{}},
			},
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
		var newqr Result
		err = bson.Unmarshal(actual, &newqr)
		if err != nil {
			t.Errorf("Error on %d: %v", caseno, err)
		}
		if !reflect.DeepEqual(newqr, tcase.unmarshalled) {
			t.Errorf("Case: %d,\n%#v, want\n%#v", caseno, newqr, tcase.unmarshalled)
		}
	}
}
