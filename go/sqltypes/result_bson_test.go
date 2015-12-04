// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/bson"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

type TestCase struct {
	qr      Result
	encoded string
}

var testcases = []TestCase{
	// Empty
	{
		qr:      Result{},
		encoded: "E\x00\x00\x00\x04Fields\x00\x05\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x00\x05\x00\x00\x00\x00\x00",
	},
	// Only fields set
	{
		qr: Result{
			Fields: []*querypb.Field{
				{Name: "foo", Type: Int8},
			},
		},
		encoded: "x\x00\x00\x00\x04Fields\x008\x00\x00\x00\x030\x000\x00\x00\x00\x05Name\x00\x03\x00\x00\x00\x00foo\x12Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x12Flags\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12RowsAffected\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04Rows\x00\x05\x00\x00\x00\x00\x00",
	},
	// two rows
	{
		qr: Result{
			Fields: []*querypb.Field{
				{Name: "foo", Type: VarChar},
				{Name: "bar", Type: Int64},
				{Name: "baz", Type: Float64},
			},
			Rows: [][]Value{
				{testVal(VarBinary, "abcd"), testVal(VarBinary, "1234"), testVal(VarBinary, "1.234")},
				{testVal(VarBinary, "efgh"), testVal(VarBinary, "5678"), testVal(VarBinary, "5.678")},
			},
		},
		encoded: "",
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
		if len(newqr.Fields) == 0 {
			newqr.Fields = nil
		}
		if len(newqr.Rows) == 0 {
			newqr.Rows = nil
		}
		if !reflect.DeepEqual(newqr, tcase.qr) {
			t.Errorf("Case: %d,\n%#v, want\n%#v", caseno, newqr, tcase.qr)
		}
	}
}
