/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vitessdriver

import (
	"database/sql/driver"
	"io"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var rowsResult1 = sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "field1",
			Type: sqltypes.Int32,
		},
		{
			Name: "field2",
			Type: sqltypes.Float32,
		},
		{
			Name: "field3",
			Type: sqltypes.VarChar,
		},
		// Signed types which are smaller than uint64, will become an int64.
		{
			Name: "field4",
			Type: sqltypes.Uint32,
		},
		// Signed uint64 values must be mapped to uint64.
		{
			Name: "field5",
			Type: sqltypes.Uint64,
		},
	},
	RowsAffected: 2,
	InsertID:     0,
	Rows: [][]sqltypes.Value{
		{
			sqltypes.NewInt32(1),
			sqltypes.TestValue(sqltypes.Float32, "1.1"),
			sqltypes.NewVarChar("value1"),
			sqltypes.TestValue(sqltypes.Uint32, "2147483647"), // 2^31-1, NOT out of range for int32 => should become int64
			sqltypes.NewUint64(9223372036854775807),           // 2^63-1, NOT out of range for int64
		},
		{
			sqltypes.NewInt32(2),
			sqltypes.TestValue(sqltypes.Float32, "2.2"),
			sqltypes.NewVarChar("value2"),
			sqltypes.TestValue(sqltypes.Uint32, "4294967295"), // 2^32-1, out of range for int32 => should become int64
			sqltypes.NewUint64(18446744073709551615),          // 2^64-1, out of range for int64
		},
	},
}

func logMismatchedTypes(t *testing.T, gotRow, wantRow []driver.Value) {
	for i := 1; i < len(wantRow); i++ {
		got := gotRow[i]
		want := wantRow[i]
		v1 := reflect.ValueOf(got)
		v2 := reflect.ValueOf(want)
		if v1.Type() != v2.Type() {
			t.Errorf("Wrong type: field: %d got: %T want: %T", i+1, got, want)
		}
	}
}

func TestRows(t *testing.T) {
	ri := newRows(&rowsResult1, &converter{})
	wantCols := []string{
		"field1",
		"field2",
		"field3",
		"field4",
		"field5",
	}
	gotCols := ri.Columns()
	if !reflect.DeepEqual(gotCols, wantCols) {
		t.Errorf("cols: %v, want %v", gotCols, wantCols)
	}

	wantRow := []driver.Value{
		int64(1),
		float64(1.1),
		[]byte("value1"),
		uint64(2147483647),
		uint64(9223372036854775807),
	}
	gotRow := make([]driver.Value, len(wantRow))
	err := ri.Next(gotRow)
	require.NoError(t, err)
	if !reflect.DeepEqual(gotRow, wantRow) {
		t.Errorf("row1: %#v, want %#v type: %T", gotRow, wantRow, wantRow[3])
		logMismatchedTypes(t, gotRow, wantRow)
	}

	wantRow = []driver.Value{
		int64(2),
		float64(2.2),
		[]byte("value2"),
		uint64(4294967295),
		uint64(18446744073709551615),
	}
	err = ri.Next(gotRow)
	require.NoError(t, err)
	if !reflect.DeepEqual(gotRow, wantRow) {
		t.Errorf("row1: %v, want %v", gotRow, wantRow)
		logMismatchedTypes(t, gotRow, wantRow)
	}

	err = ri.Next(gotRow)
	if err != io.EOF {
		t.Errorf("got: %v, want %v", err, io.EOF)
	}

	_ = ri.Close()
}
