package client

import (
	"database/sql/driver"
	"io"
	"reflect"
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
)

var rowsResult1 = mproto.QueryResult{
	Fields: []mproto.Field{
		mproto.Field{
			Name: "field1",
			Type: mproto.VT_LONG,
		},
		mproto.Field{
			Name: "field2",
			Type: mproto.VT_FLOAT,
		},
		mproto.Field{
			Name: "field3",
			Type: mproto.VT_VAR_STRING,
		},
		// Signed types which are smaller than uint64, will become an int64.
		mproto.Field{
			Name:  "field4",
			Type:  mproto.VT_LONG,
			Flags: mproto.VT_UNSIGNED_FLAG,
		},
		// Signed uint64 values must be mapped to uint64.
		mproto.Field{
			Name:  "field5",
			Type:  mproto.VT_LONGLONG,
			Flags: mproto.VT_UNSIGNED_FLAG,
		},
	},
	RowsAffected: 2,
	InsertId:     0,
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("1.1")),
			sqltypes.MakeString([]byte("value1")),
			sqltypes.MakeString([]byte("2147483647")),          // 2^31-1, NOT out of range for int32 => should become int64
			sqltypes.MakeString([]byte("9223372036854775807")), // 2^63-1, NOT out of range for int64
		},
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("2")),
			sqltypes.MakeString([]byte("2.2")),
			sqltypes.MakeString([]byte("value2")),
			sqltypes.MakeString([]byte("4294967295")),           // 2^32, out of range for int32 => should become int64
			sqltypes.MakeString([]byte("18446744073709551615")), // 2^64, out of range for int64
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
	ri := newRows(&rowsResult1)
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
		int64(2147483647),
		uint64(9223372036854775807),
	}
	gotRow := make([]driver.Value, len(wantRow))
	err := ri.Next(gotRow)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(gotRow, wantRow) {
		t.Errorf("row1: %v, want %v type: %T", gotRow, wantRow, wantRow[3])
		logMismatchedTypes(t, gotRow, wantRow)
	}

	wantRow = []driver.Value{
		int64(2),
		float64(2.2),
		[]byte("value2"),
		int64(4294967295),
		uint64(18446744073709551615),
	}
	err = ri.Next(gotRow)
	if err != nil {
		t.Error(err)
	}
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

var badResult1 = mproto.QueryResult{
	Fields: []mproto.Field{
		mproto.Field{},
	},
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{},
	},
}

var badResult2 = mproto.QueryResult{
	Fields: []mproto.Field{
		mproto.Field{
			Name: "field1",
			Type: mproto.VT_LONG,
		},
	},
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("value")),
		},
	},
}

func TestRowsFail(t *testing.T) {
	ri := newRows(&badResult1)
	var dest []driver.Value
	err := ri.Next(dest)
	want := "length mismatch: dest is 0, fields are 1"
	if err == nil || err.Error() != want {
		t.Errorf("Next: %v, want %s", err, want)
	}

	ri = newRows(&badResult1)
	dest = make([]driver.Value, 1)
	err = ri.Next(dest)
	want = "internal error: length mismatch: dest is 1, fields are 0"
	if err == nil || err.Error() != want {
		t.Errorf("Next: %v, want %s", err, want)
	}

	ri = newRows(&badResult2)
	dest = make([]driver.Value, 1)
	err = ri.Next(dest)
	want = `conversion error: field: {field1 3 0}, val: value: strconv.ParseInt: parsing "value": invalid syntax`
	if err == nil || err.Error() != want {
		t.Errorf("Next: %v, want %s", err, want)
	}
}
