package vtgateconn

import (
	"database/sql/driver"
	"io"
	"reflect"
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
)

var result1 = mproto.QueryResult{
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
	},
	RowsAffected: 3,
	InsertId:     0,
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("1.1")),
			sqltypes.MakeString([]byte("value1")),
		},
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("2")),
			sqltypes.MakeString([]byte("2.2")),
			sqltypes.MakeString([]byte("value2")),
		},
	},
}

func TestRows(t *testing.T) {
	ri := NewRows(&result1)
	wantCols := []string{
		"field1",
		"field2",
		"field3",
	}
	gotCols := ri.Columns()
	if !reflect.DeepEqual(gotCols, wantCols) {
		t.Errorf("cols: %v, want %v", gotCols, wantCols)
	}

	wantRow := []driver.Value{
		int64(1),
		float64(1.1),
		[]byte("value1"),
	}
	gotRow := make([]driver.Value, 3)
	err := ri.Next(gotRow)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(gotRow, wantRow) {
		t.Errorf("row1: %v, want %v", gotRow, wantRow)
	}

	wantRow = []driver.Value{
		int64(2),
		float64(2.2),
		[]byte("value2"),
	}
	err = ri.Next(gotRow)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(gotRow, wantRow) {
		t.Errorf("row1: %v, want %v", gotRow, wantRow)
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
	ri := NewRows(&badResult1)
	var dest []driver.Value
	err := ri.Next(dest)
	want := "length mismatch: dest is 0, fields are 1"
	if err == nil || err.Error() != want {
		t.Errorf("Next: %v, want %s", err, want)
	}

	ri = NewRows(&badResult1)
	dest = make([]driver.Value, 1)
	err = ri.Next(dest)
	want = "internal error: length mismatch: dest is 1, fields are 0"
	if err == nil || err.Error() != want {
		t.Errorf("Next: %v, want %s", err, want)
	}

	ri = NewRows(&badResult2)
	dest = make([]driver.Value, 1)
	err = ri.Next(dest)
	want = `conversion error: field: {field1 3}, val: value: strconv.ParseUint: parsing "value": invalid syntax`
	if err == nil || err.Error() != want {
		t.Errorf("Next: %v, want %s", err, want)
	}
}
