package vitessdriver

import (
	"reflect"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
)

func TestToNative(t *testing.T) {
	convertTimeLocal := &converter{
		location: time.Local,
	}

	testcases := []struct {
		convert *converter
		in      sqltypes.Value
		out     interface{}
	}{{
		convert: &converter{},
		in:      sqltypes.TestValue(sqltypes.Int32, "1"),
		out:     int64(1),
	}, {
		convert: &converter{},
		in:      sqltypes.TestValue(sqltypes.Timestamp, "2012-02-24 23:19:43"),
		out:     time.Date(2012, 02, 24, 23, 19, 43, 0, time.UTC),
	}, {
		convert: &converter{},
		in:      sqltypes.TestValue(sqltypes.Time, "23:19:43"),
		out:     []byte("23:19:43"), // TIME is not handled
	}, {
		convert: &converter{},
		in:      sqltypes.TestValue(sqltypes.Date, "2012-02-24"),
		out:     time.Date(2012, 02, 24, 0, 0, 0, 0, time.UTC),
	}, {
		convert: &converter{},
		in:      sqltypes.TestValue(sqltypes.Datetime, "2012-02-24 23:19:43"),
		out:     time.Date(2012, 02, 24, 23, 19, 43, 0, time.UTC),
	}, {
		convert: convertTimeLocal,
		in:      sqltypes.TestValue(sqltypes.Datetime, "2012-02-24 23:19:43"),
		out:     time.Date(2012, 02, 24, 23, 19, 43, 0, time.Local),
	}, {
		convert: convertTimeLocal,
		in:      sqltypes.TestValue(sqltypes.Date, "2012-02-24"),
		out:     time.Date(2012, 02, 24, 0, 0, 0, 0, time.Local),
	}}

	for _, tcase := range testcases {
		v, err := tcase.convert.ToNative(tcase.in)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(v, tcase.out) {
			t.Errorf("%v.ToNativeEx = %#v, want %#v", tcase.in, v, tcase.out)
		}
	}
}
