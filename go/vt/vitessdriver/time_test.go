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
	"reflect"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
)

var randomLocation = time.FixedZone("Nowhere", 3*60*60)

func DatetimeValue(str string) sqltypes.Value {
	return sqltypes.TestValue(sqltypes.Datetime, str)
}

func DateValue(str string) sqltypes.Value {
	return sqltypes.TestValue(sqltypes.Date, str)
}

func TestDatetimeToNative(t *testing.T) {

	tcases := []struct {
		val sqltypes.Value
		loc *time.Location
		out time.Time
		err bool
	}{{
		val: DatetimeValue("1899-08-24 17:20:00"),
		out: time.Date(1899, 8, 24, 17, 20, 0, 0, time.UTC),
	}, {
		val: DatetimeValue("1952-03-11 01:02:03"),
		loc: time.Local,
		out: time.Date(1952, 3, 11, 1, 2, 3, 0, time.Local),
	}, {
		val: DatetimeValue("1952-03-11 01:02:03"),
		loc: randomLocation,
		out: time.Date(1952, 3, 11, 1, 2, 3, 0, randomLocation),
	}, {
		val: DatetimeValue("1952-03-11 01:02:03"),
		loc: time.UTC,
		out: time.Date(1952, 3, 11, 1, 2, 3, 0, time.UTC),
	}, {
		val: DatetimeValue("1899-08-24 17:20:00.000000"),
		out: time.Date(1899, 8, 24, 17, 20, 0, 0, time.UTC),
	}, {
		val: DatetimeValue("1899-08-24 17:20:00.000001"),
		out: time.Date(1899, 8, 24, 17, 20, 0, int(1*time.Microsecond), time.UTC),
	}, {
		val: DatetimeValue("1899-08-24 17:20:00.123456"),
		out: time.Date(1899, 8, 24, 17, 20, 0, int(123456*time.Microsecond), time.UTC),
	}, {
		val: DatetimeValue("1899-08-24 17:20:00.222"),
		out: time.Date(1899, 8, 24, 17, 20, 0, int(222*time.Millisecond), time.UTC),
	}, {
		val: DatetimeValue("1899-08-24 17:20:00.1234567"),
		err: true,
	}, {
		val: DatetimeValue("1899-08-24 17:20:00.1"),
		out: time.Date(1899, 8, 24, 17, 20, 0, int(100*time.Millisecond), time.UTC),
	}, {
		val: DatetimeValue("0000-00-00 00:00:00"),
		out: time.Time{},
	}, {
		val: DatetimeValue("0000-00-00 00:00:00.0"),
		out: time.Time{},
	}, {
		val: DatetimeValue("0000-00-00 00:00:00.000"),
		out: time.Time{},
	}, {
		val: DatetimeValue("0000-00-00 00:00:00.000000"),
		out: time.Time{},
	}, {
		val: DatetimeValue("0000-00-00 00:00:00.0000000"),
		err: true,
	}, {
		val: DatetimeValue("1899-08-24T17:20:00.000000"),
		err: true,
	}, {
		val: DatetimeValue("1899-02-31 17:20:00.000000"),
		err: true,
	}, {
		val: DatetimeValue("1899-08-24 17:20:00."),
		out: time.Date(1899, 8, 24, 17, 20, 0, 0, time.UTC),
	}, {
		val: DatetimeValue("0000-00-00 00:00:00.000001"),
		err: true,
	}, {
		val: DatetimeValue("1899-08-24 17:20:00 +02:00"),
		err: true,
	}, {
		val: DatetimeValue("1899-08-24"),
		err: true,
	}, {
		val: DatetimeValue("This is not a valid timestamp"),
		err: true,
	}}

	for _, tcase := range tcases {
		got, err := DatetimeToNative(tcase.val, tcase.loc)
		if tcase.err && err == nil {
			t.Errorf("DatetimeToNative(%v, %#v) succeeded; expected error", tcase.val, tcase.loc)
		}
		if !tcase.err && err != nil {
			t.Errorf("DatetimeToNative(%v, %#v) failed: %v", tcase.val, tcase.loc, err)
		}
		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("DatetimeToNative(%v, %#v): %v, want %v", tcase.val, tcase.loc, got, tcase.out)
		}
	}
}

func TestDateToNative(t *testing.T) {
	tcases := []struct {
		val sqltypes.Value
		loc *time.Location
		out time.Time
		err bool
	}{{
		val: DateValue("1899-08-24"),
		out: time.Date(1899, 8, 24, 0, 0, 0, 0, time.UTC),
	}, {
		val: DateValue("1952-03-11"),
		loc: time.Local,
		out: time.Date(1952, 3, 11, 0, 0, 0, 0, time.Local),
	}, {
		val: DateValue("1952-03-11"),
		loc: randomLocation,
		out: time.Date(1952, 3, 11, 0, 0, 0, 0, randomLocation),
	}, {
		val: DateValue("0000-00-00"),
		out: time.Time{},
	}, {
		val: DateValue("1899-02-31"),
		err: true,
	}, {
		val: DateValue("1899-08-24 17:20:00"),
		err: true,
	}, {
		val: DateValue("0000-00-00 00:00:00"),
		err: true,
	}, {
		val: DateValue("This is not a valid timestamp"),
		err: true,
	}}

	for _, tcase := range tcases {
		got, err := DateToNative(tcase.val, tcase.loc)
		if tcase.err && err == nil {
			t.Errorf("DateToNative(%v, %#v) succeeded; expected error", tcase.val, tcase.loc)
		}
		if !tcase.err && err != nil {
			t.Errorf("DateToNative(%v, %#v) failed: %v", tcase.val, tcase.loc, err)
		}
		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("DateToNative(%v, %#v): %v, want %v", tcase.val, tcase.loc, got, tcase.out)
		}
	}
}
