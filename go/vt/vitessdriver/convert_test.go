/*
Copyright 2017 GitHub Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
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

	"github.com/youtube/vitess/go/sqltypes"
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
		out:     []byte("2012-02-24 23:19:43"), // TIMESTAMP is not handled
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
