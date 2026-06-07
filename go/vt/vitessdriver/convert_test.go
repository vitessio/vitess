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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestToNative(t *testing.T) {
	convertTimeLocal := &converter{
		location: time.Local,
	}

	testcases := []struct {
		convert *converter
		in      sqltypes.Value
		out     any
	}{
		{
			convert: &converter{},
			in:      sqltypes.NULL,
			out:     nil,
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Int8, "1"),
			out:     int64(1),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Int16, "1"),
			out:     int64(1),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Int24, "1"),
			out:     int64(1),
		}, {
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Int32, "1"),
			out:     int64(1),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Int64, "1"),
			out:     int64(1),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Uint8, "1"),
			out:     uint64(1),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Uint16, "1"),
			out:     uint64(1),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Uint24, "1"),
			out:     uint64(1),
		}, {
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Uint32, "1"),
			out:     uint64(1),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Uint64, "1"),
			out:     uint64(1),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Float32, "1.1"),
			out:     float64(1.1),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Float64, "1.1"),
			out:     float64(1.1),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Timestamp, "2012-02-24 23:19:43"),
			out:     time.Date(2012, 02, 24, 23, 19, 43, 0, time.UTC),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Time, "23:19:43"),
			out:     []byte("23:19:43"), // TIME is not handled
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Date, "2012-02-24"),
			out:     time.Date(2012, 02, 24, 0, 0, 0, 0, time.UTC),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Datetime, "2012-02-24 23:19:43"),
			out:     time.Date(2012, 02, 24, 23, 19, 43, 0, time.UTC),
		},
		{
			convert: convertTimeLocal,
			in:      sqltypes.TestValue(sqltypes.Datetime, "2012-02-24 23:19:43"),
			out:     time.Date(2012, 02, 24, 23, 19, 43, 0, time.Local),
		},
		{
			convert: convertTimeLocal,
			in:      sqltypes.TestValue(sqltypes.Date, "2012-02-24"),
			out:     time.Date(2012, 02, 24, 0, 0, 0, 0, time.Local),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Year, "1"),
			out:     uint64(1),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Decimal, "1"),
			out:     []byte("1"),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Text, "a"),
			out:     []byte("a"),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Blob, "a"),
			out:     "a",
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.VarChar, "a"),
			out:     []byte("a"),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.VarBinary, "a"),
			out:     "a",
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Char, "a"),
			out:     []byte("a"),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Binary, "a"),
			out:     "a",
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.VarChar, "a"),
			out:     []byte("a"),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Bit, "a"),
			out:     []byte("a"),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Enum, "a"),
			out:     []byte("a"),
		},
		{
			convert: &converter{},
			in:      sqltypes.TestValue(sqltypes.Set, "a"),
			out:     []byte("a"),
		},
	}

	for _, tcase := range testcases {
		v, err := tcase.convert.ToNative(tcase.in)
		require.NoError(t, err)
		require.Equal(t, tcase.out, v)
	}
}

func TestBuildBindVariable(t *testing.T) {
	testcases := []struct {
		name string
		in   any
		out  *querypb.BindVariable
	}{
		{
			name: "json bytes become varchar",
			in:   []byte("[]"),
			out:  sqltypes.StringBindVariable("[]"),
		},
		{
			name: "binary bytes become varchar",
			in:   []byte{0x00, 0xff},
			out: &querypb.BindVariable{
				Type:  querypb.Type_VARCHAR,
				Value: []byte{0x00, 0xff},
			},
		},
		{
			name: "nil bytes become null",
			in:   []byte(nil),
			out:  sqltypes.NullBindVariable,
		},
		{
			name: "empty bytes become empty varchar",
			in:   []byte{},
			out:  sqltypes.StringBindVariable(""),
		},
		{
			name: "time becomes datetime",
			in:   time.Date(2012, 0o2, 24, 23, 19, 43, 0, time.UTC),
			out:  sqltypes.ValueBindVariable(sqltypes.TestValue(sqltypes.Datetime, "2012-02-24 23:19:43")),
		},
	}

	convert := &converter{location: time.UTC}
	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			bv, err := convert.BuildBindVariable(tcase.in)
			require.NoError(t, err)
			require.Equal(t, tcase.out, bv)
		})
	}
}
