// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/proto/query"
)

func TestConvert(t *testing.T) {
	cases := []struct {
		Field *query.Field
		Val   sqltypes.Value
		Want  interface{}
	}{{
		Field: &query.Field{Name: "null", Type: sqltypes.Null},
		Val:   sqltypes.Value{},
		Want:  nil,
	}, {
		Field: &query.Field{Name: "decimal", Type: sqltypes.Decimal},
		Val:   sqltypes.MakeString([]byte("aa")),
		Want:  "aa",
	}, {
		Field: &query.Field{Name: "tiny", Type: sqltypes.Int8},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: &query.Field{Name: "short", Type: sqltypes.Int16},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: &query.Field{Name: "long", Type: sqltypes.Int32},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: &query.Field{Name: "unsigned long", Type: sqltypes.Uint8},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  uint64(1),
	}, {
		Field: &query.Field{Name: "longlong", Type: sqltypes.Int64},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: &query.Field{Name: "int24", Type: sqltypes.Int24},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: &query.Field{Name: "float", Type: sqltypes.Float32},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  float64(1),
	}, {
		Field: &query.Field{Name: "double", Type: sqltypes.Float64},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  float64(1),
	}, {
		Field: &query.Field{Name: "large int out of range for int64", Type: sqltypes.Int64},
		// 2^63, out of range for int64
		Val:  sqltypes.MakeString([]byte("9223372036854775808")),
		Want: `strconv.ParseInt: parsing "9223372036854775808": value out of range`,
	}, {
		Field: &query.Field{Name: "large int", Type: sqltypes.Uint64},
		// 2^63, not out of range for uint64
		Val:  sqltypes.MakeString([]byte("9223372036854775808")),
		Want: uint64(9223372036854775808),
	}, {
		Field: &query.Field{Name: "float for int", Type: sqltypes.Int64},
		Val:   sqltypes.MakeString([]byte("1.1")),
		Want:  `strconv.ParseInt: parsing "1.1": invalid syntax`,
	}, {
		Field: &query.Field{Name: "string for float", Type: sqltypes.Float64},
		Val:   sqltypes.MakeString([]byte("aa")),
		Want:  `strconv.ParseFloat: parsing "aa": invalid syntax`,
	}}

	for _, c := range cases {
		r, err := Convert(c.Field, c.Val)
		if err != nil {
			r = err.Error()
		} else if _, ok := r.([]byte); ok {
			r = string(r.([]byte))
		}
		if r != c.Want {
			t.Errorf("%s: %+v, want %+v", c.Field.Name, r, c.Want)
		}
	}
}
