// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"testing"

	"github.com/youtube/vitess/go/vt/proto/query"
)

func TestConvert(t *testing.T) {
	cases := []struct {
		Field *query.Field
		Val   Value
		Want  interface{}
	}{{
		Field: &query.Field{Name: "null", Type: Null},
		Val:   Value{},
		Want:  nil,
	}, {
		Field: &query.Field{Name: "decimal", Type: Decimal},
		Val:   MakeString([]byte("aa")),
		Want:  "aa",
	}, {
		Field: &query.Field{Name: "tiny", Type: Int8},
		Val:   MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: &query.Field{Name: "short", Type: Int16},
		Val:   MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: &query.Field{Name: "long", Type: Int32},
		Val:   MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: &query.Field{Name: "unsigned long", Type: Uint8},
		Val:   MakeString([]byte("1")),
		Want:  uint64(1),
	}, {
		Field: &query.Field{Name: "longlong", Type: Int64},
		Val:   MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: &query.Field{Name: "int24", Type: Int24},
		Val:   MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: &query.Field{Name: "float", Type: Float32},
		Val:   MakeString([]byte("1")),
		Want:  float64(1),
	}, {
		Field: &query.Field{Name: "double", Type: Float64},
		Val:   MakeString([]byte("1")),
		Want:  float64(1),
	}, {
		Field: &query.Field{Name: "large int out of range for int64", Type: Int64},
		// 2^63, out of range for int64
		Val:  MakeString([]byte("9223372036854775808")),
		Want: `strconv.ParseInt: parsing "9223372036854775808": value out of range`,
	}, {
		Field: &query.Field{Name: "large int", Type: Uint64},
		// 2^63, not out of range for uint64
		Val:  MakeString([]byte("9223372036854775808")),
		Want: uint64(9223372036854775808),
	}, {
		Field: &query.Field{Name: "float for int", Type: Int64},
		Val:   MakeString([]byte("1.1")),
		Want:  `strconv.ParseInt: parsing "1.1": invalid syntax`,
	}, {
		Field: &query.Field{Name: "string for float", Type: Float64},
		Val:   MakeString([]byte("aa")),
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
