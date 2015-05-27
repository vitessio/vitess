// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
)

func TestConvert(t *testing.T) {
	cases := []struct {
		Field Field
		Val   sqltypes.Value
		Want  interface{}
	}{{
		Field: Field{"null", VT_LONG, VT_ZEROVALUE_FLAG},
		Val:   sqltypes.Value{},
		Want:  nil,
	}, {
		Field: Field{"decimal", VT_DECIMAL, VT_ZEROVALUE_FLAG},
		Val:   sqltypes.MakeString([]byte("aa")),
		Want:  "aa",
	}, {
		Field: Field{"tiny", VT_TINY, VT_ZEROVALUE_FLAG},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: Field{"short", VT_SHORT, VT_ZEROVALUE_FLAG},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: Field{"long", VT_LONG, VT_ZEROVALUE_FLAG},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: Field{"unsigned long", VT_LONG, VT_UNSIGNED_FLAG},
		Val:   sqltypes.MakeString([]byte("1")),
		// Unsigned types which aren't VT_LONGLONG are mapped to int64.
		Want: int64(1),
	}, {
		Field: Field{"longlong", VT_LONGLONG, VT_ZEROVALUE_FLAG},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: Field{"int24", VT_INT24, VT_ZEROVALUE_FLAG},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  int64(1),
	}, {
		Field: Field{"float", VT_FLOAT, VT_ZEROVALUE_FLAG},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  float64(1),
	}, {
		Field: Field{"double", VT_DOUBLE, VT_ZEROVALUE_FLAG},
		Val:   sqltypes.MakeString([]byte("1")),
		Want:  float64(1),
	}, {
		Field: Field{"large int out of range for int64", VT_LONGLONG, VT_ZEROVALUE_FLAG},
		// 2^63, out of range for int64
		Val:  sqltypes.MakeString([]byte("9223372036854775808")),
		Want: `strconv.ParseInt: parsing "9223372036854775808": value out of range`,
	}, {
		Field: Field{"large int", VT_LONGLONG, VT_UNSIGNED_FLAG},
		// 2^63, not out of range for uint64
		Val:  sqltypes.MakeString([]byte("9223372036854775808")),
		Want: uint64(9223372036854775808),
	}, {
		Field: Field{"float for int", VT_LONGLONG, VT_ZEROVALUE_FLAG},
		Val:   sqltypes.MakeString([]byte("1.1")),
		Want:  `strconv.ParseInt: parsing "1.1": invalid syntax`,
	}, {
		Field: Field{"string for float", VT_FLOAT, VT_ZEROVALUE_FLAG},
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
