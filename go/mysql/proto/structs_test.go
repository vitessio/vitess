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
		Desc string
		Typ  int64
		Val  sqltypes.Value
		Want interface{}
	}{{
		Desc: "null",
		Typ:  VT_LONG,
		Val:  sqltypes.Value{},
		Want: nil,
	}, {
		Desc: "decimal",
		Typ:  VT_DECIMAL,
		Val:  sqltypes.MakeString([]byte("aa")),
		Want: "aa",
	}, {
		Desc: "tiny",
		Typ:  VT_TINY,
		Val:  sqltypes.MakeString([]byte("1")),
		Want: int64(1),
	}, {
		Desc: "short",
		Typ:  VT_SHORT,
		Val:  sqltypes.MakeString([]byte("1")),
		Want: int64(1),
	}, {
		Desc: "long",
		Typ:  VT_LONG,
		Val:  sqltypes.MakeString([]byte("1")),
		Want: int64(1),
	}, {
		Desc: "longlong",
		Typ:  VT_LONGLONG,
		Val:  sqltypes.MakeString([]byte("1")),
		Want: int64(1),
	}, {
		Desc: "int24",
		Typ:  VT_INT24,
		Val:  sqltypes.MakeString([]byte("1")),
		Want: int64(1),
	}, {
		Desc: "float",
		Typ:  VT_FLOAT,
		Val:  sqltypes.MakeString([]byte("1")),
		Want: float64(1),
	}, {
		Desc: "double",
		Typ:  VT_DOUBLE,
		Val:  sqltypes.MakeString([]byte("1")),
		Want: float64(1),
	}, {
		Desc: "large int",
		Typ:  VT_LONGLONG,
		Val:  sqltypes.MakeString([]byte("9223372036854775808")),
		Want: uint64(9223372036854775808),
	}, {
		Desc: "float for int",
		Typ:  VT_LONGLONG,
		Val:  sqltypes.MakeString([]byte("1.1")),
		Want: `strconv.ParseUint: parsing "1.1": invalid syntax`,
	}, {
		Desc: "string for float",
		Typ:  VT_FLOAT,
		Val:  sqltypes.MakeString([]byte("aa")),
		Want: `strconv.ParseFloat: parsing "aa": invalid syntax`,
	}}

	for _, c := range cases {
		r, err := Convert(c.Typ, c.Val)
		if err != nil {
			r = err.Error()
		} else if _, ok := r.([]byte); ok {
			r = string(r.([]byte))
		}
		if r != c.Want {
			t.Errorf("%s: %+v, want %+v", c.Desc, r, c.Want)
		}
	}
}
