// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"errors"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
)

func TestGetTableName(t *testing.T) {
	testcases := []struct {
		in, out string
	}{{
		in:  "select * from t",
		out: "t",
	}, {
		in:  "select * from t.t",
		out: "",
	}, {
		in:  "select * from (select * from t) as tt",
		out: "",
	}}

	for _, tc := range testcases {
		tree, err := Parse(tc.in)
		if err != nil {
			t.Error(err)
			continue
		}
		out := GetTableName(tree.(*Select).From[0].(*AliasedTableExpr).Expr)
		if out.String() != tc.out {
			t.Errorf("GetTableName('%s'): %s, want %s", tc.in, out, tc.out)
		}
	}
}

func TestIsColName(t *testing.T) {
	testcases := []struct {
		in  ValExpr
		out bool
	}{{
		in:  &ColName{},
		out: true,
	}, {
		in: newHexVal(""),
	}}
	for _, tc := range testcases {
		out := IsColName(tc.in)
		if out != tc.out {
			t.Errorf("IsColName(%T): %v, want %v", tc.in, out, tc.out)
		}
	}
}

func TestIsValue(t *testing.T) {
	testcases := []struct {
		in  ValExpr
		out bool
	}{{
		in:  newStrVal(""),
		out: true,
	}, {
		in:  newHexVal(""),
		out: true,
	}, {
		in:  newIntVal(""),
		out: true,
	}, {
		in:  newValArg(""),
		out: true,
	}, {
		in: &NullVal{},
	}}
	for _, tc := range testcases {
		out := IsValue(tc.in)
		if out != tc.out {
			t.Errorf("IsValue(%T): %v, want %v", tc.in, out, tc.out)
		}
	}
}

func TestIsNull(t *testing.T) {
	testcases := []struct {
		in  ValExpr
		out bool
	}{{
		in:  &NullVal{},
		out: true,
	}, {
		in: newStrVal(""),
	}}
	for _, tc := range testcases {
		out := IsNull(tc.in)
		if out != tc.out {
			t.Errorf("IsNull(%T): %v, want %v", tc.in, out, tc.out)
		}
	}
}

func TestIsSimpleTuple(t *testing.T) {
	testcases := []struct {
		in  ValExpr
		out bool
	}{{
		in:  ValTuple{newStrVal("")},
		out: true,
	}, {
		in: ValTuple{&ColName{}},
	}, {
		in:  ListArg(""),
		out: true,
	}, {
		in: &ColName{},
	}}
	for _, tc := range testcases {
		out := IsSimpleTuple(tc.in)
		if out != tc.out {
			t.Errorf("IsSimpleTuple(%T): %v, want %v", tc.in, out, tc.out)
		}
	}
}

func TestAsInterface(t *testing.T) {
	testcases := []struct {
		in  ValExpr
		out interface{}
	}{{
		in:  ValTuple{newStrVal("aa")},
		out: []interface{}{sqltypes.MakeString([]byte("aa"))},
	}, {
		in:  ValTuple{&ColName{}},
		out: errors.New("unexpected node ''"),
	}, {
		in:  newValArg(":aa"),
		out: ":aa",
	}, {
		in:  ListArg("::aa"),
		out: "::aa",
	}, {
		in:  newStrVal("aa"),
		out: sqltypes.MakeString([]byte("aa")),
	}, {
		in:  newHexVal("3131"),
		out: sqltypes.MakeString([]byte("11")),
	}, {
		in:  newHexVal("313"),
		out: errors.New("encoding/hex: odd length hex string"),
	}, {
		in:  newIntVal("313"),
		out: sqltypes.MakeTrusted(sqltypes.Int64, []byte("313")),
	}, {
		in:  newIntVal("18446744073709551616"),
		out: errors.New("type mismatch: strconv.ParseUint: parsing \"18446744073709551616\": value out of range"),
	}, {
		in:  newFloatVal("1.2"),
		out: errors.New("unexpected node '1.2'"),
	}, {
		in:  &NullVal{},
		out: nil,
	}, {
		in:  &ColName{},
		out: errors.New("unexpected node ''"),
	}}
	for _, tc := range testcases {
		out, err := AsInterface(tc.in)
		if err != nil {
			out = err
		}
		if !reflect.DeepEqual(out, tc.out) {
			t.Errorf("AsInterface(%#v): %#v, want %#v", tc.in, out, tc.out)
		}
	}
}

func TestStringIn(t *testing.T) {
	testcases := []struct {
		in1 string
		in2 []string
		out bool
	}{{
		in1: "v1",
		in2: []string{"v1", "v2"},
		out: true,
	}, {
		in1: "v0",
		in2: []string{"v1", "v2"},
	}}
	for _, tc := range testcases {
		out := StringIn(tc.in1, tc.in2...)
		if out != tc.out {
			t.Errorf("StringIn(%v,%v): %#v, want %#v", tc.in1, tc.in2, out, tc.out)
		}
	}
}

func newStrVal(in string) *SQLVal {
	return NewStrVal([]byte(in))
}

func newIntVal(in string) *SQLVal {
	return NewIntVal([]byte(in))
}

func newFloatVal(in string) *SQLVal {
	return NewFloatVal([]byte(in))
}

func newHexVal(in string) *SQLVal {
	return NewHexVal([]byte(in))
}

func newValArg(in string) *SQLVal {
	return NewValArg([]byte(in))
}
