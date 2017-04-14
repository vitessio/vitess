// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func TestValEqual(t *testing.T) {
	ts := &tabsym{}
	testcases := []struct {
		in1, in2 interface{}
		out      bool
	}{{
		in1: &sqlparser.ColName{Metadata: ts, Name: sqlparser.NewColIdent("c1")},
		in2: &sqlparser.ColName{Metadata: ts, Name: sqlparser.NewColIdent("c1")},
		out: true,
	}, {
		in1: &sqlparser.ColName{Metadata: ts, Name: sqlparser.NewColIdent("c1")},
		in2: &sqlparser.ColName{Metadata: ts, Name: sqlparser.NewColIdent("c2")},
	}, {
		in1: newValArg(":aa"),
		in2: newValArg(":aa"),
		out: true,
	}, {
		in1: newValArg(":aa"),
		in2: newValArg(":bb"),
	}, {
		in1: newStrVal("aa"),
		in2: newStrVal("aa"),
		out: true,
	}, {
		in1: newStrVal("11"),
		in2: newHexVal("3131"),
		out: true,
	}, {
		in1: newHexVal("3131"),
		in2: newStrVal("11"),
		out: true,
	}, {
		in1: newHexVal("3131"),
		in2: newHexVal("3131"),
		out: true,
	}, {
		in1: newHexVal("3131"),
		in2: newHexVal("3132"),
	}, {
		in1: newHexVal("313"),
		in2: newHexVal("3132"),
	}, {
		in1: newHexVal("3132"),
		in2: newHexVal("313"),
	}, {
		in1: newHexVal("3132"),
		in2: newIntVal("313"),
	}, {
		in1: newIntVal("313"),
		in2: newIntVal("313"),
		out: true,
	}, {
		in1: newIntVal("313"),
		in2: newIntVal("314"),
	}}
	for _, tc := range testcases {
		out := valEqual(tc.in1, tc.in2)
		if out != tc.out {
			t.Errorf("valEqual(%#v, %#v): %v, want %v", tc.in1, tc.in2, out, tc.out)
		}
	}
}

func TestValConvert(t *testing.T) {
	testcases := []struct {
		in  sqlparser.Expr
		out interface{}
	}{{
		in:  newValArg(":aa"),
		out: ":aa",
	}, {
		in:  newStrVal("aa"),
		out: []byte("aa"),
	}, {
		in:  newHexVal("3131"),
		out: []byte("11"),
	}, {
		in:  newIntVal("3131"),
		out: int64(3131),
	}, {
		in:  newIntVal("18446744073709551615"),
		out: uint64(18446744073709551615),
	}, {
		in:  newIntVal("aa"),
		out: "strconv.ParseUint: parsing \"aa\": invalid syntax",
	}, {
		in:  sqlparser.ListArg("::aa"),
		out: "::aa is not a value",
	}}
	for _, tc := range testcases {
		out, err := valConvert(tc.in)
		if err != nil {
			out = err.Error()
		}
		if !reflect.DeepEqual(out, tc.out) {
			t.Errorf("ValConvert(%#v): %#v, want %#v", tc.in, out, tc.out)
		}
	}
}

func newStrVal(in string) *sqlparser.SQLVal {
	return sqlparser.NewStrVal([]byte(in))
}

func newIntVal(in string) *sqlparser.SQLVal {
	return sqlparser.NewIntVal([]byte(in))
}

func newHexVal(in string) *sqlparser.SQLVal {
	return sqlparser.NewHexVal([]byte(in))
}

func newValArg(in string) *sqlparser.SQLVal {
	return sqlparser.NewValArg([]byte(in))
}
