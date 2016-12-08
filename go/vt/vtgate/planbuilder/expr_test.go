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
		in1: sqlparser.ValArg(":aa"),
		in2: sqlparser.ValArg(":aa"),
		out: true,
	}, {
		in1: sqlparser.ValArg(":aa"),
		in2: sqlparser.ValArg(":bb"),
	}, {
		in1: sqlparser.StrVal("aa"),
		in2: sqlparser.StrVal("aa"),
		out: true,
	}, {
		in1: sqlparser.StrVal("11"),
		in2: sqlparser.HexVal("3131"),
		out: true,
	}, {
		in1: sqlparser.HexVal("3131"),
		in2: sqlparser.StrVal("11"),
		out: true,
	}, {
		in1: sqlparser.HexVal("3131"),
		in2: sqlparser.HexVal("3131"),
		out: true,
	}, {
		in1: sqlparser.HexVal("3131"),
		in2: sqlparser.HexVal("3132"),
	}, {
		in1: sqlparser.HexVal("313"),
		in2: sqlparser.HexVal("3132"),
	}, {
		in1: sqlparser.HexVal("3132"),
		in2: sqlparser.HexVal("313"),
	}, {
		in1: sqlparser.HexVal("3132"),
		in2: sqlparser.NumVal("313"),
	}, {
		in1: sqlparser.NumVal("313"),
		in2: sqlparser.NumVal("313"),
		out: true,
	}, {
		in1: sqlparser.NumVal("313"),
		in2: sqlparser.NumVal("314"),
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
		in  sqlparser.ValExpr
		out interface{}
	}{{
		in:  sqlparser.ValArg(":aa"),
		out: ":aa",
	}, {
		in:  sqlparser.StrVal("aa"),
		out: []byte("aa"),
	}, {
		in:  sqlparser.HexVal("3131"),
		out: []byte("11"),
	}, {
		in:  sqlparser.NumVal("3131"),
		out: int64(3131),
	}, {
		in:  sqlparser.NumVal("18446744073709551615"),
		out: uint64(18446744073709551615),
	}, {
		in:  sqlparser.NumVal("aa"),
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
			t.Errorf("valConvert(%#v): %#v, want %#v", tc.in, out, tc.out)
		}
	}
}
