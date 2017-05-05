/*
Copyright 2017 Google Inc.

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
