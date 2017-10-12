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
	c1 := &column{}
	c2 := &column{}
	testcases := []struct {
		in1, in2 sqlparser.Expr
		out      bool
	}{{
		in1: &sqlparser.ColName{Metadata: c1, Name: sqlparser.NewColIdent("c1")},
		in2: &sqlparser.ColName{Metadata: c1, Name: sqlparser.NewColIdent("c1")},
		out: true,
	}, {
		// Objects that have the same name need not be the same because
		// they might have appeared in different scopes and could have
		// resolved to different columns.
		in1: &sqlparser.ColName{Metadata: c1, Name: sqlparser.NewColIdent("c1")},
		in2: &sqlparser.ColName{Metadata: c2, Name: sqlparser.NewColIdent("c1")},
		out: false,
	}, {
		in1: newValArg(":aa"),
		in2: &sqlparser.ColName{Metadata: c1, Name: sqlparser.NewColIdent("c1")},
		out: false,
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
		out: false,
	}, {
		in1: newHexVal("313"),
		in2: newHexVal("3132"),
		out: false,
	}, {
		in1: newHexVal("3132"),
		in2: newHexVal("313"),
		out: false,
	}, {
		in1: newIntVal("313"),
		in2: newHexVal("3132"),
		out: false,
	}, {
		in1: newHexVal("3132"),
		in2: newIntVal("313"),
		out: false,
	}, {
		in1: newIntVal("313"),
		in2: newIntVal("313"),
		out: true,
	}, {
		in1: newIntVal("313"),
		in2: newIntVal("314"),
		out: false,
	}}
	for _, tc := range testcases {
		out := valEqual(tc.in1, tc.in2)
		if out != tc.out {
			t.Errorf("valEqual(%#v, %#v): %v, want %v", tc.in1, tc.in2, out, tc.out)
		}
	}
}

func TestSkipParenthesis(t *testing.T) {
	baseNode := newIntVal("1")
	paren1 := &sqlparser.ParenExpr{Expr: baseNode}
	paren2 := &sqlparser.ParenExpr{Expr: paren1}
	for _, tcase := range []sqlparser.Expr{baseNode, paren1, paren2} {
		if got, want := skipParenthesis(tcase), baseNode; !reflect.DeepEqual(got, want) {
			t.Errorf("skipParenthesis(%v): %v, want %v", sqlparser.String(tcase), sqlparser.String(got), sqlparser.String(want))
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
