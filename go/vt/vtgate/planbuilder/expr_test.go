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

package planbuilder

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
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
		in1: sqlparser.NewArgument(":aa"),
		in2: &sqlparser.ColName{Metadata: c1, Name: sqlparser.NewColIdent("c1")},
		out: false,
	}, {
		in1: sqlparser.NewArgument(":aa"),
		in2: sqlparser.NewArgument(":aa"),
		out: true,
	}, {
		in1: sqlparser.NewArgument(":aa"),
		in2: sqlparser.NewArgument(":bb"),
	}, {
		in1: sqlparser.NewStrLiteral("aa"),
		in2: sqlparser.NewStrLiteral("aa"),
		out: true,
	}, {
		in1: sqlparser.NewStrLiteral("11"),
		in2: sqlparser.NewHexLiteral("3131"),
		out: true,
	}, {
		in1: sqlparser.NewHexLiteral("3131"),
		in2: sqlparser.NewStrLiteral("11"),
		out: true,
	}, {
		in1: sqlparser.NewHexLiteral("3131"),
		in2: sqlparser.NewHexLiteral("3131"),
		out: true,
	}, {
		in1: sqlparser.NewHexLiteral("3131"),
		in2: sqlparser.NewHexLiteral("3132"),
		out: false,
	}, {
		in1: sqlparser.NewHexLiteral("313"),
		in2: sqlparser.NewHexLiteral("3132"),
		out: false,
	}, {
		in1: sqlparser.NewHexLiteral("3132"),
		in2: sqlparser.NewHexLiteral("313"),
		out: false,
	}, {
		in1: sqlparser.NewIntLiteral("313"),
		in2: sqlparser.NewHexLiteral("3132"),
		out: false,
	}, {
		in1: sqlparser.NewHexLiteral("3132"),
		in2: sqlparser.NewIntLiteral("313"),
		out: false,
	}, {
		in1: sqlparser.NewIntLiteral("313"),
		in2: sqlparser.NewIntLiteral("313"),
		out: true,
	}, {
		in1: sqlparser.NewIntLiteral("313"),
		in2: sqlparser.NewIntLiteral("314"),
		out: false,
	}}
	for _, tc := range testcases {
		out := valEqual(tc.in1, tc.in2)
		if out != tc.out {
			t.Errorf("valEqual(%#v, %#v): %v, want %v", tc.in1, tc.in2, out, tc.out)
		}
	}
}
