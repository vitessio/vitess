/*
Copyright 2021 The Vitess Authors.

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

package evalengine

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/sqltypes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

/*
These tests should in theory live in the sqltypes package but they live here so we can
exercise both expression conversion and evaluation in the same test file
*/

type dummyCollation collations.ID

func (d dummyCollation) ColumnLookup(_ *sqlparser.ColName) (int, error) {
	panic("not supported")
}

func (d dummyCollation) CollationIDLookup(_ sqlparser.Expr) collations.ID {
	return collations.ID(d)
}

func TestConvertSimplification(t *testing.T) {
	type ast struct {
		literal, err string
	}
	ok := func(in string) ast {
		return ast{literal: in}
	}
	err := func(in string) ast {
		return ast{err: in}
	}

	var testCases = []struct {
		expression string
		converted  ast
		simplified ast
	}{
		{"42", ok("INT64(42)"), ok("INT64(42)")},
		{"1 + (1 + 1) * 8", ok("(INT64(1) + ((INT64(1) + INT64(1)) * INT64(8)))"), ok("INT64(17)")},
		{"1.0 + (1 + 1) * 8.0", ok("(FLOAT64(1) + ((INT64(1) + INT64(1)) * FLOAT64(8)))"), ok("FLOAT64(17)")},
		{"'pokemon' LIKE 'poke%'", ok("(VARBINARY(\"pokemon\") like VARBINARY(\"poke%\"))"), ok("INT32(1)")},
		{
			"'foo' COLLATE utf8mb4_general_ci IN ('bar' COLLATE latin1_swedish_ci, 'baz')",
			ok(`(VARBINARY("foo") COLLATE utf8mb4_general_ci in TUPLE(VARBINARY("bar") COLLATE latin1_swedish_ci, VARBINARY("baz")))`),
			err("COLLATION 'latin1_swedish_ci' is not valid for CHARACTER SET 'utf8mb4'"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			stmt, err := sqlparser.Parse("select " + tc.expression)
			if err != nil {
				t.Fatal(err)
			}

			astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
			converted, err := ConvertEx(astExpr, dummyCollation(45), false)
			if err != nil {
				if tc.converted.err == "" {
					t.Fatalf("failed to Convert (simplify=false): %v", err)
				}
				if !strings.Contains(err.Error(), tc.converted.err) {
					t.Fatalf("wrong Convert error (simplify=false): %q (expected %q)", err, tc.converted.err)
				}
				return
			}
			if converted.String() != tc.converted.literal {
				t.Errorf("mismatch (simplify=false): got %q, expected %q", converted.String(), tc.converted.literal)
			}

			simplified, err := ConvertEx(astExpr, dummyCollation(45), true)
			if err != nil {
				if tc.simplified.err == "" {
					t.Fatalf("failed to Convert (simplify=true): %v", err)
				}
				if !strings.Contains(err.Error(), tc.simplified.err) {
					t.Fatalf("wrong Convert error (simplify=true): %q (expected %q)", err, tc.simplified.err)
				}
				return
			}
			if simplified.String() != tc.simplified.literal {
				t.Errorf("mismatch (simplify=true): got %q, expected %q", simplified.String(), tc.simplified.literal)
			}
		})
	}
}

func TestEvaluate(t *testing.T) {
	type testCase struct {
		expression string
		expected   sqltypes.Value
	}

	tests := []testCase{{
		expression: "42",
		expected:   sqltypes.NewInt64(42),
	}, {
		expression: "42.42",
		expected:   sqltypes.NewFloat64(42.42),
	}, {
		expression: "40+2",
		expected:   sqltypes.NewInt64(42),
	}, {
		expression: "40-2",
		expected:   sqltypes.NewInt64(38),
	}, {
		expression: "40*2",
		expected:   sqltypes.NewInt64(80),
	}, {
		expression: "40/2",
		expected:   sqltypes.NewFloat64(20),
	}, {
		expression: ":exp",
		expected:   sqltypes.NewInt64(66),
	}, {
		expression: ":uint64_bind_variable",
		expected:   sqltypes.NewUint64(22),
	}, {
		expression: ":string_bind_variable",
		expected:   sqltypes.NewVarBinary("bar"),
	}, {
		expression: ":float_bind_variable",
		expected:   sqltypes.NewFloat64(2.2),
	}, {
		expression: "42 in (41, 42)",
		expected:   sqltypes.NewInt32(1),
	}, {
		expression: "42 in (41, 43)",
		expected:   sqltypes.NewInt32(0),
	}, {
		expression: "42 in (null, 41, 43)",
		expected:   NULL,
	}, {
		expression: "(1,2) in ((1,2), (2,3))",
		expected:   sqltypes.NewInt32(1),
	}, {
		expression: "(1,2) = (1,2)",
		expected:   sqltypes.NewInt32(1),
	}, {
		expression: "1 = 'sad'",
		expected:   sqltypes.NewInt32(0),
	}, {
		expression: "(1,2) = (1,3)",
		expected:   sqltypes.NewInt32(0),
	}, {
		expression: "(1,2) = (1,null)",
		expected:   NULL,
	}, {
		expression: "(1,2) in ((4,2), (2,3))",
		expected:   sqltypes.NewInt32(0),
	}, {
		expression: "(1,2) in ((1,null), (2,3))",
		expected:   NULL,
	}, {
		expression: "(1,2) in ((3,2), (2,3), null)",
		expected:   NULL,
	}, {
		expression: "(1,(1,2,3),(1,(1,2),4),2) = (1,(1,2,3),(1,(1,2),4),2)",
		expected:   sqltypes.NewInt32(1),
	}, {
		expression: "(1,(1,2,3),(1,(1,NULL),4),2) = (1,(1,2,3),(1,(1,2),4),2)",
		expected:   NULL,
	}}

	for _, test := range tests {
		t.Run(test.expression, func(t *testing.T) {
			// Given
			stmt, err := sqlparser.Parse("select " + test.expression)
			require.NoError(t, err)
			astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
			sqltypesExpr, err := Convert(astExpr, dummyCollation(45))
			require.Nil(t, err)
			require.NotNil(t, sqltypesExpr)
			env := &ExpressionEnv{
				BindVars: map[string]*querypb.BindVariable{
					"exp":                  sqltypes.Int64BindVariable(66),
					"string_bind_variable": sqltypes.StringBindVariable("bar"),
					"uint64_bind_variable": sqltypes.Uint64BindVariable(22),
					"float_bind_variable":  sqltypes.Float64BindVariable(2.2),
				},
				Row: nil,
			}

			// When
			r, err := sqltypesExpr.Evaluate(env)

			// Then
			require.NoError(t, err)
			assert.Equal(t, test.expected, r.Value(), "expected %s", test.expected.String())
		})
	}
}
