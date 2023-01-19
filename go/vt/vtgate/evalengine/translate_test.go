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

func TestTranslateSimplification(t *testing.T) {
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
		{"1 + (1 + 1) * 8", ok("INT64(1) + ((INT64(1) + INT64(1)) * INT64(8))"), ok("INT64(17)")},
		{"1.0e0 + (1 + 1) * 8.0e0", ok("FLOAT64(1) + ((INT64(1) + INT64(1)) * FLOAT64(8))"), ok("FLOAT64(17)")},
		{"'pokemon' LIKE 'poke%'", ok("VARCHAR(\"pokemon\") LIKE VARCHAR(\"poke%\")"), ok("INT64(1)")},
		{
			"'foo' COLLATE utf8mb4_general_ci IN ('bar' COLLATE latin1_swedish_ci, 'baz')",
			ok(`VARCHAR("foo") COLLATE utf8mb4_general_ci IN (VARCHAR("bar") COLLATE latin1_swedish_ci, VARCHAR("baz"))`),
			err("COLLATION 'latin1_swedish_ci' is not valid for CHARACTER SET 'utf8mb4'"),
		},
		{`"pokemon" in ("bulbasaur", "venusaur", "charizard")`,
			ok(`VARCHAR("pokemon") IN (VARCHAR("bulbasaur"), VARCHAR("venusaur"), VARCHAR("charizard"))`),
			ok("INT64(0)"),
		},
		{`"pokemon" in ("bulbasaur", "venusaur", "pokemon")`,
			ok(`VARCHAR("pokemon") IN (VARCHAR("bulbasaur"), VARCHAR("venusaur"), VARCHAR("pokemon"))`),
			ok("INT64(1)"),
		},
		{`"pokemon" in ("bulbasaur", "venusaur", "pokemon", NULL)`,
			ok(`VARCHAR("pokemon") IN (VARCHAR("bulbasaur"), VARCHAR("venusaur"), VARCHAR("pokemon"), NULL)`),
			ok(`INT64(1)`),
		},
		{`"pokemon" in ("bulbasaur", "venusaur", NULL)`,
			ok(`VARCHAR("pokemon") IN (VARCHAR("bulbasaur"), VARCHAR("venusaur"), NULL)`),
			ok(`NULL`),
		},
		{"0 + NULL", ok("INT64(0) + NULL"), ok("NULL")},
		{"1.00000 + 2.000", ok("DECIMAL(1.00000) + DECIMAL(2.000)"), ok("DECIMAL(3.00000)")},
		{"1 + 0.05", ok("INT64(1) + DECIMAL(0.05)"), ok("DECIMAL(1.05)")},
		{"1 + 0.05e0", ok("INT64(1) + FLOAT64(0.05)"), ok("FLOAT64(1.05)")},
		{"1 / 1", ok("INT64(1) / INT64(1)"), ok("DECIMAL(1.0000)")},
		{"(14620 / 9432456) / (24250 / 9432456)", ok("(INT64(14620) / INT64(9432456)) / (INT64(24250) / INT64(9432456))"), ok("DECIMAL(0.60288653)")},
		{"COALESCE(NULL, 2, NULL, 4)", ok("COALESCE(NULL, INT64(2), NULL, INT64(4))"), ok("INT64(2)")},
		{"coalesce(NULL, 2, NULL, 4)", ok("COALESCE(NULL, INT64(2), NULL, INT64(4))"), ok("INT64(2)")},
		{"coalesce(NULL, NULL)", ok("COALESCE(NULL, NULL)"), ok("NULL")},
		{"coalesce(NULL)", ok("COALESCE(NULL)"), ok("NULL")},
		{"weight_string('foobar')", ok(`WEIGHT_STRING(VARCHAR("foobar"))`), ok(`VARBINARY("\x00F\x00O\x00O\x00B\x00A\x00R")`)},
		{"weight_string('foobar' as char(12))", ok(`WEIGHT_STRING(VARCHAR("foobar") AS CHAR(12))`), ok(`VARBINARY("\x00F\x00O\x00O\x00B\x00A\x00R\x00 \x00 \x00 \x00 \x00 \x00 ")`)},
		{"case when 1 = 1 then 2 else 3 end", ok("CASE WHEN INT64(1) = INT64(1) THEN INT64(2) ELSE INT64(3)"), ok("INT64(2)")},
		{"case when null then 2 when 12 = 4 then 'ohnoes' else 42 end", ok(`CASE WHEN NULL THEN INT64(2) WHEN INT64(12) = INT64(4) THEN VARCHAR("ohnoes") ELSE INT64(42)`), ok(`VARCHAR("42")`)},
		{"convert('a', char(2) character set utf8mb4)", ok(`CONVERT(VARCHAR("a"), CHAR(2) CHARACTER SET utf8mb4_0900_ai_ci)`), ok(`VARCHAR("a")`)},
		{"convert('a', char(2) character set latin1 binary)", ok(`CONVERT(VARCHAR("a"), CHAR(2) CHARACTER SET latin1_bin)`), ok(`VARCHAR("a")`)},
		{"cast('a' as char(2) character set utf8mb4)", ok(`CONVERT(VARCHAR("a"), CHAR(2) CHARACTER SET utf8mb4_0900_ai_ci)`), ok(`VARCHAR("a")`)},
		{"cast('a' as char(2) character set latin1 binary)", ok(`CONVERT(VARCHAR("a"), CHAR(2) CHARACTER SET latin1_bin)`), ok(`VARCHAR("a")`)},
	}

	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			stmt, err := sqlparser.Parse("select " + tc.expression)
			if err != nil {
				t.Fatal(err)
			}

			astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
			converted, err := TranslateEx(astExpr, LookupDefaultCollation(45), false)
			if err != nil {
				if tc.converted.err == "" {
					t.Fatalf("failed to Convert (simplify=false): %v", err)
				}
				if !strings.Contains(err.Error(), tc.converted.err) {
					t.Fatalf("wrong Convert error (simplify=false): %q (expected %q)", err, tc.converted.err)
				}
				return
			}
			assert.Equal(t, tc.converted.literal, FormatExpr(converted))

			simplified, err := TranslateEx(astExpr, LookupDefaultCollation(45), true)
			if err != nil {
				if tc.simplified.err == "" {
					t.Fatalf("failed to Convert (simplify=true): %v", err)
				}
				if !strings.Contains(err.Error(), tc.simplified.err) {
					t.Fatalf("wrong Convert error (simplify=true): %q (expected %q)", err, tc.simplified.err)
				}
				return
			}
			assert.Equal(t, tc.simplified.literal, FormatExpr(simplified))
		})
	}
}

func TestEvaluate(t *testing.T) {
	type testCase struct {
		expression string
		expected   sqltypes.Value
	}

	True := sqltypes.NewInt64(1)
	False := sqltypes.NewInt64(0)
	tests := []testCase{{
		expression: "42",
		expected:   sqltypes.NewInt64(42),
	}, {
		expression: "42.42e0",
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
		expected:   sqltypes.NewDecimal("20.0000"),
	}, {
		expression: ":exp",
		expected:   sqltypes.NewInt64(66),
	}, {
		expression: ":int32_bind_variable",
		expected:   sqltypes.NewInt64(20),
	}, {
		expression: ":uint32_bind_variable",
		expected:   sqltypes.NewUint64(21),
	}, {
		expression: ":uint64_bind_variable",
		expected:   sqltypes.NewUint64(22),
	}, {
		expression: ":string_bind_variable",
		expected:   sqltypes.NewVarChar("bar"),
	}, {
		expression: ":float_bind_variable",
		expected:   sqltypes.NewFloat64(2.2),
	}, {
		expression: "42 in (41, 42)",
		expected:   True,
	}, {
		expression: "42 in (41, 43)",
		expected:   False,
	}, {
		expression: "42 in (null, 41, 43)",
		expected:   NULL,
	}, {
		expression: "(1,2) in ((1,2), (2,3))",
		expected:   True,
	}, {
		expression: "(1,2) = (1,2)",
		expected:   True,
	}, {
		expression: "1 = 'sad'",
		expected:   False,
	}, {
		expression: "(1,2) = (1,3)",
		expected:   False,
	}, {
		expression: "(1,2) = (1,null)",
		expected:   NULL,
	}, {
		expression: "(1,2) in ((4,2), (2,3))",
		expected:   False,
	}, {
		expression: "(1,2) in ((1,null), (2,3))",
		expected:   NULL,
	}, {
		expression: "(1,(1,2,3),(1,(1,2),4),2) = (1,(1,2,3),(1,(1,2),4),2)",
		expected:   True,
	}, {
		expression: "(1,(1,2,3),(1,(1,NULL),4),2) = (1,(1,2,3),(1,(1,2),4),2)",
		expected:   NULL,
	}, {
		expression: "null is null",
		expected:   True,
	}, {
		expression: "true is null",
		expected:   False,
	}, {
		expression: "42 is null",
		expected:   False,
	}, {
		expression: "null is not null",
		expected:   False,
	}, {
		expression: "42 is not null",
		expected:   True,
	}, {
		expression: "true is not null",
		expected:   True,
	}, {
		expression: "null is true",
		expected:   False,
	}, {
		expression: "42 is true",
		expected:   True,
	}, {
		expression: "true is true",
		expected:   True,
	}, {
		expression: "null is false",
		expected:   False,
	}, {
		expression: "42 is false",
		expected:   False,
	}, {
		expression: "false is false",
		expected:   True,
	}, {
		expression: "null is not true",
		expected:   True,
	}, {
		expression: "42 is not true",
		expected:   False,
	}, {
		expression: "true is not true",
		expected:   False,
	}, {
		expression: "null is not false",
		expected:   True,
	}, {
		expression: "42 is not false",
		expected:   True,
	}, {
		expression: "false is not false",
		expected:   False,
	}}

	for _, test := range tests {
		t.Run(test.expression, func(t *testing.T) {
			// Given
			stmt, err := sqlparser.Parse("select " + test.expression)
			require.NoError(t, err)
			astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
			sqltypesExpr, err := Translate(astExpr, LookupDefaultCollation(45))
			require.Nil(t, err)
			require.NotNil(t, sqltypesExpr)
			env := EnvWithBindVars(
				map[string]*querypb.BindVariable{
					"exp":                  sqltypes.Int64BindVariable(66),
					"string_bind_variable": sqltypes.StringBindVariable("bar"),
					"int32_bind_variable":  sqltypes.Int32BindVariable(20),
					"uint32_bind_variable": sqltypes.Uint32BindVariable(21),
					"uint64_bind_variable": sqltypes.Uint64BindVariable(22),
					"float_bind_variable":  sqltypes.Float64BindVariable(2.2),
				}, 0)

			// When
			r, err := env.Evaluate(sqltypesExpr)

			// Then
			require.NoError(t, err)
			assert.Equal(t, test.expected, r.Value(), "expected %s", test.expected.String())
		})
	}
}

func TestEvaluateTuple(t *testing.T) {
	type testCase struct {
		expression string
		expected   []sqltypes.Value
	}

	tests := []testCase{{
		expression: "(1,2,4)",
		expected:   []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2), sqltypes.NewInt64(4)},
	}, {
		expression: "(1,'2',4)",
		expected:   []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewVarChar("2"), sqltypes.NewInt64(4)},
	}, {
		expression: "(1,'2',4.0)",
		expected:   []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewVarChar("2"), sqltypes.NewDecimal("4.0")},
	}}

	for _, test := range tests {
		t.Run(test.expression, func(t *testing.T) {
			// Given
			stmt, err := sqlparser.Parse("select " + test.expression)
			require.NoError(t, err)
			astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
			sqltypesExpr, err := Translate(astExpr, LookupDefaultCollation(45))
			require.Nil(t, err)
			require.NotNil(t, sqltypesExpr)

			// When
			r, err := EmptyExpressionEnv().Evaluate(sqltypesExpr)

			// Then
			require.NoError(t, err)
			require.NotNil(t, r)
			gotValues := r.TupleValues()
			assert.Equal(t, test.expected, gotValues, "expected: %s, got: %s", test.expected, gotValues)
		})
	}
}

// TestTranslationFailures tests that translation fails for functions that we don't support evaluation for.
func TestTranslationFailures(t *testing.T) {
	testcases := []struct {
		expression  string
		expectedErr string
	}{
		{
			expression:  "cast('2023-01-07 12:34:56' as date)",
			expectedErr: "Unsupported type conversion: DATE",
		}, {
			expression:  "cast('2023-01-07 12:34:56' as datetime(5))",
			expectedErr: "Unsupported type conversion: DATETIME(5)",
		}, {
			expression:  "cast('3.4' as FLOAT)",
			expectedErr: "Unsupported type conversion: FLOAT",
		}, {
			expression:  "cast('3.4' as FLOAT(3))",
			expectedErr: "Unsupported type conversion: FLOAT(3)",
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.expression, func(t *testing.T) {
			// Given
			stmt, err := sqlparser.Parse("select " + testcase.expression)
			require.NoError(t, err)
			astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
			_, err = Translate(astExpr, LookupDefaultCollation(45))
			require.EqualError(t, err, testcase.expectedErr)
		})
	}

}
