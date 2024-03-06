/*
Copyright 2023 The Vitess Authors.

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
	"context"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"

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
		{"42", ok("42"), ok("42")},
		{"1 + (1 + 1) * 8", ok("1 + (1 + 1) * 8"), ok("17")},
		{"1.0e0 + (1 + 1) * 8.0e0", ok("1 + (1 + 1) * 8"), ok("17")},
		{"'pokemon' LIKE 'poke%'", ok("'pokemon' like 'poke%'"), ok("1")},
		{
			"'foo' COLLATE utf8mb4_general_ci IN ('bar' COLLATE latin1_swedish_ci, 'baz')",
			ok(`'foo' COLLATE utf8mb4_general_ci in ('bar' COLLATE latin1_swedish_ci, 'baz')`),
			err("COLLATION 'latin1_swedish_ci' is not valid for CHARACTER SET 'utf8mb4'"),
		},
		{`"pokemon" in ("bulbasaur", "venusaur", "charizard")`,
			ok(`'pokemon' in ('bulbasaur', 'venusaur', 'charizard')`),
			ok("0"),
		},
		{`"pokemon" in ("bulbasaur", "venusaur", "pokemon")`,
			ok(`'pokemon' in ('bulbasaur', 'venusaur', 'pokemon')`),
			ok("1"),
		},
		{`"pokemon" in ("bulbasaur", "venusaur", "pokemon", NULL)`,
			ok(`'pokemon' in ('bulbasaur', 'venusaur', 'pokemon', null)`),
			ok(`1`),
		},
		{`"pokemon" in ("bulbasaur", "venusaur", NULL)`,
			ok(`'pokemon' in ('bulbasaur', 'venusaur', null)`),
			ok(`null`),
		},
		{"0 + NULL", ok("0 + null"), ok("null")},
		{"1.00000 + 2.000", ok("1.00000 + 2.000"), ok("3.00000")},
		{"1 + 0.05", ok("1 + 0.05"), ok("1.05")},
		{"1 + 0.05e0", ok("1 + 0.05"), ok("1.05")},
		{"1 / 1", ok("1 / 1"), ok("1.0000")},
		{"(14620 / 9432456) / (24250 / 9432456)", ok("14620 / 9432456 / (24250 / 9432456)"), ok("0.60288653")},
		{"COALESCE(NULL, 2, NULL, 4)", ok("coalesce(null, 2, null, 4)"), ok("2")},
		{"coalesce(NULL, 2, NULL, 4)", ok("coalesce(null, 2, null, 4)"), ok("2")},
		{"coalesce(NULL, NULL)", ok("coalesce(null, null)"), ok("null")},
		{"coalesce(NULL)", ok("coalesce(null)"), ok("null")},
		{"weight_string('foobar')", ok(`weight_string('foobar')`), ok("'\x1c\xe5\x1d\xdd\x1d\xdd\x1c`\x1cG\x1e3'")},
		{"weight_string('foobar' as char(12))", ok(`weight_string('foobar' as char(12))`), ok("'\x1c\xe5\x1d\xdd\x1d\xdd\x1c`\x1cG\x1e3'")},
		{"case when 1 = 1 then 2 else 3 end", ok("case when 1 = 1 then 2 else 3"), ok("2")},
		{"case when null then 2 when 12 = 4 then 'ohnoes' else 42 end", ok(`case when null then 2 when 12 = 4 then 'ohnoes' else 42`), ok(`'42'`)},
		{"convert('a', char(2) character set utf8mb4)", ok(`convert('a', CHAR(2) character set utf8mb4_0900_ai_ci)`), ok(`'a'`)},
		{"convert('a', char(2) character set latin1 binary)", ok(`convert('a', CHAR(2) character set latin1_bin)`), ok(`'a'`)},
		{"cast('a' as char(2) character set utf8mb4)", ok(`convert('a', CHAR(2) character set utf8mb4_0900_ai_ci)`), ok(`'a'`)},
		{"cast('a' as char(2) character set latin1 binary)", ok(`convert('a', CHAR(2) character set latin1_bin)`), ok(`'a'`)},
		{"date'2022-10-03'", ok(`'2022-10-03'`), ok(`'2022-10-03'`)},
		{"time'12:34:45'", ok(`'12:34:45'`), ok(`'12:34:45'`)},
		{"timestamp'2022-10-03 12:34:45'", ok(`'2022-10-03 12:34:45'`), ok(`'2022-10-03 12:34:45'`)},
		{"date'2022'", err(`Incorrect DATE value: '2022'`), err(`Incorrect DATE value: '2022'`)},
		{"time'2022-10-03'", err(`Incorrect TIME value: '2022-10-03'`), err(`Incorrect TIME value: '2022-10-03'`)},
		{"timestamp'2022-10-03'", err(`Incorrect DATETIME value: '2022-10-03'`), err(`Incorrect DATETIME value: '2022-10-03'`)},
		{"ifnull(12, 23)", ok(`case when 12 is null then 23 else 12`), ok(`12`)},
		{"ifnull(null, 23)", ok(`case when null is null then 23 else null`), ok(`23`)},
		{"nullif(1, 1)", ok(`case when 1 = 1 then null else 1`), ok(`null`)},
		{"nullif(1, 2)", ok(`case when 1 = 2 then null else 1`), ok(`1`)},
		{"12 between 5 and 20", ok("12 >= 5 and 12 <= 20"), ok(`1`)},
		{"12 not between 5 and 20", ok("12 < 5 or 12 > 20"), ok(`0`)},
		{"2 not between 5 and 20", ok("2 < 5 or 2 > 20"), ok(`1`)},
		{"json->\"$.c\"", ok("JSON_EXTRACT(`json`, '$.c')"), ok("JSON_EXTRACT(`json`, '$.c')")},
		{"json->>\"$.c\"", ok("JSON_UNQUOTE(JSON_EXTRACT(`json`, '$.c'))"), ok("JSON_UNQUOTE(JSON_EXTRACT(`json`, '$.c'))")},
	}

	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			stmt, err := venv.Parser().Parse("select " + tc.expression)
			if err != nil {
				t.Fatal(err)
			}

			fields := FieldResolver([]*querypb.Field{
				{Name: "json", Type: sqltypes.TypeJSON, Charset: collations.CollationUtf8mb4ID},
			})

			cfg := &Config{
				ResolveColumn:     fields.Column,
				Collation:         venv.CollationEnv().DefaultConnectionCharset(),
				Environment:       venv,
				NoConstantFolding: true,
				NoCompilation:     true,
			}

			astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
			converted, err := Translate(astExpr, cfg)
			if err != nil {
				if tc.converted.err == "" {
					t.Fatalf("failed to Convert (simplify=false): %v", err)
				}
				if !strings.Contains(err.Error(), tc.converted.err) {
					t.Fatalf("wrong Convert error (simplify=false): %q (expected %q)", err, tc.converted.err)
				}
				return
			}
			assert.Equal(t, tc.converted.literal, sqlparser.String(converted))

			cfg.NoConstantFolding = false
			simplified, err := Translate(astExpr, cfg)
			if err != nil {
				if tc.simplified.err == "" {
					t.Fatalf("failed to Convert (simplify=true): %v", err)
				}
				if !strings.Contains(err.Error(), tc.simplified.err) {
					t.Fatalf("wrong Convert error (simplify=true): %q (expected %q)", err, tc.simplified.err)
				}
				return
			}
			assert.Equal(t, tc.simplified.literal, sqlparser.String(simplified))
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

	venv := vtenv.NewTestEnv()
	for _, test := range tests {
		t.Run(test.expression, func(t *testing.T) {
			// Given
			stmt, err := sqlparser.NewTestParser().Parse("select " + test.expression)
			require.NoError(t, err)
			astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
			sqltypesExpr, err := Translate(astExpr, &Config{
				Collation:   venv.CollationEnv().DefaultConnectionCharset(),
				Environment: venv,
			})
			require.Nil(t, err)
			require.NotNil(t, sqltypesExpr)
			env := NewExpressionEnv(context.Background(), map[string]*querypb.BindVariable{
				"exp":                  sqltypes.Int64BindVariable(66),
				"string_bind_variable": sqltypes.StringBindVariable("bar"),
				"int32_bind_variable":  sqltypes.Int32BindVariable(20),
				"uint32_bind_variable": sqltypes.Uint32BindVariable(21),
				"uint64_bind_variable": sqltypes.Uint64BindVariable(22),
				"float_bind_variable":  sqltypes.Float64BindVariable(2.2),
			}, NewEmptyVCursor(venv, time.Local))

			// When
			r, err := env.Evaluate(sqltypesExpr)

			// Then
			require.NoError(t, err)
			assert.Equal(t, test.expected, r.Value(collations.MySQL8().DefaultConnectionCharset()), "expected %s", test.expected.String())
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

	venv := vtenv.NewTestEnv()
	for _, test := range tests {
		t.Run(test.expression, func(t *testing.T) {
			// Given
			stmt, err := sqlparser.NewTestParser().Parse("select " + test.expression)
			require.NoError(t, err)
			astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
			sqltypesExpr, err := Translate(astExpr, &Config{
				Collation:   venv.CollationEnv().DefaultConnectionCharset(),
				Environment: venv,
			})
			require.Nil(t, err)
			require.NotNil(t, sqltypesExpr)

			// When
			r, err := EmptyExpressionEnv(venv).Evaluate(sqltypesExpr)

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
			expression:  "cast('3.4' as FLOAT)",
			expectedErr: "Unsupported type conversion: FLOAT",
		}, {
			expression:  "cast('3.4' as FLOAT(3))",
			expectedErr: "Unsupported type conversion: FLOAT(3)",
		},
	}

	venv := vtenv.NewTestEnv()
	for _, testcase := range testcases {
		t.Run(testcase.expression, func(t *testing.T) {
			// Given
			stmt, err := sqlparser.NewTestParser().Parse("select " + testcase.expression)
			require.NoError(t, err)
			astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
			_, err = Translate(astExpr, &Config{
				Collation:   venv.CollationEnv().DefaultConnectionCharset(),
				Environment: venv,
			})
			require.EqualError(t, err, testcase.expectedErr)
		})
	}

}

func TestCardinalityWithBindVariables(t *testing.T) {
	testcases := []struct {
		expr string
		err  string
	}{
		{expr: `:foo + 1`},
		{expr: `1 IN ::bar`},
		{expr: `:foo IN ::bar`},
		{expr: `:foo IN _binary ::bar`, err: "syntax error"},
		{expr: `::foo + 1`, err: "syntax error"},
		{expr: `::foo = 1`, err: "syntax error"},
		{expr: `::foo = ::bar`, err: "syntax error"},
		{expr: `::foo = :bar`, err: "syntax error"},
		{expr: `:foo = :bar`},
		{expr: `:foo IN :bar`, err: "syntax error"},
		{expr: `:foo IN ::bar`},
		{expr: `(1, 2) IN ::bar`, err: "Operand should contain 1 column"},
		{expr: `1 IN ::bar`},
	}

	venv := vtenv.NewTestEnv()
	for _, testcase := range testcases {
		t.Run(testcase.expr, func(t *testing.T) {
			err := func() error {
				stmt, err := sqlparser.NewTestParser().Parse("select " + testcase.expr)
				if err != nil {
					return err
				}

				astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
				_, err = Translate(astExpr, &Config{
					Collation:     venv.CollationEnv().DefaultConnectionCharset(),
					Environment:   venv,
					NoCompilation: true,
				})
				return err
			}()

			if testcase.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, testcase.err)
			}
		})
	}
}
