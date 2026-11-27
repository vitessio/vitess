/*
Copyright 2026 The Vitess Authors.

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

package evalengine_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// TestFloatArithmeticOverflow checks that +, - and * fail with MySQL's
// "DOUBLE value is out of range" error when the result of a DOUBLE operation
// is not finite, instead of returning +Inf or -Inf, and that results that
// still fit in a DOUBLE are not affected. It runs each expression with the
// AST interpreter and with the compiled program.
func TestFloatArithmeticOverflow(t *testing.T) {
	const overflow = "DOUBLE value is out of range"

	maxFloat := sqltypes.NewFloat64(math.MaxFloat64)
	minFloat := sqltypes.NewFloat64(-math.MaxFloat64)

	testCases := []struct {
		expression string
		values     []sqltypes.Value
		result     string
		err        string
	}{
		// Overflowing sums, differences and products.
		{expression: "column0 + column1", values: []sqltypes.Value{maxFloat, maxFloat}, err: overflow},
		{expression: "column0 + column1", values: []sqltypes.Value{minFloat, minFloat}, err: overflow},
		{expression: "column0 - column1", values: []sqltypes.Value{maxFloat, minFloat}, err: overflow},
		{expression: "column0 - column1", values: []sqltypes.Value{minFloat, maxFloat}, err: overflow},
		{expression: "column0 * column1", values: []sqltypes.Value{maxFloat, sqltypes.NewFloat64(10)}, err: overflow},
		{expression: "column0 * column1", values: []sqltypes.Value{minFloat, sqltypes.NewFloat64(10)}, err: overflow},
		{expression: "column0 * column1", values: []sqltypes.Value{maxFloat, maxFloat}, err: overflow},
		// An integer operand is converted to a float. Adding it to a huge float or
		// subtracting it still fits, but multiplying it can overflow.
		{expression: "column0 + column1", values: []sqltypes.Value{maxFloat, sqltypes.NewInt64(math.MaxInt64)}, result: "FLOAT64(1.7976931348623157e308)"},
		{expression: "column1 + column0", values: []sqltypes.Value{maxFloat, sqltypes.NewInt64(math.MaxInt64)}, result: "FLOAT64(1.7976931348623157e308)"},
		{expression: "column1 - column0", values: []sqltypes.Value{maxFloat, sqltypes.NewInt64(math.MaxInt64)}, result: "FLOAT64(-1.7976931348623157e308)"},
		{expression: "column0 - column1", values: []sqltypes.Value{minFloat, sqltypes.NewInt64(math.MaxInt64)}, result: "FLOAT64(-1.7976931348623157e308)"},
		{expression: "column0 * column1", values: []sqltypes.Value{maxFloat, sqltypes.NewInt64(2)}, err: overflow},
		{expression: "column1 * column0", values: []sqltypes.Value{maxFloat, sqltypes.NewInt64(2)}, err: overflow},
		// The overflow is reported inside a larger expression too.
		{expression: "(column0 * column1) - 1", values: []sqltypes.Value{maxFloat, sqltypes.NewFloat64(10)}, err: overflow},
		// Results that fit in a DOUBLE are returned as before.
		{expression: "column0 + column1", values: []sqltypes.Value{maxFloat, minFloat}, result: "FLOAT64(0)"},
		{expression: "column0 - column1", values: []sqltypes.Value{maxFloat, maxFloat}, result: "FLOAT64(0)"},
		{expression: "column0 * column1", values: []sqltypes.Value{maxFloat, sqltypes.NewFloat64(0.5)}, result: "FLOAT64(8.988465674311579e307)"},
		{expression: "column0 * column1", values: []sqltypes.Value{maxFloat, sqltypes.NewFloat64(0)}, result: "FLOAT64(0)"},
		{expression: "column0 + column1", values: []sqltypes.Value{sqltypes.NewFloat64(1e308), sqltypes.NewFloat64(7e307)}, result: "FLOAT64(1.7e308)"},
	}

	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s/%s/%s", tc.expression, tc.values[0], tc.values[1]), func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)

			fields := evalengine.FieldResolver(makeFields(tc.values))
			converted, err := evalengine.Translate(expr, &evalengine.Config{
				ResolveColumn:     fields.Column,
				ResolveType:       fields.Type,
				Collation:         collations.CollationUtf8mb4ID,
				Environment:       venv,
				NoConstantFolding: true,
			})
			require.NoError(t, err)

			env := evalengine.NewExpressionEnv(t.Context(), nil, evalengine.NewEmptyVCursor(venv, time.UTC))
			env.Row = tc.values

			check := func(t *testing.T, name string, res evalengine.EvalResult, err error) {
				t.Helper()
				switch {
				case tc.err == "" && err != nil:
					assert.Failf(t, "unexpected error", "%s: %v", name, err)
				case tc.err == "" && err == nil:
					assert.Equal(t, tc.result, res.String(), name)
				case err == nil:
					assert.Failf(t, "missing error", "%s returned %s, want an error containing %q", name, res.String(), tc.err)
				default:
					assert.ErrorContains(t, err, tc.err, name)
				}
			}

			interpreted, err := env.EvaluateAST(converted)
			check(t, "interpreter", interpreted, err)

			compiled, err := env.Evaluate(converted)
			check(t, "compiler", compiled, err)
		})
	}
}

// TestFloatArithmeticOverflowConstants checks that the overflow of a constant
// DOUBLE expression is reported when the expression is translated, like the
// out-of-range error of a constant division.
func TestFloatArithmeticOverflowConstants(t *testing.T) {
	venv := vtenv.NewTestEnv()
	for _, query := range []string{
		"1e308 + 1e308",
		"-1e308 - 1e308",
		"1e308 * 10",
		"1e308 / 0.1",
	} {
		t.Run(query, func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(query)
			require.NoError(t, err)

			_, err = evalengine.Translate(expr, &evalengine.Config{
				Collation:   collations.CollationUtf8mb4ID,
				Environment: venv,
			})
			require.ErrorContains(t, err, "DOUBLE value is out of range")
		})
	}
}
