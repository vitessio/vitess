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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// TestMathFunctionDoubleOverflow checks the math functions whose result can
// leave the DOUBLE range. MySQL 8.4 fails the statement with "DOUBLE value is
// out of range" when EXP, POW, COT or DEGREES produce an infinite or NaN
// result; the expected values below come from MySQL 8.4.
func TestMathFunctionDoubleOverflow(t *testing.T) {
	testCases := []struct {
		expression string
		bind       float64
		result     string
		err        string
	}{
		{expression: `EXP(:v)`, bind: 710, err: "DOUBLE value is out of range in 'exp(710)'"},
		{expression: `EXP(:v)`, bind: 709, result: `FLOAT64(8.218407461554972e307)`},
		{expression: `EXP(:v)`, bind: -1000, result: `FLOAT64(0)`},
		{expression: `POW(:v, -1)`, bind: 0, err: "DOUBLE value is out of range in 'pow(0,-1)'"},
		{expression: `POW(:v, 400)`, bind: 10, err: "DOUBLE value is out of range in 'pow(10,400)'"},
		{expression: `POW(:v, 309)`, bind: -10, err: "DOUBLE value is out of range in 'pow(-10,309)'"},
		{expression: `POW(:v, 0.5)`, bind: -1, err: "DOUBLE value is out of range in 'pow(-1,0.5)'"},
		{expression: `POWER(:v, 2)`, bind: 1e200, err: "DOUBLE value is out of range in 'pow(1e+200,2)'"},
		{expression: `POW(:v, 3)`, bind: 2, result: `FLOAT64(8)`},
		{expression: `POW(:v, 0)`, bind: 0, result: `FLOAT64(1)`},
		{expression: `COT(:v)`, bind: 0, err: "DOUBLE value is out of range in 'cot(0)'"},
		{expression: `COT(:v)`, bind: math.Copysign(0, -1), err: "DOUBLE value is out of range in 'cot(-0)'"},
		{expression: `DEGREES(:v)`, bind: 1e308, err: "DOUBLE value is out of range in 'degrees(1e+308)'"},
		{expression: `DEGREES(:v)`, bind: -1e308, err: "DOUBLE value is out of range in 'degrees(-1e+308)'"},
		{expression: `DEGREES(:v)`, bind: 1, result: `FLOAT64(57.29577951308232)`},
		{expression: `RADIANS(:v)`, bind: 1e308, result: `FLOAT64(1.7453292519943295e306)`},
	}

	check := func(t *testing.T, want, wantErr string, res evalengine.EvalResult, err error) {
		t.Helper()
		if wantErr != "" {
			require.EqualError(t, err, wantErr)
			return
		}
		require.NoError(t, err)
		require.Equal(t, want, res.String())
	}

	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s / %v", tc.expression, tc.bind), func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)

			translated, err := evalengine.Translate(expr, &evalengine.Config{
				Collation:         collations.CollationUtf8mb4ID,
				Environment:       venv,
				NoConstantFolding: true,
			})
			require.NoError(t, err)
			untyped, ok := translated.(*evalengine.UntypedExpr)
			require.True(t, ok)

			env := evalengine.NewExpressionEnv(t.Context(), map[string]*querypb.BindVariable{"v": sqltypes.Float64BindVariable(tc.bind)}, evalengine.NewEmptyVCursor(venv, nil))

			interpreted, err := env.EvaluateAST(untyped)
			check(t, tc.result, tc.err, interpreted, err)

			program, err := untyped.Compile(env)
			require.NoError(t, err)
			evaluated, err := env.EvaluateVM(program)
			check(t, tc.result, tc.err, evaluated, err)
		})
	}
}
