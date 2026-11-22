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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// TestRandomBytesLength checks the length argument of RANDOM_BYTES. MySQL 8.4
// fails the statement with "length value is out of range in 'random_bytes'"
// when the length, rounded to an integer, is outside of 1..1024; the expected
// results below come from MySQL 8.4.
func TestRandomBytesLength(t *testing.T) {
	const errLength = "length value is out of range in 'random_bytes'"
	testCases := []struct {
		bind   *querypb.BindVariable
		result string
		err    string
	}{
		{bind: sqltypes.Int64BindVariable(0), err: errLength},
		{bind: sqltypes.Int64BindVariable(-1), err: errLength},
		{bind: sqltypes.Int64BindVariable(1025), err: errLength},
		{bind: sqltypes.Uint64BindVariable(18446744073709551615), err: errLength},
		{bind: sqltypes.Float64BindVariable(0.4), err: errLength},
		{bind: sqltypes.Float64BindVariable(1024.5), err: errLength},
		{bind: sqltypes.StringBindVariable("abc"), err: errLength},
		{bind: sqltypes.Int64BindVariable(1), result: `INT64(1)`},
		{bind: sqltypes.Int64BindVariable(1024), result: `INT64(1024)`},
		{bind: sqltypes.Float64BindVariable(0.5), result: `INT64(1)`},
		{bind: sqltypes.NullBindVariable, result: `NULL`},
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
	expr, err := venv.Parser().ParseExpr(`LENGTH(RANDOM_BYTES(:v))`)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s %s", tc.bind.Type, tc.bind.Value), func(t *testing.T) {
			translated, err := evalengine.Translate(expr, &evalengine.Config{
				Collation:         collations.CollationUtf8mb4ID,
				Environment:       venv,
				NoConstantFolding: true,
			})
			require.NoError(t, err)
			untyped, ok := translated.(*evalengine.UntypedExpr)
			require.True(t, ok)

			env := evalengine.NewExpressionEnv(t.Context(), map[string]*querypb.BindVariable{"v": tc.bind}, evalengine.NewEmptyVCursor(venv, nil))

			interpreted, err := env.EvaluateAST(untyped)
			check(t, tc.result, tc.err, interpreted, err)

			program, err := untyped.Compile(env)
			require.NoError(t, err)
			evaluated, err := env.EvaluateVM(program)
			check(t, tc.result, tc.err, evaluated, err)
		})
	}
}
