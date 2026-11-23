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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// TestRegexpNullArgumentWithBadPattern checks that MySQL compiles the pattern
// of a regexp function before it looks at the subject and the other arguments:
// a pattern that does not compile is an error even when one of them is NULL.
// A NULL pattern or match type is still NULL. The expected results come from
// MySQL 8.4.
func TestRegexpNullArgumentWithBadPattern(t *testing.T) {
	// MySQL 8.4 fails with ER_REGEXP_MISSING_CLOSE_BRACKET (3696) and
	// ER_REGEXP_ILLEGAL_ARGUMENT (3685).
	const (
		errBracket = vterrors.RegexpMissingCloseBracket
		errIllegal = vterrors.RegexpIllegalArgument
	)
	null := sqltypes.NullBindVariable
	testCases := []struct {
		expr   string
		s, p   *querypb.BindVariable
		r      *querypb.BindVariable
		result string
		err    vterrors.State
	}{
		{expr: `:s REGEXP :p`, s: null, p: sqltypes.StringBindVariable("["), err: errBracket},
		{expr: `:s NOT REGEXP :p`, s: null, p: sqltypes.StringBindVariable("["), err: errBracket},
		{expr: `:s REGEXP :p`, s: null, p: sqltypes.StringBindVariable(""), err: errIllegal},
		{expr: `:s REGEXP '['`, s: null, err: errBracket},
		{expr: `REGEXP_LIKE(:s, :p, 'i')`, s: null, p: sqltypes.StringBindVariable("["), err: errBracket},
		{expr: `REGEXP_INSTR(:s, :p)`, s: null, p: sqltypes.StringBindVariable("["), err: errBracket},
		{expr: `REGEXP_INSTR('dog', '[', :s)`, s: null, err: errBracket},
		{expr: `REGEXP_INSTR('dog', '[', 1, :s)`, s: null, err: errBracket},
		{expr: `REGEXP_INSTR('dog', '[', 1, 1, :s)`, s: null, err: errBracket},
		{expr: `REGEXP_SUBSTR(:s, '[', 1, 1, 'i')`, s: null, err: errBracket},
		{expr: `REGEXP_SUBSTR('dog cat dog', '[', :s, 1, 'i')`, s: null, err: errBracket},
		{expr: `REGEXP_SUBSTR('dog cat dog', '[', 1, :s, 'i')`, s: null, err: errBracket},
		{expr: `REGEXP_REPLACE(:s, '[', 'bar', 1, 1, 'i')`, s: null, err: errBracket},
		{expr: `REGEXP_REPLACE('dog cat dog', '[', :s, 1, 1, 'i')`, s: null, err: errBracket},
		{expr: `REGEXP_REPLACE('dog cat dog', '[', 'bar', :s, 1, 'i')`, s: null, err: errBracket},
		{expr: `REGEXP_REPLACE('dog cat dog', '[', 'bar', 1, :s, 'i')`, s: null, err: errBracket},
		{expr: `REGEXP_REPLACE(:s, :p, 'bar')`, s: null, p: sqltypes.StringBindVariable("["), err: errBracket},

		// A pattern that compiles, or a NULL pattern or match type, gives NULL.
		{expr: `:s REGEXP :p`, s: null, p: sqltypes.StringBindVariable("a"), result: `NULL`},
		{expr: `:s REGEXP 'a'`, s: null, result: `NULL`},
		{expr: `:s REGEXP :p`, s: sqltypes.StringBindVariable("a"), p: null, result: `NULL`},
		{expr: `:s REGEXP :p`, s: null, p: null, result: `NULL`},
		{expr: `:s NOT REGEXP :p`, s: null, p: null, result: `NULL`},
		{expr: `REGEXP_LIKE(:s, :p)`, s: null, p: null, result: `NULL`},
		{expr: `REGEXP_LIKE(:s, :p, 'i')`, s: null, p: null, result: `NULL`},
		{expr: `REGEXP_LIKE(:s, :p, :r)`, s: null, p: null, r: sqltypes.StringBindVariable("i"), result: `NULL`},
		{expr: `REGEXP_INSTR(:s, :p, NULL)`, s: null, p: null, result: `NULL`},
		{expr: `REGEXP_INSTR(:s, :p, :r)`, s: null, p: null, r: null, result: `NULL`},
		{expr: `REGEXP_SUBSTR(:s, :p, 1, NULL, 'i')`, s: null, p: null, result: `NULL`},
		{expr: `REGEXP_REPLACE(:s, :p, 'x')`, s: null, p: null, result: `NULL`},
		{expr: `REGEXP_REPLACE(:s, :p, :r)`, s: null, p: null, r: null, result: `NULL`},
		{expr: `REGEXP_REPLACE(:s, :p, :r, 1, 1, 'i')`, s: null, p: null, r: null, result: `NULL`},
		{expr: `REGEXP_LIKE(:s, '[', NULL)`, s: null, result: `NULL`},
		{expr: `REGEXP_INSTR('dog', 'o', :s)`, s: null, result: `NULL`},
		{expr: `REGEXP_SUBSTR('dog cat dog', 'dog', 1, :s)`, s: null, result: `NULL`},
		{expr: `REGEXP_REPLACE(:s, 'DOG', 'bar', 1, 1, 'i')`, s: null, result: `NULL`},
		{expr: `REGEXP_REPLACE('dog cat dog', :p, 'bar', 1, :s)`, s: null, p: sqltypes.StringBindVariable("dog"), result: `NULL`},

		// Without a NULL argument the results do not change.
		{expr: `:s REGEXP :p`, s: sqltypes.StringBindVariable("abc"), p: sqltypes.StringBindVariable("b"), result: `INT64(1)`},
		{expr: `REGEXP_INSTR('dog', 'o', :s)`, s: sqltypes.Int64BindVariable(1), result: `INT64(2)`},
		{expr: `REGEXP_SUBSTR('dog cat dog', 'dog', 1, :s)`, s: sqltypes.Int64BindVariable(2), result: `VARCHAR("dog")`},
		{expr: `REGEXP_REPLACE(:s, 'DOG', 'bar', 1, 1, 'i')`, s: sqltypes.StringBindVariable("dog cat dog"), result: `TEXT("bar cat dog")`},
	}

	check := func(t *testing.T, want string, wantErr vterrors.State, res evalengine.EvalResult, err error) {
		t.Helper()
		if wantErr != vterrors.Undefined {
			require.Error(t, err)
			require.Equal(t, wantErr, vterrors.ErrState(err), err.Error())
			return
		}
		require.NoError(t, err)
		require.Equal(t, want, res.String())
	}

	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		t.Run(tc.expr, func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(tc.expr)
			require.NoError(t, err)

			translated, err := evalengine.Translate(expr, &evalengine.Config{
				Collation:         collations.CollationUtf8mb4ID,
				Environment:       venv,
				NoConstantFolding: true,
			})
			require.NoError(t, err)
			untyped, ok := translated.(*evalengine.UntypedExpr)
			require.True(t, ok)

			bv := map[string]*querypb.BindVariable{"s": tc.s}
			if tc.p != nil {
				bv["p"] = tc.p
			}
			if tc.r != nil {
				bv["r"] = tc.r
			}
			env := evalengine.NewExpressionEnv(t.Context(), bv, evalengine.NewEmptyVCursor(venv, nil))

			interpreted, err := env.EvaluateAST(untyped)
			check(t, tc.result, tc.err, interpreted, err)

			program, err := untyped.Compile(env)
			require.NoError(t, err)
			evaluated, err := env.EvaluateVM(program)
			check(t, tc.result, tc.err, evaluated, err)
		})
	}
}
