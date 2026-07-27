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

package evalengine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtenv"
)

// TestPadPreservesRow tests that LPAD and RPAD leave the input row untouched.
// The values of a row share one buffer, so every value but the last is followed
// in memory by the next one: padding that grew its operand in place would write
// over the column that comes after it.
func TestPadPreservesRow(t *testing.T) {
	venv := vtenv.NewTestEnv()
	coll := collations.MySQL8().DefaultConnectionCharset()
	fields := []*querypb.Field{
		{Name: "l", Type: sqltypes.VarChar, Charset: collations.CollationUtf8mb4ID},
		{Name: "r", Type: sqltypes.VarChar, Charset: collations.CollationUtf8mb4ID},
	}
	resolver := FieldResolver(fields)

	testCases := []struct {
		expression string
		result     string
	}{
		{expression: `rpad(l, 4, 'x')`, result: "ABxx"},
		{expression: `rpad(l, 5, 'xy')`, result: "ABxyx"},
		{expression: `lpad(l, 4, 'x')`, result: "xxAB"},
		{expression: `lpad(l, 5, 'xy')`, result: "xyxAB"},
	}

	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			astExpr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)
			translated, err := Translate(astExpr, &Config{
				ResolveColumn: resolver.Column,
				ResolveType:   resolver.Type,
				Collation:     coll,
				Environment:   venv,
			})
			require.NoError(t, err)

			// A row as it arrives from a shard: one buffer holding every value,
			// so "AB" is backed by spare capacity that runs into "CD".
			newEnv := func() *ExpressionEnv {
				env := NewExpressionEnv(t.Context(), nil, NewEmptyVCursor(venv, time.Local))
				env.Row = sqltypes.MakeRowTrusted(fields, &querypb.Row{
					Lengths: []int64{2, 2},
					Values:  []byte("ABCD"),
				})
				return env
			}

			t.Run("compiled", func(t *testing.T) {
				env := newEnv()
				require.IsType(t, &CompiledExpr{}, translated)

				res, err := env.Evaluate(translated)
				require.NoError(t, err)

				require.Equal(t, tc.result, res.Value(coll).ToString())
				require.Equal(t, "AB", env.Row[0].ToString())
				require.Equal(t, "CD", env.Row[1].ToString())
			})

			t.Run("interpreted", func(t *testing.T) {
				env := newEnv()

				res, err := env.EvaluateAST(translated)
				require.NoError(t, err)

				require.Equal(t, tc.result, res.Value(coll).ToString())
				require.Equal(t, "AB", env.Row[0].ToString())
				require.Equal(t, "CD", env.Row[1].ToString())
			})
		})
	}
}
