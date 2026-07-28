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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
)

// TestJSONDoublesByOrigin pins which double a JSON number is worth, by where
// the JSON came from. A bind variable holds character data: wherever it
// travels it reaches mysqld as text for its document parser to read, so its
// numbers convert the way MySQL converts them — an ULP or two from the
// nearest double for long significands. A column's text spells the double the
// tablet's mysqld already holds, so it converts back to exactly that double.
// The two expected doubles below differ by one ULP; the MySQL one is what
// 8.0.45, 8.4.11 and 9.4.0 store for this document.
func TestJSONDoublesByOrigin(t *testing.T) {
	venv := vtenv.NewTestEnv()
	coll := collations.MySQL8().DefaultConnectionCharset()

	const doc = `{"x": 99999999999999999999999999999999999999999}`
	mysqlDouble := math.Float64frombits(0x48725dfa371a19e6)   // 9.999999999999998e40
	nearestDouble := math.Float64frombits(0x48725dfa371a19e7) // 1.0000000000000002e41

	evalBits := func(t *testing.T, env *ExpressionEnv, expr Expr, compiled bool) uint64 {
		t.Helper()
		var res EvalResult
		if compiled {
			res = evaluateCompiled(t, env, expr)
		} else {
			var err error
			res, err = env.EvaluateAST(expr)
			require.NoError(t, err)
		}
		f, err := res.Value(coll).ToFloat64()
		require.NoError(t, err)
		return math.Float64bits(f)
	}

	t.Run("bind variable", func(t *testing.T) {
		astExpr, err := venv.Parser().ParseExpr(`cast(json_extract(:j, '$.x') as double)`)
		require.NoError(t, err)
		astExpr = sqlparser.Rewrite(astExpr, nil, func(c *sqlparser.Cursor) bool {
			if arg, ok := c.Node().(*sqlparser.Argument); ok {
				arg.Type = sqltypes.TypeJSON
			}
			return true
		}).(sqlparser.Expr)

		translated, err := Translate(astExpr, &Config{
			Collation:   coll,
			Environment: venv,
		})
		require.NoError(t, err)

		newEnv := func() *ExpressionEnv {
			return NewExpressionEnv(t.Context(), map[string]*querypb.BindVariable{
				"j": {Type: sqltypes.TypeJSON, Value: []byte(doc)},
			}, NewEmptyVCursor(venv, time.UTC))
		}

		t.Run("compiled", func(t *testing.T) {
			require.Equal(t, math.Float64bits(mysqlDouble), evalBits(t, newEnv(), translated, true))
		})
		t.Run("interpreted", func(t *testing.T) {
			require.Equal(t, math.Float64bits(mysqlDouble), evalBits(t, newEnv(), translated, false))
		})
	})

	t.Run("column", func(t *testing.T) {
		fields := FieldResolver([]*querypb.Field{
			{Name: "j", Type: sqltypes.TypeJSON, Charset: collations.CollationBinaryID},
		})
		astExpr, err := venv.Parser().ParseExpr(`cast(json_extract(j, '$.x') as double)`)
		require.NoError(t, err)
		translated, err := Translate(astExpr, &Config{
			ResolveColumn: fields.Column,
			ResolveType:   fields.Type,
			Collation:     coll,
			Environment:   venv,
		})
		require.NoError(t, err)

		newEnv := func() *ExpressionEnv {
			env := NewExpressionEnv(t.Context(), nil, NewEmptyVCursor(venv, time.UTC))
			env.Row = []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(doc))}
			return env
		}

		t.Run("compiled", func(t *testing.T) {
			require.Equal(t, math.Float64bits(nearestDouble), evalBits(t, newEnv(), translated, true))
		})
		t.Run("interpreted", func(t *testing.T) {
			require.Equal(t, math.Float64bits(nearestDouble), evalBits(t, newEnv(), translated, false))
		})
	})
}
