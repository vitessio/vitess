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

// bitwiseOperandOwnership covers the binary-string forms of the bitwise
// operators, which produce a binary string of the same length as their
// operands. The operand bytes are borrowed: they can belong to a literal in a
// translated expression, to a bind variable, or to the input row, so an
// operator that wrote its result over them would corrupt a value it does not
// own. Every case uses the operands "AB" and "!!".
var bitwiseOperandOwnership = []struct {
	name       string
	expression string
	result     string
}{
	{name: "not", expression: `~:l`, result: "\xbe\xbd"},
	{name: "and", expression: `:l & :r`, result: "\x01\x00"},
	{name: "or", expression: `:l | :r`, result: "ac"},
	{name: "xor", expression: `:l ^ :r`, result: "`c"},
	{name: "literal lhs", expression: `_binary'AB' ^ :r`, result: "`c"},
	{name: "literal rhs", expression: `:l ^ _binary'!!'`, result: "`c"},
}

func bitwiseBindVars() map[string]*querypb.BindVariable {
	return map[string]*querypb.BindVariable{
		"l": sqltypes.BytesBindVariable([]byte("AB")),
		"r": sqltypes.BytesBindVariable([]byte("!!")),
	}
}

// evaluateCompiled evaluates expr through the virtual machine. Translate only
// returns a *CompiledExpr when it knows the types of the arguments, so an
// untyped expression is compiled against env first.
func evaluateCompiled(t *testing.T, env *ExpressionEnv, expr Expr) EvalResult {
	t.Helper()

	if untyped, ok := expr.(*UntypedExpr); ok {
		compiled, err := untyped.Compile(env)
		require.NoError(t, err)
		expr = compiled
	}
	require.IsType(t, &CompiledExpr{}, expr)

	res, err := env.Evaluate(expr)
	require.NoError(t, err)
	return res
}

// TestBitwiseBinaryRepeatedEvaluation evaluates one translated expression
// several times against one bind variable map, the way a query evaluates an
// expression once per row. Every evaluation must return the same result: the
// operands do not change between them, and a translated expression is also
// shared by concurrent queries through the plan cache.
func TestBitwiseBinaryRepeatedEvaluation(t *testing.T) {
	venv := vtenv.NewTestEnv()
	coll := collations.MySQL8().DefaultConnectionCharset()

	for _, tc := range bitwiseOperandOwnership {
		t.Run(tc.name, func(t *testing.T) {
			astExpr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)
			translated, err := Translate(astExpr, &Config{Collation: coll, Environment: venv})
			require.NoError(t, err)

			bindVars := bitwiseBindVars()
			for i := range 4 {
				env := EmptyExpressionEnv(venv)
				env.BindVars = bindVars

				res := evaluateCompiled(t, env, translated)
				require.Equalf(t, tc.result, res.Value(coll).ToString(), "evaluation %d", i)
			}
		})
	}
}

// TestBitwiseBinaryResultIsPlainBinary tests that the binary-string form of the
// bitwise operators returns a plain binary string even when an operand is a hex
// or bit literal. MySQL 8.0+ reads the result as a binary string, which is only true
// when its bytes parse as a non-zero number:
//
//	SELECT IF(X'FF' & _binary'A', 'true', 'false')  =>  false
//	SELECT IF(X'41', 'true', 'false')               =>  true
//
// A result that carried the operand's hex or bit literal marker would instead be
// read as the number 65 and come out true.
func TestBitwiseBinaryResultIsPlainBinary(t *testing.T) {
	venv := vtenv.NewTestEnv()
	coll := collations.MySQL8().DefaultConnectionCharset()

	for _, tc := range []struct {
		expression string
		result     string
	}{
		{expression: `x'ff' & :r`, result: "A"},
		{expression: `b'11111111' & :r`, result: "A"},
		{expression: `x'ff' | :r`, result: "\xff"},
		{expression: `x'ff' ^ :r`, result: "\xbe"},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			astExpr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)
			translated, err := Translate(astExpr, &Config{Collation: coll, Environment: venv})
			require.NoError(t, err)

			newEnv := func() *ExpressionEnv {
				env := EmptyExpressionEnv(venv)
				env.BindVars = map[string]*querypb.BindVariable{
					"r": sqltypes.BytesBindVariable([]byte("A")),
				}
				return env
			}

			interpreted, err := newEnv().EvaluateAST(translated)
			require.NoError(t, err)
			compiled := evaluateCompiled(t, newEnv(), translated)

			require.Equal(t, tc.result, interpreted.Value(coll).ToString())
			require.Equal(t, tc.result, compiled.Value(coll).ToString())

			require.False(t, interpreted.ToBoolean())
			require.False(t, compiled.ToBoolean())
		})
	}
}

// TestBitwiseBinaryPreservesBindVars tests that evaluating a bitwise operator
// leaves the caller's bind variables untouched. The same bind variable map
// builds the queries sent to the shards, so writing through it would change the
// query that is executed.
func TestBitwiseBinaryPreservesBindVars(t *testing.T) {
	venv := vtenv.NewTestEnv()
	coll := collations.MySQL8().DefaultConnectionCharset()

	for _, tc := range bitwiseOperandOwnership {
		t.Run(tc.name, func(t *testing.T) {
			astExpr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)
			translated, err := Translate(astExpr, &Config{Collation: coll, Environment: venv})
			require.NoError(t, err)

			t.Run("compiled", func(t *testing.T) {
				env := EmptyExpressionEnv(venv)
				env.BindVars = bitwiseBindVars()

				evaluateCompiled(t, env, translated)

				require.Equal(t, []byte("AB"), env.BindVars["l"].Value)
				require.Equal(t, []byte("!!"), env.BindVars["r"].Value)
			})

			t.Run("interpreted", func(t *testing.T) {
				env := EmptyExpressionEnv(venv)
				env.BindVars = bitwiseBindVars()

				_, err := env.EvaluateAST(translated)
				require.NoError(t, err)

				require.Equal(t, []byte("AB"), env.BindVars["l"].Value)
				require.Equal(t, []byte("!!"), env.BindVars["r"].Value)
			})
		})
	}
}

// TestBitwiseBinaryPreservesRow tests that evaluating a bitwise operator leaves
// the input row untouched. A row is read once per output column and is owned by
// the result the input primitive returned, so writing through it would change
// the values that other columns and later consumers observe.
func TestBitwiseBinaryPreservesRow(t *testing.T) {
	venv := vtenv.NewTestEnv()
	coll := collations.MySQL8().DefaultConnectionCharset()
	fields := FieldResolver([]*querypb.Field{
		{Name: "l", Type: sqltypes.VarBinary, Charset: collations.CollationBinaryID},
		{Name: "r", Type: sqltypes.VarBinary, Charset: collations.CollationBinaryID},
	})

	for _, expression := range []string{`~l`, `l & r`, `l | r`, `l ^ r`} {
		t.Run(expression, func(t *testing.T) {
			astExpr, err := venv.Parser().ParseExpr(expression)
			require.NoError(t, err)
			translated, err := Translate(astExpr, &Config{
				ResolveColumn: fields.Column,
				ResolveType:   fields.Type,
				Collation:     coll,
				Environment:   venv,
			})
			require.NoError(t, err)

			newEnv := func() *ExpressionEnv {
				env := NewExpressionEnv(t.Context(), nil, NewEmptyVCursor(venv, time.Local))
				env.Row = []sqltypes.Value{
					sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("AB")),
					sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("!!")),
				}
				return env
			}

			t.Run("compiled", func(t *testing.T) {
				env := newEnv()
				evaluateCompiled(t, env, translated)

				require.Equal(t, "AB", env.Row[0].ToString())
				require.Equal(t, "!!", env.Row[1].ToString())
			})

			t.Run("interpreted", func(t *testing.T) {
				env := newEnv()
				_, err := env.EvaluateAST(translated)
				require.NoError(t, err)

				require.Equal(t, "AB", env.Row[0].ToString())
				require.Equal(t, "!!", env.Row[1].ToString())
			})
		})
	}
}
