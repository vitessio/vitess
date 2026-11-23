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
	"fmt"
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

func TestReverseAndOrdOnMalformedInput(t *testing.T) {
	collation := collations.TypedCollation{
		Collation: collations.MySQL8().LookupByName("utf16_general_ci"),
	}
	input := newEvalText([]byte{0xD8, 0x00, 0x00, 0x31}, collation)

	require.Equal(t, []byte{0x00, 0x31, 0xD8, 0x00}, reverse(input))
	require.Equal(t, int64(0xD800), charOrd(input.bytes, collation.Collation))
}

// TestChangeCaseNumericRepertoire checks that the text LOWER and UPPER make out
// of a number or a date keeps the ASCII repertoire, as it does in MySQL, in
// both the interpreter and the compiler. A Unicode repertoire makes a merge with
// text in another character set fail where it did not before.
func TestChangeCaseNumericRepertoire(t *testing.T) {
	venv := vtenv.NewTestEnv()

	for _, expression := range []string{`LOWER(1)`, `UPPER(1e20)`, `LOWER(DATE '2020-01-02')`} {
		t.Run(expression, func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(expression)
			require.NoError(t, err)
			converted, err := Translate(expr, &Config{
				Collation:         collations.ID(8), // latin1_swedish_ci
				Environment:       venv,
				NoConstantFolding: true,
			})
			require.NoError(t, err)

			env := EmptyExpressionEnv(venv)
			want := collations.TypedCollation{
				Collation:    8,
				Coercibility: collations.CoerceCoercible,
				Repertoire:   collations.RepertoireASCII,
			}

			ast, err := env.EvaluateAST(converted)
			require.NoError(t, err)
			require.Equal(t, want, ast.v.(*evalBytes).col, "bad collation from eval engine")

			vm, err := env.EvaluateVM(converted.(*CompiledExpr))
			require.NoError(t, err)
			require.Equal(t, want, vm.v.(*evalBytes).col, "bad collation from compiler")
		})
	}
}

// TestChangeCaseEnumSetCollation checks that LOWER and UPPER change the case of
// an ENUM or SET label in the character set of its column, not in the
// connection character set, and keep the collation of the column on the result,
// in both the interpreter and the compiler, for a column the planner has a type
// for and for one it has none for. A collation of utf8mb4 or utf8mb3 without a
// case mapping of its own changes the case as Go does, instead of failing.
func TestChangeCaseEnumSetCollation(t *testing.T) {
	venv := vtenv.NewTestEnv()

	for _, tc := range []struct {
		name       string
		typ        sqltypes.Type
		collation  collations.ID
		expression string
		in, out    string
	}{
		// 0xC9 is E acute and 0xE9 is e acute in latin1, and neither is valid utf8mb4.
		{"enum lower", sqltypes.Enum, 8, `LOWER(column0)`, "\xC9A", "\xE9a"},
		{"enum upper", sqltypes.Enum, 8, `UPPER(column0)`, "\xE9a", "\xC9A"},
		{"set lower", sqltypes.Set, 8, `LOWER(column0)`, "\xC9A,B", "\xE9a,b"},
		{"set upper", sqltypes.Set, 8, `UPPER(column0)`, "\xE9a,b", "\xC9A,B"},
		// utf8mb4_general_ci and utf8mb3_general_ci have no case mapping of their own
		{"set upper utf8mb4_general_ci", sqltypes.Set, 45, `UPPER(column0)`, "ab,c", "AB,C"},
		{"enum lower utf8mb3_general_ci", sqltypes.Enum, 33, `LOWER(column0)`, "AB", "ab"},
	} {
		for _, typed := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/typed=%v", tc.name, typed), func(t *testing.T) {
				fields := FieldResolver{{Name: "column0", Type: tc.typ, Charset: uint32(tc.collation)}}
				expr, err := venv.Parser().ParseExpr(tc.expression)
				require.NoError(t, err)
				cfg := &Config{
					ResolveColumn:     fields.Column,
					Collation:         collations.CollationUtf8mb4ID,
					Environment:       venv,
					NoConstantFolding: true,
				}
				if typed {
					cfg.ResolveType = fields.Type
				}
				converted, err := Translate(expr, cfg)
				require.NoError(t, err)

				env := EmptyExpressionEnv(venv)
				env.Fields = fields
				env.Row = []sqltypes.Value{sqltypes.MakeTrusted(tc.typ, []byte(tc.in))}
				want := collations.TypedCollation{
					Collation:    tc.collation,
					Coercibility: collations.CoerceCoercible,
					Repertoire:   collations.RepertoireUnicode,
				}

				ast, err := env.EvaluateAST(converted)
				require.NoError(t, err)
				require.Equal(t, tc.out, string(ast.v.(*evalBytes).bytes), "bad value from eval engine")
				require.Equal(t, want, ast.v.(*evalBytes).col, "bad collation from eval engine")

				vm, err := env.Evaluate(converted)
				require.NoError(t, err)
				require.Equal(t, tc.out, string(vm.v.(*evalBytes).bytes), "bad value from compiler")
				require.Equal(t, want, vm.v.(*evalBytes).col, "bad collation from compiler")
			})
		}
	}
}
