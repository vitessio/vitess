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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func BenchmarkCompilerExpressions(b *testing.B) {
	testCases := []struct {
		name       string
		expression string
		values     []sqltypes.Value
	}{
		{"complex_arith", "((23 + column0) * 4.0e0) = ((column1 / 3.33e0) * 100)", []sqltypes.Value{sqltypes.NewInt64(666), sqltypes.NewUint64(420)}},
		{"comparison_i64", "column0 = 12", []sqltypes.Value{sqltypes.NewInt64(666)}},
		{"comparison_u64", "column0 = 12", []sqltypes.Value{sqltypes.NewUint64(666)}},
		{"comparison_dec", "column0 = 12", []sqltypes.Value{sqltypes.NewDecimal("420")}},
		{"comparison_f", "column0 = 12", []sqltypes.Value{sqltypes.NewFloat64(420.0)}},
	}

	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		expr, err := venv.Parser().ParseExpr(tc.expression)
		if err != nil {
			b.Fatal(err)
		}

		fields := evalengine.FieldResolver(makeFields(tc.values))
		cfg := &evalengine.Config{
			ResolveColumn: fields.Column,
			ResolveType:   fields.Type,
			Collation:     collations.CollationUtf8mb4ID,
			Environment:   venv,
		}

		translated, err := evalengine.Translate(expr, cfg)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(tc.name+"/eval=ast", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			var env evalengine.ExpressionEnv
			env.Row = tc.values
			for n := 0; n < b.N; n++ {
				_, _ = env.EvaluateAST(translated)
			}
		})

		b.Run(tc.name+"/eval=vm", func(b *testing.B) {
			compiled := translated.(*evalengine.CompiledExpr)

			b.ResetTimer()
			b.ReportAllocs()

			var env evalengine.ExpressionEnv
			env.Row = tc.values
			for n := 0; n < b.N; n++ {
				_, _ = env.EvaluateVM(compiled)
			}
		})
	}
}
