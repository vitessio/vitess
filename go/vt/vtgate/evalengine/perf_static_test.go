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

var staticBenchCases = []struct {
	name       string
	expression string
	values     []sqltypes.Value
}{
	// result is an integer whose declared type equals its representation
	{"int64_add", "column0 + 1", []sqltypes.Value{sqltypes.NewInt64(666)}},
	// result is a pass-through of an INT column: declared INT32, representation Int64
	{"int32_case", "case when column0 > 0 then column0 else column1 end", []sqltypes.Value{sqltypes.NewInt32(666), sqltypes.NewInt32(1)}},
	// bare INT column
	{"int32_col", "column0", []sqltypes.Value{sqltypes.NewInt32(666)}},
	// string result
	{"varchar_concat", "concat(column0, 'x')", []sqltypes.Value{sqltypes.NewVarChar("hello world")}},
}

// BenchmarkStaticValue measures Evaluate(...).Value(...) per row for a typed expression
// (the shape of engine.Projection).
func BenchmarkStaticValue(b *testing.B) {
	venv := vtenv.NewTestEnv()
	for _, tc := range staticBenchCases {
		expr, err := venv.Parser().ParseExpr(tc.expression)
		if err != nil {
			b.Fatal(err)
		}
		fields := evalengine.FieldResolver(makeFields(tc.values))
		translated, err := evalengine.Translate(expr, &evalengine.Config{
			ResolveColumn: fields.Column,
			ResolveType:   fields.Type,
			Collation:     collations.CollationUtf8mb4ID,
			Environment:   venv,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.Run(tc.name, func(b *testing.B) {
			env := evalengine.EmptyExpressionEnv(venv)
			env.Row = tc.values
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				r, err := env.Evaluate(translated)
				if err != nil {
					b.Fatal(err)
				}
				_ = r.Value(collations.CollationUtf8mb4ID)
			}
		})
	}
}

// BenchmarkUntyped measures an expression whose columns have no static type
// (Translate returns an UntypedExpr), evaluated with Evaluate(...).Value(...).
// On the base commit Evaluate runs the interpreter; in the prototype it compiles
// for the row's types (cached) and runs the VM.
func BenchmarkUntyped(b *testing.B) {
	venv := vtenv.NewTestEnv()
	for _, tc := range staticBenchCases {
		expr, err := venv.Parser().ParseExpr(tc.expression)
		if err != nil {
			b.Fatal(err)
		}
		fields := makeFields(tc.values)
		cfg := &evalengine.Config{
			ResolveColumn: evalengine.FieldResolver(fields).Column,
			Collation:     collations.CollationUtf8mb4ID,
			Environment:   venv,
		}

		// steady state: same translated expression for every row
		b.Run(tc.name+"/steady", func(b *testing.B) {
			translated, err := evalengine.Translate(expr, cfg)
			if err != nil {
				b.Fatal(err)
			}
			env := evalengine.EmptyExpressionEnv(venv)
			env.Row = tc.values
			env.Fields = fields
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				r, err := env.Evaluate(translated)
				if err != nil {
					b.Fatal(err)
				}
				_ = r.Value(collations.CollationUtf8mb4ID)
			}
		})

		// first row: translate + first evaluation (includes the run-time compile in the prototype)
		b.Run(tc.name+"/first_row", func(b *testing.B) {
			env := evalengine.EmptyExpressionEnv(venv)
			env.Row = tc.values
			env.Fields = fields
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				translated, err := evalengine.Translate(expr, cfg)
				if err != nil {
					b.Fatal(err)
				}
				r, err := env.Evaluate(translated)
				if err != nil {
					b.Fatal(err)
				}
				_ = r.Value(collations.CollationUtf8mb4ID)
			}
		})

		// translate alone, to subtract from first_row
		b.Run(tc.name+"/translate_only", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err := evalengine.Translate(expr, cfg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
