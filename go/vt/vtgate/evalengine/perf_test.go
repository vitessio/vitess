package evalengine_test

import (
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func BenchmarkCompilerExpressions(b *testing.B) {
	var testCases = []struct {
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

	for _, tc := range testCases {
		expr, err := sqlparser.ParseExpr(tc.expression)
		if err != nil {
			b.Fatal(err)
		}
		converted, err := evalengine.TranslateEx(expr, &evalengine.LookupIntegrationTest{collations.CollationUtf8mb4ID}, true)
		if err != nil {
			b.Fatal(err)
		}

		compiled, err := evalengine.Compile(converted, makeFields(tc.values))
		if err != nil {
			b.Fatal(err)
		}

		b.Run(tc.name+"/eval=ast", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			var env evalengine.ExpressionEnv
			env.Row = tc.values
			for n := 0; n < b.N; n++ {
				_, _ = env.Evaluate(converted)
			}
		})

		b.Run(tc.name+"/eval=vm", func(b *testing.B) {
			compiled := compiled
			values := tc.values

			b.ResetTimer()
			b.ReportAllocs()

			var vm evalengine.VirtualMachine
			for n := 0; n < b.N; n++ {
				_, _ = vm.Run(compiled, values)
			}
		})
	}
}
