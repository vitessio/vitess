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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// protoCase is one expression with the result measured on MySQL 8.4.13.
// alt is the result of MySQL 8.0.46 where it differs.
type protoCase struct {
	expr, want, alt string
}

var protoForms = []string{"%s + 0", "%s = 12594", "%s | 0", "%s div 5", "cast(%s as signed)", "cast(%s as decimal(10,2))", "conv(%s, 10, 10)"}

// protoMatrix lists, for each expression, MySQL's result for each of protoForms.
var protoMatrix = []string{
	`0x3132 | UINT64(12594) | INT64(1) | UINT64(12594) | UINT64(2518) | INT64(12594) | DECIMAL(12594.00) | VARCHAR("12594")`,
	`if(true, 0x3132, 0x33) | FLOAT64(12594) | INT64(1) | UINT64(12594) | INT64(2518) [8.0: UINT64(2518)] | INT64(12) | DECIMAL(12594.00) | VARCHAR("12")`,
	`if(false, 0x3132, '34') | FLOAT64(34) | INT64(0) | UINT64(34) | INT64(6) | INT64(34) | DECIMAL(34.00) | VARCHAR("34")`,
	`case when true then 0x3132 else 0x33 end | FLOAT64(12594) | INT64(1) | UINT64(12594) | INT64(2518) [8.0: UINT64(2518)] | INT64(12) | DECIMAL(12594.00) | VARCHAR("12")`,
	`nullif(0x3132, 0x33) | FLOAT64(12594) | INT64(1) | UINT64(12594) | UINT64(2518) | INT64(12) | DECIMAL(12594.00) | VARCHAR("12")`,
	`elt(1, 0x3132) | FLOAT64(12594) | INT64(1) | UINT64(12594) | INT64(2) | INT64(12) | DECIMAL(12.00) | VARCHAR("12")`,
	`case when true then elt(1, 0x3132) end | FLOAT64(12594) | INT64(1) | UINT64(12594) | INT64(2) | INT64(12) | DECIMAL(12.00) | VARCHAR("12")`,
	`elt(1, if(true, 0x3132, 0x33)) | FLOAT64(12594) | INT64(1) | UINT64(12594) | INT64(2) | INT64(12) | DECIMAL(12.00) | VARCHAR("12")`,
	`ifnull(0x3132, 0x33) | FLOAT64(12) | INT64(0) | UINT64(12) | INT64(2) [8.0: UINT64(2)] | INT64(12) | DECIMAL(12.00) | VARCHAR("12")`,
	`coalesce(if(true, 0x3132, 0x33)) | FLOAT64(12) | INT64(0) | UINT64(12) | INT64(2) [8.0: UINT64(2)] | INT64(12) | DECIMAL(12.00) | VARCHAR("12")`,
	`concat(if(true, 0x3132, 0x33)) | FLOAT64(12) | INT64(0) | UINT64(12) | INT64(2) | INT64(12) | DECIMAL(12.00) | VARCHAR("12")`,
}

var protoFurther = []protoCase{
	{`if(if(true, 0x30, 0x31), 'T', 'F')`, `VARCHAR("T")`, ""},
	{`if(case when true then 0x30 end, 'T', 'F')`, `VARCHAR("T")`, ""},
	{`if(nullif(0x30, 0x31), 'T', 'F')`, `VARCHAR("T")`, ""},
	{`if(elt(1, 0x30), 'T', 'F')`, `VARCHAR("T")`, ""},
	{`if(ifnull(0x30, 0x31), 'T', 'F')`, `VARCHAR("F")`, ""},
	{`if(coalesce(0x30), 'T', 'F')`, `VARCHAR("F")`, ""},
	{`not if(true, 0x30, 0x31)`, `INT64(0)`, ""},
	{`substring('abcdefgh', if(true, 0x32, 0x31))`, `VARCHAR("")`, ""},
	{`elt(if(true, 0x32, 0x31), 'a', 'b', 'c')`, `NULL`, ""},
	{`if(true, 0x3132, 0x33) + 1.5`, `FLOAT64(12595.5)`, ""},
	{`if(true, if(true, 0x3132, 5), 6) + 0`, `FLOAT64(12594)`, ""},
	{`if(true, 0b00110000, 0b1) + 0`, `FLOAT64(48)`, ""},
	{`-(if(true, 0x3132, 0x33))`, `FLOAT64(-12594)`, ""},
	{`abs(if(true, 0x3132, 0x33))`, `FLOAT64(12594)`, ""},
	{`concat(if(true, 0x3132, 0x33), 'x')`, `VARBINARY("12x")`, ""},
	{`if(true, 0x3132, 0x33) = '12'`, `INT64(1)`, ""},
	{`cast(if(true, 0x3132, 0x33) as double)`, `FLOAT64(12594)`, ""},
	{`bit_count(if(true, 0x3132, 0x33))`, `INT64(6)`, ""},
}

func protoCases(t *testing.T) []protoCase {
	var cases []protoCase
	for _, line := range protoMatrix {
		cols := strings.Split(line, " | ")
		require.Len(t, cols, len(protoForms)+1)
		for i, form := range protoForms {
			want, alt := cols[i+1], ""
			if idx := strings.Index(want, " [8.0: "); idx >= 0 {
				alt = strings.TrimSuffix(want[idx+len(" [8.0: "):], "]")
				want = want[:idx]
			}
			cases = append(cases, protoCase{fmt.Sprintf(form, cols[0]), want, alt})
		}
	}
	return append(cases, protoFurther...)
}

// protoEval evaluates an expression without constant folding, through the
// interpreter and through the compiled VM.
func protoEval(t *testing.T, expression string) (interp, vm string) {
	venv := vtenv.NewTestEnv()
	expr, err := venv.Parser().ParseExpr(expression)
	require.NoError(t, err)
	cfg := &evalengine.Config{
		Collation:         collations.CollationUtf8mb4ID,
		Environment:       venv,
		NoConstantFolding: true,
	}
	converted, err := evalengine.Translate(expr, cfg)
	if err != nil {
		return "ERR: " + err.Error(), "ERR: " + err.Error()
	}
	env := evalengine.NewExpressionEnv(t.Context(), nil, evalengine.NewEmptyVCursor(venv, time.UTC))
	if res, err := env.EvaluateAST(converted); err != nil {
		interp = "ERR: " + err.Error()
	} else {
		interp = res.String()
	}
	if res, err := env.Evaluate(converted); err != nil {
		vm = "ERR: " + err.Error()
	} else {
		vm = res.String()
	}
	return interp, vm
}

// TestConversionRequestPrototype compares the compiled VM with the results
// measured on MySQL for expressions that select a hex or bit literal.
func TestConversionRequestPrototype(t *testing.T) {
	cases := protoCases(t)
	var report strings.Builder
	var right, rightAlt, interpRight int
	for _, tc := range cases {
		interp, vm := protoEval(t, tc.expr)
		status := "WRONG"
		switch {
		case vm == tc.want:
			status = "ok"
			right++
		case tc.alt != "" && vm == tc.alt:
			status = "ok-8.0"
			rightAlt++
		}
		if interp == tc.want {
			interpRight++
		}
		fmt.Fprintf(&report, "%s\t%s\tmysql=%s\tvm=%s\tinterp=%s\n", status, tc.expr, tc.want, vm, interp)
		assert.Truef(t, status != "WRONG", "%s: MySQL %s, VM %s", tc.expr, tc.want, vm)
	}
	fmt.Fprintf(&report, "TOTAL %d, VM right (8.4) %d, VM right only for 8.0 %d, interpreter right (8.4) %d\n", len(cases), right, rightAlt, interpRight)
	t.Log("\n" + report.String())
	if path := os.Getenv("PROTO_REPORT"); path != "" {
		require.NoError(t, os.WriteFile(path, []byte(report.String()), 0o644))
	}
}

// BenchmarkConversionRequestCase measures a compiled CASE whose consumer
// asks for no conversion: the cost of the request check when nobody asks.
func BenchmarkConversionRequestCase(b *testing.B) {
	venv := vtenv.NewTestEnv()
	for _, expression := range []string{
		"if(column0 > 0, column0, column0) + 1",
		"case when column0 > 0 then column0 when column0 < 0 then -column0 else 0 end",
	} {
		expr, err := venv.Parser().ParseExpr(expression)
		require.NoError(b, err)
		values := []sqltypes.Value{sqltypes.NewInt64(666)}
		fields := evalengine.FieldResolver(makeFields(values))
		translated, err := evalengine.Translate(expr, &evalengine.Config{
			ResolveColumn: fields.Column,
			ResolveType:   fields.Type,
			Collation:     collations.CollationUtf8mb4ID,
			Environment:   venv,
		})
		require.NoError(b, err)
		compiled := translated.(*evalengine.CompiledExpr)
		b.Run(expression, func(b *testing.B) {
			b.ReportAllocs()
			var env evalengine.ExpressionEnv
			env.Row = values
			for b.Loop() {
				_, _ = env.EvaluateVM(compiled)
			}
		})
	}
}
