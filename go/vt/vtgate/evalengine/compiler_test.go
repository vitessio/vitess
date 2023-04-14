/*
Copyright 2023 The Vitess Authors.

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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/olekukonko/tablewriter"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/evalengine/testcases"
)

func makeFields(values []sqltypes.Value) (fields []*querypb.Field) {
	for i, v := range values {
		field := &querypb.Field{
			Name: fmt.Sprintf("column%d", i),
			Type: v.Type(),
		}
		if sqltypes.IsText(field.Type) {
			field.Charset = uint32(collations.CollationUtf8mb4ID)
		} else {
			field.Charset = uint32(collations.CollationBinaryID)
		}
		fields = append(fields, field)
	}
	return
}

type Tracker struct {
	buf              strings.Builder
	tbl              *tablewriter.Table
	supported, total int
}

func NewTracker() *Tracker {
	track := &Tracker{}
	track.tbl = tablewriter.NewWriter(&track.buf)
	return track
}

func (s *Tracker) Add(name string, supported, total int) {
	s.tbl.Append([]string{
		name,
		strconv.Itoa(supported),
		strconv.Itoa(total),
		fmt.Sprintf("%.02f%%", 100*float64(supported)/float64(total)),
	})
	s.supported += supported
	s.total += total
}

func (s *Tracker) String() string {
	s.tbl.SetBorder(false)
	s.tbl.SetColumnAlignment([]int{
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_RIGHT,
		tablewriter.ALIGN_RIGHT,
		tablewriter.ALIGN_RIGHT,
	})
	s.tbl.SetFooterAlignment(tablewriter.ALIGN_RIGHT)
	s.tbl.SetFooter([]string{
		"",
		strconv.Itoa(s.supported),
		strconv.Itoa(s.total),
		fmt.Sprintf("%.02f%%", 100*float64(s.supported)/float64(s.total)),
	})
	s.tbl.Render()
	return s.buf.String()
}

func TestCompilerReference(t *testing.T) {
	now := time.Now()
	evalengine.SystemTime = func() time.Time { return now }
	defer func() { evalengine.SystemTime = time.Now }()

	track := NewTracker()

	for _, tc := range testcases.Cases {
		t.Run(tc.Name(), func(t *testing.T) {
			var supported, total int
			env := evalengine.EmptyExpressionEnv()

			tc.Run(func(query string, row []sqltypes.Value) {
				env.Row = row

				stmt, err := sqlparser.ParseExpr(query)
				if err != nil {
					// no need to test un-parseable queries
					return
				}

				fields := evalengine.FieldResolver(tc.Schema)
				cfg := &evalengine.Config{
					ResolveColumn: fields.Column,
					ResolveType:   fields.Type,
					Collation:     collations.CollationUtf8mb4ID,
					Optimization:  evalengine.OptimizationLevelCompilerDebug,
				}

				converted, err := evalengine.Translate(stmt, cfg)
				if err != nil {
					return
				}

				expected, evalErr := env.Evaluate(evalengine.Deoptimize(converted))
				total++

				if cfg.CompilerErr != nil {
					switch {
					case vterrors.Code(cfg.CompilerErr) == vtrpcpb.Code_UNIMPLEMENTED:
						t.Logf("unsupported: %s", query)
					case evalErr == nil:
						t.Errorf("failed compilation:\nSQL:  %s\nError: %s", query, cfg.CompilerErr)
					case evalErr.Error() != cfg.CompilerErr.Error():
						t.Errorf("error mismatch:\nSQL:  %s\nError eval: %s\nError comp: %s", query, evalErr, cfg.CompilerErr)
					default:
						supported++
					}
					return
				}

				res, vmErr := func() (res evalengine.EvalResult, err error) {
					res, err = env.EvaluateVM(converted.(*evalengine.CompiledExpr))
					return
				}()

				if vmErr != nil {
					switch {
					case evalErr == nil:
						t.Errorf("failed evaluation from compiler:\nSQL:  %s\nError: %s", query, vmErr)
					case evalErr.Error() != vmErr.Error():
						t.Errorf("error mismatch:\nSQL:  %s\nError eval: %s\nError comp: %s", query, evalErr, vmErr)
					default:
						supported++
					}
					return
				}

				eval := expected.String()
				comp := res.String()

				if eval != comp {
					t.Errorf("bad evaluation from compiler:\nSQL:  %s\nEval: %s\nComp: %s", query, eval, comp)
					return
				}

				supported++
			})

			track.Add(tc.Name(), supported, total)
		})
	}

	t.Logf("\n%s", track.String())
}

type debugCompiler struct {
	t testing.TB
}

func (d *debugCompiler) Instruction(ins string, args ...any) {
	ins = fmt.Sprintf(ins, args...)
	d.t.Logf("> %s", ins)
}

func (d *debugCompiler) Stack(old, new int) {
	d.t.Logf("\tsp = %d -> %d", old, new)
}

func TestCompilerSingle(t *testing.T) {
	var testCases = []struct {
		expression string
		values     []sqltypes.Value
		result     string
	}{
		{
			expression: "1 + column0",
			values:     []sqltypes.Value{sqltypes.NewInt64(1)},
			result:     "INT64(2)",
		},
		{
			expression: "1 + column0",
			values:     []sqltypes.Value{sqltypes.NewFloat64(1)},
			result:     "FLOAT64(2)",
		},
		{
			expression: "1.0e0 - column0",
			values:     []sqltypes.Value{sqltypes.NewFloat64(1)},
			result:     "FLOAT64(0)",
		},
		{
			expression: "128 - column0",
			values:     []sqltypes.Value{sqltypes.NewFloat64(1)},
			result:     "FLOAT64(127)",
		},
		{
			expression: "(128 - column0) * 3",
			values:     []sqltypes.Value{sqltypes.NewFloat64(1)},
			result:     "FLOAT64(381)",
		},
		{
			expression: "1.0e0 < column0",
			values:     []sqltypes.Value{sqltypes.NewFloat64(2)},
			result:     "INT64(1)",
		},
		{
			expression: "1.0e0 < column0",
			values:     []sqltypes.Value{sqltypes.NewFloat64(-1)},
			result:     "INT64(0)",
		},
		{
			expression: `'foo' = 'FOO' collate utf8mb4_0900_as_cs`,
			result:     "INT64(0)",
		},
		{
			expression: `'foo' < 'bar'`,
			result:     "INT64(0)",
		},
		{
			expression: `case when false then 0 else 18446744073709551615 end`,
			result:     `DECIMAL(18446744073709551615)`,
		},
		{
			expression: `case when true then _binary "foobar" else 'foo' collate utf8mb4_0900_as_cs end`,
			result:     `VARCHAR("foobar")`,
		},
		{
			expression: `- 18446744073709551615`,
			result:     `DECIMAL(-18446744073709551615)`,
		},
		{
			expression: `CAST(CAST(true AS JSON) AS BINARY)`,
			result:     `BLOB("true")`,
		},
		{
			expression: `JSON_ARRAY(true, 1.0)`,
			result:     `JSON("[true, 1.0]")`,
		},
		{
			expression: `cast(true as json) + 0`,
			result:     `FLOAT64(1)`,
		},
		{
			expression: `CAST(CAST(0 AS JSON) AS CHAR(16))`,
			result:     `VARCHAR("0")`,
		},
		{
			expression: `1 OR cast('invalid' as json)`,
			result:     `INT64(1)`,
		},
		{
			expression: `NULL AND 1`,
			result:     `NULL`,
		},
		{
			expression: `CONV(-1.5e0, 1.5e0, 1.5e0)`,
			result:     `VARCHAR("1111111111111111111111111111111111111111111111111111111111111111")`,
		},
		{
			expression: `CONV(9223372036854775810.4, 13, 7)`,
			result:     `VARCHAR("45012021522523134134601")`,
		},
		{
			expression: `CONV(-9223372036854775809, 13e0, 13e0)`,
			result:     `VARCHAR("0")`,
		},
		{
			expression: `0 + time '10:04:58'`,
			result:     `INT64(100458)`,
		},
		{
			expression: `0 + time '101:34:58'`,
			result:     `INT64(1013458)`,
		},
		{
			expression: `time '10:04:58' < '101:34:58'`,
			result:     `INT64(1)`,
		},
		{
			expression: `1.7 / 173458`,
			result:     `DECIMAL(0.00001)`,
		},
		{
			expression: `cast(time '5 12:34:58' as json)`,
			result:     `JSON("\"04:34:58.000000\"")`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			expr, err := sqlparser.ParseExpr(tc.expression)
			if err != nil {
				t.Fatal(err)
			}

			fields := evalengine.FieldResolver(makeFields(tc.values))
			cfg := &evalengine.Config{
				ResolveColumn: fields.Column,
				ResolveType:   fields.Type,
				Collation:     collations.CollationUtf8mb4ID,
				Optimization:  evalengine.OptimizationLevelCompilerDebug,
			}

			converted, err := evalengine.Translate(expr, cfg)
			if err != nil {
				t.Fatal(err)
			}

			if cfg.CompilerErr != nil {
				t.Fatalf("bad compilation: %v", cfg.CompilerErr)
			}

			env := evalengine.EmptyExpressionEnv()
			env.Row = tc.values

			expected, err := env.Evaluate(evalengine.Deoptimize(converted))
			if err != nil {
				t.Fatal(err)
			}
			if expected.String() != tc.result {
				t.Fatalf("bad evaluation from eval engine: got %s, want %s", expected.String(), tc.result)
			}

			// re-run the same evaluation multiple times to ensure results are always consistent
			for i := 0; i < 8; i++ {
				res, err := env.EvaluateVM(converted.(*evalengine.CompiledExpr))
				if err != nil {
					t.Fatal(err)
				}

				if res.String() != tc.result {
					t.Errorf("bad evaluation from compiler: got %s, want %s (iteration %d)", res, tc.result, i)
				}
			}
		})
	}
}
