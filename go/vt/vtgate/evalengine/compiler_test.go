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
		{
			expression: `CAST(20000229235959.999950 AS DATETIME(4))`,
			result:     `DATETIME("2000-03-01 00:00:00.0000")`,
		},
		{
			expression: `CAST(1.5678 AS TIME(2))`,
			result:     `TIME("00:00:01.57")`,
		},
		{
			expression: `CAST(235959.995 AS TIME(2))`,
			result:     `TIME("24:00:00.00")`,
		},
		{
			expression: `CAST(-235959.995 AS TIME(2))`,
			result:     `TIME("-24:00:00.00")`,
		},
		{
			expression: `WEEK('2000-01-02', 6)`,
			result:     `INT64(1)`,
		},
		{
			expression: `WEEK(date '2000-01-01', 4)`,
			result:     `INT64(0)`,
		},
		{
			// This is the day of DST change in Europe/Amsterdam when
			// the year started on a Wednesday. Regression test for
			// using 24 hour time diffing instead of days.
			expression: `WEEK(date '2014-10-26', 6)`,
			result:     `INT64(44)`,
		},
		{
			expression: `MAKEDATE(cast('invalid' as json), NULL)`,
			result:     `NULL`,
		},
		{
			expression: `MAKETIME(NULL, '', cast('invalid' as json))`,
			result:     `NULL`,
		},
		{
			expression: `1 = ' 1 '`,
			result:     `INT64(1)`,
		},
		{
			expression: `CAST(' 0 ' AS TIME)`,
			result:     `TIME("00:00:00")`,
		},
		{
			expression: `CAST('0' AS TIME)`,
			result:     `TIME("00:00:00")`,
		},
		{
			expression: `timestamp '2000-01-01 10:34:58.978654' DIV '\t1 foo\t'`,
			result:     `INT64(20000101103458)`,
		},
		{
			expression: `UNHEX('f')`,
			result:     `VARBINARY("\x0f")`,
		},
		{
			expression: `STRCMP(1234, '12_4')`,
			result:     `INT64(-1)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, 0)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, 0, 1, 0)`,
			result:     `INT64(1)`,
		},
		{
			expression: `INTERVAL(0, 1, 0, 0)`,
			result:     `INT64(0)`,
		},
		{
			expression: `INTERVAL(0, -1, 0, 0)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, 1, 1, 1)`,
			result:     `INT64(0)`,
		},
		{
			expression: `INTERVAL(0, -1, -1, -1)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, 1)`,
			result:     `INT64(2)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, -1)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, NULL, 0, 0)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(NULL, 0, 0, 0)`,
			result:     `INT64(-1)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, NULL)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, NULL, 1, 1)`,
			result:     `INT64(3)`,
		},
		{
			expression: `INTERVAL(0, 0, 2, NULL, 1, 1)`,
			result:     `INT64(1)`,
		},
		{
			expression: `INTERVAL(0, 2, -1, NULL, -1, 1)`,
			result:     `INT64(0)`,
		},
		{
			expression: `INTERVAL(0, 2, NULL, NULL, -1, 1)`,
			result:     `INT64(0)`,
		},
		{
			expression: `INTERVAL(0, NULL, NULL, NULL, -1, 1)`,
			result:     `INT64(4)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, -1, NULL, 1)`,
			result:     `INT64(4)`,
		},
		{
			expression: `INTERVAL(0, 0, 0, -1, NULL, NULL, 1)`,
			result:     `INT64(5)`,
		},
		{
			expression: `REGEXP_REPLACE(1234, 12, 6, 1)`,
			result:     `TEXT("634")`,
		},
		{
			expression: `_latin1 0xFF`,
			result:     `VARCHAR("ÿ")`,
		},
		{
			expression: `TRIM(_latin1 0xA078A0 FROM _utf8mb4 0xC2A078C2A0)`,
			result:     `VARCHAR("")`,
		},
		{
			expression: `CONCAT_WS("😊😂🤢", date '2000-01-01', _latin1 0xFF)`,
			result:     `VARCHAR("2000-01-01😊😂🤢ÿ")`,
		},
		{
			expression: `concat('test', _latin1 0xff)`,
			result:     `VARCHAR("testÿ")`,
		},
		{
			expression: `WEIGHT_STRING('foobar' as char(3))`,
			result:     `VARBINARY("\x1c\xe5\x1d\xdd\x1d\xdd")`,
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

			env := evalengine.EmptyExpressionEnv()
			env.Row = tc.values

			expected, err := env.Evaluate(evalengine.Deoptimize(converted))
			if err != nil {
				t.Fatal(err)
			}
			if expected.String() != tc.result {
				t.Fatalf("bad evaluation from eval engine: got %s, want %s", expected.String(), tc.result)
			}

			if cfg.CompilerErr != nil {
				t.Fatalf("bad compilation: %v", cfg.CompilerErr)
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

func TestBindVarLiteral(t *testing.T) {
	var testCases = []struct {
		expression string
		bindType   func(expr sqlparser.Expr)
		bindVar    *querypb.BindVariable
		result     string
	}{
		{
			expression: `_latin1 :vtg1 /* HEXNUM */`,
			bindType: func(expr sqlparser.Expr) {
				expr.(*sqlparser.IntroducerExpr).Expr.(*sqlparser.Argument).Type = sqltypes.HexNum
			},
			bindVar: sqltypes.HexNumBindVariable([]byte("0xFF")),
			result:  `VARCHAR("ÿ")`,
		},
		{
			expression: `cast(:vtg1 /* HEXVAL */ as char character set latin1)`,
			bindType: func(expr sqlparser.Expr) {
				expr.(*sqlparser.CastExpr).Expr.(*sqlparser.Argument).Type = sqltypes.HexVal
			},
			bindVar: sqltypes.HexValBindVariable([]byte("0'FF'")),
			result:  `VARCHAR("ÿ")`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			expr, err := sqlparser.ParseExpr(tc.expression)
			if err != nil {
				t.Fatal(err)
			}

			tc.bindType(expr)

			fields := evalengine.FieldResolver(makeFields(nil))
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

			result := `VARCHAR("ÿ")`

			env := evalengine.EmptyExpressionEnv()
			env.BindVars = map[string]*querypb.BindVariable{
				"vtg1": tc.bindVar,
			}

			expected, err := env.Evaluate(evalengine.Deoptimize(converted))
			if err != nil {
				t.Fatal(err)
			}
			if expected.String() != result {
				t.Fatalf("bad evaluation from eval engine: got %s, want %s", expected.String(), result)
			}

			if cfg.CompilerErr != nil {
				t.Fatalf("bad compilation: %v", cfg.CompilerErr)
			}

			// re-run the same evaluation multiple times to ensure results are always consistent
			for i := 0; i < 8; i++ {
				res, err := env.EvaluateVM(converted.(*evalengine.CompiledExpr))
				if err != nil {
					t.Fatal(err)
				}

				if res.String() != result {
					t.Errorf("bad evaluation from compiler: got %s, want %s (iteration %d)", res, result, i)
				}
			}
		})
	}
}
