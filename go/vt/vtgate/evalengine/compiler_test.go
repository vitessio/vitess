package evalengine_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

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
	for _, v := range values {
		field := &querypb.Field{
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
	track := NewTracker()

	for _, tc := range testcases.Cases {
		t.Run(tc.Name(), func(t *testing.T) {
			var supported, total int
			env := evalengine.EmptyExpressionEnv()
			env.Fields = tc.Schema

			tc.Run(func(query string, row []sqltypes.Value) {
				stmt, err := sqlparser.Parse("SELECT " + query)
				if err != nil {
					// no need to test un-parseable queries
					return
				}

				queryAST := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
				converted, err := evalengine.TranslateEx(queryAST, &evalengine.LookupIntegrationTest{collations.CollationUtf8mb4ID}, false)
				if err != nil {
					return
				}

				total++
				env.Row = row
				expected, evalErr := env.Evaluate(converted)
				program, compileErr := evalengine.Compile(converted, env.Fields)

				if compileErr != nil {
					switch {
					case vterrors.Code(compileErr) == vtrpcpb.Code_UNIMPLEMENTED:
						t.Logf("unsupported: %s", query)
					case evalErr == nil:
						t.Errorf("failed compilation:\nSQL:  %s\nError: %s", query, compileErr)
					case evalErr.Error() != compileErr.Error():
						t.Errorf("error mismatch:\nSQL:  %s\nError eval: %s\nError comp: %s", query, evalErr, compileErr)
					default:
						supported++
					}
					return
				}

				var vm evalengine.VirtualMachine
				res, vmErr := func() (res evalengine.EvalResult, err error) {
					defer func() {
						if r := recover(); r != nil {
							err = fmt.Errorf("PANIC: %v", r)
						}
					}()
					res, err = vm.Run(program, env.Row)
					return
				}()

				if vmErr != nil {
					switch {
					case evalErr == nil:
						t.Errorf("failed evaluation from compiler:\nSQL:  %s\nError: %s", query, err)
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

func TestCompiler(t *testing.T) {
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
	}

	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			expr, err := sqlparser.ParseExpr(tc.expression)
			if err != nil {
				t.Fatal(err)
			}

			converted, err := evalengine.TranslateEx(expr, &evalengine.LookupIntegrationTest{collations.CollationUtf8mb4ID}, false)
			if err != nil {
				t.Fatal(err)
			}

			compiled, err := evalengine.Compile(converted, makeFields(tc.values), evalengine.WithAssemblerLog(&debugCompiler{t}))
			if err != nil {
				t.Fatal(err)
			}

			var vm evalengine.VirtualMachine
			// re-run the same evaluation multiple times to ensure results are always consistent
			for i := 0; i < 8; i++ {
				res, err := vm.Run(compiled, tc.values)
				if err != nil {
					t.Fatal(err)
				}

				if res.String() != tc.result {
					t.Fatalf("bad evaluation: got %s, want %s (iteration %d)", res, tc.result, i)
				}
			}
		})
	}
}
