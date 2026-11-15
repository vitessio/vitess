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

package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// latin1Result is a result with an INT and a latin1_german2_ci VARCHAR(10)
// with 'Müller' as a tablet with --queryserver-config-raw-result-charset
// returns it: 4D FC 6C 6C 65 72 with the collation 31.
func latin1Result() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int64, Charset: 63},
			{Name: "l", Type: sqltypes.VarChar, Charset: 31, ColumnLength: 10},
		},
		Rows: []sqltypes.Row{{sqltypes.NewInt64(1), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xFCller"))}},
	}
}

// TestJoinStatementValues checks the bind variable that a join makes of a
// value of a left row for its right side. With --convert-result-charset the
// value of a latin1 column is latin1 in the row, and has to be utf8mb4, 4D C3
// BC 6C 6C 65 72, in the bind variable, which goes into a statement. The row
// that the join returns keeps the value in latin1, with its field. Without the
// flag the value is the bind variable as it is.
func TestJoinStatementValues(t *testing.T) {
	for _, convert := range []bool{true, false} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("convert %v, stream %v", convert, stream), func(t *testing.T) {
				leftPrim := &fakePrimitive{results: []*sqltypes.Result{latin1Result()}}
				rightPrim := &fakePrimitive{results: []*sqltypes.Result{
					sqltypes.MakeTestResult(sqltypes.MakeTestFields("col4", "int64"), "4"),
				}}
				jn := &Join{
					Opcode: InnerJoin,
					Left:   leftPrim,
					Right:  rightPrim,
					Cols:   []int{-2, 1},
					Vars:   map[string]int{"bv": 1},
				}
				vcursor := &noopVCursor{convertResultCharset: convert}

				var r *sqltypes.Result
				var err error
				if stream {
					r, err = wrapStreamExecute(jn, vcursor, map[string]*querypb.BindVariable{}, true)
				} else {
					r, err = jn.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, true)
				}
				require.NoError(t, err)

				want := "M\xFCller"
				if convert {
					want = "Müller"
				}
				method := "Execute"
				if stream {
					method = "StreamExecute"
				}
				rightPrim.ExpectLog(t, []string{
					fmt.Sprintf(`%s %v true`, method, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable(want)})),
				})

				require.Len(t, r.Rows, 1)
				assert.Equal(t, uint32(31), r.Fields[0].Charset)
				assert.Equal(t, "M\xFCller", r.Rows[0][0].ToString())
			})
		}
	}
}

// TestSemiJoinStatementValues checks the bind variable that a semi-join makes
// of a value of a left row for its subquery. With --convert-result-charset the
// value of a latin1 column is utf8mb4 in the bind variable, and the left row
// that the semi-join returns keeps it in latin1. The semi-join is asked for no
// fields, so that it has to ask for the fields of its left side itself.
func TestSemiJoinStatementValues(t *testing.T) {
	for _, convert := range []bool{true, false} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("convert %v, stream %v", convert, stream), func(t *testing.T) {
				leftPrim := &fakePrimitive{results: []*sqltypes.Result{latin1Result()}}
				rightPrim := &fakePrimitive{results: []*sqltypes.Result{
					sqltypes.MakeTestResult(sqltypes.MakeTestFields("col4", "int64"), "4"),
				}}
				jn := &SemiJoin{Left: leftPrim, Right: rightPrim, Vars: map[string]int{"bv": 1}}
				vcursor := &noopVCursor{convertResultCharset: convert}

				var r *sqltypes.Result
				var err error
				if stream {
					r, err = wrapStreamExecute(jn, vcursor, map[string]*querypb.BindVariable{}, false)
				} else {
					r, err = jn.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, false)
				}
				require.NoError(t, err)

				method := "Execute"
				if stream {
					method = "StreamExecute"
				}
				rightPrim.ExpectLog(t, []string{
					fmt.Sprintf(`%s %v false`, method, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable(wantStatementValue(convert))})),
				})

				require.Len(t, r.Rows, 1)
				assert.Equal(t, "M\xFCller", r.Rows[0][1].ToString())
			})
		}
	}
}

// wantStatementValue is 'Müller' of latin1Result as it has to be in a bind
// variable: utf8mb4 with --convert-result-charset, and the bytes of the row
// without it.
func wantStatementValue(convert bool) string {
	if convert {
		return "Müller"
	}
	return "M\xFCller"
}

// TestRecurseCTEStatementValues checks the bind variables that a recursive CTE
// makes of the values of the rows of its seed and of each recursion, for the
// next recursion. With --convert-result-charset 'Müller' of the seed and 'é'
// of the first recursion, both of a latin1 column, are utf8mb4 in the bind
// variable, and the rows that the CTE returns keep them in latin1. The CTE is
// asked for no fields, so that it has to ask for the fields itself.
func TestRecurseCTEStatementValues(t *testing.T) {
	for _, convert := range []bool{true, false} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("convert %v, stream %v", convert, stream), func(t *testing.T) {
				seed := &fakePrimitive{results: []*sqltypes.Result{latin1Result()}}
				recursion := latin1Result()
				recursion.Rows = []sqltypes.Row{{sqltypes.NewInt64(2), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("\xE9"))}}
				end := latin1Result()
				end.Rows = nil
				term := &fakePrimitive{results: []*sqltypes.Result{recursion, end}}
				cte := &RecurseCTE{Seed: seed, Term: term, Vars: map[string]int{"bv": 1}}
				vcursor := &noopVCursor{convertResultCharset: convert}

				var r *sqltypes.Result
				var err error
				method := "Execute"
				if stream {
					method = "StreamExecute"
					r, err = wrapStreamExecute(cte, vcursor, map[string]*querypb.BindVariable{}, false)
				} else {
					r, err = cte.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, false)
				}
				require.NoError(t, err)

				first, second := "M\xFCller", "\xE9"
				if convert {
					first, second = "Müller", "é"
				}
				term.ExpectLog(t, []string{
					fmt.Sprintf(`%s %v %v`, method, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable(first)}), convert),
					fmt.Sprintf(`%s %v %v`, method, printBindVars(map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable(second)}), convert),
				})

				require.Len(t, r.Rows, 2)
				assert.Equal(t, "M\xFCller", r.Rows[0][1].ToString())
				assert.Equal(t, "\xE9", r.Rows[1][1].ToString())
			})
		}
	}
}

// TestValuesJoinStatementValues checks the rows of the VALUES that a values
// join makes of its left rows for its right side. With --convert-result-charset
// the value of a latin1 column is utf8mb4 in the row of the VALUES, and the
// row that the join returns keeps it in latin1. The join is asked for no
// fields, so that it has to ask for the fields of its left side itself.
func TestValuesJoinStatementValues(t *testing.T) {
	for _, convert := range []bool{true, false} {
		t.Run(fmt.Sprintf("convert %v", convert), func(t *testing.T) {
			leftPrim := &fakePrimitive{results: []*sqltypes.Result{latin1Result()}}
			rightPrim := &fakePrimitive{results: []*sqltypes.Result{
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("x|id", "int64|int64"), "4|0"),
			}}
			jv := &ValuesJoin{
				Left:              leftPrim,
				Right:             rightPrim,
				Vars:              []int{1},
				RowConstructorArg: "v",
				Cols:              []int{-2, 1},
				ColNames:          []string{"l", "x"},
			}
			vcursor := &noopVCursor{convertResultCharset: convert}

			bindVars := map[string]*querypb.BindVariable{}
			r, err := jv.TryExecute(t.Context(), vcursor, bindVars, false)
			require.NoError(t, err)

			want := sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewInt64(0), sqltypes.NewVarChar(wantStatementValue(convert))})
			require.Len(t, bindVars["v"].Values, 1)
			assert.True(t, proto.Equal(want, bindVars["v"].Values[0]), "got %v", bindVars["v"].Values[0])

			require.Len(t, r.Rows, 1)
			assert.Equal(t, "M\xFCller", r.Rows[0][0].ToString())
		})
	}
}

// TestUncorrelatedSubqueryStatementValues checks the bind variables that a
// pulled-out subquery makes of the values of its rows for the outer query: a
// value, the columns of a row, a list of values and a list of rows. With
// --convert-result-charset 'Müller' of a latin1 column is utf8mb4 in each of
// them. Without the flag the values are the bind variables as they are.
func TestUncorrelatedSubqueryStatementValues(t *testing.T) {
	// The subquery returns the latin1 column first, and the INT second.
	subqueryResult := func() *sqltypes.Result {
		r := latin1Result()
		r.Fields[0], r.Fields[1] = r.Fields[1], r.Fields[0]
		r.Rows[0][0], r.Rows[0][1] = r.Rows[0][1], r.Rows[0][0]
		return r
	}
	for _, convert := range []bool{true, false} {
		want := wantStatementValue(convert)
		tuple := func(values ...*querypb.Value) *querypb.BindVariable {
			return &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: values}
		}
		tests := []struct {
			name string
			ps   *UncorrelatedSubquery
			want map[string]*querypb.BindVariable
		}{{
			name: "value",
			ps:   &UncorrelatedSubquery{Opcode: opcode.PulloutValue, SubqueryResult: "sq"},
			want: map[string]*querypb.BindVariable{"sq": sqltypes.StringBindVariable(want)},
		}, {
			name: "row",
			ps:   &UncorrelatedSubquery{Opcode: opcode.PulloutValue, SubqueryResult: "sq", RowResults: []string{"sq_0", "sq_1"}},
			want: map[string]*querypb.BindVariable{"sq_0": sqltypes.StringBindVariable(want), "sq_1": sqltypes.Int64BindVariable(1)},
		}, {
			name: "in",
			ps:   &UncorrelatedSubquery{Opcode: opcode.PulloutIn, SubqueryResult: "sq", HasValues: "has_values"},
			want: map[string]*querypb.BindVariable{
				"has_values": sqltypes.Int64BindVariable(1),
				"sq":         tuple(sqltypes.ValueToProto(sqltypes.NewVarChar(want))),
			},
		}, {
			name: "in rows",
			ps:   &UncorrelatedSubquery{Opcode: opcode.PulloutIn, SubqueryResult: "sq", HasValues: "has_values", RowColumns: 2},
			want: map[string]*querypb.BindVariable{
				"has_values": sqltypes.Int64BindVariable(1),
				"sq":         tuple(sqltypes.TupleToProto([]sqltypes.Value{sqltypes.NewVarChar(want), sqltypes.NewInt64(1)})),
			},
		}}
		for _, tc := range tests {
			t.Run(fmt.Sprintf("convert %v, %s", convert, tc.name), func(t *testing.T) {
				outer := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("x", "int64"), "1")}}
				tc.ps.Subquery = &fakePrimitive{results: []*sqltypes.Result{subqueryResult()}}
				tc.ps.Outer = outer
				vcursor := &noopVCursor{convertResultCharset: convert}

				_, err := tc.ps.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, false)
				require.NoError(t, err)
				outer.ExpectLog(t, []string{fmt.Sprintf("Execute %v false", deprecatedPrintBindVars(tc.want))})
			})
		}
	}
}
