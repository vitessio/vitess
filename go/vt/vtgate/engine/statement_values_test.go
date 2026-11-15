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

	"vitess.io/vitess/go/sqltypes"

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
