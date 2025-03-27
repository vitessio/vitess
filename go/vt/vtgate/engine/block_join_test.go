/*
Copyright 2025 The Vitess Authors.

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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/sqltypes"
)

func TestBlockJoinExecute(t *testing.T) {
	// Tests executing the BlockJoin primitive with fake input primitives.
	type testCase struct {
		cols             []int
		CopyColumnsToRHS []int
		rhsResults       []*sqltypes.Result
		expectedRHSLog   []string
	}

	testCases := []testCase{
		{
			/*
				select col1, col2, col3, col4, col5, col6 from left join right on left.col1 = right.col4
				LHS: select col1, col2, col3 from left
				RHS: select col1, col2, col3, col4, col5, col6 from (values row(1,2,3), ...) left(col1,col2,col3) join right on left.col1 = right.col4
			*/

			rhsResults: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"col1|col2|col3|col4|col5|col6",
						"int64|varchar|varchar|int64|varchar|varchar",
					),
					"1|a|aa|1|d|dd",
					"2|b|bb|2|e|ee",
					"3|c|cc|3|f|ff",
					"4|d|dd|4|g|gg",
				),
			},
			expectedRHSLog: []string{
				`Execute a: type:INT64 value:"10" v: [[INT64(1) VARCHAR("a") VARCHAR("aa")][INT64(2) VARCHAR("b") VARCHAR("bb")][INT64(3) VARCHAR("c") VARCHAR("cc")][INT64(4) VARCHAR("d") VARCHAR("dd")]] true`,
			},
		},
	}

	for _, tc := range testCases {
		t.Run("toto", func(t *testing.T) {
			leftPrim := &fakePrimitive{
				useNewPrintBindVars: true,
				results: []*sqltypes.Result{
					sqltypes.MakeTestResult(
						sqltypes.MakeTestFields(
							"col1|col2|col3",
							"int64|varchar|varchar",
						),
						"1|a|aa",
						"2|b|bb",
						"3|c|cc",
						"4|d|dd",
					),
				},
			}
			rightPrim := &fakePrimitive{
				useNewPrintBindVars: true,
				results:             tc.rhsResults,
			}

			bv := map[string]*querypb.BindVariable{
				"a": sqltypes.Int64BindVariable(10),
			}

			vjn := &BlockJoin{
				Left:        leftPrim,
				Right:       rightPrim,
				BindVarName: "v",
			}

			r, err := vjn.TryExecute(context.Background(), &noopVCursor{}, bv, true)
			require.NoError(t, err)
			leftPrim.ExpectLog(t, []string{
				`Execute a: type:INT64 value:"10" true`,
			})
			rightPrim.ExpectLog(t, tc.expectedRHSLog)

			result := sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3|col4|col5|col6",
					"int64|varchar|varchar|int64|varchar|varchar",
				),
				"1|a|aa|1|d|dd",
				"2|b|bb|2|e|ee",
				"3|c|cc|3|f|ff",
				"4|d|dd|4|g|gg",
			)
			expectResult(t, r, result)
		})
	}
}
