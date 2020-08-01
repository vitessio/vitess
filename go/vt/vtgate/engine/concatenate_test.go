/*
Copyright 2020 The Vitess Authors.

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
	"errors"
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
)

func TestConcatenate_NoSourcesErr(t *testing.T) {
	type testCase struct {
		testName       string
		inputs         []*sqltypes.Result
		expectedResult *sqltypes.Result
		expectedError  string
	}

	testCases := []*testCase{{
		testName: "empty results",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id1|col11|col12", "int64|varbinary|varbinary")),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id2|col21|col22", "int64|varbinary|varbinary")),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id3|col31|col32", "int64|varbinary|varbinary")),
		},
		expectedResult: sqltypes.MakeTestResult(sqltypes.MakeTestFields("id1|col11|col12", "int64|varbinary|varbinary")),
	}, {
		testName: "2 non empty result",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("myid|mycol1|mycol2", "int64|varchar|varbinary"), "11|m1|n1", "22|m2|n2"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varchar|varbinary"), "1|a1|b1", "2|a2|b2"),
		},
		expectedResult: sqltypes.MakeTestResult(sqltypes.MakeTestFields("myid|mycol1|mycol2", "int64|varchar|varbinary"), "11|m1|n1", "22|m2|n2", "1|a1|b1", "2|a2|b2"),
	}, {
		testName: "mismatch field type",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varbinary|varbinary"), "1|a1|b1", "2|a2|b2"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col3|col4", "int64|varchar|varbinary"), "1|a1|b1", "2|a2|b2"),
		},
		expectedError: "column field type does not match for name: (col1, col3) types: (VARBINARY, VARCHAR)",
	}, {
		testName: "input source has different column count",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varchar|varchar"), "1|a1|b1", "2|a2|b2"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col3|col4|col5", "int64|varchar|varchar|int32"), "1|a1|b1|5", "2|a2|b2|6"),
		},
		expectedError: "The used SELECT statements have a different number of columns (errno 1222) (sqlstate 21000)",
	}, {
		testName: "1 empty result and 1 non empty result",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("myid|mycol1|mycol2", "int64|varchar|varbinary")),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varchar|varbinary"), "1|a1|b1", "2|a2|b2"),
		},
		expectedResult: sqltypes.MakeTestResult(sqltypes.MakeTestFields("myid|mycol1|mycol2", "int64|varchar|varbinary"), "1|a1|b1", "2|a2|b2"),
	}}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			var fps []Primitive
			for _, input := range tc.inputs {
				fps = append(fps, &fakePrimitive{results: []*sqltypes.Result{input, input}, sendErr: errors.New("abc")})
			}
			concatenate := &Concatenate{Sources: fps}

			t.Run("Execute wantfields true", func(t *testing.T) {
				qr, err := concatenate.Execute(&noopVCursor{ctx: context.Background()}, nil, true)
				if tc.expectedError == "" {
					require.NoError(t, err)
					require.Equal(t, tc.expectedResult, qr)
				} else {
					require.EqualError(t, err, tc.expectedError)
				}
			})

			t.Run("StreamExecute wantfields true", func(t *testing.T) {
				qr, err := wrapStreamExecute(concatenate, &noopVCursor{ctx: context.Background()}, nil, true)
				if tc.expectedError == "" {
					require.NoError(t, err)
					require.Equal(t, utils.SortString(fmt.Sprintf("%v", tc.expectedResult.Rows)), utils.SortString(fmt.Sprintf("%v", qr.Rows)))
				} else {
					require.EqualError(t, err, tc.expectedError)
				}
			})
		})
	}
}

func TestConcatenate_WithSourcesErrFirst(t *testing.T) {
	type testCase struct {
		testName string
		inputs   []*sqltypes.Result
	}

	strFailed := "failed"
	executeErr := "Concatenate.Execute: " + strFailed
	testCases := []*testCase{{
		testName: "empty results",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id1|col11|col12", "int64|varbinary|varbinary")),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id2|col21|col22", "int64|varbinary|varbinary")),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id3|col31|col32", "int64|varbinary|varbinary")),
		},
	}, {
		testName: "2 non empty result",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("myid|mycol1|mycol2", "int64|varchar|varbinary"), "11|m1|n1", "22|m2|n2"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varchar|varbinary"), "1|a1|b1", "2|a2|b2"),
		},
	}, {
		testName: "mismatch field type",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varbinary|varbinary"), "1|a1|b1", "2|a2|b2"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col3|col4", "int64|varchar|varbinary"), "1|a1|b1", "2|a2|b2"),
		},
	}, {
		testName: "input source has different column count",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varchar|varchar"), "1|a1|b1", "2|a2|b2"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col3|col4|col5", "int64|varchar|varchar|int32"), "1|a1|b1|5", "2|a2|b2|6"),
		},
	}, {
		testName: "1 empty result and 1 non empty result",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("myid|mycol1|mycol2", "int64|varchar|varbinary")),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varchar|varbinary"), "1|a1|b1", "2|a2|b2"),
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			var fps []Primitive
			fps = append(fps, &fakePrimitive{results: []*sqltypes.Result{nil, nil}, sendErr: errors.New(strFailed)})
			for _, input := range tc.inputs {
				fps = append(fps, &fakePrimitive{results: []*sqltypes.Result{input, input}})
			}
			concatenate := &Concatenate{Sources: fps}

			t.Run("Execute wantfields true", func(t *testing.T) {
				_, err := concatenate.Execute(&noopVCursor{ctx: context.Background()}, nil, true)
				require.EqualError(t, err, executeErr)

			})

			t.Run("StreamExecute wantfields true", func(t *testing.T) {
				_, err := wrapStreamExecute(concatenate, &noopVCursor{ctx: context.Background()}, nil, true)
				require.EqualError(t, err, strFailed)
			})
		})
	}
}

func TestConcatenate_WithSourcesErrLast(t *testing.T) {
	type testCase struct {
		testName               string
		inputs                 []*sqltypes.Result
		execErr, streamExecErr string
		nonDeterministicErr    bool
	}

	strFailed := "failed"
	executeErr := "Concatenate.Execute: " + strFailed
	testCases := []*testCase{{
		testName: "empty results",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id1|col11|col12", "int64|varbinary|varbinary")),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id2|col21|col22", "int64|varbinary|varbinary")),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id3|col31|col32", "int64|varbinary|varbinary")),
		},
		execErr:       executeErr,
		streamExecErr: strFailed,
	}, {
		testName: "2 non empty result",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("myid|mycol1|mycol2", "int64|varchar|varbinary"), "11|m1|n1", "22|m2|n2"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varchar|varbinary"), "1|a1|b1", "2|a2|b2"),
		},
		execErr:       executeErr,
		streamExecErr: strFailed,
	}, {
		testName: "mismatch field type",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varbinary|varbinary"), "1|a1|b1", "2|a2|b2"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col3|col4", "int64|varchar|varbinary"), "1|a1|b1", "2|a2|b2"),
		},
		execErr:             "column field type does not match for name: (col1, col3) types: (VARBINARY, VARCHAR)",
		streamExecErr:       "column field type does not match for name: (col1, col3) types: (VARBINARY, VARCHAR)",
		nonDeterministicErr: true,
	}, {
		testName: "input source has different column count",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varchar|varchar"), "1|a1|b1", "2|a2|b2"),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col3|col4|col5", "int64|varchar|varchar|int32"), "1|a1|b1|5", "2|a2|b2|6"),
		},
		execErr:             "The used SELECT statements have a different number of columns (errno 1222) (sqlstate 21000)",
		streamExecErr:       strFailed,
		nonDeterministicErr: true,
	}, {
		testName: "1 empty result and 1 non empty result",
		inputs: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("myid|mycol1|mycol2", "int64|varchar|varbinary")),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1|col2", "int64|varchar|varbinary"), "1|a1|b1", "2|a2|b2"),
		},
		execErr:       executeErr,
		streamExecErr: strFailed,
	}}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			var fps []Primitive
			for _, input := range tc.inputs {
				fps = append(fps, &fakePrimitive{results: []*sqltypes.Result{input, input}})
			}
			fps = append(fps, &fakePrimitive{results: []*sqltypes.Result{nil, nil}, sendErr: errors.New(strFailed)})
			concatenate := &Concatenate{Sources: fps}

			t.Run("Execute wantfields true", func(t *testing.T) {
				_, err := concatenate.Execute(&noopVCursor{ctx: context.Background()}, nil, true)
				require.EqualError(t, err, tc.execErr)
			})

			t.Run("StreamExecute wantfields true", func(t *testing.T) {
				_, err := wrapStreamExecute(concatenate, &noopVCursor{ctx: context.Background()}, nil, true)
				require.Error(t, err)
				if !tc.nonDeterministicErr {
					require.EqualError(t, err, tc.streamExecErr)
				}
			})
		})
	}
}
