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

func r(names, types string, rows ...string) *sqltypes.Result {
	return sqltypes.MakeTestResult(sqltypes.MakeTestFields(names, types), rows...)
}

func TestConcatenate_NoErrors(t *testing.T) {
	type testCase struct {
		testName       string
		inputs         []*sqltypes.Result
		expectedResult *sqltypes.Result
		expectedError  string
	}

	testCases := []*testCase{{
		testName: "empty results",
		inputs: []*sqltypes.Result{
			r("id1|col11|col12", "int64|varbinary|varbinary"),
			r("id2|col21|col22", "int64|varbinary|varbinary"),
			r("id3|col31|col32", "int64|varbinary|varbinary"),
		},
		expectedResult: r("id1|col11|col12", "int64|varbinary|varbinary"),
	}, {
		testName: "2 non empty result",
		inputs: []*sqltypes.Result{
			r("myid|mycol1|mycol2", "int64|varchar|varbinary", "11|m1|n1", "22|m2|n2"),
			r("id|col1|col2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2"),
			r("id2|col2|col3", "int64|varchar|varbinary", "3|a3|b3"),
			r("id2|col2|col3", "int64|varchar|varbinary", "4|a4|b4"),
		},
		expectedResult: r("myid|mycol1|mycol2", "int64|varchar|varbinary", "11|m1|n1", "22|m2|n2", "1|a1|b1", "2|a2|b2", "3|a3|b3", "4|a4|b4"),
	}, {
		testName: "mismatch field type",
		inputs: []*sqltypes.Result{
			r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2"),
			r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2"),
			r("id|col3|col4", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2"),
		},
		expectedError: "merging field of different types is not supported",
	}, {
		testName: "input source has different column count",
		inputs: []*sqltypes.Result{
			r("id|col1|col2", "int64|varchar|varchar", "1|a1|b1", "2|a2|b2"),
			r("id|col1|col2", "int64|varchar|varchar", "1|a1|b1", "2|a2|b2"),
			r("id|col3|col4|col5", "int64|varchar|varchar|int32", "1|a1|b1|5", "2|a2|b2|6"),
		},
		expectedError: "The used SELECT statements have a different number of columns",
	}, {
		testName: "1 empty result and 1 non empty result",
		inputs: []*sqltypes.Result{
			r("myid|mycol1|mycol2", "int64|varchar|varbinary"),
			r("id|col1|col2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2"),
		},
		expectedResult: r("myid|mycol1|mycol2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2"),
	}}

	for _, tc := range testCases {
		var sources []Primitive
		for _, input := range tc.inputs {
			// input is added twice, since the first one is used by execute and the next by stream execute
			sources = append(sources, &fakePrimitive{results: []*sqltypes.Result{input, input}})
		}
		concatenate := &Concatenate{
			Sources: sources,
		}

		t.Run(tc.testName+"-Execute", func(t *testing.T) {
			qr, err := concatenate.Execute(&noopVCursor{ctx: context.Background()}, nil, true)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedResult, qr)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			}
		})

		t.Run(tc.testName+"-StreamExecute", func(t *testing.T) {
			qr, err := wrapStreamExecute(concatenate, &noopVCursor{ctx: context.Background()}, nil, true)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, utils.SortString(fmt.Sprintf("%v", tc.expectedResult.Rows)), utils.SortString(fmt.Sprintf("%v", qr.Rows)))
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			}
		})
	}
}

func TestConcatenate_WithErrors(t *testing.T) {
	strFailed := "failed"

	fake := r("id|col1|col2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2")
	concatenate := &Concatenate{
		Sources: []Primitive{
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
			&fakePrimitive{results: []*sqltypes.Result{nil, nil}, sendErr: errors.New(strFailed)},
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
		},
	}
	ctx := context.Background()
	_, err := concatenate.Execute(&noopVCursor{ctx: ctx}, nil, true)
	require.EqualError(t, err, strFailed)

	_, err = wrapStreamExecute(concatenate, &noopVCursor{ctx: ctx}, nil, true)
	require.EqualError(t, err, strFailed)

	concatenate = &Concatenate{
		Sources: []Primitive{
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
			&fakePrimitive{results: []*sqltypes.Result{nil, nil}, sendErr: errors.New(strFailed)},
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
		},
	}
	_, err = concatenate.Execute(&noopVCursor{ctx: ctx}, nil, true)
	require.EqualError(t, err, strFailed)
	_, err = wrapStreamExecute(concatenate, &noopVCursor{ctx: ctx}, nil, true)
	require.EqualError(t, err, strFailed)
}
