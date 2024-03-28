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
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

func r(names, types string, rows ...string) *sqltypes.Result {
	fields := sqltypes.MakeTestFields(names, types)
	for _, f := range fields {
		if sqltypes.IsText(f.Type) {
			f.Charset = collations.CollationUtf8mb4ID
		} else {
			f.Charset = collations.CollationBinaryID
		}
		_, flags := sqltypes.TypeToMySQL(f.Type)
		f.Flags = uint32(flags)
	}
	return sqltypes.MakeTestResult(fields, rows...)
}

func TestConcatenate_NoErrors(t *testing.T) {
	type testCase struct {
		testName       string
		inputs         []*sqltypes.Result
		expectedResult *sqltypes.Result
		expectedError  string
		ignoreTypes    []int
	}

	r1 := r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2")
	r2 := r("id|col1|col2", "int32|varbinary|varbinary", "1|a1|b1", "2|a2|b2")
	combinedResult := r1
	combinedResult.Rows = append(combinedResult.Rows, r2.Rows...)

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
		expectedResult: r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2", "1|a1|b1", "2|a2|b2", "1|a1|b1", "2|a2|b2"),
	}, {
		testName: "ignored field types - ignored",
		inputs: []*sqltypes.Result{
			r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2"),
			r("id|col1|col2", "int32|varbinary|varbinary", "1|a1|b1", "2|a2|b2"),
		},
		expectedResult: combinedResult,
		ignoreTypes:    []int{0},
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
		for _, tx := range []bool{false, true} {
			var sources []Primitive
			for _, input := range tc.inputs {
				// input is added twice, since the first one is used by execute and the next by stream execute
				sources = append(sources, &fakePrimitive{results: []*sqltypes.Result{input, input}})
			}

			concatenate := NewConcatenate(sources, tc.ignoreTypes)
			vcursor := &noopVCursor{inTx: tx}
			txStr := "InTx"
			if !tx {
				txStr = "NotInTx"
			}
			t.Run(fmt.Sprintf("%s-%s-Exec", txStr, tc.testName), func(t *testing.T) {
				qr, err := concatenate.TryExecute(context.Background(), vcursor, nil, true)
				if tc.expectedError == "" {
					require.NoError(t, err)
					utils.MustMatch(t, tc.expectedResult.Fields, qr.Fields, "fields")
					utils.MustMatch(t, tc.expectedResult.Rows, qr.Rows)
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.expectedError)
				}
			})

			t.Run(fmt.Sprintf("%s-%s-StreamExec", txStr, tc.testName), func(t *testing.T) {
				qr, err := wrapStreamExecute(concatenate, vcursor, nil, true)
				if tc.expectedError == "" {
					require.NoError(t, err)
					require.NoError(t, sqltypes.RowsEquals(tc.expectedResult.Rows, qr.Rows))
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.expectedError)
				}
			})
		}
	}
}

func TestConcatenate_WithErrors(t *testing.T) {
	strFailed := "failed"

	fake := r("id|col1|col2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2")
	concatenate := NewConcatenate(
		[]Primitive{
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
			&fakePrimitive{results: []*sqltypes.Result{nil, nil}, sendErr: errors.New(strFailed)},
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
		}, nil,
	)
	_, err := concatenate.TryExecute(context.Background(), &noopVCursor{}, nil, true)
	require.EqualError(t, err, strFailed)

	_, err = wrapStreamExecute(concatenate, &noopVCursor{}, nil, true)
	require.EqualError(t, err, strFailed)

	concatenate = NewConcatenate(
		[]Primitive{
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
			&fakePrimitive{results: []*sqltypes.Result{nil, nil}, sendErr: errors.New(strFailed)},
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
		}, nil)

	_, err = concatenate.TryExecute(context.Background(), &noopVCursor{}, nil, true)
	require.EqualError(t, err, strFailed)
	_, err = wrapStreamExecute(concatenate, &noopVCursor{}, nil, true)
	require.EqualError(t, err, strFailed)
}

func TestConcatenateTypes(t *testing.T) {
	tests := []struct {
		t1, t2, expected string
	}{
		{t1: "int32", t2: "int64", expected: `[name:"id" type:int64 charset:63]`},
		{t1: "int32", t2: "int32", expected: `[name:"id" type:int32 charset:63]`},
		{t1: "int32", t2: "varchar", expected: `[name:"id" type:varchar charset:255]`},
		{t1: "int32", t2: "decimal", expected: `[name:"id" type:decimal charset:63]`},
		{t1: "hexval", t2: "uint64", expected: `[name:"id" type:varchar charset:255]`},
		{t1: "varchar", t2: "varbinary", expected: `[name:"id" type:varbinary charset:63 flags:128]`},
	}

	for _, test := range tests {
		name := fmt.Sprintf("%s - %s", test.t1, test.t2)
		t.Run(name, func(t *testing.T) {
			in1 := r("id", test.t1, "1")
			in2 := r("id", test.t2, "1")
			concatenate := NewConcatenate(
				[]Primitive{
					&fakePrimitive{results: []*sqltypes.Result{in1}},
					&fakePrimitive{results: []*sqltypes.Result{in2}},
				}, nil,
			)

			res, err := concatenate.GetFields(context.Background(), &noopVCursor{}, nil)
			require.NoError(t, err)

			assert.Equal(t, test.expected, strings.ToLower(fmt.Sprintf("%v", res.Fields)))
		})
	}
}
