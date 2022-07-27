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
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

func TestDistinct(t *testing.T) {
	type testCase struct {
		testName       string
		inputs         *sqltypes.Result
		collations     []collations.ID
		expectedResult *sqltypes.Result
		expectedError  string
	}

	testCases := []*testCase{{
		testName:       "empty",
		inputs:         r("id1|col11|col12", "int64|varbinary|varbinary"),
		expectedResult: r("id1|col11|col12", "int64|varbinary|varbinary"),
	}, {
		testName:       "int64 numbers",
		inputs:         r("myid", "int64", "0", "1", "1", "null", "null"),
		expectedResult: r("myid", "int64", "0", "1", "null"),
	}, {
		testName:       "int64 numbers, two columns",
		inputs:         r("a|b", "int64|int64", "0|0", "1|1", "1|1", "null|null", "null|null", "1|2"),
		expectedResult: r("a|b", "int64|int64", "0|0", "1|1", "null|null", "1|2"),
	}, {
		testName:       "int64 numbers, two columns",
		inputs:         r("a|b", "int64|int64", "3|3", "3|3", "3|4", "5|1", "5|1"),
		expectedResult: r("a|b", "int64|int64", "3|3", "3|4", "5|1"),
	}, {
		testName:       "float64 columns designed to produce the same hashcode but not be equal",
		inputs:         r("a|b", "float64|float64", "0.1|0.2", "0.1|0.3", "0.1|0.4", "0.1|0.5"),
		expectedResult: r("a|b", "float64|float64", "0.1|0.2", "0.1|0.3", "0.1|0.4", "0.1|0.5"),
	}, {
		testName:      "varchar columns without collations",
		inputs:        r("myid", "varchar", "monkey", "horse"),
		expectedError: "text type with an unknown/unsupported collation cannot be hashed",
	}, {
		testName:       "varchar columns with collations",
		collations:     []collations.ID{collations.ID(0x21)},
		inputs:         r("myid", "varchar", "monkey", "horse", "Horse", "Monkey", "horses", "MONKEY"),
		expectedResult: r("myid", "varchar", "monkey", "horse", "horses"),
	}, {
		testName:       "mixed columns",
		collations:     []collations.ID{collations.ID(0x21), collations.Unknown},
		inputs:         r("myid|id", "varchar|int64", "monkey|1", "horse|1", "Horse|1", "Monkey|1", "horses|1", "MONKEY|2"),
		expectedResult: r("myid|id", "varchar|int64", "monkey|1", "horse|1", "horses|1", "MONKEY|2"),
	}}

	for _, tc := range testCases {
		var checkCols []CheckCol
		if len(tc.inputs.Rows) > 0 {
			for i := range tc.inputs.Rows[0] {
				collID := collations.Unknown
				if tc.collations != nil {
					collID = tc.collations[i]
				}
				if sqltypes.IsNumber(tc.inputs.Fields[i].Type) {
					collID = collations.CollationBinaryID
				}
				checkCols = append(checkCols, CheckCol{
					Col:       i,
					Collation: collID,
				})
			}
		}
		t.Run(tc.testName+"-Execute", func(t *testing.T) {
			distinct := &Distinct{
				Source:    &fakePrimitive{results: []*sqltypes.Result{tc.inputs}},
				CheckCols: checkCols,
				Truncate:  false,
			}

			qr, err := distinct.TryExecute(context.Background(), &noopVCursor{}, nil, true)
			if tc.expectedError == "" {
				require.NoError(t, err)
				got := fmt.Sprintf("%v", qr.Rows)
				expected := fmt.Sprintf("%v", tc.expectedResult.Rows)
				utils.MustMatch(t, expected, got, "result not what correct")
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
		t.Run(tc.testName+"-StreamExecute", func(t *testing.T) {
			distinct := &Distinct{
				Source:    &fakePrimitive{results: []*sqltypes.Result{tc.inputs}},
				CheckCols: checkCols,
			}

			result, err := wrapStreamExecute(distinct, &noopVCursor{}, nil, true)

			if tc.expectedError == "" {
				require.NoError(t, err)
				got := fmt.Sprintf("%v", result.Rows)
				expected := fmt.Sprintf("%v", tc.expectedResult.Rows)
				utils.MustMatch(t, expected, got, "result not what correct")
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

func TestWeightStringFallBack(t *testing.T) {
	offsetOne := 1
	checkCols := []CheckCol{{
		Col:       0,
		WsCol:     &offsetOne,
		Collation: collations.Unknown,
	}}
	input := r("myid|weightstring(myid)",
		"varchar|varbinary",
		"monkey|monkey",
		"horse|horse",
		"horse|horse")

	distinct := &Distinct{
		Source:    &fakePrimitive{results: []*sqltypes.Result{input}},
		CheckCols: checkCols,
		Truncate:  true,
	}

	qr, err := distinct.TryExecute(context.Background(), &noopVCursor{}, nil, true)
	require.NoError(t, err)

	got := fmt.Sprintf("%v", qr.Rows)
	expected := fmt.Sprintf("%v", r("myid", "varchar", "monkey", "horse").Rows)
	utils.MustMatch(t, expected, got)

	// the primitive must not change just because one run needed weight strings
	utils.MustMatch(t, []CheckCol{{
		Col:       0,
		WsCol:     &offsetOne,
		Collation: collations.Unknown,
	}}, distinct.CheckCols, "checkCols should not be updated")
}
