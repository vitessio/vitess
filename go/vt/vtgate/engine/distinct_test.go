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
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

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
		collations:     []collations.ID{collations.CollationUtf8mb4ID},
		inputs:         r("myid", "varchar", "monkey", "horse", "Horse", "Monkey", "horses", "MONKEY"),
		expectedResult: r("myid", "varchar", "monkey", "horse", "horses"),
	}, {
		testName:       "mixed columns",
		collations:     []collations.ID{collations.CollationUtf8mb4ID, collations.Unknown},
		inputs:         r("myid|id", "varchar|int64", "monkey|1", "horse|1", "Horse|1", "Monkey|1", "horses|1", "MONKEY|2"),
		expectedResult: r("myid|id", "varchar|int64", "monkey|1", "horse|1", "horses|1", "MONKEY|2"),
	}, {
		testName:       "json arrays and objects that share a shape but not a value",
		collations:     []collations.ID{collations.CollationBinaryID},
		inputs:         r("myid", "json", `[1]`, `[2]`, `[1]`, `{"a":1}`, `{"b":2}`, `{"a":1}`, `[1,2]`),
		expectedResult: r("myid", "json", `[1]`, `[2]`, `{"a":1}`, `{"b":2}`, `[1,2]`),
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
					Col:          i,
					Type:         evalengine.NewTypeEx(tc.inputs.Fields[i].Type, collID, false, 0, 0, nil),
					CollationEnv: collations.MySQL8(),
				})
			}
		}
		t.Run(tc.testName+"-Execute", func(t *testing.T) {
			distinct := &Distinct{
				Source:    &fakePrimitive{results: []*sqltypes.Result{tc.inputs}},
				CheckCols: checkCols,
			}

			qr, err := distinct.TryExecute(t.Context(), &noopVCursor{}, nil, true)
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

func TestDistinctStreamAsync(t *testing.T) {
	distinct := &Distinct{
		Source: &fakePrimitive{
			results: sqltypes.MakeTestStreamingResults(sqltypes.MakeTestFields("myid|id|num|name", "varchar|int64|int64|varchar"),
				"a|1|1|a",
				"a|1|1|a",
				"a|1|1|a",
				"a|1|1|a",
				"---",
				"c|1|1|a",
				"a|1|1|a",
				"z|1|1|a",
				"a|1|1|t",
				"a|1|1|a",
				"a|1|1|a",
				"a|1|1|a",
				"---",
				"c|1|1|a",
				"a|1|1|a",
				"---",
				"c|1|1|a",
				"a|1|1|a",
				"a|1|1|a",
				"c|1|1|a",
				"a|1|1|a",
				"a|1|1|a",
				"---",
				"c|1|1|a",
				"a|1|1|a",
			),
			async: true,
		},
		CheckCols: []CheckCol{
			{Col: 0, Type: evalengine.NewType(sqltypes.VarChar, collations.CollationUtf8mb4ID)},
			{Col: 1, Type: evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID)},
			{Col: 2, Type: evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID)},
			{Col: 3, Type: evalengine.NewType(sqltypes.VarChar, collations.CollationUtf8mb4ID)},
		},
	}

	qr := &sqltypes.Result{}
	err := distinct.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(result *sqltypes.Result) error {
		qr.Rows = append(qr.Rows, result.Rows...)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, sqltypes.RowsEqualsStr(`
[[VARCHAR("c") INT64(1) INT64(1) VARCHAR("a")] 
[VARCHAR("a") INT64(1) INT64(1) VARCHAR("a")] 
[VARCHAR("z") INT64(1) INT64(1) VARCHAR("a")] 
[VARCHAR("a") INT64(1) INT64(1) VARCHAR("t")]]`, qr.Rows))
}

func TestWeightStringFallBack(t *testing.T) {
	offsetOne := 1
	checkCols := []CheckCol{{
		Col:   0,
		WsCol: &offsetOne,
		Type:  evalengine.NewType(sqltypes.VarBinary, collations.CollationBinaryID),
	}}
	input := r("myid|weightstring(myid)",
		"varchar|varbinary",
		"monkey|monkey",
		"horse|horse",
		"horse|horse")

	distinct := &Distinct{
		Source:    &fakePrimitive{results: []*sqltypes.Result{input}},
		CheckCols: checkCols,
		Truncate:  1,
	}

	qr, err := distinct.TryExecute(t.Context(), &noopVCursor{}, nil, true)
	require.NoError(t, err)

	got := fmt.Sprintf("%v", qr.Rows)
	expected := fmt.Sprintf("%v", r("myid", "varchar", "monkey", "horse").Rows)
	utils.MustMatch(t, expected, got)

	// the primitive must not change just because one run needed weight strings
	utils.MustMatch(t, []CheckCol{{
		Col:   0,
		WsCol: &offsetOne,
		Type:  evalengine.NewType(sqltypes.VarBinary, collations.CollationBinaryID),
	}}, distinct.CheckCols, "checkCols should not be updated")
}

// TestWeightStringFallBackUnknownType checks that text of an unknown type uses its weight string,
// instead of being hashed as a number, which made all non-numeric strings equal.
func TestWeightStringFallBackUnknownType(t *testing.T) {
	input := r("name|weightstring(name)",
		"varchar|varbinary",
		"alice|alice",
		"bob|bob",
		"alice|alice")

	distinct := &Distinct{
		Source:    &fakePrimitive{results: []*sqltypes.Result{input}},
		CheckCols: []CheckCol{{Col: 0, WsCol: new(1), Type: evalengine.NewUnknownType()}},
		Truncate:  1,
	}

	qr, err := distinct.TryExecute(t.Context(), &noopVCursor{}, nil, true)
	require.NoError(t, err)
	utils.MustMatch(t, fmt.Sprintf("%v", r("name", "varchar", "alice", "bob").Rows), fmt.Sprintf("%v", qr.Rows))
}

// TestDistinctUnknownTypeValues checks that values of an unknown type that aren't text are compared
// by their weight string, or as decimals for numbers, instead of all being hashed as the same float.
// This is what a plain UNION or SELECT DISTINCT over a column that vtgate can't type relies on.
func TestDistinctUnknownTypeValues(t *testing.T) {
	testcases := []struct {
		typ    string
		values []string
		expect []string
	}{
		{"enum", []string{"X|WX", "Y|WY", "X|WX"}, []string{"X", "Y"}},
		{"set", []string{"X|WX", "Y|WY", "X|WX"}, []string{"X", "Y"}},
		{"json", []string{`{"a":1}|J1`, `{"a":2}|J2`, `{"a":1}|J1`}, []string{`{"a":1}`, `{"a":2}`}},
		{"bit", []string{"X|WX", "Y|WY", "X|WX"}, []string{"X", "Y"}},
		{"geometry", []string{"X|WX", "Y|WY", "X|WX"}, []string{"X", "Y"}},
		{"vector", []string{"X|WX", "Y|WY", "X|WX"}, []string{"X", "Y"}},
		// decimals above 2^53 would be rounded together as floats
		{
			"decimal",
			[]string{"9007199254740992.00|", "9007199254740993.00|", "9007199254740992.00|"},
			[]string{"9007199254740992.00", "9007199254740993.00"},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.typ, func(t *testing.T) {
			distinct := &Distinct{
				Source:    &fakePrimitive{results: []*sqltypes.Result{r("n|ws", tc.typ+"|varbinary", tc.values...)}},
				CheckCols: []CheckCol{{Col: 0, WsCol: new(1), Type: evalengine.NewUnknownType()}},
				Truncate:  1,
			}

			qr, err := distinct.TryExecute(t.Context(), &noopVCursor{}, nil, true)
			require.NoError(t, err)
			var got []string
			for _, row := range qr.Rows {
				got = append(got, row[0].ToString())
			}
			require.Equal(t, tc.expect, got)
		})
	}
}

// TestDistinctOnlyFallsBackOnUnsupportedCollation checks that a hash error that the weight
// string can't explain is returned, instead of switching columns to the weight string midway.
func TestDistinctOnlyFallsBackOnUnsupportedCollation(t *testing.T) {
	input := r("n|ws", "varchar|varbinary", "1|1", "1.5|1.5")

	distinct := &Distinct{
		Source:    &fakePrimitive{results: []*sqltypes.Result{input}},
		CheckCols: []CheckCol{{Col: 0, WsCol: new(1), Type: evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID)}},
		Truncate:  1,
	}

	_, err := distinct.TryExecute(t.Context(), &noopVCursor{}, nil, true)
	require.ErrorIs(t, err, evalengine.ErrHashCoercionIsNotExact)
}

// TestDistinctUnknownTypeWithCollation checks that text of an unknown type, with a collation but
// without a weight string, is compared in that collation instead of being hashed as a number.
func TestDistinctUnknownTypeWithCollation(t *testing.T) {
	input := r("name", "varchar", "alice", "bob", "ALICE")

	distinct := &Distinct{
		Source:    &fakePrimitive{results: []*sqltypes.Result{input}},
		CheckCols: []CheckCol{{Col: 0, Type: evalengine.NewType(sqltypes.Unknown, collations.CollationUtf8mb4ID)}},
	}

	qr, err := distinct.TryExecute(t.Context(), &noopVCursor{}, nil, true)
	require.NoError(t, err)
	utils.MustMatch(t, fmt.Sprintf("%v", r("name", "varchar", "alice", "bob").Rows), fmt.Sprintf("%v", qr.Rows))
}
