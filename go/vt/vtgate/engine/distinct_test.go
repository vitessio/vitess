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
		t.Run(tc.testName+"-Execute", func(t *testing.T) {
			distinct := &Distinct{
				Source:        &fakePrimitive{results: []*sqltypes.Result{tc.inputs}},
				ColCollations: tc.collations,
			}

			qr, err := distinct.TryExecute(&noopVCursor{ctx: context.Background()}, nil, true)
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
				Source:        &fakePrimitive{results: []*sqltypes.Result{tc.inputs}},
				ColCollations: tc.collations,
			}

			result, err := wrapStreamExecute(distinct, &noopVCursor{ctx: context.Background()}, nil, true)

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
