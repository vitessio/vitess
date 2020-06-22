package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
)

func TestConcatenate_Execute(t *testing.T) {
	type testCase struct {
		testName       string
		inputs         []*sqltypes.Result
		expectedResult *sqltypes.Result
		expectedError  string
	}

	testCases := []*testCase{
		{
			testName: "2 empty result",
			inputs: []*sqltypes.Result{
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("", ""), ""),
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("", ""), ""),
			},
			expectedResult: nil,
			expectedError:  "",
		},
		{
			testName: "3 empty result",
			inputs: []*sqltypes.Result{
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("", ""), ""),
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("", ""), ""),
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("", ""), ""),
			},
			expectedResult: nil,
			expectedError:  "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			var fps []Primitive
			for _, input := range tc.inputs {
				fps = append(fps, &fakePrimitive{results: []*sqltypes.Result{input}})
			}
			concatenate := Concatenate{Sources: fps}
			qr, err := concatenate.Execute(&noopVCursor{}, nil, true)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedResult, qr)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}

}
