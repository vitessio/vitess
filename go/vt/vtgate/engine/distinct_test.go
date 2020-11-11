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
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
)

func TestDistinct(t *testing.T) {
	type testCase struct {
		testName       string
		inputs         *sqltypes.Result
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
		expectedResult: r("myid", "int64", "11|m1|n1", "22|m2|n2", "1|a1|b1", "2|a2|b2"),
	}, {
		testName:      "varchar columns",
		inputs:        r("myid", "varchar", "monkey", "horse"),
		expectedError: "types does not support hashcode yet",
	}}

	for _, tc := range testCases {
		t.Run(tc.testName+"-Execute", func(t *testing.T) {

			distinct := &Distinct{Source: &fakePrimitive{results: []*sqltypes.Result{tc.inputs}}}

			qr, err := distinct.Execute(&noopVCursor{ctx: context.Background()}, nil, true)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedResult, qr)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			}
		})
	}
}
