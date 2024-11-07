/*
Copyright 2024 The Vitess Authors.

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

package vreplication

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestIsUnrecoverableError tests the different error cases for isUnrecoverableError().
func TestIsUnrecoverableError(t *testing.T) {
	if runNoBlobTest {
		t.Skip()
	}

	type testCase struct {
		name     string
		err      error
		expected bool
	}

	testCases := []testCase{
		{
			name:     "Nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "vterrors.Code_FAILED_PRECONDITION",
			err:      vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "test error"),
			expected: true,
		},
		{
			name:     "vterrors.Code_FAILED_PRECONDITION, WrongTablet",
			err:      vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s: %v, want: %v or %v", vterrors.WrongTablet, "PRIMARY", "REPLICA", nil),
			expected: false,
		},
		{
			name:     "Non-SQL error",
			err:      errors.New("non-SQL error"),
			expected: false,
		},
		{
			name:     "SQL error with ERUnknownError",
			err:      sqlerror.NewSQLError(sqlerror.ERUnknownError, "test SQL error", "test"),
			expected: false,
		},
		{
			name:     "SQL error with ERAccessDeniedError",
			err:      sqlerror.NewSQLError(sqlerror.ERAccessDeniedError, "access denied", "test"),
			expected: true,
		},
		{
			name:     "SQL error with ERDataOutOfRange",
			err:      sqlerror.NewSQLError(sqlerror.ERDataOutOfRange, "data out of range", "test"),
			expected: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isUnrecoverableError(tc.err)
			require.Equal(t, tc.expected, result)
		})
	}
}
