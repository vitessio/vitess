/*
Copyright 2026 The Vitess Authors.

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

package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestShouldRetryTabletError(t *testing.T) {
	tcases := []struct {
		name           string
		code           vtrpcpb.Code
		msg            string
		expectedAction TabletErrorAction
	}{
		{
			name:           "failed precondition",
			code:           vtrpcpb.Code_FAILED_PRECONDITION,
			msg:            "",
			expectedAction: TabletErrorActionRetry,
		},
		{
			name:           "gtid mismatch",
			code:           vtrpcpb.Code_INVALID_ARGUMENT,
			msg:            "GTIDSet Mismatch aa",
			expectedAction: TabletErrorActionIgnoreTablet,
		},
		{
			name:           "unavailable",
			code:           vtrpcpb.Code_UNAVAILABLE,
			msg:            "",
			expectedAction: TabletErrorActionRetry,
		},
		{
			name:           "invalid argument",
			code:           vtrpcpb.Code_INVALID_ARGUMENT,
			msg:            "final error",
			expectedAction: TabletErrorActionFail,
		},
		{
			name:           "internal error",
			code:           vtrpcpb.Code_INTERNAL,
			msg:            "missing journaling participants",
			expectedAction: TabletErrorActionFail,
		},
		{
			name:           "query interrupted",
			code:           vtrpcpb.Code_UNKNOWN,
			msg:            "vttablet: rpc error: code = Unknown desc = Query execution was interrupted, maximum statement execution time exceeded (errno 3024) (sqlstate HY000)",
			expectedAction: TabletErrorActionRetry,
		},
		{
			name:           "binary log purged",
			code:           vtrpcpb.Code_UNKNOWN,
			msg:            "vttablet: rpc error: code = Unknown desc = stream (at source tablet) error @ (including the GTID we failed to process) 013c5ddc-dd89-11ed-b3a1-125a006436b9:1-305627274,fe50e15a-0213-11ee-bfbe-0a048e8090b5:1-340389717: Cannot replicate because the source purged required binary logs. Replicate the missing transactions from elsewhere, or provision a new replica from backup. Consider increasing the source's binary log expiration period. The GTID sets and the missing purged transactions are too long to print in this message. For more information, please see the source's error log or the manual for GTID_SUBTRACT (errno 1236) (sqlstate HY000)",
			expectedAction: TabletErrorActionIgnoreTablet,
		},
		{
			name:           "source purged required gtids",
			code:           vtrpcpb.Code_UNKNOWN,
			msg:            "vttablet: rpc error: code = Unknown desc = Cannot replicate because the source purged required binary logs. Replicate the missing transactions from elsewhere, or provision a new replica from backup. Consider increasing the source's binary log expiration period. Missing transactions are: 013c5ddc-dd89-11ed-b3a1-125a006436b9:305627275-305627280 (errno 1789) (sqlstate HY000)",
			expectedAction: TabletErrorActionIgnoreTablet,
		},
		{
			name:           "non-ephemeral sql error",
			code:           vtrpcpb.Code_UNKNOWN,
			msg:            "vttablet: rpc error: code = Unknown desc = Duplicate entry '1' for key 'PRIMARY' (errno 1062) (sqlstate 23000)",
			expectedAction: TabletErrorActionFail,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			err := vterrors.New(tc.code, tc.msg)
			action := ShouldRetryTabletError(err)
			assert.Equal(t, tc.expectedAction, action)
		})
	}
}
