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

package vreplication

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestDrainAndAggregateErrors(t *testing.T) {
	tests := []struct {
		name          string
		results       []*vcopierCopyTaskResult
		vstreamErr    error
		wantNil       bool
		wantSubstr    []string
		wantNotSubstr []string
	}{
		{
			name:    "no results and no vstream error",
			wantNil: true,
		},
		{
			name: "all completed results",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskComplete},
				{state: vcopierCopyTaskComplete},
			},
			wantNil: true,
		},
		{
			name: "all canceled results",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskCancel},
				{state: vcopierCopyTaskCancel},
			},
			wantNil: true,
		},
		{
			name: "nil result in channel",
			results: []*vcopierCopyTaskResult{
				nil,
			},
			wantNil: true,
		},
		{
			name: "single task failure",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskFail, err: errors.New("insert failed")},
			},
			wantSubstr: []string{"task error", "insert failed"},
		},
		{
			name: "multiple task failures",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskFail, err: errors.New("insert failed on row 1")},
				{state: vcopierCopyTaskFail, err: errors.New("insert failed on row 2")},
			},
			wantSubstr: []string{"task error", "insert failed on row 1", "insert failed on row 2"},
		},
		{
			name:       "vstream error only",
			vstreamErr: errors.New("vstream connection lost"),
			wantSubstr: []string{"task error", "vstream connection lost"},
		},
		{
			name: "single task failure and vstream error",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskFail, err: errors.New("insert failed")},
			},
			vstreamErr: errors.New("vstream connection lost"),
			wantSubstr: []string{"task error", "insert failed", "vstream connection lost"},
		},
		{
			name: "multiple task failures and vstream error",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskFail, err: errors.New("insert failed on row 1")},
				{state: vcopierCopyTaskFail, err: errors.New("insert failed on row 2")},
			},
			vstreamErr: errors.New("vstream connection lost"),
			wantSubstr: []string{"task error", "insert failed on row 1", "insert failed on row 2", "vstream connection lost"},
		},
		{
			name: "mix of complete cancel and fail",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskComplete},
				{state: vcopierCopyTaskCancel},
				{state: vcopierCopyTaskFail, err: errors.New("the only error")},
				{state: vcopierCopyTaskComplete},
			},
			wantSubstr: []string{"task error", "the only error"},
		},
		{
			// Cancel-state results were silently dropped in main; their
			// result.err must now appear in the aggregated message.
			name: "cancel state with real root cause is surfaced",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskCancel, err: errors.New("upstream rpc canceled: connection reset")},
			},
			wantSubstr: []string{"task error", "upstream rpc canceled", "connection reset"},
		},
		{
			// The dependentBatchFailure sentinel must be partitioned out as
			// a count, not surfaced as a real error.
			name: "context-expired cancels collapse to a count",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskCancel, err: &dependentBatchFailure{msg: "context has expired"}},
				{state: vcopierCopyTaskCancel, err: &dependentBatchFailure{msg: "context has expired"}},
			},
			wantSubstr: []string{"task error", "2 batches failed waiting on a concurrent insert worker's batch"},
		},
		{
			// A real Fail mixed with dependent-batch echoes: the real error
			// must dominate the message, echoes collapse to the count.
			name: "fail with dependent-batch echoes surfaces the root cause",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskFail, err: errors.New("failed inserting rows: EOF")},
				{state: vcopierCopyTaskFail, err: &dependentBatchFailure{msg: "received result is not complete"}},
				{state: vcopierCopyTaskFail, err: &dependentBatchFailure{msg: "received result is not complete"}},
				{state: vcopierCopyTaskFail, err: &dependentBatchFailure{msg: "received result is not complete"}},
			},
			wantSubstr: []string{
				"task error",
				"failed inserting rows: EOF",
				"+3 batches failed waiting on this to complete",
			},
			wantNotSubstr: []string{"received result is not complete"},
		},
	}

	// Under a canceled ctx, dependent-batch and ctx-derived errors must be
	// stripped so copyAll's "interrupted due to context expiration" path
	// fires instead of a generic workflow error. Real Fail-state errors
	// that raced the cancellation still surface.
	cancelTests := []struct {
		name    string
		results []*vcopierCopyTaskResult
		wantNil bool
		wantErr string
	}{
		{
			name: "pure dependent cancels returns nil",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskCancel, err: &dependentBatchFailure{msg: "context has expired", code: vtrpcpb.Code_CANCELED}},
				{state: vcopierCopyTaskCancel, err: &dependentBatchFailure{msg: "context has expired", code: vtrpcpb.Code_CANCELED}},
			},
			wantNil: true,
		},
		{
			name: "real Fail mixed with dep cancels still surfaces real error",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskCancel, err: &dependentBatchFailure{msg: "context has expired", code: vtrpcpb.Code_CANCELED}},
				{state: vcopierCopyTaskFail, err: errors.New("failed inserting rows: EOF")},
			},
			wantErr: "failed inserting rows: EOF",
		},
		{
			name: "raw context.Canceled and vterrors-wrapped variants return nil",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskCancel, err: context.Canceled},
				{state: vcopierCopyTaskCancel, err: vterrors.Wrap(context.Canceled, "ExecuteWithRetry")},
			},
			wantNil: true,
		},
		{
			name: "stdlib fmt.Errorf %w-wrapped context errors return nil",
			results: []*vcopierCopyTaskResult{
				{state: vcopierCopyTaskCancel, err: fmt.Errorf("row stream ended: %w", context.Canceled)},
				{state: vcopierCopyTaskCancel, err: fmt.Errorf("vstream deadline: %w", context.DeadlineExceeded)},
			},
			wantNil: true,
		},
	}
	for _, tt := range cancelTests {
		t.Run("ctx canceled / "+tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			ch := make(chan *vcopierCopyTaskResult, len(tt.results))
			for _, r := range tt.results {
				ch <- r
			}
			err := drainAndAggregateErrors(ctx, ch, nil, nil)
			if tt.wantNil {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}

	t.Run("preTerrs is included in aggregation", func(t *testing.T) {
		// VStreamTables callback stashes result.err in preTerrs before
		// returning io.EOF. The drain must surface those stashed errors
		// even when the channel has no further Fail/Cancel results.
		ch := make(chan *vcopierCopyTaskResult, 1)
		preTerrs := []error{errors.New("failed inserting rows: EOF")}

		err := drainAndAggregateErrors(t.Context(), ch, nil, preTerrs)
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed inserting rows: EOF")
	})

	t.Run("preTerrs combined with channel results and a vstream error", func(t *testing.T) {
		ch := make(chan *vcopierCopyTaskResult, 2)
		ch <- &vcopierCopyTaskResult{state: vcopierCopyTaskFail, err: &dependentBatchFailure{msg: "received result is not complete"}}
		preTerrs := []error{errors.New("failed inserting rows: EOF")}

		err := drainAndAggregateErrors(t.Context(), ch, errors.New("vstream connection lost"), preTerrs)
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed inserting rows: EOF")
		assert.ErrorContains(t, err, "vstream connection lost")
		assert.ErrorContains(t, err, "+1 batches failed waiting on this to complete")
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan *vcopierCopyTaskResult, len(tt.results)+1)
			for _, r := range tt.results {
				ch <- r
			}

			err := drainAndAggregateErrors(t.Context(), ch, tt.vstreamErr, nil)

			// Verify the channel was fully drained.
			assert.Equal(t, 0, len(ch), "channel should be empty after draining")

			if tt.wantNil {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, substr := range tt.wantSubstr {
				assert.Contains(t, err.Error(), substr)
			}
			for _, substr := range tt.wantNotSubstr {
				assert.NotContains(t, err.Error(), substr)
			}
		})
	}
}
