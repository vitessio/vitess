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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDrainAndAggregateErrors(t *testing.T) {
	tests := []struct {
		name       string
		results    []*vcopierCopyTaskResult
		vstreamErr error
		wantNil    bool
		wantSubstr []string
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan *vcopierCopyTaskResult, len(tt.results)+1)
			for _, r := range tt.results {
				ch <- r
			}

			err := drainAndAggregateErrors(ch, tt.vstreamErr)

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
		})
	}
}
