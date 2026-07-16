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

	"vitess.io/vitess/go/sqltypes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestRunWithRecover(t *testing.T) {
	t.Run("panic is converted into an error", func(t *testing.T) {
		err := runWithRecover("wf1", "applyEvents", func() error {
			panic("boom")
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "panic in applyEvents")
		assert.ErrorContains(t, err, "boom")
	})

	t.Run("runtime panic (slice bounds out of range) is converted into an error", func(t *testing.T) {
		// Mirrors the #20360 shape: a runtime panic from indexing a too-short
		// slice inside the wrapped function should surface as a clean error,
		// not crash the test process.
		err := runWithRecover("wf2", "vstream", func() error {
			s := []int{}
			_ = s[0]
			return nil
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "panic in vstream")
		assert.ErrorContains(t, err, "index out of range")
	})

	t.Run("error returned by fn is passed through unchanged", func(t *testing.T) {
		sentinel := errors.New("real error")
		err := runWithRecover("wf3", "applyEvents", func() error {
			return sentinel
		})
		assert.ErrorIs(t, err, sentinel)
	})

	t.Run("nil return is preserved", func(t *testing.T) {
		err := runWithRecover("wf4", "applyEvents", func() error {
			return nil
		})
		assert.NoError(t, err)
	})
}

func TestBulkApplicableShapes(t *testing.T) {
	row := func(id int64) *querypb.Row {
		return sqltypes.RowToProto3([]sqltypes.Value{sqltypes.NewInt64(id)})
	}
	insert := func(id int64) *binlogdatapb.RowChange {
		return &binlogdatapb.RowChange{After: row(id)}
	}
	del := func(id int64) *binlogdatapb.RowChange {
		return &binlogdatapb.RowChange{Before: row(id)}
	}
	update := func(id int64) *binlogdatapb.RowChange {
		return &binlogdatapb.RowChange{Before: row(id), After: row(id)}
	}

	testcases := []struct {
		name            string
		rowChanges      []*binlogdatapb.RowChange
		wantDeletesOnly bool
		wantInsertsOnly bool
		wantErr         string
	}{{
		name:            "all inserts",
		rowChanges:      []*binlogdatapb.RowChange{insert(1), insert(2)},
		wantInsertsOnly: true,
	}, {
		name:            "all deletes",
		rowChanges:      []*binlogdatapb.RowChange{del(1), del(2)},
		wantDeletesOnly: true,
	}, {
		name:       "insert then delete",
		rowChanges: []*binlogdatapb.RowChange{insert(1), del(2)},
	}, {
		name:       "delete then insert",
		rowChanges: []*binlogdatapb.RowChange{del(1), insert(2)},
	}, {
		name:       "insert then update",
		rowChanges: []*binlogdatapb.RowChange{insert(1), update(2)},
	}, {
		name:       "update then delete",
		rowChanges: []*binlogdatapb.RowChange{update(1), del(2)},
	}, {
		// A change with no images (nil or empty) is malformed: neither apply
		// path can handle it, so it must be rejected before routing.
		name:       "empty change",
		rowChanges: []*binlogdatapb.RowChange{{}, insert(1)},
		wantErr:    "malformed row change",
	}, {
		name:       "nil change",
		rowChanges: []*binlogdatapb.RowChange{insert(1), nil},
		wantErr:    "malformed row change",
	}, {
		// Malformed changes must be detected even after the scan has already
		// concluded the event is not bulk-applicable: an early exit would
		// pass the unvalidated entry to the per-change path.
		name:       "nil change after update",
		rowChanges: []*binlogdatapb.RowChange{update(1), nil},
		wantErr:    "malformed row change",
	}, {
		name:       "nil change after mixed shapes",
		rowChanges: []*binlogdatapb.RowChange{insert(1), del(2), nil},
		wantErr:    "malformed row change",
	}, {
		// An image that is present but has no column values is the malformed
		// shape from https://github.com/vitessio/vitess/issues/20360 and must
		// be rejected rather than classified by the nil checks: MakeRowTrusted
		// returns an empty row that later indexing panics on.
		name:       "empty Before image",
		rowChanges: []*binlogdatapb.RowChange{del(1), {Before: &querypb.Row{}}},
		wantErr:    "malformed row change",
	}, {
		name:       "empty After image",
		rowChanges: []*binlogdatapb.RowChange{insert(1), {After: &querypb.Row{}}},
		wantErr:    "malformed row change",
	}, {
		name:       "update with empty After image",
		rowChanges: []*binlogdatapb.RowChange{{Before: row(1), After: &querypb.Row{}}},
		wantErr:    "malformed row change",
	}}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			deletesOnly, insertsOnly, err := bulkApplicableShapes("t1", tc.rowChanges)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				// A malformed event replays identically on every retry, so
				// the workflow must transition to the Error state instead of
				// retrying forever.
				assert.True(t, isUnrecoverableError(err), "error must be terminal")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantDeletesOnly, deletesOnly, "deletesOnly")
			assert.Equal(t, tc.wantInsertsOnly, insertsOnly, "insertsOnly")
		})
	}
}
