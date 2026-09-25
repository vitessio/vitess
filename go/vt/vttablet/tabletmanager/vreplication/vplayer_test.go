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

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/vterrors"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

// multiStatementDBClient is a DBClient that records the multi statement states
// it was asked for.
type multiStatementDBClient struct {
	binlogplayer.DBClient
	calls  []bool
	closed bool
	err    error
}

func (dc *multiStatementDBClient) SetMultiStatements(on bool) error {
	dc.calls = append(dc.calls, on)
	return dc.err
}

func (dc *multiStatementDBClient) IsClosed() bool {
	return dc.closed
}

// TestVPlayerMultiStatements checks that the vplayer only leaves multi
// statement support on for the connection it batches transactions on.
func TestVPlayerMultiStatements(t *testing.T) {
	newVR := func(t *testing.T, dbClient binlogplayer.DBClient, batching bool) *vreplicator {
		t.Helper()

		config := *vttablet.DefaultVReplicationConfig
		if batching {
			config.ExperimentalFlags |= vttablet.VReplicationExperimentalFlagVPlayerBatching
		} else {
			config.ExperimentalFlags &^= vttablet.VReplicationExperimentalFlagVPlayerBatching
		}
		stats := binlogplayer.NewStats()
		return &vreplicator{
			source:         &binlogdatapb.BinlogSource{},
			dbClient:       newVDBClient(dbClient, stats, config.RelayLogMaxItems),
			stats:          stats,
			workflowConfig: &config,
		}
	}
	// The copy phase is not batched, whether or not the flag is set.
	copyStateInProgress := map[string]*sqltypes.Result{"t1": nil}
	maxAllowedPacket := sqltypes.MakeTestResult(sqltypes.MakeTestFields("max_allowed_packet", "int64"), "1024")

	t.Run("batching disabled", func(t *testing.T) {
		dbClient := &multiStatementDBClient{DBClient: binlogplayer.NewMockDBClient(t)}
		vr := newVR(t, dbClient, false)
		vp := newVPlayer(vr, binlogplayer.VRSettings{}, nil, replication.Position{}, "replicate")
		require.False(t, vp.batchMode)
		require.NoError(t, vp.setConnectionBatchMode())
		require.Equal(t, []bool{false}, dbClient.calls)
		require.Zero(t, vr.dbClient.maxBatchSize)
	})

	t.Run("batching enabled while replicating", func(t *testing.T) {
		mock := binlogplayer.NewMockDBClient(t)
		mock.ExpectRequest(SqlMaxAllowedPacket, maxAllowedPacket, nil)
		dbClient := &multiStatementDBClient{DBClient: mock}
		vr := newVR(t, dbClient, true)
		vp := newVPlayer(vr, binlogplayer.VRSettings{}, nil, replication.Position{}, "replicate")
		require.True(t, vp.batchMode)
		// Constructing the player must not be what lets the client batch.
		require.Zero(t, vr.dbClient.maxBatchSize)
		require.NoError(t, vp.setConnectionBatchMode())
		require.Equal(t, []bool{true}, dbClient.calls)
		require.NotZero(t, vr.dbClient.maxBatchSize)
	})

	t.Run("batching enabled while copying", func(t *testing.T) {
		dbClient := &multiStatementDBClient{DBClient: binlogplayer.NewMockDBClient(t)}
		vr := newVR(t, dbClient, true)
		vp := newVPlayer(vr, binlogplayer.VRSettings{}, copyStateInProgress, replication.Position{}, "catchup")
		require.False(t, vp.batchMode)
		require.NoError(t, vp.setConnectionBatchMode())
		require.Equal(t, []bool{false}, dbClient.calls)
		require.Zero(t, vr.dbClient.maxBatchSize)
	})

	// A player that does not batch has to take the ability to batch away from
	// the client it inherits, or it would send a batch on a connection it just
	// turned multi statement support off for.
	t.Run("a non batching player follows a batching one", func(t *testing.T) {
		mock := binlogplayer.NewMockDBClient(t)
		mock.ExpectRequest(SqlMaxAllowedPacket, maxAllowedPacket, nil)
		dbClient := &multiStatementDBClient{DBClient: mock}
		vr := newVR(t, dbClient, true)

		batching := newVPlayer(vr, binlogplayer.VRSettings{}, nil, replication.Position{}, "replicate")
		require.NoError(t, batching.setConnectionBatchMode())
		require.NotZero(t, vr.dbClient.maxBatchSize)

		copying := newVPlayer(vr, binlogplayer.VRSettings{}, copyStateInProgress, replication.Position{}, "catchup")
		require.False(t, copying.batchMode)
		require.NoError(t, copying.setConnectionBatchMode())
		require.Equal(t, []bool{true, false}, dbClient.calls)
		require.Zero(t, vr.dbClient.maxBatchSize)
	})

	t.Run("the connection refuses", func(t *testing.T) {
		mock := binlogplayer.NewMockDBClient(t)
		mock.ExpectRequest(SqlMaxAllowedPacket, maxAllowedPacket, nil)
		dbClient := &multiStatementDBClient{DBClient: mock, err: errors.New("connection is gone")}
		vr := newVR(t, dbClient, true)
		vp := newVPlayer(vr, binlogplayer.VRSettings{}, nil, replication.Position{}, "replicate")
		err := vp.setConnectionBatchMode()
		require.ErrorContains(t, err, "connection is gone")
		// A refusal is a property of the server, not a transient failure, so
		// the workflow has to stop rather than retry until it gives up.
		require.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.True(t, isUnrecoverableError(err))
		// A connection that refused must not be batched onto.
		require.Zero(t, vr.dbClient.maxBatchSize)
	})

	t.Run("the connection is lost", func(t *testing.T) {
		mock := binlogplayer.NewMockDBClient(t)
		mock.ExpectRequest(SqlMaxAllowedPacket, maxAllowedPacket, nil)
		lost := sqlerror.NewSQLErrorf(sqlerror.CRServerLost, sqlerror.SSUnknownSQLState, "%v", "EOF")
		dbClient := &multiStatementDBClient{DBClient: mock, err: lost}
		vr := newVR(t, dbClient, true)
		vp := newVPlayer(vr, binlogplayer.VRSettings{}, nil, replication.Position{}, "replicate")
		err := vp.setConnectionBatchMode()
		require.ErrorContains(t, err, "EOF")
		// A connection that died during the exchange is re-created and the
		// workflow replays, so this must not end it.
		require.False(t, isUnrecoverableError(err))
		// The connection is gone either way, so it must not be batched onto.
		require.Zero(t, vr.dbClient.maxBatchSize)
	})

	// An answer that leaves the state of the connection unknown closes it with
	// an error that is not a connection error. The client is re-created on the
	// next run all the same, so this must not end the workflow either.
	t.Run("the connection is closed by an unexpected answer", func(t *testing.T) {
		mock := binlogplayer.NewMockDBClient(t)
		mock.ExpectRequest(SqlMaxAllowedPacket, maxAllowedPacket, nil)
		unexpected := vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected packet for COM_SET_OPTION: [254]")
		dbClient := &multiStatementDBClient{DBClient: mock, err: unexpected, closed: true}
		vr := newVR(t, dbClient, true)
		vp := newVPlayer(vr, binlogplayer.VRSettings{}, nil, replication.Position{}, "replicate")
		err := vp.setConnectionBatchMode()
		require.ErrorContains(t, err, "unexpected packet for COM_SET_OPTION")
		require.False(t, isUnrecoverableError(err))
		require.Zero(t, vr.dbClient.maxBatchSize)
	})

	// A player that batched gives the capability back when it is done, so that
	// a connection nobody is batching on does not keep carrying it.
	t.Run("a batching player clears the connection on the way out", func(t *testing.T) {
		mock := binlogplayer.NewMockDBClient(t)
		mock.ExpectRequest(SqlMaxAllowedPacket, maxAllowedPacket, nil)
		dbClient := &multiStatementDBClient{DBClient: mock}
		vr := newVR(t, dbClient, true)
		vp := newVPlayer(vr, binlogplayer.VRSettings{}, nil, replication.Position{}, "replicate")
		require.NoError(t, vp.setConnectionBatchMode())
		require.NotZero(t, vr.dbClient.maxBatchSize)

		vp.clearConnectionBatchMode()
		require.Equal(t, []bool{true, false}, dbClient.calls)
		// The client has to lose the ability to build a batch along with the
		// connection's ability to run one.
		require.Zero(t, vr.dbClient.maxBatchSize)
	})

	// Giving the capability back is the last thing a player does, so a failure
	// is not something it can act on. What it must not do is leave the client
	// believing it can still batch.
	t.Run("clearing the connection fails", func(t *testing.T) {
		mock := binlogplayer.NewMockDBClient(t)
		mock.ExpectRequest(SqlMaxAllowedPacket, maxAllowedPacket, nil)
		dbClient := &multiStatementDBClient{DBClient: mock}
		vr := newVR(t, dbClient, true)
		vp := newVPlayer(vr, binlogplayer.VRSettings{}, nil, replication.Position{}, "replicate")
		require.NoError(t, vp.setConnectionBatchMode())

		dbClient.err = errors.New("connection is gone")
		vp.clearConnectionBatchMode()
		require.Equal(t, []bool{true, false}, dbClient.calls)
		require.Zero(t, vr.dbClient.maxBatchSize)
	})

	// Losing the connection is the usual reason a player stops, and a connection
	// that is gone has nothing left to give back.
	t.Run("a closed connection is not asked to give it back", func(t *testing.T) {
		mock := binlogplayer.NewMockDBClient(t)
		mock.ExpectRequest(SqlMaxAllowedPacket, maxAllowedPacket, nil)
		dbClient := &multiStatementDBClient{DBClient: mock}
		vr := newVR(t, dbClient, true)
		vp := newVPlayer(vr, binlogplayer.VRSettings{}, nil, replication.Position{}, "replicate")
		require.NoError(t, vp.setConnectionBatchMode())
		require.Equal(t, []bool{true}, dbClient.calls)

		dbClient.closed = true
		vp.clearConnectionBatchMode()
		require.Equal(t, []bool{true}, dbClient.calls, "a closed connection must not be written to")
		require.Zero(t, vr.dbClient.maxBatchSize)
	})
}
