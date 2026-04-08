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
	"io"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

// testCtx returns the test context, which is cancelled when the test ends.
// This is essential for tests that create an applyScheduler, because
// newApplyScheduler spawns a goroutine that blocks on ctx.Done().
func testCtx(t *testing.T) context.Context {
	t.Helper()
	return t.Context()
}

// ---------- computeLastEventTimestamp tests ----------

func TestComputeLastEventTimestamp_EmptyEvents(t *testing.T) {
	ts, ct := computeLastEventTimestamp(nil)
	assert.Equal(t, int64(0), ts)
	assert.Equal(t, int64(0), ct)

	ts, ct = computeLastEventTimestamp([]*binlogdatapb.VEvent{})
	assert.Equal(t, int64(0), ts)
	assert.Equal(t, int64(0), ct)
}

func TestComputeLastEventTimestamp_LastEventHasTimestamp(t *testing.T) {
	events := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_ROW, Timestamp: 100, CurrentTime: 200},
		{Type: binlogdatapb.VEventType_ROW, Timestamp: 300, CurrentTime: 400},
	}
	ts, ct := computeLastEventTimestamp(events)
	assert.Equal(t, int64(300), ts)
	assert.Equal(t, int64(400), ct)
}

func TestComputeLastEventTimestamp_SkipsZeroTimestamp(t *testing.T) {
	events := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_ROW, Timestamp: 100, CurrentTime: 200},
		{Type: binlogdatapb.VEventType_COMMIT, Timestamp: 0, CurrentTime: 0},
	}
	ts, ct := computeLastEventTimestamp(events)
	assert.Equal(t, int64(100), ts)
	assert.Equal(t, int64(200), ct)
}

func TestComputeLastEventTimestamp_SkipsThrottledHeartbeat(t *testing.T) {
	events := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_ROW, Timestamp: 100, CurrentTime: 200},
		{Type: binlogdatapb.VEventType_HEARTBEAT, Timestamp: 500, CurrentTime: 600, Throttled: true},
	}
	ts, ct := computeLastEventTimestamp(events)
	assert.Equal(t, int64(100), ts)
	assert.Equal(t, int64(200), ct)
}

func TestComputeLastEventTimestamp_NonThrottledHeartbeatCounts(t *testing.T) {
	events := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_ROW, Timestamp: 100, CurrentTime: 200},
		{Type: binlogdatapb.VEventType_HEARTBEAT, Timestamp: 500, CurrentTime: 600, Throttled: false},
	}
	ts, ct := computeLastEventTimestamp(events)
	assert.Equal(t, int64(500), ts)
	assert.Equal(t, int64(600), ct)
}

func TestComputeLastEventTimestamp_AllZeroTimestamp(t *testing.T) {
	events := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_COMMIT, Timestamp: 0},
		{Type: binlogdatapb.VEventType_BEGIN, Timestamp: 0},
	}
	ts, ct := computeLastEventTimestamp(events)
	assert.Equal(t, int64(0), ts)
	assert.Equal(t, int64(0), ct)
}

// ---------- sync.Pool helpers tests ----------

func TestAcquireReleaseApplyTxn(t *testing.T) {
	txn := acquireApplyTxn()
	require.NotNil(t, txn)

	// Set some fields
	txn.order = 42
	txn.sequenceNumber = 10
	txn.forceGlobal = true

	// Release should zero out the struct
	releaseApplyTxn(txn)

	// Acquire again — may get the same or a new object, but it should be zeroed
	txn2 := acquireApplyTxn()
	require.NotNil(t, txn2)
	assert.Equal(t, int64(0), txn2.order)
	assert.Equal(t, int64(0), txn2.sequenceNumber)
	assert.False(t, txn2.forceGlobal)
	releaseApplyTxn(txn2)
}

func TestAcquireReleaseApplyTxnPayload(t *testing.T) {
	p := acquireApplyTxnPayload()
	require.NotNil(t, p)

	p.timestamp = 999
	p.mustSave = true

	// Attach to a txn and release
	txn := acquireApplyTxn()
	txn.payload = p
	releaseApplyTxn(txn)

	p2 := acquireApplyTxnPayload()
	require.NotNil(t, p2)
	assert.Equal(t, int64(0), p2.timestamp)
	assert.False(t, p2.mustSave)
	applyTxnPayloadPool.Put(p2)
}

func TestReleaseApplyTxnNilPayload(t *testing.T) {
	txn := acquireApplyTxn()
	txn.order = 5
	txn.payload = nil
	// Should not panic
	releaseApplyTxn(txn)
}

// ---------- scheduler gaps: advanceCommittedSequence, waitForIdle, close ----------

func TestApplySchedulerAdvanceCommittedSequence(t *testing.T) {
	ctx := testCtx(t)
	s := newApplyScheduler(ctx)

	// Initially zero
	assert.Equal(t, int64(0), s.lastCommittedSequence)

	// Advance to 5
	s.advanceCommittedSequence(5)
	s.mu.Lock()
	assert.Equal(t, int64(5), s.lastCommittedSequence)
	s.mu.Unlock()

	// Advance to 10
	s.advanceCommittedSequence(10)
	s.mu.Lock()
	assert.Equal(t, int64(10), s.lastCommittedSequence)
	s.mu.Unlock()

	// Lower value does not regress
	s.advanceCommittedSequence(3)
	s.mu.Lock()
	assert.Equal(t, int64(10), s.lastCommittedSequence)
	s.mu.Unlock()

	// Zero is a no-op
	s.advanceCommittedSequence(0)
	s.mu.Lock()
	assert.Equal(t, int64(10), s.lastCommittedSequence)
	s.mu.Unlock()

	// Negative is a no-op
	s.advanceCommittedSequence(-1)
	s.mu.Lock()
	assert.Equal(t, int64(10), s.lastCommittedSequence)
	s.mu.Unlock()
}

func TestApplySchedulerAdvanceUnblocksMeta(t *testing.T) {
	ctx := testCtx(t)
	s := newApplyScheduler(ctx)

	// Enqueue a non-meta txn first AND keep it inflight so that when
	// meta2 is enqueued, the seeding condition is NOT met (inflightMissingMeta > 0).
	// This ensures lastCommittedSequence stays 0 and meta2 is blocked.
	blocker := &applyTxn{order: 1, writeset: []uint64{100}}
	require.NoError(t, s.enqueue(blocker))
	gotBlocker, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, blocker, gotBlocker)
	// blocker is still inflight (inflightMissingMeta=1)

	meta2 := &applyTxn{order: 2, sequenceNumber: 5, commitParent: 3, hasCommitMeta: true}
	require.NoError(t, s.enqueue(meta2))

	// meta2 has empty writeset, commitParent=3, lastCommittedSequence=0.
	// Also blocked by inflightMissingMeta > 0 from the blocker.
	readyCh := make(chan *applyTxn, 1)
	go func() {
		txn, err := s.nextReady(ctx)
		if err == nil {
			readyCh <- txn
		}
	}()

	assert.Never(t, func() bool {
		return len(readyCh) > 0
	}, 50*time.Millisecond, 5*time.Millisecond)

	// Commit the blocker to clear inflightMissingMeta, but
	// lastCommittedSequence is still 0 so meta2 stays blocked.
	require.NoError(t, s.markCommitted(gotBlocker))

	assert.Never(t, func() bool {
		return len(readyCh) > 0
	}, 50*time.Millisecond, 5*time.Millisecond)

	// Now advance committed sequence to 3 — should unblock meta2
	s.advanceCommittedSequence(3)

	assert.Eventually(t, func() bool {
		return len(readyCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)
}

func TestApplySchedulerWaitForIdle(t *testing.T) {
	ctx := testCtx(t)
	s := newApplyScheduler(ctx)

	// Empty scheduler: waitForIdle returns immediately
	err := s.waitForIdle(ctx)
	require.NoError(t, err)

	// Enqueue and dequeue a txn, mark committed, then waitForIdle
	txn := &applyTxn{order: 1, writeset: []uint64{100}}
	require.NoError(t, s.enqueue(txn))
	got, err := s.nextReady(ctx)
	require.NoError(t, err)

	// With inflight txn, waitForIdle should block
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- s.waitForIdle(ctx)
	}()

	assert.Never(t, func() bool {
		return len(doneCh) > 0
	}, 50*time.Millisecond, 5*time.Millisecond)

	// Mark committed → idle
	require.NoError(t, s.markCommitted(got))

	assert.Eventually(t, func() bool {
		return len(doneCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)
	require.NoError(t, <-doneCh)
}

func TestApplySchedulerWaitForIdleCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	s := newApplyScheduler(ctx)

	txn := &applyTxn{order: 1, forceGlobal: true}
	require.NoError(t, s.enqueue(txn))
	_, err := s.nextReady(ctx)
	require.NoError(t, err)

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- s.waitForIdle(ctx)
	}()

	cancel()

	assert.Eventually(t, func() bool {
		return len(doneCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)
	err = <-doneCh
	require.Error(t, err)
}

func TestApplySchedulerClose(t *testing.T) {
	ctx := testCtx(t)
	s := newApplyScheduler(ctx)

	// Enqueue some transactions
	require.NoError(t, s.enqueue(&applyTxn{order: 1, writeset: []uint64{100}}))
	require.NoError(t, s.enqueue(&applyTxn{order: 2, writeset: []uint64{200}}))

	s.mu.Lock()
	assert.Equal(t, 2, s.pendingCount)
	s.mu.Unlock()

	err := s.close()
	require.Error(t, err) // returns io.EOF

	s.mu.Lock()
	assert.Equal(t, 2, s.pendingCount)
	assert.Len(t, s.pending, 2)
	assert.Equal(t, 0, s.pendingOff)
	s.mu.Unlock()
}

// ---------- noConflict scheduling tests ----------

func TestApplySchedulerNoConflictAlwaysReady(t *testing.T) {
	ctx := testCtx(t)
	s := newApplyScheduler(ctx)

	// A forceGlobal txn is inflight
	global := &applyTxn{order: 1, forceGlobal: true}
	require.NoError(t, s.enqueue(global))
	got, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, global, got)

	// Now enqueue a noConflict txn — should be ready even with inflight global
	nc := &applyTxn{order: 2, noConflict: true}
	require.NoError(t, s.enqueue(nc))

	gotNC, err := s.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, nc, gotNC)

	require.NoError(t, s.markCommitted(got))
	require.NoError(t, s.markCommitted(gotNC))
}

// ---------- removePendingLocked compaction tests ----------

func TestApplySchedulerRemovePendingCompaction(t *testing.T) {
	ctx := testCtx(t)
	s := newApplyScheduler(ctx)

	// Enqueue 4 transactions with independent writesets
	for i := int64(1); i <= 4; i++ {
		require.NoError(t, s.enqueue(&applyTxn{order: i, writeset: []uint64{uint64(i)}}))
	}

	// Dequeue all 4 — this exercises removePendingLocked compaction
	for range 4 {
		got, err := s.nextReady(ctx)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.NoError(t, s.markCommitted(got))
	}

	s.mu.Lock()
	assert.Equal(t, 0, s.pendingCount)
	s.mu.Unlock()
}

// ---------- snapshotTablePlans tests ----------

func TestSnapshotTablePlans_Nil(t *testing.T) {
	mu := &sync.RWMutex{}
	version := &atomic.Int64{}
	var cachedVersion int64
	result := snapshotTablePlans(mu, nil, version, &cachedVersion, nil)
	assert.Nil(t, result)
}

func TestSnapshotTablePlans_CopiesMap(t *testing.T) {
	mu := &sync.RWMutex{}
	plans := map[string]*TablePlan{
		"t1": {TargetName: "t1"},
		"t2": {TargetName: "t2"},
	}
	version := &atomic.Int64{}
	version.Store(1)
	var cachedVersion int64

	snap := snapshotTablePlans(mu, plans, version, &cachedVersion, nil)
	require.Len(t, snap, 2)
	assert.Equal(t, "t1", snap["t1"].TargetName)
	assert.Equal(t, "t2", snap["t2"].TargetName)
	assert.Equal(t, int64(1), cachedVersion)

	// Modify original — snapshot should not be affected
	plans["t3"] = &TablePlan{TargetName: "t3"}
	assert.Len(t, snap, 2)
}

func TestSnapshotTablePlans_UsesCacheWhenVersionMatches(t *testing.T) {
	mu := &sync.RWMutex{}
	plans := map[string]*TablePlan{
		"t1": {TargetName: "t1"},
	}
	version := &atomic.Int64{}
	version.Store(5)
	var cachedVersion int64 = 5
	cached := map[string]*TablePlan{
		"cached": {TargetName: "cached"},
	}

	snap := snapshotTablePlans(mu, plans, version, &cachedVersion, cached)
	// Should return the cached map since versions match
	require.Len(t, snap, 1)
	assert.Equal(t, "cached", snap["cached"].TargetName)
}

func TestSnapshotTablePlans_RefreshesCacheWhenVersionChanges(t *testing.T) {
	mu := &sync.RWMutex{}
	plans := map[string]*TablePlan{
		"t1": {TargetName: "t1"},
	}
	version := &atomic.Int64{}
	version.Store(6)
	var cachedVersion int64 = 5
	cached := map[string]*TablePlan{
		"stale": {TargetName: "stale"},
	}

	snap := snapshotTablePlans(mu, plans, version, &cachedVersion, cached)
	require.Len(t, snap, 1)
	assert.Equal(t, "t1", snap["t1"].TargetName)
	assert.Equal(t, int64(6), cachedVersion)
}

// ---------- scheduleItems tests ----------

// testVPlayer creates a minimal vplayer stub for testing scheduleItems.
// The returned vplayer has mocked query/commit functions and a mock DB client.
func testVPlayer(t *testing.T) (*vplayer, *binlogplayer.MockDBClient) {
	t.Helper()
	mockDB := binlogplayer.NewMockDBClient(t)
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	config, err := vttablet.NewVReplicationConfig(nil)
	require.NoError(t, err)
	vr := &vreplicator{
		id:             1,
		stats:          stats,
		dbClient:       newVDBClient(mockDB, stats, config.RelayLogMaxItems),
		workflowConfig: config,
		vre:            &Engine{},
		source:         &binlogdatapb.BinlogSource{OnDdl: binlogdatapb.OnDDLAction_IGNORE},
	}

	vp := &vplayer{
		vr:                vr,
		tablePlansMu:      &sync.RWMutex{},
		tablePlans:        make(map[string]*TablePlan),
		tablePlansVersion: &atomic.Int64{},
		serialMu:          &sync.Mutex{},
		lastTimestampNs:   &atomic.Int64{},
		timeOffsetNs:      &atomic.Int64{},
		timeLastSaved:     time.Now(),
		idStr:             "1",
		query: func(ctx context.Context, sql string) (*sqltypes.Result, error) {
			return &sqltypes.Result{}, nil
		},
		commit: func() error {
			return nil
		},
		dbClient: vr.dbClient,
	}
	return vp, mockDB
}

func TestTestVPlayerDoesNotMutateDefaultWorkflowConfig(t *testing.T) {
	defaults := vttablet.InitVReplicationConfigDefaults()
	savedWorkers := defaults.ParallelReplicationWorkers
	t.Cleanup(func() {
		defaults.ParallelReplicationWorkers = savedWorkers
	})
	defaults.ParallelReplicationWorkers = 1

	vp, _ := testVPlayer(t)
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2

	config, err := vttablet.NewVReplicationConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, 1, config.ParallelReplicationWorkers)
}

// publishExecutedDDLBarrier mirrors commitLoop's post-DDL publication so the
// scheduler tests can model only DDLs that actually executed on the target.
func publishExecutedDDLBarrier(t *testing.T, vp *vplayer, statement string) {
	t.Helper()
	vp.serialMu.Lock()
	defer vp.serialMu.Unlock()
	vp.tablePlansMu.RLock()
	renameTargets := extractDDLRenameTargets(statement, vp.vr.vre.env.Parser())
	retargetPostDDLStalePlans(vp.postDDLStalePlans, renameTargets, vp.tablePlans)
	ddlStalePlans, conservative := extractDDLAffectedTables(statement, vp.vr.vre.env.Parser(), vp.tablePlans, vp.postDDLDroppedTables)
	ddlStalePlans = unresolvedPostDDLStalePlans(vp.tablePlans, vp.postDDLDroppedTables, ddlStalePlans)
	vp.tablePlansMu.RUnlock()
	vp.postDDLStalePlans = mergePostDDLStalePlans(vp.postDDLStalePlans, ddlStalePlans)
	vp.postDDLConservative = vp.postDDLConservative || conservative
	vp.postDDLDroppedTables = mergeDroppedTables(vp.postDDLDroppedTables, extractDroppedTables(statement, vp.vr.vre.env.Parser()))
}

// commitScheduledExecutedDDL models a commitLoop DDL commit and then syncs the
// resulting barrier into scheduler state the way the next fetch would observe it.
func commitScheduledExecutedDDL(t *testing.T, ctx context.Context, scheduler *applyScheduler, state *parallelScheduleState, vp *vplayer) {
	t.Helper()
	ddlTxn, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, binlogdatapb.VEventType_DDL, ddlTxn.payload.events[0].Type)
	publishExecutedDDLBarrier(t, vp, ddlTxn.payload.events[0].Statement)
	require.NoError(t, scheduler.markCommitted(ddlTxn))
	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, nil))
}

func TestApplyEventsParallelCanceledContext(t *testing.T) {
	vp, _ := testVPlayer(t)

	ctx, cancel := context.WithCancel(testCtx(t))
	cancel()

	vp.vr.workflowConfig.ParallelReplicationWorkers = 1

	relay := newRelayLog(ctx, 10, 100)

	err := vp.applyEventsParallel(ctx, relay)
	require.ErrorIs(t, err, context.Canceled)
}

func TestApplyEventsParallelReturnsScheduleError(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)

	vp.vr.workflowConfig.ParallelReplicationWorkers = 2

	mockDB.AddInvariant("set @@session.time_zone", &sqltypes.Result{})
	mockDB.AddInvariant("set names 'binary'", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.net_read_timeout", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.net_write_timeout", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.sql_mode", &sqltypes.Result{})
	mockDB.AddInvariant("information_schema.key_column_usage", &sqltypes.Result{})
	mockDB.AddInvariant("select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys, options from _vt.vreplication where id=1", sqlModeWorkflowSettingsResult(binlogdatapb.VReplicationWorkflowType_MoveTables))
	mockDB.AddInvariant("select @@session.sql_mode as sql_mode", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("sql_mode", "varchar"),
		"STRICT_TRANS_TABLES",
	))
	mockDB.AddInvariant("select count(distinct table_name) from _vt.copy_state where vrepl_id=1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("count(distinct table_name)", "int64"),
		"0",
	))
	mockDB.AddInvariant("max_allowed_packet", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("max_allowed_packet", "int64"),
		"4194304",
	))
	mockDB.AddInvariant("rollback", &sqltypes.Result{})

	if vp.vr.vre == nil {
		vp.vr.vre = &Engine{}
	}
	if vp.vr.vre.throttlerClient == nil {
		vp.vr.vre.throttlerClient = throttle.NewBackgroundClient(nil, throttlerapp.VReplicationName, base.UndefinedScope)
	}
	if vp.vr.vre.dbClientFactoryFiltered == nil {
		vp.vr.vre.dbClientFactoryFiltered = func() binlogplayer.DBClient { return mockDB }
	}

	relay := newRelayLog(ctx, 10, 100)
	invalidGTID := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_GTID, Gtid: "invalid"}
	require.NoError(t, relay.Send([]*binlogdatapb.VEvent{invalidGTID}))

	err := vp.applyEventsParallel(ctx, relay)
	require.Error(t, err)
}

func TestApplyEventsParallelCommitsScheduledPrefixBeforeScheduleError(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)

	vp.vr.workflowConfig.ParallelReplicationWorkers = 2
	vp.vr.workflowConfig.StoreCompressedGTID = false

	mockDB.AddInvariant("set @@session.time_zone", &sqltypes.Result{})
	mockDB.AddInvariant("set names 'binary'", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.net_read_timeout", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.net_write_timeout", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.sql_mode", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.foreign_key_checks", &sqltypes.Result{})
	mockDB.AddInvariant("information_schema.key_column_usage", &sqltypes.Result{})
	mockDB.AddInvariant("select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys, options from _vt.vreplication where id=1", sqlModeWorkflowSettingsResult(binlogdatapb.VReplicationWorkflowType_MoveTables))
	mockDB.AddInvariant("select count(distinct table_name) from _vt.copy_state where vrepl_id=1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("count(distinct table_name)", "int64"),
		"0",
	))
	mockDB.AddInvariant("max_allowed_packet", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("max_allowed_packet", "int64"),
		"4194304",
	))
	mockDB.ExpectRequestRE("update _vt\\.vreplication set pos=", &sqltypes.Result{}, nil)
	mockDB.AddInvariant("rollback", &sqltypes.Result{})
	vp.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
		return vp.dbClient.Execute(sql)
	}
	vp.commit = vp.dbClient.Commit

	if vp.vr.vre == nil {
		vp.vr.vre = &Engine{}
	}
	if vp.vr.vre.throttlerClient == nil {
		vp.vr.vre.throttlerClient = throttle.NewBackgroundClient(nil, throttlerapp.VReplicationName, base.UndefinedScope)
	}
	if vp.vr.vre.dbClientFactoryFiltered == nil {
		vp.vr.vre.dbClientFactoryFiltered = func() binlogplayer.DBClient { return mockDB }
	}

	relay := newRelayLog(ctx, 10, 100)
	validGTID := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"}
	otherEvent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_OTHER, Timestamp: 100}
	invalidGTID := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_GTID, Gtid: "invalid"}
	require.NoError(t, relay.Send([]*binlogdatapb.VEvent{validGTID, otherEvent, invalidGTID}))

	err := vp.applyEventsParallel(ctx, relay)
	require.Error(t, err)
	mockDB.Wait()
}

func TestApplyEventsParallelCancelledContext(t *testing.T) {
	vp, _ := testVPlayer(t)

	ctx, cancel := context.WithCancel(testCtx(t))
	cancel()

	vp.vr.workflowConfig.ParallelReplicationWorkers = 1

	relay := newRelayLog(ctx, 10, 100)

	err := vp.applyEventsParallel(ctx, relay)
	require.ErrorIs(t, err, context.Canceled)
}

func TestScheduleItems_GTIDAndROWAndCOMMIT(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	// Set up a table plan so writeset can be built
	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "t1",
			RowChanges: []*binlogdatapb.RowChange{
				{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}},
			},
		},
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, rowEvent, commitEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	// Should have enqueued exactly one transaction
	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(1), got.order)
	assert.NotNil(t, got.payload)
	assert.Len(t, got.payload.events, 1) // ROW event only
	assert.Equal(t, binlogdatapb.VEventType_ROW, got.payload.events[0].Type)
	assert.True(t, got.payload.rowOnly)
}

func TestScheduleLoopCanceledContext(t *testing.T) {
	vp, _ := testVPlayer(t)

	ctx, cancel := context.WithCancel(testCtx(t))
	cancel()

	scheduler := newApplyScheduler(ctx)
	relay := newRelayLog(ctx, 10, 100)

	err := vp.scheduleLoop(ctx, relay, scheduler)
	require.ErrorIs(t, err, context.Canceled)
}

func TestScheduleLoopProcessesItems(t *testing.T) {
	vp, mockDB := testVPlayer(t)

	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()

	mockDB.AddInvariant("rollback", &sqltypes.Result{})

	if vp.vr.vre == nil {
		vp.vr.vre = &Engine{}
	}
	if vp.vr.vre.throttlerClient == nil {
		vp.vr.vre.throttlerClient = throttle.NewBackgroundClient(nil, throttlerapp.VReplicationName, base.UndefinedScope)
	}

	scheduler := newApplyScheduler(ctx)
	relay := newRelayLog(ctx, 10, 100)

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	gtidEvent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		},
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_COMMIT}

	require.NoError(t, relay.Send([]*binlogdatapb.VEvent{gtidEvent, rowEvent, commitEvent}))

	errCh := make(chan error, 1)
	go func() {
		errCh <- vp.scheduleLoop(ctx, relay, scheduler)
	}()

	ready, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.NotNil(t, ready)

	cancel()

	select {
	case err := <-errCh:
		require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, io.EOF))
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for scheduleLoop")
	}
}

func TestScheduleLoopThrottledUpdates(t *testing.T) {
	vp, mockDB := testVPlayer(t)

	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()

	if globalStats.ThrottledCount == nil {
		globalStats.ThrottledCount = stats.NewCounter("", "")
	}

	mockDB.AddInvariant("rollback", &sqltypes.Result{})
	mockDB.AddInvariant("time_throttled", &sqltypes.Result{})

	if vp.vr.vre == nil {
		vp.vr.vre = &Engine{}
	}
	if vp.vr.vre.throttlerClient == nil {
		vp.vr.vre.throttlerClient = throttle.NewBackgroundClient(nil, throttlerapp.VReplicationName, base.UndefinedScope)
	}
	vp.throttlerAppName = throttlerapp.TestingAlwaysThrottledName.String()
	if vp.vr.throttleUpdatesRateLimiter == nil {
		vp.vr.throttleUpdatesRateLimiter = timer.NewRateLimiter(time.Millisecond)
		defer vp.vr.throttleUpdatesRateLimiter.Stop()
	}

	scheduler := newApplyScheduler(ctx)
	relay := newRelayLog(ctx, 10, 100)

	errCh := make(chan error, 1)
	go func() {
		errCh <- vp.scheduleLoop(ctx, relay, scheduler)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, io.EOF))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for scheduleLoop")
	}
}

func TestScheduleLoopCancelledContext(t *testing.T) {
	vp, _ := testVPlayer(t)

	ctx, cancel := context.WithCancel(testCtx(t))
	cancel()

	scheduler := newApplyScheduler(ctx)
	relay := newRelayLog(ctx, 10, 100)

	err := vp.scheduleLoop(ctx, relay, scheduler)
	require.ErrorIs(t, err, context.Canceled)
}

func TestScheduleItems_EmptyTransaction(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	commitEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_COMMIT,
		Timestamp: 100,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, commitEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	// Empty transaction should NOT be enqueued — it just sets unsavedEvent
	vp.serialMu.Lock()
	assert.Equal(t, commitEvent, vp.unsavedEvent)
	vp.serialMu.Unlock()
}

func TestScheduleItems_VERSIONIsIgnoredLikeEmptyTransaction(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	versionEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_VERSION,
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_COMMIT,
		Timestamp: 100,
	}

	err := vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{gtidEvent, versionEvent, commitEvent}})
	require.NoError(t, err)

	vp.serialMu.Lock()
	assert.Equal(t, commitEvent, vp.unsavedEvent)
	vp.serialMu.Unlock()
}

func TestScheduleItems_EmptyTxnWithCommitMeta_AdvancesSequence(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	gtidEvent := &binlogdatapb.VEvent{
		Type:           binlogdatapb.VEventType_GTID,
		Gtid:           "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		SequenceNumber: 7,
		CommitParent:   6,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, commitEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	// Should have advanced the committed sequence to 7
	scheduler.mu.Lock()
	assert.Equal(t, int64(7), scheduler.lastCommittedSequence)
	scheduler.mu.Unlock()
}

func TestScheduleItems_BEGINIsIgnored(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	beginEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_BEGIN,
	}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "t1",
			RowChanges: []*binlogdatapb.RowChange{
				{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}},
			},
		},
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	// BEGIN should not be added to curEvents
	items := [][]*binlogdatapb.VEvent{{gtidEvent, beginEvent, rowEvent, commitEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	// Should only have the ROW event, not BEGIN
	assert.Len(t, got.payload.events, 1)
	assert.Equal(t, binlogdatapb.VEventType_ROW, got.payload.events[0].Type)
}

func TestScheduleItems_DDLIsForceGlobal(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	ddlEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_DDL,
		Timestamp: 200,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, ddlEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	assert.True(t, got.payload.commitOnly)
	assert.Len(t, got.payload.events, 1)
	assert.Equal(t, binlogdatapb.VEventType_DDL, got.payload.events[0].Type)
}

func TestScheduleItems_PostDDLComplexDDLDoesNotClearOnUnrelatedPlanRefresh(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	staleT1 := &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	staleT2 := &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	staleT3 := &TablePlan{TargetName: "t3", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t1"] = staleT1
	vp.tablePlans["t2"] = staleT2
	vp.tablePlans["t3"] = staleT3
	vp.tablePlansVersion.Store(1)

	ddlItems := [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t1 to t1_new, t2 to t2_new", Timestamp: 200},
	}}
	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, ddlItems))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	require.Contains(t, state.postDDLStalePlans, "t1")
	require.Contains(t, state.postDDLStalePlans, "t2")
	require.NotContains(t, state.postDDLStalePlans, "t3")

	// Simulate an unrelated plan refresh while plans for DDL-affected tables remain stale.
	vp.tablePlansMu.Lock()
	vp.tablePlans["t3"] = &TablePlan{TargetName: "t3", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansMu.Unlock()

	rowItems := [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}
	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, rowItems))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	assert.NotNil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLRenameClearsAfterRenamedTableFieldRefresh(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t1 to t1_new", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)

	vp.tablePlansMu.Lock()
	vp.tablePlans["t1_new"] = &TablePlan{
		TargetName: "t1_new",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1_new",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	assert.Nil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLUnknownDDLRetainsConservativeBarrier(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	staleT1 := &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	staleT2 := &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t1"] = staleT1
	vp.tablePlans["t2"] = staleT2
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "this is not valid ddl", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)

	vp.tablePlansMu.Lock()
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	assert.NotNil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLDropDoesNotClearOnUnrelatedPlanRefresh(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)
	vp.replicatorPlan = &ReplicatorPlan{TablePlans: map[string]*TablePlan{
		"t1": {TargetName: "t1", Insert: sqlparser.BuildParsedQuery("insert into t1 values (:a)")},
	}}

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "drop table t1", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	assert.Nil(t, state.postDDLStalePlans)

	vp.tablePlansMu.Lock()
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t2",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("11"), Lengths: []int64{1, 1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	assert.Nil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLDropClearsAfterDroppedTableSatisfied(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)
	vp.replicatorPlan = &ReplicatorPlan{TablePlans: map[string]*TablePlan{
		"t1": {TargetName: "t1", Insert: sqlparser.BuildParsedQuery("insert into t1 values (:a)")},
	}}

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "drop table t1", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	assert.Nil(t, state.postDDLStalePlans)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t2",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	assert.Nil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLDroppedTablesSnapshotDoesNotAliasVPlayer(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.postDDLDroppedTables = map[string]struct{}{"t1": {}}

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, nil))
	require.Equal(t, map[string]struct{}{"t1": {}}, state.postDDLDroppedTables)

	delete(vp.postDDLDroppedTables, "t1")
	vp.postDDLDroppedTables["t2"] = struct{}{}

	require.Equal(t, map[string]struct{}{"t1": {}}, state.postDDLDroppedTables)
}

func TestScheduleItems_PostDDLAlterRenameClearsAfterRenamedTableFieldRefresh(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "alter table t1 rename to t1_new", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)

	vp.tablePlansMu.Lock()
	vp.tablePlans["t1_new"] = &TablePlan{TargetName: "t1_new", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1_new",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	assert.Nil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLAlterClearsAfterSameTableFieldRefresh(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "alter table t1 add column c1 int", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	require.Contains(t, state.postDDLStalePlans, "t1")

	vp.tablePlansMu.Lock()
	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("11"), Lengths: []int64{1, 1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	assert.Nil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLSecondDDLDoesNotReplaceEarlierUnresolvedBarrier(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t3"] = &TablePlan{TargetName: "t3", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t1 to t1_new", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	require.Contains(t, state.postDDLStalePlans, "t1")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "alter table t2 add column c1 int", Timestamp: 250},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Contains(t, state.postDDLStalePlans, "t1")
	require.Contains(t, state.postDDLStalePlans, "t2")

	vp.tablePlansMu.Lock()
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t3",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	require.NotNil(t, state.postDDLStalePlans)
	assert.Contains(t, state.postDDLStalePlans, "t1")
}

func TestScheduleItems_PostDDLUnknownSecondDDLExpandsBarrierConservatively(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t3"] = &TablePlan{TargetName: "t3", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t1 to t1_new", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Contains(t, state.postDDLStalePlans, "t1")
	require.NotContains(t, state.postDDLStalePlans, "t2")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "this is not valid ddl", Timestamp: 250},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Contains(t, state.postDDLStalePlans, "t1")
	require.Contains(t, state.postDDLStalePlans, "t2")
	require.Contains(t, state.postDDLStalePlans, "t3")

	vp.tablePlansMu.Lock()
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlans["t3"] = &TablePlan{TargetName: "t3", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t2",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("11"), Lengths: []int64{1, 1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	require.NotNil(t, state.postDDLStalePlans)
	assert.Contains(t, state.postDDLStalePlans, "t1")
}

func TestScheduleItems_PostDDLRenameThenUnknownStillBlocksAfterRenamedTableRefresh(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)
	vp.replicatorPlan = &ReplicatorPlan{TablePlans: map[string]*TablePlan{
		"t1": {TargetName: "t1", Insert: sqlparser.BuildParsedQuery("insert into t1 values (:a)")},
	}}

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t1 to t1_new", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Contains(t, state.postDDLStalePlans, "t1")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "this is not valid ddl", Timestamp: 250},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)

	vp.tablePlansMu.Lock()
	vp.tablePlans["t1_new"] = &TablePlan{TargetName: "t1_new", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1_new",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("11"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	require.NotNil(t, state.postDDLStalePlans)
	assert.Contains(t, state.postDDLStalePlans, "t1")
}

func TestScheduleItems_PostDDLRenameRetiresOldNameFromLaterUnknownBarrier(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t1 to t1_new", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Contains(t, state.postDDLStalePlans, "t1")

	vp.tablePlansMu.Lock()
	vp.tablePlans["t1_new"] = &TablePlan{TargetName: "t1_new", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1_new",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 250},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	require.NoError(t, scheduler.markCommitted(got))
	assert.Nil(t, state.postDDLStalePlans)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "this is not valid ddl", Timestamp: 300},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	assert.NotContains(t, state.postDDLStalePlans, "t1")
	assert.Contains(t, state.postDDLStalePlans, "t1_new")
	assert.Contains(t, state.postDDLStalePlans, "t2")

	vp.tablePlansMu.Lock()
	vp.tablePlans["t1_new"] = &TablePlan{TargetName: "t1_new", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-8"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t2",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("11"), Lengths: []int64{1, 1}}}},
		}, Timestamp: 350},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err = scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	assert.Nil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLRenameRetiresOldNameEvenWhenAnotherBarrierRemains(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t3"] = &TablePlan{TargetName: "t3", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t1 to t1_new", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Contains(t, state.postDDLStalePlans, "t1")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "alter table t3 add column c1 int", Timestamp: 250},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Contains(t, state.postDDLStalePlans, "t1")
	require.Contains(t, state.postDDLStalePlans, "t3")

	vp.tablePlansMu.Lock()
	vp.tablePlans["t1_new"] = &TablePlan{TargetName: "t1_new", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1_new",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	require.NoError(t, scheduler.markCommitted(got))
	require.NotNil(t, state.postDDLStalePlans)
	assert.NotContains(t, state.postDDLStalePlans, "t1")
	assert.Contains(t, state.postDDLStalePlans, "t3")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-8"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "this is not valid ddl", Timestamp: 350},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	assert.NotContains(t, state.postDDLStalePlans, "t1")
	assert.Contains(t, state.postDDLStalePlans, "t1_new")
	assert.Contains(t, state.postDDLStalePlans, "t3")
}

func TestScheduleItems_PostDDLRenameSwapRequiresBothTablesToRefresh(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t1 to t2, t2 to t1", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)

	vp.tablePlansMu.Lock()
	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t2",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
}

func TestScheduleItems_PostDDLCreateTableDoesNotBlockUnrelatedTable(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "create table t3(id bigint primary key)", Timestamp: 200},
	}}))

	ddlTxn, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, binlogdatapb.VEventType_DDL, ddlTxn.payload.events[0].Type)
	_, err = vp.applyDDLEvent(ctx, ddlTxn.payload.events[0], nil)
	require.NoError(t, err)
	require.NoError(t, scheduler.markCommitted(ddlTxn))

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t2",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
}

func TestScheduleItems_PostDDLDropTableDoesNotBlockUnrelatedTable(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "drop table t1", Timestamp: 200},
	}}))

	ddlTxn, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, binlogdatapb.VEventType_DDL, ddlTxn.payload.events[0].Type)
	require.NoError(t, scheduler.markCommitted(ddlTxn))

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t2",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
}

func TestScheduleItems_PostDDLExecIgnoreFailureDoesNotBlockAffectedTable(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC_IGNORE
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	mockDB.AddInvariant("update _vt.vreplication set", &sqltypes.Result{})
	vp.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
		if sql == "alter table t1 add column c1 int" {
			return nil, errors.New("ddl failed")
		}
		return &sqltypes.Result{}, nil
	}

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "alter table t1 add column c1 int", Timestamp: 200},
	}}))

	ddlTxn, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.Equal(t, binlogdatapb.VEventType_DDL, ddlTxn.payload.events[0].Type)
	require.NoError(t, vp.applyEvent(ctx, ddlTxn.payload.events[0], ddlTxn.payload.mustSave))
	require.NoError(t, scheduler.markCommitted(ddlTxn))

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
}

func TestScheduleItems_PostDDLDropThenUnknownStillClearsAfterDropSatisfaction(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "drop table t1", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	assert.Nil(t, state.postDDLStalePlans)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "this is not valid ddl", Timestamp: 250},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	assert.NotContains(t, state.postDDLStalePlans, "t1")
	assert.Contains(t, state.postDDLStalePlans, "t2")

	vp.tablePlansMu.Lock()
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t2",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("11"), Lengths: []int64{1, 1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	assert.Nil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLRecreatedDroppedTableIsTrackedAgain(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	mockDB.AddInvariant("begin", &sqltypes.Result{})

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)
	vp.replicatorPlan = &ReplicatorPlan{TablePlans: map[string]*TablePlan{
		"t1": {TargetName: "t1", Insert: sqlparser.BuildParsedQuery("insert into t1 values (:a)")},
	}}

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "drop table t1", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Nil(t, state.postDDLStalePlans)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "this is not valid ddl", Timestamp: 250},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	require.NotContains(t, state.postDDLStalePlans, "t1")
	require.Contains(t, state.postDDLStalePlans, "t2")

	fieldEvent := &binlogdatapb.FieldEvent{
		TableName: "t1",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "c1", Type: querypb.Type_INT64},
		},
	}
	require.NoError(t, vp.applyEvent(ctx, &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_FIELD, FieldEvent: fieldEvent}, false))

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "this is not valid ddl", Timestamp: 300},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	assert.Contains(t, state.postDDLStalePlans, "t1")
	assert.Contains(t, state.postDDLStalePlans, "t2")
}

func TestScheduleItems_PostDDLDropThenCreateSameTableBlocksUntilFieldRefresh(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	staleT1 := &TablePlan{
		TargetName:              "t1",
		Fields:                  []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:               []bool{true},
		IdentityColumns:         []string{"id"},
		HasExtraUniqueSecondary: false,
	}
	vp.tablePlans["t1"] = staleT1
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "drop table t1", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Nil(t, state.postDDLStalePlans)
	require.Contains(t, vp.postDDLDroppedTables, "t1")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "create table t1(id bigint primary key, email bigint unique)", Timestamp: 250},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	require.Contains(t, state.postDDLStalePlans, "t1")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("11"), Lengths: []int64{1, 1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	require.NotNil(t, state.postDDLStalePlans)
	assert.Contains(t, state.postDDLStalePlans, "t1")
	require.NoError(t, scheduler.markCommitted(got))

	vp.tablePlansMu.Lock()
	vp.tablePlans["t1"] = &TablePlan{
		TargetName:              "t1",
		Fields:                  []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "email", Type: querypb.Type_INT64}},
		PKIndices:               []bool{true, false},
		IdentityColumns:         []string{"id"},
		HasExtraUniqueSecondary: true,
	}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-8"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t2",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 350},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err = scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	assert.Nil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLDropThenRenameToDroppedNameBlocksUntilFieldRefresh(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t3"] = &TablePlan{TargetName: "t3", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "drop table t1", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Nil(t, state.postDDLStalePlans)
	require.Contains(t, vp.postDDLDroppedTables, "t1")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t2 to t1", Timestamp: 250},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	require.Contains(t, state.postDDLStalePlans, "t2")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	require.NoError(t, scheduler.markCommitted(got))

	vp.tablePlansMu.Lock()
	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-8"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t3",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 350},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err = scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	assert.Nil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLRenameThenCreateSameNameRequiresBothFieldRefreshes(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t1 to t2", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	require.Contains(t, state.postDDLStalePlans, "t1")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "create table t1(id bigint primary key)", Timestamp: 250},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.NotNil(t, state.postDDLStalePlans)
	require.Contains(t, state.postDDLStalePlans, "t1")

	vp.tablePlansMu.Lock()
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t2",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("11"), Lengths: []int64{1, 1}}}},
		}, Timestamp: 300},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	require.NoError(t, scheduler.markCommitted(got))
	require.NotNil(t, state.postDDLStalePlans)
	require.Contains(t, state.postDDLStalePlans, "t1")

	vp.tablePlansMu.Lock()
	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "email", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-8"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("11"), Lengths: []int64{1, 1}}}},
		}, Timestamp: 350},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err = scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	assert.Nil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLDropCreateRenameRetargetsBarrierToFinalName(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "drop table t1", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "create table t1(id bigint primary key)", Timestamp: 250},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Contains(t, state.postDDLStalePlans, "t1")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t1 to t2", Timestamp: 300},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Contains(t, state.postDDLStalePlans, "t1")

	vp.tablePlansMu.Lock()
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, nil))
	assert.Nil(t, state.postDDLStalePlans)
}

func TestScheduleItems_PostDDLRenameChainRetargetsBarrierToFinalName(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}

	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t2"] = &TablePlan{TargetName: "t2", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlans["t3"] = &TablePlan{TargetName: "t3", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}, PKIndices: []bool{true}}
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t1 to t2", Timestamp: 200},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Contains(t, state.postDDLStalePlans, "t1")

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_DDL, Statement: "rename table t2 to t3", Timestamp: 250},
	}}))
	commitScheduledExecutedDDL(t, ctx, scheduler, state, vp)
	require.Contains(t, state.postDDLStalePlans, "t1")
	require.Contains(t, state.postDDLStalePlans, "t2")

	vp.tablePlansMu.Lock()
	vp.tablePlans["t3"] = &TablePlan{TargetName: "t3", Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "c1", Type: querypb.Type_INT64}}, PKIndices: []bool{true, false}}
	vp.tablePlansVersion.Add(1)
	vp.tablePlansMu.Unlock()

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, nil))
	assert.Nil(t, state.postDDLStalePlans)
}

func TestMergeDroppedTables_DoesNotMutateInput(t *testing.T) {
	original := map[string]struct{}{"t1": {}}
	merged := mergeDroppedTables(original, map[string]struct{}{"t2": {}})

	assert.Equal(t, map[string]struct{}{"t1": {}}, original)
	assert.Equal(t, map[string]struct{}{"t1": {}, "t2": {}}, merged)
}

func TestRetargetPostDDLStalePlans_RenameSwapUsesOriginalRefreshNames(t *testing.T) {
	t1Old := &TablePlan{TargetName: "t1"}
	t2Old := &TablePlan{TargetName: "t2"}
	t1New := &TablePlan{TargetName: "t1"}
	t2New := &TablePlan{TargetName: "t2"}

	stalePlans := map[string]postDDLStalePlan{
		"barrier": {
			stalePlan: t1Old,
			refreshedPlans: map[string]*TablePlan{
				"t1": t1Old,
				"t2": t2Old,
			},
		},
	}

	retargetPostDDLStalePlans(stalePlans, map[string]string{"t1": "t2", "t2": "t1"}, map[string]*TablePlan{"t1": t1New, "t2": t2New})

	require.Contains(t, stalePlans, "barrier")
	assert.Equal(t, map[string]*TablePlan{"t1": t1New, "t2": t2New}, stalePlans["barrier"].refreshedPlans)
}

func TestScheduleItems_OTHERIsForceGlobal(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	otherEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_OTHER,
		Timestamp: 200,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, otherEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	assert.True(t, got.payload.commitOnly)
}

func TestScheduleItems_CopyStateForceGlobal(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	// When copyState is non-empty, all transactions should be forceGlobal
	vp.copyState = map[string]*sqltypes.Result{"t1": {}}
	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "t1",
			RowChanges: []*binlogdatapb.RowChange{
				{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}},
			},
		},
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, rowEvent, commitEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
}

func TestScheduleItems_ExtraUniqueSecondaryIndexForcesGlobal(t *testing.T) {
	ctx := testCtx(t)
	vp, _ := testVPlayer(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2

	tableName := "parallel_apply_unique_secondary_idx"
	qualifiedTableName := vrepldb + "." + tableName
	execStatements(t, []string{
		"create table " + qualifiedTableName + " (id int not null, email varchar(128) not null, primary key(id), unique key uk_email(email))",
	})
	t.Cleanup(func() {
		execStatements(t, []string{"drop table if exists " + qualifiedTableName})
	})

	realDB := &realDBClient{nolog: true}
	require.NoError(t, realDB.Connect())
	t.Cleanup(realDB.Close)

	vp.vr.dbClient = newVDBClient(realDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	vp.dbClient = vp.vr.dbClient
	vp.vr.mysqld = &infoSchemaMysqld{MysqlDaemon: env.Mysqld}
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	vp.vr.source.Filter = &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: tableName}}}

	colInfoMap, err := vp.vr.buildColInfoMap(ctx)
	require.NoError(t, err)
	vp.vr.colInfoMap = colInfoMap

	vp.replicatorPlan, err = vp.vr.buildReplicatorPlan(
		vp.vr.source,
		vp.vr.colInfoMap,
		nil,
		vp.vr.stats,
		vp.vr.vre.env.CollationEnv(),
		vp.vr.vre.env.Parser(),
	)
	require.NoError(t, err)

	require.NoError(t, vp.applyEvent(ctx, &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: tableName,
			Fields: []*querypb.Field{
				{Name: "id", Type: querypb.Type_INT32},
				{Name: "email", Type: querypb.Type_VARCHAR},
			},
		},
	}, false))
	require.NoError(t, vp.dbClient.Rollback())

	err = vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName: tableName,
			RowChanges: []*binlogdatapb.RowChange{{
				After: &querypb.Row{Values: []byte("1a"), Lengths: []int64{1, 1}},
			}},
		}, Timestamp: 100},
		{Type: binlogdatapb.VEventType_COMMIT},
	}})
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
}

func TestScheduleItems_UnsupportedWritesetMappingForcesGlobal(t *testing.T) {
	ctx := testCtx(t)
	vp, _ := testVPlayer(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	env := vtenv.NewTestEnv()
	plan, err := (&vreplicator{workflowConfig: vp.vr.workflowConfig}).buildReplicatorPlan(
		getSource(&binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select a + b as c1, c as c2 from t1",
		}}}),
		map[string][]*ColumnInfo{"t1": {{Name: "c1", IsPK: true}, {Name: "c2"}}},
		nil,
		vp.vr.stats,
		env.CollationEnv(),
		env.Parser(),
	)
	require.NoError(t, err)

	tplan, err := plan.buildExecutionPlan(&binlogdatapb.FieldEvent{
		TableName: "t1",
		Fields: []*querypb.Field{
			{Name: "a", Type: querypb.Type_INT64},
			{Name: "b", Type: querypb.Type_INT64},
			{Name: "c", Type: querypb.Type_INT64},
		},
	})
	require.NoError(t, err)
	require.True(t, tplan.HasUnsupportedWritesetMapping)

	vp.tablePlans["t1"] = tplan
	vp.tablePlansVersion.Store(1)

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("123"), Lengths: []int64{1, 1, 1}}}},
		}, Timestamp: 100},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	require.NoError(t, scheduler.markCommitted(got))
}

func TestApplyEvent_FIELDMarksExtraUniqueSecondaryIndex(t *testing.T) {
	ctx := testCtx(t)
	vp, _ := testVPlayer(t)
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2

	tableName := "parallel_apply_field_unique_secondary_idx"
	qualifiedTableName := vrepldb + "." + tableName
	execStatements(t, []string{
		"create table " + qualifiedTableName + " (id int not null, email varchar(128) not null, primary key(id), unique key uk_email(email))",
	})
	t.Cleanup(func() {
		execStatements(t, []string{"drop table if exists " + qualifiedTableName})
	})

	realDB := &realDBClient{nolog: true}
	require.NoError(t, realDB.Connect())
	t.Cleanup(realDB.Close)

	vp.vr.dbClient = newVDBClient(realDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	vp.dbClient = vp.vr.dbClient
	vp.vr.mysqld = &infoSchemaMysqld{MysqlDaemon: env.Mysqld}
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	vp.vr.source.Filter = &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: tableName}}}

	colInfoMap, err := vp.vr.buildColInfoMap(ctx)
	require.NoError(t, err)
	vp.vr.colInfoMap = colInfoMap

	vp.replicatorPlan, err = vp.vr.buildReplicatorPlan(
		vp.vr.source,
		vp.vr.colInfoMap,
		nil,
		vp.vr.stats,
		vp.vr.vre.env.CollationEnv(),
		vp.vr.vre.env.Parser(),
	)
	require.NoError(t, err)

	require.NoError(t, vp.applyEvent(ctx, &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: tableName,
			Fields: []*querypb.Field{
				{Name: "id", Type: querypb.Type_INT32},
				{Name: "email", Type: querypb.Type_VARCHAR},
			},
		},
	}, false))
	require.NoError(t, vp.dbClient.Rollback())

	require.True(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)
}

func TestApplyEvent_FIELDCachesExtraUniqueSecondaryLookup(t *testing.T) {
	ctx := testCtx(t)
	vp, _ := testVPlayer(t)
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2

	tableName := "parallel_apply_cached_unique_secondary_idx"
	qualifiedTableName := vrepldb + "." + tableName
	execStatements(t, []string{
		"create table " + qualifiedTableName + " (id int not null, email varchar(128) not null, primary key(id), unique key uk_email(email))",
	})
	t.Cleanup(func() {
		execStatements(t, []string{"drop table if exists " + qualifiedTableName})
	})

	realDB := &realDBClient{nolog: true}
	require.NoError(t, realDB.Connect())
	t.Cleanup(realDB.Close)

	vp.vr.dbClient = newVDBClient(realDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	vp.dbClient = vp.vr.dbClient
	vp.vr.mysqld = &infoSchemaMysqld{MysqlDaemon: env.Mysqld}
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	vp.vr.source.Filter = &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: tableName}}}

	colInfoMap, err := vp.vr.buildColInfoMap(ctx)
	require.NoError(t, err)
	vp.vr.colInfoMap = colInfoMap

	vp.replicatorPlan, err = vp.vr.buildReplicatorPlan(
		vp.vr.source,
		vp.vr.colInfoMap,
		nil,
		vp.vr.stats,
		vp.vr.vre.env.CollationEnv(),
		vp.vr.vre.env.Parser(),
	)
	require.NoError(t, err)

	fieldEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: tableName,
			Fields: []*querypb.Field{
				{Name: "id", Type: querypb.Type_INT32},
				{Name: "email", Type: querypb.Type_VARCHAR},
			},
		},
	}
	require.NoError(t, vp.applyEvent(ctx, fieldEvent, false))
	require.NoError(t, vp.dbClient.Rollback())

	vp.vr.mysqld = nil

	require.NoError(t, vp.applyEvent(ctx, fieldEvent, false))
	require.NoError(t, vp.dbClient.Rollback())
	require.True(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)
}

func TestApplyEvent_FIELDCachesNoExtraUniqueSecondaryLookup(t *testing.T) {
	ctx := testCtx(t)
	vp, _ := testVPlayer(t)
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2

	tableName := "parallel_apply_cached_no_unique_secondary_idx"
	qualifiedTableName := vrepldb + "." + tableName
	execStatements(t, []string{
		"create table " + qualifiedTableName + " (id int not null, email varchar(128) not null, primary key(id), key idx_email(email))",
	})
	t.Cleanup(func() {
		execStatements(t, []string{"drop table if exists " + qualifiedTableName})
	})

	realDB := &realDBClient{nolog: true}
	require.NoError(t, realDB.Connect())
	t.Cleanup(realDB.Close)

	vp.vr.dbClient = newVDBClient(realDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	vp.dbClient = vp.vr.dbClient
	vp.vr.mysqld = &infoSchemaMysqld{MysqlDaemon: env.Mysqld}
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	vp.vr.source.Filter = &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: tableName}}}

	colInfoMap, err := vp.vr.buildColInfoMap(ctx)
	require.NoError(t, err)
	vp.vr.colInfoMap = colInfoMap

	vp.replicatorPlan, err = vp.vr.buildReplicatorPlan(
		vp.vr.source,
		vp.vr.colInfoMap,
		nil,
		vp.vr.stats,
		vp.vr.vre.env.CollationEnv(),
		vp.vr.vre.env.Parser(),
	)
	require.NoError(t, err)

	fieldEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: tableName,
			Fields: []*querypb.Field{
				{Name: "id", Type: querypb.Type_INT32},
				{Name: "email", Type: querypb.Type_VARCHAR},
			},
		},
	}
	require.NoError(t, vp.applyEvent(ctx, fieldEvent, false))
	require.NoError(t, vp.dbClient.Rollback())
	require.Contains(t, vp.tablePlans, tableName)
	require.False(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)

	vp.vr.mysqld = nil

	require.NoError(t, vp.applyEvent(ctx, fieldEvent, false))
	require.NoError(t, vp.dbClient.Rollback())
	require.False(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)
}

func TestApplyEvent_FIELDMarksNullableExtraUniqueSecondaryIndex(t *testing.T) {
	ctx := testCtx(t)
	vp, _ := testVPlayer(t)
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2

	tableName := "parallel_apply_field_nullable_unique_secondary_idx"
	qualifiedTableName := vrepldb + "." + tableName
	execStatements(t, []string{
		"create table " + qualifiedTableName + " (id int not null, email varchar(128) null, primary key(id), unique key uk_email(email))",
	})
	t.Cleanup(func() {
		execStatements(t, []string{"drop table if exists " + qualifiedTableName})
	})

	realDB := &realDBClient{nolog: true}
	require.NoError(t, realDB.Connect())
	t.Cleanup(realDB.Close)

	vp.vr.dbClient = newVDBClient(realDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	vp.dbClient = vp.vr.dbClient
	vp.vr.mysqld = &infoSchemaMysqld{MysqlDaemon: env.Mysqld}
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	vp.vr.source.Filter = &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: tableName}}}

	colInfoMap, err := vp.vr.buildColInfoMap(ctx)
	require.NoError(t, err)
	vp.vr.colInfoMap = colInfoMap

	vp.replicatorPlan, err = vp.vr.buildReplicatorPlan(
		vp.vr.source,
		vp.vr.colInfoMap,
		nil,
		vp.vr.stats,
		vp.vr.vre.env.CollationEnv(),
		vp.vr.vre.env.Parser(),
	)
	require.NoError(t, err)

	require.NoError(t, vp.applyEvent(ctx, &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: tableName,
			Fields: []*querypb.Field{
				{Name: "id", Type: querypb.Type_INT32},
				{Name: "email", Type: querypb.Type_VARCHAR},
			},
		},
	}, false))
	require.NoError(t, vp.dbClient.Rollback())

	require.True(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)
}

func TestApplyEvent_FIELDAfterExecutedDDLRefreshesUniqueSecondaryLookup(t *testing.T) {
	ctx := testCtx(t)
	vp, _ := testVPlayer(t)
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC

	tableName := "parallel_apply_field_refresh_after_unique_ddl"
	qualifiedTableName := vrepldb + "." + tableName
	execStatements(t, []string{
		"create table " + qualifiedTableName + " (id int not null, email varchar(128) not null, primary key(id), key idx_email(email))",
	})
	t.Cleanup(func() {
		execStatements(t, []string{"drop table if exists " + qualifiedTableName})
	})

	realDB := &realDBClient{nolog: true}
	require.NoError(t, realDB.Connect())
	t.Cleanup(realDB.Close)

	vp.vr.dbClient = newVDBClient(realDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	vp.dbClient = vp.vr.dbClient
	vp.vr.mysqld = &infoSchemaMysqld{MysqlDaemon: env.Mysqld}
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	vp.vr.source.Filter = &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: tableName}}}

	colInfoMap, err := vp.vr.buildColInfoMap(ctx)
	require.NoError(t, err)
	vp.vr.colInfoMap = colInfoMap

	vp.replicatorPlan, err = vp.vr.buildReplicatorPlan(
		vp.vr.source,
		vp.vr.colInfoMap,
		nil,
		vp.vr.stats,
		vp.vr.vre.env.CollationEnv(),
		vp.vr.vre.env.Parser(),
	)
	require.NoError(t, err)

	fieldEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: tableName,
			Fields: []*querypb.Field{
				{Name: "id", Type: querypb.Type_INT32},
				{Name: "email", Type: querypb.Type_VARCHAR},
			},
		},
	}
	require.NoError(t, vp.applyEvent(ctx, fieldEvent, false))
	require.NoError(t, vp.dbClient.Rollback())
	require.False(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)

	ddlEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_DDL,
		Statement: "alter table " + tableName + " add unique key uk_email(email)",
		Timestamp: 100,
	}
	execStatements(t, []string{"alter table " + qualifiedTableName + " add unique key uk_email(email)"})
	publishExecutedDDLBarrier(t, vp, ddlEvent.Statement)

	require.NoError(t, vp.applyEvent(ctx, fieldEvent, false))
	require.NoError(t, vp.dbClient.Rollback())
	require.True(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)

	vp.vr.mysqld = nil

	require.NoError(t, vp.applyEvent(ctx, fieldEvent, false))
	require.NoError(t, vp.dbClient.Rollback())
	require.True(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)
}

func TestWorkerLoop_FIELDRefreshesPublishedDDLBarrierState(t *testing.T) {
	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()

	vp, _ := testVPlayer(t)
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2
	vp.vr.source.OnDdl = binlogdatapb.OnDDLAction_EXEC

	tableName := "parallel_apply_worker_field_refresh_after_unique_ddl"
	qualifiedTableName := vrepldb + "." + tableName
	execStatements(t, []string{
		"create table " + qualifiedTableName + " (id int not null, email varchar(128) not null, primary key(id), key idx_email(email))",
	})
	t.Cleanup(func() {
		execStatements(t, []string{"drop table if exists " + qualifiedTableName})
	})

	realDB := &realDBClient{nolog: true}
	require.NoError(t, realDB.Connect())
	t.Cleanup(realDB.Close)

	vp.vr.dbClient = newVDBClient(realDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	vp.dbClient = vp.vr.dbClient
	vp.vr.mysqld = &infoSchemaMysqld{MysqlDaemon: env.Mysqld}
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	vp.vr.source.Filter = &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: tableName}}}

	colInfoMap, err := vp.vr.buildColInfoMap(ctx)
	require.NoError(t, err)
	vp.vr.colInfoMap = colInfoMap

	vp.replicatorPlan, err = vp.vr.buildReplicatorPlan(
		vp.vr.source,
		vp.vr.colInfoMap,
		nil,
		vp.vr.stats,
		vp.vr.vre.env.CollationEnv(),
		vp.vr.vre.env.Parser(),
	)
	require.NoError(t, err)

	fieldEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: tableName,
			Fields: []*querypb.Field{
				{Name: "id", Type: querypb.Type_INT32},
				{Name: "email", Type: querypb.Type_VARCHAR},
			},
		},
	}
	require.NoError(t, vp.applyEvent(ctx, fieldEvent, false))
	require.NoError(t, vp.dbClient.Rollback())
	require.False(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)

	workerDB := &recordingDBClient{}
	workerClient := newVDBClient(workerDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	worker := &applyWorker{
		ctx:    ctx,
		conns:  [2]*vdbClient{workerClient, workerClient},
		client: workerClient,
		query: func(ctx context.Context, sql string) (*sqltypes.Result, error) {
			return &sqltypes.Result{}, nil
		},
		commit: func() error {
			return nil
		},
	}

	scheduler := newApplyScheduler(ctx)
	commitCh := make(chan *applyTxn, 2)
	errCh := make(chan error, 1)
	go func() {
		errCh <- vp.workerLoop(ctx, scheduler, commitCh, worker)
	}()

	commitOnlyTxn := &applyTxn{
		order: 1,
		payload: &applyTxnPayload{
			commitOnly: true,
			events: []*binlogdatapb.VEvent{{
				Type: binlogdatapb.VEventType_OTHER,
			}},
		},
	}
	require.NoError(t, scheduler.enqueue(commitOnlyTxn))
	require.Same(t, commitOnlyTxn, <-commitCh)
	require.NoError(t, scheduler.markCommitted(commitOnlyTxn))

	execStatements(t, []string{"alter table " + qualifiedTableName + " add unique key uk_email(email)"})
	publishExecutedDDLBarrier(t, vp, "alter table "+tableName+" add unique key uk_email(email)")

	fieldTxn := &applyTxn{
		order:      2,
		noConflict: true,
		payload: &applyTxnPayload{
			events: []*binlogdatapb.VEvent{fieldEvent},
		},
	}
	require.NoError(t, scheduler.enqueue(fieldTxn))
	require.Same(t, fieldTxn, <-commitCh)

	assert.True(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

func TestWorkerLoop_FIELDDoesNotMutatePublishedDroppedTablesBeforeCommit(t *testing.T) {
	ctx := testCtx(t)
	vp, _ := testVPlayer(t)
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2

	tableName := "parallel_apply_worker_field_does_not_clear_dropped_state"
	qualifiedTableName := vrepldb + "." + tableName
	execStatements(t, []string{
		"create table " + qualifiedTableName + " (id int not null, primary key(id))",
	})
	t.Cleanup(func() {
		execStatements(t, []string{"drop table if exists " + qualifiedTableName})
	})

	realDB := &realDBClient{nolog: true}
	require.NoError(t, realDB.Connect())
	t.Cleanup(realDB.Close)

	vp.vr.dbClient = newVDBClient(realDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	vp.dbClient = vp.vr.dbClient
	vp.vr.mysqld = &infoSchemaMysqld{MysqlDaemon: env.Mysqld}
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	vp.vr.source.Filter = &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: tableName}}}

	colInfoMap, err := vp.vr.buildColInfoMap(ctx)
	require.NoError(t, err)
	vp.vr.colInfoMap = colInfoMap

	vp.replicatorPlan, err = vp.vr.buildReplicatorPlan(
		vp.vr.source,
		vp.vr.colInfoMap,
		nil,
		vp.vr.stats,
		vp.vr.vre.env.CollationEnv(),
		vp.vr.vre.env.Parser(),
	)
	require.NoError(t, err)

	fieldEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: tableName,
			Fields:    []*querypb.Field{{Name: "id", Type: querypb.Type_INT32}},
		},
	}
	require.NoError(t, vp.applyEvent(ctx, fieldEvent, false))
	require.NoError(t, vp.dbClient.Rollback())

	vp.postDDLDroppedTables = map[string]struct{}{tableName: {}}
	vp2 := *vp
	vp2.postDDLDroppedTables = cloneDroppedTables(vp.postDDLDroppedTables)

	require.NoError(t, vp2.applyEvent(ctx, fieldEvent, false))
	require.NoError(t, vp.dbClient.Rollback())
	assert.Contains(t, vp.postDDLDroppedTables, tableName)
}

func TestApplyEvent_FIELDRefreshTargetInvalidatesUniqueSecondaryCache(t *testing.T) {
	ctx := testCtx(t)
	vp, _ := testVPlayer(t)
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2

	tableName := "parallel_apply_field_refresh_target_unique_secondary_idx"
	qualifiedTableName := vrepldb + "." + tableName
	execStatements(t, []string{
		"create table " + qualifiedTableName + " (id int not null, email varchar(128) not null, primary key(id), unique key uk_email(email))",
	})
	t.Cleanup(func() {
		execStatements(t, []string{"drop table if exists " + qualifiedTableName})
	})

	realDB := &realDBClient{nolog: true}
	require.NoError(t, realDB.Connect())
	t.Cleanup(realDB.Close)

	vp.vr.dbClient = newVDBClient(realDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	vp.dbClient = vp.vr.dbClient
	vp.vr.mysqld = &infoSchemaMysqld{MysqlDaemon: env.Mysqld}
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	vp.vr.source.Filter = &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: tableName}}}

	colInfoMap, err := vp.vr.buildColInfoMap(ctx)
	require.NoError(t, err)
	vp.vr.colInfoMap = colInfoMap

	vp.replicatorPlan, err = vp.vr.buildReplicatorPlan(
		vp.vr.source,
		vp.vr.colInfoMap,
		nil,
		vp.vr.stats,
		vp.vr.vre.env.CollationEnv(),
		vp.vr.vre.env.Parser(),
	)
	require.NoError(t, err)

	cachedPlan := &TablePlan{
		TargetName:              tableName,
		Fields:                  []*querypb.Field{{Name: "id", Type: querypb.Type_INT32}, {Name: "email", Type: querypb.Type_VARCHAR}},
		PKIndices:               []bool{true, false},
		HasExtraUniqueSecondary: false,
	}
	vp.tablePlans[tableName] = cachedPlan
	vp.postDDLStalePlans = map[string]postDDLStalePlan{
		"old_" + tableName: {
			stalePlan:      &TablePlan{TargetName: "old_" + tableName},
			refreshedPlans: map[string]*TablePlan{tableName: cachedPlan},
		},
	}

	require.NoError(t, vp.applyEvent(ctx, &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: tableName,
			Fields: []*querypb.Field{
				{Name: "id", Type: querypb.Type_INT32},
				{Name: "email", Type: querypb.Type_VARCHAR},
			},
		},
	}, false))
	require.NoError(t, vp.dbClient.Rollback())

	require.True(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)
}

func TestApplyEvent_FIELDRefreshTargetInvalidatesUniqueSecondaryCacheAcrossMultipleBarriers(t *testing.T) {
	ctx := testCtx(t)
	vp, _ := testVPlayer(t)
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2

	tableName := "pa_field_refresh_multi_barrier_uniq_idx"
	qualifiedTableName := vrepldb + "." + tableName
	execStatements(t, []string{
		"create table " + qualifiedTableName + " (id int not null, email varchar(128) not null, primary key(id), unique key uk_email(email))",
	})
	t.Cleanup(func() {
		execStatements(t, []string{"drop table if exists " + qualifiedTableName})
	})

	realDB := &realDBClient{nolog: true}
	require.NoError(t, realDB.Connect())
	t.Cleanup(realDB.Close)

	vp.vr.dbClient = newVDBClient(realDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	vp.dbClient = vp.vr.dbClient
	vp.vr.mysqld = &infoSchemaMysqld{MysqlDaemon: env.Mysqld}
	vp.vr.vre = &Engine{env: vtenv.NewTestEnv()}
	vp.vr.source.Filter = &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: tableName}}}

	colInfoMap, err := vp.vr.buildColInfoMap(ctx)
	require.NoError(t, err)
	vp.vr.colInfoMap = colInfoMap

	vp.replicatorPlan, err = vp.vr.buildReplicatorPlan(
		vp.vr.source,
		vp.vr.colInfoMap,
		nil,
		vp.vr.stats,
		vp.vr.vre.env.CollationEnv(),
		vp.vr.vre.env.Parser(),
	)
	require.NoError(t, err)

	cachedPlan := &TablePlan{
		TargetName:              tableName,
		Fields:                  []*querypb.Field{{Name: "id", Type: querypb.Type_INT32}, {Name: "email", Type: querypb.Type_VARCHAR}},
		PKIndices:               []bool{true, false},
		HasExtraUniqueSecondary: false,
	}
	vp.tablePlans[tableName] = cachedPlan
	vp.postDDLStalePlans = map[string]postDDLStalePlan{
		"old_" + tableName: {
			stalePlan:      &TablePlan{TargetName: "old_" + tableName},
			refreshedPlans: map[string]*TablePlan{tableName: {TargetName: tableName}},
		},
		"other_old_" + tableName: {
			stalePlan:      cachedPlan,
			refreshedPlans: map[string]*TablePlan{tableName: cachedPlan},
		},
	}

	require.NoError(t, vp.applyEvent(ctx, &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: tableName,
			Fields: []*querypb.Field{
				{Name: "id", Type: querypb.Type_INT32},
				{Name: "email", Type: querypb.Type_VARCHAR},
			},
		},
	}, false))
	require.NoError(t, vp.dbClient.Rollback())

	require.True(t, vp.tablePlans[tableName].HasExtraUniqueSecondary)
}

func TestApplyEvent_FIELDWithoutParallelApplySkipsUniqueSecondaryLookup(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)
	vp.vr.workflowConfig.ParallelReplicationWorkers = 1
	mockDB.AddInvariant("begin", &sqltypes.Result{})

	vp.replicatorPlan = &ReplicatorPlan{TablePlans: map[string]*TablePlan{
		"t1": {
			TargetName:       "t1",
			IdentityColumns:  []string{"id"},
			Insert:           sqlparser.BuildParsedQuery("insert into t1 values (:a)"),
			TablePlanBuilder: &tablePlanBuilder{},
		},
	}}

	require.NoError(t, vp.applyEvent(ctx, &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: "t1",
			Fields: []*querypb.Field{
				{Name: "id", Type: querypb.Type_INT64},
			},
		},
	}, false))

	require.Contains(t, vp.tablePlans, "t1")
	require.False(t, vp.tablePlans["t1"].HasExtraUniqueSecondary)
}

func TestApplyEvent_VERSIONIsIgnored(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)

	gtid := "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
	require.NoError(t, vp.applyEvent(ctx, &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_GTID, Gtid: gtid}, false))

	versionEvent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_VERSION, Timestamp: 100}
	require.NoError(t, vp.applyEvent(ctx, versionEvent, false))
	require.False(t, vp.dbClient.InTransaction)
	require.Nil(t, vp.unsavedEvent)

	commitEvent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_COMMIT, Timestamp: 100}
	require.NoError(t, vp.applyEvent(ctx, commitEvent, false))
	require.Equal(t, commitEvent, vp.unsavedEvent)
}

func TestScheduleItems_FIELDEventDoesNotForceGlobal(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	// FIELD events are metadata (table definitions). They should NOT force
	// global serialization — they are harmless for conflict detection and
	// just need to be applied before the ROW events that follow.
	fieldEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_FIELD,
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, fieldEvent, commitEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	// FIELD events have an explicit handler that does NOT set curRowOnly=false,
	// so the transaction is scheduled normally with an empty writeset (noConflict).
	assert.False(t, got.forceGlobal)
}

func TestScheduleItems_UnknownVEventTypeFailsFast(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	err := vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType(12345)},
		{Type: binlogdatapb.VEventType_COMMIT},
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported vevent type")
}

func TestScheduleItems_TimestampTracking(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	gtidEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_GTID,
		Gtid:      "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		Timestamp: 50,
	}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "t1",
			RowChanges: []*binlogdatapb.RowChange{
				{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}},
			},
		},
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, rowEvent, commitEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	// Timestamp from the ROW event should be tracked
	assert.Equal(t, int64(100), got.payload.timestamp)
}

func TestScheduleItems_WritesetBuild(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "t1",
			RowChanges: []*binlogdatapb.RowChange{
				{After: &querypb.Row{Values: []byte("42"), Lengths: []int64{2}}},
			},
		},
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, rowEvent, commitEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.False(t, got.forceGlobal)
	assert.Contains(t, got.payload.events[0].RowEvent.TableName, "t1")
	// Writeset should contain PK-based key
	require.Len(t, got.writeset, 1)
	expected := testWritesetHash("t1", sqltypes.MakeTrusted(querypb.Type_INT64, []byte("42")))
	assert.Equal(t, expected, got.writeset[0])
}

func TestScheduleItems_MissingTablePlanReturnsWritesetError(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	// No table plan for "t1" — writeset build should fail closed.
	vp.tablePlansVersion.Store(1)

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "t1",
			RowChanges: []*binlogdatapb.RowChange{
				{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}},
			},
		},
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, rowEvent, commitEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	assert.Contains(t, err.Error(), "missing table plan for t1")
}

func TestScheduleItems_PartialRowImageReturnsWritesetError(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields: []*querypb.Field{
			{Name: "a", Type: querypb.Type_INT64},
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "b", Type: querypb.Type_INT64},
		},
		PKIndices: []bool{false, true, false},
	}
	vp.tablePlansVersion.Store(1)

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: "t1",
			RowChanges: []*binlogdatapb.RowChange{{
				After: &querypb.Row{Values: []byte("23"), Lengths: []int64{1, 1}},
				DataColumns: &binlogdatapb.RowChange_Bitmap{
					Count: 3,
					Cols:  []byte{0x06},
				},
			}},
		},
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	err := vp.scheduleItems(ctx, scheduler, state, [][]*binlogdatapb.VEvent{{gtidEvent, rowEvent, commitEvent}})
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	assert.Contains(t, err.Error(), "partial row image on table t1")

	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	assert.Zero(t, scheduler.pendingCount)
	assert.Zero(t, scheduler.inflightGlobal)
	assert.Zero(t, scheduler.inflightMissingMeta)
	assert.Zero(t, scheduler.inflightCommitMeta)
	assert.Empty(t, scheduler.pending)
	assert.Empty(t, scheduler.inflightWriteset)
}

func TestScheduleItems_CommitMeta(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	gtidEvent := &binlogdatapb.VEvent{
		Type:           binlogdatapb.VEventType_GTID,
		Gtid:           "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		SequenceNumber: 10,
		CommitParent:   9,
	}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		},
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, rowEvent, commitEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.hasCommitMeta)
	assert.Equal(t, int64(10), got.sequenceNumber)
	assert.Equal(t, int64(9), got.commitParent)
}

func TestScheduleItems_BatchingMixedCommitMetaStaysMissingMeta(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}
	vp.vr.workflowConfig.ParallelReplicationWorkers = 2
	state.maxBatchedCommits = 2

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	items := [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 100},
		{Type: binlogdatapb.VEventType_COMMIT},
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6", SequenceNumber: 11, CommitParent: 10},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("2"), Lengths: []int64{1}}}},
		}, Timestamp: 200},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}

	require.NoError(t, vp.scheduleItems(ctx, scheduler, state, items))

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.hasCommitMeta)
	assert.Zero(t, got.sequenceNumber)
	assert.Zero(t, got.commitParent)
	assert.Len(t, got.payload.events, 2)
	assert.NotNil(t, got.writeset)
	require.NoError(t, scheduler.markCommitted(got))
	scheduler.mu.Lock()
	assert.Equal(t, int64(11), scheduler.lastCommittedSequence)
	scheduler.mu.Unlock()
}

func TestScheduleItems_HeartbeatSetsMustSave(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.vr.workflowConfig.HeartbeatUpdateInterval = math.MaxInt

	vp.numAccumulatedHeartbeats = 1

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	// recordHeartbeat() calls vr.stats.RecordHeartbeat (no DB) then
	// mustUpdateHeartbeat() → false (numAccumulatedHeartbeats=0), so no DB call.
	_ = mockDB

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		},
		Timestamp: 100,
	}
	heartbeatEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_HEARTBEAT,
		Timestamp: 200,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	// GTID, ROW, HEARTBEAT, COMMIT — heartbeat should set curMustSave
	// because there are accumulated events when heartbeat arrives
	items := [][]*binlogdatapb.VEvent{{gtidEvent, rowEvent, heartbeatEvent, commitEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)
	// The heartbeat forced curMustSave=true, which means the transaction was flushed
	// even if batching would otherwise accumulate it
	assert.Equal(t, int64(1), got.order)
}

func TestScheduleItems_BatchingSkipsFlushWhenAnotherCommitAhead(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	// Two transactions in same batch — first COMMIT should be skipped (batched)
	// since another COMMIT follows
	items := [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 100},
		{Type: binlogdatapb.VEventType_COMMIT},
		// Second transaction in same batch
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("2"), Lengths: []int64{1}}}},
		}, Timestamp: 200},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}

	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	// With batching, both transactions merge into one — only one enqueue
	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)

	// The batched transaction should have both ROW events
	assert.Len(t, got.payload.events, 2)
	assert.Equal(t, int64(1), got.order)
}

func TestScheduleItems_FKRefsDisableBatching(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields: []*querypb.Field{
			{Name: "id", Type: querypb.Type_INT64},
			{Name: "parent_id", Type: querypb.Type_INT64},
		},
		PKIndices: []bool{true, false},
	}
	vp.tablePlansVersion.Store(1)

	// Set FK refs — this should disable batching
	vp.fkRefs = map[string][]fkConstraintRef{
		"t1": {{ParentTable: "parent", ChildColumnNames: []string{"parent_id"}}},
	}

	// Two transactions in same batch
	items := [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("112"), Lengths: []int64{1, 2}}}},
		}, Timestamp: 100},
		{Type: binlogdatapb.VEventType_COMMIT},
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("223"), Lengths: []int64{1, 2}}}},
		}, Timestamp: 200},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}

	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	// With FK refs, batching is disabled — two separate transactions
	got1, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.NoError(t, scheduler.markCommitted(got1))
	got2, err := scheduler.nextReady(ctx)
	require.NoError(t, err)

	assert.Len(t, got1.payload.events, 1)
	assert.Len(t, got2.payload.events, 1)
	assert.Equal(t, int64(1), got1.order)
	assert.Equal(t, int64(2), got2.order)
}

func TestScheduleItems_BatchingMergedSequenceAdvanced(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	// Two transactions with commit meta — first gets merged, its sequence should be advanced
	items := [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", SequenceNumber: 10, CommitParent: 9},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 100},
		{Type: binlogdatapb.VEventType_COMMIT},
		// Second txn
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6", SequenceNumber: 11, CommitParent: 10},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("2"), Lengths: []int64{1}}}},
		}, Timestamp: 200},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}

	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	// First txn was merged, its sequence (10) should have been advanced
	scheduler.mu.Lock()
	seq := scheduler.lastCommittedSequence
	scheduler.mu.Unlock()
	assert.GreaterOrEqual(t, seq, int64(10))
}

func TestScheduleItems_StopPosSetsMustSave(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	// Set a stop position
	stopPos, err := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	require.NoError(t, err)
	vp.stopPos = stopPos

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10", // at or past stopPos
	}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		},
		Timestamp: 100,
	}
	commitEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, rowEvent, commitEvent}}
	err = vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	got, gerr := scheduler.nextReady(ctx)
	require.NoError(t, gerr)
	require.NotNil(t, got)
	assert.True(t, got.payload.mustSave)
}

func TestScheduleItems_HeartbeatUpdatesLag(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.vr.workflowConfig.HeartbeatUpdateInterval = math.MaxInt

	vp.numAccumulatedHeartbeats = 1

	hbEvent := &binlogdatapb.VEvent{
		Type:        binlogdatapb.VEventType_HEARTBEAT,
		Timestamp:   100,
		CurrentTime: time.Now().UnixNano(),
	}

	items := [][]*binlogdatapb.VEvent{{hbEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	assert.Equal(t, int64(100*1e9), vp.lastTimestampNs.Load())
	assert.Equal(t, 2, vp.numAccumulatedHeartbeats)
}

func TestScheduleItems_ThrottledHeartbeatEstimatesLag(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.vr.workflowConfig.HeartbeatUpdateInterval = math.MaxInt
	vp.vr.throttleUpdatesRateLimiter = timer.NewRateLimiter(time.Second)
	t.Cleanup(vp.vr.throttleUpdatesRateLimiter.Stop)

	vp.numAccumulatedHeartbeats = 1

	// Set last known timestamp so estimateLag works
	vp.lastTimestampNs.Store(time.Now().Add(-5 * time.Second).UnixNano())
	vp.timeOffsetNs.Store(0)

	// updateTimeThrottled calls dbClient.ExecuteFetch
	mockDB.AddInvariant("update _vt.vreplication set", &sqltypes.Result{})

	hbEvent := &binlogdatapb.VEvent{
		Type:            binlogdatapb.VEventType_HEARTBEAT,
		Timestamp:       100,
		CurrentTime:     time.Now().UnixNano(),
		Throttled:       true,
		ThrottledReason: "test",
	}

	items := [][]*binlogdatapb.VEvent{{hbEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	// Lag should be estimated (non-zero)
	lag := vp.vr.stats.ReplicationLagSeconds.Load()
	assert.GreaterOrEqual(t, lag, int64(4))
}

// ---------- commitLoop tests ----------

func TestCommitLoop_InOrderCommit(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)

	// commitLoop calls commitTxn → updatePos → vp.query/commit on each txn.
	// For commitOnly+updatePosOnly, it calls vp.updatePos which calls
	// vp.query (binlogplayer.GenerateUpdatePos).
	// We mock the DB to accept any update/commit.
	mockDB.AddInvariant("update _vt.vreplication set pos=", &sqltypes.Result{})
	mockDB.AddInvariant("commit", &sqltypes.Result{})
	mockDB.AddInvariant("begin", &sqltypes.Result{})

	commitCh := make(chan *applyTxn, 3)

	pos1, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")

	// Send 3 transactions in order
	for i := int64(1); i <= 3; i++ {
		txn := &applyTxn{
			order: i,
			payload: &applyTxnPayload{
				pos:                pos1,
				timestamp:          100 * i,
				commitOnly:         true,
				updatePosOnly:      true,
				lastEventTimestamp: 100 * i,
			},
			done: make(chan struct{}),
		}
		commitCh <- txn
	}
	close(commitCh)

	err := vp.commitLoop(ctx, scheduler, commitCh)
	require.NoError(t, err)
}

func TestCommitLoop_OutOfOrderReordering(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)

	mockDB.AddInvariant("update _vt.vreplication set pos=", &sqltypes.Result{})
	mockDB.AddInvariant("commit", &sqltypes.Result{})
	mockDB.AddInvariant("begin", &sqltypes.Result{})

	commitCh := make(chan *applyTxn, 3)

	pos1, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")

	// Send transactions out of order: 2, 1, 3
	for _, order := range []int64{2, 1, 3} {
		txn := &applyTxn{
			order: order,
			payload: &applyTxnPayload{
				pos:                pos1,
				timestamp:          100 * order,
				commitOnly:         true,
				updatePosOnly:      true,
				lastEventTimestamp: 100 * order,
			},
			done: make(chan struct{}),
		}
		commitCh <- txn
	}
	close(commitCh)

	err := vp.commitLoop(ctx, scheduler, commitCh)
	require.NoError(t, err)
}

func TestCommitLoop_ZeroOrderNoReordering(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)

	mockDB.AddInvariant("update _vt.vreplication set pos=", &sqltypes.Result{})
	mockDB.AddInvariant("commit", &sqltypes.Result{})
	mockDB.AddInvariant("begin", &sqltypes.Result{})

	commitCh := make(chan *applyTxn, 1)

	pos1, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")

	// Order=0 → committed immediately without reordering
	txn := &applyTxn{
		order: 0,
		payload: &applyTxnPayload{
			pos:                pos1,
			timestamp:          100,
			commitOnly:         true,
			updatePosOnly:      true,
			lastEventTimestamp: 100,
		},
		done: make(chan struct{}),
	}
	commitCh <- txn
	close(commitCh)

	err := vp.commitLoop(ctx, scheduler, commitCh)
	require.NoError(t, err)
}

func TestCommitLoop_PendingLeftover(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)

	mockDB.AddInvariant("update _vt.vreplication set pos=", &sqltypes.Result{})
	mockDB.AddInvariant("commit", &sqltypes.Result{})
	mockDB.AddInvariant("begin", &sqltypes.Result{})

	commitCh := make(chan *applyTxn, 2)
	pos1, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")

	// Send order 3 and 1, but no order 2 → should error about missing order
	for _, order := range []int64{3, 1} {
		txn := &applyTxn{
			order: order,
			payload: &applyTxnPayload{
				pos:                pos1,
				timestamp:          100,
				commitOnly:         true,
				updatePosOnly:      true,
				lastEventTimestamp: 100,
			},
			done: make(chan struct{}),
		}
		commitCh <- txn
	}
	close(commitCh)

	err := vp.commitLoop(ctx, scheduler, commitCh)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parallel apply commit missing order")
}

func TestCommitLoop_MarksCommittedOnScheduler(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)

	mockDB.AddInvariant("update _vt.vreplication set pos=", &sqltypes.Result{})
	mockDB.AddInvariant("commit", &sqltypes.Result{})
	mockDB.AddInvariant("begin", &sqltypes.Result{})

	commitCh := make(chan *applyTxn, 1)
	pos1, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")

	txn := &applyTxn{
		order:          1,
		sequenceNumber: 7,
		hasCommitMeta:  true,
		payload: &applyTxnPayload{
			pos:                pos1,
			timestamp:          100,
			commitOnly:         true,
			updatePosOnly:      true,
			lastEventTimestamp: 100,
		},
		done: make(chan struct{}),
	}
	commitCh <- txn
	close(commitCh)

	err := vp.commitLoop(ctx, scheduler, commitCh)
	require.NoError(t, err)

	// markCommitted should have advanced lastCommittedSequence
	scheduler.mu.Lock()
	assert.Equal(t, int64(7), scheduler.lastCommittedSequence)
	scheduler.mu.Unlock()

	// lastCommittedOrder should be 1
	scheduler.mu.Lock()
	assert.Equal(t, int64(1), scheduler.lastCommittedOrder)
	scheduler.mu.Unlock()
}

func TestCommitLoop_UpdatesLag(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)

	mockDB.AddInvariant("update _vt.vreplication set pos=", &sqltypes.Result{})
	mockDB.AddInvariant("commit", &sqltypes.Result{})
	mockDB.AddInvariant("begin", &sqltypes.Result{})

	commitCh := make(chan *applyTxn, 1)
	pos1, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")

	now := time.Now()
	txn := &applyTxn{
		order: 1,
		payload: &applyTxnPayload{
			pos:                  pos1,
			timestamp:            100,
			commitOnly:           true,
			updatePosOnly:        true,
			lastEventTimestamp:   now.Add(-3 * time.Second).Unix(),
			lastEventCurrentTime: now.UnixNano(),
		},
		done: make(chan struct{}),
	}
	commitCh <- txn
	close(commitCh)

	err := vp.commitLoop(ctx, scheduler, commitCh)
	require.NoError(t, err)

	// Lag should be approximately 3 seconds
	lag := vp.vr.stats.ReplicationLagSeconds.Load()
	assert.GreaterOrEqual(t, lag, int64(2))
	assert.LessOrEqual(t, lag, int64(5))
}

func TestCommitLoop_CommitOnlyAppliesEvent(t *testing.T) {
	ctx := testCtx(t)
	vp, mockDB := testVPlayer(t)
	scheduler := newApplyScheduler(ctx)

	mockDB.AddInvariant("update _vt.vreplication set", &sqltypes.Result{})
	mockDB.AddInvariant("commit", &sqltypes.Result{})
	mockDB.AddInvariant("begin", &sqltypes.Result{})
	mockDB.AddInvariant("insert", &sqltypes.Result{})

	commitCh := make(chan *applyTxn, 1)
	pos1, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")

	heartbeatEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_HEARTBEAT,
		Timestamp: 100,
	}

	txn := &applyTxn{
		order: 1,
		payload: &applyTxnPayload{
			pos:                pos1,
			timestamp:          100,
			commitOnly:         true,
			updatePosOnly:      false,
			mustSave:           true,
			events:             []*binlogdatapb.VEvent{heartbeatEvent},
			lastEventTimestamp: 100,
		},
		done: make(chan struct{}),
	}
	commitCh <- txn
	close(commitCh)

	err := vp.commitLoop(ctx, scheduler, commitCh)
	require.NoError(t, err)
}

func TestCommitLoop_UpdatePosOnlyStopPosReached(t *testing.T) {
	ctx := testCtx(t)
	vp, mockDB := testVPlayer(t)
	scheduler := newApplyScheduler(ctx)

	mockDB.AddInvariant("update _vt.vreplication set", &sqltypes.Result{})
	mockDB.AddInvariant("commit", &sqltypes.Result{})
	mockDB.AddInvariant("begin", &sqltypes.Result{})

	stopPos, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	vp.stopPos = stopPos

	commitCh := make(chan *applyTxn, 1)
	pos1, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")

	txn := &applyTxn{
		order: 1,
		payload: &applyTxnPayload{
			pos:                pos1,
			timestamp:          100,
			commitOnly:         true,
			updatePosOnly:      true,
			mustSave:           true,
			lastEventTimestamp: 100,
		},
		done: make(chan struct{}),
	}
	commitCh <- txn
	close(commitCh)

	err := vp.commitLoop(ctx, scheduler, commitCh)
	require.ErrorIs(t, err, io.EOF)
}

func TestCommitTxn(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)

	calledCommit := 0
	vp.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
		return &sqltypes.Result{}, nil
	}
	vp.commit = func() error {
		calledCommit++
		return nil
	}

	posReached, err := vp.commitTxn(ctx, &applyTxnPayload{timestamp: 123})
	require.NoError(t, err)
	require.False(t, posReached)
	require.Equal(t, 1, calledCommit)
}

func TestCommitTxnWorkerRestoresSaveStop(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)

	vp.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
		return &sqltypes.Result{}, nil
	}
	vp.commit = func() error {
		return nil
	}

	vp.saveStop = true

	payload := &applyTxnPayload{timestamp: 123, client: &vdbClient{}}
	posReached, err := vp.commitTxn(ctx, payload)
	require.NoError(t, err)
	require.False(t, posReached)
	require.True(t, vp.saveStop)
}

func TestCommitTxnWorkerSetsStateOnStopPos(t *testing.T) {
	vp, mockDB := testVPlayer(t)
	ctx := testCtx(t)

	mockDB.AddInvariant("update _vt.vreplication set state=", &sqltypes.Result{})

	pos, err := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	require.NoError(t, err)
	vrClient := vp.vr.dbClient
	workerClient := newVDBClient(binlogplayer.NewMockDBClient(t), vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)

	vp.pos = pos
	vp.stopPos = pos
	vp.saveStop = true
	vp.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
		return &sqltypes.Result{}, nil
	}
	vp.commit = func() error {
		return nil
	}

	payload := &applyTxnPayload{timestamp: 123, client: workerClient}
	posReached, err := vp.commitTxn(ctx, payload)
	require.NoError(t, err)
	require.True(t, posReached)
	require.Equal(t, vrClient, vp.vr.dbClient)
}

// ---------- enqueueCommitOnly tests ----------

func TestEnqueueCommitOnly(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)

	// Set up a known position
	pos1, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	vp.serialMu.Lock()
	vp.pos = pos1
	vp.serialMu.Unlock()

	event := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_COMMIT,
		Timestamp: 200,
	}

	err := vp.enqueueCommitOnly(ctx, scheduler, event, true, true)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.forceGlobal)
	assert.True(t, got.noConflict)
	assert.True(t, got.payload.commitOnly)
	assert.True(t, got.payload.updatePosOnly)
	assert.True(t, got.payload.mustSave)
	assert.Equal(t, int64(200), got.payload.timestamp)
}

func TestEnqueueCommitOnly_NotUpdatePosOnly(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)

	pos1, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	vp.serialMu.Lock()
	vp.pos = pos1
	vp.serialMu.Unlock()

	event := &binlogdatapb.VEvent{
		Type:           binlogdatapb.VEventType_COMMIT,
		Timestamp:      200,
		SequenceNumber: 5,
		CommitParent:   4,
	}

	err := vp.enqueueCommitOnly(ctx, scheduler, event, false, false)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	assert.False(t, got.noConflict) // updatePosOnly=false → noConflict=false
	assert.False(t, got.payload.mustSave)
	assert.False(t, got.payload.updatePosOnly)
	assert.True(t, got.hasCommitMeta) // SequenceNumber=5
	assert.Equal(t, int64(5), got.sequenceNumber)
}

func TestEnqueueCommitOnly_IncrementsOrder(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)

	event := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_COMMIT, Timestamp: 100}

	require.NoError(t, vp.enqueueCommitOnly(ctx, scheduler, event, true, true))
	require.NoError(t, vp.enqueueCommitOnly(ctx, scheduler, event, true, true))

	got1, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	got2, err := scheduler.nextReady(ctx)
	require.NoError(t, err)

	assert.Equal(t, int64(1), got1.order)
	assert.Equal(t, int64(2), got2.order)
}

// ---------- workerLoop tests ----------

func TestWorkerLoop_CommitOnlyBypassesApply(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	scheduler := newApplyScheduler(ctx)
	commitCh := make(chan *applyTxn, 1)

	pos1, _ := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")

	// Enqueue a commitOnly transaction
	payload := &applyTxnPayload{
		pos:                pos1,
		commitOnly:         true,
		timestamp:          100,
		lastEventTimestamp: 100,
	}
	txn := &applyTxn{
		order:   1,
		payload: payload,
	}
	require.NoError(t, scheduler.enqueue(txn))

	// Worker is nil since commitOnly doesn't need it
	worker := &applyWorker{ctx: ctx}

	// Run workerLoop in background
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- vp.workerLoop(ctx, scheduler, commitCh, worker)
	}()

	// Should forward to commitCh
	assert.Eventually(t, func() bool {
		return len(commitCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)

	got := <-commitCh
	assert.Equal(t, txn, got)

	// Cancel to stop worker loop
	cancel()

	assert.Eventually(t, func() bool {
		return len(doneCh) > 0
	}, 200*time.Millisecond, 5*time.Millisecond)
}

func TestWorkerLoop_AppliesAndDispatches(t *testing.T) {
	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()

	vp, _ := testVPlayer(t)
	scheduler := newApplyScheduler(ctx)
	commitCh := make(chan *applyTxn, 1)

	worker := &applyWorker{
		ctx: ctx,
		query: func(ctx context.Context, sql string) (*sqltypes.Result, error) {
			return &sqltypes.Result{}, nil
		},
		commit: func() error {
			return nil
		},
	}
	activeClient := newVDBClient(&recordingDBClient{}, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)
	worker.client = activeClient

	event := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"}
	payload := &applyTxnPayload{events: []*binlogdatapb.VEvent{event}}
	gotTxn := &applyTxn{order: 1, payload: payload}

	require.NoError(t, scheduler.enqueue(gotTxn))

	errCh := make(chan error, 1)
	go func() {
		errCh <- vp.workerLoop(ctx, scheduler, commitCh, worker)
	}()

	select {
	case txn := <-commitCh:
		require.NotNil(t, txn)
		assert.NotNil(t, txn.payload.query)
		assert.NotNil(t, txn.payload.commit)
		assert.Same(t, activeClient, txn.payload.client)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for commitCh")
	}

	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for workerLoop exit")
	}
}

func TestWorkerLoop_CapturesDoneChannelBeforeDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()

	vp, _ := testVPlayer(t)
	scheduler := newApplyScheduler(ctx)
	commitCh := make(chan *applyTxn)

	worker := &applyWorker{ctx: ctx}
	afterFirstSend := make(chan struct{})
	allowFirstSendReturn := make(chan struct{})
	beforeSecondWait := make(chan chan struct{}, 1)
	allowSecondWait := make(chan struct{})

	workerLoopTestHookAfterSend = func(txn *applyTxn) {
		if txn.order != 1 {
			return
		}
		close(afterFirstSend)
		<-allowFirstSendReturn
	}
	workerLoopTestHookBeforeWaitPending = func(done chan struct{}) {
		select {
		case beforeSecondWait <- done:
		default:
		}
		<-allowSecondWait
	}
	t.Cleanup(func() {
		workerLoopTestHookAfterSend = nil
		workerLoopTestHookBeforeWaitPending = nil
	})

	firstTxn := acquireApplyTxn()
	firstTxn.order = 1
	firstTxn.noConflict = true
	firstTxn.payload = &applyTxnPayload{}
	require.NoError(t, scheduler.enqueue(firstTxn))

	errCh := make(chan error, 1)
	go func() {
		errCh <- vp.workerLoop(ctx, scheduler, commitCh, worker)
	}()

	dispatchedFirst := <-commitCh
	<-afterFirstSend
	require.Same(t, firstTxn, dispatchedFirst)
	originalDone := dispatchedFirst.done

	releaseApplyTxn(dispatchedFirst)

	replacementDone := make(chan struct{}, 1)
	// Model the exact race directly: the commitLoop has released the dispatched
	// object, and immediate reuse repurposes that same struct before workerLoop
	// records pendingDone. This avoids depending on sync.Pool returning the same
	// pointer while still proving the correctness invariant.
	dispatchedFirst.done = replacementDone
	dispatchedFirst.order = 2
	dispatchedFirst.noConflict = true
	dispatchedFirst.payload = &applyTxnPayload{}
	require.NoError(t, scheduler.enqueue(dispatchedFirst))

	// Pre-signal the replacement channel. The old workerLoop ordering can latch
	// this reused channel after dispatching txn1, which incorrectly lets txn2 run
	// before txn1's original done channel is signaled.
	replacementDone <- struct{}{}
	close(allowFirstSendReturn)

	waitingOn := <-beforeSecondWait
	require.True(t, originalDone == waitingOn)
	require.True(t, replacementDone != waitingOn)

	originalDone <- struct{}{}
	close(allowSecondWait)

	dispatchedSecond := <-commitCh
	require.Same(t, dispatchedFirst, dispatchedSecond)
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

func TestWorkerLoop_ErrorRollsBack(t *testing.T) {
	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()

	vp, _ := testVPlayer(t)
	scheduler := newApplyScheduler(ctx)
	commitCh := make(chan *applyTxn, 1)

	mockDB := binlogplayer.NewMockDBClient(t)
	mockDB.AddInvariant("rollback", &sqltypes.Result{})

	worker := &applyWorker{
		ctx:    ctx,
		client: newVDBClient(mockDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems),
	}

	badEvent := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_GTID, Gtid: "invalid"}
	payload := &applyTxnPayload{events: []*binlogdatapb.VEvent{badEvent}}
	gotTxn := &applyTxn{order: 1, payload: payload}

	require.NoError(t, scheduler.enqueue(gotTxn))

	errCh := make(chan error, 1)
	go func() {
		errCh <- vp.workerLoop(ctx, scheduler, commitCh, worker)
	}()

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for workerLoop error")
	}
}

// ---------- Batch time bound test ----------

func TestScheduleItems_BatchTimeBoundForcesSave(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)

	// Set lastFlushTime to long ago to trigger the 500ms time bound
	state := &parallelScheduleState{
		lastFlushTime:        time.Now().Add(-1 * time.Second),
		lastHeartbeatRefresh: time.Now(),
	}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	// Two transactions in same batch — but time bound should force flush
	items := [][]*binlogdatapb.VEvent{{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		}, Timestamp: 100},
		{Type: binlogdatapb.VEventType_COMMIT},
		{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("2"), Lengths: []int64{1}}}},
		}, Timestamp: 200},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}

	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	// Time bound forced a flush — should have 2 separate transactions
	got1, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	got2, err := scheduler.nextReady(ctx)
	require.NoError(t, err)

	assert.Equal(t, int64(1), got1.order)
	assert.Equal(t, int64(2), got2.order)
}

// ---------- Empty txn with stop position enqueues commitOnly ----------

func TestScheduleItems_EmptyTxnWithStopPos(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	stopPos, err := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	require.NoError(t, err)
	vp.stopPos = stopPos

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10",
	}
	commitEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_COMMIT,
		Timestamp: 300,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, commitEvent}}
	err = vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	// Empty txn at/past stop pos → enqueueCommitOnly should fire
	got, gerr := scheduler.nextReady(ctx)
	require.NoError(t, gerr)
	require.NotNil(t, got)
	assert.True(t, got.forceGlobal)
	assert.True(t, got.payload.commitOnly)
}

// ---------- JOURNAL is ForceGlobal ----------

func TestScheduleItems_JOURNALIsForceGlobal(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	journalEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_JOURNAL,
		Timestamp: 200,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, journalEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	got, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	assert.True(t, got.forceGlobal)
	assert.True(t, got.payload.commitOnly)
}

// ---------- DDL after accumulated ROW events flushes first ----------

func TestScheduleItems_DDLFlushesAccumulatedEvents(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := testCtx(t)
	scheduler := newApplyScheduler(ctx)
	state := &parallelScheduleState{lastFlushTime: time.Now(), lastHeartbeatRefresh: time.Now()}

	vp.tablePlans["t1"] = &TablePlan{
		TargetName: "t1",
		Fields:     []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
		PKIndices:  []bool{true},
	}
	vp.tablePlansVersion.Store(1)

	gtidEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	}
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName:  "t1",
			RowChanges: []*binlogdatapb.RowChange{{After: &querypb.Row{Values: []byte("1"), Lengths: []int64{1}}}},
		},
		Timestamp: 100,
	}
	ddlEvent := &binlogdatapb.VEvent{
		Type:      binlogdatapb.VEventType_DDL,
		Timestamp: 200,
	}

	items := [][]*binlogdatapb.VEvent{{gtidEvent, rowEvent, ddlEvent}}
	err := vp.scheduleItems(ctx, scheduler, state, items)
	require.NoError(t, err)

	// Should have 2 transactions: the flush of ROW events, then the DDL
	got1, err := scheduler.nextReady(ctx)
	require.NoError(t, err)
	require.NoError(t, scheduler.markCommitted(got1))

	got2, err := scheduler.nextReady(ctx)
	require.NoError(t, err)

	// First is the row data flush
	assert.Len(t, got1.payload.events, 1)
	assert.Equal(t, binlogdatapb.VEventType_ROW, got1.payload.events[0].Type)

	// Second is the DDL (commitOnly, forceGlobal)
	assert.True(t, got2.forceGlobal)
	assert.Equal(t, binlogdatapb.VEventType_DDL, got2.payload.events[0].Type)
}
