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

package logic

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	cache "github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/test"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestAuditTopologyRecoveryLogs checks that a step without a recovery is
// logged without a recovery_id.
func TestAuditTopologyRecoveryLogs(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	require.NoError(t, AuditTopologyRecovery(nil, "step without a recovery"))
	assert.NotContains(t, logs.Record(t, "topology_recovery"), "recovery_id")
}

// setupRecovery prepares executeCheckAndRecoverFunction to run on the primary
// zone1-0000000101 of ks/0: the topo has the keyspace, shard and primary. It
// returns the primary.
func setupRecovery(t *testing.T) *topodatapb.Tablet {
	db.ClearVTOrcDatabase()
	t.Cleanup(db.ClearVTOrcDatabase)

	ctx := t.Context()
	primary := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
		Hostname:      "localhost",
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_PRIMARY,
		MysqlHostname: "localhost",
		MysqlPort:     6708,
	}
	oldTs, oldTmc := ts, tmc
	t.Cleanup(func() { ts, tmc = oldTs, oldTmc })
	ts = memorytopo.NewServer(ctx, "zone1")
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}))
	require.NoError(t, ts.CreateShard(ctx, "ks", "0"))
	require.NoError(t, ts.CreateTablet(ctx, primary))
	// UndoDemotePrimary has no result set, so FixPrimary fails.
	tmc = &testutil.TabletManagerClient{}

	prevKeys := recentDiscoveryOperationKeys
	recentDiscoveryOperationKeys = cache.New(config.GetInstancePollTime(), time.Second)
	t.Cleanup(func() { recentDiscoveryOperationKeys = prevKeys })

	oldDB := db.Db
	t.Cleanup(func() { db.Db = oldDB })
	// Let the timer of a failed instance read fire while db.Db is still this
	// test's, so its write can't take the backend's only connection in a later
	// test.
	t.Cleanup(waitForLastAttemptedCheck)
	return primary
}

// waitForLastAttemptedCheck waits out the timer that a failed instance read
// leaves: a second after the read started, the timer records the attempt in
// the backend, which reads VTOrc's flags. It sleeps past that second, then
// takes and releases the instance write semaphore. A write that has finished
// has released it, so this orders that write before what the caller does next
// for the race detector, which a sleep alone doesn't. It doesn't wait for a
// write that is still running.
func waitForLastAttemptedCheck() {
	time.Sleep(1200 * time.Millisecond)
	_ = inst.ExecDBWriteFunc(func() error { return nil })
}

// setRecoveryDB makes db.Db answer the queries of a recovery on primary. They
// are, in order: the detection's insert, the analysis that
// recheckPrimaryHealth runs, the shard's primary, and the analysis after the
// shard lock. Each analysis gets analysisRows.
func setRecoveryDB(t *testing.T, primary *topodatapb.Tablet, analysisRows []sqlutils.RowMap) {
	detection := []sqlutils.RowMap{{"detection_id": sqlutils.CellData{String: "1", Valid: true}}}
	primaryInfo, err := prototext.Marshal(primary)
	require.NoError(t, err)
	primaryRow := []sqlutils.RowMap{{"info": sqlutils.CellData{String: string(primaryInfo), Valid: true}}}
	db.Db = test.NewTestDB([][]sqlutils.RowMap{detection, analysisRows, primaryRow, analysisRows})
}

// TestExecuteCheckAndRecoverFunctionLogsRecoveryNotAttempted checks that the
// line logged when the recovery function doesn't attempt a recovery, and
// returns no error, has no error attributes.
func TestExecuteCheckAndRecoverFunctionLogsRecoveryNotAttempted(t *testing.T) {
	primary := setupRecovery(t)
	info := &test.InfoForRecoveryAnalysis{
		TabletInfo:        primary,
		DurabilityPolicy:  policy.DurabilityNone,
		LastCheckValid:    0,
		CountReplicas:     3,
		IsPrimary:         1,
		CurrentTabletType: int(topodatapb.TabletType_PRIMARY),
	}
	info.SetValuesFromTabletInfo()
	setRecoveryDB(t, primary, []sqlutils.RowMap{info.ConvertToRowMap()})

	logs := logtest.Capture(t, slog.LevelInfo)
	require.NoError(t, executeCheckAndRecoverFunction(&inst.DetectionAnalysis{
		Analysis:              inst.DeadPrimaryAndReplicas,
		AnalyzedInstanceAlias: primary.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}))

	record := logs.Record(t, "Recovery for DeadPrimaryAndReplicas on ks/0: Recovery not attempted")
	assert.NotContains(t, record, "error")
	assert.NotContains(t, record, "error_code")
}

var logTestRuns atomic.Uint32

// newRunUIDOffset returns a multiple of 1000 that no earlier call returned. A
// test that checks a line logged once a minute per tablet (util.ClearToLog)
// adds it to its tablets' UIDs, so the line is logged again on each run.
func newRunUIDOffset() uint32 {
	return 1000 * logTestRuns.Add(1)
}

// TestExecuteCheckAndRecoverFunctionLogsSkippedRecovery checks that the line
// logged when a recovery is skipped leaves out the recovery when there is
// none.
func TestExecuteCheckAndRecoverFunctionLogsSkippedRecovery(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	require.NoError(t, executeCheckAndRecoverFunction(&inst.DetectionAnalysis{
		Analysis:              inst.AnalysisCode("UnhandledProblem"),
		AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 302 + newRunUIDOffset()},
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}))
	record := logs.Record(t, "Recovery for UnhandledProblem on ks/0: Skipping recovery for problem, aborting recovery")
	assert.NotContains(t, record, "recovery")
}

// setupReparentShard saves a primary and a replica of ks/0 in the backend and
// the topo, with a keyspace that has no semi-sync. It returns the primary.
func setupReparentShard(t *testing.T) *topodatapb.Tablet {
	db.ClearVTOrcDatabase()
	t.Cleanup(db.ClearVTOrcDatabase)
	ctx := t.Context()
	primary := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname:      "localhost",
		MysqlHostname: "localhost",
		MysqlPort:     6709,
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_PRIMARY,
	}
	replica := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
		Hostname:      "localhost",
		MysqlHostname: "localhost",
		MysqlPort:     6710,
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_REPLICA,
	}
	require.NoError(t, inst.SaveTablet(primary))
	require.NoError(t, inst.SaveTablet(replica))
	keyspaceInfo := &topo.KeyspaceInfo{Keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}}
	keyspaceInfo.SetKeyspaceName("ks")
	require.NoError(t, inst.SaveKeyspace(keyspaceInfo))

	oldTs, oldTmc := ts, tmc
	t.Cleanup(func() { ts, tmc = oldTs, oldTmc })
	ts = memorytopo.NewServer(ctx, "zone1")
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}))
	require.NoError(t, ts.CreateShard(ctx, "ks", "0"))
	require.NoError(t, ts.CreateTablet(ctx, primary))
	require.NoError(t, ts.CreateTablet(ctx, replica))
	return primary
}

// saveRestartShard saves a primary and the given replicas of ks/0 in the topo.
// The replicas in withInstance are also saved in the backend, with an instance
// that replicates directly from the primary.
func saveRestartShard(t *testing.T, replicas []*topodatapb.Tablet, withInstance map[uint32]bool) *topodatapb.Tablet {
	db.ClearVTOrcDatabase()
	t.Cleanup(db.ClearVTOrcDatabase)
	inst.InitializeForgetAliasesCache()

	primaryTablet := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname:      "primary",
		MysqlHostname: "primary",
		MysqlPort:     3306,
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_PRIMARY,
	}
	require.NoError(t, inst.SaveTablet(primaryTablet))
	keyspaceInfo := &topo.KeyspaceInfo{Keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}}
	keyspaceInfo.SetKeyspaceName("ks")
	require.NoError(t, inst.SaveKeyspace(keyspaceInfo))

	ctx := t.Context()
	oldTs, oldTmc := ts, tmc
	t.Cleanup(func() { ts, tmc = oldTs, oldTmc })
	ts = memorytopo.NewServer(ctx, "zone1")
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}))
	require.NoError(t, ts.CreateShard(ctx, "ks", "0"))
	require.NoError(t, ts.CreateTablet(ctx, primaryTablet))
	for _, replica := range replicas {
		require.NoError(t, ts.CreateTablet(ctx, replica))
		if withInstance[replica.Alias.Uid] {
			require.NoError(t, inst.SaveTablet(replica))
			require.NoError(t, inst.WriteInstance(&inst.Instance{
				InstanceAlias:    replica.Alias,
				Hostname:         replica.MysqlHostname,
				Port:             int(replica.MysqlPort),
				SourceHost:       "primary",
				SourcePort:       3306,
				ReplicationDepth: 1,
			}, true, nil))
		}
	}
	urgentOperations.Flush()
	return primaryTablet
}

func restartReplica(uid uint32) *topodatapb.Tablet {
	return &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: uid},
		Hostname:      "replica",
		MysqlHostname: "replica",
		MysqlPort:     3306,
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_REPLICA,
	}
}

// TestRestartDirectReplicasLogs checks that the lines of a restart of direct
// replicas leave out the error when there is none: the summary of a
// successful restart, and the line for a replica with no instance.
func TestRestartDirectReplicasLogs(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		primary := saveRestartShard(t, []*topodatapb.Tablet{restartReplica(101)}, map[uint32]bool{101: true})
		tmc = &testutil.TabletManagerClient{RestartReplicationResults: map[string]error{"zone1-0000000101": nil}}

		logs := logtest.Capture(t, slog.LevelInfo)
		attempted, _, err := restartDirectReplicas(t.Context(), &inst.DetectionAnalysis{
			Analysis:              inst.UnreachablePrimary,
			AnalyzedInstanceAlias: primary.Alias,
			AnalyzedKeyspace:      "ks",
			AnalyzedShard:         "0",
		}, 0, log.NewPrefixedLogger("test"))
		require.NoError(t, err)
		require.True(t, attempted)

		record := logs.Record(t, "test: Completed restart of direct replicas for unreachable primary")
		assert.NotContains(t, record, "error")
	})

	t.Run("errors", func(t *testing.T) {
		// zone1-0000000101 fails to restart; zone1-0000000102 has no instance row.
		primary := saveRestartShard(t, []*topodatapb.Tablet{restartReplica(101), restartReplica(102)}, map[uint32]bool{101: true})
		tmc = &testutil.TabletManagerClient{
			RestartReplicationResults: map[string]error{"zone1-0000000101": vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "replica is down")},
			StartReplicationResults:   map[string]error{"zone1-0000000101": nil},
		}

		logs := logtest.Capture(t, slog.LevelInfo)
		attempted, _, err := restartDirectReplicas(t.Context(), &inst.DetectionAnalysis{
			Analysis:              inst.UnreachablePrimary,
			AnalyzedInstanceAlias: primary.Alias,
			AnalyzedKeyspace:      "ks",
			AnalyzedShard:         "0",
		}, 0, log.NewPrefixedLogger("test"))
		require.Error(t, err)
		require.True(t, attempted)

		record := logs.Record(t, "test: Could not read instance information")
		assert.NotContains(t, record, "error")

		record = logs.Record(t, "test: Completed restart of direct replicas for unreachable primary")
		assert.Equal(t, "replica is down", record["error"])
	})
}
