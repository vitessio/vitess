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
	"go.uber.org/mock/gomock"
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
	tmcmock "vitess.io/vitess/go/vt/vttablet/tmclient/mock"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestAuditTopologyRecoveryLogs(t *testing.T) {
	t.Cleanup(db.ClearVTOrcDatabase)
	logs := logtest.Capture(t, slog.LevelInfo)

	require.NoError(t, AuditTopologyRecovery(nil, "step without a recovery"))
	require.NoError(t, AuditTopologyRecovery(&TopologyRecovery{ID: 7}, "step of a recovery"))

	var records []map[string]any
	for _, record := range logs.Records(t) {
		if record["msg"] == "topology_recovery" {
			records = append(records, record)
		}
	}
	require.Len(t, records, 2)
	for _, record := range records {
		assert.Equal(t, "INFO", record["level"])
	}
	assert.Equal(t, "step without a recovery", records[0]["message"])
	assert.NotContains(t, records[0], "recovery_id")
	assert.Equal(t, "step of a recovery", records[1]["message"])
	assert.EqualValues(t, 7, records[1]["recovery_id"])
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

// readOnlyPrimaryRows returns the analysis rows of a read-only primary.
func readOnlyPrimaryRows(primary *topodatapb.Tablet) []sqlutils.RowMap {
	info := &test.InfoForRecoveryAnalysis{
		TabletInfo:         primary,
		DurabilityPolicy:   policy.DurabilityNone,
		LastCheckValid:     1,
		CountReplicas:      1,
		CountValidReplicas: 1,
		IsPrimary:          1,
		ReadOnly:           1,
		CurrentTabletType:  int(topodatapb.TabletType_PRIMARY),
	}
	info.SetValuesFromTabletInfo()
	return []sqlutils.RowMap{info.ConvertToRowMap()}
}

// TestExecuteCheckAndRecoverFunctionLogsRecovery runs a FixPrimary recovery
// through executeCheckAndRecoverFunction and checks the line that reports the
// recovery: it logs the recovery and the fields of its analysis (see
// TestDetectionAnalysisLogAttrs).
func TestExecuteCheckAndRecoverFunctionLogsRecovery(t *testing.T) {
	primary := setupRecovery(t)
	setRecoveryDB(t, primary, readOnlyPrimaryRows(primary))

	analysis := &inst.DetectionAnalysis{
		Analysis:              inst.PrimaryIsReadOnly,
		AnalyzedInstanceAlias: primary.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
		LastCheckValid:        true,
	}

	logs := logtest.Capture(t, slog.LevelInfo)
	err := executeCheckAndRecoverFunction(analysis)
	require.Error(t, err)

	prefix := "Recovery for PrimaryIsReadOnly on ks/0: "
	record := logs.Record(t, prefix+"Failed to recover")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, err.Error(), record["error"])
	assert.Equal(t, "UNKNOWN", record["error_code"])

	record = logs.Record(t, prefix+"Topology recovery")
	assert.Equal(t, "INFO", record["level"])
	assert.NotZero(t, record["recovery_id"])
	assert.Equal(t, false, record["successful"])
	assert.NotContains(t, record, "promoted_tablet")
	assert.EqualValues(t, 1, record["detection_id"])
}

// TestExecuteCheckAndRecoverFunctionLogsLockError checks the line logged when
// the shard can't be locked: here the analysis has no keyspace.
func TestExecuteCheckAndRecoverFunctionLogsLockError(t *testing.T) {
	primary := setupRecovery(t)
	setRecoveryDB(t, primary, readOnlyPrimaryRows(primary))

	logs := logtest.Capture(t, slog.LevelInfo)
	recoverErr := executeCheckAndRecoverFunction(&inst.DetectionAnalysis{
		Analysis:              inst.PrimaryIsReadOnly,
		AnalyzedInstanceAlias: primary.Alias,
		AnalyzedShard:         "0",
		LastCheckValid:        true,
	})
	require.ErrorContains(t, recoverErr, "can't lock shard")

	record := logs.Record(t, "Recovery for PrimaryIsReadOnly on /0: Failed to lock shard, aborting recovery")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, recoverErr.Error(), record["error"])
	assert.Equal(t, "UNKNOWN", record["error_code"])
}

// TestExecuteCheckAndRecoverFunctionLogsRecoveryNotAttempted checks the line
// logged when the recovery function doesn't attempt a recovery, with and
// without an error.
func TestExecuteCheckAndRecoverFunctionLogsRecoveryNotAttempted(t *testing.T) {
	t.Run("active recovery", func(t *testing.T) {
		primary := setupRecovery(t)
		setRecoveryDB(t, primary, readOnlyPrimaryRows(primary))
		_, err := writeTopologyRecovery(NewTopologyRecovery(inst.DetectionAnalysis{
			Analysis:              inst.DeadPrimary,
			AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 102},
			AnalyzedKeyspace:      "ks",
			AnalyzedShard:         "0",
		}))
		require.NoError(t, err)

		logs := logtest.Capture(t, slog.LevelInfo)
		recoverErr := executeCheckAndRecoverFunction(&inst.DetectionAnalysis{
			Analysis:              inst.PrimaryIsReadOnly,
			AnalyzedInstanceAlias: primary.Alias,
			AnalyzedKeyspace:      "ks",
			AnalyzedShard:         "0",
			LastCheckValid:        true,
		})
		require.Error(t, recoverErr)

		record := logs.Record(t, "Recovery for PrimaryIsReadOnly on ks/0: Recovery not attempted")
		assert.Equal(t, "ERROR", record["level"])
		assert.Equal(t, recoverErr.Error(), record["error"])
		assert.Equal(t, "UNKNOWN", record["error_code"])
	})

	t.Run("no recovery action", func(t *testing.T) {
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
		assert.Equal(t, "ERROR", record["level"])
		assert.NotContains(t, record, "error")
		assert.NotContains(t, record, "error_code")
	})
}

var logTestRuns atomic.Uint32

// newRunUIDOffset returns a multiple of 1000 that no earlier call returned. A
// test that checks a line logged once a minute per tablet (util.ClearToLog)
// adds it to its tablets' UIDs, so the line is logged again on each run.
func newRunUIDOffset() uint32 {
	return 1000 * logTestRuns.Add(1)
}

// TestExecuteCheckAndRecoverFunctionLogsSkippedRecovery checks the line logged
// when a recovery is skipped: it names the recovery only when there is one.
func TestExecuteCheckAndRecoverFunctionLogsSkippedRecovery(t *testing.T) {
	prev := config.ConvertTabletWithErrantGTIDs()
	config.SetConvertTabletWithErrantGTIDs(false)
	t.Cleanup(func() { config.SetConvertTabletWithErrantGTIDs(prev) })
	offset := newRunUIDOffset()

	logs := logtest.Capture(t, slog.LevelInfo)
	require.NoError(t, executeCheckAndRecoverFunction(&inst.DetectionAnalysis{
		Analysis:              inst.ErrantGTIDDetected,
		AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 301 + offset},
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}))
	record := logs.Record(t, "Recovery for ErrantGTIDDetected on ks/0: Skipping recovery for problem, aborting recovery")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "ErrantGTIDDetected", record["analysis"])
	assert.Equal(t, RecoverErrantGTIDDetectedName, record["recovery"])
	assert.Equal(t, RecoverySkipNoRecoveryAction.String(), record["reason"])

	logs.Reset()
	require.NoError(t, executeCheckAndRecoverFunction(&inst.DetectionAnalysis{
		Analysis:              inst.AnalysisCode("UnhandledProblem"),
		AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 302 + offset},
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}))
	record = logs.Record(t, "Recovery for UnhandledProblem on ks/0: Skipping recovery for problem, aborting recovery")
	assert.Equal(t, "UnhandledProblem", record["analysis"])
	assert.NotContains(t, record, "recovery")
	assert.Equal(t, RecoverySkipNoRecoveryAction.String(), record["reason"])
}

// TestRecoverShardAnalysesLogsErrors checks the line logged for a failed
// recovery, run in order and concurrently.
func TestRecoverShardAnalysesLogsErrors(t *testing.T) {
	analyses := []*inst.DetectionAnalysis{
		{Analysis: inst.DeadPrimary, AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 1}, AnalyzedKeyspace: "ks", AnalyzedShard: "0"},
		{Analysis: inst.ReplicaIsWritable, AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 2}, AnalyzedKeyspace: "ks", AnalyzedShard: "0"},
	}
	logs := logtest.Capture(t, slog.LevelInfo)
	recoverShardAnalyses(analyses, func(entry *inst.DetectionAnalysis) error {
		return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "recovery of %s failed", entry.Analysis)
	})

	byAnalysis := map[any]map[string]any{}
	for _, record := range logs.Records(t) {
		if record["msg"] == "Failed to execute CheckAndRecover function" {
			byAnalysis[record["analysis"]] = record
		}
	}
	require.Len(t, byAnalysis, 2)
	for _, analysis := range []string{"DeadPrimary", "ReplicaIsWritable"} {
		record := byAnalysis[analysis]
		require.NotNil(t, record, analysis)
		assert.Equal(t, "ERROR", record["level"])
		assert.Equal(t, "ks", record["keyspace"])
		assert.Equal(t, "0", record["shard"])
		assert.Equal(t, "recovery of "+analysis+" failed", record["error"])
		assert.Equal(t, "UNAVAILABLE", record["error_code"])
	}
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

// TestRunEmergencyReparentOpLogsError checks the line logged when ERS fails.
func TestRunEmergencyReparentOpLogsError(t *testing.T) {
	primary := setupReparentShard(t)
	// No tablet answers, so ERS finds no candidate.
	tmc = &testutil.TabletManagerClient{}
	entry := &inst.DetectionAnalysis{Analysis: inst.DeadPrimary, AnalyzedInstanceAlias: primary.Alias, AnalyzedKeyspace: "ks", AnalyzedShard: "0"}
	require.NoError(t, InsertRecoveryDetection(entry))

	logs := logtest.Capture(t, slog.LevelInfo)
	attempted, _, err := runEmergencyReparentOp(t.Context(), entry, RecoverDeadPrimaryRecoveryName, false, log.NewPrefixedLogger("test"))
	require.Error(t, err)
	assert.True(t, attempted)

	record := logs.Record(t, "test: Error running ERS")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, err.Error(), record["error"])
	assert.Equal(t, vterrors.Code(err).String(), record["error_code"])
}

// TestFixReplicaLogsSetReadOnlyError checks the line logged when fixReplica
// can't set the replica read-only.
func TestFixReplicaLogsSetReadOnlyError(t *testing.T) {
	db.ClearVTOrcDatabase()
	t.Cleanup(db.ClearVTOrcDatabase)
	primary := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}, MysqlHostname: "primary", MysqlPort: 3306,
		Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_PRIMARY,
	}
	replica := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, MysqlHostname: "replica", MysqlPort: 3306,
		Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_REPLICA,
	}
	require.NoError(t, inst.SaveTablet(primary))
	require.NoError(t, inst.SaveTablet(replica))
	keyspaceInfo := &topo.KeyspaceInfo{Keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}}
	keyspaceInfo.SetKeyspaceName("ks")
	require.NoError(t, inst.SaveKeyspace(keyspaceInfo))

	mockTMC := tmcmock.NewMockTabletManagerClient(gomock.NewController(t))
	mockTMC.EXPECT().SetReadOnly(gomock.Any(), gomock.Any()).Return(vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "tablet is down"))
	oldTMC := tmc
	tmc = mockTMC
	t.Cleanup(func() { tmc = oldTMC })

	logs := logtest.Capture(t, slog.LevelInfo)
	attempted, _, err := fixReplica(t.Context(), &inst.DetectionAnalysis{
		Analysis:              inst.ReplicaIsWritable,
		AnalyzedInstanceAlias: replica.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}, log.NewPrefixedLogger("test"))
	require.Error(t, err)
	assert.True(t, attempted)

	record := logs.Record(t, "test: Could not set the tablet to readonly")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "zone1-0000000101", record["tablet"])
	assert.Equal(t, "tablet is down", record["error"])
	assert.Equal(t, "UNAVAILABLE", record["error_code"])
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

// TestRestartDirectReplicasLogs checks the lines of a restart of direct
// replicas: the summary leaves out the error when there is none, and an
// unreadable or failed replica is logged by its alias, with the error only
// when there is one.
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

		record := logs.Record(t, "test: Successfully restarted replication")
		assert.Equal(t, "INFO", record["level"])
		assert.Equal(t, "zone1-0000000101", record["tablet"])

		record = logs.Record(t, "test: Completed restart of direct replicas for unreachable primary")
		assert.Equal(t, "INFO", record["level"])
		assert.EqualValues(t, 1, record["restarted_replicas"])
		assert.EqualValues(t, 1, record["expected_replicas"])
		assert.Equal(t, "zone1-0000000100", record["tablet"])
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
		assert.Equal(t, "WARN", record["level"])
		assert.Equal(t, "zone1-0000000102", record["tablet"])
		assert.NotContains(t, record, "error")

		record = logs.Record(t, "test: Failed to restart replication")
		assert.Equal(t, "ERROR", record["level"])
		assert.Equal(t, "zone1-0000000101", record["tablet"])
		assert.Equal(t, "replica is down", record["error"])
		assert.Equal(t, "UNAVAILABLE", record["error_code"])

		record = logs.Record(t, "test: Completed restart of direct replicas for unreachable primary")
		assert.EqualValues(t, 0, record["restarted_replicas"])
		assert.EqualValues(t, 1, record["expected_replicas"])
		assert.Equal(t, "replica is down", record["error"])
		assert.Equal(t, "UNAVAILABLE", record["error_code"])
	})
}

// TestActiveRecoveryLogs checks the line each recovery logs when a recovery is
// already active on the shard: it names the recovery and the tablet.
func TestActiveRecoveryLogs(t *testing.T) {
	tests := []struct {
		recovery string
		run      func(*inst.DetectionAnalysis) (bool, *TopologyRecovery, error)
	}{
		{FixReplicaRecoveryName, func(a *inst.DetectionAnalysis) (bool, *TopologyRecovery, error) {
			return fixReplica(t.Context(), a, log.NewPrefixedLogger("test"))
		}},
		{FixPrimaryRecoveryName, func(a *inst.DetectionAnalysis) (bool, *TopologyRecovery, error) {
			return fixPrimary(t.Context(), a, log.NewPrefixedLogger("test"))
		}},
		{RecoverPrimaryHasPrimaryRecoveryName, func(a *inst.DetectionAnalysis) (bool, *TopologyRecovery, error) {
			return recoverPrimaryHasPrimary(t.Context(), a, log.NewPrefixedLogger("test"))
		}},
		{RecoverErrantGTIDDetectedName, func(a *inst.DetectionAnalysis) (bool, *TopologyRecovery, error) {
			return recoverErrantGTIDDetected(t.Context(), a, log.NewPrefixedLogger("test"))
		}},
		{ElectNewPrimaryRecoveryName, func(a *inst.DetectionAnalysis) (bool, *TopologyRecovery, error) {
			return runPlannedReparentOp(t.Context(), a, ElectNewPrimaryRecoveryName, log.NewPrefixedLogger("test"))
		}},
	}
	for i, tt := range tests {
		t.Run(tt.recovery, func(t *testing.T) {
			db.ClearVTOrcDatabase()
			t.Cleanup(db.ClearVTOrcDatabase)

			active, err := writeTopologyRecovery(NewTopologyRecovery(inst.DetectionAnalysis{
				Analysis:              inst.DeadPrimary,
				AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}))
			require.NoError(t, err)
			require.NotNil(t, active)

			logs := logtest.Capture(t, slog.LevelInfo)
			attempted, _, err := tt.run(&inst.DetectionAnalysis{
				Analysis:              inst.ReplicationStopped,
				AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			})
			require.Error(t, err)
			require.False(t, attempted)

			if i == 0 {
				record := logs.Record(t, "AttemptRecoveryRegistration: Active recovery in the cluster")
				assert.Equal(t, "ERROR", record["level"])
				assert.EqualValues(t, active.ID, record["recovery_id"])
				assert.Equal(t, "DeadPrimary", record["analysis"])
				assert.Equal(t, "ks", record["keyspace"])
				assert.Equal(t, "0", record["shard"])
			}

			record := logs.Record(t, "test: found an active or recent recovery. Will not issue another recovery.")
			assert.Equal(t, "WARN", record["level"])
			assert.Equal(t, "zone1-0000000100", record["tablet"])
			assert.Equal(t, tt.recovery, record["recovery"])
		})
	}
}
