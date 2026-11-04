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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// auditSteps returns the text of the recovery's audit steps, in order.
func auditSteps(t *testing.T, recoveryID int64) []string {
	var steps []string
	require.NoError(t, db.QueryVTOrc("select message from topology_recovery_steps where recovery_id = ? order by recovery_step_id", []any{recoveryID}, func(row sqlutils.RowMap) error {
		steps = append(steps, row.GetString("message"))
		return nil
	}))
	return steps
}

// saveReparentShardFixture saves a primary and a replica of ks/0, with no
// semi-sync, in the backend and in a memory topo. It returns the primary.
func saveReparentShardFixture(t *testing.T) *topodatapb.Tablet {
	db.ClearVTOrcDatabase()
	t.Cleanup(db.ClearVTOrcDatabase)
	ctx := t.Context()
	oldTs, oldTmc := ts, tmc
	t.Cleanup(func() { ts, tmc = oldTs, oldTmc })
	ts = memorytopo.NewServer(ctx, "zone1")
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}))
	require.NoError(t, ts.CreateShard(ctx, "ks", "0"))
	keyspaceInfo := &topo.KeyspaceInfo{Keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}}
	keyspaceInfo.SetKeyspaceName("ks")
	require.NoError(t, inst.SaveKeyspace(keyspaceInfo))

	var primary *topodatapb.Tablet
	for uid, tabletType := range map[uint32]topodatapb.TabletType{100: topodatapb.TabletType_PRIMARY, 101: topodatapb.TabletType_REPLICA} {
		tablet := &topodatapb.Tablet{
			Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: uid},
			Hostname:      "localhost",
			MysqlHostname: "localhost",
			MysqlPort:     int32(6600 + uid),
			Keyspace:      "ks",
			Shard:         "0",
			Type:          tabletType,
		}
		require.NoError(t, inst.SaveTablet(tablet))
		require.NoError(t, ts.CreateTablet(ctx, tablet))
		if tabletType == topodatapb.TabletType_PRIMARY {
			primary = tablet
		}
	}
	return primary
}

// TestRunEmergencyReparentOpForwardsLines checks that VTOrc logs ERS's lines,
// INFO included, with their own message and attributes, and audits their text.
func TestRunEmergencyReparentOpForwardsLines(t *testing.T) {
	primary := saveReparentShardFixture(t)
	// No tablet answers, so ERS fails after its first lines.
	tmc = &testutil.TabletManagerClient{
		StopReplicationAndGetStatusResults: map[string]struct {
			StopStatus *replicationdatapb.StopReplicationStatus
			Error      error
		}{
			"zone1-0000000100": {Error: vterrors.New(vtrpcpb.Code_UNAVAILABLE, "tablet down")},
		},
	}
	entry := &inst.DetectionAnalysis{Analysis: inst.DeadPrimary, AnalyzedInstanceAlias: primary.Alias, AnalyzedKeyspace: "ks", AnalyzedShard: "0"}
	require.NoError(t, InsertRecoveryDetection(entry))

	logs := logtest.Capture(t, slog.LevelInfo)
	_, topologyRecovery, err := runEmergencyReparentOp(t.Context(), entry, RecoverDeadPrimaryRecoveryName, false, log.NewPrefixedLogger("test"))
	require.Error(t, err)
	require.NotNil(t, topologyRecovery)

	record := logs.Record(t, "test: will initiate emergency reparent shard")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "ERS", record["reparent"])
	assert.Equal(t, "ks", record["keyspace"])
	assert.Equal(t, "0", record["shard"])
	steps := auditSteps(t, topologyRecovery.ID)
	assert.Contains(t, steps, "will initiate emergency reparent shard keyspace=ks shard=0")

	var found bool
	for _, record := range logs.Records(t) {
		if record["msg"] == "test: failed to get replication status" && record["tablet"] == "zone1-0000000100" {
			found = true
			assert.Equal(t, "WARN", record["level"])
			assert.Equal(t, "ERS", record["reparent"])
			assert.Equal(t, "tablet down", record["error"])
			assert.Equal(t, "UNAVAILABLE", record["error_code"])
		}
	}
	assert.True(t, found)
	assert.Contains(t, steps, `failed to get replication status tablet=zone1-0000000100 error="Code: UNAVAILABLE\ntablet down\n" error_code=UNAVAILABLE`)
}

// TestRunPlannedReparentOpForwardsLines checks that VTOrc logs PRS's warnings
// with their own message and attributes, doesn't log its INFO lines, and
// audits both.
func TestRunPlannedReparentOpForwardsLines(t *testing.T) {
	primary := saveReparentShardFixture(t)
	tmc = &testutil.TabletManagerClient{
		GetGlobalStatusVarsResults: map[string]struct {
			Statuses map[string]string
			Error    error
		}{
			"zone1-0000000101": {Statuses: map[string]string{reparentutil.InnodbBufferPoolsDataVar: "not-a-number"}},
		},
	}
	entry := &inst.DetectionAnalysis{Analysis: inst.ClusterHasNoPrimary, AnalyzedInstanceAlias: primary.Alias, AnalyzedKeyspace: "ks", AnalyzedShard: "0"}
	require.NoError(t, InsertRecoveryDetection(entry))

	logs := logtest.Capture(t, slog.LevelInfo)
	_, topologyRecovery, _ := runPlannedReparentOp(t.Context(), entry, ElectNewPrimaryRecoveryName, log.NewPrefixedLogger("test"))
	require.NotNil(t, topologyRecovery)

	record := logs.Record(t, "test: could not parse status variable for tablet as int")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "PRS", record["reparent"])
	assert.Equal(t, "zone1-0000000101", record["tablet"])
	assert.Equal(t, "not-a-number", record["status_value"])

	steps := auditSteps(t, topologyRecovery.ID)
	assert.Contains(t, steps, "Getting a new durability policy durability_policy=none")
	for _, record := range logs.Records(t) {
		assert.NotEqual(t, "test: Getting a new durability policy", record["msg"])
	}
}
