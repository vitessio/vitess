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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/inst"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

// setShardDurabilityPolicy stores the shard's own durability policy in the shard record of ks/0,
// as MigrateReplicationMode does when it converts the shard, and in VTOrc's copy of the record.
func setShardDurabilityPolicy(t *testing.T, durability string) {
	t.Helper()
	si, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.DurabilityPolicy = durability
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, inst.SaveShard(si))
	t.Cleanup(func() { _ = inst.DeleteShard("ks", "0") })
}

// TestReconcileStaleTopoPrimaryVoterOfConvertedShard checks that StaleTopoPrimary treats an old
// primary that is a voter of a shard converted to Group Replication, while its keyspace's policy is
// still semi_sync, as a voter: it only fixes its tablet type, and does not configure asynchronous
// replication next to the group that the voter rejoins (NEW-3 of the Group Replication failover
// audit).
func TestReconcileStaleTopoPrimaryVoterOfConvertedShard(t *testing.T) {
	primary := recoveryTablet("zone1", 100, topodatapb.TabletType_PRIMARY)
	primary.PrimaryTermStartTime = &vttimepb.Time{Seconds: 1000}
	stale := recoveryTablet("zone2", 200, topodatapb.TabletType_PRIMARY)
	stale.PrimaryTermStartTime = &vttimepb.Time{Seconds: 500}
	mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilitySemiSync, primary, stale)
	setVoters(t, primary, stale)
	setShardDurabilityPolicy(t, policy.DurabilityGroupReplication)

	mockTMC.EXPECT().DemotePrimary(gomock.Any(), sameTablet(stale), true).Return(&replicationdatapb.PrimaryStatus{}, nil)
	mockTMC.EXPECT().SetReplicationSource(gomock.Any(), sameTablet(stale), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	analysisEntry := &inst.DetectionAnalysis{
		Analysis:              inst.StaleTopoPrimary,
		AnalyzedInstanceAlias: stale.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	attempted, topologyRecovery, err := reconcileStaleTopoPrimary(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
	require.NoError(t, err)
	require.True(t, attempted)
	require.NotNil(t, topologyRecovery)
	updated, err := ts.GetTablet(t.Context(), stale.Alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_REPLICA, updated.Type)
}

// TestBootstrapGroupReplicationOfConvertedShard checks that VTOrc bootstraps the group of a shard
// converted to Group Replication, while its keyspace's policy is still semi_sync, once every member
// left it: the shard's own policy, from the shard record read under the shard lock, decides.
func TestBootstrapGroupReplicationOfConvertedShard(t *testing.T) {
	const groupName = "6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41"
	tablets := []*topodatapb.Tablet{
		recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA),
		recoveryTablet("zone2", 101, topodatapb.TabletType_REPLICA),
		recoveryTablet("zone3", 102, topodatapb.TabletType_REPLICA),
	}
	mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilitySemiSync, tablets...)
	setVoters(t, tablets...)
	setShardDurabilityPolicy(t, policy.DurabilityGroupReplication)
	for _, tablet := range tablets {
		mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).Return(&replicationdatapb.FullStatus{
			PrimaryStatus: &replicationdatapb.PrimaryStatus{Position: "MySQL56/" + groupName + ":1-10"},
			GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
				PluginActive: true,
				MemberState:  mysql.GroupMemberStateOffline,
			},
		}, nil)
	}
	mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablets[0]), startRequest(true)).
		Return(&replicationdatapb.GroupReplicationStatus{ViewId: "1790000123:1"}, nil)
	joined := make(chan struct{}, len(tablets))
	for _, tablet := range tablets[1:] {
		mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablet), startRequest(false)).
			DoAndReturn(func(context.Context, *topodatapb.Tablet, *tabletmanagerdatapb.StartGroupReplicationRequest) (*replicationdatapb.GroupReplicationStatus, error) {
				joined <- struct{}{}
				return &replicationdatapb.GroupReplicationStatus{ViewId: "1790000123:2"}, nil
			})
	}

	analysisEntry := &inst.DetectionAnalysis{
		Analysis:              inst.GroupNotBootstrapped,
		AnalyzedInstanceAlias: tablets[0].Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	lockedCtx, unlock, err := ts.LockShard(t.Context(), "ks", "0", "test")
	require.NoError(t, err)
	defer unlock(&err)
	attempted, topologyRecovery, err := bootstrapGroupReplication(lockedCtx, analysisEntry, log.NewPrefixedLogger("test"))
	require.NoError(t, err)
	require.True(t, attempted)
	assert.True(t, topologyRecovery.IsSuccessful)
	si, err := ts.GetShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, "1790000123", si.GroupReplicationIncarnation)
	assert.Eventually(t, func() bool { return len(joined) == 2 }, 30*time.Second, 10*time.Millisecond)
}

// TestGetShardDurabilityPolicy checks the policy that VTOrc's recoveries resolve for a shard from
// its copies of the keyspace and shard records.
func TestGetShardDurabilityPolicy(t *testing.T) {
	groupReplicationRecoveryTestWithPolicy(t, policy.DurabilitySemiSync)
	durability, err := inst.GetShardDurabilityPolicy("ks", "0")
	require.NoError(t, err)
	assert.False(t, policy.IsGroupReplication(durability), "a shard without its own policy has the keyspace's")

	setShardDurabilityPolicy(t, policy.DurabilityGroupReplication)
	durability, err = inst.GetShardDurabilityPolicy("ks", "0")
	require.NoError(t, err)
	assert.True(t, policy.IsGroupReplication(durability))
}

// TestGetShardDurabilityPolicyMigrationSource checks the policy that VTOrc's recoveries resolve for
// a shard while MigrateReplicationMode converts its keyspace to Group Replication: the keyspace
// record names the target policy, and its migration source applies to a shard that has no policy of
// its own.
func TestGetShardDurabilityPolicyMigrationSource(t *testing.T) {
	groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplication)
	ki, err := inst.ReadKeyspace("ks")
	require.NoError(t, err)
	ki.MigrationSourceDurabilityPolicy = policy.DurabilitySemiSync
	require.NoError(t, inst.SaveKeyspace(ki))

	durability, err := inst.GetShardDurabilityPolicy("ks", "0")
	require.NoError(t, err)
	assert.False(t, policy.IsGroupReplication(durability), "a shard that is not converted has the migration source's policy")
	durability, err = inst.GetShardRecordDurabilityPolicy("ks", &topodatapb.Shard{})
	require.NoError(t, err)
	assert.False(t, policy.IsGroupReplication(durability))

	setShardDurabilityPolicy(t, policy.DurabilityGroupReplication)
	durability, err = inst.GetShardDurabilityPolicy("ks", "0")
	require.NoError(t, err)
	assert.True(t, policy.IsGroupReplication(durability), "a converted shard has its own policy")
}
