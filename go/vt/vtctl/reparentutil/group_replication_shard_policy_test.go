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

package reparentutil

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// otherShard is a second shard of the test keyspace. It has no primary, so a migration cannot
// tell that it is converted, and keeps the keyspace's policy.
const otherShard = "80-"

func addOtherShard(t *testing.T, ts *topo.Server) {
	require.NoError(t, ts.CreateShard(t.Context(), "ks", otherShard))
}

func shardDurability(t *testing.T, ts *topo.Server, shard string) string {
	durability, err := ts.GetShardDurability(t.Context(), "ks", shard)
	require.NoError(t, err)
	return durability
}

func migrateShards(t *testing.T, m *ReplicationModeMigrator, durability string, dryRun bool, shards ...string) (*vtctldatapb.MigrateReplicationModeResponse, error) {
	return m.Migrate(t.Context(), "ks", MigrateReplicationModeOptions{Shards: shards, DurabilityPolicy: durability, DryRun: dryRun, WaitTimeout: 30 * time.Second})
}

func stepIndex(steps []*vtctldatapb.ReplicationModeMigrationStep, action, alias string) int {
	return slices.IndexFunc(steps, func(s *vtctldatapb.ReplicationModeMigrationStep) bool {
		return s.Action == action && (alias == "" || topoproto.TabletAliasString(s.Tablet) == alias)
	})
}

// TestMigrateReplicationModeConvertsOneShardOfSeveral converts one shard of a keyspace whose other
// shard is not converted: the converted shard gets the target policy as its own, as the last step
// of its conversion, while the keyspace keeps its policy for the other shard. Once every shard is
// converted, the keyspace policy switches and the shard's own policy is removed.
func TestMigrateReplicationModeConvertsOneShardOfSeveral(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	addOtherShard(t, ts)
	m := newTestMigrator(c, ts)

	resp, err := migrateShards(t, m, "group_replication_cross_cell", false, "-")
	require.NoError(t, err)
	assert.Empty(t, c.violationsSoFar())

	assert.Equal(t, "semi_sync", keyspaceDurability(t, ts), "the keyspace keeps its policy while a shard is not converted")
	assert.Equal(t, MigrationStepSkipped, stepStatuses(resp.KeyspaceSteps)[MigrationActionKeepDurabilityPolicy])
	assert.Equal(t, "group_replication_cross_cell", c.shardPolicy(t))
	assert.Equal(t, "group_replication_cross_cell", shardDurability(t, ts, "-"))
	assert.Equal(t, "semi_sync", shardDurability(t, ts, otherShard))

	// The shard's policy changes once its group runs with every voter, and the tablets that are
	// not voters replicate asynchronously.
	steps := resp.Shards[0].Steps
	setIdx := stepIndex(steps, MigrationActionSetShardDurabilityPolicy, "")
	require.GreaterOrEqual(t, setIdx, 0)
	assert.Equal(t, MigrationStepDone, steps[setIdx].Status)
	assert.Equal(t, len(steps)-1, setIdx, "the shard's policy is the last step of its conversion")
	for _, before := range []int{
		stepIndex(steps, MigrationActionSetIncarnation, aliasP),
		stepIndex(steps, MigrationActionJoinGroup, alias300),
		stepIndex(steps, MigrationActionWaitSemiSyncDisabled, aliasP),
		stepIndex(steps, MigrationActionSetReplicationSource, alias101),
	} {
		require.GreaterOrEqual(t, before, 0)
		assert.Less(t, before, setIdx)
	}

	// Once the other shard is gone, the shard is the keyspace's only shard: the keyspace switches,
	// and the shard's own policy, now the keyspace's, is removed.
	require.NoError(t, ts.DeleteShard(t.Context(), "ks", otherShard))
	c.reset()
	resp, err = migrateShards(t, m, "group_replication_cross_cell", false)
	require.NoError(t, err)
	assert.Empty(t, c.mutatingCalls())
	assert.Equal(t, "group_replication_cross_cell", keyspaceDurability(t, ts))
	assert.Empty(t, c.shardPolicy(t))
	assert.Equal(t, "group_replication_cross_cell", shardDurability(t, ts, "-"))
	assert.Equal(t, MigrationStepSkipped, stepStatuses(resp.Shards[0].Steps)[MigrationActionSetShardDurabilityPolicy])
	keyspaceSteps := stepStatuses(resp.KeyspaceSteps)
	assert.Equal(t, MigrationStepDone, keyspaceSteps[MigrationActionSetDurabilityPolicy])
	assert.Equal(t, MigrationStepDone, keyspaceSteps[MigrationActionClearShardDurabilityPolicy])
}

// TestMigrateReplicationModeConvertsOneShardBack converts one shard of a Group Replication keyspace
// back to semi-sync while its other shard keeps its group: the shard gets the semi-sync policy as
// its own before its group shrinks, so that its primary re-enables semi-sync before it leaves its
// group, and the keyspace keeps the Group Replication policy that the other shard still runs.
func TestMigrateReplicationModeConvertsOneShardBack(t *testing.T) {
	c, ts := newFakeGRCluster(t, "group_replication", migrationTestShard()...)
	c.formGroup(t, "group_replication")
	c.setIncarnation(t, "1790000000")
	addOtherShard(t, ts)
	m := newTestMigrator(c, ts)

	resp, err := migrateShards(t, m, "semi_sync", false, "-")
	require.NoError(t, err)
	assert.Empty(t, c.violationsSoFar())

	assert.Equal(t, "group_replication", keyspaceDurability(t, ts), "the keyspace keeps its policy while a shard still runs its group")
	assert.Equal(t, "group_replication", shardDurability(t, ts, otherShard))
	assert.Equal(t, "semi_sync", c.shardPolicy(t))
	assert.Equal(t, "semi_sync", shardDurability(t, ts, "-"))
	assert.Empty(t, c.voters(t))
	primary := c.tablet(aliasP)
	assert.False(t, primary.member)
	assert.True(t, primary.semiSyncPrimary)

	steps := resp.Shards[0].Steps
	setIdx := stepIndex(steps, MigrationActionSetShardDurabilityPolicy, "")
	require.GreaterOrEqual(t, setIdx, 0)
	assert.Equal(t, MigrationStepDone, steps[setIdx].Status)
	firstLeave := stepIndex(steps, MigrationActionLeaveGroup, "")
	require.GreaterOrEqual(t, firstLeave, 0)
	assert.Less(t, setIdx, firstLeave, "the shard's policy changes before its group shrinks")

	require.NoError(t, ts.DeleteShard(t.Context(), "ks", otherShard))
	c.reset()
	resp, err = migrateShards(t, m, "semi_sync", false)
	require.NoError(t, err)
	assert.Empty(t, c.mutatingCalls())
	assert.Equal(t, "semi_sync", keyspaceDurability(t, ts))
	assert.Empty(t, c.shardPolicy(t))
	assert.Equal(t, MigrationStepDone, stepStatuses(resp.KeyspaceSteps)[MigrationActionClearShardDurabilityPolicy])
}

// TestMigrateReplicationModeRefusesTabletsWithoutShardPolicy checks that a migration refuses a
// shard whose voters or members run a vttablet that manages the shard by the keyspace's policy
// only, before anything changes.
func TestMigrateReplicationModeRefusesTabletsWithoutShardPolicy(t *testing.T) {
	specs := func() []fakeGRTabletSpec {
		specs := migrationTestShard()
		specs[2].noShardPolicy = true // zone2-0000000200
		return specs
	}
	const problem = "zone2-0000000200: vttablet does not apply the shard's own durability policy"

	t.Run("to group replication", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "semi_sync", specs()...)
		addOtherShard(t, ts)
		_, err := migrateShards(t, newTestMigrator(c, ts), "group_replication", false, "-")
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, problem)
		assert.Empty(t, c.mutatingCalls())
		assert.Empty(t, c.shardPolicy(t))
	})

	t.Run("back to semi-sync", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "group_replication", specs()...)
		c.formGroup(t, "group_replication")
		addOtherShard(t, ts)
		_, err := migrateShards(t, newTestMigrator(c, ts), "semi_sync", false, "-")
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, problem)
		assert.Empty(t, c.mutatingCalls())
		assert.Empty(t, c.shardPolicy(t))
	})
}

// TestMigrateReplicationModeDryRunPlansShardPolicy checks that a dry run plans the shard's own
// policy and its removal, and changes neither.
func TestMigrateReplicationModeDryRunPlansShardPolicy(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	resp, err := migrate(t, newTestMigrator(c, ts), "group_replication", true)
	require.NoError(t, err)
	assert.Empty(t, c.mutatingCalls())
	assert.Empty(t, c.shardPolicy(t))
	assert.Equal(t, MigrationStepPlanned, stepStatuses(resp.Shards[0].Steps)[MigrationActionSetShardDurabilityPolicy])
	keyspaceSteps := stepStatuses(resp.KeyspaceSteps)
	assert.Equal(t, MigrationStepPlanned, keyspaceSteps[MigrationActionSetDurabilityPolicy])
	assert.Equal(t, MigrationStepPlanned, keyspaceSteps[MigrationActionClearShardDurabilityPolicy])
}

// newConvertedShardOfSemiSyncKeyspace is newFailedGroupShard in a keyspace whose policy is still
// semi_sync, as while MigrateReplicationMode converts its shards one at a time: the shard was
// converted, and has the group_replication policy as its own.
func newConvertedShardOfSemiSyncKeyspace(t *testing.T) (*fakeGRCluster, *topo.Server) {
	c, ts := newFakeGRCluster(t, "semi_sync",
		fakeGRTabletSpec{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
		fakeGRTabletSpec{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone1", uid: 102, tabletType: topodatapb.TabletType_RDONLY},
	)
	c.formGroup(t, "group_replication")
	c.setShardPolicy(t, "group_replication")
	c.tablets[aliasP].unreachable = true
	c.groupPrimary = alias200
	return c, ts
}

// TestEmergencyReparentConvertedShardOfSemiSyncKeyspace checks that ERS on a shard that was
// converted to Group Replication, while its keyspace's policy is still semi_sync, takes the Group
// Replication path: it follows the primary the group elected, instead of stopping replication on
// the members and promoting the most advanced one on its own.
func TestEmergencyReparentConvertedShardOfSemiSyncKeyspace(t *testing.T) {
	c, ts := newConvertedShardOfSemiSyncKeyspace(t)
	erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())

	ev, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{WaitReplicasTimeout: 30 * time.Second})
	require.NoError(t, err)
	assert.Empty(t, c.violationsSoFar())
	require.NotNil(t, ev.NewPrimary)
	assert.Equal(t, alias200, topoproto.TabletAliasString(ev.NewPrimary.Alias))
	calls := c.mutatingCalls()
	assert.Equal(t, []string{"PromoteReplica(" + alias200 + ")"}, callsWithPrefix(calls, "PromoteReplica"))
	assert.Empty(t, callsWithPrefix(calls, "StopReplicationAndGetStatus"))
}

// TestPlannedReparentConvertedShardOfSemiSyncKeyspace checks that PRS on a shard that was converted
// to Group Replication, while its keyspace's policy is still semi_sync, takes the Group Replication
// path: a tablet that is not a voter is refused before anything changes, and without a requested
// primary only the voters are elected.
func TestPlannedReparentConvertedShardOfSemiSyncKeyspace(t *testing.T) {
	t.Run("not a voter", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
		c.formGroup(t, "group_replication_cross_cell")
		c.setShardPolicy(t, "group_replication_cross_cell")
		require.Equal(t, []string{aliasP, alias200, alias300}, c.voters(t))
		pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

		_, err := pr.ReparentShard(t.Context(), "ks", "-", PlannedReparentOptions{
			NewPrimaryAlias:     mustAlias(t, alias101),
			WaitReplicasTimeout: 30 * time.Second,
		})
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, "only voters can be promoted")
		assert.Empty(t, c.mutatingCalls())
	})

	t.Run("not an online member", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
		c.formGroup(t, "group_replication")
		c.setShardPolicy(t, "group_replication")
		c.tablets[alias300].member = false
		pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

		_, err := pr.ReparentShard(t.Context(), "ks", "-", PlannedReparentOptions{
			NewPrimaryAlias:     mustAlias(t, alias300),
			WaitReplicasTimeout: 30 * time.Second,
		})
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, "is not an ONLINE member of the replication group")
		assert.Empty(t, c.mutatingCalls())
	})
}

// TestEmergencyReparentShardConvertedBack checks the reverse: in a keyspace whose policy is still
// group_replication, a shard that was converted back to semi-sync has semi_sync as its own policy,
// and ERS takes the asynchronous path, which the fake does not simulate, instead of looking for a
// group that no longer exists.
func TestEmergencyReparentShardConvertedBack(t *testing.T) {
	c, ts := newFakeGRCluster(t, "group_replication",
		fakeGRTabletSpec{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
		fakeGRTabletSpec{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
	)
	c.setShardPolicy(t, "semi_sync")
	c.tablets[aliasP].unreachable = true
	erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())

	_, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{WaitReplicasTimeout: 30 * time.Second})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "replication group", "ERS must not take the group replication path")
	assert.NotEmpty(t, callsWithPrefix(c.mutatingCalls(), "StopReplicationAndGetStatus"), "ERS takes the asynchronous path")
}
