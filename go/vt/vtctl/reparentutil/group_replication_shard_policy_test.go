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

// keyspaceRecord returns the record of the test keyspace.
func keyspaceRecord(t *testing.T, ts *topo.Server) *topodatapb.Keyspace {
	ki, err := ts.GetKeyspace(t.Context(), "ks")
	require.NoError(t, err)
	return ki.Keyspace
}

// TestMigrateReplicationModeConvertsOneShardOfSeveral converts one shard of a keyspace whose other
// shard is not converted: the converted shard gets the target policy as its own, as the last step
// of its conversion, while the keyspace's migration source keeps the other shard's policy. Once
// every shard is converted, the migration source is cleared and the shard's own policy is removed.
func TestMigrateReplicationModeConvertsOneShardOfSeveral(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	addOtherShard(t, ts)
	m := newTestMigrator(c, ts)

	resp, err := migrateShards(t, m, "group_replication_cross_cell", false, "-")
	require.NoError(t, err)
	assert.Empty(t, c.violationsSoFar())

	assert.Equal(t, "semi_sync", keyspaceRecord(t, ts).MigrationSourceDurabilityPolicy, "the keyspace keeps the policy of the shards that are not converted as its migration source")
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

	// Once the other shard is gone, the shard is the keyspace's only shard: the migration source is
	// cleared, and the shard's own policy, now the keyspace's, is removed.
	require.NoError(t, ts.DeleteShard(t.Context(), "ks", otherShard))
	c.reset()
	resp, err = migrateShards(t, m, "group_replication_cross_cell", false)
	require.NoError(t, err)
	assert.Empty(t, c.mutatingCalls())
	assert.Equal(t, "group_replication_cross_cell", keyspaceDurability(t, ts))
	assert.Empty(t, keyspaceRecord(t, ts).MigrationSourceDurabilityPolicy)
	assert.Empty(t, c.shardPolicy(t))
	assert.Equal(t, "group_replication_cross_cell", shardDurability(t, ts, "-"))
	assert.Equal(t, MigrationStepSkipped, stepStatuses(resp.Shards[0].Steps)[MigrationActionSetShardDurabilityPolicy])
	keyspaceSteps := stepStatuses(resp.KeyspaceSteps)
	assert.Equal(t, MigrationStepSkipped, keyspaceSteps[MigrationActionSetDurabilityPolicy], "the first run named the target policy")
	assert.Equal(t, MigrationStepDone, keyspaceSteps[MigrationActionClearMigrationSource])
	assert.Equal(t, MigrationStepDone, keyspaceSteps[MigrationActionClearShardDurabilityPolicy])
}

// TestMigrateReplicationModeHidesKeyspaceFromOlderComponents checks the first step of a migration to
// Group Replication: before its first bootstrap, it names the target policy in the keyspace record,
// in one write that keeps the policy it converts from as the keyspace's migration source. A
// component that does not know the migration source, nor the shard's own policy, then reads the
// group replication policy for every shard of the keyspace, and fails safe if it does not know it
// either, instead of managing a shard that runs a group by the semi-sync policy. A component that
// knows them resolves the same policy for every shard as before.
func TestMigrateReplicationModeHidesKeyspaceFromOlderComponents(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	addOtherShard(t, ts)
	resp, err := migrateShards(t, newTestMigrator(c, ts), "group_replication_cross_cell", false, "-")
	require.NoError(t, err)

	require.NotNil(t, c.keyspaceAtFirstBootstrap, "the migration bootstrapped a group")
	assert.Equal(t, "group_replication_cross_cell", c.keyspaceAtFirstBootstrap.DurabilityPolicy, "the keyspace names the target policy before the first bootstrap")
	assert.Equal(t, "semi_sync", c.keyspaceAtFirstBootstrap.MigrationSourceDurabilityPolicy)
	assert.Equal(t, MigrationStepDone, stepStatuses(resp.KeyspaceSteps)[MigrationActionSetDurabilityPolicy])

	ks := keyspaceRecord(t, ts)
	assert.Equal(t, "group_replication_cross_cell", ks.DurabilityPolicy)
	assert.Equal(t, "semi_sync", ks.MigrationSourceDurabilityPolicy)
	assert.Equal(t, "group_replication_cross_cell", shardDurability(t, ts, "-"))
	assert.Equal(t, "semi_sync", shardDurability(t, ts, otherShard))
}

// TestMigrateReplicationModeBackClearsMigrationSource converts back to semi-sync a keyspace whose
// migration to Group Replication was interrupted after one shard: the keyspace record keeps the
// group replication policy until the shard has left its group, then names semi-sync again, in the
// same write that removes the migration source.
func TestMigrateReplicationModeBackClearsMigrationSource(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	addOtherShard(t, ts)
	m := newTestMigrator(c, ts)
	_, err := migrateShards(t, m, "group_replication", false, "-")
	require.NoError(t, err)
	require.Equal(t, "semi_sync", keyspaceRecord(t, ts).MigrationSourceDurabilityPolicy)

	require.NoError(t, ts.DeleteShard(t.Context(), "ks", otherShard))
	c.reset()
	resp, err := migrateShards(t, m, "semi_sync", false)
	require.NoError(t, err)
	assert.Empty(t, c.violationsSoFar())
	assert.Equal(t, &topodatapb.Keyspace{DurabilityPolicy: "semi_sync"}, keyspaceRecord(t, ts))
	assert.Empty(t, c.shardPolicy(t))
	assert.Equal(t, MigrationStepDone, stepStatuses(resp.KeyspaceSteps)[MigrationActionSetDurabilityPolicy])
}

// TestMigrateReplicationModeRefusesOlderTabletsInKeyspace checks that a migration to Group
// Replication refuses, before it changes anything, a keyspace in which a tablet that answers runs a
// vttablet that does not resolve the shard's own policy and the keyspace's migration source: once the
// keyspace names the target policy, such a tablet would manage its shard by it. Every tablet of the
// keyspace counts, not only the voters of the converted shard. A tablet that does not answer is left
// out: a vttablet that does not know the policy exits when it starts.
func TestMigrateReplicationModeRefusesOlderTabletsInKeyspace(t *testing.T) {
	const problem = "vttablet does not apply the shard's own durability policy"
	for _, tt := range []struct {
		name  string
		setup func(t *testing.T, c *fakeGRCluster)
		// wantErr is a part of the error; empty means that the migration runs.
		wantErr string
	}{{
		name: "an RDONLY tablet of the converted shard",
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.tablets[alias102].shardPolicy = false
		},
		wantErr: "zone1-0000000102: " + problem,
	}, {
		name: "a tablet of another shard",
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.addTabletOfOtherShard(t, "zone2", 210, otherShard).shardPolicy = false
		},
		wantErr: "zone2-0000000210: " + problem,
	}, {
		name: "a tablet of another shard that does not answer",
		setup: func(t *testing.T, c *fakeGRCluster) {
			ft := c.addTabletOfOtherShard(t, "zone2", 210, otherShard)
			ft.shardPolicy = false
			ft.unreachable = true
		},
	}} {
		t.Run(tt.name, func(t *testing.T) {
			c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
			addOtherShard(t, ts)
			tt.setup(t, c)
			_, err := migrateShards(t, newTestMigrator(c, ts), "group_replication_cross_cell", false, "-")
			if tt.wantErr == "" {
				require.NoError(t, err)
				assert.Equal(t, "group_replication_cross_cell", shardDurability(t, ts, "-"))
				return
			}
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			require.ErrorContains(t, err, tt.wantErr)
			assert.Empty(t, c.mutatingCalls())
			assert.Equal(t, &topodatapb.Keyspace{DurabilityPolicy: "semi_sync"}, keyspaceRecord(t, ts), "the keyspace record must not change")
		})
	}
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
	assert.Equal(t, MigrationStepPlanned, keyspaceSteps[MigrationActionClearMigrationSource])
	assert.Equal(t, MigrationStepPlanned, keyspaceSteps[MigrationActionClearShardDurabilityPolicy])
	assert.Equal(t, &topodatapb.Keyspace{DurabilityPolicy: "semi_sync"}, keyspaceRecord(t, ts), "a dry run must not change the keyspace record")
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

// TestMigrateReplicationModeLeavesVotersOfConvertedShardToVTOrc checks that a migration run again on
// a shard that it converted, whose group runs, does not change the shard's voters: VTOrc maintains
// them (GroupVotersOutOfDate) under the rule that keeps a view without a majority of the current
// voters from holding a majority of the new list, and the re-check right before its write. Here two
// of four voters left the group cleanly, and an operator made one of them RDONLY: the migration's
// selection drops it, and the view of the two remaining members would hold two of the three new
// voters, so that its primary would serve with half of the voters it had. The migration keeps the
// recorded voters and goes on, so that a run over the whole keyspace still converts the shards
// that follow.
func TestMigrateReplicationModeLeavesVotersOfConvertedShardToVTOrc(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync",
		fakeGRTabletSpec{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
		fakeGRTabletSpec{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone3", uid: 300, tabletType: topodatapb.TabletType_REPLICA},
	)
	c.formGroup(t, "group_replication")
	c.setIncarnation(t, "1790000000")
	c.setShardPolicy(t, "group_replication")
	// The migration converted the shard: the keyspace names the target, with its source.
	setKeyspaceRecord(t, ts, "group_replication", "semi_sync")
	require.Equal(t, []string{aliasP, alias101, alias200, alias300}, c.voters(t))
	c.mu.Lock()
	c.tablets[alias200].member = false
	c.tablets[alias300].member = false
	c.mu.Unlock()
	_, err := ts.UpdateTabletFields(t.Context(), mustAlias(t, alias300), func(tablet *topodatapb.Tablet) error {
		tablet.Type = topodatapb.TabletType_RDONLY
		return nil
	})
	require.NoError(t, err)

	resp, err := migrate(t, newTestMigrator(c, ts), "group_replication", false)
	require.NoError(t, err)
	assert.Equal(t, []string{aliasP, alias101, alias200, alias300}, c.voters(t))
	idx := stepIndex(resp.Shards[0].Steps, MigrationActionSetVoters, "")
	require.GreaterOrEqual(t, idx, 0)
	assert.Equal(t, MigrationStepSkipped, resp.Shards[0].Steps[idx].Status)
	assert.Contains(t, resp.Shards[0].Steps[idx].Description, "VTOrc maintains the voters of a converted shard")
}

// setKeyspaceRecord replaces the durability fields of the test keyspace's record.
func setKeyspaceRecord(t *testing.T, ts *topo.Server, durability, source string) {
	ctx, unlock, err := ts.LockKeyspace(t.Context(), "ks", "test")
	require.NoError(t, err)
	ki, err := ts.GetKeyspace(ctx, "ks")
	require.NoError(t, err)
	ki.DurabilityPolicy, ki.MigrationSourceDurabilityPolicy = durability, source
	require.NoError(t, ts.UpdateKeyspace(ctx, ki))
	unlock(&err)
	require.NoError(t, err)
}

// TestMigrateReplicationModeRefusesSameReplicationMode checks that MigrateReplicationMode refuses to
// switch a keyspace between two policies of the replication mode it already runs, which
// SetKeyspaceDurabilityPolicy does: from group_replication to group_replication_cross_cell, it would
// keep group_replication as the keyspace's migration source, which is no asynchronous policy, and
// select the voters of every running group again for the target policy, outside the rules that
// keep a minority view from serving. A run again to the keyspace's own policy is still allowed.
func TestMigrateReplicationModeRefusesSameReplicationMode(t *testing.T) {
	t.Run("group replication to another group replication policy", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "group_replication", migrationTestShard()...)
		c.formGroup(t, "group_replication")
		c.setIncarnation(t, "1790000000")
		voters := c.voters(t)
		_, err := migrate(t, newTestMigrator(c, ts), "group_replication_cross_cell", false)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, "use SetKeyspaceDurabilityPolicy")
		assert.Empty(t, c.mutatingCalls())
		assert.Equal(t, voters, c.voters(t))
		assert.Equal(t, &topodatapb.Keyspace{DurabilityPolicy: "group_replication"}, keyspaceRecord(t, ts))
	})
	t.Run("semi-sync to another asynchronous policy", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
		_, err := migrate(t, newTestMigrator(c, ts), "none", false)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, "use SetKeyspaceDurabilityPolicy")
		assert.Empty(t, c.mutatingCalls())
	})
	t.Run("an interrupted migration to another group replication policy", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
		setKeyspaceRecord(t, ts, "group_replication", "semi_sync")
		_, err := migrate(t, newTestMigrator(c, ts), "group_replication_cross_cell", false)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		assert.Empty(t, c.mutatingCalls())
	})
	t.Run("again to the keyspace's own policy", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "group_replication", migrationTestShard()...)
		c.formGroup(t, "group_replication")
		c.setIncarnation(t, "1790000000")
		_, err := migrate(t, newTestMigrator(c, ts), "group_replication", false)
		require.NoError(t, err)
		assert.Empty(t, c.mutatingCalls())
	})
}

// TestMigrateReplicationModeVoterGuardOnAnyGroupReplicationPolicy checks that the migration leaves the
// voters of a shard whose group runs to VTOrc whenever the shard's policy is a group replication
// policy, not only the migration's target: here the shard has group_replication as its own policy,
// and the keyspace names group_replication_cross_cell, whose one voter per cell would drop a voter
// of the running group.
func TestMigrateReplicationModeVoterGuardOnAnyGroupReplicationPolicy(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	c.formGroup(t, "group_replication")
	c.setIncarnation(t, "1790000000")
	c.setShardPolicy(t, "group_replication")
	setKeyspaceRecord(t, ts, "group_replication_cross_cell", "semi_sync")
	voters := c.voters(t)
	require.Len(t, voters, 4)

	resp, err := migrate(t, newTestMigrator(c, ts), "group_replication_cross_cell", false)
	require.NoError(t, err)
	assert.Equal(t, voters, c.voters(t))
	assert.Empty(t, callsWithPrefix(c.mutatingCalls(), "StopGroupReplication"), "no member leaves its group")
	assert.Equal(t, MigrationStepSkipped, stepStatuses(resp.Shards[0].Steps)[MigrationActionSetVoters])
}

// TestMigrateReplicationModeConcurrentDirections checks the invariant that the keyspace's policy names
// a group replication policy whenever a shard may run a group, when a migration to Group Replication
// and one back run at the same time.
func TestMigrateReplicationModeConcurrentDirections(t *testing.T) {
	t.Run("the forward run does not bootstrap after the keyspace was switched back", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
		// The first schema check is the dry run before step 0; by the second, which the conversion
		// runs under the shard lock, a migration back has switched the keyspace to semi_sync.
		c.onQuery = func(n int) {
			if n == 2 {
				setKeyspaceRecord(t, ts, "semi_sync", "")
			}
		}
		_, err := migrate(t, newTestMigrator(c, ts), "group_replication", false)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, "does not name a group replication policy")
		assert.Empty(t, callsWithPrefix(c.mutatingCalls(), "StartGroupReplication"), "no group is bootstrapped")
	})
	t.Run("the backward run does not switch the keyspace while a shard gets a group", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "group_replication", migrationTestShard()...)
		c.formGroup(t, "group_replication")
		c.setIncarnation(t, "1790000000")
		// While the shard is converted back, a migration to Group Replication creates another shard
		// and stores its voters, before its bootstrap.
		c.onCall = map[string]func(){"StopGroupReplication(" + aliasP + ")": func() {
			require.NoError(t, ts.CreateShard(t.Context(), "ks", otherShard))
			_, err := ts.UpdateShardFields(t.Context(), "ks", otherShard, func(si *topo.ShardInfo) error {
				si.GroupReplicationVoters = []*topodatapb.TabletAlias{{Cell: "zone1", Uid: 900}}
				return nil
			})
			require.NoError(t, err)
		}}
		_, err := migrateShards(t, newTestMigrator(c, ts), "semi_sync", false, "-")
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, otherShard)
		assert.Equal(t, "group_replication", keyspaceRecord(t, ts).DurabilityPolicy, "the keyspace keeps the group replication policy")
	})
}

// TestMigrateReplicationModeWarnsOfSourceNextToAsyncPolicy checks a keyspace record with a migration
// source next to an asynchronous policy, which only an older vtctld's SetKeyspaceDurabilityPolicy
// writes: the migration says so in a warning, and, run again to Group Replication, names the target
// policy in the keyspace record again.
func TestMigrateReplicationModeWarnsOfSourceNextToAsyncPolicy(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	setKeyspaceRecord(t, ts, "semi_sync", "semi_sync")
	m := newTestMigrator(c, ts)
	_, err := migrate(t, m, "group_replication", true)
	require.NoError(t, err)
	logs := m.logger.(*logutil.MemoryLogger).String()
	assert.Contains(t, logs, "an older vtctld probably changed it")
	assert.Contains(t, logs, "run MigrateReplicationMode to a group replication policy again")

	_, err = migrate(t, m, "group_replication", false)
	require.NoError(t, err)
	assert.Equal(t, &topodatapb.Keyspace{DurabilityPolicy: "group_replication"}, keyspaceRecord(t, ts), "the migration names the target policy again, and ends")

	// Run back to the source policy instead, the migration ends with the source removed.
	c, ts = newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	setKeyspaceRecord(t, ts, "semi_sync", "semi_sync")
	_, err = migrate(t, newTestMigrator(c, ts), "semi_sync", false)
	require.NoError(t, err)
	assert.Equal(t, &topodatapb.Keyspace{DurabilityPolicy: "semi_sync"}, keyspaceRecord(t, ts))
}

// TestMigrateReplicationModeRefusesKeyspaceWithoutPolicy checks that a migration refuses a keyspace
// whose record names no durability policy: VTOrc does not manage such a keyspace, and step 0 would
// keep "none" as its migration source, so that VTOrc would start to recover the shards that are not
// converted yet.
func TestMigrateReplicationModeRefusesKeyspaceWithoutPolicy(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	setKeyspaceRecord(t, ts, "", "")
	_, err := migrate(t, newTestMigrator(c, ts), "group_replication", false)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	require.ErrorContains(t, err, "set one with SetKeyspaceDurabilityPolicy first")
	assert.Empty(t, c.mutatingCalls())
	assert.Equal(t, &topodatapb.Keyspace{}, keyspaceRecord(t, ts))
}
