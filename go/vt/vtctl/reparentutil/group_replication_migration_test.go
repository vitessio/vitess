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

func newTestMigrator(c *fakeGRCluster, ts *topo.Server) *ReplicationModeMigrator {
	m := NewReplicationModeMigrator(ts, c, logutil.NewMemoryLogger())
	m.pollInterval = time.Millisecond
	return m
}

func migrate(t *testing.T, m *ReplicationModeMigrator, durability string, dryRun bool) (*vtctldatapb.MigrateReplicationModeResponse, error) {
	return m.Migrate(t.Context(), "ks", MigrateReplicationModeOptions{DurabilityPolicy: durability, DryRun: dryRun, WaitTimeout: 30 * time.Second})
}

func stepStatuses(steps []*vtctldatapb.ReplicationModeMigrationStep) map[string]string {
	statuses := make(map[string]string)
	for _, step := range steps {
		key := step.Action
		if step.Tablet != nil {
			key += " " + topoproto.TabletAliasString(step.Tablet)
		}
		statuses[key] = step.Status
	}
	return statuses
}

func keyspaceDurability(t *testing.T, ts *topo.Server) string {
	durability, err := ts.GetKeyspaceDurability(t.Context(), "ks")
	require.NoError(t, err)
	return durability
}

// TestMigrateReplicationModeToGroupReplication converts a semi-sync shard to the cross-cell
// policy: the voters (one per cell, the primary among them) are stored in the shard record,
// the group is bootstrapped on the primary, the cross-cell voters join, the zone1 replica
// that is not a voter keeps acking until the primary disabled semi-sync and then replicates
// asynchronously, the rdonly tablet stays an async replica, and the keyspace policy is
// switched at the end.
func TestMigrateReplicationModeToGroupReplication(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	m := newTestMigrator(c, ts)

	resp, err := migrate(t, m, "group_replication_cross_cell", false)
	require.NoError(t, err)
	assert.Empty(t, c.violationsSoFar())

	assert.Equal(t, []string{aliasP, alias200, alias300}, c.voters(t))
	// The group the migration bootstrapped is recorded as the shard's legitimate group.
	assert.Equal(t, "1790000001", c.recordedIncarnation(t))
	assert.Equal(t, []string{
		"StartGroupReplication(" + aliasP + ", bootstrap)",
		"StartGroupReplication(" + alias200 + ")",
		"StartGroupReplication(" + alias300 + ")",
	}, callsWithPrefix(c.mutatingCalls(), "StartGroupReplication"))
	for _, alias := range []string{aliasP, alias200, alias300} {
		assert.True(t, c.tablet(alias).member, "%s should be a member", alias)
	}
	for _, alias := range []string{alias101, alias102} {
		ft := c.tablet(alias)
		assert.False(t, ft.member, alias)
		assert.Equal(t, aliasP, ft.source, alias)
		assert.False(t, ft.semiSyncReplica, alias)
	}
	assert.False(t, c.tablet(aliasP).semiSyncPrimary)

	assert.Equal(t, "group_replication_cross_cell", keyspaceDurability(t, ts))
	assert.Equal(t, "group_replication_cross_cell", resp.DurabilityPolicy)
	require.Len(t, resp.Shards, 1)
	assert.Equal(t, "group_replication", resp.Shards[0].ReplicationMode)
	steps := resp.Shards[0].Steps
	stepIdx := func(action, alias string) int {
		return slices.IndexFunc(steps, func(s *vtctldatapb.ReplicationModeMigrationStep) bool {
			return s.Action == action && (alias == "" || topoproto.TabletAliasString(s.Tablet) == alias)
		})
	}
	votersIdx := stepIdx(MigrationActionSetVoters, "")
	require.GreaterOrEqual(t, votersIdx, 0)
	assert.Less(t, votersIdx, stepIdx(MigrationActionBootstrapGroup, aliasP), "the voters must be stored before the group is bootstrapped")
	assert.Less(t, stepIdx(MigrationActionBootstrapGroup, aliasP), stepIdx(MigrationActionSetIncarnation, aliasP), "the incarnation is recorded once the group exists")
	assert.Less(t, stepIdx(MigrationActionSetIncarnation, aliasP), stepIdx(MigrationActionJoinGroup, alias200), "the incarnation is recorded before other members join")
	waitIdx := stepIdx(MigrationActionWaitSemiSyncDisabled, aliasP)
	require.GreaterOrEqual(t, waitIdx, 0)
	assert.Less(t, waitIdx, stepIdx(MigrationActionSetReplicationSource, alias101),
		"the wait for semi-sync to be disabled must precede turning semi-sync off on the acker that is not a voter")
	assert.Equal(t, MigrationStepDone, stepStatuses(steps)[MigrationActionSetReplicationSource+" "+alias101])
	assert.Equal(t, MigrationStepSkipped, stepStatuses(steps)[MigrationActionSetReplicationSource+" "+alias102])
	assert.Equal(t, MigrationStepDone, stepStatuses(resp.KeyspaceSteps)[MigrationActionSetDurabilityPolicy])

	require.Len(t, c.queries, 1)
	assert.Contains(t, c.queries[0], "'_vt'")
}

// TestMigrateReplicationModeDefersLastAcker checks that the only semi-sync acker does not
// join the group before the group has two ONLINE members: with cross_cell semi-sync, the
// zone2 replica is the only acker, so the zone1 replicas join first even though cross-cell
// replicas are preferred.
func TestMigrateReplicationModeDefersLastAcker(t *testing.T) {
	c, ts := newFakeGRCluster(t, "cross_cell",
		fakeGRTabletSpec{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
		fakeGRTabletSpec{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone1", uid: 102, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
	)
	require.True(t, c.tablet(alias200).semiSyncReplica)
	require.False(t, c.tablet(alias101).semiSyncReplica)
	m := newTestMigrator(c, ts)

	_, err := migrate(t, m, "group_replication", false)
	require.NoError(t, err)
	assert.Empty(t, c.violationsSoFar())
	assert.Equal(t, []string{
		"StartGroupReplication(" + aliasP + ", bootstrap)",
		"StartGroupReplication(" + alias101 + ")",
		"StartGroupReplication(" + alias200 + ")",
		"StartGroupReplication(" + alias102 + ")",
	}, callsWithPrefix(c.mutatingCalls(), "StartGroupReplication"))
}

// oneCellTwoReplicasShard is a primary in zone1, two replicas in zone2 and one in zone3.
func oneCellTwoReplicasShard() []fakeGRTabletSpec {
	return []fakeGRTabletSpec{
		{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
		{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
		{cell: "zone2", uid: 201, tabletType: topodatapb.TabletType_REPLICA},
		{cell: "zone3", uid: 300, tabletType: topodatapb.TabletType_REPLICA},
	}
}

// TestMigrateReplicationModeOneVoterPerCell checks that under the cross-cell policy only one
// of the two REPLICA tablets of a cell becomes a voter; the other one stays an asynchronous
// replica of the primary, with semi-sync off.
func TestMigrateReplicationModeOneVoterPerCell(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", oneCellTwoReplicasShard()...)
	m := newTestMigrator(c, ts)

	_, err := migrate(t, m, "group_replication_cross_cell", false)
	require.NoError(t, err)
	assert.Empty(t, c.violationsSoFar())
	assert.Equal(t, []string{aliasP, alias200, alias300}, c.voters(t))
	assert.Equal(t, []string{
		"StartGroupReplication(" + aliasP + ", bootstrap)",
		"StartGroupReplication(" + alias200 + ")",
		"StartGroupReplication(" + alias300 + ")",
	}, callsWithPrefix(c.mutatingCalls(), "StartGroupReplication"))
	nonVoter := c.tablet(alias201)
	assert.False(t, nonVoter.member)
	assert.Equal(t, aliasP, nonVoter.source)
	assert.False(t, nonVoter.semiSyncReplica)
	assert.Equal(t, "group_replication_cross_cell", keyspaceDurability(t, ts))
}

// TestMigrateReplicationModeReusesVoters checks that a migration keeps the voters the shard
// record already lists: zone2-0000000201 was listed, so it joins, and zone2-0000000200, which
// would be chosen otherwise, stays an asynchronous replica.
func TestMigrateReplicationModeReusesVoters(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", oneCellTwoReplicasShard()...)
	c.setVoters(t, aliasP, alias201, alias300)
	m := newTestMigrator(c, ts)

	resp, err := migrate(t, m, "group_replication_cross_cell", false)
	require.NoError(t, err)
	assert.Empty(t, c.violationsSoFar())
	assert.Equal(t, []string{aliasP, alias201, alias300}, c.voters(t))
	assert.Equal(t, MigrationStepSkipped, stepStatuses(resp.Shards[0].Steps)[MigrationActionSetVoters])
	assert.True(t, c.tablet(alias201).member)
	assert.False(t, c.tablet(alias200).member)
	assert.Equal(t, aliasP, c.tablet(alias200).source)
}

// TestMigrateReplicationModeDryRun checks that a dry run reports the plan and changes nothing.
func TestMigrateReplicationModeDryRun(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	m := newTestMigrator(c, ts)

	resp, err := migrate(t, m, "group_replication", true)
	require.NoError(t, err)
	assert.Empty(t, c.mutatingCalls())
	assert.Equal(t, "semi_sync", keyspaceDurability(t, ts))

	require.Len(t, resp.Shards, 1)
	statuses := stepStatuses(resp.Shards[0].Steps)
	assert.Equal(t, MigrationStepPlanned, statuses[MigrationActionBootstrapGroup+" "+aliasP])
	assert.Equal(t, MigrationStepPlanned, statuses[MigrationActionSetVoters])
	assert.Empty(t, c.voters(t))
	for _, alias := range []string{alias101, alias200, alias300} {
		assert.Equal(t, MigrationStepPlanned, statuses[MigrationActionJoinGroup+" "+alias], alias)
	}
	assert.Equal(t, MigrationStepPlanned, statuses[MigrationActionWaitSemiSyncDisabled+" "+aliasP])
	assert.Equal(t, MigrationStepPlanned, stepStatuses(resp.KeyspaceSteps)[MigrationActionSetDurabilityPolicy])
}

// TestMigrateReplicationModeResumes checks that a migration that failed half-way continues
// where it stopped when run again, and that a completed migration is a no-op.
func TestMigrateReplicationModeResumes(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	m := newTestMigrator(c, ts)
	c.failOnce["StartGroupReplication("+alias300+")"] = true

	_, err := migrate(t, m, "group_replication", false)
	require.Error(t, err)
	require.ErrorContains(t, err, alias300)
	assert.Equal(t, "semi_sync", keyspaceDurability(t, ts), "the keyspace policy must not change before the shard is converted")
	assert.True(t, c.tablet(alias200).member)
	assert.False(t, c.tablet(alias300).member)

	c.reset()
	resp, err := migrate(t, m, "group_replication", false)
	require.NoError(t, err)
	assert.Empty(t, c.violationsSoFar())
	assert.Equal(t, []string{
		"StartGroupReplication(" + alias300 + ")",
		"StartGroupReplication(" + alias101 + ")",
	}, callsWithPrefix(c.mutatingCalls(), "StartGroupReplication"))
	statuses := stepStatuses(resp.Shards[0].Steps)
	assert.Equal(t, MigrationStepSkipped, statuses[MigrationActionBootstrapGroup+" "+aliasP])
	assert.Equal(t, MigrationStepSkipped, statuses[MigrationActionJoinGroup+" "+alias200])
	assert.Equal(t, "group_replication", keyspaceDurability(t, ts))

	c.reset()
	resp, err = migrate(t, m, "group_replication", false)
	require.NoError(t, err)
	assert.Empty(t, c.mutatingCalls())
	for _, step := range resp.Shards[0].Steps {
		if step.Action != MigrationActionPreflight {
			assert.Equal(t, MigrationStepSkipped, step.Status, step.Description)
		}
	}
	assert.Equal(t, MigrationStepSkipped, stepStatuses(resp.KeyspaceSteps)[MigrationActionSetDurabilityPolicy])
}

// TestMigrateReplicationModePreflight checks that a shard that cannot run Group Replication
// is refused before anything changes.
func TestMigrateReplicationModePreflight(t *testing.T) {
	tests := []struct {
		name       string
		durability string
		specs      []fakeGRTabletSpec
		setup      func(c *fakeGRCluster)
		errContain string
	}{
		{
			name:       "table without primary key",
			durability: "group_replication",
			specs:      migrationTestShard(),
			setup:      func(c *fakeGRCluster) { c.schemaRows = []string{"app|nopk|InnoDB", "app|legacy|MyISAM"} },
			errContain: "app.nopk has no primary key",
		},
		{
			name:       "voting tablet without gr port",
			durability: "group_replication",
			specs: []fakeGRTabletSpec{
				{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
				{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA, noGRPort: true},
				{cell: "zone3", uid: 300, tabletType: topodatapb.TabletType_REPLICA},
			},
			errContain: `zone2-0000000200: the tablet does not publish a "gr" port`,
		},
		{
			name:       "too few voting members",
			durability: "group_replication",
			specs: []fakeGRTabletSpec{
				{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
				{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
				{cell: "zone1", uid: 102, tabletType: topodatapb.TabletType_RDONLY},
			},
			errContain: "the group would have 2 voting members",
		},
		{
			name:       "fewer than three cells with cross-cell policy",
			durability: "group_replication_cross_cell",
			specs: []fakeGRTabletSpec{
				{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
				{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
				{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
				{cell: "zone2", uid: 201, tabletType: topodatapb.TabletType_REPLICA},
			},
			errContain: "the group would have 2 voting members (zone1-0000000100, zone2-0000000200) in cells zone1, zone2; at least 3 are required; " +
				"group_replication_cross_cell allows one voter per cell",
		},
		{
			name:       "old MySQL version",
			durability: "group_replication",
			specs:      migrationTestShard(),
			setup:      func(c *fakeGRCluster) { c.tablets[alias200].version = "8.0.26" },
			errContain: `zone2-0000000200: MySQL version "8.0.26"`,
		},
		{
			name:       "unreachable tablet",
			durability: "group_replication",
			specs:      migrationTestShard(),
			setup:      func(c *fakeGRCluster) { c.tablets[alias300].unreachable = true },
			errContain: "every tablet of shard ks/- must be reachable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, ts := newFakeGRCluster(t, "semi_sync", tt.specs...)
			if tt.setup != nil {
				tt.setup(c)
			}
			m := newTestMigrator(c, ts)
			_, err := migrate(t, m, tt.durability, false)
			require.Error(t, err)
			require.ErrorContains(t, err, tt.errContain)
			assert.Empty(t, c.mutatingCalls())
			assert.Equal(t, "semi_sync", keyspaceDurability(t, ts))
		})
	}

	t.Run("error code", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
		c.schemaRows = []string{"app|nopk|InnoDB"}
		_, err := migrate(t, newTestMigrator(c, ts), "group_replication", false)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	})
}

// TestMigrateReplicationModeFromGroupReplication converts a group back to semi-sync: the
// policy changes first, the secondaries leave one by one and replicate from the primary with
// semi-sync, the migrator waits for the primary to re-enable semi-sync, the primary leaves
// last and becomes writable, and the voters are removed from the shard record. Running it
// again changes nothing.
func TestMigrateReplicationModeFromGroupReplication(t *testing.T) {
	c, ts := newFakeGRCluster(t, "group_replication", migrationTestShard()...)
	c.formGroup(t, "group_replication")
	c.setIncarnation(t, "1790000000")
	require.Equal(t, []string{aliasP, alias101, alias200, alias300}, c.voters(t))
	m := newTestMigrator(c, ts)

	resp, err := migrate(t, m, "semi_sync", false)
	require.NoError(t, err)
	assert.Empty(t, c.violationsSoFar())
	assert.Equal(t, "semi_sync", keyspaceDurability(t, ts))
	assert.Empty(t, c.voters(t))
	assert.Empty(t, c.recordedIncarnation(t), "the incarnation of the group that no longer exists is cleared")
	assert.Equal(t, MigrationStepDone, stepStatuses(resp.Shards[0].Steps)[MigrationActionClearIncarnation])
	assert.Equal(t, MigrationStepDone, stepStatuses(resp.Shards[0].Steps)[MigrationActionClearVoters])
	assert.Equal(t, MigrationStepDone, stepStatuses(resp.KeyspaceSteps)[MigrationActionSetDurabilityPolicy])

	stops := callsWithPrefix(c.mutatingCalls(), "StopGroupReplication")
	require.Len(t, stops, 4)
	assert.Equal(t, "StopGroupReplication("+aliasP+")", stops[3], "the primary must leave last")
	for _, alias := range []string{alias101, alias200, alias300} {
		ft := c.tablet(alias)
		assert.False(t, ft.member, alias)
		assert.Equal(t, aliasP, ft.source, alias)
		assert.True(t, ft.semiSyncReplica, alias)
	}
	primary := c.tablet(aliasP)
	assert.False(t, primary.member)
	assert.False(t, primary.superReadOnly)
	assert.True(t, primary.semiSyncPrimary)
	require.Len(t, resp.Shards, 1)
	assert.Equal(t, "async", resp.Shards[0].ReplicationMode)
	assert.Equal(t, MigrationStepDone, stepStatuses(resp.Shards[0].Steps)[MigrationActionWaitSemiSyncEnabled+" "+aliasP])

	c.reset()
	_, err = migrate(t, m, "semi_sync", false)
	require.NoError(t, err)
	assert.Empty(t, c.mutatingCalls())
}

// TestMigrateReplicationModeFromGroupReplicationDryRun checks that a dry run of the reverse
// migration changes neither the policy nor the group.
func TestMigrateReplicationModeFromGroupReplicationDryRun(t *testing.T) {
	c, ts := newFakeGRCluster(t, "group_replication", migrationTestShard()...)
	c.formGroup(t, "group_replication")
	m := newTestMigrator(c, ts)

	resp, err := migrate(t, m, "semi_sync", true)
	require.NoError(t, err)
	assert.Empty(t, c.mutatingCalls())
	assert.Equal(t, "group_replication", keyspaceDurability(t, ts))
	statuses := stepStatuses(resp.Shards[0].Steps)
	assert.Equal(t, MigrationStepPlanned, statuses[MigrationActionLeaveGroup+" "+aliasP])
	assert.Equal(t, MigrationStepPlanned, statuses[MigrationActionClearVoters])
	assert.NotEmpty(t, c.voters(t))
	assert.Equal(t, MigrationStepPlanned, statuses[MigrationActionWaitSemiSyncEnabled+" "+aliasP])
	assert.Equal(t, MigrationStepPlanned, stepStatuses(resp.KeyspaceSteps)[MigrationActionSetDurabilityPolicy])
}

// TestMigrateReplicationModeInvalidPolicy checks that an unknown target policy is refused.
func TestMigrateReplicationModeInvalidPolicy(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync", migrationTestShard()...)
	_, err := migrate(t, newTestMigrator(c, ts), "no_such_policy", false)
	assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
}
