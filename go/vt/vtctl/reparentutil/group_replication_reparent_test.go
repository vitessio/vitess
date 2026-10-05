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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// newFailedGroupShard is a group replication shard of three voting members (primary in zone1,
// replicas in zone1 and zone2) and an rdonly async replica. The primary is unreachable, and
// the group elected the zone2 replica.
func newFailedGroupShard(t *testing.T) (*fakeGRCluster, *topo.Server) {
	c, ts := newFakeGRCluster(t, "group_replication",
		fakeGRTabletSpec{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
		fakeGRTabletSpec{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone1", uid: 102, tabletType: topodatapb.TabletType_RDONLY},
	)
	c.formGroup(t, "group_replication")
	c.tablets[aliasP].unreachable = true
	c.groupPrimary = alias200
	return c, ts
}

func mustAlias(t *testing.T, alias string) *topodatapb.TabletAlias {
	a, err := topoproto.ParseTabletAlias(alias)
	require.NoError(t, err)
	return a
}

// TestEmergencyReparentGroupReplicationFollowsGroup checks that ERS on a group replication
// shard makes the topology follow the primary the group elected: it promotes that tablet,
// writes the reparent journal, fixes the other member's type, repoints the async replica,
// and leaves the failed primary alone.
func TestEmergencyReparentGroupReplicationFollowsGroup(t *testing.T) {
	c, ts := newFailedGroupShard(t)
	erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())

	ev, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{WaitReplicasTimeout: 30 * time.Second})
	require.NoError(t, err)
	require.NotNil(t, ev.NewPrimary)
	assert.Equal(t, alias200, topoproto.TabletAliasString(ev.NewPrimary.Alias))
	assert.Empty(t, c.violationsSoFar())

	calls := c.mutatingCalls()
	assert.Equal(t, []string{"PromoteReplica(" + alias200 + ")"}, callsWithPrefix(calls, "PromoteReplica"))
	assert.Equal(t, []string{"PopulateReparentJournal(" + alias200 + ")"}, callsWithPrefix(calls, "PopulateReparentJournal"))
	assert.ElementsMatch(t, []string{
		"SetReplicationSource(" + alias101 + ", " + alias200 + ", semiSync=false)",
		"SetReplicationSource(" + alias102 + ", " + alias200 + ", semiSync=false)",
	}, callsWithPrefix(calls, "SetReplicationSource"))
	assert.Equal(t, alias200, c.tablet(alias102).source)
}

// TestEmergencyReparentGroupReplicationRequestedPrimary checks NewPrimaryAlias: an ONLINE
// member is made the group's primary, and a tablet outside the group is refused before
// anything changes.
func TestEmergencyReparentGroupReplicationRequestedPrimary(t *testing.T) {
	t.Run("online member", func(t *testing.T) {
		c, ts := newFailedGroupShard(t)
		erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())
		ev, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{
			NewPrimaryAlias:     mustAlias(t, alias101),
			WaitReplicasTimeout: 30 * time.Second,
		})
		require.NoError(t, err)
		assert.Equal(t, alias101, topoproto.TabletAliasString(ev.NewPrimary.Alias))
		assert.Equal(t, []string{"PromoteReplica(" + alias101 + ")"}, callsWithPrefix(c.mutatingCalls(), "PromoteReplica"))
		assert.Contains(t, c.mutatingCalls(), "SetReplicationSource("+alias200+", "+alias101+", semiSync=false)")
	})

	t.Run("async replica", func(t *testing.T) {
		c, ts := newFailedGroupShard(t)
		erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())
		_, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{
			NewPrimaryAlias:     mustAlias(t, alias102),
			WaitReplicasTimeout: 30 * time.Second,
		})
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, "not an ONLINE member")
		assert.Empty(t, c.mutatingCalls())
	})
}

// TestEmergencyReparentGroupReplicationPreventCrossCell checks that with
// PreventCrossCellPromotion, ERS moves the group's primary back to the previous primary's
// cell instead of following a cross-cell election.
func TestEmergencyReparentGroupReplicationPreventCrossCell(t *testing.T) {
	c, ts := newFailedGroupShard(t)
	erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())
	ev, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{
		PreventCrossCellPromotion: true,
		WaitReplicasTimeout:       30 * time.Second,
	})
	require.NoError(t, err)
	assert.Equal(t, alias101, topoproto.TabletAliasString(ev.NewPrimary.Alias))
	assert.Equal(t, []string{"PromoteReplica(" + alias101 + ")"}, callsWithPrefix(c.mutatingCalls(), "PromoteReplica"))
}

// TestEmergencyReparentGroupReplicationNoQuorum checks that ERS fails without changing
// anything when no reachable member has quorum.
func TestEmergencyReparentGroupReplicationNoQuorum(t *testing.T) {
	c, ts := newFailedGroupShard(t)
	c.tablets[alias200].unreachable = true
	erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())

	_, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{WaitReplicasTimeout: 30 * time.Second})
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
	require.ErrorContains(t, err, "no reachable member of the replication group has quorum")
	assert.Empty(t, c.mutatingCalls())
}

// TestEmergencyReparentGroupReplicationRefusesSplitBrainOverride checks that the split-brain
// override, which has no meaning for a group, is refused.
func TestEmergencyReparentGroupReplicationRefusesSplitBrainOverride(t *testing.T) {
	c, ts := newFailedGroupShard(t)
	erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())
	_, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{
		NewPrimaryAlias:          mustAlias(t, alias101),
		AllowSplitBrainPromotion: true,
		WaitReplicasTimeout:      30 * time.Second,
	})
	assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
	assert.Empty(t, c.mutatingCalls())
}

// TestPlannedReparentGroupReplicationRequiresOnlineMember checks that PRS on a group
// replication shard refuses a primary-elect that is not an ONLINE member of the primary's
// group, before it demotes the primary.
func TestPlannedReparentGroupReplicationRequiresOnlineMember(t *testing.T) {
	c, ts := newFakeGRCluster(t, "group_replication", migrationTestShard()...)
	c.formGroup(t, "group_replication")
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
}

// TestPlannedReparentGroupReplicationPreflight checks the group membership preflight of PRS:
// an ONLINE member passes, a RECOVERING member or a primary outside a group fails, a shard
// without a current primary needs an elect in a group with quorum, and an asynchronous
// keyspace makes no FullStatus call at all.
func TestPlannedReparentGroupReplicationPreflight(t *testing.T) {
	tests := []struct {
		name       string
		durability string
		setup      func(c *fakeGRCluster)
		errContain string
		// noCurrentPrimary demotes the primary's tablet record, so that the shard has a
		// primary term but no current primary.
		noCurrentPrimary bool
	}{
		{
			name:       "online member",
			durability: "group_replication",
			setup:      func(c *fakeGRCluster) { c.formGroup(t, "group_replication") },
		},
		{
			name:       "recovering member",
			durability: "group_replication",
			setup: func(c *fakeGRCluster) {
				c.formGroup(t, "group_replication")
				c.tablets[alias200].state = "RECOVERING"
			},
			errContain: "state: RECOVERING",
		},
		{
			name:       "primary not in a group",
			durability: "group_replication",
			errContain: "current primary zone1-0000000100 is not an active member",
		},
		{
			name:             "no current primary, elect is an online member",
			durability:       "group_replication",
			setup:            func(c *fakeGRCluster) { c.formGroup(t, "group_replication") },
			noCurrentPrimary: true,
		},
		{
			name:             "no current primary, no group",
			durability:       "group_replication",
			noCurrentPrimary: true,
			errContain:       "is not an ONLINE member of a replication group with quorum",
		},
		{
			name:       "async keyspace makes no FullStatus call",
			durability: "semi_sync",
			setup: func(c *fakeGRCluster) {
				for _, ft := range c.tablets {
					ft.unreachable = true
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, ts := newFakeGRCluster(t, tt.durability, migrationTestShard()...)
			if tt.setup != nil {
				tt.setup(c)
			}
			d, err := policy.GetDurabilityPolicy(tt.durability)
			require.NoError(t, err)
			tabletMap, err := ts.GetTabletMapForShard(t.Context(), "ks", "-")
			require.NoError(t, err)
			if tt.noCurrentPrimary {
				tabletMap[aliasP].Type = topodatapb.TabletType_REPLICA
			}
			si, err := ts.GetShard(t.Context(), "ks", "-")
			require.NoError(t, err)

			pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())
			ev := &events.Reparent{ShardInfo: *si}
			opts := &PlannedReparentOptions{NewPrimaryAlias: mustAlias(t, alias200), durability: d}
			isNoop, err := pr.preflightChecks(t.Context(), ev, tabletMap, nil, opts)
			if tt.errContain != "" {
				require.Error(t, err)
				assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
				require.ErrorContains(t, err, tt.errContain)
				return
			}
			require.NoError(t, err)
			assert.False(t, isNoop)
			assert.Equal(t, alias200, topoproto.TabletAliasString(ev.NewPrimary.Alias))
		})
	}
}

// TestPlannedReparentGroupReplicationRequiresVoter checks that PRS refuses to promote a
// tablet that is not a listed voter, and names the voters, before it changes anything.
func TestPlannedReparentGroupReplicationRequiresVoter(t *testing.T) {
	c, ts := newFakeGRCluster(t, "group_replication_cross_cell", migrationTestShard()...)
	c.formGroup(t, "group_replication_cross_cell")
	require.Equal(t, []string{aliasP, alias200, alias300}, c.voters(t))
	pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

	_, err := pr.ReparentShard(t.Context(), "ks", "-", PlannedReparentOptions{
		NewPrimaryAlias:     mustAlias(t, alias101),
		WaitReplicasTimeout: 30 * time.Second,
	})
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	require.ErrorContains(t, err, "only voters can be promoted (voters: zone1-0000000100, zone2-0000000200, zone3-0000000300)")
	assert.Empty(t, c.mutatingCalls())
}

// TestPlannedReparentGroupReplicationInitialPromotionStoresVoters checks that the initial
// promotion of a group replication shard stores the voters, with the primary-elect among
// them, before InitPrimary bootstraps the group.
func TestPlannedReparentGroupReplicationInitialPromotionStoresVoters(t *testing.T) {
	c, ts := newFakeGRCluster(t, "group_replication_cross_cell",
		fakeGRTabletSpec{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
		fakeGRTabletSpec{cell: "zone3", uid: 300, tabletType: topodatapb.TabletType_REPLICA},
	)
	pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

	_, err := pr.ReparentShard(t.Context(), "ks", "-", PlannedReparentOptions{
		NewPrimaryAlias:     mustAlias(t, alias101),
		WaitReplicasTimeout: 30 * time.Second,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"InitPrimary(" + alias101 + ")"}, callsWithPrefix(c.mutatingCalls(), "InitPrimary"))
	assert.Equal(t, "zone1-0000000101, zone2-0000000200, zone3-0000000300", c.votersAtInitPrimary)
	assert.Equal(t, []string{alias101, alias200, alias300}, c.voters(t))
	assert.Equal(t, "1790000001", c.recordedIncarnation(t), "the group InitPrimary bootstrapped is recorded as the shard's legitimate group")
}

// TestEmergencyReparentGroupReplicationFollowsOnlyLegitimateGroup checks that ERS does not follow
// a member that is the ONLINE primary of a group with quorum in its own view, when that group is
// not the shard's legitimate group: the member formed a new group incarnation on its own (S7d in
// the Group Replication failover audit), or the group shrank below a majority of the shard's
// voters. Without incarnation and voters in the shard record, the view quorum still decides.
func TestEmergencyReparentGroupReplicationFollowsOnlyLegitimateGroup(t *testing.T) {
	tests := []struct {
		name        string
		incarnation string
		voters      []string
		// memberIncarnation is the incarnation the lone member reports.
		memberIncarnation string
		wantPrimary       string
	}{{
		name:              "member alone in a new incarnation",
		incarnation:       "1790000000",
		voters:            []string{aliasP, alias101, alias200},
		memberIncarnation: "1799999999",
	}, {
		name:              "member alone in the recorded incarnation",
		incarnation:       "1790000000",
		voters:            []string{aliasP, alias101, alias200},
		memberIncarnation: "1790000000",
	}, {
		name:              "nothing recorded",
		memberIncarnation: "1799999999",
		wantPrimary:       alias200,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, ts := newFailedGroupShard(t)
			// zone2's member is alone in its group: the others left it.
			c.tablets[aliasP].member = false
			c.tablets[alias101].member = false
			c.tablets[alias200].incarnation = tt.memberIncarnation
			c.setIncarnation(t, tt.incarnation)
			c.setVoters(t, tt.voters...)
			erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())

			ev, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{WaitReplicasTimeout: 30 * time.Second})
			if tt.wantPrimary != "" {
				require.NoError(t, err)
				assert.Equal(t, tt.wantPrimary, topoproto.TabletAliasString(ev.NewPrimary.Alias))
				return
			}
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
			assert.Empty(t, c.mutatingCalls())
		})
	}
}

// TestPlannedReparentGroupReplicationRequiresLegitimateGroup checks that PRS refuses to switch
// primaries inside a group that is not the shard's legitimate group.
func TestPlannedReparentGroupReplicationRequiresLegitimateGroup(t *testing.T) {
	t.Run("foreign incarnation", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "group_replication", migrationTestShard()...)
		c.formGroup(t, "group_replication")
		c.setIncarnation(t, "1780000000")
		pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

		_, err := pr.ReparentShard(t.Context(), "ks", "-", PlannedReparentOptions{
			NewPrimaryAlias:     mustAlias(t, alias200),
			WaitReplicasTimeout: 30 * time.Second,
		})
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, `is in group incarnation "1790000000", but the shard record's group incarnation is "1780000000"`)
		assert.Empty(t, c.mutatingCalls())
	})
	t.Run("minority of the voters", func(t *testing.T) {
		c, ts := newFakeGRCluster(t, "group_replication", migrationTestShard()...)
		c.formGroup(t, "group_replication")
		c.setIncarnation(t, "1790000000")
		require.Equal(t, []string{aliasP, alias101, alias200, alias300}, c.voters(t))
		// Two of the four voters left the group: the other two still have quorum in a view of two.
		c.tablets[alias101].member = false
		c.tablets[alias300].member = false
		pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

		_, err := pr.ReparentShard(t.Context(), "ks", "-", PlannedReparentOptions{
			NewPrimaryAlias:     mustAlias(t, alias200),
			WaitReplicasTimeout: 30 * time.Second,
		})
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, "only 2 of the 4 voters of the shard are ONLINE")
		assert.Empty(t, c.mutatingCalls())
	})
}

// TestEmergencyReparentGroupReplicationRepointsNonVoters checks that ERS repoints a REPLICA
// tablet that is not a listed voter as an asynchronous replica of the new primary, while the
// failed voter is left to rejoin the group.
func TestEmergencyReparentGroupReplicationRepointsNonVoters(t *testing.T) {
	c, ts := newFakeGRCluster(t, "group_replication_cross_cell", migrationTestShard()...)
	c.formGroup(t, "group_replication_cross_cell")
	require.Equal(t, []string{aliasP, alias200, alias300}, c.voters(t))
	c.tablets[aliasP].unreachable = true
	c.groupPrimary = alias200
	erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())

	ev, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{WaitReplicasTimeout: 30 * time.Second})
	require.NoError(t, err)
	assert.Equal(t, alias200, topoproto.TabletAliasString(ev.NewPrimary.Alias))
	assert.ElementsMatch(t, []string{
		"SetReplicationSource(" + alias101 + ", " + alias200 + ", semiSync=false)",
		"SetReplicationSource(" + alias102 + ", " + alias200 + ", semiSync=false)",
		"SetReplicationSource(" + alias300 + ", " + alias200 + ", semiSync=false)",
	}, callsWithPrefix(c.mutatingCalls(), "SetReplicationSource"))
	assert.Equal(t, alias200, c.tablet(alias101).source)
	assert.False(t, c.tablet(alias101).member)
}

// TestEmergencyReparentGroupReplicationPromotesOnlyVoters checks that ERS makes only a listed voter the
// shard's primary: a tablet that is not one does not serve as PRIMARY (the tablet's serving rule), so ERS
// would report success while nothing serves. When the group elected a member that is not a voter, ERS
// moves the group's primary to an eligible voter; a requested primary that is not a voter is refused
// before anything changes.
func TestEmergencyReparentGroupReplicationPromotesOnlyVoters(t *testing.T) {
	newShard := func(t *testing.T) (*fakeGRCluster, *topo.Server) {
		c, ts := newFakeGRCluster(t, "group_replication",
			fakeGRTabletSpec{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
			fakeGRTabletSpec{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
			fakeGRTabletSpec{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
		)
		c.formGroup(t, "group_replication")
		// zone1-101 completed a join after the voter list dropped it, and the group elected it.
		c.setVoters(t, aliasP, alias200)
		c.groupPrimary = alias101
		return c, ts
	}
	t.Run("the group's primary is not a voter", func(t *testing.T) {
		c, ts := newShard(t)
		erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())
		ev, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{WaitReplicasTimeout: 30 * time.Second})
		require.NoError(t, err)
		assert.Contains(t, []string{aliasP, alias200}, topoproto.TabletAliasString(ev.NewPrimary.Alias))
		assert.NotContains(t, callsWithPrefix(c.mutatingCalls(), "PromoteReplica"), "PromoteReplica("+alias101+")")
	})
	t.Run("a requested primary that is not a voter", func(t *testing.T) {
		c, ts := newShard(t)
		erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())
		_, err := erp.ReparentShard(t.Context(), "ks", "-", EmergencyReparentOptions{
			NewPrimaryAlias:     mustAlias(t, alias101),
			WaitReplicasTimeout: 30 * time.Second,
		})
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, "not a voter")
		assert.Empty(t, c.mutatingCalls())
	})
}
