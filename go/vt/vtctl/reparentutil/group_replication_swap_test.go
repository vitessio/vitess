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
	"context"
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

// newSwapShard is a group replication shard of three voters, the primary zone1-0000000100, zone2-0000000200 and
// zone3-0000000300, all ONLINE in the primary's view, with replicas zone1-0000000101 and zone2-0000000201 that
// are not voters, and an RDONLY tablet zone1-0000000102. Every tablet executed the same transactions.
func newSwapShard(t *testing.T) (*fakeGRCluster, *topo.Server) {
	c, ts := newFakeGRCluster(t, policy.DurabilityGroupReplicationCrossCell,
		append(migrationTestShard(), fakeGRTabletSpec{cell: "zone2", uid: 201, tabletType: topodatapb.TabletType_REPLICA})...)
	c.formGroup(t, policy.DurabilityGroupReplicationCrossCell)
	require.Equal(t, []string{aliasP, alias200, alias300}, c.voters(t))
	c.setIncarnation(t, c.incarnation)
	for _, ft := range c.tablets {
		ft.executed = gtids("1-100")
	}
	return c, ts
}

// planSwap runs PRS's preflight checks for the given primary-elect, and returns the swap it planned.
func planSwap(t *testing.T, c *fakeGRCluster, ts *topo.Server, elect string) (*groupSwapPlan, error) {
	tabletMap, err := ts.GetTabletMapForShard(t.Context(), "ks", "-")
	require.NoError(t, err)
	si, err := ts.GetShard(t.Context(), "ks", "-")
	require.NoError(t, err)
	d, err := policy.GetDurabilityPolicy(policy.DurabilityGroupReplicationCrossCell)
	require.NoError(t, err)
	pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())
	opts := &PlannedReparentOptions{NewPrimaryAlias: mustAlias(t, elect), durability: d, WaitReplicasTimeout: 30 * time.Second}
	_, err = pr.preflightChecks(t.Context(), &events.Reparent{ShardInfo: *si}, tabletMap, nil, opts)
	return opts.grSwap, err
}

func aliasStrings(aliases []*topodatapb.TabletAlias) []string {
	var out []string
	for _, alias := range aliases {
		out = append(out, topoproto.TabletAliasString(alias))
	}
	return out
}

// TestPlannedReparentGroupReplicationSwapPlan checks how PRS promotes a tablet that is not a voter: it takes the
// seat of the voter of its cell, after the demotion when that voter is the current primary, or a new seat when
// its cell has no voter; and the checks that refuse the swap before anything changes.
func TestPlannedReparentGroupReplicationSwapPlan(t *testing.T) {
	tests := []struct {
		name            string
		elect           string
		setup           func(t *testing.T, c *fakeGRCluster)
		wantReplaced    string
		wantAfterDemote bool
		wantVoters      []string
		wantCode        vtrpcpb.Code
		wantErr         string
	}{{
		name:         "a replica of a cell whose voter is not the primary",
		elect:        alias201,
		wantReplaced: alias200,
		wantVoters:   []string{aliasP, alias201, alias300},
	}, {
		name:            "a replica of the primary's cell",
		elect:           alias101,
		wantReplaced:    aliasP,
		wantAfterDemote: true,
		wantVoters:      []string{alias101, alias200, alias300},
	}, {
		name:  "a replica of a cell without a voter",
		elect: alias300,
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.setVoters(t, aliasP, alias200)
			c.tablets[alias300].member = false
		},
		wantVoters: []string{aliasP, alias200, alias300},
	}, {
		name:     "an RDONLY tablet",
		elect:    alias102,
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "is a RDONLY tablet, which the durability policy does not allow as a voter",
	}, {
		name:  "a replica that is in the group already",
		elect: alias201,
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.tablets[alias201].member = true
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "is not a voter, but its MySQL is ONLINE in a replication group",
	}, {
		name:  "a replica that executed a transaction the primary lacks",
		elect: alias201,
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.tablets[alias201].executed = gtids("1-101")
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "executed transactions that the current primary zone1-0000000100 lacks (" + gtids("101") + ")",
	}, {
		name:  "the new list would lack a majority in the primary's view",
		elect: alias201,
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.tablets[alias300].unreachable = true
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "with 1 of the 3 voters of the new list [zone1-0000000100, zone2-0000000201, zone3-0000000300] ONLINE before zone2-0000000201 joins, not a majority",
	}, {
		name:  "a group of a single voter",
		elect: alias200,
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.setVoters(t, aliasP)
			c.tablets[alias200].member = false
			c.tablets[alias300].member = false
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "the group has a single voter (zone1-0000000100)",
	}, {
		name:  "the primary's election is in progress",
		elect: alias201,
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.electionInProgress = true
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "the election of the group primary zone1-0000000100 is in progress",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, ts := newSwapShard(t)
			if tt.setup != nil {
				tt.setup(t, c)
			}
			votersBefore := c.voters(t)
			plan, err := planSwap(t, c, ts, tt.elect)
			assert.Empty(t, c.mutatingCalls())
			assert.Equal(t, votersBefore, c.voters(t))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Equal(t, tt.wantCode, vterrors.Code(err))
				return
			}
			require.NoError(t, err)
			require.NotNil(t, plan)
			if tt.wantReplaced == "" {
				assert.Nil(t, plan.replaced)
			} else {
				assert.Equal(t, tt.wantReplaced, topoproto.TabletAliasString(plan.replaced))
			}
			assert.Equal(t, tt.wantAfterDemote, plan.afterDemote)
			assert.Equal(t, tt.wantVoters, aliasStrings(plan.newVoters))
		})
	}
}

// lockShard locks the shard for the test, and returns the locked context.
func lockShard(t *testing.T, ts *topo.Server) context.Context {
	lockCtx, unlock, err := ts.LockShard(t.Context(), "ks", "-", "test")
	require.NoError(t, err)
	t.Cleanup(func() { unlock(&err) })
	return lockCtx
}

// TestPlannedReparentGroupReplicationSwapIn checks the swap of a primary-elect for a voter other than the
// primary: a compare-and-swap of the voters, then the elect joins the group. A concurrent change of the voters
// fails the write, and the elect does not join.
func TestPlannedReparentGroupReplicationSwapIn(t *testing.T) {
	t.Run("the elect takes the seat and joins", func(t *testing.T) {
		c, ts := newSwapShard(t)
		plan, err := planSwap(t, c, ts, alias201)
		require.NoError(t, err)
		si, err := ts.GetShard(t.Context(), "ks", "-")
		require.NoError(t, err)
		ev := &events.Reparent{ShardInfo: *si}
		pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

		require.NoError(t, pr.swapInElect(lockShard(t, ts), ev, "ks", "-", plan, PlannedReparentOptions{WaitReplicasTimeout: 30 * time.Second}))
		assert.Equal(t, []string{aliasP, alias201, alias300}, c.voters(t))
		assert.Equal(t, []string{aliasP, alias201, alias300}, aliasStrings(ev.ShardInfo.GroupReplicationVoters))
		assert.Equal(t, []string{"StartGroupReplication(" + alias201 + ")"}, c.mutatingCalls())
		assert.True(t, c.tablet(alias201).member)
	})
	t.Run("the voters changed concurrently", func(t *testing.T) {
		c, ts := newSwapShard(t)
		plan, err := planSwap(t, c, ts, alias201)
		require.NoError(t, err)
		c.setVoters(t, alias101, alias200, alias300)
		si, err := ts.GetShard(t.Context(), "ks", "-")
		require.NoError(t, err)
		pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

		err = pr.swapInElect(lockShard(t, ts), &events.Reparent{ShardInfo: *si}, "ks", "-", plan, PlannedReparentOptions{WaitReplicasTimeout: 30 * time.Second})
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		assert.Equal(t, []string{alias101, alias200, alias300}, c.voters(t))
		assert.Empty(t, c.mutatingCalls())
	})
}

// TestPlannedReparentGroupReplicationSwapAfterDemote checks the swap of a primary-elect for the demoted primary,
// the voter of its cell: on a fresh read, the new voters, then the join. When the join fails, PRS stops it and
// writes the old voters back, so that the demotion can be undone; but not when a voter left the group meanwhile,
// so that the primary's view lacks a majority of the new voters and would hold one of the old ones. When the
// join's RPC fails although the elect joined, the reparent goes on.
func TestPlannedReparentGroupReplicationSwapAfterDemote(t *testing.T) {
	tests := []struct {
		name         string
		joinFails    bool
		voterLeaves  bool
		electJoined  bool
		wantReverted bool
		wantVoters   []string
	}{{
		name:       "the elect joins",
		wantVoters: []string{alias101, alias200, alias300},
	}, {
		name:         "the join fails",
		joinFails:    true,
		wantReverted: true,
		wantVoters:   []string{aliasP, alias200, alias300},
	}, {
		name:        "the join's RPC fails, but the elect joined",
		joinFails:   true,
		electJoined: true,
		wantVoters:  []string{alias101, alias200, alias300},
	}, {
		name:        "the join fails while a voter leaves the group",
		joinFails:   true,
		voterLeaves: true,
		wantVoters:  []string{alias101, alias200, alias300},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, ts := newSwapShard(t)
			plan, err := planSwap(t, c, ts, alias101)
			require.NoError(t, err)
			require.True(t, plan.afterDemote)
			join := "StartGroupReplication(" + alias101 + ")"
			if tt.joinFails {
				c.failOnce[join] = true
			}
			switch {
			case tt.voterLeaves:
				c.onCall = map[string]func(){join: func() { c.tablets[alias200].member = false }}
			case tt.electJoined:
				c.onCall = map[string]func(){join: func() { c.tablets[alias101].member = true }}
			}
			// DemotePrimary demoted the PRIMARY tablet.
			c.tablets[aliasP].demoted, c.tablets[aliasP].superReadOnly = true, true
			si, err := ts.GetShard(t.Context(), "ks", "-")
			require.NoError(t, err)
			ev := &events.Reparent{ShardInfo: *si}
			pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

			reverted, err := pr.swapAfterDemote(lockShard(t, ts), ev, "ks", "-", plan, c.tabletRecs[aliasP], "MySQL56/"+gtids("1-100"),
				PlannedReparentOptions{WaitReplicasTimeout: 30 * time.Second})
			assert.Equal(t, tt.wantVoters, c.voters(t))
			assert.Equal(t, tt.wantVoters, aliasStrings(ev.ShardInfo.GroupReplicationVoters))
			if !tt.joinFails || tt.electJoined {
				require.NoError(t, err)
				assert.Equal(t, []string{join}, c.mutatingCalls())
				return
			}
			require.ErrorContains(t, err, "primary-elect "+alias101+", now a voter, failed to join the replication group in time")
			assert.Equal(t, tt.wantReverted, reverted)
			assert.Equal(t, []string{join, "StopGroupReplication(" + alias101 + ")"}, c.mutatingCalls())
		})
	}
}

// TestPlannedReparentGroupReplicationSwapWaitsForNewVoters checks that PRS swaps the demoted primary out only once
// the voters of the new list executed every transaction it executed: otherwise a bootstrap from them would lose
// one that only the demoted primary executed. A transaction that a voter only received does not count. It fails without changing the voters when they do not catch up in time,
// and the demotion can be undone.
func TestPlannedReparentGroupReplicationSwapWaitsForNewVoters(t *testing.T) {
	c, ts := newSwapShard(t)
	plan, err := planSwap(t, c, ts, alias101)
	require.NoError(t, err)
	c.tablets[aliasP].executed = gtids("1-120")
	// A voter of the new list only received them: its relay log does not count.
	c.tablets[alias200].received = gtids("101-120")
	si, err := ts.GetShard(t.Context(), "ks", "-")
	require.NoError(t, err)
	pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

	reverted, err := pr.swapAfterDemote(lockShard(t, ts), &events.Reparent{ShardInfo: *si}, "ks", "-", plan, c.tabletRecs[aliasP], "MySQL56/"+gtids("1-120"),
		PlannedReparentOptions{WaitReplicasTimeout: time.Second})
	require.ErrorContains(t, err, "did not execute every transaction that the demoted primary executed")
	require.ErrorContains(t, err, "they lack "+gtids("101-120"))
	assert.Equal(t, vtrpcpb.Code_DEADLINE_EXCEEDED, vterrors.Code(err))
	assert.True(t, reverted, "the voters did not change: the demotion can be undone")
	assert.Equal(t, []string{aliasP, alias200, alias300}, c.voters(t))
	assert.Empty(t, c.mutatingCalls())
}

// TestPlannedReparentGroupReplicationSwapNeedsDemotion checks that PRS drops the demoted primary from the voters only
// while its demotion holds: a tablet whose demotion did not take (it was not PRIMARY when demoted, or its type
// changed since) may serve again, as a primary that is not a voter. The voters do not change.
func TestPlannedReparentGroupReplicationSwapNeedsDemotion(t *testing.T) {
	c, ts := newSwapShard(t)
	plan, err := planSwap(t, c, ts, alias101)
	require.NoError(t, err)
	c.tablets[aliasP].superReadOnly = true
	si, err := ts.GetShard(t.Context(), "ks", "-")
	require.NoError(t, err)
	pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

	reverted, err := pr.swapAfterDemote(lockShard(t, ts), &events.Reparent{ShardInfo: *si}, "ks", "-", plan, c.tabletRecs[aliasP], "MySQL56/"+gtids("1-100"),
		PlannedReparentOptions{WaitReplicasTimeout: 30 * time.Second})
	require.ErrorContains(t, err, "the demotion of the current primary zone1-0000000100 does not hold")
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	assert.True(t, reverted)
	assert.Equal(t, []string{aliasP, alias200, alias300}, c.voters(t))
	assert.Empty(t, c.mutatingCalls())
}
