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

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const forceTestUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

// gtids returns the GTID set of the given intervals of forceTestUUID, such as "1-100".
func gtids(intervals string) string {
	return forceTestUUID + ":" + intervals
}

// newLostMajorityShard is newFailedGroupShard once its group lost its majority: its incarnation is
// recorded, every member left the group, and the voters that are not survivors do not answer. Every
// tablet executed the same transactions.
func newLostMajorityShard(t *testing.T, survivors ...string) (*fakeGRCluster, *topo.Server) {
	c, ts := newFailedGroupShard(t)
	c.setIncarnation(t, c.incarnation)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groupPrimary = ""
	for alias, ft := range c.tablets {
		ft.member = false
		ft.executed = gtids("1-100")
		ft.unreachable = (alias == aliasP || alias == alias200 || alias == alias300) && !slices.Contains(survivors, alias)
	}
	return c, ts
}

func forceOptions() EmergencyReparentOptions {
	return EmergencyReparentOptions{WaitReplicasTimeout: 30 * time.Second, GroupReplicationForceNewGroup: true}
}

// TestEmergencyReparentGroupReplicationForceNewGroup checks the forced reparent of a shard whose group lost its
// majority, with one voter left: it drops the voters that do not answer, bootstraps a new group on the
// surviving voter and records its incarnation, promotes it, and repoints the tablets that are not voters. A
// tablet that holds transactions the new group lacks is reported.
func TestEmergencyReparentGroupReplicationForceNewGroup(t *testing.T) {
	c, ts := newLostMajorityShard(t, alias300)
	c.tablets[alias102].executed = gtids("1-120")
	logger := logutil.NewMemoryLogger()
	erp := NewEmergencyReparenter(ts, c, logger)

	ev, err := erp.ReparentShard(t.Context(), "ks", "-", forceOptions())
	require.NoError(t, err)
	require.NotNil(t, ev.NewPrimary)
	assert.Equal(t, alias300, topoproto.TabletAliasString(ev.NewPrimary.Alias))
	assert.Empty(t, c.violationsSoFar())

	assert.Equal(t, []string{alias300}, c.voters(t))
	assert.Equal(t, "1790000001", c.recordedIncarnation(t))
	si, err := ts.GetShard(t.Context(), "ks", "-")
	require.NoError(t, err)
	assert.Nil(t, CurrentGroupReplicationBootstrapIntent(si.Shard), "recording the bootstrap clears its intent")

	calls := c.mutatingCalls()
	assert.Equal(t, []string{
		"StartGroupReplication(" + alias300 + ", bootstrap)",
		"PromoteReplica(" + alias300 + ")",
		"PopulateReparentJournal(" + alias300 + ")",
	}, calls[:3])
	assert.ElementsMatch(t, []string{
		"SetReplicationSource(" + alias101 + ", " + alias300 + ", semiSync=false)",
		"SetReplicationSource(" + alias102 + ", " + alias300 + ", semiSync=false)",
	}, callsWithPrefix(calls, "SetReplicationSource"))
	assert.Contains(t, logger.String(), "dropping the voters that do not answer ([zone1-0000000100, zone2-0000000200])")
	assert.Contains(t, logger.String(), "tablet zone1-0000000102 holds transactions that the new group lacks ("+gtids("101-120")+")")
}

// TestEmergencyReparentGroupReplicationForceNewGroupCandidate checks which surviving voter the new group is
// bootstrapped on: one that holds every transaction the others executed or received, preferring one that
// executed them all; and that the other surviving voters join the group before the promotion.
func TestEmergencyReparentGroupReplicationForceNewGroupCandidate(t *testing.T) {
	tests := []struct {
		name                     string
		executed200, received200 string
		executed300              string
		wantPrimary, wantJoin    string
		newPrimary               string
		wantErr                  string
		requiredPosition         string
	}{{
		name:        "the voter that executed every transaction",
		executed200: gtids("1-90"), received200: gtids("91-95"),
		executed300: gtids("1-95"),
		wantPrimary: alias300, wantJoin: alias200,
	}, {
		name:        "the only voter that holds every transaction, some only in its relay log",
		executed200: gtids("1-90"), received200: gtids("91-100"),
		executed300: gtids("1-95"),
		wantPrimary: alias200, wantJoin: alias300,
	}, {
		name:        "a requested primary that lacks transactions",
		executed200: gtids("1-90"), executed300: gtids("1-95"),
		newPrimary: alias200,
		wantErr:    "requested primary " + alias200 + " cannot be promoted: voter " + alias200 + " lacks " + gtids("91-95"),
	}, {
		name:        "a required position that no surviving voter holds",
		executed200: gtids("1-90"), executed300: gtids("1-95"),
		requiredPosition: gtids("1-96"),
		wantErr:          "no surviving voter executed or received the required position " + gtids("1-96"),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, ts := newLostMajorityShard(t, alias200, alias300)
			c.tablets[alias200].executed, c.tablets[alias200].received = tt.executed200, tt.received200
			c.tablets[alias300].executed = tt.executed300
			erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())
			opts := forceOptions()
			if tt.newPrimary != "" {
				opts.NewPrimaryAlias = mustAlias(t, tt.newPrimary)
			}
			if tt.requiredPosition != "" {
				pos, err := replication.DecodePosition("MySQL56/" + tt.requiredPosition)
				require.NoError(t, err)
				opts.RequiredPosition = pos
			}

			ev, err := erp.ReparentShard(t.Context(), "ks", "-", opts)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
				assert.Empty(t, c.mutatingCalls())
				assert.Equal(t, []string{aliasP, alias200, alias300}, c.voters(t))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantPrimary, topoproto.TabletAliasString(ev.NewPrimary.Alias))
			assert.Equal(t, []string{alias200, alias300}, c.voters(t))
			assert.Equal(t, []string{
				"StartGroupReplication(" + tt.wantPrimary + ", bootstrap)",
				"StartGroupReplication(" + tt.wantJoin + ")",
				"PromoteReplica(" + tt.wantPrimary + ")",
			}, c.mutatingCalls()[:3])
			assert.Empty(t, c.violationsSoFar())
		})
	}
}

// TestEmergencyReparentGroupReplicationForceNewGroupRefusals checks that the forced reparent changes nothing,
// neither the voters nor any tablet, when a group may still run, when it would lose more than what only the
// voters that do not answer held, or when it is not needed.
func TestEmergencyReparentGroupReplicationForceNewGroupRefusals(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T, c *fakeGRCluster)
		opts     func(opts *EmergencyReparentOptions)
		wantCode vtrpcpb.Code
		wantErr  string
	}{{
		name: "the group has quorum",
		setup: func(t *testing.T, c *fakeGRCluster) {
			for _, alias := range []string{aliasP, alias200, alias300} {
				c.tablets[alias].member = true
			}
			c.tablets[alias200].unreachable = false
			c.groupPrimary = alias200
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "the replication group has quorum",
	}, {
		name: "a member without quorum is still in the group",
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.tablets[alias300].member = true
			c.tablets[aliasP].member = true
			c.groupPrimary = aliasP
			c.noQuorum = true
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "a group still runs",
	}, {
		name: "a START GROUP_REPLICATION runs",
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.tablets[alias300].startInProgress = true
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "a START GROUP_REPLICATION runs on the MySQL of tablet " + alias300,
	}, {
		name: "every voter answers",
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.tablets[aliasP].unreachable = false
			c.tablets[alias200].unreachable = false
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "every voter of the shard answers",
	}, {
		name: "no voter answers",
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.tablets[alias300].unreachable = true
		},
		wantCode: vtrpcpb.Code_UNAVAILABLE,
		wantErr:  "no voter of the shard answers",
	}, {
		name:     "an ignored voter",
		opts:     func(opts *EmergencyReparentOptions) { opts.IgnoreReplicas = sets.New(alias300) },
		wantCode: vtrpcpb.Code_INVALID_ARGUMENT,
		wantErr:  "voter " + alias300 + " is ignored",
	}, {
		name: "no incarnation recorded",
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.setIncarnation(t, "")
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "the shard record lists no group replication incarnation or no voters",
	}, {
		name: "a live bootstrap intent",
		setup: func(t *testing.T, c *fakeGRCluster) {
			lockCtx, unlock, err := c.ts.LockShard(t.Context(), "ks", "-", "test")
			require.NoError(t, err)
			_, err = WriteGroupReplicationBootstrapIntent(lockCtx, c.ts, "ks", "-", mustAlias(t, alias300), c.incarnation, time.Now())
			unlock(&err)
			require.NoError(t, err)
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "a bootstrap of the replication group on " + alias300 + " started at",
	}, {
		name: "a surviving voter that does not run group replication",
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.tablets[alias300].grEnabled = false
		},
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "tablet " + alias300 + " does not run Group Replication",
	}, {
		name:     "a requested primary that is not a surviving voter",
		opts:     func(opts *EmergencyReparentOptions) { opts.NewPrimaryAlias = mustAlias(t, alias101) },
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "requested primary " + alias101 + " is not a voter that answers",
	}, {
		name:     "cross-cell promotion prevented",
		opts:     func(opts *EmergencyReparentOptions) { opts.PreventCrossCellPromotion = true },
		wantCode: vtrpcpb.Code_FAILED_PRECONDITION,
		wantErr:  "voter " + alias300 + " is not in the cell of the previous primary " + aliasP,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, ts := newLostMajorityShard(t, alias300)
			if tt.setup != nil {
				tt.setup(t, c)
			}
			votersBefore := c.voters(t)
			incarnationBefore := c.recordedIncarnation(t)
			erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())
			opts := forceOptions()
			if tt.opts != nil {
				tt.opts(&opts)
			}

			_, err := erp.ReparentShard(t.Context(), "ks", "-", opts)
			require.ErrorContains(t, err, tt.wantErr)
			assert.Equal(t, tt.wantCode, vterrors.Code(err))
			assert.Empty(t, c.mutatingCalls())
			assert.Equal(t, votersBefore, c.voters(t))
			assert.Equal(t, incarnationBefore, c.recordedIncarnation(t))
		})
	}
}

// TestEmergencyReparentGroupReplicationForceNewGroupRefusedBootstrap checks a bootstrap that the candidate
// refuses definitively (its MySQL lost a required transaction since it was read): the intent is withdrawn,
// the incarnation stays, and the shard keeps the surviving voters, from which VTOrc bootstraps the group.
func TestEmergencyReparentGroupReplicationForceNewGroupRefusedBootstrap(t *testing.T) {
	c, ts := newLostMajorityShard(t, alias300)
	c.tablets[alias300].refuseBootstrap = true
	erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())

	_, err := erp.ReparentShard(t.Context(), "ks", "-", forceOptions())
	require.ErrorContains(t, err, alias300+" refused to bootstrap the new replication group")
	assert.Equal(t, []string{"StartGroupReplication(" + alias300 + ", bootstrap)"}, c.mutatingCalls())
	assert.Equal(t, []string{alias300}, c.voters(t))
	assert.Equal(t, "1790000000", c.recordedIncarnation(t))
	si, err := ts.GetShard(t.Context(), "ks", "-")
	require.NoError(t, err)
	assert.Nil(t, CurrentGroupReplicationBootstrapIntent(si.Shard))
}

// TestWriteForcedGroupVotersCompareAndSwap checks that the voter write of a forced reparent refuses to
// overwrite voters or an incarnation that changed since its read.
func TestWriteForcedGroupVotersCompareAndSwap(t *testing.T) {
	c, ts := newLostMajorityShard(t, alias300)
	plan := &forcedGroupPlan{
		incarnation: c.incarnation,
		voters:      []*topodatapb.TabletAlias{mustAlias(t, aliasP), mustAlias(t, alias200), mustAlias(t, alias300)},
		survivors:   []*topodatapb.TabletAlias{mustAlias(t, alias300)},
	}
	lockCtx, unlock, err := ts.LockShard(t.Context(), "ks", "-", "test")
	require.NoError(t, err)
	t.Cleanup(func() { unlock(&err) })

	c.setVoters(t, alias101, alias200, alias300)
	err = writeForcedGroupVoters(lockCtx, ts, "ks", "-", plan)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	assert.Equal(t, []string{alias101, alias200, alias300}, c.voters(t))

	c.setVoters(t, aliasP, alias200, alias300)
	c.setIncarnation(t, "1790000005")
	err = writeForcedGroupVoters(lockCtx, ts, "ks", "-", plan)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	assert.Equal(t, []string{aliasP, alias200, alias300}, c.voters(t))

	c.setIncarnation(t, c.incarnation)
	require.NoError(t, writeForcedGroupVoters(lockCtx, ts, "ks", "-", plan))
	assert.Equal(t, []string{alias300}, c.voters(t))
}

// TestEmergencyReparentForceNewGroupRequiresGroupReplication checks that the forced reparent is refused on a
// shard whose durability policy does not use group replication.
func TestEmergencyReparentForceNewGroupRequiresGroupReplication(t *testing.T) {
	c, ts := newFakeGRCluster(t, "semi_sync",
		fakeGRTabletSpec{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
		fakeGRTabletSpec{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
	)
	erp := NewEmergencyReparenter(ts, c, logutil.NewMemoryLogger())

	_, err := erp.ReparentShard(t.Context(), "ks", "-", forceOptions())
	require.ErrorContains(t, err, "--group-replication-force-new-group applies only to a shard whose durability policy uses group replication")
	assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
	assert.Empty(t, c.mutatingCalls())
}
