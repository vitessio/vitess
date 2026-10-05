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

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestPlannedReparentGroupReplicationInitialPromotionAfterBootstrap reproduces the TLA+ model's init_orc
// trace (doc/design-docs/group_replication_tla): VTOrc bootstraps the group of a shard that never had a
// primary on its own (GroupVotersOutOfDate selects the voters, GroupNotBootstrapped bootstraps one of
// them) and records its incarnation. Until a majority of the voters joined it, no tablet is PRIMARY, and
// the shard record has no primary term: a PlannedReparentShard that got the shard lock after VTOrc's
// recovery takes the initial promotion path, and InitPrimary bootstraps a second group on its elect,
// which it records over VTOrc's (the compare-and-swap expects the incarnation PRS read). A shard whose
// record lists an incarnation has a group: the initial promotion must not bootstrap another one.
//
// The same holds while a bootstrap intent of VTOrc is live and no incarnation is recorded yet: VTOrc's
// bootstrap RPC failed while MySQL's START still ran, and VTOrc adopts its group later. The intent
// fences a bootstrap on another tablet; InitPrimary's bootstrap ignored it.
//
// The first subtest relies on the fake MySQL, in which no tablet is a member of VTOrc's group and none
// logged a GTID for it. On MySQL 8.4.11, with group_replication_view_change_uuid left at AUTOMATIC as
// Vitess leaves it, neither a bootstrap nor a join logs a view-change GTID either (raw lab of three
// instances, outside Vitess), so the elect's GTID checks do not see VTOrc's group; only the recorded
// incarnation, or the active member that VTOrc's bootstrap target is while its MySQL runs the group,
// does. The second subtest is the race that remains whatever MySQL logs: VTOrc's bootstrap RPC timed
// out while MySQL's START still ran, so that no incarnation is recorded and no tablet reports an
// active member yet, and only the live intent tells that a group is being created. A guard on the live
// intent alone is not enough (the model's init_orc_vgtid: the intent expired, and a voter joined
// VTOrc's unrecorded group and left it): the initial promotion also refuses while any tablet is an
// active member (TestPlannedReparentGroupReplicationInitialPromotionRefusals).
func TestPlannedReparentGroupReplicationInitialPromotionAfterBootstrap(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(t *testing.T, c *fakeGRCluster)
	}{{
		name: "VTOrc recorded the incarnation of the group it bootstrapped",
		setup: func(t *testing.T, c *fakeGRCluster) {
			c.setIncarnation(t, "1780000001")
		},
	}, {
		name: "a bootstrap intent of VTOrc is live",
		setup: func(t *testing.T, c *fakeGRCluster) {
			_, err := c.ts.UpdateShardFields(t.Context(), c.keyspace, "-", func(si *topo.ShardInfo) error {
				si.GroupReplicationBootstrapIntent = &topodatapb.GroupReplicationBootstrapIntent{
					Target: mustAlias(t, alias200),
					Time:   protoutil.TimeToProto(time.Now()),
					Token:  "1780000001-0123456789abcdef",
				}
				return nil
			})
			require.NoError(t, err)
		},
	}} {
		t.Run(tt.name, func(t *testing.T) {
			c, ts := newFakeGRCluster(t, "group_replication_cross_cell",
				fakeGRTabletSpec{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
				fakeGRTabletSpec{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
				fakeGRTabletSpec{cell: "zone3", uid: 300, tabletType: topodatapb.TabletType_REPLICA},
			)
			c.setVoters(t, alias101, alias200, alias300)
			tt.setup(t, c)
			before := c.recordedIncarnation(t)
			pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

			_, err := pr.ReparentShard(t.Context(), "ks", "-", PlannedReparentOptions{
				NewPrimaryAlias:     mustAlias(t, alias101),
				WaitReplicasTimeout: 30 * time.Second,
			})
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			assert.Empty(t, callsWithPrefix(c.mutatingCalls(), "InitPrimary"), "InitPrimary must not bootstrap a second group")
			assert.Equal(t, before, c.recordedIncarnation(t), "the shard record's incarnation must stay as it is")
		})
	}
}

// TestPlannedReparentGroupReplicationInitialPromotionRefusals checks the errors with which the initial
// promotion of PlannedReparentShard refuses to bootstrap a group on a shard that has one, or is getting
// one, and that it still initializes a shard whose bootstrap intent expired without any group.
func TestPlannedReparentGroupReplicationInitialPromotionRefusals(t *testing.T) {
	intent := func(age time.Duration) func(t *testing.T, c *fakeGRCluster) {
		return func(t *testing.T, c *fakeGRCluster) {
			_, err := c.ts.UpdateShardFields(t.Context(), c.keyspace, "-", func(si *topo.ShardInfo) error {
				si.GroupReplicationBootstrapIntent = &topodatapb.GroupReplicationBootstrapIntent{
					Target: mustAlias(t, alias200),
					Time:   protoutil.TimeToProto(time.Now().Add(-age)),
					Token:  "1780000001-0123456789abcdef",
				}
				return nil
			})
			require.NoError(t, err)
		}
	}
	member := func(state string) func(t *testing.T, c *fakeGRCluster) {
		return func(t *testing.T, c *fakeGRCluster) {
			c.mu.Lock()
			defer c.mu.Unlock()
			ft := c.tablets[alias200]
			ft.member = true
			ft.state = state
			c.groupPrimary = alias200
		}
	}
	tests := []struct {
		name  string
		setup func(t *testing.T, c *fakeGRCluster)
		// wantErr is a part of the error; empty means that the initial promotion runs.
		wantErr string
	}{{
		name:    "a bootstrap intent is live",
		setup:   intent(0),
		wantErr: "shard ks/- has a live bootstrap intent for its replication group, on tablet zone2-0000000200 since ",
	}, {
		name:    "an incarnation is recorded",
		setup:   func(t *testing.T, c *fakeGRCluster) { c.setIncarnation(t, "1780000001") },
		wantErr: "shard ks/- already has a replication group: the shard record lists its incarnation 1780000001",
	}, {
		name:    "a voter is an ONLINE member of a group",
		setup:   member(""),
		wantErr: "tablet zone2-0000000200 of shard ks/- is already ONLINE in replication group " + policy.GroupName("ks", "-") + " (view 1790000000:1): the shard has a group",
	}, {
		name:    "a voter is a RECOVERING member of a group",
		setup:   member(mysql.GroupMemberStateRecovering),
		wantErr: "tablet zone2-0000000200 of shard ks/- is already RECOVERING in replication group",
	}, {
		name:  "the bootstrap intent expired, and no tablet is in a group",
		setup: intent(GroupReplicationBootstrapIntentFence + time.Second),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, ts := newFakeGRCluster(t, "group_replication_cross_cell",
				fakeGRTabletSpec{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
				fakeGRTabletSpec{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
				fakeGRTabletSpec{cell: "zone3", uid: 300, tabletType: topodatapb.TabletType_REPLICA},
			)
			c.setVoters(t, alias101, alias200, alias300)
			tt.setup(t, c)
			pr := NewPlannedReparenter(ts, c, logutil.NewMemoryLogger())

			_, err := pr.ReparentShard(t.Context(), "ks", "-", PlannedReparentOptions{
				NewPrimaryAlias:     mustAlias(t, alias101),
				WaitReplicasTimeout: 30 * time.Second,
			})
			if tt.wantErr == "" {
				require.NoError(t, err)
				assert.Equal(t, []string{"InitPrimary(" + alias101 + ")"}, callsWithPrefix(c.mutatingCalls(), "InitPrimary"))
				return
			}
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			require.ErrorContains(t, err, tt.wantErr)
			require.ErrorContains(t, err, "wait until the group's primary tablet is PRIMARY (VTOrc promotes it), then run PlannedReparentShard again")
			assert.Empty(t, c.mutatingCalls(), "the initial promotion must change nothing")
			assert.Equal(t, []string{alias101, alias200, alias300}, c.voters(t))
		})
	}
}
