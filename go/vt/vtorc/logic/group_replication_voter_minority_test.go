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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestUpdateGroupReplicationVotersKeepsSeatsOfMinorityView reproduces the TLA+ model's
// voters_minority trace (doc/design-docs/group_replication_tla): the group's view shrank to its
// primary through clean leaves, which keep MySQL's view quorum, and the two other voters then
// stayed unreachable for longer than the replacement grace period. The primary does not serve: its
// view holds one of the three voters. VTOrc must not write a voter list under which that view
// holds a majority: the primary would serve again, and acknowledge writes on a single voter, which
// is what "Group shrink fails closed" rules out.
func TestUpdateGroupReplicationVotersKeepsSeatsOfMinorityView(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	inst.UnreachableGroupTablets.Reset()
	config.SetGroupReplicationVoterReplacementGracePeriod(0)

	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter2, voter3)
	setVoters(t, primary, voter2, voter3)

	errUnreachable := errors.New("unreachable")
	// The primary's view holds the primary only, with quorum in that view.
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(groupMemberStatus(primary, primary, primary), nil).AnyTimes()
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(nil, errUnreachable).AnyTimes()
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(nil, errUnreachable).AnyTimes()

	analysisEntry := &inst.DetectionAnalysis{
		Analysis:              inst.GroupVotersOutOfDate,
		AnalyzedInstanceAlias: primary.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	_, _, _ = updateGroupReplicationVoters(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
	require.Equal(t, []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"}, readVoters(t),
		"a view that holds one of three voters must not become a majority of the voter list")
}

// TestUpdateGroupReplicationVotersReplacesFailedVoterWithSpare checks that the rule against shrinking
// the voter list to a minority view keeps the replacement of a failed voter: of three voters, one per
// cell, the one of zone3 failed, and the other tablet of zone3 takes its seat and joins the group.
func TestUpdateGroupReplicationVotersReplacesFailedVoterWithSpare(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	inst.UnreachableGroupTablets.Reset()
	config.SetGroupReplicationVoterReplacementGracePeriod(0)

	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	spare3 := recoveryTablet("zone3", 301, topodatapb.TabletType_REPLICA)
	mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter2, voter3, spare3)
	setVoters(t, primary, voter2, voter3)

	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(groupMemberStatus(primary, primary, primary, voter2), nil)
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(groupMemberStatus(voter2, primary, primary, voter2), nil)
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(nil, errors.New("unreachable"))
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(spare3)).Return(notMemberStatus(spare3), nil)
	mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(spare3), startRequest(false)).Return(&replicationdatapb.GroupReplicationStatus{}, nil)

	analysisEntry := &inst.DetectionAnalysis{
		Analysis:              inst.GroupVotersOutOfDate,
		AnalyzedInstanceAlias: primary.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	_, _, err := updateGroupReplicationVoters(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
	require.NoError(t, err)
	assert.Equal(t, []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000301"}, readVoters(t))
}

// TestUpdateGroupReplicationVotersCompareAndSwap checks that the write of the voter list is a
// compare-and-swap on the list that the selection was made against. Here another VTOrc, whose shard
// lock expired, or that took the lock after this one's expired, writes another list while this one
// reads the statuses of the tablets: this one's selection is stale, and must not overwrite it.
func TestUpdateGroupReplicationVotersCompareAndSwap(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	tests := []struct {
		name string
		// concurrent is what the other VTOrc writes.
		concurrent func(si *topo.ShardInfo)
	}{{
		name: "another VTOrc wrote the voters",
		concurrent: func(si *topo.ShardInfo) {
			si.GroupReplicationVoters = []*topodatapb.TabletAlias{{Cell: "zone1", Uid: 101}}
		},
	}, {
		name: "another component recorded a new incarnation",
		concurrent: func(si *topo.ShardInfo) {
			si.GroupReplicationIncarnation = "1780000002"
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst.UnreachableGroupTablets.Reset()
			config.SetGroupReplicationVoterReplacementGracePeriod(0)
			primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
			voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
			spare2 := recoveryTablet("zone2", 201, topodatapb.TabletType_REPLICA)
			mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter2, spare2)
			setVoters(t, primary, voter2)
			var want *topo.ShardInfo

			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(groupMemberStatus(primary, primary, primary), nil)
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(nil, errors.New("unreachable"))
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(spare2)).DoAndReturn(
				func(ctx context.Context, _ *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
					si, err := ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
						tt.concurrent(si)
						return nil
					})
					require.NoError(t, err)
					want = si
					return notMemberStatus(spare2), nil
				})
			mockTMC.EXPECT().StartGroupReplication(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			analysisEntry := &inst.DetectionAnalysis{
				Analysis:              inst.GroupVotersOutOfDate,
				AnalyzedInstanceAlias: primary.Alias,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}
			_, _, err := updateGroupReplicationVoters(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			require.ErrorContains(t, err, "changed concurrently")
			si, err := ts.GetShard(t.Context(), "ks", "0")
			require.NoError(t, err)
			assert.True(t, proto.Equal(want.Shard, si.Shard), "the other component's write must stand")
		})
	}
}
