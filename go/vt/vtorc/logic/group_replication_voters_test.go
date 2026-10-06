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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
	tmcmock "vitess.io/vitess/go/vt/vttablet/tmclient/mock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// voterTestIncarnation is the incarnation of the shard's group in the voter recovery tests.
const voterTestIncarnation = "1790000001"

// settledMember returns the status of an ONLINE member of the recorded incarnation, whose primary is
// groupPrimary, whose view holds the online tablets ONLINE, and that executed the given transactions.
func settledMember(tablet, groupPrimary *topodatapb.Tablet, executed string, online ...*topodatapb.Tablet) *replicationdatapb.FullStatus {
	status := withPosition(groupMemberStatus(tablet, groupPrimary, online...), executed)
	status.GroupReplicationStatus.ViewId = voterTestIncarnation + ":2"
	return status
}

// spareStatus returns the status of a tablet whose MySQL is not a group member, and that executed
// the given transactions.
func spareStatus(tablet *topodatapb.Tablet, executed string) *replicationdatapb.FullStatus {
	return withPosition(notMemberStatus(tablet), executed)
}

// lockedShard locks shard ks/0, as the recovery framework does before it runs a recovery, and returns
// the context that holds the lock.
func lockedShard(t *testing.T) context.Context {
	ctx, unlock, err := ts.LockShard(t.Context(), "ks", "0", "test")
	require.NoError(t, err)
	t.Cleanup(func() {
		var err error
		unlock(&err)
	})
	return ctx
}

func voterRecoveryEntry(tablet *topodatapb.Tablet, code inst.AnalysisCode) *inst.DetectionAnalysis {
	return &inst.DetectionAnalysis{Analysis: code, AnalyzedInstanceAlias: tablet.Alias, AnalyzedKeyspace: "ks", AnalyzedShard: "0"}
}

// TestUpdateGroupReplicationVoters checks the changes that the voter recovery writes, each decided on
// its one read of the shard under the shard lock, and the ones it refuses.
func TestUpdateGroupReplicationVoters(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	errUnreachable := errors.New("unreachable")

	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	spare2 := recoveryTablet("zone2", 201, topodatapb.TabletType_REPLICA)
	voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	spare3 := recoveryTablet("zone3", 301, topodatapb.TabletType_REPLICA)

	tests := []struct {
		name string
		// tablets have a tablet record; voters is the recorded list, which may list others.
		tablets     []*topodatapb.Tablet
		voters      []*topodatapb.Tablet
		recorded    bool
		gracePeriod time.Duration
		// deleted are the tablets whose record the operator deletes before the recovery.
		deleted    []*topodatapb.Tablet
		setup      func(t *testing.T, m *tmcmock.MockTabletManagerClient)
		wantVoters []string
		// wantErr is a part of the FAILED_PRECONDITION error; empty means that the change is written.
		wantErr string
	}{{
		name:        "SwapVoter: a voter failed for the grace period; the spare of its cell takes its seat and joins",
		tablets:     []*topodatapb.Tablet{primary, voter2, spare2, voter3},
		voters:      []*topodatapb.Tablet{primary, voter2, voter3},
		recorded:    true,
		gracePeriod: 0,
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(settledMember(primary, primary, "1-10", primary, voter3), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(nil, errUnreachable)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(spare2)).Return(spareStatus(spare2, "1-5"), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(settledMember(voter3, primary, "1-10", primary, voter3), nil)
			m.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(spare2), startRequest(false)).Return(&replicationdatapb.GroupReplicationStatus{}, nil)
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000201", "zone3-0000000300"},
	}, {
		name:        "a voter unreachable within the grace period keeps its seat",
		tablets:     []*topodatapb.Tablet{primary, voter2, spare2, voter3},
		voters:      []*topodatapb.Tablet{primary, voter2, voter3},
		recorded:    true,
		gracePeriod: time.Hour,
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(settledMember(primary, primary, "1-10", primary, voter3), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(nil, errUnreachable)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(spare2)).Return(spareStatus(spare2, "1-5"), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(settledMember(voter3, primary, "1-10", primary, voter3), nil)
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"},
		wantErr:    "nothing to change",
	}, {
		name:        "P1, read fresh: the election of the group primary is in progress",
		tablets:     []*topodatapb.Tablet{primary, voter2, spare2, voter3},
		voters:      []*topodatapb.Tablet{primary, voter2, voter3},
		recorded:    true,
		gracePeriod: 0,
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			electing := settledMember(primary, primary, "1-10", primary, voter3)
			electing.GroupReplicationStatus.PrimaryElectionInProgress = true
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(electing, nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(nil, errUnreachable)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(spare2)).Return(spareStatus(spare2, "1-5"), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(settledMember(voter3, primary, "1-10", primary, voter3), nil)
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"},
		wantErr:    "the election of the group primary zone1-0000000101 is in progress",
	}, {
		name:        "P3, read fresh: the spare executed a transaction that the primary lacks",
		tablets:     []*topodatapb.Tablet{primary, voter2, spare2, voter3},
		voters:      []*topodatapb.Tablet{primary, voter2, voter3},
		recorded:    true,
		gracePeriod: 0,
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(settledMember(primary, primary, "1-10", primary, voter3), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(nil, errUnreachable)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(spare2)).Return(spareStatus(spare2, "1-11"), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(settledMember(voter3, primary, "1-10", primary, voter3), nil)
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"},
		wantErr:    "zone2-0000000201: it executed transactions that the primary lacks",
	}, {
		name:     "RemoveVoter: a voter has no tablet record, is down and in no view, and its cell has no spare",
		tablets:  []*topodatapb.Tablet{primary, voter2, voter3},
		voters:   []*topodatapb.Tablet{primary, voter2, voter3},
		deleted:  []*topodatapb.Tablet{voter3},
		recorded: true,
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(nil, errUnreachable).AnyTimes()
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(settledMember(primary, primary, "1-10", primary, voter2), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(settledMember(voter2, primary, "1-10", primary, voter2), nil)
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200"},
	}, {
		name:     "SwapVoter: a voter has no tablet record, is down and in no view; the spare of its cell takes its seat",
		tablets:  []*topodatapb.Tablet{primary, voter2, spare3, voter3},
		voters:   []*topodatapb.Tablet{primary, voter2, voter3},
		deleted:  []*topodatapb.Tablet{voter3},
		recorded: true,
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(nil, errUnreachable).AnyTimes()
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(settledMember(primary, primary, "1-10", primary, voter2), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(settledMember(voter2, primary, "1-10", primary, voter2), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(spare3)).Return(spareStatus(spare3, "1-10"), nil)
			m.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(spare3), startRequest(false)).Return(&replicationdatapb.GroupReplicationStatus{}, nil)
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000301"},
	}, {
		name:     "a voter has no tablet record and its vttablet is down, but its MySQL is still ONLINE in the group: it stays",
		tablets:  []*topodatapb.Tablet{primary, voter2, voter3},
		voters:   []*topodatapb.Tablet{primary, voter2, voter3},
		deleted:  []*topodatapb.Tablet{voter3},
		recorded: true,
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(nil, errUnreachable).AnyTimes()
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(settledMember(primary, primary, "1-10", primary, voter2, voter3), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(settledMember(voter2, primary, "1-10", primary, voter2, voter3), nil)
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"},
		wantErr:    "voter zone3-0000000300 has no tablet record, but its server_uuid is unknown, and the active member 00000000-0000-0000-0000-000000000300 is the MySQL of no tablet that answers",
	}, {
		name:     "RemoveVoterNoGroup: no group runs, and a voter that is down has no tablet record",
		tablets:  []*topodatapb.Tablet{primary, voter2, voter3},
		voters:   []*topodatapb.Tablet{primary, voter2, voter3},
		deleted:  []*topodatapb.Tablet{voter3},
		recorded: true,
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(nil, errUnreachable).AnyTimes()
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(spareStatus(primary, "1-10"), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(spareStatus(voter2, "1-9"), nil)
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200"},
	}, {
		// The bootstrap would include a voter that answers: it is not removed, although its tablet
		// record is gone.
		name:     "RemoveVoterNoGroup: the deleted voter's vttablet answers at the address VTOrc last knew",
		tablets:  []*topodatapb.Tablet{primary, voter2, voter3},
		voters:   []*topodatapb.Tablet{primary, voter2, voter3},
		deleted:  []*topodatapb.Tablet{voter3},
		recorded: true,
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(spareStatus(primary, "1-10"), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(spareStatus(voter2, "1-9"), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(spareStatus(voter3, "1-11"), nil).AnyTimes()
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"},
		wantErr:    "voter zone3-0000000300 has no tablet record, but VTOrc cannot tell that its vttablet is down",
	}, {
		name:     "RemoveVoterNoGroup: a bootstrap intent is live",
		tablets:  []*topodatapb.Tablet{primary, voter2, voter3},
		voters:   []*topodatapb.Tablet{primary, voter2, voter3},
		deleted:  []*topodatapb.Tablet{voter3},
		recorded: true,
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(nil, errUnreachable).AnyTimes()
			_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
				si.GroupReplicationBootstrapIntent = &topodatapb.GroupReplicationBootstrapIntent{
					Target: primary.Alias, Time: protoutil.TimeToProto(time.Now()), Token: "1790000002-0123456789abcdef",
					PreviousIncarnation: voterTestIncarnation,
				}
				return nil
			})
			require.NoError(t, err)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(spareStatus(primary, "1-10"), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(spareStatus(voter2, "1-9"), nil)
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"},
		wantErr:    "no voter is the reachable primary",
	}, {
		name:     "GrowVoter: a cell with an eligible tablet has no voter",
		tablets:  []*topodatapb.Tablet{primary, voter2, voter3},
		voters:   []*topodatapb.Tablet{primary, voter2},
		recorded: true,
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(settledMember(primary, primary, "1-10", primary, voter2), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(settledMember(voter2, primary, "1-10", primary, voter2), nil)
			m.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(spareStatus(voter3, "1-3"), nil)
			m.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(voter3), startRequest(false)).Return(&replicationdatapb.GroupReplicationStatus{}, nil)
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"},
	}, {
		name:    "initial voters: one per cell, with no member active",
		tablets: []*topodatapb.Tablet{primary, voter2, voter3},
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			for _, tablet := range []*topodatapb.Tablet{primary, voter2, voter3} {
				m.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).Return(notMemberStatus(tablet), nil)
			}
		},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"},
	}, {
		// With one voter per cell, a group of two voters keeps no majority when one of them fails.
		name:    "initial voters: the eligible tablets are in two cells, no voter is written",
		tablets: []*topodatapb.Tablet{primary, voter2, spare2},
		setup: func(t *testing.T, m *tmcmock.MockTabletManagerClient) {
			for _, tablet := range []*topodatapb.Tablet{primary, voter2, spare2} {
				m.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).Return(notMemberStatus(tablet), nil)
			}
		},
		wantErr: "the shard needs them in at least 3 cells",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst.UnreachableGroupTablets.Reset()
			config.SetGroupReplicationVoterReplacementGracePeriod(tt.gracePeriod)
			mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, tt.tablets...)
			setVoters(t, tt.voters...)
			if tt.recorded {
				setIncarnation(t, voterTestIncarnation)
			}
			tt.setup(t, mockTMC)
			// The operator deletes the records, and VTOrc refreshes the shard's tablet records before
			// the recovery, as executeCheckAndRecoverFunction does.
			for _, tablet := range tt.deleted {
				require.NoError(t, ts.DeleteTablet(t.Context(), tablet.Alias))
			}
			if len(tt.deleted) > 0 {
				refreshReachableTabletInfoOfShard(t.Context(), "ks", "0")
			}

			attempted, topologyRecovery, err := updateGroupReplicationVoters(lockedShard(t), voterRecoveryEntry(tt.tablets[0], inst.GroupVotersOutOfDate), log.NewPrefixedLogger("test"))
			require.True(t, attempted)
			require.NotNil(t, topologyRecovery)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantVoters, readVoters(t))
		})
	}
}

// TestUpdateGroupReplicationVotersCompareAndSwap checks that the write of the voter list is a
// compare-and-swap on the list and the incarnation that the decision read. Here another VTOrc, whose
// shard lock expired, or that took the lock after this one's expired, writes while this one reads the
// statuses: this one's decision is stale, and must not overwrite the other write.
func TestUpdateGroupReplicationVotersCompareAndSwap(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	for _, tt := range []struct {
		name       string
		concurrent func(si *topo.ShardInfo)
	}{{
		name:       "another VTOrc wrote the voters",
		concurrent: func(si *topo.ShardInfo) { si.GroupReplicationVoters = si.GroupReplicationVoters[:2] },
	}, {
		name:       "another component recorded a new incarnation",
		concurrent: func(si *topo.ShardInfo) { si.GroupReplicationIncarnation = "1790000002" },
	}} {
		t.Run(tt.name, func(t *testing.T) {
			inst.UnreachableGroupTablets.Reset()
			config.SetGroupReplicationVoterReplacementGracePeriod(0)
			primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
			voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
			spare2 := recoveryTablet("zone2", 201, topodatapb.TabletType_REPLICA)
			voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
			mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter2, spare2, voter3)
			setVoters(t, primary, voter2, voter3)
			setIncarnation(t, voterTestIncarnation)
			var want *topo.ShardInfo

			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(settledMember(primary, primary, "1-10", primary, voter3), nil)
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(nil, errors.New("unreachable"))
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(settledMember(voter3, primary, "1-10", primary, voter3), nil)
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(spare2)).DoAndReturn(
				func(ctx context.Context, _ *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
					si, err := ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
						tt.concurrent(si)
						return nil
					})
					require.NoError(t, err)
					want = si
					return spareStatus(spare2, "1-5"), nil
				})
			mockTMC.EXPECT().StartGroupReplication(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			_, _, err := updateGroupReplicationVoters(lockedShard(t), voterRecoveryEntry(primary, inst.GroupVotersOutOfDate), log.NewPrefixedLogger("test"))
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			require.ErrorContains(t, err, "changed concurrently")
			si, err := ts.GetShard(t.Context(), "ks", "0")
			require.NoError(t, err)
			assert.True(t, proto.Equal(want.Shard, si.Shard), "the other component's write must stand")
		})
	}
}

// TestMoveGroupPrimaryToVoter checks that VTOrc moves the primary of the shard's group, which is not a
// voter and so does not serve, to an ONLINE voter of its view, and leaves the voter list as it is.
func TestMoveGroupPrimaryToVoter(t *testing.T) {
	t.Cleanup(groupPrimaryMoves.reset)
	groupPrimaryMoves.reset()
	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	voter1 := recoveryTablet("zone1", 102, topodatapb.TabletType_REPLICA)
	voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter1, voter2, voter3)
	setVoters(t, voter1, voter2, voter3)
	setIncarnation(t, voterTestIncarnation)
	for _, tablet := range []*topodatapb.Tablet{primary, voter1, voter2, voter3} {
		mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).Return(settledMember(tablet, primary, "1-10", primary, voter1, voter2, voter3), nil)
	}
	mockTMC.EXPECT().PromoteReplica(gomock.Any(), sameTablet(voter1), false).Return("MySQL56/"+"6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41:1-10", nil)
	mockTMC.EXPECT().PopulateReparentJournal(gomock.Any(), sameTablet(voter1), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	attempted, topologyRecovery, err := moveGroupPrimaryToVoter(lockedShard(t), voterRecoveryEntry(primary, inst.GroupPrimaryNotVoter), log.NewPrefixedLogger("test"))
	require.NoError(t, err)
	require.True(t, attempted)
	require.NotNil(t, topologyRecovery)
	assert.Equal(t, []string{"zone1-0000000102", "zone2-0000000200", "zone3-0000000300"}, readVoters(t))
}

// TestMoveGroupPrimaryOffDeletedVoter checks that VTOrc moves the primary role away from a voter whose
// tablet record was deleted (DeleteTablets --allow-primary) while it serves: the deletion does not
// stop a running primary, which keeps acknowledging writes that a later removal of the voter would
// lose. The list does not change.
func TestMoveGroupPrimaryOffDeletedVoter(t *testing.T) {
	t.Cleanup(groupPrimaryMoves.reset)
	groupPrimaryMoves.reset()
	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	// The primary's tablet record is gone.
	mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, voter2, voter3)
	setVoters(t, primary, voter2, voter3)
	setIncarnation(t, voterTestIncarnation)
	for _, tablet := range []*topodatapb.Tablet{voter2, voter3} {
		mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).Return(settledMember(tablet, primary, "1-10", primary, voter2, voter3), nil)
	}
	mockTMC.EXPECT().PromoteReplica(gomock.Any(), sameTablet(voter2), false).Return("MySQL56/6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41:1-10", nil)
	mockTMC.EXPECT().PopulateReparentJournal(gomock.Any(), sameTablet(voter2), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	attempted, topologyRecovery, err := moveGroupPrimaryToVoter(lockedShard(t), voterRecoveryEntry(voter2, inst.GroupPrimaryNotVoter), log.NewPrefixedLogger("test"))
	require.NoError(t, err)
	require.True(t, attempted)
	require.NotNil(t, topologyRecovery)
	assert.Equal(t, []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"}, readVoters(t))
}

// TestUpdateGroupReplicationVotersKeepsLiveDeletedVoter checks, on the state that production reaches,
// that a voter whose tablet record was deleted while its vttablet and MySQL still run is not removed:
// VTOrc refreshes the shard's tablet records before every recovery, and that refresh used to forget
// the deleted voter, its last tablet record and its server_uuid, so that the recovery could neither
// probe it nor tell it from a dead one. Here no group runs, and the deleted voter answers.
func TestUpdateGroupReplicationVotersKeepsLiveDeletedVoter(t *testing.T) {
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
	setIncarnation(t, voterTestIncarnation)
	// VTOrc discovered every voter, server_uuid included.
	for _, tablet := range []*topodatapb.Tablet{primary, voter2, voter3} {
		require.NoError(t, inst.WriteInstance(&inst.Instance{
			InstanceAlias: tablet.Alias, Hostname: tablet.MysqlHostname, Port: int(tablet.MysqlPort), ServerUUID: voterTestUUID(tablet),
		}, true, nil))
	}
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(spareStatus(primary, "1-10"), nil).AnyTimes()
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).Return(spareStatus(voter2, "1-9"), nil).AnyTimes()
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).Return(spareStatus(voter3, "1-11"), nil).AnyTimes()

	// The operator deletes the record of voter3, whose vttablet and MySQL still run, and VTOrc
	// refreshes the shard's tablet records before the recovery, as executeCheckAndRecoverFunction does.
	require.NoError(t, ts.DeleteTablet(t.Context(), voter3.Alias))
	refreshReachableTabletInfoOfShard(t.Context(), "ks", "0")

	_, _, err := updateGroupReplicationVoters(lockedShard(t), voterRecoveryEntry(primary, inst.GroupVotersOutOfDate), log.NewPrefixedLogger("test"))
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	assert.Equal(t, []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"}, readVoters(t), "a deleted voter that still runs must keep its seat")
}
