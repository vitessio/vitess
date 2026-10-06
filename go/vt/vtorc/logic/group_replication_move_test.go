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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/inst"
	tmcmock "vitess.io/vitess/go/vt/vttablet/tmclient/mock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// legitimateMemberStatus is groupMemberStatus in the group of shard ks/0, in the given view.
func legitimateMemberStatus(tablet, groupPrimary *topodatapb.Tablet, viewID string, online ...*topodatapb.Tablet) *replicationdatapb.FullStatus {
	status := groupMemberStatus(tablet, groupPrimary, online...)
	status.TabletType = tablet.Type
	status.GroupReplicationStatus.GroupName = policy.GroupName("ks", "0")
	status.GroupReplicationStatus.ViewId = viewID
	return status
}

// groupPrimaryMoveTest sets up a shard whose voters are one tablet per cell: the old primary in
// zone1, whose MySQL died; a replica in zone2; and the member that the group elected in zone3, whose
// tablet is not the topology primary. VTOrc last discovered the elected member's server_uuid.
func groupPrimaryMoveTest(t *testing.T, incarnation string, extraVoters ...*topodatapb.Tablet) (mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet) {
	oldCellTimeout, oldGrace := groupReplicationCellTimeout, groupPrimaryMoveGracePeriod
	t.Cleanup(func() {
		groupReplicationCellTimeout, groupPrimaryMoveGracePeriod = oldCellTimeout, oldGrace
		groupPrimaryMoves.reset()
		inst.GroupReplicationConditions.Reset()
	})
	groupReplicationCellTimeout = 100 * time.Millisecond
	groupPrimaryMoveGracePeriod = 0
	groupPrimaryMoves.reset()
	inst.GroupReplicationConditions.Reset()

	oldPrimary = recoveryTablet("zone1", 100, topodatapb.TabletType_PRIMARY)
	replica = recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	elected = recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	tablets := append([]*topodatapb.Tablet{oldPrimary, replica, elected}, extraVoters...)
	mockTMC = groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, tablets...)
	setVoters(t, tablets...)
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = oldPrimary.Alias
		si.GroupReplicationIncarnation = incarnation
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, inst.WriteInstance(&inst.Instance{
		InstanceAlias: elected.Alias,
		Hostname:      elected.MysqlHostname,
		Port:          int(elected.MysqlPort),
		ServerUUID:    voterTestUUID(elected),
	}, true, nil))
	return mockTMC, oldPrimary, replica, elected
}

// TestMoveGroupPrimaryOutOfUnreachableCell reproduces NEW-4 of the Group Replication failover audit
// (G9b and S9b chaos scenarios): the primary dies, and the group elects a member whose cell's
// topology server is down. Its tablet writes its own record in that server before it becomes
// PRIMARY, so neither it nor VTOrc's PromoteGroupPrimary could promote it, and the shard had no
// primary until the server came back. VTOrc now makes a voter whose cell answers the group primary,
// without depending on the elected member's vttablet, but only within the shard's legitimate group,
// and never away from a member whose cell answers.
func TestMoveGroupPrimaryOutOfUnreachableCell(t *testing.T) {
	const (
		incarnation = "1790000001"
		viewID      = incarnation + ":7"
	)
	errUnreachable := errors.New("unreachable")
	tests := []struct {
		name string
		// cellAnswers leaves the elected member's cell reachable.
		cellAnswers bool
		// setup sets the FullStatus results and the expected RPCs, after the shard is set up.
		setup       func(t *testing.T, mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet)
		wantErrCode vtrpcpb.Code
		// wantMoved is the tablet that becomes the group primary, if any.
		wantMoved bool
		// wantBackoff is whether the recovery of the elected member then waits before it runs again.
		wantBackoff bool
	}{
		{
			name: "the elected member's cell does not answer: the voter of a cell that answers becomes the group primary",
			setup: func(t *testing.T, mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet) {
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(oldPrimary)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(legitimateMemberStatus(replica, elected, viewID, replica, elected), nil).AnyTimes()
				// The move does not depend on the elected member's vttablet.
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(elected)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().ChangeType(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockTMC.EXPECT().PromoteReplica(gomock.Any(), sameTablet(replica), false).Return("MySQL56/6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41:1-10", nil)
				mockTMC.EXPECT().PopulateReparentJournal(gomock.Any(), sameTablet(replica), gomock.Any(), gomock.Any(), replica.Alias, "MySQL56/6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41:1-10").Return(nil)
			},
			wantMoved: true,
		},
		{
			name:        "the elected member's cell answers: its tablet is promoted and the group primary stays",
			cellAnswers: true,
			setup: func(t *testing.T, mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet) {
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(oldPrimary)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(legitimateMemberStatus(replica, elected, viewID, replica, elected), nil).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(elected)).Return(legitimateMemberStatus(elected, elected, viewID, replica, elected), nil).AnyTimes()
				mockTMC.EXPECT().PromoteReplica(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockTMC.EXPECT().ChangeType(gomock.Any(), sameTablet(elected), topodatapb.TabletType_PRIMARY, false).Return(nil)
				mockTMC.EXPECT().PrimaryPosition(gomock.Any(), sameTablet(elected)).Return("MySQL56/6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41:1-10", nil)
				mockTMC.EXPECT().PopulateReparentJournal(gomock.Any(), sameTablet(elected), gomock.Any(), gomock.Any(), elected.Alias, gomock.Any()).Return(nil)
			},
		},
		{
			name: "no voter of a cell that answers is eligible: nothing moves, and the recovery waits before it tries again",
			setup: func(t *testing.T, mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet) {
				// The voter of zone2 was made RDONLY: the durability policy does not promote it.
				replica.Type = topodatapb.TabletType_RDONLY
				_, err := ts.UpdateTabletFields(t.Context(), replica.Alias, func(tablet *topodatapb.Tablet) error {
					tablet.Type = topodatapb.TabletType_RDONLY
					return nil
				})
				require.NoError(t, err)
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(oldPrimary)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(legitimateMemberStatus(replica, elected, viewID, replica, elected), nil).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(elected)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().PromoteReplica(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockTMC.EXPECT().ChangeType(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantErrCode: vtrpcpb.Code_UNAVAILABLE,
			wantBackoff: true,
		},
		{
			name: "the voter of a cell that answers does not run Group Replication: nothing moves",
			setup: func(t *testing.T, mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet) {
				// Its MySQL is an ONLINE member, but its vttablet runs without --enable-group-replication.
				status := legitimateMemberStatus(replica, elected, viewID, replica, elected)
				status.GroupReplicationEnabled = false
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(oldPrimary)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(status, nil).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(elected)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().PromoteReplica(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockTMC.EXPECT().ChangeType(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantErrCode: vtrpcpb.Code_UNAVAILABLE,
			wantBackoff: true,
		},
		{
			name: "the members that answer are in another incarnation than the recorded one: nothing moves",
			setup: func(t *testing.T, mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet) {
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(oldPrimary)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(legitimateMemberStatus(replica, elected, "1799999999:2", replica, elected), nil).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(elected)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().PromoteReplica(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockTMC.EXPECT().ChangeType(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantErrCode: vtrpcpb.Code_UNAVAILABLE,
			wantBackoff: true,
		},
		{
			name: "the members that answer do not see a majority of the voters: nothing moves",
			setup: func(t *testing.T, mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet) {
				// Two more voters, unreachable and not in the view: 2 of the 5 voters are ONLINE.
				setVoters(t, oldPrimary, replica, elected, recoveryTablet("zone1", 101, topodatapb.TabletType_REPLICA), recoveryTablet("zone2", 201, topodatapb.TabletType_REPLICA))
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(oldPrimary)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(legitimateMemberStatus(replica, elected, viewID, replica, elected), nil).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(elected)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().PromoteReplica(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockTMC.EXPECT().ChangeType(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantErrCode: vtrpcpb.Code_UNAVAILABLE,
			wantBackoff: true,
		},
		{
			name: "the elected member's tablet already runs as PRIMARY: nothing moves",
			setup: func(t *testing.T, mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet) {
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(oldPrimary)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(legitimateMemberStatus(replica, elected, viewID, replica, elected), nil).AnyTimes()
				status := legitimateMemberStatus(elected, elected, viewID, replica, elected)
				status.TabletType = topodatapb.TabletType_PRIMARY
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(elected)).Return(status, nil).AnyTimes()
				mockTMC.EXPECT().PromoteReplica(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockTMC.EXPECT().ChangeType(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantErrCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			name: "the members that answer have elected another primary meanwhile: nothing moves",
			setup: func(t *testing.T, mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet) {
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(oldPrimary)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(legitimateMemberStatus(replica, replica, viewID, replica, elected), nil).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(elected)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().PromoteReplica(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				mockTMC.EXPECT().ChangeType(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantErrCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			name: "the group primary was just moved away from the voter that answers: it is not moved back",
			setup: func(t *testing.T, mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet) {
				groupPrimaryMoves.recordMoveAway(topoproto.TabletAliasString(replica.Alias), time.Now())
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(oldPrimary)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(legitimateMemberStatus(replica, elected, viewID, replica, elected), nil).AnyTimes()
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(elected)).Return(nil, errUnreachable).AnyTimes()
				mockTMC.EXPECT().PromoteReplica(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantErrCode: vtrpcpb.Code_UNAVAILABLE,
			wantBackoff: true,
		},
		{
			name: "the group primary was just moved away from the elected member: it is not moved away again",
			setup: func(t *testing.T, mockTMC *tmcmock.MockTabletManagerClient, oldPrimary, replica, elected *topodatapb.Tablet) {
				groupPrimaryMoves.recordMoveAway(topoproto.TabletAliasString(elected.Alias), time.Now())
				mockTMC.EXPECT().FullStatus(gomock.Any(), gomock.Any()).Times(0)
				mockTMC.EXPECT().PromoteReplica(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			},
			wantErrCode: vtrpcpb.Code_FAILED_PRECONDITION,
			wantBackoff: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockTMC, oldPrimary, replica, elected := groupPrimaryMoveTest(t, incarnation)
			if !tt.cellAnswers {
				cutOffCell(t, "zone3")
			}
			tt.setup(t, mockTMC, oldPrimary, replica, elected)

			analysisEntry := &inst.DetectionAnalysis{
				Analysis:              inst.GroupPrimaryNotInTopo,
				AnalyzedInstanceAlias: elected.Alias,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}
			// The recovery runs under the shard lock.
			lockCtx, unlock, err := ts.LockShard(t.Context(), "ks", "0", "test")
			require.NoError(t, err)
			defer unlock(&err)
			start := time.Now()
			attempted, topologyRecovery, err := promoteGroupPrimary(lockCtx, analysisEntry, log.NewPrefixedLogger("test"))
			assert.Less(t, time.Since(start), topo.RemoteOperationTimeout/2, "the unreachable cell must not take the recovery's whole deadline")
			require.True(t, attempted)
			require.NotNil(t, topologyRecovery)
			if tt.wantErrCode != vtrpcpb.Code_OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, vterrors.Code(err), "%v", err)
				assert.False(t, topologyRecovery.IsSuccessful)
			} else {
				require.NoError(t, err)
				assert.True(t, topologyRecovery.IsSuccessful)
			}
			if tt.wantMoved {
				assert.True(t, topoproto.TabletAliasEqual(replica.Alias, topologyRecovery.SuccessorAlias), "successor %v", topologyRecovery.SuccessorAlias)
				assert.Positive(t, groupPrimaryMoves.holdRemaining(topoproto.TabletAliasString(elected.Alias), time.Now()), "the member moved away from must be held")
			}
			_, skipCode := getCheckAndRecoverFunctionCode(analysisEntry)
			if tt.wantBackoff {
				assert.Equal(t, RecoverySkipGroupPrimaryMoveBackoff, skipCode, "the recovery must not run again on the next poll")
			} else {
				assert.Equal(t, RecoverySkipNone, skipCode)
			}
		})
	}
}

// TestMoveGroupPrimaryWaitsForGracePeriod checks that VTOrc does not move the group primary while
// its tablet may still be promoting itself.
func TestMoveGroupPrimaryWaitsForGracePeriod(t *testing.T) {
	mockTMC, _, _, elected := groupPrimaryMoveTest(t, "")
	groupPrimaryMoveGracePeriod = time.Hour
	cutOffCell(t, "zone3")
	mockTMC.EXPECT().FullStatus(gomock.Any(), gomock.Any()).Times(0)
	mockTMC.EXPECT().PromoteReplica(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	analysisEntry := &inst.DetectionAnalysis{
		Analysis:              inst.GroupPrimaryNotInTopo,
		AnalyzedInstanceAlias: elected.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	attempted, _, err := promoteGroupPrimary(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
	require.True(t, attempted)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
	_, skipCode := getCheckAndRecoverFunctionCode(analysisEntry)
	assert.Equal(t, RecoverySkipGroupPrimaryMoveBackoff, skipCode)
}

// TestGroupPrimaryMoveTarget checks which member the group primary is moved to: an ONLINE voter of
// the legitimate view in a cell that answers, that the durability policy allows to be promoted and
// that the group primary was not just moved away from. The highest member weight wins, then the
// lowest alias.
func TestGroupPrimaryMoveTarget(t *testing.T) {
	t.Cleanup(groupPrimaryMoves.reset)
	groupPrimaryMoves.reset()
	durability, err := policy.GetDurabilityPolicy(policy.DurabilityGroupReplicationCrossCell)
	require.NoError(t, err)
	grd, ok := policy.AsGroupReplication(durability)
	require.True(t, ok)

	elected := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	a := recoveryTablet("zone1", 101, topodatapb.TabletType_REPLICA)
	b := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	cutOff := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	rdonly := recoveryTablet("zone1", 102, topodatapb.TabletType_RDONLY)
	notVoter := recoveryTablet("zone1", 99, topodatapb.TabletType_REPLICA)
	recovering := recoveryTablet("zone1", 98, topodatapb.TabletType_REPLICA)
	online := []*topodatapb.Tablet{elected, a, b, cutOff, rdonly, notVoter}
	voters := []*topodatapb.TabletAlias{elected.Alias, a.Alias, b.Alias, cutOff.Alias, rdonly.Alias, recovering.Alias}
	legitimate := policy.NewLegitimateGroup("", voters, nil, map[string]string{
		topoproto.TabletAliasString(elected.Alias): voterTestUUID(elected),
		topoproto.TabletAliasString(a.Alias):       voterTestUUID(a),
		topoproto.TabletAliasString(b.Alias):       voterTestUUID(b),
		topoproto.TabletAliasString(cutOff.Alias):  voterTestUUID(cutOff),
		topoproto.TabletAliasString(rdonly.Alias):  voterTestUUID(rdonly),
	})
	view := legitimateMemberStatus(a, elected, "", online...).GetGroupReplicationStatus()
	recoveringStatus := legitimateMemberStatus(recovering, elected, "", online...)
	recoveringStatus.GroupReplicationStatus.MemberState = mysql.GroupMemberStateRecovering
	statuses := []*shardTabletStatus{
		{tablet: a, status: legitimateMemberStatus(a, elected, "", online...)},
		{tablet: b, status: legitimateMemberStatus(b, elected, "", online...)},
		{tablet: cutOff, status: legitimateMemberStatus(cutOff, elected, "", online...)},
		{tablet: rdonly, status: legitimateMemberStatus(rdonly, elected, "", online...)},
		{tablet: notVoter, status: legitimateMemberStatus(notVoter, elected, "", online...)},
		{tablet: recovering, status: recoveringStatus},
	}

	target, rejected := groupPrimaryMoveTarget(durability, grd, voters, elected, legitimate, view, statuses, []string{"zone2", "zone3"}, time.Now())
	require.NotNil(t, target)
	assert.Equal(t, "zone1-0000000100", topoproto.TabletAliasString(target.Alias), "equal weights: the lowest alias")
	assert.ElementsMatch(t, []string{
		"zone2-0000000200: the topology server of its cell does not answer",
		"zone1-0000000102: must not be promoted according to the durability policy",
		"zone1-0000000099: not a voter",
		"zone1-0000000098: not an ONLINE member of the shard's legitimate group",
	}, rejected)

	// The member the group primary was just moved away from is not a target.
	groupPrimaryMoves.recordMoveAway("zone1-0000000100", time.Now())
	target, _ = groupPrimaryMoveTarget(durability, grd, voters, elected, legitimate, view, statuses, []string{"zone2", "zone3"}, time.Now())
	require.NotNil(t, target)
	assert.Equal(t, "zone1-0000000101", topoproto.TabletAliasString(target.Alias))

	// The highest member weight wins over the lowest alias.
	assert.Equal(t, "zone1-0000000101", topoproto.TabletAliasString(chooseGroupPrimaryMoveTarget([]groupPrimaryMoveCandidate{
		{tablet: b, weight: 50},
		{tablet: a, weight: 75},
	}).Alias))
	assert.Nil(t, chooseGroupPrimaryMoveTarget(nil))
}

// TestGroupReplicationTabletRecoveriesRefreshReachableCells checks which recoveries refresh the
// shard's tablet records from the cells that answer only (refreshReachableTabletInfoOfShard): the
// group replication recoveries of a single tablet, which work with those cells. The others keep
// waiting for every cell.
func TestGroupReplicationTabletRecoveriesRefreshReachableCells(t *testing.T) {
	assert.True(t, isGroupReplicationTabletRecovery(promoteGroupPrimaryFunc))
	assert.True(t, isGroupReplicationTabletRecovery(startGroupReplicationFunc))
	assert.True(t, isGroupReplicationTabletRecovery(updateGroupReplicationVotersFunc))
	assert.False(t, isGroupReplicationTabletRecovery(fixReplicaFunc))
	assert.False(t, isGroupReplicationTabletRecovery(reconcileStaleTopoPrimaryFunc))
	assert.False(t, isGroupReplicationTabletRecovery(bootstrapGroupReplicationFunc))
}

// TestRefreshReachableTabletInfoOfShard checks that the group replication recoveries refresh the
// shard's tablet records without waiting for a cell whose topology server is cut off, and without
// forgetting that cell's tablets: before, the refresh under the shard lock waited the whole remote
// operation timeout for that cell and then refreshed nothing.
func TestRefreshReachableTabletInfoOfShard(t *testing.T) {
	oldCellTimeout := groupReplicationCellTimeout
	t.Cleanup(func() { groupReplicationCellTimeout = oldCellTimeout })
	groupReplicationCellTimeout = 100 * time.Millisecond

	replica := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	// VTOrc remembers a forgotten alias for the whole process (inst.ForgetInstance), and would then
	// ignore a tablet of another test of the package with the same alias: this one is used nowhere
	// else.
	deleted := recoveryTablet("zone1", 199, topodatapb.TabletType_REPLICA)
	cutOff := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	groupReplicationRecoveryTest(t, replica, deleted, cutOff)
	ctx := t.Context()
	_, err := ts.UpdateTabletFields(ctx, replica.Alias, func(tablet *topodatapb.Tablet) error {
		tablet.Type = topodatapb.TabletType_PRIMARY
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, ts.DeleteTablet(ctx, deleted.Alias))
	cutOffCell(t, "zone3")

	start := time.Now()
	refreshReachableTabletInfoOfShard(ctx, "ks", "0")
	assert.Less(t, time.Since(start), topo.RemoteOperationTimeout/2, "the cut-off cell must not take the remote operation timeout")

	refreshed, err := inst.ReadTablet(replica.Alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, refreshed.Type, "the tablet records of the cells that answer are refreshed")
	_, err = inst.ReadTablet(deleted.Alias)
	require.ErrorIs(t, err, inst.ErrTabletAliasNil, "a tablet deleted from a cell that answers is forgotten")
	kept, err := inst.ReadTablet(cutOff.Alias)
	require.NoError(t, err, "the tablets of the cut-off cell are kept")
	assert.Equal(t, topodatapb.TabletType_REPLICA, kept.Type)
}
