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
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	tmcmock "vitess.io/vitess/go/vt/vttablet/tmclient/mock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

func TestGetCheckAndRecoverFunctionCodeGroupReplication(t *testing.T) {
	prevERS := config.ERSEnabled()
	config.SetERSEnabled(true)
	prevGrace := config.GetGroupReplicationFailoverGracePeriod()
	t.Cleanup(func() {
		config.SetERSEnabled(prevERS)
		config.SetGroupReplicationFailoverGracePeriod(prevGrace)
		inst.GroupReplicationConditions.Reset()
	})

	const (
		primaryUUID    = "00000000-0000-0000-0000-000000000101"
		newPrimaryUUID = "00000000-0000-0000-0000-000000000100"
	)
	entry := func(code inst.AnalysisCode, activeMembers uint, groupPrimaryUUID string) *inst.DetectionAnalysis {
		return &inst.DetectionAnalysis{
			Analysis:                code,
			AnalyzedKeyspace:        "ks",
			AnalyzedShard:           "0",
			AnalyzedServerUUID:      primaryUUID,
			ShardGroupActiveMembers: activeMembers,
			ShardGroupPrimaryUUID:   groupPrimaryUUID,
		}
	}

	tests := []struct {
		name          string
		gracePeriod   time.Duration
		analysisEntry *inst.DetectionAnalysis
		wantFunc      recoveryFunction
		wantSkipCode  RecoverySkipCode
	}{
		{
			name:          "DeadPrimary without group members fails over right away",
			gracePeriod:   time.Hour,
			analysisEntry: entry(inst.DeadPrimary, 0, ""),
			wantFunc:      recoverDeadPrimaryFunc,
		},
		{
			name:          "DeadPrimary with group members waits for the group",
			gracePeriod:   time.Hour,
			analysisEntry: entry(inst.DeadPrimary, 2, newPrimaryUUID),
			wantFunc:      recoverDeadPrimaryFunc,
			wantSkipCode:  RecoverySkipGroupReplicationGracePeriod,
		},
		{
			name:          "DeadPrimary with group members after the grace period",
			analysisEntry: entry(inst.DeadPrimary, 2, newPrimaryUUID),
			wantFunc:      recoverDeadPrimaryFunc,
		},
		{
			name:          "DeadPrimary whose MySQL is still the group's primary",
			analysisEntry: entry(inst.DeadPrimary, 2, primaryUUID),
			wantFunc:      recoverDeadPrimaryFunc,
			wantSkipCode:  RecoverySkipGroupPrimaryAlive,
		},
		{
			name:          "DeadPrimaryWithoutReplicas without group members has no recovery",
			analysisEntry: entry(inst.DeadPrimaryWithoutReplicas, 0, ""),
			wantFunc:      noRecoveryFunc,
			wantSkipCode:  RecoverySkipNoRecoveryAction,
		},
		{
			name:          "DeadPrimaryWithoutReplicas with group members fails over after the grace period",
			analysisEntry: entry(inst.DeadPrimaryWithoutReplicas, 2, ""),
			wantFunc:      recoverDeadPrimaryFunc,
		},
		{
			name:          "DeadPrimaryWithoutReplicas with group members waits for the group",
			gracePeriod:   time.Hour,
			analysisEntry: entry(inst.DeadPrimaryWithoutReplicas, 2, ""),
			wantFunc:      recoverDeadPrimaryFunc,
			wantSkipCode:  RecoverySkipGroupReplicationGracePeriod,
		},
		{
			name:          "PrimaryTabletUnreachableByQuorum with group members waits for the group",
			gracePeriod:   time.Hour,
			analysisEntry: entry(inst.PrimaryTabletUnreachableByQuorum, 2, primaryUUID),
			wantFunc:      recoverDeadPrimaryFunc,
			wantSkipCode:  RecoverySkipGroupReplicationGracePeriod,
		},
		{
			name:          "PrimaryTabletDeleted with group members waits for the group",
			gracePeriod:   time.Hour,
			analysisEntry: entry(inst.PrimaryTabletDeleted, 2, newPrimaryUUID),
			wantFunc:      recoverPrimaryTabletDeletedFunc,
			wantSkipCode:  RecoverySkipGroupReplicationGracePeriod,
		},
		{
			name:          "ClusterHasNoPrimary with group members waits for the group",
			gracePeriod:   time.Hour,
			analysisEntry: entry(inst.ClusterHasNoPrimary, 2, newPrimaryUUID),
			wantFunc:      electNewPrimaryFunc,
			wantSkipCode:  RecoverySkipGroupReplicationGracePeriod,
		},
		{
			name:          "IncapacitatedPrimary with group members waits for the group",
			gracePeriod:   time.Hour,
			analysisEntry: entry(inst.IncapacitatedPrimary, 2, primaryUUID),
			wantFunc:      recoverIncapacitatedPrimaryFunc,
			wantSkipCode:  RecoverySkipGroupReplicationGracePeriod,
		},
		{
			name:          "GroupPrimaryNotInTopo",
			analysisEntry: entry(inst.GroupPrimaryNotInTopo, 2, newPrimaryUUID),
			wantFunc:      promoteGroupPrimaryFunc,
		},
		{
			name:          "GroupMemberNotOnline",
			analysisEntry: entry(inst.GroupMemberNotOnline, 2, newPrimaryUUID),
			wantFunc:      startGroupReplicationFunc,
		},
		{
			name:          "GroupNotBootstrapped",
			analysisEntry: entry(inst.GroupNotBootstrapped, 0, ""),
			wantFunc:      bootstrapGroupReplicationFunc,
		},
		{
			name:          "GroupQuorumLost has no recovery",
			analysisEntry: entry(inst.GroupQuorumLost, 2, ""),
			wantFunc:      noRecoveryFunc,
			wantSkipCode:  RecoverySkipNoRecoveryAction,
		},
		{
			name:          "GroupCellMajority has no recovery",
			analysisEntry: entry(inst.GroupCellMajority, 3, primaryUUID),
			wantFunc:      noRecoveryFunc,
			wantSkipCode:  RecoverySkipNoRecoveryAction,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst.GroupReplicationConditions.Reset()
			config.SetGroupReplicationFailoverGracePeriod(tt.gracePeriod)
			gotFunc, gotSkipCode := getCheckAndRecoverFunctionCode(tt.analysisEntry)
			assert.Equal(t, tt.wantFunc, gotFunc)
			assert.Equal(t, tt.wantSkipCode.String(), gotSkipCode.String())
		})
	}
}

// TestGroupReplicationFailoverGracePeriod verifies that a failover waits for the group while the
// condition lasts less than --group-replication-failover-grace-period, and runs afterwards.
func TestGroupReplicationFailoverGracePeriod(t *testing.T) {
	prevGrace := config.GetGroupReplicationFailoverGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationFailoverGracePeriod(prevGrace)
		inst.GroupReplicationConditions.Reset()
	})
	config.SetGroupReplicationFailoverGracePeriod(30 * time.Second)
	inst.GroupReplicationConditions.Reset()

	analysisEntry := &inst.DetectionAnalysis{
		Analysis:                inst.DeadPrimary,
		AnalyzedKeyspace:        "ks",
		AnalyzedShard:           "0",
		ShardGroupActiveMembers: 2,
	}
	start := time.Now()
	// The recovery is requested on every recovery poll.
	for elapsed := time.Duration(0); elapsed < 30*time.Second; elapsed += time.Second {
		require.Equal(t, RecoverySkipGroupReplicationGracePeriod, groupReplicationFailoverSkipCode(analysisEntry, start.Add(elapsed)), "after %v", elapsed)
	}
	assert.Equal(t, RecoverySkipNone, groupReplicationFailoverSkipCode(analysisEntry, start.Add(30*time.Second)))

	// Another shard has its own grace period.
	otherShard := *analysisEntry
	otherShard.AnalyzedShard = "1"
	assert.Equal(t, RecoverySkipGroupReplicationGracePeriod, groupReplicationFailoverSkipCode(&otherShard, start.Add(30*time.Second)))
}

// recoveryTopoFactory is the factory of the memory topology of the last
// groupReplicationRecoveryTest.
var recoveryTopoFactory *memorytopo.Factory

// cutOffCell makes the topology server of the cell unreachable, like the one of a partitioned
// cell: its reads do not answer until the caller gives up.
func cutOffCell(t *testing.T, cell string) {
	ctx := t.Context()
	require.NoError(t, ts.UpdateCellInfoFields(ctx, cell, func(ci *topodatapb.CellInfo) error {
		ci.ServerAddress = memorytopo.UnreachableServerAddr
		return nil
	}))
	cutOff, err := topo.NewWithFactory(recoveryTopoFactory, "", "")
	require.NoError(t, err)
	t.Cleanup(cutOff.Close)
	ts = cutOff
}

// groupReplicationRecoveryTest sets up the VTOrc backend, a memory topology and a mock tablet
// manager client for a group replication recovery on shard ks/0.
func groupReplicationRecoveryTest(t *testing.T, tablets ...*topodatapb.Tablet) *tmcmock.MockTabletManagerClient {
	return groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplication, tablets...)
}

// groupReplicationRecoveryTestWithPolicy is groupReplicationRecoveryTest with the given
// durability policy.
func groupReplicationRecoveryTestWithPolicy(t *testing.T, durability string, tablets ...*topodatapb.Tablet) *tmcmock.MockTabletManagerClient {
	// The backend is shared by the tests of the package; only its tables are cleared.
	orcDB, _, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	for _, table := range []string{"topology_recovery_steps", "topology_recovery", "recovery_detection", "vitess_tablet", "vitess_keyspace", "database_instance"} {
		_, err = orcDB.Exec("delete from " + table)
		require.NoError(t, err)
	}

	keyspaceInfo := &topo.KeyspaceInfo{Keyspace: &topodatapb.Keyspace{DurabilityPolicy: durability}}
	keyspaceInfo.SetKeyspaceName("ks")
	require.NoError(t, inst.SaveKeyspace(keyspaceInfo))

	oldTS, oldTMC := ts, tmc
	t.Cleanup(func() { ts, tmc = oldTS, oldTMC })
	ctx := t.Context()
	ts, recoveryTopoFactory = memorytopo.NewServerAndFactory(ctx, "zone1", "zone2", "zone3")
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: durability}))
	require.NoError(t, ts.CreateShard(ctx, "ks", "0"))
	for _, tablet := range tablets {
		require.NoError(t, inst.SaveTablet(tablet))
		require.NoError(t, ts.CreateTablet(ctx, tablet))
	}

	mockTMC := tmcmock.NewMockTabletManagerClient(gomock.NewController(t))
	tmc = mockTMC
	return mockTMC
}

func recoveryTablet(cell string, uid uint32, tabletType topodatapb.TabletType) *topodatapb.Tablet {
	return &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: cell, Uid: uid},
		Hostname:      "host",
		MysqlHostname: "host",
		MysqlPort:     int32(3000 + uid),
		Keyspace:      "ks",
		Shard:         "0",
		Type:          tabletType,
		PortMap:       map[string]int32{"grpc": int32(15000 + uid)},
	}
}

// setVoters records the voters of shard ks/0.
func setVoters(t *testing.T, tablets ...*topodatapb.Tablet) {
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationVoters = nil
		for _, tablet := range tablets {
			si.GroupReplicationVoters = append(si.GroupReplicationVoters, tablet.Alias)
		}
		return nil
	})
	require.NoError(t, err)
}

// readVoters returns the voters recorded for shard ks/0.
func readVoters(t *testing.T) []string {
	si, err := ts.GetShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	var voters []string
	for _, alias := range si.GroupReplicationVoters {
		voters = append(voters, topoproto.TabletAliasString(alias))
	}
	return voters
}

// sameTablet matches a tablet argument by alias.
func sameTablet(tablet *topodatapb.Tablet) gomock.Matcher {
	return gomock.Cond(func(x any) bool {
		other, ok := x.(*topodatapb.Tablet)
		return ok && topoproto.TabletAliasEqual(other.Alias, tablet.Alias)
	})
}

func TestPromoteGroupPrimary(t *testing.T) {
	tests := []struct {
		name        string
		memberRole  string
		viewID      string
		incarnation string
		wantErrCode vtrpcpb.Code
	}{
		{
			name:       "member is still the group primary",
			memberRole: mysql.GroupMemberRolePrimary,
		},
		{
			name:        "member is no longer the group primary",
			memberRole:  mysql.GroupMemberRoleSecondary,
			wantErrCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			name:        "member is the primary of the recorded incarnation",
			memberRole:  mysql.GroupMemberRolePrimary,
			viewID:      "1790000001:4",
			incarnation: "1790000001",
		},
		{
			// S7d of the Group Replication failover audit: a member formed a new group on its own.
			name:        "member is the primary of a group of another incarnation",
			memberRole:  mysql.GroupMemberRolePrimary,
			viewID:      "1799999999:1",
			incarnation: "1790000001",
			wantErrCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupPrimary := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
			oldPrimary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
			mockTMC := groupReplicationRecoveryTest(t, oldPrimary, groupPrimary)
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(oldPrimary)).Return(nil, errors.New("unreachable"))
			if tt.incarnation != "" {
				_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
					si.GroupReplicationIncarnation = tt.incarnation
					return nil
				})
				require.NoError(t, err)
			}

			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(groupPrimary)).Return(&replicationdatapb.FullStatus{
				GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
					PluginActive: true,
					MemberState:  mysql.GroupMemberStateOnline,
					MemberRole:   tt.memberRole,
					HasQuorum:    true,
					ViewId:       tt.viewID,
				},
			}, nil)
			promotions := 0
			if tt.wantErrCode == vtrpcpb.Code_OK {
				promotions = 1
			}
			mockTMC.EXPECT().ChangeType(gomock.Any(), sameTablet(groupPrimary), topodatapb.TabletType_PRIMARY, false).Return(nil).Times(promotions)
			mockTMC.EXPECT().PrimaryPosition(gomock.Any(), sameTablet(groupPrimary)).Return("MySQL56/6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41:1-10", nil).Times(promotions)
			mockTMC.EXPECT().PopulateReparentJournal(gomock.Any(), sameTablet(groupPrimary), gomock.Any(), gomock.Any(), groupPrimary.Alias, "MySQL56/6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41:1-10").Return(nil).Times(promotions)

			analysisEntry := &inst.DetectionAnalysis{
				Analysis:              inst.GroupPrimaryNotInTopo,
				AnalyzedInstanceAlias: groupPrimary.Alias,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}
			attempted, topologyRecovery, err := promoteGroupPrimary(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
			require.True(t, attempted)
			require.NotNil(t, topologyRecovery)
			if tt.wantErrCode != vtrpcpb.Code_OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, vterrors.Code(err))
				assert.False(t, topologyRecovery.IsSuccessful)
				return
			}
			require.NoError(t, err)
			assert.True(t, topologyRecovery.IsSuccessful)
			assert.True(t, topoproto.TabletAliasEqual(groupPrimary.Alias, topologyRecovery.SuccessorAlias))
		})
	}
}

// TestPromoteGroupPrimaryWithUnreachableCell reproduces the S9i chaos scenario: the old
// primary's cell, including its topology server, is partitioned from the other cells, and the
// group elected a member in another cell with a majority of the voters. Reading the shard's
// tablets waited for the partitioned cell until the recovery's deadline and failed with a partial
// result, so the elected member was not made the shard primary until the partition healed.
func TestPromoteGroupPrimaryWithUnreachableCell(t *testing.T) {
	oldCellTimeout := groupReplicationCellTimeout
	t.Cleanup(func() { groupReplicationCellTimeout = oldCellTimeout })
	groupReplicationCellTimeout = 100 * time.Millisecond

	groupPrimary := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	secondary := recoveryTablet("zone1", 101, topodatapb.TabletType_REPLICA)
	oldPrimary := recoveryTablet("zone2", 200, topodatapb.TabletType_PRIMARY)
	mockTMC := groupReplicationRecoveryTest(t, groupPrimary, secondary, oldPrimary)
	setVoters(t, groupPrimary, secondary, oldPrimary)
	cutOffCell(t, "zone2")

	view := []*replicationdatapb.GroupReplicationMember{
		{MemberUuid: "uuid-100", State: mysql.GroupMemberStateOnline, Role: mysql.GroupMemberRolePrimary},
		{MemberUuid: "uuid-101", State: mysql.GroupMemberStateOnline, Role: mysql.GroupMemberRoleSecondary},
	}
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(groupPrimary)).Return(&replicationdatapb.FullStatus{
		ServerUuid: "uuid-100",
		GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
			PluginActive: true, MemberState: mysql.GroupMemberStateOnline, MemberRole: mysql.GroupMemberRolePrimary,
			HasQuorum: true, Members: view,
		},
	}, nil)
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(secondary)).Return(&replicationdatapb.FullStatus{
		ServerUuid: "uuid-101",
		GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
			PluginActive: true, MemberState: mysql.GroupMemberStateOnline, MemberRole: mysql.GroupMemberRoleSecondary,
			HasQuorum: true, Members: view,
		},
	}, nil)
	mockTMC.EXPECT().ChangeType(gomock.Any(), sameTablet(groupPrimary), topodatapb.TabletType_PRIMARY, false).Return(nil)
	mockTMC.EXPECT().PrimaryPosition(gomock.Any(), sameTablet(groupPrimary)).Return("MySQL56/6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41:1-10", nil)
	mockTMC.EXPECT().PopulateReparentJournal(gomock.Any(), sameTablet(groupPrimary), gomock.Any(), gomock.Any(), groupPrimary.Alias, gomock.Any()).Return(nil)

	analysisEntry := &inst.DetectionAnalysis{
		Analysis:              inst.GroupPrimaryNotInTopo,
		AnalyzedInstanceAlias: groupPrimary.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	start := time.Now()
	attempted, topologyRecovery, err := promoteGroupPrimary(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
	require.True(t, attempted)
	require.NoError(t, err)
	assert.True(t, topologyRecovery.IsSuccessful)
	assert.Less(t, time.Since(start), topo.RemoteOperationTimeout/2, "the partitioned cell must not take the recovery's whole deadline")
}

// TestStartGroupReplicationOnMember checks that VTOrc only makes a voter join its group while
// another tablet is an active member of the shard's legitimate group with quorum. A join into no
// group blocks until MySQL's join timeout, during which a bootstrap on the member fails (the
// bootstrap starvation of the Group Replication failover audit).
func TestStartGroupReplicationOnMember(t *testing.T) {
	online := func(viewID string) *replicationdatapb.FullStatus {
		return &replicationdatapb.FullStatus{
			ServerUuid: "uuid-101",
			GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
				PluginActive: true,
				GroupName:    policy.GroupName("ks", "0"),
				MemberState:  mysql.GroupMemberStateOnline,
				MemberRole:   mysql.GroupMemberRolePrimary,
				HasQuorum:    true,
				ViewId:       viewID,
			},
		}
	}
	offline := &replicationdatapb.FullStatus{
		ServerUuid:             "uuid-101",
		GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{PluginActive: true, MemberState: mysql.GroupMemberStateError},
	}
	tests := []struct {
		name      string
		other     *replicationdatapb.FullStatus
		wantStart bool
	}{
		{name: "the group is active on another tablet", other: online("1790000001:5"), wantStart: true},
		{name: "no other member is active", other: offline},
		{name: "the other member is in a group of another incarnation", other: online("1799999999:1")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			member := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
			other := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
			mockTMC := groupReplicationRecoveryTest(t, other, member)
			_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
				si.GroupReplicationIncarnation = "1790000001"
				return nil
			})
			require.NoError(t, err)
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(other)).Return(tt.other, nil)
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(member)).Return(offline, nil)
			starts := 0
			if tt.wantStart {
				starts = 1
			}
			joined := make(chan struct{})
			mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(member), false).DoAndReturn(
				func(context.Context, *topodatapb.Tablet, bool) (*replicationdatapb.GroupReplicationStatus, error) {
					close(joined)
					return &replicationdatapb.GroupReplicationStatus{}, nil
				}).Times(starts)

			analysisEntry := &inst.DetectionAnalysis{
				Analysis:              inst.GroupMemberNotOnline,
				AnalyzedInstanceAlias: member.Alias,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}
			attempted, topologyRecovery, err := startGroupReplicationOnMember(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
			require.True(t, attempted)
			require.NotNil(t, topologyRecovery)
			if tt.wantStart {
				require.NoError(t, err)
				// The join runs in the background, without the shard lock.
				select {
				case <-joined:
				case <-time.After(30 * time.Second):
					require.FailNow(t, "the join was not started")
				}
				waitForGroupJoinsDone(t, member)
				return
			}
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			assert.False(t, groupJoinsInFlight.running(topoproto.TabletAliasString(member.Alias)))
		})
	}
}

// TestStartGroupReplicationOnMemberDoesNotWaitForUnreachableTablet reproduces S7d run 3 of the
// Group Replication failover audit: VTOrc checked that the group was active, then waited 9s for
// the FullStatus of an isolated tablet before it started the join, by which time the group had
// lost its majority. The join ended with the member alone in a group of its own, which kept the
// shard's group from being bootstrapped for 20s. The check must not wait for a tablet that does
// not answer once the answers it has show an active member of the legitimate group.
func TestStartGroupReplicationOnMemberDoesNotWaitForUnreachableTablet(t *testing.T) {
	member := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	isolated := recoveryTablet("zone2", 102, topodatapb.TabletType_REPLICA)
	mockTMC := groupReplicationRecoveryTest(t, primary, member, isolated)
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationIncarnation = "1790000001"
		return nil
	})
	require.NoError(t, err)
	active := groupMemberStatus(primary, primary, primary, isolated)
	active.GroupReplicationStatus.GroupName = policy.GroupName("ks", "0")
	active.GroupReplicationStatus.ViewId = "1790000001:7"
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(active, nil)
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(member)).Return(notMemberStatus(member), nil)
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(isolated)).DoAndReturn(
		func(ctx context.Context, _ *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
	mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(member), false).Return(&replicationdatapb.GroupReplicationStatus{}, nil)

	analysisEntry := &inst.DetectionAnalysis{
		Analysis:              inst.GroupMemberNotOnline,
		AnalyzedInstanceAlias: member.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	start := time.Now()
	attempted, _, err := startGroupReplicationOnMember(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
	require.True(t, attempted)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), groupJoinCheckTimeout, "the join must not wait for the isolated tablet")
	waitForGroupJoinsDone(t, member)
}

// waitForGroupJoinsDone waits until the join that VTOrc started on the tablet in the background
// returned.
func waitForGroupJoinsDone(t *testing.T, tablet *topodatapb.Tablet) {
	t.Helper()
	alias := topoproto.TabletAliasString(tablet.Alias)
	assert.Eventually(t, func() bool { return !groupJoinsInFlight.running(alias) }, 30*time.Second, 10*time.Millisecond)
}

// TestStartGroupReplicationOnMemberDoesNotHoldShardLock reproduces cycle 4 of run 3 of the G12
// chaos scenario: VTOrc's GroupMemberNotOnline recovery started a join on a voter right before the
// group lost its majority. The join blocked behind the voter's own START, which could not join
// anything, and the recovery held the shard lock for 29.5s, which delayed the bootstrap of the
// group, a recovery that needs the shard lock, by 7.7s. The legitimacy check still runs under the
// shard lock, but the join runs after the recovery released it; the polls that see the voter
// OFFLINE while its join runs do not start another one.
func TestStartGroupReplicationOnMemberDoesNotHoldShardLock(t *testing.T) {
	member := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	primary := recoveryTablet("zone2", 101, topodatapb.TabletType_PRIMARY)
	mockTMC := groupReplicationRecoveryTest(t, primary, member)
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationIncarnation = "1790000001"
		return nil
	})
	require.NoError(t, err)
	active := groupMemberStatus(primary, primary, primary, member)
	active.GroupReplicationStatus.GroupName = policy.GroupName("ks", "0")
	active.GroupReplicationStatus.ViewId = "1790000001:7"
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(active, nil)
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(member)).Return(notMemberStatus(member), nil)
	// The voter's join blocks, like a START GROUP_REPLICATION into a group that lost its majority.
	joinStarted, releaseJoin := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseJoin:
		default:
			close(releaseJoin)
		}
	})
	mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(member), false).DoAndReturn(
		func(ctx context.Context, _ *topodatapb.Tablet, _ bool) (*replicationdatapb.GroupReplicationStatus, error) {
			close(joinStarted)
			select {
			case <-releaseJoin:
				return &replicationdatapb.GroupReplicationStatus{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Times(1)

	analysisEntry := &inst.DetectionAnalysis{
		Analysis:              inst.GroupMemberNotOnline,
		AnalyzedInstanceAlias: member.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	// The recovery runs under the shard lock, as executeCheckAndRecoverFunction runs it.
	recovered := make(chan error, 1)
	go func() {
		lockCtx, unlock, err := LockShard(t.Context(), "ks", "0", "GroupMemberNotOnline")
		if err != nil {
			recovered <- err
			return
		}
		_, _, err = startGroupReplicationOnMember(lockCtx, analysisEntry, log.NewPrefixedLogger("test"))
		unlock(&err)
		recovered <- err
	}()
	select {
	case <-joinStarted:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the join was not started")
	}
	select {
	case err := <-recovered:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the recovery must not wait for the join")
	}

	// While the join still blocks, the shard lock is free, for example for a bootstrap.
	_, unlock, err := LockShard(t.Context(), "ks", "0", "GroupNotBootstrapped")
	require.NoError(t, err, "the shard lock must be free while the join runs")
	unlock(&err)
	// The polls that still see the voter OFFLINE do not start another join on it.
	code, skip := getCheckAndRecoverFunctionCode(analysisEntry)
	assert.Equal(t, startGroupReplicationFunc, code)
	assert.Equal(t, RecoverySkipGroupJoinInFlight, skip)

	close(releaseJoin)
	waitForGroupJoinsDone(t, member)
	_, skip = getCheckAndRecoverFunctionCode(analysisEntry)
	assert.Equal(t, RecoverySkipNone, skip, "a new join may start once the previous one returned")
}

// TestGroupReplicationRecoveriesRunWithoutShardPrimary checks which recoveries that are not
// shard-wide run while the shard has no primary tablet. GroupMemberNotOnline must: a group that
// was just bootstrapped, or that lost the majority of its voters, only gets a primary tablet once
// enough voters have joined it. In the S7d chaos runs, voters stayed out of their group for up to
// 33s because VTOrc aborted every GroupMemberNotOnline with "no primary tablet found".
func TestGroupReplicationRecoveriesRunWithoutShardPrimary(t *testing.T) {
	assert.True(t, recoveryRunsWithoutShardPrimary(startGroupReplicationFunc))
	assert.True(t, recoveryRunsWithoutShardPrimary(promoteGroupPrimaryFunc))
	assert.True(t, recoveryRunsWithoutShardPrimary(updateGroupReplicationVotersFunc))
	// Asynchronous replication is repaired relative to the shard primary.
	assert.False(t, recoveryRunsWithoutShardPrimary(fixReplicaFunc))
	assert.False(t, recoveryRunsWithoutShardPrimary(reconcileStaleTopoPrimaryFunc))
}

func TestBootstrapGroupReplication(t *testing.T) {
	const (
		groupName = "6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41"
		otherUUID = "230ea8ea-81e3-11e4-972a-e25ec4bd140a"
	)
	offline := func(position, received string) *replicationdatapb.FullStatus {
		return &replicationdatapb.FullStatus{
			PrimaryStatus: &replicationdatapb.PrimaryStatus{Position: "MySQL56/" + position},
			GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
				PluginActive:           true,
				MemberState:            mysql.GroupMemberStateOffline,
				ReceivedTransactionSet: received,
			},
		}
	}
	errUnreachable := errors.New("unreachable")

	tests := []struct {
		name string
		// statuses are the FullStatus results of tablets 100, 101, 102 (REPLICA) and 103 (RDONLY).
		statuses [4]*replicationdatapb.FullStatus
		errs     [4]error
		// want is the uid of the tablet that bootstraps the group, 0 for none.
		want        uint32
		wantErrCode vtrpcpb.Code
		// startGraceOver is whether VTOrc observed the STARTs in progress for longer than the grace.
		startGraceOver bool
	}{
		{
			name: "the member with the most transactions bootstraps",
			statuses: [4]*replicationdatapb.FullStatus{
				offline(groupName+":1-10", ""),
				offline(groupName+":1-12", ""),
				offline(groupName+":1-11", ""),
				offline(groupName+":1-20", ""),
			},
			want: 101,
		},
		{
			name: "received but unapplied transactions count",
			statuses: [4]*replicationdatapb.FullStatus{
				offline(groupName+":1-10", groupName+":1-15"),
				offline(groupName+":1-12", ""),
				offline(groupName+":1-11", ""),
			},
			errs: [4]error{nil, nil, nil, errUnreachable},
			want: 100,
		},
		{
			name: "equal members: the lowest alias bootstraps",
			statuses: [4]*replicationdatapb.FullStatus{
				offline(groupName+":1-10", ""),
				offline(groupName+":1-10", ""),
				offline(groupName+":1-10", ""),
				offline(groupName+":1-10", ""),
			},
			want: 100,
		},
		{
			name: "diverged members: nothing is bootstrapped",
			statuses: [4]*replicationdatapb.FullStatus{
				offline(groupName+":1-10", ""),
				offline(groupName+":1-9,"+otherUUID+":1", ""),
				offline(groupName+":1-8", ""),
				offline(groupName+":1-8", ""),
			},
			wantErrCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			name: "a tablet is already an active member: nothing is bootstrapped",
			statuses: [4]*replicationdatapb.FullStatus{
				offline(groupName+":1-10", ""),
				offline(groupName+":1-10", ""),
				offline(groupName+":1-10", ""),
				{GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{PluginActive: true, MemberState: mysql.GroupMemberStateOnline}},
			},
			wantErrCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			// sro-eval case A: a START that finds no group reports OFFLINE for up to about a minute,
			// and can still end in a group of its own next to the one a bootstrap would create.
			name: "a START GROUP_REPLICATION is in progress on a voter: nothing is bootstrapped",
			statuses: [4]*replicationdatapb.FullStatus{
				offline(groupName+":1-10", ""),
				func() *replicationdatapb.FullStatus {
					status := offline(groupName+":1-10", "")
					status.GroupReplicationStatus.StartInProgress = true
					return status
				}(),
				offline(groupName+":1-10", ""),
				offline(groupName+":1-10", ""),
			},
			wantErrCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			// Once VTOrc observed the START for longer than the grace, it bootstraps anyway: the group the
			// START may form stays read-only, and its tablet leaves it. Among equal members, it prefers one
			// without a START, which MySQL would refuse to bootstrap.
			name: "a START GROUP_REPLICATION in progress for longer than the grace: another voter bootstraps",
			statuses: [4]*replicationdatapb.FullStatus{
				func() *replicationdatapb.FullStatus {
					status := offline(groupName+":1-10", "")
					status.GroupReplicationStatus.StartInProgress = true
					return status
				}(),
				offline(groupName+":1-10", ""),
				offline(groupName+":1-10", ""),
				offline(groupName+":1-10", ""),
			},
			startGraceOver: true,
			want:           101,
		},
		{
			name: "a voter is unreachable: nothing is bootstrapped",
			statuses: [4]*replicationdatapb.FullStatus{
				offline(groupName+":1-10", ""),
				nil,
				offline(groupName+":1-10", ""),
				offline(groupName+":1-10", ""),
			},
			errs:        [4]error{nil, errUnreachable, nil, nil},
			wantErrCode: vtrpcpb.Code_UNKNOWN,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tablets := []*topodatapb.Tablet{
				recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA),
				recoveryTablet("zone1", 101, topodatapb.TabletType_REPLICA),
				recoveryTablet("zone2", 102, topodatapb.TabletType_REPLICA),
				recoveryTablet("zone2", 103, topodatapb.TabletType_RDONLY),
			}
			mockTMC := groupReplicationRecoveryTest(t, tablets...)
			inst.GroupReplicationConditions.Reset()
			if tt.startGraceOver {
				old := inst.SetGroupStartInProgressGrace(0)
				t.Cleanup(func() { inst.SetGroupStartInProgressGrace(old) })
			}
			// The RDONLY tablet is not a voter.
			setVoters(t, tablets[:3]...)
			for i, tablet := range tablets {
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).Return(tt.statuses[i], tt.errs[i])
			}
			var joins atomic.Int32
			for _, tablet := range tablets {
				times, joinTimes := 0, 0
				if tablet.Alias.Uid == tt.want {
					times = 1
				} else if tt.want != 0 && tablet.Type != topodatapb.TabletType_RDONLY {
					// The other voters are made to join the new group right away.
					joinTimes = 1
				}
				mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablet), true).
					DoAndReturn(func(context.Context, *topodatapb.Tablet, bool) (*replicationdatapb.GroupReplicationStatus, error) {
						return &replicationdatapb.GroupReplicationStatus{ViewId: "1790000123:1"}, nil
					}).Times(times)
				mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablet), false).
					DoAndReturn(func(context.Context, *topodatapb.Tablet, bool) (*replicationdatapb.GroupReplicationStatus, error) {
						joins.Add(1)
						return &replicationdatapb.GroupReplicationStatus{ViewId: "1790000123:2"}, nil
					}).Times(joinTimes)
			}

			analysisEntry := &inst.DetectionAnalysis{
				Analysis:              inst.GroupNotBootstrapped,
				AnalyzedInstanceAlias: tablets[0].Alias,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}
			// VTOrc bootstraps a group under the shard lock.
			lockedCtx, unlock, err := ts.LockShard(t.Context(), "ks", "0", "test")
			require.NoError(t, err)
			defer unlock(&err)
			attempted, topologyRecovery, err := bootstrapGroupReplication(lockedCtx, analysisEntry, log.NewPrefixedLogger("test"))
			require.True(t, attempted)
			require.NotNil(t, topologyRecovery)
			if tt.want == 0 {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, vterrors.Code(err))
				assert.False(t, topologyRecovery.IsSuccessful)
				return
			}
			require.NoError(t, err)
			assert.True(t, topologyRecovery.IsSuccessful)
			assert.Equal(t, tt.want, topologyRecovery.SuccessorAlias.Uid)
			// The bootstrapped group is recorded as the shard's legitimate group, in the topology
			// and in VTOrc's own copy, which its analysis uses right away.
			si, err := ts.GetShard(t.Context(), "ks", "0")
			require.NoError(t, err)
			assert.Equal(t, "1790000123", si.GroupReplicationIncarnation)
			incarnation, err := inst.ReadShardGroupReplicationIncarnation("ks", "0")
			require.NoError(t, err)
			assert.Equal(t, "1790000123", incarnation)
			// The two other voters join the group without waiting for another recovery.
			assert.Eventually(t, func() bool { return joins.Load() == 2 }, 30*time.Second, 10*time.Millisecond)
		})
	}
}

// TestBootstrapGroupReplicationVoters verifies that only the voters recorded in the shard record
// take part in the bootstrap: the most advanced voter bootstraps the group even when a tablet
// that is not a voter has more transactions, and nothing is bootstrapped before voters exist.
func TestBootstrapGroupReplicationVoters(t *testing.T) {
	const groupName = "6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41"
	offline := func(position string) *replicationdatapb.FullStatus {
		return &replicationdatapb.FullStatus{
			PrimaryStatus: &replicationdatapb.PrimaryStatus{Position: "MySQL56/" + position},
			GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
				PluginActive: true,
				MemberState:  mysql.GroupMemberStateOffline,
			},
		}
	}
	tests := []struct {
		name string
		// voters are the uids of the voters.
		voters []uint32
		// want is the uid of the tablet that bootstraps the group, 0 for none.
		want uint32
	}{
		{
			name:   "the most advanced voter bootstraps, not the most advanced replica",
			voters: []uint32{100, 200},
			want:   200,
		},
		{
			name: "no voter is selected yet: nothing is bootstrapped",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tablets := []*topodatapb.Tablet{
				recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA),
				// An asynchronous replica of the previous group, with the most transactions.
				recoveryTablet("zone1", 101, topodatapb.TabletType_REPLICA),
				recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA),
			}
			statuses := []*replicationdatapb.FullStatus{
				offline(groupName + ":1-10"),
				offline(groupName + ":1-30"),
				offline(groupName + ":1-20"),
			}
			mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, tablets...)
			var voters []*topodatapb.Tablet
			for _, tablet := range tablets {
				if slices.Contains(tt.voters, tablet.Alias.Uid) {
					voters = append(voters, tablet)
				}
			}
			setVoters(t, voters...)
			joined := make(chan uint32, len(tablets))
			for i, tablet := range tablets {
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).Return(statuses[i], nil).MaxTimes(1)
				times, joinTimes := 0, 0
				if tablet.Alias.Uid == tt.want {
					times = 1
				} else if tt.want != 0 && slices.Contains(tt.voters, tablet.Alias.Uid) {
					joinTimes = 1
				}
				mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablet), true).Return(&replicationdatapb.GroupReplicationStatus{ViewId: "1790000123:1"}, nil).Times(times)
				mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablet), false).
					DoAndReturn(func(_ context.Context, tablet *topodatapb.Tablet, _ bool) (*replicationdatapb.GroupReplicationStatus, error) {
						joined <- tablet.Alias.Uid
						return &replicationdatapb.GroupReplicationStatus{ViewId: "1790000123:2"}, nil
					}).Times(joinTimes)
			}

			analysisEntry := &inst.DetectionAnalysis{
				Analysis:              inst.GroupNotBootstrapped,
				AnalyzedInstanceAlias: tablets[0].Alias,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}
			// VTOrc bootstraps a group under the shard lock.
			lockedCtx, unlock, err := ts.LockShard(t.Context(), "ks", "0", "test")
			require.NoError(t, err)
			defer unlock(&err)
			attempted, topologyRecovery, err := bootstrapGroupReplication(lockedCtx, analysisEntry, log.NewPrefixedLogger("test"))
			require.True(t, attempted)
			require.NotNil(t, topologyRecovery)
			if tt.want == 0 {
				require.Error(t, err)
				assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, topologyRecovery.SuccessorAlias.Uid)
			// The other voter joins the new group; the replica that is not a voter does not.
			select {
			case uid := <-joined:
				assert.EqualValues(t, 100, uid)
			case <-time.After(30 * time.Second):
				require.Fail(t, "the other voter was not made to join the group")
			}
		})
	}
}

func TestGetCheckAndRecoverFunctionCodeGroupVotersOutOfDate(t *testing.T) {
	gotFunc, gotSkipCode := getCheckAndRecoverFunctionCode(&inst.DetectionAnalysis{Analysis: inst.GroupVotersOutOfDate})
	assert.Equal(t, updateGroupReplicationVotersFunc, gotFunc)
	assert.Equal(t, RecoverySkipNone, gotSkipCode)
	assert.True(t, hasActionableRecovery(gotFunc))
	assert.False(t, isShardWideRecovery(gotFunc))
	assert.Equal(t, UpdateGroupReplicationVotersRecoveryName, getRecoverFunctionName(gotFunc))
}

// voterTestUUID is the server_uuid of a tablet's MySQL in the voter tests.
func voterTestUUID(tablet *topodatapb.Tablet) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", tablet.Alias.Uid)
}

// groupMemberStatus returns the FullStatus of a tablet whose MySQL is an active member of a group
// with quorum, whose primary is groupPrimary and whose ONLINE members are the given tablets.
func groupMemberStatus(tablet, groupPrimary *topodatapb.Tablet, online ...*topodatapb.Tablet) *replicationdatapb.FullStatus {
	role := mysql.GroupMemberRoleSecondary
	if topoproto.TabletAliasEqual(tablet.Alias, groupPrimary.Alias) {
		role = mysql.GroupMemberRolePrimary
	}
	var members []*replicationdatapb.GroupReplicationMember
	for _, m := range online {
		memberRole := mysql.GroupMemberRoleSecondary
		if topoproto.TabletAliasEqual(m.Alias, groupPrimary.Alias) {
			memberRole = mysql.GroupMemberRolePrimary
		}
		members = append(members, &replicationdatapb.GroupReplicationMember{MemberUuid: voterTestUUID(m), State: mysql.GroupMemberStateOnline, Role: memberRole})
	}
	return &replicationdatapb.FullStatus{
		ServerUuid:               voterTestUUID(tablet),
		ReplicationConfiguration: &replicationdatapb.Configuration{ReplicaNetTimeout: 8},
		GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
			PluginActive: true,
			MemberState:  mysql.GroupMemberStateOnline,
			MemberRole:   role,
			PrimaryUuid:  voterTestUUID(groupPrimary),
			HasQuorum:    true,
			Members:      members,
		},
	}
}

// notMemberStatus returns the FullStatus of a tablet whose MySQL is not a group member.
func notMemberStatus(tablet *topodatapb.Tablet) *replicationdatapb.FullStatus {
	return &replicationdatapb.FullStatus{
		ServerUuid: voterTestUUID(tablet),
		GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
			PluginActive: true,
			MemberState:  mysql.GroupMemberStateOffline,
		},
	}
}

func TestUpdateGroupReplicationVoters(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	errUnreachable := errors.New("unreachable")

	// The group_replication_cross_cell policy allows one voter per cell.
	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	replica := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	crossCellVoter := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	crossCellReplica := recoveryTablet("zone2", 201, topodatapb.TabletType_REPLICA)

	type expectations struct {
		mockTMC *tmcmock.MockTabletManagerClient
	}
	tests := []struct {
		name        string
		tablets     []*topodatapb.Tablet
		voters      []*topodatapb.Tablet
		gracePeriod time.Duration
		// setup sets the FullStatus results and the expected membership changes.
		setup       func(t *testing.T, e expectations)
		wantVoters  []string
		wantErrCode vtrpcpb.Code
	}{
		{
			name:        "a voter failed for longer than the grace period: a tablet of its cell replaces it and joins",
			tablets:     []*topodatapb.Tablet{primary, crossCellVoter, crossCellReplica},
			voters:      []*topodatapb.Tablet{primary, crossCellVoter},
			gracePeriod: 0,
			setup: func(t *testing.T, e expectations) {
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(groupMemberStatus(primary, primary, primary), nil)
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(crossCellVoter)).Return(nil, errUnreachable)
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(crossCellReplica)).Return(notMemberStatus(crossCellReplica), nil)
				e.mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(crossCellReplica), false).Return(&replicationdatapb.GroupReplicationStatus{}, nil)
			},
			wantVoters: []string{"zone1-0000000101", "zone2-0000000201"},
		},
		{
			name:        "a voter unreachable within the grace period keeps its seat",
			tablets:     []*topodatapb.Tablet{primary, crossCellVoter, crossCellReplica},
			voters:      []*topodatapb.Tablet{primary, crossCellVoter},
			gracePeriod: time.Hour,
			setup: func(t *testing.T, e expectations) {
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(groupMemberStatus(primary, primary, primary), nil)
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(crossCellVoter)).Return(nil, errUnreachable)
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(crossCellReplica)).Return(notMemberStatus(crossCellReplica), nil)
			},
			wantVoters: []string{"zone1-0000000101", "zone2-0000000200"},
		},
		{
			name:        "an unreachable voter whose MySQL is still an active member keeps its seat",
			tablets:     []*topodatapb.Tablet{primary, crossCellVoter, crossCellReplica},
			voters:      []*topodatapb.Tablet{primary, crossCellVoter},
			gracePeriod: 0,
			setup: func(t *testing.T, e expectations) {
				// VTOrc last saw the server_uuid of the voter's MySQL, which the primary still
				// sees ONLINE.
				require.NoError(t, inst.WriteInstance(&inst.Instance{
					InstanceAlias: crossCellVoter.Alias,
					Hostname:      crossCellVoter.MysqlHostname,
					Port:          int(crossCellVoter.MysqlPort),
					ServerUUID:    voterTestUUID(crossCellVoter),
				}, true, nil))
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(groupMemberStatus(primary, primary, primary, crossCellVoter), nil)
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(crossCellVoter)).Return(nil, errUnreachable)
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(crossCellReplica)).Return(notMemberStatus(crossCellReplica), nil)
			},
			wantVoters: []string{"zone1-0000000101", "zone2-0000000200"},
		},
		{
			name:    "an active member that is not a voter leaves the group and replicates from the group primary",
			tablets: []*topodatapb.Tablet{primary, replica, crossCellVoter},
			voters:  []*topodatapb.Tablet{primary, crossCellVoter},
			setup: func(t *testing.T, e expectations) {
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(groupMemberStatus(primary, primary, primary, replica, crossCellVoter), nil).Times(2)
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(groupMemberStatus(replica, primary, primary, replica, crossCellVoter), nil)
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(crossCellVoter)).Return(groupMemberStatus(crossCellVoter, primary, primary, replica, crossCellVoter), nil)
				gomock.InOrder(
					e.mockTMC.EXPECT().StopGroupReplication(gomock.Any(), sameTablet(replica)).Return(&replicationdatapb.GroupReplicationStatus{}, nil),
					e.mockTMC.EXPECT().SetReplicationSource(gomock.Any(), sameTablet(replica), primary.Alias, int64(0), "", true, false, 4.0).Return(nil),
				)
			},
			wantVoters: []string{"zone1-0000000101", "zone2-0000000200"},
		},
		{
			name:    "the group primary keeps its seat; the voter of its cell leaves the group",
			tablets: []*topodatapb.Tablet{primary, replica, crossCellVoter},
			voters:  []*topodatapb.Tablet{primary, crossCellVoter},
			setup: func(t *testing.T, e expectations) {
				// The group elected the replica, which is not a voter.
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(groupMemberStatus(replica, replica, primary, replica, crossCellVoter), nil).Times(2)
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(groupMemberStatus(primary, replica, primary, replica, crossCellVoter), nil)
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(crossCellVoter)).Return(groupMemberStatus(crossCellVoter, replica, primary, replica, crossCellVoter), nil)
				e.mockTMC.EXPECT().StopGroupReplication(gomock.Any(), sameTablet(replica)).Times(0)
				e.mockTMC.EXPECT().StopGroupReplication(gomock.Any(), sameTablet(primary)).Return(&replicationdatapb.GroupReplicationStatus{}, nil)
				e.mockTMC.EXPECT().SetReplicationSource(gomock.Any(), sameTablet(primary), replica.Alias, int64(0), "", true, false, 4.0).Return(nil)
			},
			wantVoters: []string{"zone1-0000000100", "zone2-0000000200"},
		},
		{
			name:    "a member that is not a voter stays while the group would lose its majority without it",
			tablets: []*topodatapb.Tablet{primary, replica},
			voters:  []*topodatapb.Tablet{primary},
			setup: func(t *testing.T, e expectations) {
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).Return(groupMemberStatus(primary, primary, primary, replica), nil).Times(2)
				e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(replica)).Return(groupMemberStatus(replica, primary, primary, replica), nil)
				e.mockTMC.EXPECT().StopGroupReplication(gomock.Any(), gomock.Any()).Times(0)
			},
			wantVoters:  []string{"zone1-0000000101"},
			wantErrCode: vtrpcpb.Code_FAILED_PRECONDITION,
		},
		{
			name:    "no voter is selected and no group exists: the voters are selected",
			tablets: []*topodatapb.Tablet{replica, primary, crossCellVoter},
			setup: func(t *testing.T, e expectations) {
				for _, tablet := range []*topodatapb.Tablet{replica, primary, crossCellVoter} {
					e.mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).Return(notMemberStatus(tablet), nil)
				}
			},
			wantVoters: []string{"zone1-0000000100", "zone2-0000000200"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst.UnreachableGroupTablets.Reset()
			config.SetGroupReplicationVoterReplacementGracePeriod(tt.gracePeriod)
			mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, tt.tablets...)
			setVoters(t, tt.voters...)
			tt.setup(t, expectations{mockTMC: mockTMC})

			analysisEntry := &inst.DetectionAnalysis{
				Analysis:              inst.GroupVotersOutOfDate,
				AnalyzedInstanceAlias: tt.tablets[0].Alias,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}
			attempted, topologyRecovery, err := updateGroupReplicationVoters(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
			require.True(t, attempted)
			require.NotNil(t, topologyRecovery)
			if tt.wantErrCode != vtrpcpb.Code_OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, vterrors.Code(err))
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantVoters, readVoters(t))
		})
	}
}

// TestReconcileStaleTopoPrimaryGroupReplicationVoter reproduces NEW-3 of the Group Replication
// failover audit: StaleTopoPrimary configured the default replication channel on an old primary
// that is a voter of the shard's group, next to the group's own rejoin. Under a group replication
// policy, a voter only gets its tablet type fixed; a tablet that is not a voter is still made an
// asynchronous replica of the primary.
func TestReconcileStaleTopoPrimaryGroupReplicationVoter(t *testing.T) {
	tests := []struct {
		name          string
		staleIsVoter  bool
		wantRepointed bool
	}{
		{name: "stale primary is a voter", staleIsVoter: true},
		{name: "stale primary is not a voter", wantRepointed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			primary := recoveryTablet("zone1", 100, topodatapb.TabletType_PRIMARY)
			primary.PrimaryTermStartTime = &vttimepb.Time{Seconds: 1000}
			stale := recoveryTablet("zone2", 200, topodatapb.TabletType_PRIMARY)
			stale.PrimaryTermStartTime = &vttimepb.Time{Seconds: 500}
			other := recoveryTablet("zone2", 201, topodatapb.TabletType_REPLICA)
			mockTMC := groupReplicationRecoveryTest(t, primary, stale, other)
			if tt.staleIsVoter {
				setVoters(t, primary, stale)
			} else {
				setVoters(t, primary, other)
			}

			mockTMC.EXPECT().DemotePrimary(gomock.Any(), sameTablet(stale), true).Return(&replicationdatapb.PrimaryStatus{}, nil)
			repoints := 0
			if tt.wantRepointed {
				repoints = 1
			}
			mockTMC.EXPECT().SetReplicationSource(gomock.Any(), sameTablet(stale), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil).Times(repoints)

			analysisEntry := &inst.DetectionAnalysis{
				Analysis:              inst.StaleTopoPrimary,
				AnalyzedInstanceAlias: stale.Alias,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}
			attempted, topologyRecovery, err := reconcileStaleTopoPrimary(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
			require.NoError(t, err)
			require.True(t, attempted)
			require.NotNil(t, topologyRecovery)
			updated, err := ts.GetTablet(t.Context(), stale.Alias)
			require.NoError(t, err)
			assert.Equal(t, topodatapb.TabletType_REPLICA, updated.Type)
		})
	}
}
