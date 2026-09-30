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

// groupReplicationRecoveryTest sets up the VTOrc backend, a memory topology and a mock tablet
// manager client for a group replication recovery on shard ks/0.
func groupReplicationRecoveryTest(t *testing.T, tablets ...*topodatapb.Tablet) *tmcmock.MockTabletManagerClient {
	// The backend is shared by the tests of the package; only its tables are cleared.
	orcDB, _, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	for _, table := range []string{"topology_recovery_steps", "topology_recovery", "recovery_detection", "vitess_tablet", "vitess_keyspace"} {
		_, err = orcDB.Exec("delete from " + table)
		require.NoError(t, err)
	}

	keyspaceInfo := &topo.KeyspaceInfo{Keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplication}}
	keyspaceInfo.SetKeyspaceName("ks")
	require.NoError(t, inst.SaveKeyspace(keyspaceInfo))

	oldTS, oldTMC := ts, tmc
	t.Cleanup(func() { ts, tmc = oldTS, oldTMC })
	ctx := t.Context()
	ts = memorytopo.NewServer(ctx, "zone1", "zone2")
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityGroupReplication}))
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupPrimary := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
			mockTMC := groupReplicationRecoveryTest(t, recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY), groupPrimary)

			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(groupPrimary)).Return(&replicationdatapb.FullStatus{
				GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
					PluginActive: true,
					MemberState:  mysql.GroupMemberStateOnline,
					MemberRole:   tt.memberRole,
					HasQuorum:    true,
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

func TestStartGroupReplicationOnMember(t *testing.T) {
	member := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	mockTMC := groupReplicationRecoveryTest(t, recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY), member)
	mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(member), false).Return(&replicationdatapb.GroupReplicationStatus{}, nil)

	analysisEntry := &inst.DetectionAnalysis{
		Analysis:              inst.GroupMemberNotOnline,
		AnalyzedInstanceAlias: member.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	attempted, topologyRecovery, err := startGroupReplicationOnMember(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
	require.NoError(t, err)
	require.True(t, attempted)
	require.NotNil(t, topologyRecovery)
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
			name: "a voting member is unreachable: nothing is bootstrapped",
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
			for i, tablet := range tablets {
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).Return(tt.statuses[i], tt.errs[i])
			}
			for _, tablet := range tablets {
				times := 0
				if tablet.Alias.Uid == tt.want {
					times = 1
				}
				mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablet), true).
					DoAndReturn(func(context.Context, *topodatapb.Tablet, bool) (*replicationdatapb.GroupReplicationStatus, error) {
						return &replicationdatapb.GroupReplicationStatus{}, nil
					}).Times(times)
			}

			analysisEntry := &inst.DetectionAnalysis{
				Analysis:              inst.GroupNotBootstrapped,
				AnalyzedInstanceAlias: tablets[0].Alias,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}
			attempted, topologyRecovery, err := bootstrapGroupReplication(t.Context(), analysisEntry, log.NewPrefixedLogger("test"))
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
		})
	}
}
