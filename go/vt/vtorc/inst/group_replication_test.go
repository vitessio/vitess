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

package inst

import (
	"testing"
	"time"

	"github.com/sjmudd/stopwatch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/test"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func grTablet(cell string, uid uint32, tabletType topodatapb.TabletType) *topodatapb.Tablet {
	return &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: cell, Uid: uid},
		Hostname:      "localhost",
		Keyspace:      "ks",
		Shard:         "0",
		Type:          tabletType,
		MysqlHostname: "localhost",
		MysqlPort:     int32(6000 + uid),
	}
}

// grRow returns the analysis row of a reachable tablet. Group members have no default
// replication channel, so they look like replication sources (is_primary) whose replication is
// stopped.
func grRow(tablet *topodatapb.Tablet, durability string) *test.InfoForRecoveryAnalysis {
	row := &test.InfoForRecoveryAnalysis{
		TabletInfo:         tablet,
		DurabilityPolicy:   durability,
		LastCheckValid:     1,
		IsPrimary:          1,
		ReplicationStopped: 1,
		ReadOnly:           1,
		CurrentTabletType:  int(tablet.Type),
		ServerUUID:         serverUUID(tablet),
	}
	if tablet.Type == topodatapb.TabletType_PRIMARY {
		row.ReadOnly = 0
		row.ReplicationStopped = 0
	}
	return row
}

func serverUUID(tablet *topodatapb.Tablet) string {
	return map[uint32]string{
		100: "00000000-0000-0000-0000-000000000100",
		101: "00000000-0000-0000-0000-000000000101",
		102: "00000000-0000-0000-0000-000000000102",
		200: "00000000-0000-0000-0000-000000000200",
	}[tablet.Alias.Uid]
}

// member makes the row's MySQL a member of a group in the given state.
func member(row *test.InfoForRecoveryAnalysis, state, role string, quorum bool, primary *topodatapb.Tablet) *test.InfoForRecoveryAnalysis {
	row.GroupPluginActive = 1
	row.GroupMemberState = state
	row.GroupMemberRole = role
	if quorum {
		row.GroupHasQuorum = 1
		row.GroupPrimaryUUID = serverUUID(primary)
	}
	return row
}

func runAnalysis(t *testing.T, rows []*test.InfoForRecoveryAnalysis) []*DetectionAnalysis {
	oldDB := db.Db
	t.Cleanup(func() { db.Db = oldDB })
	var rowMaps []sqlutils.RowMap
	for _, row := range rows {
		row.SetValuesFromTabletInfo()
		rowMaps = append(rowMaps, row.ConvertToRowMap())
	}
	db.Db = test.NewTestDB([][]sqlutils.RowMap{rowMaps})
	analyses, err := GetDetectionAnalysis("", "", &DetectionAnalysisHints{})
	require.NoError(t, err)
	return analyses
}

func analysisCodes(analyses []*DetectionAnalysis) map[string]AnalysisCode {
	codes := make(map[string]AnalysisCode)
	for _, a := range analyses {
		if a.Analysis != NoProblem {
			codes[topoproto.TabletAliasString(a.AnalyzedInstanceAlias)] = a.Analysis
		}
	}
	return codes
}

func TestGetDetectionAnalysisGroupReplication(t *testing.T) {
	resetPrimaryHealthState()
	oldGrace := groupPrimaryNotInTopoGracePeriod
	groupPrimaryNotInTopoGracePeriod = 0
	t.Cleanup(func() {
		groupPrimaryNotInTopoGracePeriod = oldGrace
		GroupReplicationConditions.Reset()
	})

	primary := grTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	replica := grTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	replica2 := grTablet("zone1", 102, topodatapb.TabletType_REPLICA)
	crossCellReplica := grTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	rdonly := grTablet("zone1", 102, topodatapb.TabletType_RDONLY)
	backup := grTablet("zone1", 102, topodatapb.TabletType_BACKUP)
	gr := policy.DurabilityGroupReplication

	tests := []struct {
		name string
		rows func() []*test.InfoForRecoveryAnalysis
		// want is the analysis of every tablet that has a problem.
		want map[string]AnalysisCode
		// notWant are analyses that must not be reported, for cases where the other analyses
		// do not matter.
		notWant []AnalysisCode
	}{
		{
			name: "group elected a replica: its tablet is promoted, the demoted primary is left alone",
			rows: func() []*test.InfoForRecoveryAnalysis {
				// The old primary's tablet already demoted itself; the topology is not updated yet.
				oldPrimary := member(grRow(primary, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, replica)
				oldPrimary.ReadOnly = 1
				oldPrimary.CurrentTabletType = int(topodatapb.TabletType_REPLICA)
				newPrimary := member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, replica)
				newPrimary.ReadOnly = 0
				return []*test.InfoForRecoveryAnalysis{oldPrimary, newPrimary}
			},
			want: map[string]AnalysisCode{"zone1-0000000100": GroupPrimaryNotInTopo},
		},
		{
			name: "group next to a working primary outside of it, with a semi-sync policy",
			rows: func() []*test.InfoForRecoveryAnalysis {
				groupPrimary := member(grRow(replica, policy.DurabilitySemiSync), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, replica)
				groupPrimary.ReadOnly = 0
				return []*test.InfoForRecoveryAnalysis{grRow(primary, policy.DurabilitySemiSync), groupPrimary}
			},
			notWant: []AnalysisCode{GroupPrimaryNotInTopo},
		},
		{
			name: "group elected a replica during a conversion, with a semi-sync policy",
			rows: func() []*test.InfoForRecoveryAnalysis {
				oldPrimary := member(grRow(primary, policy.DurabilitySemiSync), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, replica)
				oldPrimary.ReadOnly = 1
				newPrimary := member(grRow(replica, policy.DurabilitySemiSync), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, replica)
				newPrimary.ReadOnly = 0
				return []*test.InfoForRecoveryAnalysis{oldPrimary, newPrimary}
			},
			want: map[string]AnalysisCode{"zone1-0000000100": GroupPrimaryNotInTopo},
		},
		{
			name: "dead primary: the group primary's tablet is promoted before any failover",
			rows: func() []*test.InfoForRecoveryAnalysis {
				deadPrimary := member(grRow(primary, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary)
				deadPrimary.LastCheckValid = 0
				newPrimary := member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, replica)
				newPrimary.ReadOnly = 0
				secondary := member(grRow(replica2, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, replica)
				return []*test.InfoForRecoveryAnalysis{deadPrimary, newPrimary, secondary}
			},
			want: map[string]AnalysisCode{
				"zone1-0000000101": DeadPrimaryWithoutReplicas,
				"zone1-0000000100": GroupPrimaryNotInTopo,
			},
		},
		{
			name: "voting member is offline while the group is active; the async replica keeps its analysis",
			rows: func() []*test.InfoForRecoveryAnalysis {
				groupPrimary := member(grRow(primary, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary)
				offline := member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil)
				asyncReplica := grRow(rdonly, gr)
				asyncReplica.IsPrimary = 0
				asyncReplica.PrimaryTabletInfo = primary
				return []*test.InfoForRecoveryAnalysis{groupPrimary, offline, asyncReplica}
			},
			want: map[string]AnalysisCode{
				"zone1-0000000100": GroupMemberNotOnline,
				"zone1-0000000102": ReplicationStopped,
			},
		},
		{
			name: "no member is active: the group is bootstrapped instead of electing a primary",
			rows: func() []*test.InfoForRecoveryAnalysis {
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil),
					grRow(replica2, gr),
				}
			},
			want: map[string]AnalysisCode{"zone1-0000000100": GroupNotBootstrapped},
		},
		{
			name: "no member is active but a voting member is unreachable: nothing is bootstrapped",
			rows: func() []*test.InfoForRecoveryAnalysis {
				unreachable := member(grRow(replica2, gr), mysql.GroupMemberStateOffline, "", false, nil)
				unreachable.LastCheckValid = 0
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil),
					unreachable,
				}
			},
			notWant: []AnalysisCode{GroupNotBootstrapped, ClusterHasNoPrimary},
		},
		{
			name: "a tablet taking a backup is an active member: nothing is bootstrapped",
			rows: func() []*test.InfoForRecoveryAnalysis {
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil),
					member(grRow(backup, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, backup),
				}
			},
			notWant: []AnalysisCode{GroupNotBootstrapped},
		},
		{
			name: "no member has quorum",
			rows: func() []*test.InfoForRecoveryAnalysis {
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(primary, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, false, nil),
					member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, false, nil),
				}
			},
			want: map[string]AnalysisCode{
				"zone1-0000000101": GroupQuorumLost,
				"zone1-0000000100": GroupQuorumLost,
			},
		},
		{
			name: "one cell holds a majority of the online members with the cross-cell policy",
			rows: func() []*test.InfoForRecoveryAnalysis {
				crossCell := policy.DurabilityGroupReplicationCrossCell
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(primary, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary),
					member(grRow(replica, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
					member(grRow(crossCellReplica, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
				}
			},
			want: map[string]AnalysisCode{"zone1-0000000101": GroupCellMajority},
		},
		{
			name: "one cell holds a majority of the online members without the cross-cell policy",
			rows: func() []*test.InfoForRecoveryAnalysis {
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(primary, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary),
					member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
					member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
				}
			},
			want: map[string]AnalysisCode{},
		},
		{
			name: "active members of a shard with a semi-sync policy get no asynchronous replication analysis",
			rows: func() []*test.InfoForRecoveryAnalysis {
				groupPrimary := member(grRow(primary, policy.DurabilitySemiSync), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary)
				// The group supersedes semi-sync, so the tablet disabled it.
				groupPrimary.CountValidSemiSyncReplicatingReplicas = 1
				secondary := member(grRow(replica, policy.DurabilitySemiSync), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary)
				secondary.HeartbeatInterval = 1
				return []*test.InfoForRecoveryAnalysis{groupPrimary, secondary}
			},
			want: map[string]AnalysisCode{},
		},
		{
			name: "semi-sync enabled on an active group primary with the group replication policy",
			rows: func() []*test.InfoForRecoveryAnalysis {
				groupPrimary := member(grRow(primary, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary)
				groupPrimary.SemiSyncPrimaryEnabled = 1
				secondary := member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary)
				secondary.SemiSyncReplicaEnabled = 1
				return []*test.InfoForRecoveryAnalysis{groupPrimary, secondary}
			},
			want: map[string]AnalysisCode{},
		},
		{
			name: "writable group secondary",
			rows: func() []*test.InfoForRecoveryAnalysis {
				secondary := member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary)
				secondary.ReadOnly = 0
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(primary, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary),
					secondary,
				}
			},
			want: map[string]AnalysisCode{"zone1-0000000100": ReplicaIsWritable},
		},
		{
			name: "unreachable primary whose members replicate through the group is not dead",
			rows: func() []*test.InfoForRecoveryAnalysis {
				invalidPrimary := grRow(primary, gr)
				invalidPrimary.IsInvalid = 1
				invalidPrimary.LastCheckValid = 0
				return []*test.InfoForRecoveryAnalysis{
					invalidPrimary,
					member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
					member(grRow(replica2, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
				}
			},
			want: map[string]AnalysisCode{"zone1-0000000101": InvalidPrimary},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GroupReplicationConditions.Reset()
			got := analysisCodes(runAnalysis(t, tt.rows()))
			if tt.want != nil {
				assert.Equal(t, tt.want, got)
			}
			for _, code := range tt.notWant {
				assert.NotContains(t, got, code)
				for alias, gotCode := range got {
					assert.NotEqual(t, code, gotCode, "tablet %s", alias)
				}
			}
		})
	}
}

// TestGetDetectionAnalysisGroupReplicationShardState verifies the shard-wide Group Replication
// state that the recoveries use to decide whether to wait for the group.
func TestGetDetectionAnalysisGroupReplicationShardState(t *testing.T) {
	resetPrimaryHealthState()
	t.Cleanup(GroupReplicationConditions.Reset)

	primary := grTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	replica := grTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	replica2 := grTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	gr := policy.DurabilityGroupReplication

	deadPrimary := member(grRow(primary, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary)
	deadPrimary.LastCheckValid = 0
	analyses := runAnalysis(t, []*test.InfoForRecoveryAnalysis{
		deadPrimary,
		// The members still see the unreachable primary as their primary.
		member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
		member(grRow(replica2, gr), mysql.GroupMemberStateRecovering, "", true, primary),
	})
	require.Len(t, analyses, 1)
	a := analyses[0]
	assert.Equal(t, DeadPrimaryWithoutReplicas, a.Analysis)
	assert.Equal(t, serverUUID(primary), a.AnalyzedServerUUID)
	assert.EqualValues(t, 2, a.ShardGroupActiveMembers)
	assert.EqualValues(t, 2, a.ShardGroupQuorumMembers)
	assert.Equal(t, serverUUID(primary), a.ShardGroupPrimaryUUID)
	assert.Nil(t, a.ShardGroupPrimaryAlias, "the unreachable primary is not a reachable group primary")
	assert.EqualValues(t, 3, a.ShardGroupVotingMembers)
	assert.EqualValues(t, 1, a.ShardGroupUnreachableVotingMembers)
}

func TestMatchGroupPrimaryNotInTopoGracePeriod(t *testing.T) {
	t.Cleanup(GroupReplicationConditions.Reset)
	GroupReplicationConditions.Reset()

	a := &DetectionAnalysis{
		AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		TabletType:            topodatapb.TabletType_REPLICA,
		CurrentTabletType:     topodatapb.TabletType_REPLICA,
		LastCheckValid:        true,
		IsGroupMemberActive:   true,
		IsGroupPrimary:        true,
	}
	durability, err := policy.GetDurabilityPolicy(policy.DurabilityGroupReplication)
	require.NoError(t, err)
	ca := &clusterAnalysis{durability: durability}
	start := time.Now()
	assert.False(t, matchGroupPrimaryNotInTopo(a, ca, start), "the tablet gets time to promote itself")
	assert.False(t, matchGroupPrimaryNotInTopo(a, ca, start.Add(time.Second)))
	assert.True(t, matchGroupPrimaryNotInTopo(a, ca, start.Add(groupPrimaryNotInTopoGracePeriod)))

	// The tablet already runs as PRIMARY; only VTOrc's copy of its record is behind.
	a.CurrentTabletType = topodatapb.TabletType_PRIMARY
	assert.False(t, matchGroupPrimaryNotInTopo(a, ca, start.Add(groupPrimaryNotInTopoGracePeriod)))
}

func TestConditionTracker(t *testing.T) {
	ct := NewConditionTracker(10 * time.Second)
	start := time.Now()
	assert.Equal(t, time.Duration(0), ct.Observe("a", start))
	assert.Equal(t, 5*time.Second, ct.Observe("a", start.Add(5*time.Second)))
	assert.Equal(t, time.Duration(0), ct.Observe("b", start.Add(5*time.Second)), "conditions are tracked separately")
	assert.Equal(t, 15*time.Second, ct.Observe("a", start.Add(15*time.Second)))
	// "a" is not observed for longer than the forget period: a new period starts.
	assert.Equal(t, time.Duration(0), ct.Observe("a", start.Add(26*time.Second)))
	ct.Reset()
	assert.Equal(t, time.Duration(0), ct.Observe("a", start.Add(27*time.Second)))
}

// TestDetectErrantGTIDsGroupReplication verifies that the transactions of a replication group,
// which carry the group name as their UUID on every member, are not reported as errant when a
// member or an asynchronous replica of the group has applied more of them than the stale state
// of the shard primary shows.
func TestDetectErrantGTIDsGroupReplication(t *testing.T) {
	const (
		groupName   = "6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41"
		oldPrimary  = "230ea8ea-81e3-11e4-972a-e25ec4bd140a"
		primaryUUID = "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9"
		replicaUUID = "316d193c-70e5-11e5-adb2-ecf4bb2262ff"
	)
	primaryTablet := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone-1", Uid: 101},
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_PRIMARY,
		MysqlHostname: "primary-host",
		MysqlPort:     6714,
	}
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone-1", Uid: 100},
		Keyspace: "ks",
		Shard:    "0",
	}
	groupMember := func(i *Instance) *Instance {
		i.GroupReplicationPluginActive = true
		i.GroupName = groupName
		i.GroupMemberState = mysql.GroupMemberStateOnline
		return i
	}

	tests := []struct {
		name           string
		instance       *Instance
		primaryMember  bool
		wantErrantGTID string
	}{
		{
			name: "group member ahead of the primary's last known state",
			instance: groupMember(&Instance{
				ServerUUID:      replicaUUID,
				AncestryUUID:    replicaUUID,
				ExecutedGtidSet: oldPrimary + ":1-50," + groupName + ":1-120",
			}),
			primaryMember: true,
		},
		{
			name: "group member with its own errant transaction",
			instance: groupMember(&Instance{
				ServerUUID:      replicaUUID,
				AncestryUUID:    replicaUUID,
				ExecutedGtidSet: oldPrimary + ":1-50," + groupName + ":1-120," + replicaUUID + ":1-2",
			}),
			primaryMember:  true,
			wantErrantGTID: replicaUUID + ":1-2",
		},
		{
			name: "group member with an errant transaction of an old primary",
			instance: groupMember(&Instance{
				ServerUUID:      replicaUUID,
				AncestryUUID:    replicaUUID,
				ExecutedGtidSet: oldPrimary + ":1-51," + groupName + ":1-120",
			}),
			primaryMember:  true,
			wantErrantGTID: oldPrimary + ":51",
		},
		{
			name: "asynchronous replica of the group primary ahead of the primary's last known state",
			instance: &Instance{
				ServerUUID:             replicaUUID,
				SourceHost:             primaryTablet.MysqlHostname,
				SourcePort:             int(primaryTablet.MysqlPort),
				SourceUUID:             primaryUUID,
				AncestryUUID:           primaryUUID + "," + replicaUUID,
				ExecutedGtidSet:        oldPrimary + ":1-50," + groupName + ":1-120",
				primaryExecutedGtidSet: oldPrimary + ":1-50," + groupName + ":1-100",
			},
			primaryMember: true,
		},
		{
			name: "replica ahead of a primary that is not a group member",
			instance: &Instance{
				ServerUUID:      replicaUUID,
				AncestryUUID:    replicaUUID,
				ExecutedGtidSet: oldPrimary + ":1-50," + groupName + ":1-120",
			},
			wantErrantGTID: groupName + ":101-120",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db.ClearVTOrcDatabase()
			t.Cleanup(db.ClearVTOrcDatabase)
			require.NoError(t, SaveShard(topo.NewShardInfo("ks", "0", &topodatapb.Shard{PrimaryAlias: primaryTablet.Alias}, nil)))
			require.NoError(t, SaveTablet(primaryTablet))
			primaryInstance := &Instance{
				InstanceAlias:   primaryTablet.Alias,
				Hostname:        primaryTablet.MysqlHostname,
				Port:            int(primaryTablet.MysqlPort),
				ServerUUID:      primaryUUID,
				ExecutedGtidSet: oldPrimary + ":1-50," + groupName + ":1-100",
			}
			if tt.primaryMember {
				groupMember(primaryInstance)
				primaryInstance.GroupMemberRole = mysql.GroupMemberRolePrimary
			}
			require.NoError(t, WriteInstance(primaryInstance, true, nil))

			tt.instance.InstanceAlias = tablet.Alias
			require.NoError(t, detectErrantGTIDs(tt.instance, tablet))
			assert.Equal(t, tt.wantErrantGTID, tt.instance.GtidErrant)
		})
	}
}

// TestReadTopologyInstanceGroupReplication verifies that discovery stores the Group Replication
// state that a tablet reports in its FullStatus.
func TestReadTopologyInstanceGroupReplication(t *testing.T) {
	_, err := db.OpenVTOrc()
	require.NoError(t, err)
	db.ClearVTOrcDatabase()
	t.Cleanup(db.ClearVTOrcDatabase)

	oldTmc := tmc
	t.Cleanup(func() { tmc = oldTmc })

	tablet := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname:      "localhost",
		MysqlHostname: "localhost",
		MysqlPort:     6100,
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_REPLICA,
	}
	require.NoError(t, SaveTablet(tablet))
	require.NoError(t, SaveShard(topo.NewShardInfo("ks", "0", &topodatapb.Shard{}, nil)))

	const groupName = "6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41"
	tmc = &testutil.TabletManagerClient{
		FullStatusResult: &replicationdatapb.FullStatus{
			TabletType: topodatapb.TabletType_REPLICA,
			ServerUuid: "00000000-0000-0000-0000-000000000100",
			GtidMode:   "ON",
			GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
				PluginActive: true,
				GroupName:    groupName,
				MemberState:  mysql.GroupMemberStateOnline,
				MemberRole:   mysql.GroupMemberRoleSecondary,
				PrimaryUuid:  "00000000-0000-0000-0000-000000000101",
				HasQuorum:    true,
				Members: []*replicationdatapb.GroupReplicationMember{
					{MemberUuid: "00000000-0000-0000-0000-000000000100", State: mysql.GroupMemberStateOnline, Role: mysql.GroupMemberRoleSecondary},
					{MemberUuid: "00000000-0000-0000-0000-000000000101", State: mysql.GroupMemberStateOnline, Role: mysql.GroupMemberRolePrimary},
					{MemberUuid: "00000000-0000-0000-0000-000000000102", State: mysql.GroupMemberStateRecovering, Role: mysql.GroupMemberRoleSecondary},
				},
			},
		},
	}

	latency := stopwatch.NewNamedStopwatch()
	require.NoError(t, latency.AddMany([]string{"backend", "instance", "total"}))
	_, err = ReadTopologyInstanceBufferable(tablet.Alias, latency)
	require.NoError(t, err)

	instance, found, err := ReadInstance(tablet.Alias)
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, instance.GroupReplicationPluginActive)
	assert.Equal(t, groupName, instance.GroupName)
	assert.Equal(t, mysql.GroupMemberStateOnline, instance.GroupMemberState)
	assert.Equal(t, mysql.GroupMemberRoleSecondary, instance.GroupMemberRole)
	assert.Equal(t, "00000000-0000-0000-0000-000000000101", instance.GroupPrimaryUUID)
	assert.True(t, instance.GroupHasQuorum)
	assert.EqualValues(t, 2, instance.GroupOnlineMembers)
	assert.EqualValues(t, 3, instance.GroupViewMembers)
	assert.True(t, instance.IsGroupMemberActive())
	assert.False(t, instance.IsGroupPrimary())
}
