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
	"fmt"
	"slices"
	"strings"
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
	"vitess.io/vitess/go/vt/vtorc/config"
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

// sees makes the row's MySQL see the MySQL of the given tablets as active members of its group.
func sees(row *test.InfoForRecoveryAnalysis, tablets ...*topodatapb.Tablet) *test.InfoForRecoveryAnalysis {
	var uuids []string
	for _, tablet := range tablets {
		uuids = append(uuids, serverUUID(tablet))
	}
	row.GroupActiveMemberUUIDs = strings.Join(uuids, ",")
	row.GroupOnlineMemberUUIDs = row.GroupActiveMemberUUIDs
	return row
}

// voterList formats the voters of a shard as they are stored in the database.
func voterList(tablets ...*topodatapb.Tablet) string {
	var aliases []*topodatapb.TabletAlias
	for _, tablet := range tablets {
		aliases = append(aliases, tablet.Alias)
	}
	slices.SortFunc(aliases, func(a, b *topodatapb.TabletAlias) int {
		return strings.Compare(topoproto.TabletAliasString(a), topoproto.TabletAliasString(b))
	})
	return formatGroupReplicationVoters(aliases)
}

func runAnalysis(t *testing.T, rows []*test.InfoForRecoveryAnalysis) []*DetectionAnalysis {
	oldDB := db.Db
	t.Cleanup(func() { db.Db = oldDB })
	// A member whose view the test does not set (sees) sees every ONLINE member of the rows.
	var online []string
	for _, row := range rows {
		if row.GroupPluginActive == 1 && row.GroupMemberState == mysql.GroupMemberStateOnline {
			online = append(online, row.ServerUUID)
		}
	}
	var rowMaps []sqlutils.RowMap
	for _, row := range rows {
		if row.GroupPluginActive == 1 && row.GroupActiveMemberUUIDs == "" && row.GroupOnlineMemberUUIDs == "" &&
			(row.GroupMemberState == mysql.GroupMemberStateOnline || row.GroupMemberState == mysql.GroupMemberStateRecovering) {
			row.GroupOnlineMemberUUIDs = strings.Join(online, ",")
		}
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
	oldVoterGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		groupPrimaryNotInTopoGracePeriod = oldGrace
		config.SetGroupReplicationVoterReplacementGracePeriod(oldVoterGrace)
		GroupReplicationConditions.Reset()
		UnreachableGroupTablets.Reset()
	})

	primary := grTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	replica := grTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	replica2 := grTablet("zone1", 102, topodatapb.TabletType_REPLICA)
	crossCellReplica := grTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	crossCellReplica2 := grTablet("zone2", 201, topodatapb.TabletType_REPLICA)
	rdonly := grTablet("zone1", 102, topodatapb.TabletType_RDONLY)
	backup := grTablet("zone1", 102, topodatapb.TabletType_BACKUP)
	gr := policy.DurabilityGroupReplication
	crossCell := policy.DurabilityGroupReplicationCrossCell

	tests := []struct {
		name string
		rows func() []*test.InfoForRecoveryAnalysis
		// voters are the voters recorded in the shard record.
		voters []*topodatapb.Tablet
		// voterGracePeriod is --group-replication-voter-replacement-grace-period; 0 means 1h.
		voterGracePeriod time.Duration
		// want is the analysis of every tablet that has a problem.
		want map[string]AnalysisCode
		// wantDesiredVoters are the voters that the policy selects, if they are computed.
		wantDesiredVoters []*topodatapb.Tablet
		// alsoMatched are problems that a tablet has besides the reported one.
		alsoMatched map[string]AnalysisCode
		// notWant are analyses that must not be reported, for cases where the other analyses
		// do not matter.
		notWant []AnalysisCode
	}{
		{
			name:   "group elected a replica: its tablet is promoted, the demoted primary is left alone",
			voters: []*topodatapb.Tablet{primary, replica},
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
			name:   "dead primary: the group primary's tablet is promoted before any failover",
			voters: []*topodatapb.Tablet{primary, replica, replica2},
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
			name:   "voting member is offline while the group is active; the async replica keeps its analysis",
			voters: []*topodatapb.Tablet{primary, replica},
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
			name:   "no member is active: the group is bootstrapped instead of electing a primary",
			voters: []*topodatapb.Tablet{replica, replica2},
			rows: func() []*test.InfoForRecoveryAnalysis {
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil),
					grRow(replica2, gr),
				}
			},
			want: map[string]AnalysisCode{"zone1-0000000100": GroupNotBootstrapped},
		},
		{
			name:   "no member is active but a voting member is unreachable: nothing is bootstrapped",
			voters: []*topodatapb.Tablet{replica, replica2},
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
			name:   "a tablet taking a backup is an active member: nothing is bootstrapped",
			voters: []*topodatapb.Tablet{replica, backup},
			rows: func() []*test.InfoForRecoveryAnalysis {
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil),
					member(grRow(backup, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, backup),
				}
			},
			notWant: []AnalysisCode{GroupNotBootstrapped},
		},
		{
			name:   "no member has quorum",
			voters: []*topodatapb.Tablet{primary, replica},
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
			name: "one cell holds a majority of the online members with the cross-cell policy: the extra member loses its seat",
			rows: func() []*test.InfoForRecoveryAnalysis {
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(primary, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary),
					member(grRow(replica, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
					member(grRow(crossCellReplica, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
				}
			},
			voters:            []*topodatapb.Tablet{primary, replica, crossCellReplica},
			want:              map[string]AnalysisCode{"zone1-0000000101": GroupVotersOutOfDate},
			wantDesiredVoters: []*topodatapb.Tablet{primary, crossCellReplica},
			alsoMatched:       map[string]AnalysisCode{"zone1-0000000101": GroupCellMajority},
		},
		{
			name: "an active member that is not a voter must leave the group",
			rows: func() []*test.InfoForRecoveryAnalysis {
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(primary, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary),
					member(grRow(replica, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
					member(grRow(crossCellReplica, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
				}
			},
			voters:            []*topodatapb.Tablet{primary, crossCellReplica},
			want:              map[string]AnalysisCode{"zone1-0000000101": GroupVotersOutOfDate},
			wantDesiredVoters: []*topodatapb.Tablet{primary, crossCellReplica},
		},
		{
			name: "a replica that is not a voter is an asynchronous replica and keeps its analyses",
			rows: func() []*test.InfoForRecoveryAnalysis {
				asyncReplica := grRow(replica, crossCell)
				asyncReplica.IsPrimary = 0
				asyncReplica.PrimaryTabletInfo = primary
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(primary, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary),
					asyncReplica,
					member(grRow(crossCellReplica, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
				}
			},
			voters: []*topodatapb.Tablet{primary, crossCellReplica},
			want:   map[string]AnalysisCode{"zone1-0000000100": ReplicationStopped},
		},
		{
			name: "a voter failed for longer than the grace period: a tablet of its cell replaces it",
			rows: func() []*test.InfoForRecoveryAnalysis {
				failed := grRow(crossCellReplica, crossCell)
				failed.LastCheckValid = 0
				failed.IsPrimary = 0
				failed.ReplicationStopped = 0
				failed.PrimaryTabletInfo = primary
				asyncReplica := grRow(crossCellReplica2, crossCell)
				asyncReplica.IsPrimary = 0
				asyncReplica.ReplicationStopped = 0
				asyncReplica.PrimaryTabletInfo = primary
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(primary, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary),
					failed,
					asyncReplica,
				}
			},
			voters:            []*topodatapb.Tablet{primary, crossCellReplica},
			voterGracePeriod:  -1,
			want:              map[string]AnalysisCode{"zone1-0000000101": GroupVotersOutOfDate},
			wantDesiredVoters: []*topodatapb.Tablet{primary, crossCellReplica2},
		},
		{
			name: "a voter unreachable within the grace period keeps its seat",
			rows: func() []*test.InfoForRecoveryAnalysis {
				failed := grRow(crossCellReplica, crossCell)
				failed.LastCheckValid = 0
				failed.IsPrimary = 0
				failed.ReplicationStopped = 0
				failed.PrimaryTabletInfo = primary
				asyncReplica := grRow(crossCellReplica2, crossCell)
				asyncReplica.IsPrimary = 0
				asyncReplica.ReplicationStopped = 0
				asyncReplica.PrimaryTabletInfo = primary
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(primary, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary),
					failed,
					asyncReplica,
				}
			},
			voters:  []*topodatapb.Tablet{primary, crossCellReplica},
			notWant: []AnalysisCode{GroupVotersOutOfDate},
		},
		{
			name: "the group primary's view lacks the voter majority: the voters it lacks keep their seats",
			rows: func() []*test.InfoForRecoveryAnalysis {
				// The two other voters left the view cleanly, which kept MySQL's view quorum, and
				// have been unreachable for longer than the grace period.
				var rows []*test.InfoForRecoveryAnalysis
				for _, tablet := range []*topodatapb.Tablet{replica, replica2} {
					failed := grRow(tablet, gr)
					failed.LastCheckValid = 0
					failed.IsPrimary = 0
					failed.ReplicationStopped = 0
					failed.PrimaryTabletInfo = primary
					rows = append(rows, failed)
				}
				return append(rows, sees(member(grRow(primary, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary), primary))
			},
			voters:           []*topodatapb.Tablet{primary, replica, replica2},
			voterGracePeriod: -1,
			// The primary's view lacks the voter majority, so it is no legitimate group primary
			// (ClusterHasNoPrimary, which hides any GroupVotersOutOfDate): the selection itself must
			// keep the three voters, where the policy alone would select the primary only.
			notWant:           []AnalysisCode{GroupVotersOutOfDate},
			wantDesiredVoters: []*topodatapb.Tablet{replica, primary, replica2},
		},
		{
			name: "an unreachable voter whose MySQL is still an active member keeps its seat",
			rows: func() []*test.InfoForRecoveryAnalysis {
				// Its vttablet is down, but the group primary still sees its MySQL ONLINE.
				unreachable := member(grRow(crossCellReplica, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary)
				unreachable.LastCheckValid = 0
				asyncReplica := grRow(crossCellReplica2, crossCell)
				asyncReplica.IsPrimary = 0
				asyncReplica.ReplicationStopped = 0
				asyncReplica.PrimaryTabletInfo = primary
				return []*test.InfoForRecoveryAnalysis{
					sees(member(grRow(primary, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary), primary, crossCellReplica),
					unreachable,
					asyncReplica,
				}
			},
			voters:           []*topodatapb.Tablet{primary, crossCellReplica},
			voterGracePeriod: -1,
			notWant:          []AnalysisCode{GroupVotersOutOfDate},
		},
		{
			name: "no voter is selected in an active group: they are selected",
			rows: func() []*test.InfoForRecoveryAnalysis {
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(primary, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary),
					member(grRow(replica, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
					member(grRow(crossCellReplica, crossCell), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
				}
			},
			want:              map[string]AnalysisCode{"zone1-0000000101": GroupVotersOutOfDate},
			wantDesiredVoters: []*topodatapb.Tablet{primary, crossCellReplica},
		},
		{
			name: "no voter is selected and no member is active: they are selected before the group is bootstrapped",
			rows: func() []*test.InfoForRecoveryAnalysis {
				return []*test.InfoForRecoveryAnalysis{
					member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil),
					grRow(replica2, gr),
				}
			},
			want: map[string]AnalysisCode{
				"zone1-0000000100": GroupVotersOutOfDate,
				"zone1-0000000102": NotConnectedToPrimary,
			},
			wantDesiredVoters: []*topodatapb.Tablet{replica, replica2},
			notWant:           []AnalysisCode{GroupNotBootstrapped},
		},
		{
			name:   "one cell holds a majority of the online members without the cross-cell policy",
			voters: []*topodatapb.Tablet{primary, replica, crossCellReplica},
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
			name:   "semi-sync enabled on an active group primary with the group replication policy",
			voters: []*topodatapb.Tablet{primary, replica},
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
			name:   "writable group secondary",
			voters: []*topodatapb.Tablet{primary, replica},
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
			name:   "unreachable primary whose members replicate through the group is not dead",
			voters: []*topodatapb.Tablet{primary, replica, replica2},
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
			UnreachableGroupTablets.Reset()
			switch tt.voterGracePeriod {
			case 0:
				config.SetGroupReplicationVoterReplacementGracePeriod(time.Hour)
			case -1:
				config.SetGroupReplicationVoterReplacementGracePeriod(0)
			default:
				config.SetGroupReplicationVoterReplacementGracePeriod(tt.voterGracePeriod)
			}
			rows := tt.rows()
			for _, row := range rows {
				row.ShardGroupReplicationVoters = voterList(tt.voters...)
			}
			analyses := runAnalysis(t, rows)
			got := analysisCodes(analyses)
			if tt.want != nil {
				assert.Equal(t, tt.want, got)
			}
			if tt.wantDesiredVoters != nil {
				require.NotEmpty(t, analyses)
				assert.Equal(t, voterList(tt.wantDesiredVoters...), formatGroupReplicationVoters(analyses[0].ShardGroupDesiredVoters))
			}
			for alias, code := range tt.alsoMatched {
				var matched []AnalysisCode
				for _, a := range analyses {
					if topoproto.TabletAliasString(a.AnalyzedInstanceAlias) == alias {
						for _, problem := range a.AnalysisMatchedProblems {
							matched = append(matched, problem.Analysis)
						}
					}
				}
				assert.Contains(t, matched, code, "tablet %s", alias)
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

// TestGetDetectionAnalysisGroupReplicationLegitimateGroup checks that VTOrc only follows the
// shard's legitimate group: a member alone in a group incarnation that the shard record does not
// list (S7d of the Group Replication failover audit), or a group view without a majority of the
// shard's voters, is not a group primary to promote, and a voter only joins a group whose members
// are in the recorded incarnation.
func TestGetDetectionAnalysisGroupReplicationLegitimateGroup(t *testing.T) {
	resetPrimaryHealthState()
	oldGrace := groupPrimaryNotInTopoGracePeriod
	groupPrimaryNotInTopoGracePeriod = 0
	oldVoterGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	config.SetGroupReplicationVoterReplacementGracePeriod(time.Hour)
	t.Cleanup(func() {
		groupPrimaryNotInTopoGracePeriod = oldGrace
		config.SetGroupReplicationVoterReplacementGracePeriod(oldVoterGrace)
		GroupReplicationConditions.Reset()
		UnreachableGroupTablets.Reset()
	})

	primary := grTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	replica := grTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	crossCellReplica := grTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	gr := policy.DurabilityGroupReplication
	const recorded = "1790785744160779"

	tests := []struct {
		name string
		rows func() []*test.InfoForRecoveryAnalysis
		want map[string]AnalysisCode
		// notWant are analyses that must not be reported, when the others do not matter.
		notWant []AnalysisCode
	}{{
		name: "member alone in a new incarnation is not promoted",
		rows: func() []*test.InfoForRecoveryAnalysis {
			// The old primary's tablet demoted itself and its MySQL left the group.
			oldPrimary := member(grRow(primary, gr), mysql.GroupMemberStateOffline, "", false, nil)
			oldPrimary.CurrentTabletType = int(topodatapb.TabletType_REPLICA)
			alone := sees(member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, crossCellReplica), crossCellReplica)
			alone.GroupViewID = "17907858161940982:1"
			alone.ReadOnly = 0
			return []*test.InfoForRecoveryAnalysis{oldPrimary, member(grRow(replica, gr), mysql.GroupMemberStateError, "", false, nil), alone}
		},
		// Nothing to join, nothing to promote: the voters do not start a join into a foreign group.
		notWant: []AnalysisCode{GroupPrimaryNotInTopo, GroupMemberNotOnline, GroupNotBootstrapped},
	}, {
		name: "group primary without a majority of the voters is not promoted",
		rows: func() []*test.InfoForRecoveryAnalysis {
			oldPrimary := member(grRow(primary, gr), mysql.GroupMemberStateOffline, "", false, nil)
			oldPrimary.CurrentTabletType = int(topodatapb.TabletType_REPLICA)
			alone := sees(member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, crossCellReplica), crossCellReplica)
			alone.GroupViewID = recorded + ":9"
			alone.ReadOnly = 0
			return []*test.InfoForRecoveryAnalysis{oldPrimary, member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil), alone}
		},
		// The other voters may join the group of the recorded incarnation to restore its majority. VTOrc
		// does not join the old primary, whose record still says PRIMARY: its tablet runs as a REPLICA
		// and rejoins on its own. Nor does it undo its demotion: its MySQL is not the group's primary.
		want: map[string]AnalysisCode{"zone1-0000000100": GroupMemberNotOnline},
	}, {
		name: "group primary with a majority of the voters in the recorded incarnation is promoted",
		rows: func() []*test.InfoForRecoveryAnalysis {
			oldPrimary := member(grRow(primary, gr), mysql.GroupMemberStateOffline, "", false, nil)
			oldPrimary.CurrentTabletType = int(topodatapb.TabletType_REPLICA)
			newPrimary := sees(member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, crossCellReplica), crossCellReplica, replica)
			newPrimary.GroupViewID = recorded + ":10"
			newPrimary.ReadOnly = 0
			secondary := sees(member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, crossCellReplica), crossCellReplica, replica)
			secondary.GroupViewID = recorded + ":10"
			return []*test.InfoForRecoveryAnalysis{oldPrimary, secondary, newPrimary}
		},
		want: map[string]AnalysisCode{
			"zone2-0000000200": GroupPrimaryNotInTopo,
		},
	}, {
		// S7d after the fixes: VTOrc bootstrapped the group again on zone2, whose view holds one
		// of the three voters, and no tablet is PRIMARY. A reparent cannot follow a primary that
		// lacks the majority of the voters; the missing voters must rejoin first, and the shard-wide
		// failover analyses must not starve their GroupMemberNotOnline.
		name: "no primary tablet and a group without the majority of its voters: the voters rejoin",
		rows: func() []*test.InfoForRecoveryAnalysis {
			formerPrimary := grRow(grTablet("zone1", 101, topodatapb.TabletType_REPLICA), gr)
			formerPrimary.GroupPluginActive = 1
			formerPrimary.GroupMemberState = mysql.GroupMemberStateOffline
			offline := member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil)
			alone := sees(member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, crossCellReplica), crossCellReplica)
			alone.GroupViewID = recorded + ":1"
			alone.ReadOnly = 0
			rows := []*test.InfoForRecoveryAnalysis{formerPrimary, offline, alone}
			for _, row := range rows {
				row.ShardPrimaryTermTimestamp = "2026-09-30 18:10:00.000000 +0000 UTC"
			}
			return rows
		},
		want: map[string]AnalysisCode{
			"zone1-0000000100": GroupMemberNotOnline,
			"zone1-0000000101": GroupMemberNotOnline,
			"zone2-0000000200": PrimaryTabletDeleted,
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GroupReplicationConditions.Reset()
			UnreachableGroupTablets.Reset()
			rows := tt.rows()
			for _, row := range rows {
				row.ShardGroupReplicationVoters = voterList(primary, replica, crossCellReplica)
				row.ShardGroupReplicationIncarnation = recorded
			}
			got := analysisCodes(runAnalysis(t, rows))
			if tt.want != nil {
				assert.Equal(t, tt.want, got)
			}
			for alias, code := range got {
				assert.NotContains(t, tt.notWant, code, "tablet %s", alias)
			}
		})
	}
}

// TestGetDetectionAnalysisGroupBootstrapNotRecorded checks that VTOrc detects a bootstrap whose reply
// was lost (S7d chaos scenario, run r3): the target of the shard's bootstrap intent is the primary of
// a group of another incarnation than the shard record lists. A member alone in a new incarnation
// that is not the intent's target, such as a stray group that a failed join formed, is not adopted.
func TestGetDetectionAnalysisGroupBootstrapNotRecorded(t *testing.T) {
	resetPrimaryHealthState()
	oldVoterGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	config.SetGroupReplicationVoterReplacementGracePeriod(time.Hour)
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(oldVoterGrace)
		GroupReplicationConditions.Reset()
		UnreachableGroupTablets.Reset()
	})

	oldPrimary := grTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	replica := grTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	crossCellReplica := grTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	gr := policy.DurabilityGroupReplication
	const recorded = "1790785744160779"

	// rows returns the shard after the bootstrap on target: its MySQL is alone in a new
	// incarnation, and the other voters' MySQL is OFFLINE.
	rows := func(target *topodatapb.Tablet) []*test.InfoForRecoveryAnalysis {
		var result []*test.InfoForRecoveryAnalysis
		for _, tablet := range []*topodatapb.Tablet{oldPrimary, replica, crossCellReplica} {
			row := member(grRow(tablet, gr), mysql.GroupMemberStateOffline, "", false, nil)
			if tablet == target {
				row = sees(member(grRow(tablet, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, tablet), tablet)
				row.GroupViewID = "17908892198863259:1"
				row.ReadOnly = 0
			}
			row.CurrentTabletType = int(topodatapb.TabletType_REPLICA)
			result = append(result, row)
		}
		return result
	}
	tests := []struct {
		name string
		// target is the tablet whose MySQL is alone in a new incarnation.
		target *topodatapb.Tablet
		// intent is the target of the shard's bootstrap intent, nil for none.
		intent *topodatapb.Tablet
		want   bool
	}{
		{name: "the intent's target bootstrapped a group that is not recorded", target: crossCellReplica, intent: crossCellReplica, want: true},
		{name: "no intent", target: crossCellReplica},
		{name: "a member that is not the intent's target is alone in a new incarnation", target: replica, intent: crossCellReplica},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GroupReplicationConditions.Reset()
			UnreachableGroupTablets.Reset()
			rows := rows(tt.target)
			for _, row := range rows {
				row.ShardGroupReplicationVoters = voterList(oldPrimary, replica, crossCellReplica)
				row.ShardGroupReplicationIncarnation = recorded
				if tt.intent != nil {
					row.ShardGroupReplicationBootstrapTarget = topoproto.TabletAliasString(tt.intent.Alias)
				}
			}
			got := analysisCodes(runAnalysis(t, rows))
			targetAlias := topoproto.TabletAliasString(tt.target.Alias)
			if tt.want {
				assert.Equal(t, GroupBootstrapNotRecorded, got[targetAlias])
			} else {
				for alias, code := range got {
					assert.NotEqual(t, GroupBootstrapNotRecorded, code, "tablet %s", alias)
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
	rows := []*test.InfoForRecoveryAnalysis{
		deadPrimary,
		// The members still see the unreachable primary as their primary.
		member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
		member(grRow(replica2, gr), mysql.GroupMemberStateRecovering, "", true, primary),
	}
	for _, row := range rows {
		row.ShardGroupReplicationVoters = voterList(primary, replica, replica2)
	}
	analyses := runAnalysis(t, rows)
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
	assert.Equal(t, voterList(primary, replica, replica2), formatGroupReplicationVoters(a.ShardGroupVoters))
	assert.True(t, a.IsGroupVoter)
}

func TestMatchGroupPrimaryNotInTopoGracePeriod(t *testing.T) {
	t.Cleanup(GroupReplicationConditions.Reset)
	GroupReplicationConditions.Reset()

	a := &DetectionAnalysis{
		AnalyzedInstanceAlias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		TabletType:               topodatapb.TabletType_REPLICA,
		CurrentTabletType:        topodatapb.TabletType_REPLICA,
		LastCheckValid:           true,
		IsGroupMemberActive:      true,
		IsGroupPrimary:           true,
		IsLegitimateGroupPrimary: true,
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
					{MemberUuid: "00000000-0000-0000-0000-000000000103", State: mysql.GroupMemberStateUnreachable, Role: mysql.GroupMemberRoleSecondary},
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
	assert.EqualValues(t, 4, instance.GroupViewMembers)
	assert.Equal(t, []string{
		"00000000-0000-0000-0000-000000000100",
		"00000000-0000-0000-0000-000000000101",
		"00000000-0000-0000-0000-000000000102",
	}, instance.GroupActiveMemberUUIDs, "an UNREACHABLE member is not active")
	assert.True(t, instance.IsGroupMemberActive())
	assert.False(t, instance.IsGroupPrimary())
}

// TestGetDetectionAnalysisGroupReplicationJoinTargets checks which voters VTOrc joins to their group,
// and that it does not bootstrap a group while a START GROUP_REPLICATION is in progress:
//   - A PRIMARY tablet, in the topology or in its own state, whose MySQL is out of its group is never
//     joined: the tablet demotes itself first (a stale primary), and a join on it would hold its action
//     lock for up to a minute meanwhile.
//   - A voter whose START is still in progress is not joined again: MySQL refuses another START, and a
//     STOP, until it ends.
//   - While any voter's START is in progress, the group is not bootstrapped, for at most
//     groupStartInProgressGrace: that member reports OFFLINE for up to about a minute and can still end
//     in a group of its own (sro-eval case A), which stays read-only. Nor does the shard get a
//     failover meanwhile.
func TestGetDetectionAnalysisGroupReplicationJoinTargets(t *testing.T) {
	resetPrimaryHealthState()
	// Every tablet is reachable: the voter replacement grace period does not matter.
	t.Cleanup(func() {
		GroupReplicationConditions.Reset()
		UnreachableGroupTablets.Reset()
	})

	stalePrimary := grTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	replica := grTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	groupPrimaryTablet := grTablet("zone1", 100, topodatapb.TabletType_PRIMARY)
	crossCellReplica := grTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	gr := policy.DurabilityGroupReplication
	const recorded = "1790785744160779"

	// group returns the shard whose group runs on 100 (its primary, and the shard primary) and 200,
	// with the MySQL of the third voter, 101, OFFLINE.
	group := func(third *test.InfoForRecoveryAnalysis) []*test.InfoForRecoveryAnalysis {
		groupPrimary := sees(member(grRow(groupPrimaryTablet, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, replica), replica, crossCellReplica)
		groupPrimary.ReadOnly = 0
		secondary := sees(member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, replica), replica, crossCellReplica)
		rows := []*test.InfoForRecoveryAnalysis{groupPrimary, secondary, third}
		for _, row := range rows {
			if row.GroupMemberState == mysql.GroupMemberStateOnline {
				row.GroupViewID = recorded + ":7"
			}
		}
		return rows
	}
	tests := []struct {
		name string
		rows func() []*test.InfoForRecoveryAnalysis
		// notWant are analyses that no tablet may report.
		notWant []AnalysisCode
		// want is the analysis of the given tablets.
		want map[string]AnalysisCode
		// startGrace is groupStartInProgressGrace; 0 means its default, a negative value none.
		startGrace time.Duration
	}{{
		name: "a PRIMARY tablet out of its group is not joined",
		rows: func() []*test.InfoForRecoveryAnalysis {
			return group(member(grRow(stalePrimary, gr), mysql.GroupMemberStateOffline, "", false, nil))
		},
		notWant: []AnalysisCode{GroupMemberNotOnline},
	}, {
		name: "a tablet that runs as PRIMARY, out of its group, is not joined",
		rows: func() []*test.InfoForRecoveryAnalysis {
			row := member(grRow(grTablet("zone1", 101, topodatapb.TabletType_REPLICA), gr), mysql.GroupMemberStateOffline, "", false, nil)
			row.CurrentTabletType = int(topodatapb.TabletType_PRIMARY)
			return group(row)
		},
		notWant: []AnalysisCode{GroupMemberNotOnline},
	}, {
		name: "a REPLICA out of its group is joined",
		rows: func() []*test.InfoForRecoveryAnalysis {
			return group(member(grRow(grTablet("zone1", 101, topodatapb.TabletType_REPLICA), gr), mysql.GroupMemberStateOffline, "", false, nil))
		},
		want: map[string]AnalysisCode{"zone1-0000000101": GroupMemberNotOnline},
	}, {
		name: "a voter whose START is in progress is not joined again",
		rows: func() []*test.InfoForRecoveryAnalysis {
			row := member(grRow(grTablet("zone1", 101, topodatapb.TabletType_REPLICA), gr), mysql.GroupMemberStateOffline, "", false, nil)
			row.GroupStartInProgress = 1
			return group(row)
		},
		notWant: []AnalysisCode{GroupMemberNotOnline, ClusterHasNoPrimary, PrimaryTabletDeleted},
	}, {
		name: "no group is bootstrapped while a voter's START is in progress",
		rows: func() []*test.InfoForRecoveryAnalysis {
			starting := member(grRow(grTablet("zone1", 101, topodatapb.TabletType_REPLICA), gr), mysql.GroupMemberStateOffline, "", false, nil)
			starting.GroupStartInProgress = 1
			return []*test.InfoForRecoveryAnalysis{
				member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil),
				member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOffline, "", false, nil),
				starting,
			}
		},
		notWant: []AnalysisCode{GroupNotBootstrapped, ClusterHasNoPrimary, PrimaryTabletDeleted, GroupMemberNotOnline},
	}, {
		name: "the group is bootstrapped once VTOrc observed a voter's START for longer than the grace",
		rows: func() []*test.InfoForRecoveryAnalysis {
			starting := member(grRow(grTablet("zone1", 101, topodatapb.TabletType_REPLICA), gr), mysql.GroupMemberStateOffline, "", false, nil)
			starting.GroupStartInProgress = 1
			return []*test.InfoForRecoveryAnalysis{
				member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil),
				member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOffline, "", false, nil),
				starting,
			}
		},
		startGrace: -1,
		want:       map[string]AnalysisCode{"zone1-0000000100": GroupNotBootstrapped},
	}, {
		name: "the group is bootstrapped once no START is in progress",
		rows: func() []*test.InfoForRecoveryAnalysis {
			return []*test.InfoForRecoveryAnalysis{
				member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil),
				member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOffline, "", false, nil),
				member(grRow(grTablet("zone1", 101, topodatapb.TabletType_REPLICA), gr), mysql.GroupMemberStateOffline, "", false, nil),
			}
		},
		want: map[string]AnalysisCode{"zone1-0000000100": GroupNotBootstrapped},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GroupReplicationConditions.Reset()
			UnreachableGroupTablets.Reset()
			oldGrace := groupStartInProgressGrace
			t.Cleanup(func() { groupStartInProgressGrace = oldGrace })
			if tt.startGrace != 0 {
				groupStartInProgressGrace = max(tt.startGrace, 0)
			}
			rows := tt.rows()
			for _, row := range rows {
				row.ShardGroupReplicationVoters = voterList(stalePrimary, replica, crossCellReplica)
				row.ShardGroupReplicationIncarnation = recorded
			}
			analyses := runAnalysis(t, rows)
			got := analysisCodes(analyses)
			for alias, code := range tt.want {
				assert.Equal(t, code, got[alias], "tablet %s", alias)
			}
			for _, a := range analyses {
				for _, problem := range a.AnalysisMatchedProblems {
					assert.NotContains(t, tt.notWant, problem.Analysis, "tablet %s", topoproto.TabletAliasString(a.AnalyzedInstanceAlias))
				}
			}
		})
	}
}

// TestGetDetectionAnalysisGroupReplicationUndoDemotePrimary checks that VTOrc's PrimaryIsReadOnly and
// PrimaryCurrentTypeMismatch, whose recovery is UndoDemotePrimary, apply under a group replication
// policy only to the primary of the shard's legitimate group. Group Replication leaves the primary it
// elects super_read_only (the member action mysql_disable_super_read_only_if_primary is disabled), and
// the tablet keeps MySQL read-only while the serving invariant does not hold, refusing
// UndoDemotePrimary: VTOrc would retry it every few seconds. Under another policy they apply as before.
func TestGetDetectionAnalysisGroupReplicationUndoDemotePrimary(t *testing.T) {
	resetPrimaryHealthState()
	// Every tablet is reachable: the voter replacement grace period does not matter.
	t.Cleanup(func() {
		GroupReplicationConditions.Reset()
		UnreachableGroupTablets.Reset()
	})

	primary := grTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	replica := grTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	crossCellReplica := grTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	gr := policy.DurabilityGroupReplication
	const recorded = "1790785744160779"

	// Each fault makes one of the analyses match a PRIMARY tablet that is otherwise healthy.
	faults := []struct {
		analysis AnalysisCode
		apply    func(row *test.InfoForRecoveryAnalysis)
	}{{
		analysis: PrimaryIsReadOnly,
		apply:    func(row *test.InfoForRecoveryAnalysis) { row.ReadOnly = 1 },
	}, {
		analysis: PrimaryCurrentTypeMismatch,
		// A PRIMARY in the topology whose tablet runs as a REPLICA, for example after a
		// DemotePrimary that its caller did not follow up.
		apply: func(row *test.InfoForRecoveryAnalysis) { row.CurrentTabletType = int(topodatapb.TabletType_REPLICA) },
	}}
	tests := []struct {
		name string
		// rows returns the rows of the shard, the PRIMARY tablet's first.
		rows func() []*test.InfoForRecoveryAnalysis
		// match is whether the analyses match the PRIMARY tablet.
		match bool
	}{{
		name: "the legitimate group's primary",
		rows: func() []*test.InfoForRecoveryAnalysis {
			return []*test.InfoForRecoveryAnalysis{
				sees(member(grRow(primary, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary), primary, replica, crossCellReplica),
				sees(member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary), primary, replica, crossCellReplica),
				sees(member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary), primary, replica, crossCellReplica),
			}
		},
		match: true,
	}, {
		name: "the primary of a view without the voter majority",
		rows: func() []*test.InfoForRecoveryAnalysis {
			return []*test.InfoForRecoveryAnalysis{
				sees(member(grRow(primary, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary), primary),
				member(grRow(replica, gr), mysql.GroupMemberStateOffline, "", false, nil),
				member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOffline, "", false, nil),
			}
		},
	}, {
		name: "a PRIMARY tablet whose MySQL is out of its group",
		rows: func() []*test.InfoForRecoveryAnalysis {
			return []*test.InfoForRecoveryAnalysis{
				member(grRow(primary, gr), mysql.GroupMemberStateOffline, "", false, nil),
				sees(member(grRow(replica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, replica), replica, crossCellReplica),
				sees(member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, replica), replica, crossCellReplica),
			}
		},
	}, {
		name: "a primary under a semi-sync policy",
		rows: func() []*test.InfoForRecoveryAnalysis {
			return []*test.InfoForRecoveryAnalysis{grRow(primary, policy.DurabilitySemiSync)}
		},
		match: true,
	}}
	for _, fault := range faults {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s: %s", fault.analysis, tt.name), func(t *testing.T) {
				GroupReplicationConditions.Reset()
				UnreachableGroupTablets.Reset()
				rows := tt.rows()
				fault.apply(rows[0])
				for _, row := range rows {
					if row.DurabilityPolicy == gr {
						row.ShardGroupReplicationVoters = voterList(primary, replica, crossCellReplica)
						row.ShardGroupReplicationIncarnation = recorded
						if row.GroupMemberState == mysql.GroupMemberStateOnline {
							row.GroupViewID = recorded + ":7"
						}
					}
				}
				var matched bool
				for _, a := range runAnalysis(t, rows) {
					if topoproto.TabletAliasEqual(a.AnalyzedInstanceAlias, primary.Alias) {
						for _, problem := range a.AnalysisMatchedProblems {
							matched = matched || problem.Analysis == fault.analysis
						}
					}
				}
				assert.Equal(t, tt.match, matched)
			})
		}
	}
}
