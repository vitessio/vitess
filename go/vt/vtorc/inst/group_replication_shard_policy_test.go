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

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/test"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestGetDetectionAnalysisShardPolicy checks that VTOrc analyzes a shard by its own durability
// policy, which MigrateReplicationMode sets while it converts a keyspace one shard at a time: a
// shard converted to Group Replication while the keyspace's policy is still semi_sync gets the
// Group Replication analyses and none of the semi-sync analyses that would undo its conversion, and
// a shard converted back while the keyspace's policy is still group_replication gets none of the
// Group Replication analyses, nor does a shard not converted yet while the keyspace names the target
// policy and keeps the policy it converts from as its migration source.
func TestGetDetectionAnalysisShardPolicy(t *testing.T) {
	resetPrimaryHealthState()
	oldVoterGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	config.SetGroupReplicationVoterReplacementGracePeriod(time.Hour)
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(oldVoterGrace)
		GroupReplicationConditions.Reset()
		UnreachableGroupTablets.Reset()
	})

	primary := grTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	replica := grTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	replica2 := grTablet("zone1", 102, topodatapb.TabletType_REPLICA)
	crossCellReplica := grTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	semiSync := policy.DurabilitySemiSync
	gr := policy.DurabilityGroupReplication

	// row is the analysis row of a tablet of the shard, whose keyspace has the policy
	// keyspacePolicy and the shard its own policy shardPolicy.
	row := func(tablet *topodatapb.Tablet, keyspacePolicy, shardPolicy string) *test.InfoForRecoveryAnalysis {
		r := grRow(tablet, keyspacePolicy)
		r.ShardDurabilityPolicy = shardPolicy
		return r
	}

	tests := []struct {
		name    string
		voters  []*topodatapb.Tablet
		rows    func() []*test.InfoForRecoveryAnalysis
		want    map[string]AnalysisCode
		notWant []AnalysisCode
	}{
		{
			name:   "converted shard: a voter whose MySQL left the group rejoins it",
			voters: []*topodatapb.Tablet{primary, replica, replica2},
			rows: func() []*test.InfoForRecoveryAnalysis {
				rows := []*test.InfoForRecoveryAnalysis{
					member(row(primary, semiSync, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary),
					member(row(replica, semiSync, gr), mysql.GroupMemberStateOffline, "", false, nil),
					member(row(replica2, semiSync, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
				}
				// The migration recorded the incarnation of the group it bootstrapped.
				rows[0].GroupViewID, rows[2].GroupViewID = "1790000001:3", "1790000001:3"
				for _, r := range rows {
					r.ShardGroupReplicationIncarnation = "1790000001"
				}
				return rows
			},
			want: map[string]AnalysisCode{"zone1-0000000100": GroupMemberNotOnline},
		},
		{
			name:   "converted shard: no member is active, the group is bootstrapped",
			voters: []*topodatapb.Tablet{replica, replica2, crossCellReplica},
			rows: func() []*test.InfoForRecoveryAnalysis {
				return []*test.InfoForRecoveryAnalysis{
					member(row(replica, semiSync, gr), mysql.GroupMemberStateOffline, "", false, nil),
					member(row(replica2, semiSync, gr), mysql.GroupMemberStateOffline, "", false, nil),
					member(row(crossCellReplica, semiSync, gr), mysql.GroupMemberStateOffline, "", false, nil),
				}
			},
			want: map[string]AnalysisCode{"zone1-0000000100": GroupNotBootstrapped},
		},
		{
			name:   "converted shard: an asynchronous replica outside the group does not acknowledge semi-sync",
			voters: []*topodatapb.Tablet{primary, replica2, crossCellReplica},
			rows: func() []*test.InfoForRecoveryAnalysis {
				asyncReplica := row(replica, semiSync, gr)
				asyncReplica.IsPrimary = 0
				asyncReplica.ReplicationStopped = 0
				asyncReplica.PrimaryTabletInfo = primary
				return []*test.InfoForRecoveryAnalysis{
					member(row(primary, semiSync, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, primary),
					member(row(replica2, semiSync, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
					member(row(crossCellReplica, semiSync, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, primary),
					asyncReplica,
				}
			},
			notWant: []AnalysisCode{ReplicaSemiSyncMustBeSet, PrimarySemiSyncMustBeSet},
		},
		{
			name: "shard converted back: no group analysis while the keyspace still uses group replication",
			rows: func() []*test.InfoForRecoveryAnalysis {
				// The voters were cleared; every tablet replicates asynchronously from the primary.
				asyncReplica := row(replica, gr, semiSync)
				asyncReplica.IsPrimary = 0
				asyncReplica.ReplicationStopped = 0
				asyncReplica.SemiSyncReplicaEnabled = 1
				asyncReplica.PrimaryTabletInfo = primary
				primaryRow := row(primary, gr, semiSync)
				primaryRow.SemiSyncPrimaryEnabled = 1
				primaryRow.CountReplicas = 1
				primaryRow.CountValidReplicas = 1
				primaryRow.CountValidReplicatingReplicas = 1
				return []*test.InfoForRecoveryAnalysis{primaryRow, asyncReplica}
			},
			notWant: []AnalysisCode{GroupVotersOutOfDate, GroupNotBootstrapped, PrimarySemiSyncMustNotBeSet, ReplicaSemiSyncMustNotBeSet},
		},
		{
			name: "shard not converted yet: the keyspace names the target policy, and its migration source applies",
			rows: func() []*test.InfoForRecoveryAnalysis {
				// MigrateReplicationMode named group_replication in the keyspace record before its
				// first bootstrap, and kept semi_sync as the migration source.
				asyncReplica := row(replica, gr, "")
				asyncReplica.KeyspaceMigrationSourceDurabilityPolicy = semiSync
				asyncReplica.IsPrimary = 0
				asyncReplica.ReplicationStopped = 0
				asyncReplica.SemiSyncReplicaEnabled = 1
				asyncReplica.PrimaryTabletInfo = primary
				primaryRow := row(primary, gr, "")
				primaryRow.KeyspaceMigrationSourceDurabilityPolicy = semiSync
				primaryRow.SemiSyncPrimaryEnabled = 1
				primaryRow.CountReplicas = 1
				primaryRow.CountValidReplicas = 1
				primaryRow.CountValidReplicatingReplicas = 1
				return []*test.InfoForRecoveryAnalysis{primaryRow, asyncReplica}
			},
			notWant: []AnalysisCode{GroupVotersOutOfDate, GroupNotBootstrapped, PrimarySemiSyncMustNotBeSet, ReplicaSemiSyncMustNotBeSet},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GroupReplicationConditions.Reset()
			UnreachableGroupTablets.Reset()
			rows := tt.rows()
			for _, r := range rows {
				r.ShardGroupReplicationVoters = voterList(tt.voters...)
			}
			got := analysisCodes(runAnalysis(t, rows))
			if tt.want != nil {
				assert.Equal(t, tt.want, got)
			}
			for _, code := range tt.notWant {
				for alias, gotCode := range got {
					assert.NotEqual(t, code, gotCode, "tablet %s", alias)
				}
			}
		})
	}
}
