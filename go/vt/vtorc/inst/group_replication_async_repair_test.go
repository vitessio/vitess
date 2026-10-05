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

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/test"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestGetDetectionAnalysisGroupReplicationVoterNotRepairedAsync reproduces the G13 r1 chaos run: a voter
// whose join was in flight (its START in progress, so GroupMemberNotOnline does not match) matched
// NotConnectedToPrimary, and VTOrc's fixReplica configured the default replication channel on it
// (SetReplicationSource). Once the member was back in its group, the replication lag poller read that
// channel, reported a lag that grew, and the member served no replica reads. Under a group replication
// policy, a voter replicates through the group, active or not: the asynchronous replication analyses
// do not apply to it. A tablet that is not a voter replicates asynchronously, and keeps them.
func TestGetDetectionAnalysisGroupReplicationVoterNotRepairedAsync(t *testing.T) {
	resetPrimaryHealthState()
	t.Cleanup(func() {
		GroupReplicationConditions.Reset()
		UnreachableGroupTablets.Reset()
	})
	replica := grTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	groupPrimaryTablet := grTablet("zone1", 100, topodatapb.TabletType_PRIMARY)
	voter := grTablet("zone1", 101, topodatapb.TabletType_REPLICA)
	crossCellReplica := grTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	nonVoter := grTablet("zone1", 102, topodatapb.TabletType_REPLICA)
	gr := policy.DurabilityGroupReplication
	const recorded = "1790785744160779"
	asyncRepairs := []AnalysisCode{
		NotConnectedToPrimary, ReplicationStopped, ConnectedToWrongPrimary, ReplicaMisconfigured,
		ReplicaSemiSyncMustBeSet, ReplicaSemiSyncMustNotBeSet,
	}

	// group returns the shard whose group runs on 100 (its primary, and the shard primary) and 200,
	// with the given other tablets.
	group := func(others ...*test.InfoForRecoveryAnalysis) []*test.InfoForRecoveryAnalysis {
		groupPrimary := sees(member(grRow(groupPrimaryTablet, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary, true, replica), replica, crossCellReplica)
		groupPrimary.ReadOnly = 0
		groupPrimary.GroupViewID = recorded + ":7"
		secondary := sees(member(grRow(crossCellReplica, gr), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary, true, replica), replica, crossCellReplica)
		secondary.GroupViewID = recorded + ":7"
		return append([]*test.InfoForRecoveryAnalysis{groupPrimary, secondary}, others...)
	}
	// outOfGroup is the voter 101 out of its group, in the given state; its default replication
	// channel is not connected to the primary, nor running.
	outOfGroup := func(state string) *test.InfoForRecoveryAnalysis {
		row := member(grRow(voter, gr), state, "", false, nil)
		row.ReplicaNetTimeout = 30
		return row
	}
	tests := []struct {
		name string
		rows func() []*test.InfoForRecoveryAnalysis
		// matched are problems that the tablet must have.
		matched map[string]AnalysisCode
		// notMatched are problems that the voter 101 must not have.
		notMatched []AnalysisCode
	}{{
		name: "a voter whose join is in flight",
		rows: func() []*test.InfoForRecoveryAnalysis {
			row := outOfGroup(mysql.GroupMemberStateOffline)
			row.GroupStartInProgress = 1
			return group(row)
		},
		notMatched: asyncRepairs,
	}, {
		name: "a voter in the ERROR state",
		rows: func() []*test.InfoForRecoveryAnalysis {
			return group(outOfGroup(mysql.GroupMemberStateError))
		},
		matched:    map[string]AnalysisCode{"zone1-0000000101": GroupMemberNotOnline},
		notMatched: asyncRepairs,
	}, {
		name: "a tablet that is not a voter still replicates asynchronously",
		rows: func() []*test.InfoForRecoveryAnalysis {
			async := grRow(nonVoter, gr)
			async.IsPrimary = 0
			async.PrimaryTabletInfo = groupPrimaryTablet
			return group(outOfGroup(mysql.GroupMemberStateError), async)
		},
		matched: map[string]AnalysisCode{"zone1-0000000102": ReplicationStopped},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GroupReplicationConditions.Reset()
			UnreachableGroupTablets.Reset()
			rows := tt.rows()
			for _, row := range rows {
				row.ShardGroupReplicationVoters = voterList(replica, voter, crossCellReplica)
				row.ShardGroupReplicationIncarnation = recorded
			}
			analyses := runAnalysis(t, rows)
			matched := make(map[string][]AnalysisCode)
			for _, a := range analyses {
				alias := topoproto.TabletAliasString(a.AnalyzedInstanceAlias)
				matched[alias] = append(matched[alias], a.Analysis)
				for _, problem := range a.AnalysisMatchedProblems {
					matched[alias] = append(matched[alias], problem.Analysis)
				}
			}
			for alias, code := range tt.matched {
				assert.Contains(t, matched[alias], code, "tablet %s", alias)
			}
			for _, code := range tt.notMatched {
				assert.NotContains(t, matched["zone1-0000000101"], code)
			}
		})
	}
}
