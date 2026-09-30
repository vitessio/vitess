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
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// groupPrimaryNotInTopoGracePeriod is how long VTOrc must observe that a group primary's tablet
// is not the topology primary before it reports GroupPrimaryNotInTopo. Tablets promote their
// own record within about a second (--group-replication-sync-interval), so VTOrc only steps
// in when the tablet does not. It is a variable so that tests can shorten it.
var groupPrimaryNotInTopoGracePeriod = 2 * time.Second

// isGroupMemberActive returns whether a member in the given state is part of a group.
func isGroupMemberActive(pluginActive bool, memberState string) bool {
	return mysql.IsGroupMemberActive(&replicationdatapb.GroupReplicationStatus{
		PluginActive: pluginActive,
		MemberState:  memberState,
	})
}

// isGroupPrimary returns whether a member in the given state is the writable primary of a
// group that has quorum.
func isGroupPrimary(pluginActive bool, memberState, memberRole string, hasQuorum bool) bool {
	return mysql.IsGroupPrimary(&replicationdatapb.GroupReplicationStatus{
		PluginActive: pluginActive,
		MemberState:  memberState,
		MemberRole:   memberRole,
		HasQuorum:    hasQuorum,
	})
}

// countOnlineGroupMembers returns the number of ONLINE members in a member's view of its group.
func countOnlineGroupMembers(status *replicationdatapb.GroupReplicationStatus) uint {
	var online uint
	for _, m := range status.GetMembers() {
		if m.GetState() == mysql.GroupMemberStateOnline {
			online++
		}
	}
	return online
}

// groupReplicationRow is the Group Replication state of one tablet of a shard, as used to
// compute the shard-wide Group Replication state.
type groupReplicationRow struct {
	tablet *topodatapb.Tablet
	// valid is true when VTOrc's last check of the tablet succeeded, so its state is current.
	valid bool
	// active, online, groupPrimary and hasQuorum describe the tablet's last known member state.
	active       bool
	online       bool
	groupPrimary bool
	hasQuorum    bool
	primaryUUID  string
}

// groupReplicationShardState is the Group Replication state of a shard, aggregated over all of
// its tablets.
type groupReplicationShardState struct {
	// activeMembers is the number of reachable tablets whose MySQL is an active group member.
	activeMembers uint
	// anyActive is true when any tablet, reachable or not, last reported an active member. An
	// unreachable tablet that was a member may still be one, so a new group must not be
	// bootstrapped.
	anyActive bool
	// reachableNonMemberPrimary is true when VTOrc reached a tablet of type PRIMARY whose MySQL is
	// not an active group member: the shard has a working primary outside of any group.
	reachableNonMemberPrimary bool
	// quorumMembers is the number of reachable active members that have quorum.
	quorumMembers uint
	// primaryAlias is the tablet whose MySQL is the group's primary, if VTOrc reached it.
	primaryAlias *topodatapb.TabletAlias
	// primaryUUID is the group primary's server_uuid, as reported by reachable members that
	// have quorum.
	primaryUUID string
	// votingMembers is the number of tablets that the durability policy makes voting members.
	votingMembers uint
	// unreachableVotingMembers is the number of voting member tablets VTOrc could not reach.
	unreachableVotingMembers uint
	// cellMajority is the cell that holds a majority of the ONLINE members, if any.
	cellMajority string
}

// computeGroupReplicationShardState aggregates the Group Replication state of the tablets of a
// shard.
func computeGroupReplicationShardState(durability policy.Durabler, rows []*groupReplicationRow) *groupReplicationShardState {
	state := &groupReplicationShardState{}
	var onlineTablets []*topodatapb.Tablet
	for _, row := range rows {
		if row.active {
			state.anyActive = true
		}
		if row.valid && !row.active && row.tablet.GetType() == topodatapb.TabletType_PRIMARY {
			state.reachableNonMemberPrimary = true
		}
		if policy.IsGroupMember(durability, row.tablet) {
			state.votingMembers++
			if !row.valid {
				state.unreachableVotingMembers++
			}
		}
		if !row.valid || !row.active {
			continue
		}
		state.activeMembers++
		if row.online {
			onlineTablets = append(onlineTablets, row.tablet)
		}
		if row.hasQuorum {
			state.quorumMembers++
			if row.primaryUUID != "" {
				state.primaryUUID = row.primaryUUID
			}
		}
		if row.groupPrimary {
			state.primaryAlias = row.tablet.GetAlias()
		}
	}
	// A single ONLINE member trivially holds a majority of the ONLINE members. That is the
	// normal state right after a bootstrap, before the other members join; the members that
	// do not join are reported as GroupMemberNotOnline.
	if len(onlineTablets) > 1 {
		state.cellMajority, _ = policy.CellHoldsMajority(onlineTablets)
	}
	return state
}

// applyGroupReplicationShardState copies the shard-wide Group Replication state into an analysis.
func applyGroupReplicationShardState(a *DetectionAnalysis, state *groupReplicationShardState) {
	if state == nil {
		return
	}
	a.ShardGroupActiveMembers = state.activeMembers
	a.ShardGroupQuorumMembers = state.quorumMembers
	a.ShardGroupPrimaryAlias = state.primaryAlias
	a.ShardGroupPrimaryUUID = state.primaryUUID
	a.ShardGroupVotingMembers = state.votingMembers
	a.ShardGroupUnreachableVotingMembers = state.unreachableVotingMembers
	a.ShardGroupCellMajority = state.cellMajority
	a.shardGroupAnyActive = state.anyActive
	a.shardReachableNonMemberPrimary = state.reachableNonMemberPrimary
}

// isGroupSecondary returns whether the analyzed tablet's MySQL is an active group member that is
// not the group's primary. Such a member is read-only, and it must not be a shard primary.
func isGroupSecondary(a *DetectionAnalysis) bool {
	return a.IsGroupMemberActive && !a.IsGroupPrimary
}

// groupNeedsBootstrap returns whether the shard's durability policy uses Group Replication but no
// tablet of the shard is known to be an active member. Such a shard gets a primary by
// bootstrapping its group (GroupNotBootstrapped), not by a reparent.
func groupNeedsBootstrap(a *DetectionAnalysis, ca *clusterAnalysis) bool {
	return policy.IsGroupReplication(ca.durability) && !a.shardGroupAnyActive
}

// matchGroupNotBootstrapped returns whether the shard's group must be bootstrapped: its durability
// policy uses Group Replication, no tablet is known to be an active member, and VTOrc reached
// every voting member on its last check, so it knows that none of them is. The analysis is
// reported on the voting members.
func matchGroupNotBootstrapped(a *DetectionAnalysis, ca *clusterAnalysis, tablet *topodatapb.Tablet) bool {
	return groupNeedsBootstrap(a, ca) && policy.IsGroupMember(ca.durability, tablet) && a.LastCheckValid &&
		a.ShardGroupVotingMembers > 0 && a.ShardGroupUnreachableVotingMembers == 0
}

// otherActiveGroupMembers returns the number of reachable active members of the shard's group,
// not counting the analyzed tablet.
func otherActiveGroupMembers(a *DetectionAnalysis) uint {
	if a.LastCheckValid && a.IsGroupMemberActive && a.ShardGroupActiveMembers > 0 {
		return a.ShardGroupActiveMembers - 1
	}
	return a.ShardGroupActiveMembers
}

// matchGroupPrimaryNotInTopo returns whether the analyzed tablet's MySQL is the group primary
// but the tablet is not the shard's topology primary, and that has been the case for longer
// than the grace period during which the tablet is expected to promote itself.
//
// While the durability policy does not use Group Replication, the shard may be in the middle of a
// conversion, and the group only replaces the shard primary when that primary is a member too. A
// group that runs next to a working primary outside of it is left alone.
func matchGroupPrimaryNotInTopo(a *DetectionAnalysis, ca *clusterAnalysis, now time.Time) bool {
	if !a.LastCheckValid || !a.IsGroupPrimary {
		return false
	}
	if !policy.IsGroupReplication(ca.durability) && a.shardReachableNonMemberPrimary {
		return false
	}
	// The tablet already runs as PRIMARY; VTOrc's copy of the topology is merely behind.
	if a.TabletType == topodatapb.TabletType_PRIMARY || a.CurrentTabletType == topodatapb.TabletType_PRIMARY {
		return false
	}
	key := "GroupPrimaryNotInTopo/" + topoproto.TabletAliasString(a.AnalyzedInstanceAlias)
	return GroupReplicationConditions.Observe(key, now) >= groupPrimaryNotInTopoGracePeriod
}

// ConditionTracker remembers since when a condition has been observed continuously. A condition
// that is not observed again within forgetAfter is considered to have ended, and the next
// observation starts a new period.
type ConditionTracker struct {
	mu          sync.Mutex
	forgetAfter time.Duration
	conditions  map[string]*observedCondition
}

type observedCondition struct {
	firstSeen time.Time
	lastSeen  time.Time
}

// NewConditionTracker returns a ConditionTracker that forgets conditions that are not observed
// for longer than forgetAfter.
func NewConditionTracker(forgetAfter time.Duration) *ConditionTracker {
	return &ConditionTracker{
		forgetAfter: forgetAfter,
		conditions:  make(map[string]*observedCondition),
	}
}

// GroupReplicationConditions tracks the Group Replication conditions for which VTOrc waits
// before it acts: a group primary whose tablet does not promote itself, and a failed primary
// in a shard whose group is expected to elect a new primary on its own. Analysis and recovery
// run every --recovery-poll-duration (1s by default), which observes a persisting condition
// well within the forget period.
var GroupReplicationConditions = NewConditionTracker(10 * time.Second)

// Observe records that the condition identified by key holds at now, and returns for how long
// it has held.
func (ct *ConditionTracker) Observe(key string, now time.Time) time.Duration {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	for k, c := range ct.conditions {
		if now.Sub(c.lastSeen) > ct.forgetAfter {
			delete(ct.conditions, k)
		}
	}
	c, ok := ct.conditions[key]
	if !ok {
		c = &observedCondition{firstSeen: now}
		ct.conditions[key] = c
	}
	c.lastSeen = now
	return now.Sub(c.firstSeen)
}

// Reset forgets all conditions. It is used by tests.
func (ct *ConditionTracker) Reset() {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.conditions = make(map[string]*observedCondition)
}
