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
	"slices"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
)

// groupPrimaryNotInTopoGracePeriod is how long VTOrc must observe that a group primary's tablet
// is not the topology primary before it reports GroupPrimaryNotInTopo. Tablets promote their
// own record within about a second (--group-replication-sync-interval), so VTOrc only steps
// in when the tablet does not. It is a variable so that tests can shorten it.
var groupPrimaryNotInTopoGracePeriod = 2 * time.Second

// groupStartInProgressGrace bounds how long a START GROUP_REPLICATION in progress on a tablet keeps
// VTOrc from bootstrapping the shard's group, from the moment VTOrc first observed it. Such a START
// reports its member OFFLINE until it ends, for up to about a minute when it finds no group, and
// MySQL refuses to stop it meanwhile. It cannot join anything while no member is active, which is when
// the group needs a bootstrap, and a group of its own that it forms stays super_read_only (the member
// action mysql_disable_super_read_only_if_primary is disabled), and is left by its tablet as a
// foreign group, or adopted if its member is the target of a bootstrap intent. Waiting longer only
// delays the bootstrap: in the G12 chaos scenario, by 6-22s per cycle. It is a variable so that tests
// can change it.
var groupStartInProgressGrace = 10 * time.Second

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

// ActiveGroupMemberUUIDs returns the sorted server_uuids of the members that a member sees as
// active (ONLINE or RECOVERING) in its view of its group.
func ActiveGroupMemberUUIDs(status *replicationdatapb.GroupReplicationStatus) []string {
	var uuids []string
	for _, m := range status.GetMembers() {
		if m.GetMemberUuid() == "" {
			continue
		}
		if m.GetState() == mysql.GroupMemberStateOnline || m.GetState() == mysql.GroupMemberStateRecovering {
			uuids = append(uuids, m.GetMemberUuid())
		}
	}
	slices.Sort(uuids)
	return uuids
}

// OnlineGroupMemberUUIDs returns the sorted server_uuids of the members that a member sees as
// ONLINE in its view of its group.
func OnlineGroupMemberUUIDs(status *replicationdatapb.GroupReplicationStatus) []string {
	var uuids []string
	for _, m := range status.GetMembers() {
		if m.GetMemberUuid() != "" && m.GetState() == mysql.GroupMemberStateOnline {
			uuids = append(uuids, m.GetMemberUuid())
		}
	}
	slices.Sort(uuids)
	return uuids
}

// groupRowStatus rebuilds the group replication status of a member from what VTOrc stores about
// it, as far as the legitimacy of its group needs it: its own state and role, its view id, and
// the members it sees as ONLINE.
func groupRowStatus(pluginActive bool, memberState, memberRole string, hasQuorum bool, primaryUUID, viewID string, onlineMemberUUIDs []string) *replicationdatapb.GroupReplicationStatus {
	status := &replicationdatapb.GroupReplicationStatus{
		PluginActive: pluginActive,
		MemberState:  memberState,
		MemberRole:   memberRole,
		HasQuorum:    hasQuorum,
		PrimaryUuid:  primaryUUID,
		ViewId:       viewID,
	}
	for _, uuid := range onlineMemberUUIDs {
		status.Members = append(status.Members, &replicationdatapb.GroupReplicationMember{MemberUuid: uuid, State: mysql.GroupMemberStateOnline})
	}
	return status
}

// splitGroupMemberUUIDs parses a comma-separated list of server_uuids, as stored in the database.
func splitGroupMemberUUIDs(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

// UnreachableGroupTablets tracks since when VTOrc cannot reach the tablets of shards whose
// durability policy uses Group Replication. A voting member that stays unreachable, and that the
// group no longer sees as an active member, for longer than
// --group-replication-voter-replacement-grace-period gives its seat to another tablet. Analysis
// runs every --recovery-poll-duration (1s by default), well within the forget period.
var UnreachableGroupTablets = NewConditionTracker(10 * time.Second)

// aliasesString returns the tablet aliases, sorted and separated by commas.
func aliasesString(aliases []*topodatapb.TabletAlias) string {
	strs := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		strs = append(strs, topoproto.TabletAliasString(alias))
	}
	slices.Sort(strs)
	return strings.Join(strs, ", ")
}

// SameGroupReplicationVoters returns whether the two lists hold the same voters, in any order.
func SameGroupReplicationVoters(a, b []*topodatapb.TabletAlias) bool {
	if len(a) != len(b) {
		return false
	}
	for _, alias := range a {
		if !policy.IsVoter(b, alias) {
			return false
		}
	}
	return true
}

// groupReplicationRow is the Group Replication state of one tablet of a shard, as used to
// compute the shard-wide Group Replication state.
type groupReplicationRow struct {
	tablet *topodatapb.Tablet
	// valid is true when VTOrc's last check of the tablet succeeded, so its state is current.
	valid bool
	// serverUUID is the server_uuid of the tablet's MySQL, as last seen.
	serverUUID string
	// active, online, groupPrimary and hasQuorum describe the tablet's last known member state.
	active       bool
	online       bool
	groupPrimary bool
	hasQuorum    bool
	primaryUUID  string
	// activeMemberUUIDs are the members that the tablet's MySQL last saw as active.
	activeMemberUUIDs []string
	// startInProgress is true when a START GROUP_REPLICATION ran on the tablet's MySQL, as last seen.
	startInProgress bool
	// deleted is true when the tablet is a voter whose tablet record was deleted, which VTOrc keeps
	// discovering (see inst.MarkDeletedGroupVoter).
	deleted bool
	// status is the member's group replication status, as far as VTOrc stores it.
	status *replicationdatapb.GroupReplicationStatus
	// foreign is true when the member is active in a group of another incarnation than the one
	// the shard record lists.
	foreign bool
}

// groupReplicationShardState is the Group Replication state of a shard, aggregated over all of
// its tablets.
type groupReplicationShardState struct {
	// activeMembers is the number of reachable tablets whose MySQL is an active group member.
	activeMembers uint
	// legitimateActiveMembers is the number of reachable tablets whose MySQL is an active member
	// of the shard's legitimate group (the recorded incarnation, if one is recorded) with quorum
	// in its view: members that a voter can join.
	legitimateActiveMembers uint
	// foreignMembers are the tablets whose MySQL is active in a group of another incarnation than
	// the one the shard record lists.
	foreignMembers map[string]bool
	// anyActive is true when any tablet, reachable or not, last reported an active member, or a
	// START GROUP_REPLICATION in progress that VTOrc observed for less than
	// groupStartInProgressGrace. An unreachable tablet that was a member may still be one, so a new
	// group must not be bootstrapped. Nor right away while a START runs: its member reports OFFLINE,
	// for up to about a minute when it finds no group, and can still end as the primary of a group
	// of its own, next to the one a bootstrap creates; MySQL also refuses to stop it until it ends.
	// A voter whose tablet record was deleted counts only while VTOrc reaches it.
	anyActive bool
	// anyMember is anyActive without the STARTs in progress: whether any tablet last reported an
	// active member.
	anyMember bool
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
	// voters are the voting members of the shard's group, as recorded in the shard record.
	voters []*topodatapb.TabletAlias
	// votingMembers is the number of voters.
	votingMembers uint
	// unreachableVotingMembers is the number of voters that VTOrc could not reach, including
	// voters whose tablet no longer exists.
	unreachableVotingMembers uint
	// cellMajority is the cell that holds a majority of the ONLINE members, if any.
	cellMajority string
	// desiredVoters is the voter list that VTOrc writes next (GroupVotersOutOfDate), if any.
	desiredVoters []*topodatapb.TabletAlias
	// voterAnalysis is the shard-wide voter analysis (GroupVotersOutOfDate, GroupPrimaryNotVoter, or
	// one of the alerts), and voterReason says why. See PlanGroupVoters.
	voterAnalysis AnalysisCode
	voterReason   string
	// votersReporter is the tablet on which voterAnalysis is reported: the group primary that is not a
	// voter for GroupPrimaryNotVoter; otherwise the group primary's tablet if VTOrc reached it, else
	// the reachable tablet with the lowest alias. Only PRIMARY and replica type tablets qualify,
	// because only they are analyzed.
	votersReporter *topodatapb.TabletAlias
}

// computeGroupReplicationShardState aggregates the Group Replication state of the tablets of a
// shard. Only the shard's legitimate group counts (see policy.LegitimateGroup): a member active
// in a group of another incarnation than the recorded one neither has quorum nor is a primary
// for VTOrc, and the group primary is the primary of a view that holds a majority of the
// shard's voters.
func computeGroupReplicationShardState(durability policy.Durabler, incarnation string, voters []*topodatapb.TabletAlias, rows []*groupReplicationRow, now time.Time) *groupReplicationShardState {
	state := &groupReplicationShardState{
		voters:         voters,
		votingMembers:  uint(len(voters)),
		foreignMembers: make(map[string]bool),
	}
	applyGroupLegitimacy(state, incarnation, voters, rows)
	reachable := make(map[string]bool)
	var onlineTablets []*topodatapb.Tablet
	for _, row := range rows {
		// A voter whose tablet record was deleted counts as unreachable: no recovery can use it.
		if row.valid && !row.deleted {
			reachable[topoproto.TabletAliasString(row.tablet.GetAlias())] = true
		}
		// The state that VTOrc last saw on a voter whose tablet record was deleted, and that it no longer
		// reaches, is not a member: the operator's deletion hands it to the voter planner, which removes
		// it once it is down (DeletedVoter.Down) or keeps it with an alert. It would otherwise make a
		// shard where no group runs look like one with a group, whose missing PRIMARY tablet an
		// emergency reparent would replace, in vain and ahead of the voter change.
		staleDeleted := row.deleted && !row.valid
		if row.active && !staleDeleted {
			state.anyActive = true
			state.anyMember = true
		}
		if row.startInProgress && !staleDeleted && GroupStartInProgressBlocksBootstrap(row.tablet.GetAlias(), now) {
			state.anyActive = true
		}
		if row.valid && !row.active && row.tablet.GetType() == topodatapb.TabletType_PRIMARY {
			state.reachableNonMemberPrimary = true
		}
		if !row.valid || !row.active {
			continue
		}
		state.activeMembers++
		if !row.foreign && row.hasQuorum {
			state.legitimateActiveMembers++
		}
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
	for _, voter := range voters {
		if !reachable[topoproto.TabletAliasString(voter)] {
			state.unreachableVotingMembers++
		}
	}
	computeGroupReplicationVoters(state, durability, incarnation, rows, now)
	return state
}

// applyGroupLegitimacy restricts the group primary and the quorum of the rows to the shard's
// legitimate group. The voters are identified in the views by the server_uuids that VTOrc last
// saw on their tablets, and by their MySQL addresses.
func applyGroupLegitimacy(state *groupReplicationShardState, incarnation string, voters []*topodatapb.TabletAlias, rows []*groupReplicationRow) {
	tablets := make(map[string]*topodatapb.Tablet, len(rows))
	uuids := make(map[string]string, len(rows))
	for _, row := range rows {
		alias := topoproto.TabletAliasString(row.tablet.GetAlias())
		tablets[alias] = row.tablet
		uuids[alias] = row.serverUUID
	}
	legitimate := policy.NewLegitimateGroup(incarnation, voters, tablets, uuids)
	for _, row := range rows {
		if row.status == nil {
			// The status is not known; only the view quorum applies.
			continue
		}
		if legitimate.IsForeignIncarnation(row.status) {
			row.foreign = true
			row.groupPrimary = false
			row.hasQuorum = false
			row.primaryUUID = ""
			state.foreignMembers[topoproto.TabletAliasString(row.tablet.GetAlias())] = true
			continue
		}
		row.groupPrimary = row.groupPrimary && legitimate.IsLegitimatePrimary(row.status)
	}
}

// computeGroupReplicationVoters plans, on VTOrc's stored state, the change of the shard's voters, or
// the alert, that the analysis reports (see PlanGroupVoters). That state lacks the members' executed
// GTID sets and their primary_election_in_progress: the recovery reads the shard again under the shard
// lock, and decides on that read.
func computeGroupReplicationVoters(state *groupReplicationShardState, durability policy.Durabler, incarnation string, rows []*groupReplicationRow, now time.Time) {
	grd, ok := policy.AsGroupReplication(durability)
	if !ok {
		return
	}
	in := &VoterPlanInput{
		Durability:  grd,
		Voters:      state.voters,
		Incarnation: incarnation,
		GracePeriod: config.GetGroupReplicationVoterReplacementGracePeriod(),
	}
	recorded := make(map[string]bool, len(rows))
	for _, row := range rows {
		alias := topoproto.TabletAliasString(row.tablet.GetAlias())
		recorded[alias] = true
		if row.deleted {
			// VTOrc kept the voter whose record was deleted, and discovers it at the address it knew.
			if in.DeletedVoters == nil {
				in.DeletedVoters = make(map[string]*DeletedVoter)
			}
			// Down: VTOrc has not reached it for the grace period, as the recovery decides on its probe.
			down := !row.valid && UnreachableGroupTablets.Observe(alias, now) >= in.GracePeriod
			in.DeletedVoters[alias] = &DeletedVoter{Alias: row.tablet.GetAlias(), Tablet: row.tablet, ServerUUID: row.serverUUID, Down: down}
			continue
		}
		vt := &VoterTablet{Tablet: row.tablet, Reachable: row.valid, ServerUUID: row.serverUUID}
		if row.valid {
			vt.Status = analysisGroupStatus(row)
		} else {
			vt.UnreachableFor = UnreachableGroupTablets.Observe(alias, now)
		}
		in.Tablets = append(in.Tablets, vt)
	}
	for _, voter := range state.voters {
		alias := topoproto.TabletAliasString(voter)
		if recorded[alias] {
			continue
		}
		// VTOrc knows nothing about this voter: it is not down.
		if in.DeletedVoters == nil {
			in.DeletedVoters = make(map[string]*DeletedVoter)
		}
		in.DeletedVoters[alias] = &DeletedVoter{Alias: voter}
	}
	plan := PlanGroupVoters(in)
	state.voterReason = plan.Reason
	switch {
	case plan.Action.ChangesVoters():
		state.voterAnalysis = GroupVotersOutOfDate
		state.desiredVoters = plan.Voters
		state.votersReporter = groupVotersReporter(rows)
	case plan.Action == VoterActionMovePrimary:
		state.voterAnalysis = GroupPrimaryNotVoter
		state.votersReporter = plan.GroupPrimary.GetAlias()
		if plan.PrimaryRecordDeleted {
			// The primary has no row: the analysis is reported on another tablet.
			state.votersReporter = groupVotersReporter(rows)
		}
	case plan.Alert != "":
		state.voterAnalysis = plan.Alert
		state.votersReporter = groupVotersReporter(rows)
	}
}

// analysisGroupStatus returns the group replication status of a reachable member as VTOrc stored it:
// its view lists the members it saw ONLINE, and those it saw active but not ONLINE as RECOVERING.
func analysisGroupStatus(row *groupReplicationRow) *replicationdatapb.GroupReplicationStatus {
	status := row.status.CloneVT()
	if status == nil {
		status = &replicationdatapb.GroupReplicationStatus{}
	}
	status.StartInProgress = row.startInProgress
	online := make(map[string]bool, len(status.Members))
	for _, m := range status.Members {
		online[m.GetMemberUuid()] = true
	}
	for _, uuid := range row.activeMemberUUIDs {
		if !online[uuid] {
			status.Members = append(status.Members, &replicationdatapb.GroupReplicationMember{MemberUuid: uuid, State: mysql.GroupMemberStateRecovering})
		}
	}
	return status
}

// groupVotersReporter returns the tablet on which the shard-wide voter analyses are reported: the
// group primary's tablet if VTOrc reached it, else the reachable tablet with the lowest alias. Only
// PRIMARY and replica type tablets with a tablet record qualify, because only they are analyzed.
func groupVotersReporter(rows []*groupReplicationRow) *topodatapb.TabletAlias {
	var reporter *topodatapb.TabletAlias
	for _, row := range rows {
		// A voter whose tablet record was deleted is not analyzed.
		if !row.valid || row.deleted || (row.tablet.GetType() != topodatapb.TabletType_PRIMARY && !topo.IsReplicaType(row.tablet.GetType())) {
			continue
		}
		if row.groupPrimary {
			return row.tablet.GetAlias()
		}
		if reporter == nil || topoproto.TabletAliasString(row.tablet.GetAlias()) < topoproto.TabletAliasString(reporter) {
			reporter = row.tablet.GetAlias()
		}
	}
	return reporter
}

// applyGroupReplicationShardState copies the shard-wide Group Replication state into an analysis.
func applyGroupReplicationShardState(a *DetectionAnalysis, state *groupReplicationShardState) {
	if state == nil {
		return
	}
	a.ShardGroupActiveMembers = state.activeMembers
	a.ShardGroupLegitimateActiveMembers = state.legitimateActiveMembers
	a.IsGroupMemberForeign = state.foreignMembers[topoproto.TabletAliasString(a.AnalyzedInstanceAlias)]
	a.IsLegitimateGroupPrimary = a.IsGroupPrimary && topoproto.TabletAliasEqual(state.primaryAlias, a.AnalyzedInstanceAlias)
	a.ShardGroupQuorumMembers = state.quorumMembers
	a.ShardGroupPrimaryAlias = state.primaryAlias
	a.ShardGroupPrimaryUUID = state.primaryUUID
	a.ShardGroupVotingMembers = state.votingMembers
	a.ShardGroupUnreachableVotingMembers = state.unreachableVotingMembers
	a.ShardGroupCellMajority = state.cellMajority
	a.ShardGroupVoters = state.voters
	a.ShardGroupDesiredVoters = state.desiredVoters
	a.IsGroupVoter = policy.IsVoter(state.voters, a.AnalyzedInstanceAlias)
	a.shardGroupAnyActive = state.anyActive
	a.shardGroupAnyMember = state.anyMember
	a.shardReachableNonMemberPrimary = state.reachableNonMemberPrimary
	if topoproto.TabletAliasEqual(state.votersReporter, a.AnalyzedInstanceAlias) {
		a.groupVoterAnalysis = state.voterAnalysis
		a.GroupVoterReason = state.voterReason
	}
}

// replicatesThroughGroup returns whether the analyzed tablet replicates through its shard's group
// rather than asynchronously from the primary, so that the asynchronous replication analyses
// (NotConnectedToPrimary, ReplicationStopped, ConnectedToWrongPrimary, ReplicaMisconfigured and the
// replica semi-sync ones), whose recovery points the default replication channel at the primary, do
// not apply to it: an active group member, and under a group replication policy a voter, also while
// it is out of its group, joining it, or in the ERROR state. Configuring the default channel on a
// voter makes the replication lag poller read that channel once the voter is back in its group, and
// the voter then serves no replica reads (G13 chaos run). While no voter is listed, every tablet that
// the policy allows in the group counts as a voter. A tablet that is not a voter replicates
// asynchronously, and keeps the analyses.
func replicatesThroughGroup(a *DetectionAnalysis, ca *clusterAnalysis) bool {
	if a.IsGroupMemberActive {
		return true
	}
	if !policy.IsGroupReplication(ca.durability) {
		return false
	}
	if len(a.ShardGroupVoters) == 0 {
		return policy.IsGroupMember(ca.durability, &topodatapb.Tablet{Alias: a.AnalyzedInstanceAlias, Type: a.TabletType})
	}
	return a.IsGroupVoter
}

// isGroupSecondary returns whether the analyzed tablet's MySQL is an active group member that is
// not the group's primary. Such a member is read-only, and it must not be a shard primary.
func isGroupSecondary(a *DetectionAnalysis) bool {
	return a.IsGroupMemberActive && !a.IsGroupPrimary
}

// groupNeedsBootstrap returns whether the shard's durability policy uses Group Replication but no
// tablet of the shard is known to be an active member, nor to run a START GROUP_REPLICATION. Such a
// shard gets a primary by bootstrapping its group (GroupNotBootstrapped), not by a reparent.
func groupNeedsBootstrap(a *DetectionAnalysis, ca *clusterAnalysis) bool {
	return policy.IsGroupReplication(ca.durability) && !a.shardGroupAnyActive
}

// groupHasNoMember returns whether the shard's durability policy uses Group Replication but no tablet
// of the shard is known to be an active member. Such a shard gets a primary by bootstrapping its
// group, or once a START in progress ends, not by a reparent.
func groupHasNoMember(a *DetectionAnalysis, ca *clusterAnalysis) bool {
	return policy.IsGroupReplication(ca.durability) && !a.shardGroupAnyMember
}

// matchPrimaryOfGroupShard returns whether a PRIMARY analysis that VTOrc repairs with
// UndoDemotePrimary (PrimaryIsReadOnly, PrimaryCurrentTypeMismatch) applies to the analyzed tablet:
// always under a policy that does not use Group Replication, and under one that does only on the
// primary of the shard's legitimate group. Group Replication leaves the primary it elects
// super_read_only, and a PRIMARY tablet whose MySQL is not that primary, or that may not serve, stays
// read-only by design: the tablet refuses UndoDemotePrimary until the serving invariant holds, and
// VTOrc would retry it every few seconds.
func matchPrimaryOfGroupShard(a *DetectionAnalysis, ca *clusterAnalysis) bool {
	return !policy.IsGroupReplication(ca.durability) || a.IsLegitimateGroupPrimary
}

// matchGroupNotBootstrapped returns whether the shard's group must be bootstrapped: its durability
// policy uses Group Replication, no tablet is known to be an active member, voters have been
// selected, and VTOrc reached every voter on its last check, so it knows that none of them is.
// The analysis is reported on the voters. While no voter is selected, GroupVotersOutOfDate
// selects them first.
func matchGroupNotBootstrapped(a *DetectionAnalysis, ca *clusterAnalysis) bool {
	return groupNeedsBootstrap(a, ca) && a.IsGroupVoter && a.LastCheckValid &&
		a.ShardGroupVotingMembers > 0 && a.ShardGroupUnreachableVotingMembers == 0
}

// matchGroupBootstrapNotRecorded returns whether the analyzed tablet is the target of its shard's
// bootstrap intent, and its MySQL is the primary of a group, with quorum, of another incarnation
// than the shard record lists: a bootstrap whose reply was lost. VTOrc adopts the group after it
// checked it again under the shard lock (see reparentutil.AdoptGroupReplicationBootstrap). The
// tablet trusts the group it bootstrapped for a minute, and then leaves it.
//
// It also matches the primary of a group while the shard record lists no incarnation, without an
// intent for it: a group that nobody recorded, for example one that the initial promotion of
// PlannedReparentShard bootstrapped and failed to record. No voter joins such a group
// (matchGroupMemberNotOnline), so VTOrc records it, when it is the only group the shard can have
// (see logic.adoptUnrecordedGroup).
func matchGroupBootstrapNotRecorded(a *DetectionAnalysis, ca *clusterAnalysis) bool {
	return policy.IsGroupReplication(ca.durability) && a.LastCheckValid &&
		(a.IsGroupBootstrapIntentTarget || a.ShardGroupIncarnation == "") &&
		a.IsGroupPrimary && a.GroupViewIncarnation != "" && a.GroupViewIncarnation != a.ShardGroupIncarnation
}

// matchGroupMemberNotOnline returns whether the analyzed tablet is a voter of its shard's group
// but its MySQL is not an active member, while other tablets are active members of the shard's
// legitimate group with quorum in their view.
//
// Such a voter is not analyzed for the shard-wide failovers of a shard without a primary tablet
// (ClusterHasNoPrimary, PrimaryTabletDeleted), which outrank it: a group that lost the majority of
// its voters has no primary that a reparent could follow, and it only gets one back once the
// missing voters rejoin it. The failovers are still analyzed on the members of the group. Starting a join while no such group exists cannot
// join anything: the join blocks until MySQL's join timeout, during which a bootstrap on the
// member fails, and a member has been seen to form a group of its own. Tablets that are not
// voters replicate asynchronously and keep the asynchronous replication analyses.
//
// A PRIMARY tablet never matches: its MySQL out of its group makes it a stale primary, which the
// tablet demotes on its own first, and the tablet refuses a join meanwhile (a join holds its action
// lock for up to a minute, which keeps the demotion from running).
//
// Nor does it match while the shard record lists no incarnation: every group then counts as the
// shard's group, also one that a join formed on its own when the member it joined left (the TLA+
// model's init_orc_lost). The component that bootstraps the group records its incarnation, and then
// makes the voters join it; VTOrc adopts and records a bootstrap whose reply was lost
// (GroupBootstrapNotRecorded).
func matchGroupMemberNotOnline(a *DetectionAnalysis, ca *clusterAnalysis) bool {
	return policy.IsGroupReplication(ca.durability) && a.IsGroupVoter && a.ShardGroupIncarnation != "" &&
		a.TabletType != topodatapb.TabletType_PRIMARY && a.CurrentTabletType != topodatapb.TabletType_PRIMARY &&
		a.LastCheckValid && !a.IsGroupMemberActive && !a.IsGroupMemberForeign && a.ShardGroupLegitimateActiveMembers > 0
}

// matchGroupVotersOutOfDate returns whether the voters of the analyzed tablet's shard must be
// updated. The analysis is reported on a single tablet of the shard, the group primary's if
// VTOrc reached it.
func matchGroupVotersOutOfDate(a *DetectionAnalysis, ca *clusterAnalysis) bool {
	return matchGroupVoterAnalysis(a, ca, GroupVotersOutOfDate)
}

// matchGroupVoterAnalysis returns whether the shard-wide voter analysis of the analyzed tablet's shard
// is code, and is reported on the analyzed tablet (see PlanGroupVoters).
func matchGroupVoterAnalysis(a *DetectionAnalysis, ca *clusterAnalysis, code AnalysisCode) bool {
	return policy.IsGroupReplication(ca.durability) && a.LastCheckValid && a.groupVoterAnalysis == code
}

// matchGroupPrimaryNotInTopo returns whether the analyzed tablet's MySQL is the primary of the
// shard's legitimate group but the tablet is not the shard's topology primary, and that has been
// the case for longer than the grace period during which the tablet is expected to promote itself.
//
// While the durability policy does not use Group Replication, the shard may be in the middle of a
// conversion, and the group only replaces the shard primary when that primary is a member too. A
// group that runs next to a working primary outside of it is left alone.
func matchGroupPrimaryNotInTopo(a *DetectionAnalysis, ca *clusterAnalysis, now time.Time) bool {
	if !a.LastCheckValid || !a.IsLegitimateGroupPrimary {
		return false
	}
	// A group primary that is not a voter does not serve: VTOrc moves the group primary to a voter
	// instead (GroupPrimaryNotVoter).
	if policy.IsGroupReplication(ca.durability) && len(a.ShardGroupVoters) > 0 && !a.IsGroupVoter {
		return false
	}
	if !policy.IsGroupReplication(ca.durability) && a.shardReachableNonMemberPrimary {
		return false
	}
	// The tablet already runs as PRIMARY; VTOrc's copy of the topology is merely behind.
	if a.TabletType == topodatapb.TabletType_PRIMARY || a.CurrentTabletType == topodatapb.TabletType_PRIMARY {
		return false
	}
	return ObserveGroupPrimaryNotInTopo(a.AnalyzedInstanceAlias, now) >= groupPrimaryNotInTopoGracePeriod
}

// SetGroupStartInProgressGrace sets groupStartInProgressGrace, and returns its previous value. It is
// used by tests.
func SetGroupStartInProgressGrace(grace time.Duration) time.Duration {
	previous := groupStartInProgressGrace
	groupStartInProgressGrace = grace
	return previous
}

// GroupStartInProgressBlocksBootstrap records that a START GROUP_REPLICATION runs on the tablet's
// MySQL, and returns whether it still keeps VTOrc from bootstrapping the shard's group: VTOrc has
// observed it for less than groupStartInProgressGrace.
func GroupStartInProgressBlocksBootstrap(alias *topodatapb.TabletAlias, now time.Time) bool {
	return GroupReplicationConditions.Observe("GroupStartInProgress/"+topoproto.TabletAliasString(alias), now) < groupStartInProgressGrace
}

// ObserveGroupPrimaryNotInTopo records that the tablet's MySQL is the primary of its shard's
// legitimate group while the tablet is not the topology primary, and returns for how long VTOrc has
// observed that.
func ObserveGroupPrimaryNotInTopo(alias *topodatapb.TabletAlias, now time.Time) time.Duration {
	return GroupReplicationConditions.Observe("GroupPrimaryNotInTopo/"+topoproto.TabletAliasString(alias), now)
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
