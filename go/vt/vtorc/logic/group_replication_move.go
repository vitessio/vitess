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
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// Group Replication elects a new primary without looking at the topology. When it elects a member
// whose cell's topology server does not answer, the member's tablet cannot become PRIMARY: it
// writes its own tablet record, in its cell's topology server, before anything else, and so does
// VTOrc's PromoteGroupPrimary through the tablet. The shard then has no primary until that server
// answers again (NEW-4 of the Group Replication failover audit: 104.6s in the G9b chaos scenario,
// 169.6s in S9b). VTOrc instead makes another member the group primary, one whose cell answers,
// with group_replication_set_as_primary (PromoteReplica on that member, as a planned reparent
// does): its tablet can write its record, and becomes the shard primary.
var (
	// groupPrimaryMoveGracePeriod is how long the group primary's tablet must have failed to become
	// the topology primary (since VTOrc first saw GroupPrimaryNotInTopo) before VTOrc moves the
	// group primary away from it. A tablet promotes itself within one --group-replication-sync-interval
	// (1s by default, a few milliseconds after the election in the end-to-end tests), and VTOrc only
	// gets here after its own per-cell read of the topology (groupReplicationCellTimeout, 2s) found
	// the tablet's cell unreachable. 5s also rides out a short unavailability of a topology server,
	// such as an etcd leader election (1s election timeout by default), and stays well below the 15s
	// after which the tablet's own attempt to write its record gives up. A primary that is moved
	// needlessly costs one more switch of the group primary, while the shard has no primary anyway.
	groupPrimaryMoveGracePeriod = 5 * time.Second

	// groupPrimaryMoveHoldPeriod is how long a member that VTOrc moved the group primary away from
	// is neither moved to nor moved away from again, so that the group primary does not go back
	// and forth between members whose cells' topology servers come and go.
	groupPrimaryMoveHoldPeriod = time.Minute

	// groupPrimaryMoveRetryInterval is how long VTOrc waits before it runs the recovery of a group
	// primary that it could not move again (no eligible member, or the hold period), so that it
	// does not take the shard lock and report a failed recovery on every poll.
	groupPrimaryMoveRetryInterval = 10 * time.Second

	// groupPrimaryMoveStatusTimeout bounds the FullStatus RPC on the tablet of the group primary
	// that is about to be moved. The move does not depend on that tablet; its answer only stops the
	// move when the tablet is PRIMARY already.
	groupPrimaryMoveStatusTimeout = 2 * time.Second
)

// groupPrimaryMoveTracker remembers, in this VTOrc, the members that the group primary was moved
// away from, and the group primaries whose recovery waits before it runs again.
type groupPrimaryMoveTracker struct {
	mu sync.Mutex
	// movedAway maps a tablet alias to when the group primary was moved away from its MySQL.
	movedAway map[string]time.Time
	// retryAt maps a tablet alias to when its GroupPrimaryNotInTopo recovery may run again.
	retryAt map[string]time.Time
}

// groupPrimaryMoves is the groupPrimaryMoveTracker of this VTOrc. The shard lock serializes the
// moves of the VTOrcs of all cells; each one re-reads the group under the lock, and the tracker
// only keeps one VTOrc from undoing its own move.
var groupPrimaryMoves = newGroupPrimaryMoveTracker()

func newGroupPrimaryMoveTracker() *groupPrimaryMoveTracker {
	return &groupPrimaryMoveTracker{
		movedAway: make(map[string]time.Time),
		retryAt:   make(map[string]time.Time),
	}
}

// recordMoveAway records that the group primary was moved away from the tablet at now.
func (t *groupPrimaryMoveTracker) recordMoveAway(alias string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.movedAway[alias] = now
	delete(t.retryAt, alias)
}

// holdRemaining returns for how long the tablet, which the group primary was moved away from, is
// still neither moved to nor away from. It is 0 when no such move is recent.
func (t *groupPrimaryMoveTracker) holdRemaining(alias string, now time.Time) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	for a, at := range t.movedAway {
		if now.Sub(at) >= groupPrimaryMoveHoldPeriod {
			delete(t.movedAway, a)
		}
	}
	at, ok := t.movedAway[alias]
	if !ok {
		return 0
	}
	return groupPrimaryMoveHoldPeriod - now.Sub(at)
}

// backOff delays the next GroupPrimaryNotInTopo recovery of the tablet until the given time.
func (t *groupPrimaryMoveTracker) backOff(alias string, until time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.retryAt[alias] = until
}

// backingOff returns whether the GroupPrimaryNotInTopo recovery of the tablet must wait.
func (t *groupPrimaryMoveTracker) backingOff(alias string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	until, ok := t.retryAt[alias]
	if !ok {
		return false
	}
	if !now.Before(until) {
		delete(t.retryAt, alias)
		return false
	}
	return true
}

// reset forgets everything. It is used by tests.
func (t *groupPrimaryMoveTracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.movedAway = make(map[string]time.Time)
	t.retryAt = make(map[string]time.Time)
}

// groupPrimaryMoveSkipCode returns RecoverySkipGroupPrimaryMoveBackoff while the
// GroupPrimaryNotInTopo recovery of the analyzed tablet waits after it could not move the group
// primary.
func groupPrimaryMoveSkipCode(analysisEntry *inst.DetectionAnalysis, now time.Time) RecoverySkipCode {
	if groupPrimaryMoves.backingOff(topoproto.TabletAliasString(analysisEntry.AnalyzedInstanceAlias), now) {
		return RecoverySkipGroupPrimaryMoveBackoff
	}
	return RecoverySkipNone
}

// groupPrimaryMoveCandidate is a member that the group primary could be moved to.
type groupPrimaryMoveCandidate struct {
	tablet *topodatapb.Tablet
	// weight is the group_replication_member_weight that the durability policy gives the member.
	weight int
}

// chooseGroupPrimaryMoveTarget returns the candidate with the highest member weight, the one the
// group itself would prefer, and among those the lowest tablet alias, so that every VTOrc makes
// the same choice. It returns nil without candidates.
func chooseGroupPrimaryMoveTarget(candidates []groupPrimaryMoveCandidate) *topodatapb.Tablet {
	if len(candidates) == 0 {
		return nil
	}
	sorted := slices.Clone(candidates)
	slices.SortFunc(sorted, func(a, b groupPrimaryMoveCandidate) int {
		if c := cmp.Compare(b.weight, a.weight); c != 0 {
			return c
		}
		return cmp.Compare(topoproto.TabletAliasString(a.tablet.Alias), topoproto.TabletAliasString(b.tablet.Alias))
	})
	return sorted[0].tablet
}

// legitimateGroupView returns the membership view of a reachable member of the shard's legitimate
// group: an ONLINE member with quorum in the group of the shard, in the recorded incarnation, with
// a majority of the listed voters ONLINE in its view. Every such member must agree on the
// incarnation and on the group primary.
func legitimateGroupView(keyspace, shard string, legitimate *policy.LegitimateGroup, statuses []*shardTabletStatus) (*replicationdatapb.GroupReplicationStatus, error) {
	groupName := policy.GroupName(keyspace, shard)
	var view *replicationdatapb.GroupReplicationStatus
	for _, st := range statuses {
		if st.err != nil {
			continue
		}
		gs := st.status.GetGroupReplicationStatus()
		if gs.GetMemberState() != mysql.GroupMemberStateOnline || !gs.GetHasQuorum() || gs.GetPrimaryUuid() == "" || gs.GetGroupName() != groupName {
			continue
		}
		if !legitimate.IsLegitimateMember(gs) || !legitimate.HasVoterMajority(gs) {
			continue
		}
		if view == nil {
			view = gs
			continue
		}
		if policy.GroupIncarnation(view.GetViewId()) != policy.GroupIncarnation(gs.GetViewId()) || view.GetPrimaryUuid() != gs.GetPrimaryUuid() {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"the members of the replication group of %s disagree about the group or its primary (%s/%s vs %s/%s at %s); the membership is changing",
				topoproto.KeyspaceShardString(keyspace, shard), view.GetViewId(), view.GetPrimaryUuid(), gs.GetViewId(), gs.GetPrimaryUuid(), topoproto.TabletAliasString(st.tablet.Alias))
		}
	}
	if view == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE,
			"no reachable tablet of %s in a cell whose topology server answers is an ONLINE member of the shard's legitimate replication group (its recorded incarnation %q, with at least %d of the %d voters ONLINE)",
			topoproto.KeyspaceShardString(keyspace, shard), legitimate.Incarnation, legitimate.VoterMajority(), len(legitimate.Voters))
	}
	return view, nil
}

// isOnlineInView returns whether the member with the given server_uuid is ONLINE in the view.
func isOnlineInView(view *replicationdatapb.GroupReplicationStatus, serverUUID string) bool {
	if serverUUID == "" {
		return false
	}
	for _, m := range view.GetMembers() {
		if m.GetMemberUuid() == serverUUID {
			return m.GetState() == mysql.GroupMemberStateOnline
		}
	}
	return false
}

// moveGroupPrimaryOutOfUnreachableCell makes another member the primary of the shard's replication
// group, when the group primary's tablet is not the topology primary and its cell's topology server
// did not answer (failedCells, from the per-cell read of the shard's tablets under the shard lock).
// It returns the tablet whose MySQL it made the group primary, if any. The tablet of the new group
// primary becomes the shard primary in the same RPC, as in a planned reparent.
//
// The move does not depend on the tablet of the current group primary: it re-reads, under the shard
// lock, the status of the tablets of the cells that answer, and only moves when:
//   - the durability policy uses Group Replication;
//   - the group primary's tablet has not been the topology primary for groupPrimaryMoveGracePeriod;
//   - the members of the shard's legitimate group (recorded incarnation, a majority of the voters
//     ONLINE) that answer agree that the analyzed tablet's MySQL is still their primary;
//   - the tablet is not PRIMARY: neither in the shard record nor, if it answers, in its own state;
//   - the group primary was not moved away from it within groupPrimaryMoveHoldPeriod;
//   - an eligible target exists: an ONLINE voter in that view, in a cell whose topology server
//     answered, that the durability policy allows to be promoted, and that the group primary was
//     not moved away from within groupPrimaryMoveHoldPeriod. The highest member weight wins, then
//     the lowest alias.
//
// When it does not move for lack of a target, or because of the hold period, the recovery of the
// tablet waits groupPrimaryMoveRetryInterval before it runs again.
func moveGroupPrimaryOutOfUnreachableCell(ctx context.Context, analysisEntry *inst.DetectionAnalysis, groupPrimary *topodatapb.Tablet, shardInfo *topo.ShardInfo,
	tabletInfos []*topo.TabletInfo, failedCells []string, topologyRecovery *TopologyRecovery, logger *log.PrefixedLogger,
) (*topodatapb.Tablet, error) {
	now := time.Now()
	keyspace, shard := groupPrimary.Keyspace, groupPrimary.Shard
	keyspaceShard := topoproto.KeyspaceShardString(keyspace, shard)
	groupPrimaryAlias := topoproto.TabletAliasString(groupPrimary.Alias)
	cell := groupPrimary.Alias.Cell

	durability, err := inst.GetShardRecordDurabilityPolicy(keyspace, shardInfo.Shard)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read the durability policy of shard %s", keyspaceShard)
	}
	grd, ok := policy.AsGroupReplication(durability)
	if !ok {
		// While the shard is being converted the policy is still asynchronous, and the shard
		// primary may not be a member of the group: the group primary is not moved.
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"cannot promote %s: the topology server of its cell %s does not answer, and the durability policy of shard %s does not use group replication, so the group primary is not moved",
			groupPrimaryAlias, cell, keyspaceShard)
	}
	if remaining := groupPrimaryMoves.holdRemaining(groupPrimaryAlias, now); remaining > 0 {
		groupPrimaryMoves.backOff(groupPrimaryAlias, now.Add(min(remaining, groupPrimaryMoveRetryInterval)))
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"not moving the primary of the replication group of %s away from %s again: it was moved away from it less than %v ago",
			keyspaceShard, groupPrimaryAlias, groupPrimaryMoveHoldPeriod)
	}
	if notInTopoFor := inst.ObserveGroupPrimaryNotInTopo(groupPrimary.Alias, now); notInTopoFor < groupPrimaryMoveGracePeriod {
		groupPrimaryMoves.backOff(groupPrimaryAlias, now.Add(groupPrimaryMoveGracePeriod-notInTopoFor))
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE,
			"%s, the primary of the replication group of %s, cannot become PRIMARY while the topology server of its cell %s does not answer; waiting %v for it before moving the group primary",
			groupPrimaryAlias, keyspaceShard, cell, groupPrimaryMoveGracePeriod-notInTopoFor)
	}

	// The group primary's own state, if its tablet answers. The move does not wait for it longer
	// than groupPrimaryMoveStatusTimeout.
	var (
		ownStatus *replicationdatapb.FullStatus
		ownErr    error
		ownDone   = make(chan struct{})
	)
	go func() {
		defer close(ownDone)
		statusCtx, cancel := context.WithTimeout(ctx, groupPrimaryMoveStatusTimeout)
		defer cancel()
		ownStatus, ownErr = tabletFullStatus(statusCtx, groupPrimary)
	}()
	statuses := readShardTabletStatuses(ctx, tabletInfos)
	<-ownDone

	if topoproto.TabletAliasEqual(shardInfo.PrimaryAlias, groupPrimary.Alias) {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s is already the primary of %s in the shard record", groupPrimaryAlias, keyspaceShard)
	}
	if ownErr == nil && ownStatus.GetTabletType() == topodatapb.TabletType_PRIMARY {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s already runs as PRIMARY", groupPrimaryAlias)
	}

	// Only the shard's legitimate group counts, as the members of the cells that answer see it.
	// The group primary is identified by the server_uuid that its tablet reports, or that VTOrc
	// last discovered.
	legitimate := legitimateGroupOf(shardInfo, statuses)
	view, err := legitimateGroupView(keyspace, shard, legitimate, statuses)
	if err != nil {
		groupPrimaryMoves.backOff(groupPrimaryAlias, now.Add(groupPrimaryMoveRetryInterval))
		message := fmt.Sprintf("not moving the primary of the replication group of %s away from %s: %v", keyspaceShard, groupPrimaryAlias, err)
		logger.Warn(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return nil, vterrors.Wrapf(err, "not moving the primary of the replication group away from %s", groupPrimaryAlias)
	}
	groupPrimaryUUID := ownStatus.GetServerUuid()
	if ownErr != nil || groupPrimaryUUID == "" {
		if instance, _, err := inst.ReadInstance(groupPrimary.Alias); err == nil && instance != nil {
			groupPrimaryUUID = instance.ServerUUID
		}
	}
	if groupPrimaryUUID == "" || view.GetPrimaryUuid() != groupPrimaryUUID {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"the MySQL of %s (server_uuid %q) is no longer the primary of the shard's replication group, whose primary is %s",
			groupPrimaryAlias, groupPrimaryUUID, view.GetPrimaryUuid())
	}

	target, rejected := groupPrimaryMoveTarget(durability, grd, shardInfo.GetGroupReplicationVoters(), groupPrimary, legitimate, view, statuses, failedCells, now)
	if target == nil {
		groupPrimaryMoves.backOff(groupPrimaryAlias, now.Add(groupPrimaryMoveRetryInterval))
		message := fmt.Sprintf("cannot promote %s, the primary of the replication group of %s, while the topology server of its cell %s does not answer, "+
			"and no other member can become the group primary: no ONLINE voter of the shard's legitimate group in a cell whose topology server answers is eligible (%s); cells that do not answer: %s; retrying in %v",
			groupPrimaryAlias, keyspaceShard, cell, strings.Join(rejected, "; "), strings.Join(failedCells, ", "), groupPrimaryMoveRetryInterval)
		logger.Warn(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, message)
	}

	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return nil, vterrors.Wrapf(err, "lost the lock of %s before moving the group primary", keyspaceShard)
	}
	targetAlias := topoproto.TabletAliasString(target.Alias)
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("the topology server of cell %s does not answer, so %s, the primary of the replication group, cannot become PRIMARY: making %s the group primary",
		cell, groupPrimaryAlias, targetAlias))
	// PromoteReplica on an ONLINE member runs group_replication_set_as_primary, which waits for the
	// member to apply its backlog, and then makes its tablet PRIMARY: like the emergency reparent of
	// a group, it gets the replica wait timeout.
	promoteCtx, cancel := context.WithTimeout(ctx, max(config.GetWaitReplicasTimeout(), topo.RemoteOperationTimeout))
	defer cancel()
	position, err := tmc.PromoteReplica(promoteCtx, target, policy.SemiSyncAckers(durability, target) > 0)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to make %s the primary of the replication group of %s", targetAlias, keyspaceShard)
	}
	groupPrimaryMoves.recordMoveAway(groupPrimaryAlias, time.Now())
	_ = inst.AuditOperation(PromoteGroupPrimaryRecoveryName, target.Alias,
		fmt.Sprintf("moved the primary of the replication group away from %s, whose cell %s's topology server does not answer", groupPrimaryAlias, cell))
	logger.Info(fmt.Sprintf("moved the primary of the replication group of %s from %s to %s: the topology server of cell %s does not answer", keyspaceShard, groupPrimaryAlias, targetAlias, cell))

	// Record the reparent in the journal, like a reparent does. The tablet is already the shard
	// primary; a failure here is reported but does not undo the move.
	if err := tmc.PopulateReparentJournal(promoteCtx, target, time.Now().UnixNano(), getLockAction(target.Alias, analysisEntry.Analysis), target.Alias, position); err != nil {
		return target, vterrors.Wrapf(err, "made %s the primary of the replication group, but failed to write the reparent journal", targetAlias)
	}
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s: moved the primary of the replication group from %s to %s", PromoteGroupPrimaryRecoveryName, groupPrimaryAlias, targetAlias))
	return target, nil
}

// groupPrimaryMoveTarget returns the member that the group primary is moved to, or nil and why
// each reachable tablet is not eligible. See moveGroupPrimaryOutOfUnreachableCell.
func groupPrimaryMoveTarget(durability policy.Durabler, grd policy.GroupReplicationDurabler, voters []*topodatapb.TabletAlias, groupPrimary *topodatapb.Tablet,
	legitimate *policy.LegitimateGroup, view *replicationdatapb.GroupReplicationStatus, statuses []*shardTabletStatus, failedCells []string, now time.Time,
) (*topodatapb.Tablet, []string) {
	var (
		candidates []groupPrimaryMoveCandidate
		rejected   []string
	)
	incarnation := policy.GroupIncarnation(view.GetViewId())
	for _, st := range statuses {
		tablet := st.tablet
		alias := topoproto.TabletAliasString(tablet.Alias)
		if topoproto.TabletAliasEqual(tablet.Alias, groupPrimary.Alias) {
			continue
		}
		gs := st.status.GetGroupReplicationStatus()
		reason := ""
		switch {
		case st.err != nil:
			reason = "unreachable"
		case slices.Contains(failedCells, tablet.Alias.Cell):
			reason = "the topology server of its cell does not answer"
		case len(voters) > 0 && !policy.IsVoter(voters, tablet.Alias):
			reason = "not a voter"
		case len(voters) == 0 && !policy.IsGroupMember(durability, tablet):
			reason = "not allowed in the group by the durability policy"
		case policy.PromotionRule(durability, tablet) == promotionrule.MustNot:
			reason = "must not be promoted according to the durability policy"
		case gs.GetMemberState() != mysql.GroupMemberStateOnline || gs.GetGroupName() != view.GetGroupName() ||
			!legitimate.IsLegitimateMember(gs) || policy.GroupIncarnation(gs.GetViewId()) != incarnation ||
			!isOnlineInView(view, st.status.GetServerUuid()):
			reason = "not an ONLINE member of the shard's legitimate group"
		case groupPrimaryMoves.holdRemaining(alias, now) > 0:
			reason = fmt.Sprintf("the group primary was moved away from it less than %v ago", groupPrimaryMoveHoldPeriod)
		}
		if reason != "" {
			rejected = append(rejected, alias+": "+reason)
			continue
		}
		candidates = append(candidates, groupPrimaryMoveCandidate{tablet: tablet, weight: grd.MemberWeight(tablet)})
	}
	return chooseGroupPrimaryMoveTarget(candidates), rejected
}
