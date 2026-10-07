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

package reparentutil

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// groupSwapPlan is the change of the voters that a PlannedReparentShard makes to promote a tablet that is
// not a voter: the elect takes the seat of the voter of its cell (replaced), or a new seat when its cell has
// none (a grow).
type groupSwapPlan struct {
	// incarnation and voters are what the shard record listed: the voter write is a compare-and-swap on both.
	incarnation string
	voters      []*topodatapb.TabletAlias
	newVoters   []*topodatapb.TabletAlias
	// replaced is the voter whose seat the elect takes, nil for a grow.
	replaced *topodatapb.TabletAlias
	elect    *topodatapb.Tablet
	// tablets are the tablets of the shard, whose statuses swapAfterDemote reads again.
	tablets []*topodatapb.Tablet
	// afterDemote is set when replaced is the current primary: PRS demotes it first, and swaps then
	// (swapAfterDemote).
	afterDemote bool
}

// planGroupReplicationSwap decides how a PlannedReparentShard promotes primaryElect, a tablet that is not a
// voter of the shard's group, on one read of every tablet's status under the shard lock. The elect takes the
// seat of the voter of its cell, or a new seat when its cell has none, with the checks of VTOrc's voter
// changes (see inst.PlanGroupVoters):
//   - P1: the current primary is the settled legitimate primary: the ONLINE primary, with quorum, of a view of
//     the recorded incarnation, whose election ended, and whose view holds a majority of the voters ONLINE;
//   - P3: the elect is a valid spare: a REPLICA that the policy allows as a voter, running Group Replication,
//     whose MySQL is in no group and runs no START GROUP_REPLICATION, and which executed nothing that the
//     current primary lacks;
//   - the new list holds a majority of its voters ONLINE in the current primary's view without the voter it
//     drops and without the elect, which is in no view yet: the primary keeps a majority of the new list
//     before the elect joins. When the replaced voter is the current primary, PRS demotes it first and
//     checks this again (swapAfterDemote).
//
// A group of a single voter is refused: its primary's view cannot hold a majority of a grown list before the
// elect joins it; VTOrc grows such a group (JoinSpareBeforeGrow).
func planGroupReplicationSwap(ctx context.Context, pr *PlannedReparenter, shard *topodatapb.Shard, grd policy.GroupReplicationDurabler,
	tablets []*topodatapb.Tablet, currentPrimary, primaryElect *topodatapb.Tablet,
) (*groupSwapPlan, error) {
	electAlias := topoproto.TabletAliasString(primaryElect.Alias)
	plan := &groupSwapPlan{incarnation: shard.GetGroupReplicationIncarnation(), voters: shard.GetGroupReplicationVoters(), elect: primaryElect, tablets: tablets}
	if currentPrimary == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v is not a voter of the shard's replication group (voters: %s), and the shard has no current primary to swap it in under", electAlias, votersString(plan.voters))
	}
	if plan.incarnation == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v is not a voter, and the shard record lists no group incarnation: its voters cannot be changed until VTOrc recorded the group", electAlias)
	}
	if primaryElect.Type != topodatapb.TabletType_REPLICA || !grd.IsGroupMember(primaryElect) {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v is a %v tablet, which the durability policy does not allow as a voter of the replication group", electAlias, primaryElect.Type)
	}
	for _, voter := range plan.voters {
		if voter.Cell == primaryElect.Alias.Cell {
			plan.replaced = voter
		}
	}
	plan.afterDemote = plan.replaced != nil && topoproto.TabletAliasEqual(plan.replaced, currentPrimary.Alias)
	if plan.replaced == nil {
		if len(plan.voters) == 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"primary-elect %v is not a voter, its cell has no voter, and the group has a single voter (%s): its primary's view cannot hold a majority of a grown list before %v joins. "+
					"VTOrc grows the group (the spare joins first); retry once %v is a voter, or promote a voter",
				electAlias, votersString(plan.voters), electAlias, electAlias)
		}
		if len(plan.voters)+1 > policy.MaxGroupReplicationMembers {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"primary-elect %v is not a voter, and the group has %d voters already, the most MySQL allows", electAlias, len(plan.voters))
		}
		plan.newVoters = sortedAliases(append(slices.Clone(plan.voters), primaryElect.Alias))
	} else {
		plan.newVoters = sortedAliases(append(withoutVoter(plan.voters, plan.replaced), primaryElect.Alias))
	}

	statuses := fetchFullStatuses(ctx, pr.tmc, tablets, topo.RemoteOperationTimeout)
	if err := checkGroupSwap(plan, statuses, currentPrimary); err != nil {
		return nil, err
	}
	return plan, nil
}

// checkGroupSwap checks P1 on the current primary, P3 on the elect, and the majority of the new list in the
// current primary's view, on the given statuses (see planGroupReplicationSwap).
func checkGroupSwap(plan *groupSwapPlan, statuses map[string]*fullStatusResult, currentPrimary *topodatapb.Tablet) error {
	primaryAlias := topoproto.TabletAliasString(currentPrimary.Alias)
	electAlias := topoproto.TabletAliasString(plan.elect.Alias)
	primary, elect := statuses[primaryAlias], statuses[electAlias]
	if primary == nil || primary.err != nil {
		return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "cannot read the status of the current primary %v to swap primary-elect %v in as a voter", primaryAlias, electAlias)
	}
	if elect == nil || elect.err != nil {
		return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "cannot read the status of primary-elect %v to swap it in as a voter", electAlias)
	}

	// P1, the settled legitimate primary.
	legitimate := legitimateGroup(plan.incarnation, plan.voters, statuses)
	gs := primary.groupStatus()
	if !legitimate.IsLegitimatePrimary(gs) || !legitimate.HasVoterMajority(gs) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"current primary %v is not the settled primary of the shard's replication group (incarnation %q, %d of the %d voters ONLINE in its view): the voters cannot be changed to swap in primary-elect %v",
			primaryAlias, plan.incarnation, legitimate.OnlineVoters(gs), len(plan.voters), electAlias)
	}
	if gs.GetPrimaryElectionInProgress() {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the election of the group primary %v is in progress: retry the reparent", primaryAlias)
	}

	// P3, a valid spare.
	egs := elect.groupStatus()
	switch {
	case !elect.status.GetGroupReplicationEnabled():
		return groupReplicationNotEnabledError(electAlias)
	case mysql.IsGroupMemberActive(egs):
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "primary-elect %v is not a voter, but its MySQL is %s in a replication group", electAlias, egs.GetMemberState())
	case egs.GetStartInProgress():
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "a START GROUP_REPLICATION runs on the MySQL of primary-elect %v: retry the reparent", electAlias)
	case egs.GetPluginActive() && egs.GetMemberState() != "" && egs.GetMemberState() != mysql.GroupMemberStateOffline:
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the MySQL of primary-elect %v is in a group (%s): stop group replication on it first", electAlias, egs.GetMemberState())
	}
	primaryExecuted, _, err := GroupMemberGTIDSets(primary.status)
	if err != nil {
		return vterrors.Wrapf(err, "failed to read the GTID set of the current primary %v", primaryAlias)
	}
	electExecuted, _, err := GroupMemberGTIDSets(elect.status)
	if err != nil {
		return vterrors.Wrapf(err, "failed to read the GTID set of primary-elect %v", electAlias)
	}
	if !primaryExecuted.Contains(electExecuted) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v executed transactions that the current primary %v lacks (%s): it cannot join the group", electAlias, primaryAlias, gtidSetDifference(electExecuted, primaryExecuted))
	}

	// The new list's majority in the primary's view: the elect is in no view yet, and the replaced voter is
	// not in the new list.
	next := legitimateGroup(plan.incarnation, plan.newVoters, statuses)
	if online := next.OnlineVoters(gs); online < next.VoterMajority() {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"swapping primary-elect %v in as a voter would leave the view of the current primary %v with %d of the %d voters of the new list [%s] ONLINE before %v joins, not a majority",
			electAlias, primaryAlias, online, len(plan.newVoters), votersString(plan.newVoters), electAlias)
	}
	return nil
}

// writeGroupSwap writes the plan's new voters, under the shard lock, which is re-checked first: a
// compare-and-swap on the voters and the incarnation that the plan was decided on.
func writeGroupSwap(ctx context.Context, ts *topo.Server, keyspace, shard string, plan *groupSwapPlan, from, to []*topodatapb.TabletAlias) error {
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}
	_, err := ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		if !votersEqual(si.GroupReplicationVoters, from) || si.GroupReplicationIncarnation != plan.incarnation {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"the shard record of %s/%s changed concurrently (voters [%s], incarnation %q; expected voters [%s], incarnation %q): not writing the voters [%s]",
				keyspace, shard, votersString(si.GroupReplicationVoters), si.GroupReplicationIncarnation, votersString(from), plan.incarnation, votersString(to))
		}
		si.GroupReplicationVoters = to
		return nil
	})
	if err != nil {
		if vterrors.Code(err) == vtrpcpb.Code_FAILED_PRECONDITION {
			return err
		}
		return vterrors.Wrapf(err, "failed to store the voters of shard %s/%s", keyspace, shard)
	}
	return nil
}

// swapInElect writes the plan's new voters and makes the elect join the group: StartGroupReplication returns
// once its MySQL is ONLINE, within the replica wait timeout. ev.ShardInfo then lists the new voters.
func (pr *PlannedReparenter) swapInElect(ctx context.Context, ev *events.Reparent, keyspace, shard string, plan *groupSwapPlan, opts PlannedReparentOptions) error {
	electAlias := topoproto.TabletAliasString(plan.elect.Alias)
	if err := writeGroupSwap(ctx, pr.ts, keyspace, shard, plan, plan.voters, plan.newVoters); err != nil {
		return err
	}
	ev.ShardInfo.GroupReplicationVoters = plan.newVoters
	if plan.replaced != nil {
		pr.logger.Infof("swapped primary-elect %v in as a voter for %v: voters [%s]", electAlias, topoproto.TabletAliasString(plan.replaced), votersString(plan.newVoters))
	} else {
		pr.logger.Infof("added primary-elect %v as a voter: voters [%s]", electAlias, votersString(plan.newVoters))
	}
	event.DispatchUpdate(ev, "making the primary-elect join the replication group")
	joinCtx, joinCancel := context.WithTimeout(ctx, opts.WaitReplicasTimeout)
	defer joinCancel()
	if _, err := pr.tmc.StartGroupReplication(joinCtx, plan.elect, &tabletmanagerdatapb.StartGroupReplicationRequest{}); err != nil {
		return vterrors.Wrapf(err, "primary-elect %v, now a voter, failed to join the replication group in time", electAlias)
	}
	return nil
}

// swapAfterDemote swaps the elect in for the demoted current primary (see planGroupReplicationSwap): it waits
// until the voters of the new list hold what the demoted primary executed (waitForNewVotersToHold), checks the
// swap again on that read, writes the new voters, and makes the elect join the group. On a failure after
// the write, if the elect is not ONLINE in the group after all, it stops the elect's join and writes the old
// voters back (a compare-and-swap on the new ones), so
// that the caller can undo the demotion of a primary that is a voter again, unless a member's view of the
// shard's group lacks a majority of the new voters and holds one of the old ones: a voter left the group
// meanwhile, and the old list would give that view a majority it does not have under the new one (the TLA+
// model's prs_swap_norevertcheck). The new list then stays: the old primary is not a voter and does not serve,
// and VTOrc moves the group primary to a voter (GroupPrimaryNotVoter). It returns whether the old voters are
// listed again.
func (pr *PlannedReparenter) swapAfterDemote(ctx context.Context, ev *events.Reparent, keyspace, shard string, plan *groupSwapPlan,
	currentPrimary *topodatapb.Tablet, demotedPosition string, opts PlannedReparentOptions,
) (reverted bool, err error) {
	statuses, err := pr.waitForNewVotersToHold(ctx, plan, demotedPosition, opts)
	if err != nil {
		return true, err
	}
	if err := checkGroupSwap(plan, statuses, currentPrimary); err != nil {
		return true, err
	}
	err = pr.swapInElect(ctx, ev, keyspace, shard, plan, opts)
	if err == nil || !votersEqual(ev.ShardInfo.GroupReplicationVoters, plan.newVoters) {
		return err != nil, err
	}
	electAlias := topoproto.TabletAliasString(plan.elect.Alias)
	// The join's RPC failed, but the join may have completed: the elect, a voter now, may even be the group's
	// primary already, if the old primary failed meanwhile. The reparent then goes on.
	if electOnlineInGroup(plan, fetchFullStatus(ctx, pr.tmc, plan.elect, topo.RemoteOperationTimeout)) {
		pr.logger.Warningf("the join of primary-elect %v returned an error, but its MySQL is ONLINE in the shard's group: going on (%v)", electAlias, err)
		return false, nil
	}
	undoCtx, undoCancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer undoCancel()
	if _, stopErr := pr.tmc.StopGroupReplication(undoCtx, plan.elect); stopErr != nil {
		pr.logger.Warningf("failed to stop the join of primary-elect %v: %v", electAlias, stopErr)
	}
	if reason := swapRevertRefusal(plan, fetchFullStatuses(ctx, pr.tmc, plan.tablets, topo.RemoteOperationTimeout)); reason != "" {
		pr.logger.Warningf("not writing the old voters [%s] back: %s; the old primary %v is not a voter, and VTOrc moves the group primary to a voter",
			votersString(plan.voters), reason, topoproto.TabletAliasString(currentPrimary.Alias))
		return false, err
	}
	if revertErr := writeGroupSwap(ctx, pr.ts, keyspace, shard, plan, plan.newVoters, plan.voters); revertErr != nil {
		return false, vterrors.Wrapf(err, "and failed to write the old voters [%s] back: %v", votersString(plan.voters), revertErr)
	}
	ev.ShardInfo.GroupReplicationVoters = plan.voters
	return true, err
}

// swapRevertRefusal returns why the old voters of plan must not be written back over its new ones, or "": the
// elect's MySQL may still be in the group (its status cannot be read, it is an active member, or a START runs);
// or a reachable member's view of the recorded incarnation lacks a majority of the new voters but holds one of
// the old voters.
func swapRevertRefusal(plan *groupSwapPlan, statuses map[string]*fullStatusResult) string {
	current := legitimateGroup(plan.incarnation, plan.newVoters, statuses)
	old := legitimateGroup(plan.incarnation, plan.voters, statuses)
	electAlias := topoproto.TabletAliasString(plan.elect.Alias)
	if elect := statuses[electAlias]; elect == nil || elect.err != nil {
		return fmt.Sprintf("the status of primary-elect %v cannot be read: it may be in the group", electAlias)
	} else if mysql.IsGroupMemberActive(elect.groupStatus()) || elect.groupStatus().GetStartInProgress() {
		return fmt.Sprintf("the MySQL of primary-elect %v is still %s in a group", electAlias, elect.groupStatus().GetMemberState())
	}
	for _, alias := range slices.Sorted(maps.Keys(statuses)) {
		res := statuses[alias]
		if res.err != nil || !res.isActiveMember() || !current.IsLegitimateMember(res.groupStatus()) {
			continue
		}
		gs := res.groupStatus()
		if !current.HasVoterMajority(gs) && old.HasVoterMajority(gs) {
			return fmt.Sprintf("the view of %v holds %d of the %d new voters ONLINE, and would hold a majority of the old voters", alias, current.OnlineVoters(gs), len(plan.newVoters))
		}
	}
	return ""
}

// waitForNewVotersToHold waits, within the replica wait timeout, until the voters of the plan's new list hold,
// executed or received, every transaction that the demoted primary executed (demotedPosition), and returns the
// statuses of the last read. The demoted primary no longer commits, and the new list drops it: a bootstrap of
// the group from the new list, which needs every voter of it (GroupNotBootstrapped), must not lose an
// acknowledged transaction that only the demoted primary held (the TLA+ model's prs_swap_noholds). The elect,
// an asynchronous replica of the demoted primary that PRS caught up before the demotion, usually holds them
// already; the other voters receive them from the group.
func (pr *PlannedReparenter) waitForNewVotersToHold(ctx context.Context, plan *groupSwapPlan, demotedPosition string, opts PlannedReparentOptions) (map[string]*fullStatusResult, error) {
	position, err := replication.DecodePosition(demotedPosition)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to decode the position %q of the demoted primary", demotedPosition)
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, opts.WaitReplicasTimeout)
	defer waitCancel()
	for {
		statuses := fetchFullStatuses(waitCtx, pr.tmc, plan.tablets, topo.RemoteOperationTimeout)
		var held replication.GTIDSet = replication.Mysql56GTIDSet{}
		for _, voter := range plan.newVoters {
			if res := statuses[topoproto.TabletAliasString(voter)]; res != nil && res.err == nil {
				if _, all, err := GroupMemberGTIDSets(res.status); err == nil {
					held = held.Union(all)
				}
			}
		}
		if position.GTIDSet == nil || held.Contains(position.GTIDSet) {
			return statuses, nil
		}
		select {
		case <-waitCtx.Done():
			return nil, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED,
				"the voters of the new list [%s] do not hold every transaction that the demoted primary executed (%s), within %v: they lack %s",
				votersString(plan.newVoters), position.GTIDSet, opts.WaitReplicasTimeout, gtidSetDifference(position.GTIDSet, held))
		case <-time.After(swapHoldPollInterval):
		}
	}
}

// swapHoldPollInterval is how often waitForNewVotersToHold reads the voters' statuses again.
var swapHoldPollInterval = 100 * time.Millisecond

// electOnlineInGroup returns whether the elect's MySQL is ONLINE in a view of the shard's recorded incarnation.
func electOnlineInGroup(plan *groupSwapPlan, elect *fullStatusResult) bool {
	return elect.err == nil && elect.isOnlineMember() && policy.GroupIncarnation(elect.groupStatus().GetViewId()) == plan.incarnation
}

// withoutVoter returns the voters without the given one.
func withoutVoter(voters []*topodatapb.TabletAlias, removed *topodatapb.TabletAlias) []*topodatapb.TabletAlias {
	return slices.DeleteFunc(slices.Clone(voters), func(a *topodatapb.TabletAlias) bool { return topoproto.TabletAliasEqual(a, removed) })
}

// sortedAliases sorts the aliases by their string form, in place, and returns them.
func sortedAliases(aliases []*topodatapb.TabletAlias) []*topodatapb.TabletAlias {
	slices.SortFunc(aliases, func(a, b *topodatapb.TabletAlias) int {
		return strings.Compare(topoproto.TabletAliasString(a), topoproto.TabletAliasString(b))
	})
	return aliases
}
