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
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// VoterAction is a change that VTOrc makes to the voters of a shard's replication group, or the move
// of the group primary to a voter.
type VoterAction string

const (
	// VoterActionNone changes nothing.
	VoterActionNone VoterAction = ""
	// VoterActionInitial writes the first voter list of a shard that has none, and no active member.
	VoterActionInitial VoterAction = "InitialVoters"
	// VoterActionSwap gives the seat of a failed voter to a spare of its cell.
	VoterActionSwap VoterAction = "SwapVoter"
	// VoterActionGrow gives a seat to a spare of a cell that has no voter.
	VoterActionGrow VoterAction = "GrowVoter"
	// VoterActionRemove removes a voter whose tablet record was deleted, and whose cell has no spare.
	// With RemoveVoterNoGroup, it is the only change that shrinks the list.
	VoterActionRemove VoterAction = "RemoveVoter"
	// VoterActionRemoveNoGroup removes a voter whose tablet record was deleted while no group runs, so
	// that the group can be bootstrapped from the other voters (GroupNotBootstrapped needs every voter
	// reachable).
	VoterActionRemoveNoGroup VoterAction = "RemoveVoterNoGroup"
	// VoterActionMovePrimary moves the primary of the shard's group, which is not a voter, to a voter.
	// The list does not change.
	VoterActionMovePrimary VoterAction = "MoveGroupPrimaryToVoter"
)

// ChangesVoters returns whether the action writes a new voter list.
func (a VoterAction) ChangesVoters() bool {
	switch a {
	case VoterActionInitial, VoterActionSwap, VoterActionGrow, VoterActionRemove, VoterActionRemoveNoGroup:
		return true
	}
	return false
}

// VoterTablet is what VTOrc knows about a tablet of a shard, with a tablet record, when it plans a
// change of the shard's voters.
type VoterTablet struct {
	Tablet *topodatapb.Tablet
	// Reachable is true when the tablet answered, and Status is then its MySQL's group replication
	// status. A member's view lists the members it sees as ONLINE or RECOVERING, with their state.
	Reachable bool
	Status    *replicationdatapb.GroupReplicationStatus
	// ServerUUID is the server_uuid of the tablet's MySQL: as the tablet reported it, or as VTOrc
	// last saw it.
	ServerUUID string
	// LastActive is true when an unreachable tablet's MySQL was an active member when VTOrc last
	// reached it.
	LastActive bool
	// UnreachableFor is how long VTOrc has not reached the tablet.
	UnreachableFor time.Duration
	// Executed is the GTID set that the tablet's MySQL executed, when it is known.
	Executed replication.GTIDSet
}

// DeletedVoter is a voter whose tablet record no longer exists.
type DeletedVoter struct {
	Alias *topodatapb.TabletAlias
	// Tablet is the last tablet record of the voter that VTOrc kept, and ServerUUID the server_uuid of
	// its MySQL that VTOrc last saw: nil and "" when VTOrc does not know them.
	Tablet     *topodatapb.Tablet
	ServerUUID string
	// Down is true when VTOrc knows that the voter's vttablet is down: it probed it at the address it
	// kept, the probe failed, and VTOrc's discovery last reached it at least the grace period ago. A
	// deleted voter that VTOrc has no address for, did not probe, or reached recently, is not down: it
	// may be running, and hold transactions that the other voters lack.
	Down bool
}

// VoterPlanInput is the state of a shard from which PlanGroupVoters decides.
type VoterPlanInput struct {
	Durability policy.GroupReplicationDurabler
	// Voters and Incarnation are the shard record's voter list (V) and incarnation (R).
	Voters      []*topodatapb.TabletAlias
	Incarnation string
	// Tablets are the shard's tablets that have a tablet record.
	Tablets []*VoterTablet
	// DeletedVoters are the voters whose tablet record no longer exists, by alias.
	DeletedVoters map[string]*DeletedVoter
	// BootstrapIntentLive is true when the shard record holds a bootstrap intent younger than
	// reparentutil.GroupReplicationBootstrapIntentFence. Only the recovery reads it.
	BootstrapIntentLive bool
	// GracePeriod is --group-replication-voter-replacement-grace-period: a voter that is unreachable
	// for that long gets swapped.
	GracePeriod time.Duration
	// Fresh is true when the statuses were read under the shard lock, right before the write (the
	// recovery). The settled check then reads primary_election_in_progress, and the spare check
	// compares the executed GTID sets. VTOrc's stored state (the analysis) has neither: it only
	// proposes, and the recovery decides.
	Fresh bool
}

// VoterPlan is what PlanGroupVoters decided.
type VoterPlan struct {
	Action VoterAction
	// Voters is the new voter list, sorted by alias, for an action that changes it.
	Voters []*topodatapb.TabletAlias
	// Removed is the voter that the action drops (SwapVoter, RemoveVoter), and Added the spare that
	// it adds (SwapVoter, GrowVoter, which joins the group after the write).
	Removed *topodatapb.TabletAlias
	Added   *topodatapb.Tablet
	// GroupPrimary is the settled primary p of a list change, or the primary that MoveGroupPrimaryToVoter
	// moves away from: one that is not a voter, or a voter whose tablet record was deleted, of which only
	// the alias is known.
	GroupPrimary *topodatapb.Tablet
	// PrimaryRecordDeleted is set when MoveGroupPrimaryToVoter moves the primary away from a voter whose
	// tablet record was deleted, and ViewFrom is then a reachable member of the primary's view.
	PrimaryRecordDeleted bool
	ViewFrom             *topodatapb.TabletAlias
	// Alert is the alert that VTOrc reports when it changes nothing: GroupVoterRecordDeleted,
	// GroupVoterUnreplaceable or GroupVotersBelowTarget.
	Alert AnalysisCode
	// Reason says why the action or the alert, or why nothing changes.
	Reason string
}

// deletedVoterAdvice tells the operator how to end GroupVoterRecordDeleted. A deleted voter that runs
// cannot be part of a bootstrap either, which needs the tablet records of every voter.
const deletedVoterAdvice = "it keeps its seat; stop its vttablet and MySQL to let VTOrc replace or remove it, or restart its vttablet, which records the tablet again"

// voterPlanner holds what PlanGroupVoters derives from its input.
type voterPlanner struct {
	in         *VoterPlanInput
	byAlias    map[string]*VoterTablet
	legitimate *policy.LegitimateGroup
	// active are the server_uuids that the reachable members of the shard's group (the recorded
	// incarnation) report ONLINE or RECOVERING, theirs included.
	active map[string]bool
	// answering are the server_uuids of the tablets that answered.
	answering map[string]bool
}

// PlanGroupVoters decides the change that VTOrc makes to the voters of a shard's replication group,
// or the alert it reports instead. Every change is decided on one read of the shard, then written
// with a compare-and-swap on the voter list and the incarnation (see the design's "Voters"):
//
//   - InitialVoters: no voter is listed and no member is active: SelectVoters picks one voter per cell,
//     at least policy.MinGroupReplicationCells of them (GroupVotersBelowTarget otherwise).
//   - MoveGroupPrimaryToVoter: the primary of the shard's legitimate group is not a voter. It does not
//     serve; VTOrc moves the group primary to an ONLINE voter of its view.
//   - SwapVoter(v, x): voter v failed (unreachable for the grace period, or its tablet record was
//     deleted) and x is a spare of its cell. It needs P1 (a settled legitimate primary p other than v),
//     P2 (v's MySQL is active in no view) and P3 (x is a valid spare).
//   - RemoveVoter(v): v's tablet record was deleted and its cell has no spare. It needs P1, P2, and v
//     in no view of p. It is the only change that shrinks the list.
//   - RemoveVoterNoGroup(v): v's tablet record was deleted while no group runs: no reachable tablet is an
//     active member of any incarnation or runs a START GROUP_REPLICATION, no bootstrap intent is live,
//     and v's vttablet does not answer (P2): a voter that answers would be part of the bootstrap. The group is then bootstrapped from the other voters (GroupNotBootstrapped), which needs
//     every voter reachable. The operator accepts the loss of the transactions that only v held, as
//     with a forced EmergencyReparentShard. At least one voter stays.
//   - GrowVoter(x): a cell with an eligible tablet has no voter. It needs P1, P3, and a majority of
//     the grown list among the voters ONLINE in p's view.
//
// When none applies, it reports, in this order: GroupVoterRecordDeleted (a voter whose tablet record
// was deleted is still active), GroupVoterUnreplaceable (a failed voter has no valid spare) and
// GroupVotersBelowTarget (fewer voters than cells with an eligible tablet, or fewer than
// policy.MinGroupReplicationCells).
func PlanGroupVoters(in *VoterPlanInput) *VoterPlan {
	p := newVoterPlanner(in)
	if len(in.Voters) == 0 {
		return p.planInitial()
	}
	if plan := p.planMovePrimary(); plan != nil {
		return plan
	}
	if plan := p.planRemoveNoGroup(); plan != nil {
		return plan
	}
	var alerts []*VoterPlan
	primary, notSettled := p.settledPrimary()

	// Voters whose tablet record was deleted: the operator's signal to replace or remove them.
	for _, alias := range sortedAliasKeys(in.DeletedVoters) {
		voter, err := topoproto.ParseTabletAlias(alias)
		if err != nil {
			continue
		}
		uuid := in.DeletedVoters[alias].ServerUUID
		if reason := p.activeAnywhere(alias, uuid); reason != "" {
			alerts = append(alerts, &VoterPlan{
				Alert:  GroupVoterRecordDeleted,
				Reason: fmt.Sprintf("voter %s has no tablet record, but %s: %s", alias, reason, deletedVoterAdvice),
			})
			continue
		}
		if primary == nil {
			continue
		}
		if spare, rejected := p.spare(voter.GetCell(), voter, primary); spare != nil {
			return p.swap(voter, spare, primary, fmt.Sprintf("voter %s has no tablet record and its MySQL is active in no view", alias))
		} else if reason := p.inPrimaryView(primary, alias, uuid); reason != "" {
			alerts = append(alerts, &VoterPlan{
				Alert:  GroupVoterRecordDeleted,
				Reason: fmt.Sprintf("voter %s has no tablet record and no spare in its cell (%s), but %s: %s", alias, rejected, reason, deletedVoterAdvice),
			})
		} else {
			voters := withoutAlias(in.Voters, voter)
			return &VoterPlan{
				Action: VoterActionRemove, Voters: voters, Removed: voter, GroupPrimary: primary.Tablet,
				Reason: fmt.Sprintf("voter %s has no tablet record, its MySQL is active in no view, and its cell has no spare (%s): removing it leaves %d voters",
					alias, rejected, len(voters)),
			}
		}
	}

	// Voters that failed: unreachable for the grace period.
	for _, vt := range p.sortedTablets() {
		alias := topoproto.TabletAliasString(vt.Tablet.Alias)
		if !policy.IsVoter(in.Voters, vt.Tablet.Alias) || vt.Reachable || vt.UnreachableFor < in.GracePeriod {
			continue
		}
		if primary == nil {
			continue
		}
		if reason := p.activeAnywhere(alias, vt.ServerUUID); reason != "" {
			// Its vttablet is down, but its MySQL is still in the group: it keeps its seat.
			continue
		}
		spare, rejected := p.spare(vt.Tablet.Alias.Cell, vt.Tablet.Alias, primary)
		if spare == nil {
			alerts = append(alerts, &VoterPlan{
				Alert:  GroupVoterUnreplaceable,
				Reason: fmt.Sprintf("voter %s has been unreachable for %v and its MySQL is active in no view, but its cell has no valid spare (%s)", alias, vt.UnreachableFor.Round(time.Second), rejected),
			})
			continue
		}
		return p.swap(vt.Tablet.Alias, spare, primary, fmt.Sprintf("voter %s has been unreachable for %v and its MySQL is active in no view", alias, vt.UnreachableFor.Round(time.Second)))
	}

	// Cells with an eligible tablet and no voter.
	missing := p.cellsWithoutVoter()
	for _, cell := range missing {
		if primary == nil {
			break
		}
		if len(in.Voters)+1 > policy.MaxGroupReplicationMembers {
			alerts = append(alerts, &VoterPlan{
				Alert:  GroupVotersBelowTarget,
				Reason: fmt.Sprintf("cell %s has an eligible tablet but no voter, and the group has %d voters already, the most MySQL allows", cell, len(in.Voters)),
			})
			break
		}
		spare, rejected := p.spare(cell, nil, primary)
		if spare == nil {
			alerts = append(alerts, &VoterPlan{
				Alert:  GroupVotersBelowTarget,
				Reason: fmt.Sprintf("cell %s has no voter, and no valid spare (%s)", cell, rejected),
			})
			continue
		}
		grown := len(in.Voters) + 1
		if online := p.legitimate.OnlineVoters(primary.Status); online < grown/2+1 {
			alerts = append(alerts, &VoterPlan{
				Alert: GroupVotersBelowTarget,
				Reason: fmt.Sprintf("cell %s has no voter; %s could take the seat, but the view of the primary %s holds %d voters ONLINE, fewer than a majority of the %d voters it would have",
					cell, topoproto.TabletAliasString(spare.Tablet.Alias), topoproto.TabletAliasString(primary.Tablet.Alias), online, grown),
			})
			continue
		}
		voters := sortAliases(append(slices.Clone(in.Voters), spare.Tablet.Alias))
		return &VoterPlan{
			Action: VoterActionGrow, Voters: voters, Added: spare.Tablet, GroupPrimary: primary.Tablet,
			Reason: fmt.Sprintf("cell %s has an eligible tablet but no voter", cell),
		}
	}
	if len(missing) > 0 && primary == nil {
		alerts = append(alerts, &VoterPlan{
			Alert:  GroupVotersBelowTarget,
			Reason: fmt.Sprintf("cells %s have an eligible tablet but no voter, and %s", strings.Join(missing, ", "), notSettled),
		})
	}
	if len(in.Voters) < policy.MinGroupReplicationCells {
		alerts = append(alerts, &VoterPlan{
			Alert: GroupVotersBelowTarget,
			Reason: fmt.Sprintf("the group has %d voters ([%s]); with one voter per cell, it needs eligible tablets in at least %d cells to keep a majority when one voter fails",
				len(in.Voters), aliasesString(in.Voters), policy.MinGroupReplicationCells),
		})
	}

	for _, code := range []AnalysisCode{GroupVoterRecordDeleted, GroupVoterUnreplaceable, GroupVotersBelowTarget} {
		for _, alert := range alerts {
			if alert.Alert == code {
				return alert
			}
		}
	}
	if primary == nil {
		return &VoterPlan{Reason: notSettled}
	}
	return &VoterPlan{}
}

func newVoterPlanner(in *VoterPlanInput) *voterPlanner {
	p := &voterPlanner{
		in:        in,
		byAlias:   make(map[string]*VoterTablet, len(in.Tablets)),
		active:    make(map[string]bool),
		answering: make(map[string]bool),
	}
	tablets := make(map[string]*topodatapb.Tablet, len(in.Tablets))
	uuids := make(map[string]string, len(in.Tablets)+len(in.DeletedVoters))
	for _, vt := range in.Tablets {
		alias := topoproto.TabletAliasString(vt.Tablet.Alias)
		p.byAlias[alias] = vt
		tablets[alias] = vt.Tablet
		uuids[alias] = vt.ServerUUID
		if vt.Reachable && vt.ServerUUID != "" {
			p.answering[vt.ServerUUID] = true
		}
	}
	for alias, dv := range in.DeletedVoters {
		uuids[alias] = dv.ServerUUID
		if dv.Tablet != nil {
			tablets[alias] = dv.Tablet
		}
	}
	p.legitimate = policy.NewLegitimateGroup(in.Incarnation, in.Voters, tablets, uuids)
	for _, vt := range in.Tablets {
		if !vt.Reachable || !p.legitimate.IsLegitimateMember(vt.Status) {
			continue
		}
		if vt.ServerUUID != "" {
			p.active[vt.ServerUUID] = true
		}
		for _, uuid := range ActiveGroupMemberUUIDs(vt.Status) {
			p.active[uuid] = true
		}
	}
	return p
}

// planInitial selects the first voters of a shard, while no member is active.
func (p *voterPlanner) planInitial() *VoterPlan {
	for _, vt := range p.sortedTablets() {
		if (vt.Reachable && mysql.IsGroupMemberActive(vt.Status)) || (!vt.Reachable && vt.LastActive) {
			return &VoterPlan{
				Alert:  GroupVotersBelowTarget,
				Reason: fmt.Sprintf("no voter is listed, but %s is an active group member: VTOrc only selects the voters of a shard without a group", topoproto.TabletAliasString(vt.Tablet.Alias)),
			}
		}
	}
	candidates := make([]policy.VoterCandidate, 0, len(p.in.Tablets))
	for _, vt := range p.in.Tablets {
		candidates = append(candidates, policy.VoterCandidate{Tablet: vt.Tablet, Failed: !vt.Reachable && vt.UnreachableFor >= p.in.GracePeriod})
	}
	voters := policy.SelectVoters(p.in.Durability, nil, nil, candidates)
	if len(voters) < policy.MinGroupReplicationCells {
		cells := make([]string, 0, len(voters))
		for _, voter := range voters {
			cells = append(cells, voter.Cell)
		}
		return &VoterPlan{
			Alert: GroupVotersBelowTarget,
			Reason: fmt.Sprintf("the durability policy selects %d voters ([%s]), one per cell with a PRIMARY or REPLICA tablet that has not failed (cells [%s]); "+
				"the shard needs them in at least %d cells, so that its replication group keeps a majority when one voter fails",
				len(voters), aliasesString(voters), strings.Join(cells, ", "), policy.MinGroupReplicationCells),
		}
	}
	return &VoterPlan{Action: VoterActionInitial, Voters: voters, Reason: "no voter is listed and no member is active"}
}

// planRemoveNoGroup returns RemoveVoterNoGroup for the first voter whose tablet record was deleted,
// while no group runs. Every other listed voter must answer, in no group and without a START
// GROUP_REPLICATION: a voter that does not answer may run a group, and the bootstrap that follows
// needs it anyway. A reachable tablet that is an active member, of any incarnation, or that runs a
// START GROUP_REPLICATION, or a live bootstrap intent, means that a group may run or be starting.
func (p *voterPlanner) planRemoveNoGroup() *VoterPlan {
	if len(p.in.DeletedVoters) == 0 || len(p.in.Voters) < 2 || p.in.BootstrapIntentLive {
		return nil
	}
	for _, vt := range p.in.Tablets {
		if vt.Reachable && (mysql.IsGroupMemberActive(vt.Status) || vt.Status.GetStartInProgress()) {
			return nil
		}
	}
	for _, voter := range p.in.Voters {
		alias := topoproto.TabletAliasString(voter)
		if _, deleted := p.in.DeletedVoters[alias]; deleted {
			continue
		}
		if vt := p.byAlias[alias]; vt == nil || !vt.Reachable {
			return nil
		}
	}
	for _, alias := range sortedAliasKeys(p.in.DeletedVoters) {
		voter, err := topoproto.ParseTabletAlias(alias)
		if err != nil || p.activeAnywhere(alias, p.in.DeletedVoters[alias].ServerUUID) != "" {
			continue
		}
		voters := withoutAlias(p.in.Voters, voter)
		return &VoterPlan{
			Action: VoterActionRemoveNoGroup, Voters: voters, Removed: voter,
			Reason: fmt.Sprintf("voter %s has no tablet record and no group runs: removing it leaves %d voters, from which the group is bootstrapped", alias, len(voters)),
		}
	}
	return nil
}

// planMovePrimary returns MoveGroupPrimaryToVoter when the primary of the shard's legitimate group
// is not a voter.
func (p *voterPlanner) planMovePrimary() *VoterPlan {
	for _, vt := range p.sortedTablets() {
		if !vt.Reachable || policy.IsVoter(p.in.Voters, vt.Tablet.Alias) || !p.legitimate.IsLegitimatePrimary(vt.Status) {
			continue
		}
		return &VoterPlan{
			Action: VoterActionMovePrimary, GroupPrimary: vt.Tablet,
			Reason: topoproto.TabletAliasString(vt.Tablet.Alias) + ", which is not a voter, is the primary of the shard's replication group",
		}
	}
	// A primary whose tablet record was deleted (DeleteTablets --allow-primary) keeps serving and
	// acknowledging: the deletion does not stop it, and a removal after it crashed would lose those
	// acknowledgements. Its MySQL is found as the primary that the reachable members of its view
	// report: by its server_uuid, or, when VTOrc does not know it, as a primary that is the MySQL of
	// no tablet with a record.
	for _, vt := range p.sortedTablets() {
		st := vt.Status
		if !vt.Reachable || !p.legitimate.IsLegitimateMember(st) || !st.GetHasQuorum() || st.GetPrimaryUuid() == "" || !p.legitimate.HasVoterMajority(st) {
			continue
		}
		primaryUUID := st.GetPrimaryUuid()
		for _, alias := range sortedAliasKeys(p.in.DeletedVoters) {
			uuid := p.in.DeletedVoters[alias].ServerUUID
			if uuid != primaryUUID && (uuid != "" || p.recordedUUID(primaryUUID)) {
				continue
			}
			voter, err := topoproto.ParseTabletAlias(alias)
			if err != nil {
				continue
			}
			return &VoterPlan{
				Action: VoterActionMovePrimary, GroupPrimary: &topodatapb.Tablet{Alias: voter}, PrimaryRecordDeleted: true, ViewFrom: vt.Tablet.Alias,
				Reason: fmt.Sprintf("the primary of the shard's replication group (%s, as %s reports it) is voter %s, whose tablet record was deleted",
					primaryUUID, topoproto.TabletAliasString(vt.Tablet.Alias), alias),
			}
		}
	}
	return nil
}

// recordedUUID returns whether the server_uuid is the MySQL of a tablet that has a tablet record.
func (p *voterPlanner) recordedUUID(uuid string) bool {
	for _, vt := range p.in.Tablets {
		if vt.ServerUUID == uuid {
			return true
		}
	}
	return false
}

// settledPrimary returns the settled legitimate primary p (P1): a voter whose MySQL is the ONLINE
// primary, with quorum, of a view of the recorded incarnation, whose election ended, and whose view
// holds a majority of the voters ONLINE. Otherwise it returns why there is none.
func (p *voterPlanner) settledPrimary() (*VoterTablet, string) {
	if p.in.Incarnation == "" {
		return nil, "the shard record lists no incarnation of its group"
	}
	for _, vt := range p.sortedTablets() {
		if !vt.Reachable || !policy.IsVoter(p.in.Voters, vt.Tablet.Alias) || !mysql.IsGroupPrimary(vt.Status) {
			continue
		}
		alias := topoproto.TabletAliasString(vt.Tablet.Alias)
		if incarnation := policy.GroupIncarnation(vt.Status.GetViewId()); incarnation != p.in.Incarnation {
			return nil, fmt.Sprintf("the group primary %s is in incarnation %q, not the recorded %q", alias, incarnation, p.in.Incarnation)
		}
		if p.in.Fresh && vt.Status.GetPrimaryElectionInProgress() {
			return nil, fmt.Sprintf("the election of the group primary %s is in progress", alias)
		}
		if online := p.legitimate.OnlineVoters(vt.Status); online < p.legitimate.VoterMajority() {
			return nil, fmt.Sprintf("the view of the group primary %s holds %d of the %d voters ONLINE, not a majority", alias, online, len(p.in.Voters))
		}
		return vt, ""
	}
	return nil, "no voter is the reachable primary of the shard's replication group"
}

// groupVoter returns how the voter is found in a membership view: by the server_uuid of its MySQL, or
// by the MySQL address of its tablet record, as P1's voter majority finds it.
func (p *voterPlanner) groupVoter(alias, uuid string) policy.GroupVoter {
	for _, voter := range p.legitimate.Voters {
		if topoproto.TabletAliasString(voter.Alias) == alias {
			if voter.ServerUUID == "" {
				voter.ServerUUID = uuid
			}
			return voter
		}
	}
	return policy.GroupVoter{ServerUUID: uuid}
}

// activeAnywhere returns why the voter's MySQL may be active in a view of the shard's group (not P2),
// or "" when it is active in none: its tablet does not answer, and no reachable member of the
// recorded incarnation reports it ONLINE or RECOVERING, by its server_uuid or its MySQL address. When
// VTOrc does not know its server_uuid, every member that a reachable member reports active must be
// the MySQL of a tablet that answers.
func (p *voterPlanner) activeAnywhere(alias, uuid string) string {
	if vt := p.byAlias[alias]; vt != nil && vt.Reachable {
		return "its tablet answers"
	}
	if dv := p.in.DeletedVoters[alias]; dv != nil && !dv.Down {
		return "VTOrc cannot tell that its vttablet is down (it answers, VTOrc reached it within the grace period, or VTOrc has no address for it)"
	}
	voter := p.groupVoter(alias, uuid)
	for _, vt := range p.sortedTablets() {
		if !vt.Reachable || !p.legitimate.IsLegitimateMember(vt.Status) {
			continue
		}
		for _, m := range vt.Status.GetMembers() {
			if (m.GetState() == mysql.GroupMemberStateOnline || m.GetState() == mysql.GroupMemberStateRecovering) && voter.Matches(m) {
				return fmt.Sprintf("a reachable member reports its MySQL (%s) active", m.GetMemberUuid())
			}
		}
	}
	if voter.ServerUUID != "" {
		return ""
	}
	for _, member := range slices.Sorted(maps.Keys(p.active)) {
		if !p.answering[member] {
			return fmt.Sprintf("its server_uuid is unknown, and the active member %s is the MySQL of no tablet that answers", member)
		}
	}
	return ""
}

// inPrimaryView returns why the voter may be in the view of the primary, by its server_uuid or its
// MySQL address, or "" when it is in none. When VTOrc does not know the voter's server_uuid, every member of the view must be the MySQL of a
// tablet that answers.
func (p *voterPlanner) inPrimaryView(primary *VoterTablet, alias, uuid string) string {
	voter := p.groupVoter(alias, uuid)
	for _, m := range primary.Status.GetMembers() {
		switch {
		case voter.Matches(m):
			return fmt.Sprintf("it is %s in the view of the primary %s", m.GetState(), topoproto.TabletAliasString(primary.Tablet.Alias))
		case voter.ServerUUID == "" && !p.answering[m.GetMemberUuid()]:
			return fmt.Sprintf("its server_uuid is unknown, and the member %s of the view of the primary %s is the MySQL of no tablet that answers",
				m.GetMemberUuid(), topoproto.TabletAliasString(primary.Tablet.Alias))
		}
	}
	return ""
}

// spare returns the valid spare (P3) of the cell that takes the seat of replaced (nil for a grow),
// the one with the highest member weight and then the lowest alias, or nil and why each tablet of
// the cell is not one. A valid spare is not a voter; the policy allows it as a voter and it is a
// REPLICA; it answers; its MySQL is not an active member, runs no START GROUP_REPLICATION and is in
// no other group; it executed nothing that the primary lacks; and no other voter of the new list is
// in its cell.
func (p *voterPlanner) spare(cell string, replaced *topodatapb.TabletAlias, primary *VoterTablet) (*VoterTablet, string) {
	base := p.in.Voters
	if replaced != nil {
		base = withoutAlias(base, replaced)
	}
	var rejected []string
	var spares []*VoterTablet
	for _, vt := range p.sortedTablets() {
		if vt.Tablet.Alias.Cell != cell || policy.IsVoter(p.in.Voters, vt.Tablet.Alias) {
			continue
		}
		reason := ""
		gs := vt.Status
		switch {
		case vt.Tablet.Type != topodatapb.TabletType_REPLICA || !p.in.Durability.IsGroupMember(vt.Tablet):
			reason = "not a REPLICA that the policy allows as a voter"
		case !vt.Reachable:
			reason = "unreachable"
		case mysql.IsGroupMemberActive(gs):
			reason = "an active group member"
		case gs.GetStartInProgress():
			reason = "a START GROUP_REPLICATION runs"
		case gs.GetPluginActive() && gs.GetMemberState() != "" && gs.GetMemberState() != mysql.GroupMemberStateOffline:
			reason = "in a group (" + gs.GetMemberState() + ")"
		case slices.ContainsFunc(base, func(voter *topodatapb.TabletAlias) bool { return voter.Cell == cell }):
			reason = "its cell has a voter"
		case p.in.Fresh && (vt.Executed == nil || primary.Executed == nil):
			reason = "its executed transactions, or the primary's, are unknown"
		case p.in.Fresh && !primary.Executed.Contains(vt.Executed):
			reason = fmt.Sprintf("it executed transactions that the primary lacks (%s; the primary %s)", vt.Executed.String(), primary.Executed.String())
		}
		if reason != "" {
			rejected = append(rejected, topoproto.TabletAliasString(vt.Tablet.Alias)+": "+reason)
			continue
		}
		spares = append(spares, vt)
	}
	if len(spares) == 0 {
		if len(rejected) == 0 {
			return nil, "no other tablet in the cell"
		}
		return nil, strings.Join(rejected, "; ")
	}
	slices.SortStableFunc(spares, func(a, b *VoterTablet) int {
		return cmp.Compare(p.in.Durability.MemberWeight(b.Tablet), p.in.Durability.MemberWeight(a.Tablet))
	})
	return spares[0], ""
}

// swap returns the SwapVoter plan that gives the seat of voter to spare.
func (p *voterPlanner) swap(voter *topodatapb.TabletAlias, spare, primary *VoterTablet, why string) *VoterPlan {
	voters := sortAliases(append(withoutAlias(p.in.Voters, voter), spare.Tablet.Alias))
	return &VoterPlan{
		Action: VoterActionSwap, Voters: voters, Removed: voter, Added: spare.Tablet, GroupPrimary: primary.Tablet,
		Reason: fmt.Sprintf("%s: %s takes its seat", why, topoproto.TabletAliasString(spare.Tablet.Alias)),
	}
}

// cellsWithoutVoter returns the sorted cells that have a tablet that the policy allows as a voter
// (a REPLICA or the PRIMARY) but no voter.
func (p *voterPlanner) cellsWithoutVoter() []string {
	tablets := make([]*topodatapb.Tablet, 0, len(p.in.Tablets))
	for _, vt := range p.in.Tablets {
		tablets = append(tablets, vt.Tablet)
	}
	var cells []string
	for _, cell := range policy.EligibleCells(p.in.Durability, tablets) {
		if !slices.ContainsFunc(p.in.Voters, func(voter *topodatapb.TabletAlias) bool { return voter.Cell == cell }) {
			cells = append(cells, cell)
		}
	}
	return cells
}

func (p *voterPlanner) sortedTablets() []*VoterTablet {
	sorted := slices.Clone(p.in.Tablets)
	slices.SortFunc(sorted, func(a, b *VoterTablet) int {
		return strings.Compare(topoproto.TabletAliasString(a.Tablet.Alias), topoproto.TabletAliasString(b.Tablet.Alias))
	})
	return sorted
}

func withoutAlias(aliases []*topodatapb.TabletAlias, removed *topodatapb.TabletAlias) []*topodatapb.TabletAlias {
	return sortAliases(slices.DeleteFunc(slices.Clone(aliases), func(a *topodatapb.TabletAlias) bool { return topoproto.TabletAliasEqual(a, removed) }))
}

func sortAliases(aliases []*topodatapb.TabletAlias) []*topodatapb.TabletAlias {
	slices.SortFunc(aliases, func(a, b *topodatapb.TabletAlias) int {
		return strings.Compare(topoproto.TabletAliasString(a), topoproto.TabletAliasString(b))
	})
	return aliases
}

func sortedAliasKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}
