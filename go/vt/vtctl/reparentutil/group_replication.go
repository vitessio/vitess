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
	"slices"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// fullStatusResult is the outcome of a FullStatus RPC on one tablet.
type fullStatusResult struct {
	tablet *topodatapb.Tablet
	status *replicationdatapb.FullStatus
	err    error
}

// groupStatus returns the Group Replication status of the tablet, or nil if the RPC failed
// or the tablet does not report one.
func (r *fullStatusResult) groupStatus() *replicationdatapb.GroupReplicationStatus {
	if r == nil || r.status == nil {
		return nil
	}
	return r.status.GroupReplicationStatus
}

// isActiveMember returns whether the tablet's MySQL is an ONLINE or RECOVERING member of a group.
func (r *fullStatusResult) isActiveMember() bool {
	return mysql.IsGroupMemberActive(r.groupStatus())
}

// isOnlineMember returns whether the tablet's MySQL is an ONLINE member of a group.
func (r *fullStatusResult) isOnlineMember() bool {
	gs := r.groupStatus()
	return mysql.IsGroupMemberActive(gs) && gs.MemberState == mysql.GroupMemberStateOnline
}

// isGroupPrimary returns whether the tablet's MySQL is the writable primary of a group with quorum.
func (r *fullStatusResult) isGroupPrimary() bool {
	return mysql.IsGroupPrimary(r.groupStatus())
}

// fetchFullStatuses runs FullStatus on the given tablets concurrently. Every RPC is bounded
// by timeout. The result has one entry per tablet, keyed by tablet alias; failed RPCs carry
// their error.
func fetchFullStatuses(ctx context.Context, tmc tmclient.TabletManagerClient, tablets []*topodatapb.Tablet, timeout time.Duration) map[string]*fullStatusResult {
	statusCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		results = make(map[string]*fullStatusResult, len(tablets))
	)
	for _, tablet := range tablets {
		wg.Add(1)
		go func(tablet *topodatapb.Tablet) {
			defer wg.Done()
			status, err := tmc.FullStatus(statusCtx, tablet)
			if err != nil {
				err = vterrors.Wrapf(err, "failed to get FullStatus of tablet %v", topoproto.TabletAliasString(tablet.Alias))
			}
			mu.Lock()
			defer mu.Unlock()
			results[topoproto.TabletAliasString(tablet.Alias)] = &fullStatusResult{tablet: tablet, status: status, err: err}
		}(tablet)
	}
	wg.Wait()
	return results
}

// fetchFullStatus runs FullStatus on one tablet, bounded by timeout.
func fetchFullStatus(ctx context.Context, tmc tmclient.TabletManagerClient, tablet *topodatapb.Tablet, timeout time.Duration) *fullStatusResult {
	return fetchFullStatuses(ctx, tmc, []*topodatapb.Tablet{tablet}, timeout)[topoproto.TabletAliasString(tablet.Alias)]
}

// memberIsOnlineInView returns whether the member with the given server_uuid is ONLINE in
// the membership view of the reporting member.
func memberIsOnlineInView(view *replicationdatapb.GroupReplicationStatus, serverUUID string) bool {
	if view == nil || serverUUID == "" {
		return false
	}
	for _, m := range view.Members {
		if m.MemberUuid == serverUUID {
			return m.State == mysql.GroupMemberStateOnline
		}
	}
	return false
}

// checkGroupReplicationPrimaryElect verifies that a planned reparent in a shard that runs
// Group Replication can promote the primary-elect: the elect must be an ONLINE member of the
// same group as the current primary. PromoteReplica then switches the group's primary with
// group_replication_set_as_primary; a tablet outside the group cannot be promoted while the
// group is active, because the group's members would keep following the old primary.
//
// The group must be the shard's legitimate group (see policy.LegitimateGroup): its view belongs
// to the incarnation recorded in the shard record, when one is recorded, and holds a majority of
// the listed voters. A planned reparent never moves the shard to a group that formed on its own.
//
// Only a voter, a tablet listed in the shard record's voters, can be promoted: the other
// tablets replicate asynchronously and are not members of the group. Swapping a non-voter in
// for a voter is not supported yet. An empty list (voters not selected yet) skips this check.
//
// A shard that never had a primary is initialized with InitPrimary, which bootstraps the
// group, so there is nothing to check. A shard whose primary is unknown can only promote an
// ONLINE member of the legitimate group.
//
// tablets are the tablets of the shard; the statuses of the voters among them identify the
// voters in the group's view.
func checkGroupReplicationPrimaryElect(ctx context.Context, tmc tmclient.TabletManagerClient, shardInitialized bool, incarnation string, voters []*topodatapb.TabletAlias, tablets []*topodatapb.Tablet, currentPrimary, primaryElect *topodatapb.Tablet) error {
	if primaryElect == nil || (currentPrimary != nil && topoproto.TabletAliasEqual(currentPrimary.Alias, primaryElect.Alias)) {
		return nil
	}
	if currentPrimary == nil && !shardInitialized {
		return nil
	}
	if len(voters) > 0 && !policy.IsVoter(voters, primaryElect.Alias) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v is not a voting member of the shard's replication group; only voters can be promoted (voters: %s). "+
				"Promoting a tablet that is not a voter is not supported yet",
			topoproto.TabletAliasString(primaryElect.Alias), votersString(voters))
	}

	// Read the current primary, the primary-elect and the voters.
	toRead := []*topodatapb.Tablet{primaryElect}
	if currentPrimary != nil {
		toRead = append(toRead, currentPrimary)
	}
	for _, tablet := range tablets {
		if policy.IsVoter(voters, tablet.Alias) && !slices.ContainsFunc(toRead, func(t *topodatapb.Tablet) bool { return topoproto.TabletAliasEqual(t.Alias, tablet.Alias) }) {
			toRead = append(toRead, tablet)
		}
	}
	statuses := fetchFullStatuses(ctx, tmc, toRead, topo.RemoteOperationTimeout)
	legitimate := legitimateGroup(incarnation, voters, statuses)
	electAlias := topoproto.TabletAliasString(primaryElect.Alias)
	electStatus := statuses[electAlias]

	if currentPrimary == nil {
		if electStatus.err != nil {
			return vterrors.Wrapf(electStatus.err, "cannot verify the group replication membership of primary-elect %v", electAlias)
		}
		gs := electStatus.groupStatus()
		if !electStatus.isOnlineMember() || !gs.HasQuorum {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"primary-elect %v is not an ONLINE member of a replication group with quorum; the shard has no current primary to compare with", electAlias)
		}
		if err := checkLegitimateGroupView(legitimate, gs, electAlias); err != nil {
			return err
		}
		return nil
	}
	primaryAlias := topoproto.TabletAliasString(currentPrimary.Alias)
	primaryStatus := statuses[primaryAlias]
	if primaryStatus.err != nil {
		return vterrors.Wrapf(primaryStatus.err, "cannot verify the group replication membership of current primary %v", primaryAlias)
	}
	if electStatus.err != nil {
		return vterrors.Wrapf(electStatus.err, "cannot verify the group replication membership of primary-elect %v", electAlias)
	}

	primaryGroup := primaryStatus.groupStatus()
	if !primaryStatus.isActiveMember() {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"current primary %v is not an active member of a replication group; the shard's group must be running before a planned reparent (see MigrateReplicationMode)", primaryAlias)
	}
	if err := checkLegitimateGroupView(legitimate, primaryGroup, primaryAlias); err != nil {
		return err
	}
	electGroup := electStatus.groupStatus()
	if !electStatus.isOnlineMember() {
		state := "not a member"
		if electGroup != nil && electGroup.PluginActive {
			state = electGroup.MemberState
		}
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v is not an ONLINE member of the replication group of current primary %v (state: %s); only an ONLINE member can be promoted while the group is active", electAlias, primaryAlias, state)
	}
	if electGroup.GroupName != primaryGroup.GroupName {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v is a member of group %s, but current primary %v is a member of group %s", electAlias, electGroup.GroupName, primaryAlias, primaryGroup.GroupName)
	}
	if policy.GroupIncarnation(electGroup.ViewId) != policy.GroupIncarnation(primaryGroup.ViewId) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v is in group incarnation %s, but current primary %v is in group incarnation %s", electAlias,
			policy.GroupIncarnation(electGroup.ViewId), primaryAlias, policy.GroupIncarnation(primaryGroup.ViewId))
	}
	if !memberIsOnlineInView(primaryGroup, electStatus.status.ServerUuid) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"primary-elect %v is not an ONLINE member in the group view of current primary %v", electAlias, primaryAlias)
	}
	return nil
}

// checkLegitimateGroupView returns a FAILED_PRECONDITION error when the member's view is not a
// view of the shard's legitimate group: another incarnation than the recorded one, or fewer than
// a majority of the listed voters ONLINE.
func checkLegitimateGroupView(legitimate *policy.LegitimateGroup, view *replicationdatapb.GroupReplicationStatus, alias string) error {
	if legitimate.IsForeignIncarnation(view) || !legitimate.IsLegitimateMember(view) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"%v is in group incarnation %q, but the shard record's group incarnation is %q: it is not a member of the shard's replication group",
			alias, policy.GroupIncarnation(view.GetViewId()), legitimate.Incarnation)
	}
	if !legitimate.HasVoterMajority(view) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"only %d of the %d voters of the shard are ONLINE in the group view of %v; a majority is needed",
			legitimate.OnlineVoters(view), len(legitimate.Voters), alias)
	}
	return nil
}

// legitimateGroup returns the shard's legitimate replication group from the shard record's
// incarnation and voters. The voters are identified in the members' views by the server_uuids
// that the statuses report, and by the MySQL addresses of their tablet records.
func legitimateGroup(incarnation string, voters []*topodatapb.TabletAlias, statuses map[string]*fullStatusResult) *policy.LegitimateGroup {
	tablets := make(map[string]*topodatapb.Tablet, len(statuses))
	uuids := make(map[string]string, len(statuses))
	for alias, res := range statuses {
		if res == nil {
			continue
		}
		tablets[alias] = res.tablet
		if res.err == nil && res.status != nil {
			uuids[alias] = res.status.ServerUuid
		}
	}
	return policy.NewLegitimateGroup(incarnation, voters, tablets, uuids)
}

// RecordGroupReplicationIncarnation records the incarnation of the group that the tablet's MySQL
// is an active member of as the shard's legitimate group incarnation. Callers use it right after
// they bootstrapped the shard's group on the tablet, while they still hold the shard lock, which
// is re-checked first. It returns the recorded incarnation.
func RecordGroupReplicationIncarnation(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, keyspace, shard string, tablet *topodatapb.Tablet) (string, error) {
	res := fetchFullStatus(ctx, tmc, tablet, topo.RemoteOperationTimeout)
	if res.err != nil {
		return "", vterrors.Wrapf(res.err, "cannot read the group incarnation of %v", topoproto.TabletAliasString(tablet.Alias))
	}
	gs := res.groupStatus()
	incarnation := policy.GroupIncarnation(gs.GetViewId())
	if !res.isActiveMember() || incarnation == "" {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the MySQL of %v is not an active member of a group with a view (state %s, view %q)",
			topoproto.TabletAliasString(tablet.Alias), gs.GetMemberState(), gs.GetViewId())
	}
	if err := WriteGroupReplicationIncarnation(ctx, ts, keyspace, shard, incarnation); err != nil {
		return "", err
	}
	return incarnation, nil
}

// WriteGroupReplicationIncarnation stores the incarnation of the shard's legitimate replication
// group in the shard record; an empty incarnation clears it. The caller must hold the shard lock,
// which is re-checked first.
func WriteGroupReplicationIncarnation(ctx context.Context, ts *topo.Server, keyspace, shard, incarnation string) error {
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}
	_, err := ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		if si.GroupReplicationIncarnation == incarnation {
			return topo.NewError(topo.NoUpdateNeeded, keyspace+"/"+shard)
		}
		si.GroupReplicationIncarnation = incarnation
		return nil
	})
	if err != nil {
		return vterrors.Wrapf(err, "failed to store the group replication incarnation of shard %s/%s", keyspace, shard)
	}
	return nil
}

// votersString returns the aliases of the voters, sorted and separated by commas.
func votersString(voters []*topodatapb.TabletAlias) string {
	aliases := make([]string, 0, len(voters))
	for _, v := range voters {
		aliases = append(aliases, topoproto.TabletAliasString(v))
	}
	slices.Sort(aliases)
	return strings.Join(aliases, ", ")
}

// votersEqual returns whether both lists hold the same voters, in any order.
func votersEqual(a, b []*topodatapb.TabletAlias) bool {
	return len(a) == len(b) && votersString(a) == votersString(b)
}

// writeGroupReplicationVoters stores the voting members of the shard's group in the shard
// record. The caller must hold the shard lock, which is re-checked first.
func writeGroupReplicationVoters(ctx context.Context, ts *topo.Server, keyspace, shard string, voters []*topodatapb.TabletAlias) error {
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}
	_, err := ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		if votersEqual(si.GroupReplicationVoters, voters) {
			return topo.NewError(topo.NoUpdateNeeded, keyspace+"/"+shard)
		}
		si.GroupReplicationVoters = voters
		return nil
	})
	if err != nil {
		return vterrors.Wrapf(err, "failed to store the group replication voters of shard %s/%s", keyspace, shard)
	}
	return nil
}

// isListedVoter returns whether the tablet is a voting member of the shard's group according
// to the shard record. Under a group replication policy an empty list means that no voters
// were selected yet; every tablet the policy allows in the group then counts as a voter, as
// before voter lists existed.
func isListedVoter(durability policy.Durabler, voters []*topodatapb.TabletAlias, tablet *topodatapb.Tablet) bool {
	if !policy.IsGroupMember(durability, tablet) {
		return false
	}
	return len(voters) == 0 || policy.IsVoter(voters, tablet.Alias)
}

// voterCandidates returns the tablets as candidates for policy.SelectVoters. A tablet is
// active when its status says it is an active member of a group; no tablet is failed, since
// the callers only select voters when every tablet is reachable.
func voterCandidates(tablets []*topodatapb.Tablet, statuses map[string]*fullStatusResult) []policy.VoterCandidate {
	candidates := make([]policy.VoterCandidate, 0, len(tablets))
	for _, tablet := range tablets {
		res := statuses[topoproto.TabletAliasString(tablet.Alias)]
		candidates = append(candidates, policy.VoterCandidate{Tablet: tablet, Active: res != nil && res.err == nil && res.isActiveMember()})
	}
	return candidates
}
