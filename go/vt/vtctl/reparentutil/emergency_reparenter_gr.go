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
	"cmp"
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// groupReplicationView is what ERS learned about a shard's replication group from the
// reachable tablets.
type groupReplicationView struct {
	// groupName is the group_replication_group_name all quorum members agree on.
	groupName string
	// primaryUUID is the server_uuid of the group's primary, as seen by the quorum members.
	primaryUUID string
	// view is the membership view of one quorum member. All quorum members agree on the
	// primary; the view is used to check that a candidate is ONLINE in the group.
	view *replicationdatapb.GroupReplicationStatus
	// primary is the reachable tablet whose MySQL is the group's primary, if any.
	primary *fullStatusResult
}

// findGroupWithQuorum looks for a reachable member of the shard's group whose view has
// quorum, and returns what those members agree on. It fails when no reachable member has
// quorum, or when the members that have quorum disagree about the group or its primary: ERS
// must be certain about which primary it reconciles the topology with.
func findGroupWithQuorum(statuses map[string]*fullStatusResult) (*groupReplicationView, error) {
	var gv *groupReplicationView
	for _, alias := range slices.Sorted(maps.Keys(statuses)) {
		res := statuses[alias]
		if res.err != nil || !res.isOnlineMember() {
			continue
		}
		gs := res.groupStatus()
		if !gs.HasQuorum || gs.PrimaryUuid == "" {
			continue
		}
		if gv == nil {
			gv = &groupReplicationView{groupName: gs.GroupName, primaryUUID: gs.PrimaryUuid, view: gs}
		} else if gv.groupName != gs.GroupName || gv.primaryUUID != gs.PrimaryUuid {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"the members of the replication group disagree about the group or its primary (%s/%s vs %s/%s at %v); the group membership is changing, retry EmergencyReparentShard",
				gv.groupName, gv.primaryUUID, gs.GroupName, gs.PrimaryUuid, alias)
		}
	}
	if gv == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE,
			"no reachable member of the replication group has quorum; the group cannot elect a primary. "+
				"Restore enough members for a majority; forcing a new quorum (group_replication_force_members) is not supported by EmergencyReparentShard")
	}
	for _, alias := range slices.Sorted(maps.Keys(statuses)) {
		res := statuses[alias]
		if res.err == nil && res.status.ServerUuid == gv.primaryUUID {
			if !res.isGroupPrimary() {
				return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
					"tablet %v is the primary of the replication group according to its members, but does not report itself as a primary with quorum; retry EmergencyReparentShard", alias)
			}
			gv.primary = res
		}
	}
	return gv, nil
}

// groupPromotionEligibility returns an error when the tablet cannot be the shard's primary
// after an emergency reparent of a group replication shard: it must be reachable, an ONLINE
// member of the group in the view of the quorum, allowed by the durability policy and, with
// PreventCrossCellPromotion, in the cell of the previous primary.
func groupPromotionEligibility(res *fullStatusResult, gv *groupReplicationView, prevPrimary *topodatapb.Tablet, opts EmergencyReparentOptions) error {
	alias := topoproto.TabletAliasString(res.tablet.Alias)
	if res.err != nil {
		return vterrors.Wrapf(res.err, "tablet %v is not reachable", alias)
	}
	if !res.isOnlineMember() || res.groupStatus().GroupName != gv.groupName || !memberIsOnlineInView(gv.view, res.status.ServerUuid) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "tablet %v is not an ONLINE member of the shard's replication group", alias)
	}
	if policy.PromotionRule(opts.durability, res.tablet) == promotionrule.MustNot {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "tablet %v must not be promoted according to the durability policy", alias)
	}
	if opts.PreventCrossCellPromotion && prevPrimary != nil && prevPrimary.Alias.Cell != res.tablet.Alias.Cell {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "tablet %v is not in the cell of the previous primary %v, and cross-cell promotion is prevented",
			alias, topoproto.TabletAliasString(prevPrimary.Alias))
	}
	return nil
}

// chooseGroupReplicationPrimary picks the tablet that becomes the shard's primary: the
// requested one if NewPrimaryAlias is set, otherwise the group's own primary, otherwise
// (when the group's primary is not eligible or its tablet is unreachable) the eligible
// ONLINE member with the highest member weight.
func chooseGroupReplicationPrimary(statuses map[string]*fullStatusResult, gv *groupReplicationView, prevPrimary *topodatapb.Tablet, opts EmergencyReparentOptions) (*fullStatusResult, error) {
	if opts.NewPrimaryAlias != nil {
		requested, ok := statuses[topoproto.TabletAliasString(opts.NewPrimaryAlias)]
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "requested primary %v is not a tablet of the shard, or is ignored",
				topoproto.TabletAliasString(opts.NewPrimaryAlias))
		}
		if err := groupPromotionEligibility(requested, gv, prevPrimary, opts); err != nil {
			return nil, vterrors.Wrapf(err, "requested primary %v cannot be promoted", topoproto.TabletAliasString(opts.NewPrimaryAlias))
		}
		return requested, nil
	}
	if gv.primary != nil && groupPromotionEligibility(gv.primary, gv, prevPrimary, opts) == nil {
		return gv.primary, nil
	}

	var candidates []*fullStatusResult
	for _, res := range statuses {
		if groupPromotionEligibility(res, gv, prevPrimary, opts) == nil {
			candidates = append(candidates, res)
		}
	}
	if len(candidates) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"the replication group's primary (server_uuid %s) cannot become the shard primary, and no other reachable ONLINE member is eligible", gv.primaryUUID)
	}
	weight := func(t *topodatapb.Tablet) int {
		if grd, ok := policy.AsGroupReplication(opts.durability); ok {
			return grd.MemberWeight(t)
		}
		return 0
	}
	slices.SortFunc(candidates, func(a, b *fullStatusResult) int {
		if c := cmp.Compare(weight(b.tablet), weight(a.tablet)); c != 0 {
			return c
		}
		return cmp.Compare(topoproto.TabletAliasString(a.tablet.Alias), topoproto.TabletAliasString(b.tablet.Alias))
	})
	return candidates[0], nil
}

// groupReplicationReplicaTimeout bounds the replica RPCs of the group replication path: the
// replica wait timeout, or the remote operation timeout if none was given.
func groupReplicationReplicaTimeout(opts EmergencyReparentOptions) time.Duration {
	if opts.WaitReplicasTimeout > 0 {
		return opts.WaitReplicasTimeout
	}
	return topo.RemoteOperationTimeout
}

// replicationWasRunning returns whether either replication thread of the tablet's default
// channel was running or connecting.
func replicationWasRunning(status *replicationdatapb.FullStatus) bool {
	if status == nil || status.ReplicationStatus == nil {
		return false
	}
	rs := status.ReplicationStatus
	return rs.SqlState == int32(replication.ReplicationStateRunning) ||
		rs.IoState == int32(replication.ReplicationStateRunning) ||
		rs.IoState == int32(replication.ReplicationStateConnecting)
}

// reparentShardLockedGroupReplication is the EmergencyReparentShard path of a shard whose
// durability policy uses MySQL Group Replication.
//
// The group elects a new primary on its own as long as a majority of its members survives,
// and a member that loses the majority cannot commit, so the members never diverge. ERS
// therefore does not stop replication or compare positions: it finds the primary the
// group's quorum agrees on and makes the topology follow it. The steps are:
//
//  1. Collect FullStatus from every tablet, bounded by WaitReplicasTimeout.
//  2. Find the quorum: reachable ONLINE members with quorum must agree on the group and its
//     primary. Without quorum ERS fails; forcing a new quorum needs an operator.
//  3. Choose the new primary: NewPrimaryAlias, the group's primary, or the best eligible
//     ONLINE member. The choice must be an ONLINE member of the group and pass the durability
//     policy and cross-cell checks.
//  4. PromoteReplica on it: a type change on the group's primary, or
//     group_replication_set_as_primary on another member. Then write the reparent journal.
//  5. Point every other reachable tablet at it: SetReplicationSource is a type fix on active
//     members and repoints the asynchronous replicas of the group, which include the
//     tablets that are not listed voters. Listed voters that are not active are left to
//     rejoin the group on their own.
//
// The shard lock is re-checked before each step that changes anything.
func (erp *EmergencyReparenter) reparentShardLockedGroupReplication(ctx context.Context, ev *events.Reparent, keyspace, shard string, prevPrimary *topodatapb.Tablet, opts EmergencyReparentOptions) error {
	if opts.AllowSplitBrainPromotion {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"--allow-split-brain-promotion does not apply to group replication: the members of a group cannot diverge")
	}

	event.DispatchUpdate(ev, "reading all tablets")
	tabletMap, err := erp.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return vterrors.Wrapf(err, "failed to get tablet map for %v/%v", keyspace, shard)
	}

	tablets := make([]*topodatapb.Tablet, 0, len(tabletMap))
	for alias, ti := range tabletMap {
		if opts.IgnoreReplicas.Has(alias) {
			continue
		}
		tablets = append(tablets, ti.Tablet)
	}

	event.DispatchUpdate(ev, "reading the group replication status of all tablets")
	statuses := fetchFullStatuses(ctx, erp.tmc, tablets, groupReplicationReplicaTimeout(opts))
	for alias, res := range statuses {
		if res.err != nil {
			erp.logger.Warningf("tablet %v is unreachable, it is left out of the emergency reparent: %v", alias, res.err)
		}
	}

	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}

	gv, err := findGroupWithQuorum(statuses)
	if err != nil {
		return err
	}
	newPrimary, err := chooseGroupReplicationPrimary(statuses, gv, prevPrimary, opts)
	if err != nil {
		return err
	}
	newPrimaryAlias := topoproto.TabletAliasString(newPrimary.tablet.Alias)
	switching := gv.primary == nil || gv.primary != newPrimary
	if switching {
		erp.logger.Infof("making ONLINE member %v the primary of the replication group (the group's primary has server_uuid %s)", newPrimaryAlias, gv.primaryUUID)
	} else {
		erp.logger.Infof("the replication group elected %v as its primary, updating the topology", newPrimaryAlias)
	}
	ev.NewPrimary = newPrimary.tablet.CloneVT()

	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}

	// PromoteReplica on the group's primary only changes the tablet type. On another
	// member it runs group_replication_set_as_primary, which waits for the member to apply
	// its backlog, so it gets the replica wait timeout.
	promoteTimeout := topo.RemoteOperationTimeout
	if switching {
		promoteTimeout = max(opts.WaitReplicasTimeout, topo.RemoteOperationTimeout)
	}
	promoteCtx, promoteCancel := context.WithTimeout(ctx, promoteTimeout)
	defer promoteCancel()

	event.DispatchUpdate(ev, "promoting the group replication primary")
	position, err := erp.tmc.PromoteReplica(promoteCtx, newPrimary.tablet, policy.SemiSyncAckers(opts.durability, newPrimary.tablet) > 0)
	if err != nil {
		return vterrors.Wrapf(err, "primary-elect tablet %v failed to be upgraded to primary", newPrimaryAlias)
	}
	erp.logger.Infof("populating reparent journal on new primary %v", newPrimaryAlias)
	if err := erp.tmc.PopulateReparentJournal(promoteCtx, newPrimary.tablet, time.Now().UnixNano(), opts.lockAction, newPrimary.tablet.Alias, position); err != nil {
		return vterrors.Wrapf(err, "failed to PopulateReparentJournal on primary %v", newPrimaryAlias)
	}

	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}

	event.DispatchUpdate(ev, "reparenting all tablets")
	erp.reparentGroupReplicationTablets(ctx, newPrimary.tablet, statuses, ev.ShardInfo.GroupReplicationVoters, opts)
	return nil
}

// reparentGroupReplicationTablets points the reachable tablets at the new primary. The new
// primary is already serving and the group provides durability, so a tablet that fails is
// logged and left to VTOrc rather than failing the reparent.
func (erp *EmergencyReparenter) reparentGroupReplicationTablets(ctx context.Context, newPrimary *topodatapb.Tablet, statuses map[string]*fullStatusResult, voters []*topodatapb.TabletAlias, opts EmergencyReparentOptions) {
	replCtx, replCancel := context.WithTimeout(ctx, groupReplicationReplicaTimeout(opts))
	defer replCancel()

	var wg sync.WaitGroup
	for alias, res := range statuses {
		switch {
		case topoproto.TabletAliasEqual(res.tablet.Alias, newPrimary.Alias):
			continue
		case res.err != nil:
			continue
		case !res.isActiveMember() && isListedVoter(opts.durability, voters, res.tablet):
			// A voter that is not in the group (for example the failed former primary)
			// must rejoin the group, not replicate asynchronously. Its tablet rejoins on
			// its own, or VTOrc makes it. A tablet that is not a listed voter is an
			// asynchronous replica of the group and is repointed below.
			erp.logger.Infof("tablet %v should be a member of the replication group but is not active; it is left to rejoin the group", alias)
			continue
		}
		wg.Add(1)
		go func(alias string, res *fullStatusResult) {
			defer wg.Done()
			// On an active member SetReplicationSource only fixes the tablet type. The
			// asynchronous replicas of the group are repointed; they received only
			// transactions the group certified, so they cannot be ahead of the group.
			forceStart := !res.isActiveMember() && replicationWasRunning(res.status)
			semiSync := policy.IsReplicaSemiSync(opts.durability, newPrimary, res.tablet)
			if err := erp.tmc.SetReplicationSource(replCtx, res.tablet, newPrimary.Alias, 0, "", forceStart, semiSync, 0); err != nil {
				erp.logger.Warningf("tablet %v failed to SetReplicationSource(%v): %v", alias, topoproto.TabletAliasString(newPrimary.Alias), err)
			}
		}(alias, res)
	}
	wg.Wait()
}
