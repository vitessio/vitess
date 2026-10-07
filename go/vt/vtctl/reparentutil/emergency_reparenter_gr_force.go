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
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// forcedGroupPlan is what a forced emergency reparent of a group replication shard decides on its read of
// the shard record and of every tablet's status.
type forcedGroupPlan struct {
	// incarnation and voters are what the shard record listed: the voter write is a compare-and-swap on
	// both, and the bootstrap expects the incarnation.
	incarnation string
	voters      []*topodatapb.TabletAlias
	// survivors are the voters that answered: the new voter list. joiners are their statuses, but the
	// candidate's.
	survivors []*topodatapb.TabletAlias
	joiners   []*fullStatusResult
	// dropped are the voters that did not answer, or have no tablet record.
	dropped []*topodatapb.TabletAlias
	// candidate is the surviving voter that the new group is bootstrapped on.
	candidate *fullStatusResult
	// required is every transaction that a surviving voter executed or received: the new group starts
	// from all of them (StartGroupReplicationRequest.required_gtid_set).
	required replication.GTIDSet
}

// forcedCandidate is a surviving voter with the transactions it executed, and those it executed or
// received.
type forcedCandidate struct {
	res           *fullStatusResult
	executed, all replication.GTIDSet
}

// planForcedGroup decides a forced emergency reparent (--group-replication-force-new-group) of a group
// replication shard whose group lost its majority. It drops the voters that do not answer, and chooses the
// surviving voter to bootstrap the new group on. It refuses, before anything changes, unless all of these
// hold on its read:
//   - the shard record lists a group incarnation and voters: a shard whose group was never recorded is
//     VTOrc's to bootstrap (GroupNotBootstrapped);
//   - no bootstrap intent is live: a bootstrap whose reply was lost may still run;
//   - no tablet that answers is an active group member or runs a START GROUP_REPLICATION: a group may
//     still run, and a second one would take writes next to it. The members of a group that lost its
//     majority leave it on their own (group_replication_unreachable_majority_timeout);
//   - no voter is ignored (IgnoreReplicas): only a voter that does not answer is dropped;
//   - at least one voter does not answer, and at least one answers. When every voter answers, VTOrc
//     bootstraps the group from them, without the loss of a forced reparent;
//   - the surviving voters run Group Replication, and one of them, allowed by the durability policy, holds
//     every transaction that the others executed or received, and RequiredPosition if it is set. With
//     NewPrimaryAlias, that voter must be the requested one.
//
// The new group cannot hold what only the dropped voters held: that loss is what the operator accepts. A
// dropped voter must also be down. One that runs, cut off from the vtctld, keeps its group and its clients
// until its tablet reads the new voter list and stops serving; if it cannot reach the topology server
// either, the shard has two primaries.
func planForcedGroup(shard *topodatapb.Shard, statuses map[string]*fullStatusResult, prevPrimary *topodatapb.Tablet, opts EmergencyReparentOptions, now time.Time) (*forcedGroupPlan, error) {
	plan := &forcedGroupPlan{incarnation: shard.GetGroupReplicationIncarnation(), voters: shard.GetGroupReplicationVoters()}
	if plan.incarnation == "" || len(plan.voters) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"the shard record lists no group replication incarnation or no voters: the shard's group was never recorded, and VTOrc bootstraps it; --group-replication-force-new-group applies to a group that lost its majority")
	}
	if intent := LiveGroupReplicationBootstrapIntent(shard, now); intent != nil {
		started := protoutil.TimeFromProto(intent.GetTime())
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"a bootstrap of the replication group on %s started at %s and may still run: retry after %s",
			topoproto.TabletAliasString(intent.GetTarget()), started.UTC().Format(time.RFC3339Nano),
			started.Add(GroupReplicationBootstrapIntentFence).UTC().Format(time.RFC3339Nano))
	}
	for _, alias := range slices.Sorted(maps.Keys(statuses)) {
		res := statuses[alias]
		if res.err != nil {
			continue
		}
		gs := res.groupStatus()
		if mysql.IsGroupMemberActive(gs) {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"the MySQL of tablet %v is %s in the replication group %s (view %s): a group still runs, and a forced reparent would create a second one. "+
					"The members of a group that lost its majority leave it within group_replication_unreachable_majority_timeout; retry once they did, or stop group replication on them",
				alias, gs.GetMemberState(), gs.GetGroupName(), gs.GetViewId())
		}
		if gs.GetStartInProgress() {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"a START GROUP_REPLICATION runs on the MySQL of tablet %v, which may still form a group: retry once it ended", alias)
		}
	}

	var candidates []*forcedCandidate
	plan.required = replication.Mysql56GTIDSet{}
	for _, voter := range plan.voters {
		alias := topoproto.TabletAliasString(voter)
		if opts.IgnoreReplicas.Has(alias) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"voter %s is ignored (--ignore-replicas): a forced reparent drops only the voters that do not answer", alias)
		}
		res, ok := statuses[alias]
		if !ok || res.err != nil {
			plan.dropped = append(plan.dropped, voter)
			continue
		}
		if !res.status.GetGroupReplicationEnabled() {
			return nil, groupReplicationNotEnabledError(alias)
		}
		executed, all, err := GroupMemberGTIDSets(res.status)
		if err != nil {
			return nil, vterrors.Wrapf(err, "failed to read the GTID set of voter %s", alias)
		}
		plan.survivors = append(plan.survivors, voter)
		plan.required = plan.required.Union(all)
		candidates = append(candidates, &forcedCandidate{res: res, executed: executed, all: all})
	}
	if len(plan.dropped) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"every voter of the shard answers ([%s]): VTOrc bootstraps the group from all of them, without losing a transaction; run EmergencyReparentShard without --group-replication-force-new-group",
			votersString(plan.voters))
	}
	if len(plan.survivors) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "no voter of the shard answers ([%s]): there is no voter to bootstrap a new group on", votersString(plan.voters))
	}
	if !opts.RequiredPosition.IsZero() && !plan.required.Contains(opts.RequiredPosition.GTIDSet) {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"no surviving voter executed or received the required position %s: the surviving voters hold %s", opts.RequiredPosition.GTIDSet, plan.required)
	}

	eligible := func(c *forcedCandidate) error {
		alias := topoproto.TabletAliasString(c.res.tablet.Alias)
		if !c.all.Contains(plan.required) {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "voter %s lacks %s, which another surviving voter executed or received", alias, gtidSetDifference(plan.required, c.all))
		}
		if policy.PromotionRule(opts.durability, c.res.tablet) == promotionrule.MustNot {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "voter %s must not be promoted according to the durability policy", alias)
		}
		if opts.PreventCrossCellPromotion && prevPrimary != nil && prevPrimary.Alias.Cell != c.res.tablet.Alias.Cell {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "voter %s is not in the cell of the previous primary %v, and cross-cell promotion is prevented",
				alias, topoproto.TabletAliasString(prevPrimary.Alias))
		}
		return nil
	}
	if opts.NewPrimaryAlias != nil {
		requested := topoproto.TabletAliasString(opts.NewPrimaryAlias)
		for _, c := range candidates {
			if topoproto.TabletAliasEqual(c.res.tablet.Alias, opts.NewPrimaryAlias) {
				if err := eligible(c); err != nil {
					return nil, vterrors.Wrapf(err, "requested primary %s cannot be promoted", requested)
				}
				plan.candidate = c.res
				return plan.withJoiners(candidates), nil
			}
		}
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "requested primary %s is not a voter that answers (surviving voters: [%s])", requested, votersString(plan.survivors))
	}

	// Among the voters that hold every transaction, prefer one that executed them all already (one that
	// holds some only in its relay log could lose them to a restart of its mysqld before the bootstrap),
	// then the highest member weight, then the lowest alias.
	weight := func(t *topodatapb.Tablet) int {
		if grd, ok := policy.AsGroupReplication(opts.durability); ok {
			return grd.MemberWeight(t)
		}
		return 0
	}
	var refusals []string
	var best []*forcedCandidate
	for _, c := range candidates {
		if err := eligible(c); err != nil {
			refusals = append(refusals, err.Error())
			continue
		}
		best = append(best, c)
	}
	if len(best) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no surviving voter can be the primary of the new group: %s", strings.Join(refusals, "; "))
	}
	slices.SortStableFunc(best, func(a, b *forcedCandidate) int {
		aBinlog, bBinlog := a.executed.Contains(plan.required), b.executed.Contains(plan.required)
		if aBinlog != bBinlog {
			if aBinlog {
				return -1
			}
			return 1
		}
		if c := cmp.Compare(weight(b.res.tablet), weight(a.res.tablet)); c != 0 {
			return c
		}
		return cmp.Compare(topoproto.TabletAliasString(a.res.tablet.Alias), topoproto.TabletAliasString(b.res.tablet.Alias))
	})
	plan.candidate = best[0].res
	return plan.withJoiners(candidates), nil
}

// withJoiners sets the surviving voters other than the candidate as the plan's joiners.
func (p *forcedGroupPlan) withJoiners(candidates []*forcedCandidate) *forcedGroupPlan {
	for _, c := range candidates {
		if c.res != p.candidate {
			p.joiners = append(p.joiners, c.res)
		}
	}
	return p
}

// forceNewGroupReplicationGroup is the forced EmergencyReparentShard of a group replication shard whose group
// lost its majority (--group-replication-force-new-group). It runs under the shard lock, on the statuses that
// reparentShardLockedGroupReplication read, and:
//
//  1. decides on that read (planForcedGroup): the voters that do not answer are dropped, and the surviving
//     voter that holds every transaction of the others becomes the new group's primary;
//  2. writes the surviving voters as the shard's voters, a compare-and-swap on the voters and the
//     incarnation it read. From then on, the tablets of the dropped voters do not serve: a tablet that is
//     not a listed voter does not serve as PRIMARY;
//  3. bootstraps the new group on the candidate the way VTOrc's GroupNotBootstrapped does: a bootstrap
//     intent, the transactions it requires, the intent's token and the incarnation it expects; then records
//     the new incarnation (compare-and-swap), or adopts the group if the RPC failed after the bootstrap
//     ran, or withdraws the intent if the tablet refused definitively;
//  4. makes the other surviving voters join the new group, promotes the candidate, writes the reparent
//     journal, and repoints the other tablets.
//
// If the bootstrap fails after the voter write, the shard keeps the surviving voters: VTOrc then bootstraps
// the group from them (GroupNotBootstrapped), or adopts a bootstrap whose reply was lost
// (GroupBootstrapNotRecorded), as it would after an operator deleted the dropped voters' tablet records.
func (erp *EmergencyReparenter) forceNewGroupReplicationGroup(ctx context.Context, ev *events.Reparent, keyspace, shard string, prevPrimary *topodatapb.Tablet,
	statuses map[string]*fullStatusResult, opts EmergencyReparentOptions,
) error {
	plan, err := planForcedGroup(ev.ShardInfo.Shard, statuses, prevPrimary, opts, time.Now())
	if err != nil {
		return vterrors.Wrapf(err, "cannot force a new replication group")
	}
	candidate := plan.candidate.tablet
	candidateAlias := topoproto.TabletAliasString(candidate.Alias)
	erp.logger.Warningf("forcing a new replication group on %v: dropping the voters that do not answer ([%s]), keeping [%s]. "+
		"The transactions that only the dropped voters held are lost; the new group starts from %s",
		candidateAlias, votersString(plan.dropped), votersString(plan.survivors), plan.required)
	for _, alias := range slices.Sorted(maps.Keys(statuses)) {
		res := statuses[alias]
		if res.err != nil || policy.IsVoter(plan.voters, res.tablet.Alias) {
			continue
		}
		if _, all, err := GroupMemberGTIDSets(res.status); err == nil && !plan.required.Contains(all) {
			erp.logger.Warningf("tablet %v holds transactions that the new group lacks (%s): it needs to be restored from a backup of the new group", alias, gtidSetDifference(all, plan.required))
		}
	}
	ev.NewPrimary = candidate.CloneVT()

	event.DispatchUpdate(ev, "writing the surviving voters")
	if err := writeForcedGroupVoters(ctx, erp.ts, keyspace, shard, plan); err != nil {
		return err
	}

	event.DispatchUpdate(ev, "bootstrapping the new replication group")
	intent, err := WriteGroupReplicationBootstrapIntent(ctx, erp.ts, keyspace, shard, candidate.Alias, plan.incarnation, time.Now())
	if err != nil {
		return vterrors.Wrapf(err, "wrote the surviving voters [%s], but not the bootstrap intent: VTOrc bootstraps the group from them", votersString(plan.survivors))
	}
	bootstrapTimeout := max(opts.WaitReplicasTimeout, topo.RemoteOperationTimeout)
	bootstrapCtx, bootstrapCancel := context.WithTimeout(ctx, bootstrapTimeout)
	groupStatus, err := erp.tmc.StartGroupReplication(bootstrapCtx, candidate, &tabletmanagerdatapb.StartGroupReplicationRequest{
		Bootstrap:               true,
		RequiredGtidSet:         plan.required.String(),
		BootstrapIntentToken:    intent.GetToken(),
		ExpectedIncarnation:     plan.incarnation,
		ReportDefinitiveRefusal: true,
	})
	bootstrapCancel()
	var incarnation string
	switch {
	case tmclient.IsGroupBootstrapRefused(err):
		if _, withdrawErr := WithdrawGroupReplicationBootstrapIntent(ctx, erp.ts, keyspace, shard, intent); withdrawErr != nil {
			erp.logger.Warningf("failed to withdraw the bootstrap intent %s of the refused bootstrap: %v", intent.GetToken(), withdrawErr)
		}
		return vterrors.Wrapf(err, "%v refused to bootstrap the new replication group; the shard keeps the surviving voters [%s], from which VTOrc bootstraps the group", candidateAlias, votersString(plan.survivors))
	case err != nil:
		// The bootstrap may have run although its reply was lost: its group is adopted if it is the
		// candidate's new group.
		adopted, adoptErr := AdoptGroupReplicationBootstrap(ctx, erp.ts, erp.tmc, keyspace, shard, plan.incarnation, intent, candidate)
		if adoptErr != nil {
			return vterrors.Wrapf(err, "failed to bootstrap the new replication group on %v (and found no group to adopt: %v); the shard keeps the surviving voters [%s] and the bootstrap intent %s: VTOrc adopts the group if the bootstrap ran, or bootstraps it once the intent expired",
				candidateAlias, adoptErr, votersString(plan.survivors), intent.GetToken())
		}
		erp.logger.Infof("the bootstrap RPC on %v failed (%v), but it created the group: adopted its incarnation %s", candidateAlias, err, adopted)
		incarnation = adopted
	default:
		incarnation = policy.GroupIncarnation(groupStatus.GetViewId())
		if incarnation == "" {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "bootstrapped the new replication group on %v, but it reports no view id", candidateAlias)
		}
		if err := RecordGroupReplicationBootstrap(ctx, erp.ts, keyspace, shard, intent, incarnation); err != nil {
			return vterrors.Wrapf(err, "bootstrapped the new replication group on %v, but failed to record its incarnation %s", candidateAlias, incarnation)
		}
	}
	erp.logger.Infof("bootstrapped the new replication group on %v, incarnation %s", candidateAlias, incarnation)

	// The candidate serves once a majority of the surviving voters is ONLINE in its view: the other
	// surviving voters join first. A voter that fails to join is left to VTOrc (GroupMemberNotOnline).
	event.DispatchUpdate(ev, "making the surviving voters join the new group")
	erp.joinForcedGroup(ctx, plan, opts)

	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}
	event.DispatchUpdate(ev, "promoting the primary of the new replication group")
	promoteCtx, promoteCancel := context.WithTimeout(ctx, max(opts.WaitReplicasTimeout, topo.RemoteOperationTimeout))
	defer promoteCancel()
	position, err := erp.tmc.PromoteReplica(promoteCtx, candidate, policy.SemiSyncAckers(opts.durability, candidate) > 0)
	if err != nil {
		return vterrors.Wrapf(err, "bootstrapped the new replication group on %v, but it failed to be upgraded to primary", candidateAlias)
	}
	if err := erp.tmc.PopulateReparentJournal(promoteCtx, candidate, time.Now().UnixNano(), opts.lockAction, candidate.Alias, position); err != nil {
		return vterrors.Wrapf(err, "failed to PopulateReparentJournal on primary %v", candidateAlias)
	}

	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}
	event.DispatchUpdate(ev, "reparenting all tablets")
	erp.reparentGroupReplicationTablets(ctx, candidate, statuses, plan.survivors, opts)
	return nil
}

// writeForcedGroupVoters writes the surviving voters of plan as the shard's voters, under the shard lock,
// which is re-checked first. The write is a compare-and-swap on the voters and the incarnation that the
// plan was decided on: a VTOrc whose shard lock expired may have changed them since.
func writeForcedGroupVoters(ctx context.Context, ts *topo.Server, keyspace, shard string, plan *forcedGroupPlan) error {
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}
	_, err := ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		if !votersEqual(si.GroupReplicationVoters, plan.voters) || si.GroupReplicationIncarnation != plan.incarnation {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"the shard record of %s/%s changed concurrently (voters [%s], incarnation %q; read voters [%s], incarnation %q): retry the reparent",
				keyspace, shard, votersString(si.GroupReplicationVoters), si.GroupReplicationIncarnation, votersString(plan.voters), plan.incarnation)
		}
		si.GroupReplicationVoters = plan.survivors
		return nil
	})
	if err != nil {
		if vterrors.Code(err) == vtrpcpb.Code_FAILED_PRECONDITION {
			return err
		}
		return vterrors.Wrapf(err, "failed to store the surviving voters of shard %s/%s", keyspace, shard)
	}
	return nil
}

// joinForcedGroup makes the surviving voters other than the candidate join the new group, concurrently,
// bounded by the replica wait timeout. A failure is logged: VTOrc makes the voter join later.
func (erp *EmergencyReparenter) joinForcedGroup(ctx context.Context, plan *forcedGroupPlan, opts EmergencyReparentOptions) {
	joinCtx, joinCancel := context.WithTimeout(ctx, groupReplicationReplicaTimeout(opts))
	defer joinCancel()
	var wg sync.WaitGroup
	for _, res := range plan.joiners {
		tablet := res.tablet
		wg.Go(func() {
			alias := topoproto.TabletAliasString(tablet.Alias)
			if _, err := erp.tmc.StartGroupReplication(joinCtx, tablet, &tabletmanagerdatapb.StartGroupReplicationRequest{}); err != nil {
				erp.logger.Warningf("voter %v failed to join the new replication group, VTOrc makes it join later: %v", alias, err)
				return
			}
			erp.logger.Infof("voter %v joined the new replication group", alias)
		})
	}
	wg.Wait()
}

// gtidSetDifference returns the transactions of a that b lacks, for a message. Group replication runs on
// MySQL, whose GTID sets are Mysql56GTIDSets; for any other set it returns a.
func gtidSetDifference(a, b replication.GTIDSet) replication.GTIDSet {
	aSet, aOK := a.(replication.Mysql56GTIDSet)
	bSet, bOK := b.(replication.Mysql56GTIDSet)
	if !aOK || !bOK {
		return a
	}
	return aSet.Difference(bSet)
}
