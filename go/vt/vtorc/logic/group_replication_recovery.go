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
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	// PromoteGroupPrimaryRecoveryName is the recovery that makes the tablet of a group's primary
	// the shard primary in the topology.
	PromoteGroupPrimaryRecoveryName string = "PromoteGroupPrimary"
	// StartGroupReplicationRecoveryName is the recovery that makes a voting member join its group.
	StartGroupReplicationRecoveryName string = "StartGroupReplication"
	// BootstrapGroupReplicationRecoveryName is the recovery that bootstraps a shard's group.
	BootstrapGroupReplicationRecoveryName string = "BootstrapGroupReplication"
	// UpdateGroupReplicationVotersRecoveryName is the recovery that updates the voters of a
	// shard's group.
	UpdateGroupReplicationVotersRecoveryName string = "UpdateGroupReplicationVoters"
	// MoveGroupPrimaryToVoterRecoveryName is the recovery that moves the primary of a shard's group,
	// which is not a voter, to a voter.
	MoveGroupPrimaryToVoterRecoveryName string = "MoveGroupPrimaryToVoter"
	// AdoptGroupReplicationBootstrapRecoveryName is the recovery that records the incarnation of a
	// group whose bootstrap's reply was lost.
	AdoptGroupReplicationBootstrapRecoveryName string = "AdoptGroupReplicationBootstrap"
)

// groupVoterStatusesTimeout bounds the whole read of the tablets' statuses on which a change of the
// voters is decided, right before its write, so that they are still fresh at the write. The tablets
// are read concurrently, so each one has up to this long to answer.
var groupVoterStatusesTimeout = 2 * time.Second

// groupReplicationCellTimeout bounds the read of the shard's tablet records in each cell, in the
// group replication recoveries that can do with the tablets of the cells that answer. The topology
// server of a cell that is cut off does not answer until the caller gives up: read under one
// deadline, it failed the recovery, and made the promotion of the group's new primary wait until
// the partition of its old primary's cell healed (S9i chaos scenario).
var groupReplicationCellTimeout = 2 * time.Second

// getReachableShardTablets returns the tablets of the shard from the cells whose topology server
// answers within groupReplicationCellTimeout, and the cells that did not answer.
func getReachableShardTablets(ctx context.Context, keyspace, shard string) ([]*topo.TabletInfo, []string, error) {
	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	tabletMap, failedCells, err := ts.GetTabletMapAndFailedCellsForShard(ctx, keyspace, shard, groupReplicationCellTimeout)
	if err != nil && !topo.IsErrType(err, topo.PartialResult) {
		return nil, nil, err
	}
	aliases := slices.Sorted(maps.Keys(tabletMap))
	tablets := make([]*topo.TabletInfo, 0, len(aliases))
	for _, alias := range aliases {
		tablets = append(tablets, tabletMap[alias])
	}
	return tablets, failedCells, nil
}

// groupReplicationFailoverSkipCode decides whether a failover of the shard primary (an emergency
// or planned reparent) must wait for the shard's replication group. When other tablets are
// active members of a group, the group elects a new primary on its own and the tablet of that
// primary, or else VTOrc (GroupPrimaryNotInTopo), makes it the shard primary. VTOrc only falls
// back to the reparent once the condition has lasted for --group-replication-failover-grace-period.
//
// The grace period is tracked per shard in inst.GroupReplicationConditions, which remembers since
// when the recovery has been requested on every recovery poll. A shard whose failover condition
// disappears for longer than the tracker's forget period starts a new grace period.
func groupReplicationFailoverSkipCode(analysisEntry *inst.DetectionAnalysis, now time.Time) RecoverySkipCode {
	if analysisEntry.ShardGroupActiveMembers == 0 {
		return RecoverySkipNone
	}
	switch analysisEntry.Analysis {
	case inst.DeadPrimary, inst.DeadPrimaryAndSomeReplicas, inst.DeadPrimaryWithoutReplicas:
		// VTOrc cannot reach the primary tablet, but the members with quorum still see its
		// MySQL as their ONLINE primary: MySQL is alive and the group keeps it. A reparent
		// would fail over a working primary because of its vttablet.
		if analysisEntry.AnalyzedServerUUID != "" && analysisEntry.ShardGroupPrimaryUUID == analysisEntry.AnalyzedServerUUID {
			return RecoverySkipGroupPrimaryAlive
		}
	}
	key := "failover/" + topoproto.KeyspaceShardString(analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
	if inst.GroupReplicationConditions.Observe(key, now) < config.GetGroupReplicationFailoverGracePeriod() {
		return RecoverySkipGroupReplicationGracePeriod
	}
	return RecoverySkipNone
}

// promoteGroupPrimary makes the tablet whose MySQL is the group's primary the shard primary in the
// topology, and records the change in the reparent journal. The tablet normally does this on its
// own; this recovery covers the case where it does not. It runs under the shard lock.
func promoteGroupPrimary(ctx context.Context, analysisEntry *inst.DetectionAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry)
	if topologyRecovery == nil {
		message := fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another %s.", analysisEntry.AnalyzedInstanceAlias, PromoteGroupPrimaryRecoveryName)
		logger.Warn(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return false, nil, err
	}
	var promoted *inst.Instance
	defer func() {
		if err := resolveRecovery(topologyRecovery, promoted); err != nil {
			logger.Error("failed to resolve recovery", slog.String("recovery", PromoteGroupPrimaryRecoveryName), slog.Any("error", err))
		}
	}()

	tablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		return false, topologyRecovery, err
	}
	aliasString := topoproto.TabletAliasString(tablet.Alias)

	// VTOrc's view of the group is up to --instance-poll-time old. Confirm that the member is
	// still the primary of the shard's legitimate group before making its tablet the shard
	// primary: the recorded incarnation, with a majority of the shard's voters in its view.
	shardInfo, err := ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return true, topologyRecovery, vterrors.Wrapf(err, "failed to read the shard record of %s", topoproto.KeyspaceShardString(tablet.Keyspace, tablet.Shard))
	}
	// The voters of the cells that do not answer are counted as not ONLINE: the majority of the
	// voters must be ONLINE among the others.
	tabletInfos, failedCells, err := getReachableShardTablets(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return true, topologyRecovery, err
	}
	if slices.Contains(failedCells, tablet.Alias.Cell) {
		// The tablet writes its own record, in its cell's topology server, before it becomes
		// PRIMARY: neither it nor VTOrc can promote it while that server does not answer. The
		// group primary is moved to a member whose cell answers instead (NEW-4).
		target, err := moveGroupPrimaryOutOfUnreachableCell(ctx, analysisEntry, tablet, shardInfo, tabletInfos, failedCells, topologyRecovery, logger)
		if target != nil {
			promoted = &inst.Instance{InstanceAlias: target.Alias}
		}
		return true, topologyRecovery, err
	}
	statuses := readShardTabletStatuses(ctx, tabletInfos)
	var status *replicationdatapb.FullStatus
	for _, st := range statuses {
		if topoproto.TabletAliasEqual(st.tablet.Alias, tablet.Alias) {
			if st.err != nil {
				return true, topologyRecovery, vterrors.Wrapf(st.err, "failed to read the status of %s", aliasString)
			}
			status = st.status
		}
	}
	if status == nil {
		return true, topologyRecovery, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s is not a tablet of its shard", aliasString)
	}
	legitimate := legitimateGroupOf(shardInfo, statuses)
	// A deposed primary that was paused or cut off resumes with its stale view, in which it is still
	// the legitimate primary, until MySQL learns of its expulsion (FLAG 1 of the soak): VTOrc's stored
	// state then reports it as a group primary that is not the shard primary. Another member that is
	// the ONLINE primary, with quorum, of a newer view of the same incarnation is the group's primary
	// instead: it is promoted if it may be, and nothing is otherwise.
	if newer := supersedingGroupPrimary(statuses, tablet.Alias, status.GetGroupReplicationStatus()); newer != nil {
		newerAlias := topoproto.TabletAliasString(newer.tablet.Alias)
		refuse := func(why string) error {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"the view %s of %s is superseded: %s is the ONLINE primary, with quorum, of the newer view %s of the same incarnation, and %s",
				status.GetGroupReplicationStatus().GetViewId(), aliasString, newerAlias, newer.status.GetGroupReplicationStatus().GetViewId(), why)
		}
		switch {
		case topoproto.TabletAliasEqual(shardInfo.PrimaryAlias, newer.tablet.Alias) && newer.tablet.Type == topodatapb.TabletType_PRIMARY:
			return true, topologyRecovery, refuse("it is the shard primary already")
		case !policy.IsVoter(shardInfo.GetGroupReplicationVoters(), newer.tablet.Alias):
			return true, topologyRecovery, refuse("it is not a voter")
		case !legitimate.IsLegitimatePrimary(newer.status.GetGroupReplicationStatus()):
			return true, topologyRecovery, refuse("it is not the primary of the shard's legitimate group")
		case !newer.status.GetGroupReplicationEnabled():
			return true, topologyRecovery, refuse("it does not run Group Replication (--enable-group-replication)")
		}
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("the view of %s is superseded: promoting %s, the primary of the newer view, instead", aliasString, newerAlias))
		tablet, status, aliasString = newer.tablet, newer.status, newerAlias
	}
	if !legitimate.IsLegitimatePrimary(status.GetGroupReplicationStatus()) {
		gs := status.GetGroupReplicationStatus()
		return true, topologyRecovery, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"the MySQL of %s is not the primary of the shard's replication group (view %s, recorded incarnation %q, %d of %d voters ONLINE)",
			aliasString, gs.GetViewId(), legitimate.Incarnation, legitimate.OnlineVoters(gs), len(legitimate.Voters))
	}
	if !status.GetGroupReplicationEnabled() {
		// Its MySQL reports the group's state, but the tablet applies neither the serving
		// invariant nor the fence: it would serve as the shard primary without them.
		return true, topologyRecovery, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"%s does not run Group Replication (--enable-group-replication): it would serve as the shard primary without the serving invariant of the replication group", aliasString)
	}

	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoting %s, the primary of the replication group, to shard primary", aliasString))
	// The group, not semi-sync, makes transactions durable.
	if err := changeTabletType(ctx, tablet, topodatapb.TabletType_PRIMARY, false); err != nil {
		return true, topologyRecovery, vterrors.Wrapf(err, "failed to change the type of %s to PRIMARY", aliasString)
	}
	promoted = &inst.Instance{InstanceAlias: tablet.Alias}
	_ = inst.AuditOperation(PromoteGroupPrimaryRecoveryName, tablet.Alias, "promoted the primary of the replication group to shard primary")

	// Record the reparent in the journal, like a reparent does, so that the history of the
	// shard's primaries stays complete. The tablet is already the shard primary; a failure here
	// is reported but does not undo the promotion.
	if err := populateReparentJournal(ctx, tablet, getLockAction(tablet.Alias, analysisEntry.Analysis)); err != nil {
		return true, topologyRecovery, vterrors.Wrapf(err, "promoted %s, but failed to write the reparent journal", aliasString)
	}
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s: successfully promoted %s", PromoteGroupPrimaryRecoveryName, aliasString))
	return true, topologyRecovery, nil
}

// startGroupReplicationOnMember makes a voting member that is not active join the shard's group.
// VTOrc's view is up to --instance-poll-time old, so it first confirms that another tablet of the
// shard is an active member of the shard's legitimate group with quorum in its view. Starting a
// join while no such group exists cannot join anything: the START blocks until MySQL's join
// timeout, during which a bootstrap on the member fails, and a member has been seen to form a group
// of its own.
//
// The check runs under the shard lock, which the recovery holds; the join itself does not. A join
// does not need the shard lock (the tablets' own rejoins and the joins after a bootstrap run without
// it), and it can block for as long as the member's START, up to the RPC's timeout: in the G12
// chaos scenario, a join that VTOrc started right before the group lost its majority held the shard
// lock for 29.5s, and the bootstrap of the group, which needs that lock, waited for it. The join
// therefore runs in the background, after the recovery released the lock. One join per tablet runs
// at a time: while it runs, the member stays OFFLINE until MySQL starts the join, and the next
// polls would otherwise pile up joins behind the tablet's action lock.
func startGroupReplicationOnMember(ctx context.Context, analysisEntry *inst.DetectionAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	aliasString := topoproto.TabletAliasString(analysisEntry.AnalyzedInstanceAlias)
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry)
	if topologyRecovery == nil {
		message := fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another %s.", analysisEntry.AnalyzedInstanceAlias, StartGroupReplicationRecoveryName)
		logger.Warn(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return false, nil, err
	}
	defer func() {
		if err := resolveRecovery(topologyRecovery, nil); err != nil {
			logger.Error("failed to resolve recovery", slog.String("recovery", StartGroupReplicationRecoveryName), slog.Any("error", err))
		}
	}()

	tablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		return false, topologyRecovery, err
	}
	if err := checkLegitimateGroupActive(ctx, tablet); err != nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("not starting group replication on %s: %v", aliasString, err))
		return true, topologyRecovery, err
	}
	if !groupJoinsInFlight.start(aliasString) {
		_ = AuditTopologyRecovery(topologyRecovery, "a join already runs on "+aliasString)
		return true, topologyRecovery, nil
	}
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("starting group replication on %s, without holding the shard lock", aliasString))
	go func() {
		defer groupJoinsInFlight.done(aliasString)
		// The recovery's context belongs to the shard lock, which is released by now.
		if _, err := startGroupReplication(context.Background(), tablet, &tabletmanagerdatapb.StartGroupReplicationRequest{}); err != nil {
			logger.Warn("failed to make a voter join its group", slog.String("tablet", aliasString), slog.Any("error", err))
			return
		}
		_ = inst.AuditOperation(StartGroupReplicationRecoveryName, tablet.Alias, "joined its group")
		logger.Info("a voter joined its group", slog.String("tablet", aliasString))
	}()
	return true, topologyRecovery, nil
}

// groupJoinsInFlight are the tablets on which a join that startGroupReplicationOnMember started in
// the background still runs.
var groupJoinsInFlight = &groupJoins{tablets: make(map[string]bool)}

// groupJoins is a set of tablets on which a join runs.
type groupJoins struct {
	mu      sync.Mutex
	tablets map[string]bool
}

// start marks a join as running on the tablet, unless one already runs, and returns whether it did.
func (j *groupJoins) start(alias string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.tablets[alias] {
		return false
	}
	j.tablets[alias] = true
	return true
}

func (j *groupJoins) done(alias string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.tablets, alias)
}

func (j *groupJoins) running(alias string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.tablets[alias]
}

// groupJoinCheckTimeout bounds the FullStatus RPCs with which VTOrc checks, right before it makes a
// voter join its group, that the shard's legitimate group is active on another tablet.
var groupJoinCheckTimeout = 2 * time.Second

// checkLegitimateGroupActive returns an error unless a tablet of the shard other than the given
// one reports that its MySQL is an active member of the shard's legitimate group, with quorum in
// its view, and the given tablet's MySQL is not active in a group of another incarnation.
//
// It returns as soon as the answers it has settle the question, and waits at most
// groupJoinCheckTimeout for the others: the join starts right after the check, and a check that
// waited for an unreachable tablet (up to the RPC timeout) made VTOrc start a join into a group
// that had lost its majority in the meantime. Such a join ends with the member alone in a group
// of its own, which then keeps the shard's group from being bootstrapped until it has left it.
func checkLegitimateGroupActive(ctx context.Context, tablet *topodatapb.Tablet) error {
	shardInfo, err := ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return vterrors.Wrapf(err, "failed to read the shard record of %s", topoproto.KeyspaceShardString(tablet.Keyspace, tablet.Shard))
	}
	// While the shard record lists no incarnation, every group would count as the shard's group (see
	// matchGroupMemberNotOnline): the voters join once the bootstrap is recorded.
	if shardInfo.GetGroupReplicationIncarnation() == "" {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the shard record of %s lists no incarnation of its replication group: the voters join once its bootstrap is recorded",
			topoproto.KeyspaceShardString(tablet.Keyspace, tablet.Shard))
	}
	// One active member of the legitimate group is enough, in any cell that answers.
	tabletInfos, _, err := getReachableShardTablets(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return err
	}
	// Whether a member belongs to the legitimate group only depends on the recorded incarnation.
	legitimate := policy.NewLegitimateGroup(shardInfo.GetGroupReplicationIncarnation(), nil, nil, nil)
	groupName := policy.GroupName(tablet.Keyspace, tablet.Shard)

	checkCtx, cancel := context.WithTimeout(ctx, groupJoinCheckTimeout)
	defer cancel()
	results := make(chan *shardTabletStatus, len(tabletInfos))
	for _, ti := range tabletInfos {
		go func() {
			st := &shardTabletStatus{tablet: ti.Tablet}
			st.status, st.err = tabletFullStatus(checkCtx, ti.Tablet)
			results <- st
		}()
	}
	active, selfChecked := false, false
	for range tabletInfos {
		st := <-results
		gs := st.status.GetGroupReplicationStatus()
		switch {
		case topoproto.TabletAliasEqual(st.tablet.Alias, tablet.Alias):
			selfChecked = true
			if st.err == nil && legitimate.IsForeignIncarnation(gs) {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the MySQL of %s is active in group incarnation %s, not in the shard's incarnation %s",
					topoproto.TabletAliasString(tablet.Alias), policy.GroupIncarnation(gs.GetViewId()), legitimate.Incarnation)
			}
		case st.err == nil && gs.GetGroupName() == groupName && legitimate.IsLegitimateMember(gs) && gs.GetHasQuorum():
			active = true
		}
		if active && selfChecked {
			return nil
		}
	}
	if !active {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no other tablet of %s is an active member of the shard's replication group with quorum; the group must be bootstrapped first",
			topoproto.KeyspaceShardString(tablet.Keyspace, tablet.Shard))
	}
	return nil
}

// groupBootstrapCandidate is a voting member that could bootstrap the shard's group.
type groupBootstrapCandidate struct {
	tablet *topodatapb.Tablet
	// executed is the set of transactions the member executed: they are in its binlog.
	executed replication.GTIDSet
	// gtidSet is the union of the transactions the member executed and the transactions it
	// received from its group but has not applied yet. The latter are only in the relay log of its
	// group_replication_applier channel, which a restart of mysqld discards (relay_log_recovery).
	gtidSet replication.GTIDSet
}

// bootstrapGroupReplication bootstraps the shard's group on the voting member with the most
// advanced GTID set. The other members then join it (GroupMemberNotOnline, or their own
// reconcile loop). It runs under the shard lock, after VTOrc refreshed all tablets of the shard.
//
// The bootstrap RPC carries every transaction that a voter executed or received: the tablet
// refuses to bootstrap unless its MySQL has executed all of them right before its START (see
// chooseGroupBootstrapCandidate), and VTOrc chooses again on its next pass. It also carries the
// intent's token and the incarnation it was recorded for: the tablet refuses the bootstrap if the
// shard record no longer holds them when the RPC gets the tablet's action lock, or right before
// MySQL's START. An RPC can wait for that lock while this VTOrc loses its shard lock and another
// VTOrc replaces the intent.
//
// Bootstrapping a second group would split the shard's data, so the recovery re-reads the
// status of every tablet of the shard and gives up when a voting member cannot be reached, when
// any tablet is already an active group member, or when no member's GTID set contains all the
// others' (the members have diverged, and choosing one would lose transactions).
//
// Before the bootstrap, it records a bootstrap intent in the shard record (see
// reparentutil.WriteGroupReplicationBootstrapIntent), which a recent intent for another tablet
// refuses. If the bootstrap RPC fails, the group the bootstrap may have created anyway is adopted
// right away, or on a later pass (GroupBootstrapNotRecorded); the incarnation is recorded with a
// compare-and-swap against the one the intent was recorded for. If the tablet refused the bootstrap
// definitively instead (tmclient.GroupBootstrapRefusedError: its MySQL lacks a required transaction,
// typically because a restart of mysqld discarded its relay log, and no START GROUP_REPLICATION
// runs), there is nothing to adopt: the intent is withdrawn right away (see
// reparentutil.WithdrawGroupReplicationBootstrapIntent), so that the next pass can bootstrap the
// voter that holds those transactions, rather than wait for the intent to expire.
//
// A live intent can name a voter that is no longer the candidate: its bootstrap RPC failed without a
// definitive refusal (it timed out, or reached the tablet while mysqld was down), and the voter's
// mysqld then restarted and discarded transactions from its relay log that another voter holds.
// Nothing refuses that intent's bootstrap any more, so it fenced the bootstrap of the candidate until
// it expired. VTOrc then sends the intent's own bootstrap to that voter again first (see
// staleGroupBootstrapIntentTarget): its tablet refuses it definitively, the intent is withdrawn, and
// the candidate is bootstrapped in the same pass.
func bootstrapGroupReplication(ctx context.Context, analysisEntry *inst.DetectionAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry)
	if topologyRecovery == nil {
		message := fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another %s.", analysisEntry.AnalyzedInstanceAlias, BootstrapGroupReplicationRecoveryName)
		logger.Warn(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return false, nil, err
	}
	var bootstrapped *inst.Instance
	defer func() {
		if err := resolveRecovery(topologyRecovery, bootstrapped); err != nil {
			logger.Error("failed to resolve recovery", slog.String("recovery", BootstrapGroupReplicationRecoveryName), slog.Any("error", err))
		}
	}()

	// The shard is locked, so the voters and the shard's durability policy cannot change until the
	// group is bootstrapped.
	keyspaceShard := topoproto.KeyspaceShardString(analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
	shardInfo, err := ts.GetShard(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
	if err != nil {
		return false, topologyRecovery, vterrors.Wrapf(err, "failed to read the shard record of %s", keyspaceShard)
	}
	durability, err := inst.GetShardRecordDurabilityPolicy(analysisEntry.AnalyzedKeyspace, shardInfo.Shard)
	if err != nil {
		return false, topologyRecovery, err
	}
	if !policy.IsGroupReplication(durability) {
		return false, topologyRecovery, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the durability policy of shard %s does not use group replication", keyspaceShard)
	}
	tabletInfos, err := getShardTablets(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
	if err != nil {
		return false, topologyRecovery, err
	}
	intent := reparentutil.LiveGroupReplicationBootstrapIntent(shardInfo.Shard, time.Now())
	candidate, required, statuses, err := chooseGroupBootstrapCandidate(ctx, shardInfo.GroupReplicationVoters, tabletInfos, intent.GetTarget(), logger)
	if err != nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("not bootstrapping the group: %v", err))
		return true, topologyRecovery, err
	}

	if stale := staleGroupBootstrapIntentTarget(intent, candidate, shardInfo.GroupReplicationVoters, statuses); stale != nil {
		staleAlias := topoproto.TabletAliasString(stale.Alias)
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("the live bootstrap intent %s names %s, which no longer holds every transaction of the voters (%s); the candidate is %s: sending the intent's bootstrap to %s again",
			intent.GetToken(), staleAlias, required, topoproto.TabletAliasString(candidate.tablet.Alias), staleAlias))
		incarnation, withdrawn, err := runGroupBootstrap(ctx, analysisEntry, stale, intent, shardInfo.GetGroupReplicationIncarnation(), required, topologyRecovery, logger)
		if err == nil {
			// The tablet checked, right before MySQL's START, that MySQL executed every transaction
			// required now: the bootstrap is as safe as the candidate's.
			bootstrapped = &inst.Instance{InstanceAlias: stale.Alias}
			finishGroupBootstrap(ctx, analysisEntry, shardInfo.GroupReplicationVoters, tabletInfos, stale, incarnation, topologyRecovery, logger)
			return true, topologyRecovery, nil
		}
		if !withdrawn {
			// The intent stays, until it is adopted or expires: the bootstrap may still run.
			return true, topologyRecovery, vterrors.Wrapf(err, "the bootstrap of the live intent %s on %s, sent again, failed; not bootstrapping the group on %s before the intent expires",
				intent.GetToken(), staleAlias, topoproto.TabletAliasString(candidate.tablet.Alias))
		}
		// The intent no longer fences the candidate. Choose again, on a fresh read of the shard record
		// and of every voter, and bootstrap the candidate in this pass: the shard lock is still held,
		// and the intent's write checks it first.
		if shardInfo, err = ts.GetShard(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard); err != nil {
			return true, topologyRecovery, vterrors.Wrapf(err, "failed to read the shard record of %s", keyspaceShard)
		}
		if tabletInfos, err = getShardTablets(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard); err != nil {
			return true, topologyRecovery, err
		}
		intent = reparentutil.LiveGroupReplicationBootstrapIntent(shardInfo.Shard, time.Now())
		if candidate, required, _, err = chooseGroupBootstrapCandidate(ctx, shardInfo.GroupReplicationVoters, tabletInfos, intent.GetTarget(), logger); err != nil {
			_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("not bootstrapping the group: %v", err))
			return true, topologyRecovery, err
		}
	}

	aliasString := topoproto.TabletAliasString(candidate.tablet.Alias)
	// Record the intent to bootstrap first. If the bootstrap's reply is lost, the group it created
	// is adopted from the intent (adoptGroupReplicationBootstrap), and while the intent is recent
	// no other VTOrc bootstraps the group on another tablet: a bootstrap whose reply was lost may
	// still be running, its member not active yet.
	intent, err = reparentutil.WriteGroupReplicationBootstrapIntent(ctx, ts, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard,
		candidate.tablet.Alias, shardInfo.GetGroupReplicationIncarnation(), time.Now())
	if err != nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("not bootstrapping the group: %v", err))
		return true, topologyRecovery, err
	}
	saveShardRecord(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, logger)
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("bootstrapping the replication group on %s, which has the most advanced GTID set %s (intent %s)", aliasString, candidate.gtidSet, intent.GetToken()))
	incarnation, _, err := runGroupBootstrap(ctx, analysisEntry, candidate.tablet, intent, shardInfo.GetGroupReplicationIncarnation(), required, topologyRecovery, logger)
	if err != nil {
		return true, topologyRecovery, err
	}
	bootstrapped = &inst.Instance{InstanceAlias: candidate.tablet.Alias}
	finishGroupBootstrap(ctx, analysisEntry, shardInfo.GroupReplicationVoters, tabletInfos, candidate.tablet, incarnation, topologyRecovery, logger)
	return true, topologyRecovery, nil
}

// staleGroupBootstrapIntentTarget returns the tablet of the live bootstrap intent's target when VTOrc
// sends that intent's bootstrap to it again, before it bootstraps candidate: the intent fences the
// candidate, and its target is no longer a candidate itself (chooseGroupBootstrapCandidate prefers the
// intent's target among the voters that hold every transaction). It returns nil, and the intent keeps
// fencing the candidate until it is adopted or expires, unless all of these hold on this pass:
//   - the intent has a token, which the tablet checks and the withdrawal compares;
//   - its target is not the candidate, and is still a voter with a tablet record;
//   - its tablet answered FullStatus;
//   - its MySQL is not an active group member: a member would refuse the bootstrap without a
//     definitive refusal, and its group is GroupBootstrapNotRecorded's to adopt;
//   - no START GROUP_REPLICATION runs on it, which could still form a group of one; the tablet would
//     also stop it, or wait for it, before it refused.
//
// VTOrc sends the same intent's bootstrap, with the intent's token and the incarnation it expects, and
// the transactions that every voter executed or received now; it does not write the intent, so that its
// time, and the fence, are not extended. The tablet's checks make that bootstrap safe whatever it finds:
// under its action lock, it refuses a superseded intent, applies its relay log if that covers the
// required transactions, and refuses, definitively when no START runs, unless MySQL then executed all
// of them. A target that lost transactions in a restart of mysqld refuses definitively, and VTOrc then
// withdraws the intent (see runGroupBootstrap). A target that bootstraps after all holds every
// required transaction in its binlog, like any candidate, and its group is recorded as the reply of the
// intent's bootstrap. Any other failure keeps the intent, as for the intent's first bootstrap.
//
// The definitive refusal still proves that no bootstrap can start from the intent, although the
// intent's token was sent twice. The first RPC failed before VTOrc sent the second one. If its handler
// still waits for the action lock, it gets it after the refusal, and its intent check refuses it once the
// intent is withdrawn; before the withdrawal, its own required set refuses it: it was computed on an
// earlier pass, and the transactions that the voters executed or received only shrink while no group
// runs (a restart of mysqld discards its relay log), so it contains what the target lacks now.
func staleGroupBootstrapIntentTarget(intent *topodatapb.GroupReplicationBootstrapIntent, candidate *groupBootstrapCandidate, voters []*topodatapb.TabletAlias, statuses []*shardTabletStatus) *topodatapb.Tablet {
	if intent.GetToken() == "" || intent.GetTarget() == nil || topoproto.TabletAliasEqual(intent.GetTarget(), candidate.tablet.Alias) ||
		!policy.IsVoter(voters, intent.GetTarget()) {
		return nil
	}
	for _, st := range statuses {
		if !topoproto.TabletAliasEqual(st.tablet.Alias, intent.GetTarget()) {
			continue
		}
		if st.err != nil || st.status == nil {
			return nil
		}
		gs := st.status.GetGroupReplicationStatus()
		if mysql.IsGroupMemberActive(gs) || gs.GetStartInProgress() {
			return nil
		}
		return st.tablet
	}
	return nil
}

// runGroupBootstrap sends the bootstrap of intent, recorded while the shard record listed
// recordedIncarnation, to target, with the transactions it requires, and records the incarnation of the
// group it created, from the reply or, if the RPC failed, by adoption (the reply may be lost while the
// bootstrap ran). If the tablet refused definitively, it withdraws the intent instead, and reports
// whether it did. It returns the recorded incarnation, or the error.
func runGroupBootstrap(ctx context.Context, analysisEntry *inst.DetectionAnalysis, target *topodatapb.Tablet, intent *topodatapb.GroupReplicationBootstrapIntent,
	recordedIncarnation string, required replication.GTIDSet, topologyRecovery *TopologyRecovery, logger *log.PrefixedLogger,
) (incarnation string, withdrawn bool, err error) {
	aliasString := topoproto.TabletAliasString(target.Alias)
	groupStatus, err := startGroupReplication(ctx, target, &tabletmanagerdatapb.StartGroupReplicationRequest{
		Bootstrap:               true,
		RequiredGtidSet:         required.String(),
		BootstrapIntentToken:    intent.GetToken(),
		ExpectedIncarnation:     recordedIncarnation,
		ReportDefinitiveRefusal: true,
	})
	if tmclient.IsGroupBootstrapRefused(err) {
		return "", withdrawGroupReplicationBootstrapIntent(ctx, analysisEntry, intent, aliasString, err, topologyRecovery, logger), err
	}
	if err != nil {
		// The bootstrap may have happened although its reply was lost, for example because the
		// tablet was cut off again while MySQL's START ran. Its group is adopted if it is the
		// target's new group.
		adopted, adoptErr := reparentutil.AdoptGroupReplicationBootstrap(ctx, ts, tmc, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard,
			recordedIncarnation, intent, target)
		if adoptErr != nil {
			_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("the bootstrap on %s failed (%v), and no group to adopt: %v", aliasString, err, adoptErr))
			return "", false, vterrors.Wrapf(err, "failed to bootstrap the replication group on %s", aliasString)
		}
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("the bootstrap RPC on %s failed (%v), but it created the group: adopted its incarnation %s", aliasString, err, adopted))
		return adopted, false, nil
	}
	// The new group is the shard's legitimate group: record its incarnation while the shard is still
	// locked, before any other member joins it.
	incarnation = policy.GroupIncarnation(groupStatus.GetViewId())
	if incarnation == "" {
		return "", false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "bootstrapped the replication group on %s, but it reports no view id", aliasString)
	}
	if err := reparentutil.RecordGroupReplicationBootstrap(ctx, ts, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, intent, incarnation); err != nil {
		return "", false, vterrors.Wrapf(err, "bootstrapped the replication group on %s, but failed to record its incarnation %s", aliasString, incarnation)
	}
	return incarnation, false, nil
}

// finishGroupBootstrap refreshes VTOrc's copy of the shard record once the incarnation of the group
// bootstrapped on the given tablet is recorded, and makes the other voters join it.
func finishGroupBootstrap(ctx context.Context, analysisEntry *inst.DetectionAnalysis, voters []*topodatapb.TabletAlias, tabletInfos []*topo.TabletInfo,
	bootstrapped *topodatapb.Tablet, incarnation string, topologyRecovery *TopologyRecovery, logger *log.PrefixedLogger,
) {
	// Until VTOrc refreshes its copy of the shard record, its analysis would take the new group
	// for a foreign one, and not make the other voters join it.
	saveShardRecord(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, logger)
	_ = AuditTopologyRecovery(topologyRecovery, "recorded the group incarnation "+incarnation)
	joinVotersAfterBootstrap(voters, tabletInfos, bootstrapped, logger)
	_ = inst.AuditOperation(BootstrapGroupReplicationRecoveryName, bootstrapped.Alias, "bootstrapped the replication group")
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s: bootstrapped the replication group on %s", BootstrapGroupReplicationRecoveryName, topoproto.TabletAliasString(bootstrapped.Alias)))
}

// withdrawGroupReplicationBootstrapIntent withdraws the bootstrap intent of a bootstrap that its
// target refused definitively (refusal), under the shard lock that the recovery holds: a
// compare-and-swap on the intent's token, which leaves a newer intent, or an incarnation recorded
// since, as they are. A failure only costs time: the intent then expires. It returns whether it
// withdrew the intent.
func withdrawGroupReplicationBootstrapIntent(ctx context.Context, analysisEntry *inst.DetectionAnalysis, intent *topodatapb.GroupReplicationBootstrapIntent,
	aliasString string, refusal error, topologyRecovery *TopologyRecovery, logger *log.PrefixedLogger,
) bool {
	withdrawn, err := reparentutil.WithdrawGroupReplicationBootstrapIntent(ctx, ts, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, intent)
	switch {
	case err != nil:
		logger.Warn("failed to withdraw the bootstrap intent of a refused bootstrap", slog.String("intent", intent.GetToken()), slog.Any("error", err))
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s refused the bootstrap definitively (%v), but its intent %s could not be withdrawn: %v", aliasString, refusal, intent.GetToken(), err))
		return false
	case withdrawn:
		saveShardRecord(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, logger)
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s refused the bootstrap definitively (%v): withdrew its intent %s", aliasString, refusal, intent.GetToken()))
		return true
	default:
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s refused the bootstrap definitively (%v); the shard record no longer holds its intent %s", aliasString, refusal, intent.GetToken()))
		return false
	}
}

// saveShardRecord refreshes VTOrc's copy of the shard record from the topology.
func saveShardRecord(ctx context.Context, keyspace, shard string, logger *log.PrefixedLogger) {
	shardInfo, err := ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		logger.Warn("failed to read the shard record", slog.Any("error", err))
		return
	}
	if err := inst.SaveShard(shardInfo); err != nil {
		logger.Warn("failed to save the shard record", slog.Any("error", err))
	}
}

// adoptUnrecordedGroup records the incarnation of the group whose primary is the given tablet, while
// the shard record lists no incarnation and holds no bootstrap intent for that tablet: a group that
// PlannedReparentShard's initial promotion bootstrapped with InitPrimary and failed to record, or a
// bootstrap of VTOrc whose reply was lost and whose intent was replaced since. No voter joins a
// group whose incarnation is not recorded (checkLegitimateGroupToJoin, matchGroupMemberNotOnline), so
// without this the shard would never get the majority of its voters back.
//
// It records the group only when it is the only group that the shard can have, which makes the
// record equivalent to a bootstrap of VTOrc on its primary (bootstrapGroupReplication):
//   - no bootstrap intent is live: another bootstrap may be running;
//   - every voter's tablet answers;
//   - no tablet runs a START GROUP_REPLICATION, and none is an active member of a group of another
//     incarnation;
//   - the primary executed every transaction that a voter executed or received.
//
// The write is a compare-and-swap on the empty incarnation and the absence of a live intent, under
// the shard lock that the recovery holds.
func adoptUnrecordedGroup(ctx context.Context, keyspace, shard string, shardInfo *topo.ShardInfo, tablet *topodatapb.Tablet) (string, error) {
	keyspaceShard := topoproto.KeyspaceShardString(keyspace, shard)
	aliasString := topoproto.TabletAliasString(tablet.Alias)
	if shardInfo.GetGroupReplicationIncarnation() != "" {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the shard record of %s lists the incarnation %s", keyspaceShard, shardInfo.GetGroupReplicationIncarnation())
	}
	if intent := reparentutil.LiveGroupReplicationBootstrapIntent(shardInfo.Shard, time.Now()); intent != nil {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the shard record of %s holds a live bootstrap intent for %s", keyspaceShard, topoproto.TabletAliasString(intent.GetTarget()))
	}
	voters := shardInfo.GroupReplicationVoters
	if len(voters) == 0 {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the shard record of %s lists no voters", keyspaceShard)
	}
	tabletInfos, err := getShardTablets(ctx, keyspace, shard)
	if err != nil {
		return "", vterrors.Wrapf(err, "cannot read the tablets of %s to check its unrecorded group", keyspaceShard)
	}
	statuses := readShardTabletStatuses(ctx, tabletInfos)
	var primary *shardTabletStatus
	read := make(map[string]bool, len(statuses))
	for _, st := range statuses {
		read[topoproto.TabletAliasString(st.tablet.Alias)] = true
		if topoproto.TabletAliasEqual(st.tablet.Alias, tablet.Alias) {
			primary = st
		}
	}
	// A voter without a tablet record is not in the tablet list, but its MySQL may run another group.
	for _, voter := range voters {
		if !read[topoproto.TabletAliasString(voter)] {
			return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "voter %s has no tablet record: another group cannot be ruled out", topoproto.TabletAliasString(voter))
		}
	}
	if primary == nil || primary.err != nil {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot read the status of %s", aliasString)
	}
	gs := primary.status.GetGroupReplicationStatus()
	incarnation := policy.GroupIncarnation(gs.GetViewId())
	if !mysql.IsGroupPrimary(gs) || gs.GetGroupName() != policy.GroupName(keyspace, shard) || incarnation == "" {
		return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the MySQL of %s is not the primary of the shard's group with quorum (group %s, member %s %s, view %q)",
			aliasString, gs.GetGroupName(), gs.GetMemberState(), gs.GetMemberRole(), gs.GetViewId())
	}
	primaryExecuted, _, err := reparentutil.GroupMemberGTIDSets(primary.status)
	if err != nil {
		return "", vterrors.Wrapf(vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, err.Error()), "cannot read the transactions of %s", aliasString)
	}
	for _, st := range statuses {
		alias := topoproto.TabletAliasString(st.tablet.Alias)
		isVoter := policy.IsVoter(voters, st.tablet.Alias)
		if st.err != nil {
			if isVoter {
				return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "voter %s does not answer (%v): another group cannot be ruled out", alias, st.err)
			}
			continue
		}
		other := st.status.GetGroupReplicationStatus()
		if other.GetStartInProgress() {
			return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "a START GROUP_REPLICATION runs on %s", alias)
		}
		if mysql.IsGroupMemberActive(other) && policy.GroupIncarnation(other.GetViewId()) != incarnation {
			return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s is active in group incarnation %q, not %s", alias, policy.GroupIncarnation(other.GetViewId()), incarnation)
		}
		if !isVoter || st == primary {
			continue
		}
		_, all, err := reparentutil.GroupMemberGTIDSets(st.status)
		if err != nil {
			return "", vterrors.Wrapf(vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, err.Error()), "cannot read the transactions of voter %s", alias)
		}
		if !primaryExecuted.Contains(all) {
			return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "voter %s executed or received transactions that %s did not execute (%s, primary %s)", alias, aliasString, all.String(), primaryExecuted.String())
		}
	}
	if err := reparentutil.RecordUnrecordedGroupReplicationIncarnation(ctx, ts, keyspace, shard, incarnation, time.Now()); err != nil {
		return "", err
	}
	return incarnation, nil
}

// adoptGroupReplicationBootstrap records the incarnation of the group that a bootstrap created when
// the bootstrap's reply was lost (GroupBootstrapNotRecorded): VTOrc, this one or another, recorded
// a bootstrap intent for the analyzed tablet, and the tablet's MySQL is now the primary of a group
// of another incarnation than the shard record lists. Without it, the tablet trusts the group it
// bootstrapped for a minute and then leaves it, nothing else can bootstrap the group meanwhile, and
// the other voters do not join a group whose incarnation is not recorded (S7d chaos scenario, run
// r3). It runs under the shard lock: it re-reads the shard record, checks the target's status now
// (see reparentutil.AdoptGroupReplicationBootstrap), records the incarnation with a
// compare-and-swap, which clears the intent, and makes the other voters join the group.
func adoptGroupReplicationBootstrap(ctx context.Context, analysisEntry *inst.DetectionAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	aliasString := topoproto.TabletAliasString(analysisEntry.AnalyzedInstanceAlias)
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry)
	if topologyRecovery == nil {
		message := fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another %s.", analysisEntry.AnalyzedInstanceAlias, AdoptGroupReplicationBootstrapRecoveryName)
		logger.Warn(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return false, nil, err
	}
	var adopted *inst.Instance
	defer func() {
		if err := resolveRecovery(topologyRecovery, adopted); err != nil {
			logger.Error("failed to resolve recovery", slog.String("recovery", AdoptGroupReplicationBootstrapRecoveryName), slog.Any("error", err))
		}
	}()

	shardInfo, err := ts.GetShard(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
	if err != nil {
		return false, topologyRecovery, vterrors.Wrapf(err, "failed to read the shard record of %s", topoproto.KeyspaceShardString(analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard))
	}
	intent := reparentutil.CurrentGroupReplicationBootstrapIntent(shardInfo.Shard)
	forIntent := intent != nil && topoproto.TabletAliasEqual(intent.GetTarget(), analysisEntry.AnalyzedInstanceAlias)
	if !forIntent && shardInfo.GetGroupReplicationIncarnation() != "" {
		_ = AuditTopologyRecovery(topologyRecovery, "the shard record holds no bootstrap intent for "+aliasString)
		return false, topologyRecovery, nil
	}
	tablet, err := inst.ReadTablet(analysisEntry.AnalyzedInstanceAlias)
	if err != nil {
		return false, topologyRecovery, err
	}
	var incarnation string
	if forIntent {
		incarnation, err = reparentutil.AdoptGroupReplicationBootstrap(ctx, ts, tmc, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard,
			shardInfo.GetGroupReplicationIncarnation(), intent, tablet)
	} else {
		// No incarnation is recorded, and no intent names the tablet: a group that nobody recorded.
		incarnation, err = adoptUnrecordedGroup(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, shardInfo, tablet)
	}
	if err != nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("not adopting the group of %s: %v", aliasString, err))
		return true, topologyRecovery, err
	}
	adopted = &inst.Instance{InstanceAlias: tablet.Alias}
	saveShardRecord(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, logger)
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("adopted the group that %s bootstrapped: recorded its incarnation %s", aliasString, incarnation))
	tabletInfos, _, err := getReachableShardTablets(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
	if err != nil {
		logger.Warn("failed to read the tablets of the shard, the voters join the group on their own", slog.Any("error", err))
	} else {
		joinVotersAfterBootstrap(shardInfo.GroupReplicationVoters, tabletInfos, tablet, logger)
	}
	_ = inst.AuditOperation(AdoptGroupReplicationBootstrapRecoveryName, tablet.Alias, "adopted the replication group it bootstrapped")
	return true, topologyRecovery, nil
}

// joinVotersAfterBootstrap makes the voters other than the one that bootstrapped the shard's group
// join it, concurrently and in the background. A group that was just bootstrapped has one member,
// and Vitess only follows its primary once a majority of the voters are ONLINE in its view: until
// then the shard takes no writes. Every voter was reachable and not an active member when the
// group was bootstrapped, so they are made to join right away, instead of when their tablets' sync
// loops or VTOrc's next GroupMemberNotOnline get to it (seconds later, which the S7d chaos
// scenario showed to be long enough for the group to be cut off again first). A join does not need
// the shard lock, and can take as long as the member's distributed recovery. A join that fails is
// retried by the tablet's sync loop or by GroupMemberNotOnline.
func joinVotersAfterBootstrap(voters []*topodatapb.TabletAlias, tabletInfos []*topo.TabletInfo, bootstrapped *topodatapb.Tablet, logger *log.PrefixedLogger) {
	for _, ti := range tabletInfos {
		tablet := ti.Tablet
		if !policy.IsVoter(voters, tablet.Alias) || topoproto.TabletAliasEqual(tablet.Alias, bootstrapped.Alias) {
			continue
		}
		go func() {
			aliasString := topoproto.TabletAliasString(tablet.Alias)
			if _, err := startGroupReplication(context.Background(), tablet, &tabletmanagerdatapb.StartGroupReplicationRequest{}); err != nil {
				logger.Warn("failed to make a voter join the group that was just bootstrapped", slog.String("tablet", aliasString), slog.Any("error", err))
				return
			}
			logger.Info("a voter joined the group that was just bootstrapped", slog.String("tablet", aliasString))
		}()
	}
}

// legitimateGroupOf returns the shard's legitimate replication group from its shard record. The
// voters are identified in the members' views by the server_uuids that the statuses report, or
// that VTOrc last discovered, and by the MySQL addresses of their tablet records.
// supersedingGroupPrimary returns the reachable tablet, other than the given one, that reports its
// MySQL as the ONLINE primary, with quorum, of a view of the same incarnation as view and not older
// than it (policy.SupersedesGroupView), or nil if none does.
func supersedingGroupPrimary(statuses []*shardTabletStatus, alias *topodatapb.TabletAlias, view *replicationdatapb.GroupReplicationStatus) *shardTabletStatus {
	for _, st := range statuses {
		if st.err != nil || topoproto.TabletAliasEqual(st.tablet.Alias, alias) {
			continue
		}
		if policy.SupersedesGroupView(st.status.GetGroupReplicationStatus(), view) {
			return st
		}
	}
	return nil
}

func legitimateGroupOf(shardInfo *topo.ShardInfo, statuses []*shardTabletStatus) *policy.LegitimateGroup {
	tablets := make(map[string]*topodatapb.Tablet, len(statuses))
	uuids := make(map[string]string, len(statuses))
	for _, st := range statuses {
		alias := topoproto.TabletAliasString(st.tablet.Alias)
		tablets[alias] = st.tablet
		if st.err == nil {
			uuids[alias] = st.status.GetServerUuid()
		} else if instance, _, err := inst.ReadInstance(st.tablet.Alias); err == nil && instance != nil {
			uuids[alias] = instance.ServerUUID
		}
	}
	// A voter whose tablet record could not be read, because its cell's topology server did not
	// answer, is identified by the server_uuid VTOrc last discovered for it.
	for _, voter := range shardInfo.GetGroupReplicationVoters() {
		alias := topoproto.TabletAliasString(voter)
		if _, ok := tablets[alias]; ok {
			continue
		}
		if instance, _, err := inst.ReadInstance(voter); err == nil && instance != nil {
			uuids[alias] = instance.ServerUUID
		}
	}
	return policy.NewLegitimateGroup(shardInfo.GetGroupReplicationIncarnation(), shardInfo.GetGroupReplicationVoters(), tablets, uuids)
}

// shardTabletStatus is the FullStatus of a tablet of a shard, or the error that reading it returned.
type shardTabletStatus struct {
	tablet *topodatapb.Tablet
	status *replicationdatapb.FullStatus
	err    error
}

// readShardTabletStatuses reads the FullStatus of the given tablets concurrently.
func readShardTabletStatuses(ctx context.Context, tabletInfos []*topo.TabletInfo) []*shardTabletStatus {
	statuses := make([]*shardTabletStatus, len(tabletInfos))
	var wg sync.WaitGroup
	for i, ti := range tabletInfos {
		statuses[i] = &shardTabletStatus{tablet: ti.Tablet}
		wg.Go(func() {
			statuses[i].status, statuses[i].err = tabletFullStatus(ctx, ti.Tablet)
		})
	}
	wg.Wait()
	return statuses
}

// chooseGroupBootstrapCandidate reads the status of every tablet of the shard and returns the
// voter whose GTID set, executed and received, contains every other voter's, with the union of all
// voters' GTID sets, which the bootstrap requires, and the statuses it read. It returns an error when
// the group must not be bootstrapped.
//
// Every transaction that a voter executed or received must be in the new group. An acknowledged
// transaction was committed by the primary that acknowledged it, so it is in a voter's executed
// set. And a member that holds a transaction the group lacks, even one only in its relay log,
// applies it when it starts its join, and MySQL then refuses the join for good (lab, MySQL 8.4.11:
// doc/failover-audit/GroupReplication.md, "Bootstrap candidate"). The candidate need not have
// executed them all yet: a transaction that a group decided while its primary crashed before it
// committed it can be in relay logs only. The tablet applies its relay log before it bootstraps
// (StartGroupReplicationRequest.required_gtid_set), and refuses if that does not cover the required
// set, after a restart of its mysqld discarded the relay log.
//
// Among the voters that hold every transaction, it prefers the target of a recent bootstrap intent
// (preferred, if set): the intent fences a bootstrap on any other tablet, and a bootstrap on the
// same target is safe. Without it, the choice changed when the old primary's tablet demoted itself,
// and the fence then kept the group down until it expired (S7d chaos scenario, VTOrc killed during
// a bootstrap). It then prefers a voter that executed every transaction already, typically the old
// primary: one that holds some only in its relay log could lose them to a restart of its mysqld
// before the bootstrap, which the tablet would then refuse.
func chooseGroupBootstrapCandidate(ctx context.Context, voters []*topodatapb.TabletAlias, tabletInfos []*topo.TabletInfo, preferred *topodatapb.TabletAlias, logger *log.PrefixedLogger) (*groupBootstrapCandidate, replication.GTIDSet, []*shardTabletStatus, error) {
	if len(voters) == 0 {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the shard has no voters yet")
	}
	statuses := readShardTabletStatuses(ctx, tabletInfos)

	found := 0
	var candidates []*groupBootstrapCandidate
	// starting are the tablets whose START GROUP_REPLICATION still runs: MySQL refuses to bootstrap
	// them until it ends.
	starting := make(map[string]bool)
	for _, ts := range statuses {
		aliasString := topoproto.TabletAliasString(ts.tablet.Alias)
		isMember := policy.IsVoter(voters, ts.tablet.Alias)
		if isMember {
			found++
		}
		if ts.err != nil {
			if isMember {
				return nil, nil, nil, vterrors.Wrapf(ts.err, "voter %s is unreachable", aliasString)
			}
			// A tablet that is not a voter does not take part in the group.
			logger.Warn("ignoring unreachable tablet that is not a voter", slog.String("tablet", aliasString), slog.Any("error", ts.err))
			continue
		}
		if mysql.IsGroupMemberActive(ts.status.GetGroupReplicationStatus()) {
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the MySQL of %s is already an active group member", aliasString)
		}
		if ts.status.GetGroupReplicationStatus().GetStartInProgress() {
			// The member reports OFFLINE, but its START can still end in a group of its own, next to
			// the group a bootstrap would create, and MySQL refuses to stop it, or to bootstrap that
			// member, until it ends, within about a minute. VTOrc waits for it for a while
			// (groupStartInProgressGrace): the group it may form stays super_read_only, and its tablet
			// leaves it, so waiting longer only delays the bootstrap.
			if inst.GroupStartInProgressBlocksBootstrap(ts.tablet.Alias, time.Now()) {
				return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
					"a START GROUP_REPLICATION is in progress on the MySQL of %s: not bootstrapping the group until it ends", aliasString)
			}
			starting[aliasString] = true
			logger.Warn("bootstrapping the group although a START GROUP_REPLICATION still runs on a tablet", slog.String("tablet", aliasString))
		}
		if !isMember {
			continue
		}
		executed, gtidSet, err := reparentutil.GroupMemberGTIDSets(ts.status)
		if err != nil {
			return nil, nil, nil, vterrors.Wrapf(err, "failed to read the GTID set of %s", aliasString)
		}
		candidates = append(candidates, &groupBootstrapCandidate{tablet: ts.tablet, executed: executed, gtidSet: gtidSet})
	}
	if found < len(voters) {
		// A voter whose tablet no longer exists may still hold transactions that the others lack.
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "only %d of the %d voters of the shard have a tablet", found, len(voters))
	}
	// required holds every transaction that a voter executed or received.
	var required replication.GTIDSet = replication.Mysql56GTIDSet{}
	for _, c := range candidates {
		required = required.Union(c.gtidSet)
	}

	// Among the members that hold every transaction, prefer the target of a recent bootstrap intent,
	// then a member that executed them all, then a member without a START in progress, then the shard
	// primary, then the lowest alias, so that concurrent VTOrcs make the same choice.
	slices.SortStableFunc(candidates, func(a, b *groupBootstrapCandidate) int {
		aPreferred := preferred != nil && topoproto.TabletAliasEqual(a.tablet.Alias, preferred)
		bPreferred := preferred != nil && topoproto.TabletAliasEqual(b.tablet.Alias, preferred)
		if aPreferred != bPreferred {
			if aPreferred {
				return -1
			}
			return 1
		}
		aBinlog, bBinlog := a.executed.Contains(required), b.executed.Contains(required)
		if aBinlog != bBinlog {
			if aBinlog {
				return -1
			}
			return 1
		}
		aStarting, bStarting := starting[topoproto.TabletAliasString(a.tablet.Alias)], starting[topoproto.TabletAliasString(b.tablet.Alias)]
		if aStarting != bStarting {
			if bStarting {
				return -1
			}
			return 1
		}
		aPrimary := a.tablet.Type == topodatapb.TabletType_PRIMARY
		bPrimary := b.tablet.Type == topodatapb.TabletType_PRIMARY
		if aPrimary != bPrimary {
			if aPrimary {
				return -1
			}
			return 1
		}
		return strings.Compare(topoproto.TabletAliasString(a.tablet.Alias), topoproto.TabletAliasString(b.tablet.Alias))
	})
	for _, c := range candidates {
		if c.gtidSet.Contains(required) {
			return c, required, statuses, nil
		}
	}
	var sets []string
	for _, c := range candidates {
		sets = append(sets, fmt.Sprintf("%s: %s", topoproto.TabletAliasString(c.tablet.Alias), c.gtidSet))
	}
	return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no voter has all the transactions of the others, bootstrapping any of them would lose transactions: %s", strings.Join(sets, "; "))
}

// updateGroupReplicationVoters makes the change of the shard's voters that inst.PlanGroupVoters
// decides: the initial list, SwapVoter, GrowVoter or RemoveVoter. It runs under the shard lock, and
// decides on one bounded fresh read, the shard record and the FullStatus of every tablet of the shard
// (concurrently, within groupVoterStatusesTimeout), with nothing in between that read and the
// compare-and-swap of the voters and the incarnation. A new voter then joins the group. A member that
// is no longer a voter leaves the group on its own (its tablet's sync loop), and the group primary,
// if it is not a voter, is moved to one (GroupPrimaryNotVoter).
func updateGroupReplicationVoters(ctx context.Context, analysisEntry *inst.DetectionAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(analysisEntry)
	if topologyRecovery == nil {
		message := fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another %s.", analysisEntry.AnalyzedInstanceAlias, UpdateGroupReplicationVotersRecoveryName)
		logger.Warn(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return false, nil, err
	}
	defer func() {
		if err := resolveRecovery(topologyRecovery, nil); err != nil {
			logger.Error("failed to resolve recovery", slog.String("recovery", UpdateGroupReplicationVotersRecoveryName), slog.Any("error", err))
		}
	}()

	keyspace, shard := analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard
	keyspaceShard := topoproto.KeyspaceShardString(keyspace, shard)
	read, err := readGroupVoterState(ctx, keyspace, shard)
	if err != nil {
		return false, topologyRecovery, err
	}
	plan := inst.PlanGroupVoters(read.input)
	if !plan.Action.ChangesVoters() {
		reason := plan.Reason
		if reason == "" {
			reason = "nothing to change"
		}
		message := fmt.Sprintf("not changing the voters of %s [%s]: %s", keyspaceShard, formatAliases(read.input.Voters), reason)
		logger.Warn(message)
		_ = AuditTopologyRecovery(topologyRecovery, message)
		return true, topologyRecovery, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, message)
	}
	if err := writeGroupVoters(ctx, read, plan); err != nil {
		return true, topologyRecovery, err
	}
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s: %s: changed the voters of %s from [%s] to [%s]: %s",
		UpdateGroupReplicationVotersRecoveryName, plan.Action, keyspaceShard, formatAliases(read.input.Voters), formatAliases(plan.Voters), plan.Reason))

	if plan.Added != nil && read.input.Incarnation != "" {
		// The new voter joins the group that the settled primary leads. The write is done: a failed
		// join is retried by GroupMemberNotOnline.
		aliasString := topoproto.TabletAliasString(plan.Added.Alias)
		_ = AuditTopologyRecovery(topologyRecovery, "starting group replication on the new voter "+aliasString)
		if _, err := startGroupReplication(ctx, plan.Added, &tabletmanagerdatapb.StartGroupReplicationRequest{}); err != nil {
			return true, topologyRecovery, vterrors.Wrapf(err, "changed the voters of %s, but failed to start group replication on the new voter %s", keyspaceShard, aliasString)
		}
	}
	return true, topologyRecovery, nil
}

// groupVoterState is the read of a shard on which a change of its voters, or the move of its group
// primary, is decided.
type groupVoterState struct {
	keyspace, shard string
	shardInfo       *topo.ShardInfo
	durability      policy.Durabler
	statuses        []*shardTabletStatus
	input           *inst.VoterPlanInput
}

// readGroupVoterState reads, once, the shard record and the FullStatus of every tablet of the shard,
// concurrently within groupVoterStatusesTimeout, and returns them as the input of
// inst.PlanGroupVoters. A voter whose tablet record does not exist (topo NoNode) is a deleted voter;
// one whose record cannot be read fails the read.
func readGroupVoterState(ctx context.Context, keyspace, shard string) (*groupVoterState, error) {
	keyspaceShard := topoproto.KeyspaceShardString(keyspace, shard)
	shardInfo, err := ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read the shard record of %s", keyspaceShard)
	}
	durability, err := inst.GetShardRecordDurabilityPolicy(keyspace, shardInfo.Shard)
	if err != nil {
		return nil, err
	}
	grd, ok := policy.AsGroupReplication(durability)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the durability policy of shard %s does not use group replication", keyspaceShard)
	}
	// The tablet records of a cell whose topology server does not answer are not waited for: its
	// voters count as unreachable, with a record, and never as deleted.
	tabletInfos, failedCells, err := getReachableShardTablets(ctx, keyspace, shard)
	if err != nil {
		return nil, err
	}
	recorded := make(map[string]bool, len(tabletInfos))
	for _, ti := range tabletInfos {
		recorded[topoproto.TabletAliasString(ti.Alias)] = true
	}
	// A voter whose tablet record does not exist (topo NoNode) is a deleted voter. VTOrc keeps its last
	// tablet record and server_uuid (see keepDeletedGroupVoter), and probes its vttablet in the same
	// read at that address. It is down only if the probe fails, and VTOrc's discovery last reached it
	// at least --group-replication-voter-replacement-grace-period ago: a host that is gone and a
	// vttablet that is only slow both fail the probe, but the discovery keeps reaching a slow one. A
	// deleted voter that VTOrc has no address or instance for is not down.
	deleted := make(map[string]*inst.DeletedVoter)
	reachedLongAgo := make(map[string]bool)
	var probes []*topo.TabletInfo
	var unreadable []*topodatapb.TabletAlias
	for _, voter := range shardInfo.GetGroupReplicationVoters() {
		alias := topoproto.TabletAliasString(voter)
		if recorded[alias] {
			continue
		}
		if slices.Contains(failedCells, voter.GetCell()) {
			unreadable = append(unreadable, voter)
			continue
		}
		if _, err := ts.GetTablet(ctx, voter); err == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "voter %s of %s has a tablet record, but the shard's tablet list does not list it", alias, keyspaceShard)
		} else if !topo.IsErrType(err, topo.NoNode) {
			return nil, vterrors.Wrapf(err, "failed to read the tablet record of voter %s of %s", alias, keyspaceShard)
		}
		dv := &inst.DeletedVoter{Alias: voter}
		if instance, _, err := inst.ReadInstance(voter); err == nil && instance != nil {
			dv.ServerUUID = instance.ServerUUID
			reachedLongAgo[alias] = !instance.SecondsSinceLastSeen.Valid ||
				time.Duration(instance.SecondsSinceLastSeen.Int64)*time.Second >= config.GetGroupReplicationVoterReplacementGracePeriod()
		}
		if cached, err := inst.ReadTablet(voter); err == nil && cached != nil {
			dv.Tablet = cached
			probes = append(probes, &topo.TabletInfo{Tablet: cached})
		}
		deleted[alias] = dv
	}
	readCtx, cancel := context.WithTimeout(ctx, groupVoterStatusesTimeout)
	read := readShardTabletStatuses(readCtx, append(slices.Clone(tabletInfos), probes...))
	cancel()
	statuses, probed := read[:len(tabletInfos)], read[len(tabletInfos):]
	now := time.Now()

	in := &inst.VoterPlanInput{
		Durability:  grd,
		Voters:      shardInfo.GetGroupReplicationVoters(),
		Incarnation: shardInfo.GetGroupReplicationIncarnation(),
		GracePeriod: config.GetGroupReplicationVoterReplacementGracePeriod(),
		Fresh:       true,
		// The bootstrap intent's age is measured on this VTOrc's clock, as the bootstrap does.
		BootstrapIntentLive: reparentutil.LiveGroupReplicationBootstrapIntent(shardInfo.Shard, now) != nil,
	}
	for _, st := range statuses {
		alias := topoproto.TabletAliasString(st.tablet.Alias)
		vt := &inst.VoterTablet{Tablet: st.tablet}
		if st.err == nil {
			vt.Reachable = true
			vt.Status = st.status.GetGroupReplicationStatus()
			vt.ServerUUID = st.status.GetServerUuid()
			if executed, _, err := reparentutil.GroupMemberGTIDSets(st.status); err == nil {
				vt.Executed = executed
			}
		} else {
			vt.UnreachableFor = inst.UnreachableGroupTablets.Observe(alias, now)
			if instance, _, err := inst.ReadInstance(st.tablet.Alias); err == nil && instance != nil {
				vt.ServerUUID = instance.ServerUUID
			}
		}
		in.Tablets = append(in.Tablets, vt)
	}
	for _, voter := range unreadable {
		// Not read, so not measured: it is not failed either.
		tablet := &topodatapb.Tablet{Alias: voter, Keyspace: keyspace, Shard: shard}
		if cached, err := inst.ReadTablet(voter); err == nil && cached != nil {
			tablet = cached
		}
		vt := &inst.VoterTablet{Tablet: tablet}
		if instance, _, err := inst.ReadInstance(voter); err == nil && instance != nil {
			vt.ServerUUID = instance.ServerUUID
		}
		in.Tablets = append(in.Tablets, vt)
	}
	if len(deleted) > 0 {
		in.DeletedVoters = deleted
	}
	for _, st := range probed {
		dv := deleted[topoproto.TabletAliasString(st.tablet.Alias)]
		if st.err == nil {
			dv.ServerUUID = st.status.GetServerUuid()
		}
		dv.Down = st.err != nil && reachedLongAgo[topoproto.TabletAliasString(st.tablet.Alias)]
	}
	return &groupVoterState{keyspace: keyspace, shard: shard, shardInfo: shardInfo, durability: durability, statuses: statuses, input: in}, nil
}

// writeGroupVoters writes the plan's voter list with a compare-and-swap on the voters and the
// incarnation that the plan was decided on: another VTOrc, whose shard lock expired or that took it
// after this one's expired, may have written since.
func writeGroupVoters(ctx context.Context, read *groupVoterState, plan *inst.VoterPlan) error {
	keyspaceShard := topoproto.KeyspaceShardString(read.keyspace, read.shard)
	if err := topo.CheckShardLocked(ctx, read.keyspace, read.shard); err != nil {
		return vterrors.Wrapf(err, "lost the lock of %s before writing its voters", keyspaceShard)
	}
	current, incarnation := read.input.Voters, read.input.Incarnation
	_, err := ts.UpdateShardFields(ctx, read.keyspace, read.shard, func(si *topo.ShardInfo) error {
		if !inst.SameGroupReplicationVoters(si.GroupReplicationVoters, current) || si.GroupReplicationIncarnation != incarnation {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"the shard record of %s changed concurrently (voters [%s], incarnation %q; expected voters [%s], incarnation %q): not writing the voters [%s]",
				keyspaceShard, formatAliases(si.GroupReplicationVoters), si.GroupReplicationIncarnation, formatAliases(current), incarnation, formatAliases(plan.Voters))
		}
		si.GroupReplicationVoters = plan.Voters
		return nil
	})
	if err != nil {
		return vterrors.Wrapf(err, "failed to write the voters of %s", keyspaceShard)
	}
	return nil
}

// formatAliases formats a list of tablet aliases for a message.
func formatAliases(aliases []*topodatapb.TabletAlias) string {
	aliasStrings := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		aliasStrings = append(aliasStrings, topoproto.TabletAliasString(alias))
	}
	return strings.Join(aliasStrings, ", ")
}

// stopGroupReplication calls the StopGroupReplication RPC for the given tablet.
func stopGroupReplication(ctx context.Context, tablet *topodatapb.Tablet) error {
	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	_, err := tmc.StopGroupReplication(ctx, tablet)
	return err
}

// tabletFullStatus calls the FullStatus RPC for the given tablet.
func tabletFullStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	return tmc.FullStatus(ctx, tablet)
}

// startGroupReplication calls the StartGroupReplication RPC for the given tablet. Joining a group
// includes the distributed recovery of the missing transactions, so the RPC gets the longer
// --wait-replicas-timeout.
func startGroupReplication(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.StartGroupReplicationRequest) (*replicationdatapb.GroupReplicationStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, max(topo.RemoteOperationTimeout, config.GetWaitReplicasTimeout()))
	defer cancel()
	return tmc.StartGroupReplication(ctx, tablet, req)
}

// populateReparentJournal records in the reparent journal of the given primary that it became
// the shard primary at its current position.
func populateReparentJournal(ctx context.Context, primary *topodatapb.Tablet, actionName string) error {
	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	position, err := tmc.PrimaryPosition(ctx, primary)
	if err != nil {
		return err
	}
	return tmc.PopulateReparentJournal(ctx, primary, time.Now().UnixNano(), actionName, primary.Alias, position)
}

// isGroupReplicationVoter returns whether the tablet is a voter of its shard's replication group:
// the durability policy uses Group Replication and allows the tablet in the group, and the shard
// record lists it among the voters, or lists no voter yet. When the shard record cannot be read, a
// tablet that the policy allows in the group counts as a voter.
func isGroupReplicationVoter(ctx context.Context, durability policy.Durabler, tablet *topodatapb.Tablet) bool {
	if !policy.IsGroupMember(durability, tablet) {
		return false
	}
	shardCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	shardInfo, err := ts.GetShard(shardCtx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return true
	}
	voters := shardInfo.GetGroupReplicationVoters()
	return len(voters) == 0 || policy.IsVoter(voters, tablet.Alias)
}
