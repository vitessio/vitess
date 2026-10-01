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

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
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
)

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
	if !legitimate.IsLegitimatePrimary(status.GetGroupReplicationStatus()) {
		gs := status.GetGroupReplicationStatus()
		return true, topologyRecovery, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"the MySQL of %s is not the primary of the shard's replication group (view %s, recorded incarnation %q, %d of %d voters ONLINE)",
			aliasString, gs.GetViewId(), legitimate.Incarnation, legitimate.OnlineVoters(gs), len(legitimate.Voters))
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
func startGroupReplicationOnMember(ctx context.Context, analysisEntry *inst.DetectionAnalysis, logger *log.PrefixedLogger) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
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
	aliasString := topoproto.TabletAliasString(tablet.Alias)
	if err := checkLegitimateGroupActive(ctx, tablet); err != nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("not starting group replication on %s: %v", aliasString, err))
		return true, topologyRecovery, err
	}
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("starting group replication on %s", aliasString))
	if _, err := startGroupReplication(ctx, tablet, false); err != nil {
		return true, topologyRecovery, vterrors.Wrapf(err, "failed to start group replication on %s", aliasString)
	}
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s: %s joined its group", StartGroupReplicationRecoveryName, aliasString))
	return true, topologyRecovery, nil
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
	// gtidSet is the union of the transactions the member executed and the transactions it
	// received from its group but has not applied yet.
	gtidSet replication.GTIDSet
}

// bootstrapGroupReplication bootstraps the shard's group on the voting member with the most
// advanced GTID set. The other members then join it (GroupMemberNotOnline, or their own
// reconcile loop). It runs under the shard lock, after VTOrc refreshed all tablets of the shard.
//
// Bootstrapping a second group would split the shard's data, so the recovery re-reads the
// status of every tablet of the shard and gives up when a voting member cannot be reached, when
// any tablet is already an active group member, or when no member's GTID set contains all the
// others' (the members have diverged, and choosing one would lose transactions).
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

	durability, err := inst.GetDurabilityPolicy(analysisEntry.AnalyzedKeyspace)
	if err != nil {
		return false, topologyRecovery, err
	}
	if !policy.IsGroupReplication(durability) {
		return false, topologyRecovery, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the durability policy of keyspace %s does not use group replication", analysisEntry.AnalyzedKeyspace)
	}
	// The shard is locked, so the voters cannot change until the group is bootstrapped.
	shardInfo, err := ts.GetShard(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
	if err != nil {
		return false, topologyRecovery, vterrors.Wrapf(err, "failed to read the shard record of %s", topoproto.KeyspaceShardString(analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard))
	}
	tabletInfos, err := getShardTablets(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
	if err != nil {
		return false, topologyRecovery, err
	}
	candidate, err := chooseGroupBootstrapCandidate(ctx, shardInfo.GroupReplicationVoters, tabletInfos, logger)
	if err != nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("not bootstrapping the group: %v", err))
		return true, topologyRecovery, err
	}

	aliasString := topoproto.TabletAliasString(candidate.tablet.Alias)
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("bootstrapping the replication group on %s, which has the most advanced GTID set %s", aliasString, candidate.gtidSet))
	groupStatus, err := startGroupReplication(ctx, candidate.tablet, true)
	if err != nil {
		return true, topologyRecovery, vterrors.Wrapf(err, "failed to bootstrap the replication group on %s", aliasString)
	}
	bootstrapped = &inst.Instance{InstanceAlias: candidate.tablet.Alias}
	// The new group is the shard's legitimate group: record its incarnation while the shard is
	// still locked, before any other member joins it.
	incarnation := policy.GroupIncarnation(groupStatus.GetViewId())
	if incarnation == "" {
		return true, topologyRecovery, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "bootstrapped the replication group on %s, but it reports no view id", aliasString)
	}
	if err := reparentutil.WriteGroupReplicationIncarnation(ctx, ts, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard, incarnation); err != nil {
		return true, topologyRecovery, vterrors.Wrapf(err, "bootstrapped the replication group on %s, but failed to record its incarnation %s", aliasString, incarnation)
	}
	// Until VTOrc refreshes its copy of the shard record, its analysis would take the new group
	// for a foreign one, and not make the other voters join it.
	if shardInfo, err := ts.GetShard(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard); err == nil {
		if err := inst.SaveShard(shardInfo); err != nil {
			logger.Warn("failed to save the shard record", slog.Any("error", err))
		}
	}
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("recorded the group incarnation %s", incarnation))
	joinVotersAfterBootstrap(shardInfo.GroupReplicationVoters, tabletInfos, candidate.tablet, logger)
	_ = inst.AuditOperation(BootstrapGroupReplicationRecoveryName, candidate.tablet.Alias, "bootstrapped the replication group")
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s: bootstrapped the replication group on %s", BootstrapGroupReplicationRecoveryName, aliasString))
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
			if _, err := startGroupReplication(context.Background(), tablet, false); err != nil {
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
// voter whose GTID set contains every other voter's. It returns an error when the group must not
// be bootstrapped.
func chooseGroupBootstrapCandidate(ctx context.Context, voters []*topodatapb.TabletAlias, tabletInfos []*topo.TabletInfo, logger *log.PrefixedLogger) (*groupBootstrapCandidate, error) {
	if len(voters) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the shard has no voters yet")
	}
	statuses := readShardTabletStatuses(ctx, tabletInfos)

	found := 0
	var candidates []*groupBootstrapCandidate
	for _, ts := range statuses {
		aliasString := topoproto.TabletAliasString(ts.tablet.Alias)
		isMember := policy.IsVoter(voters, ts.tablet.Alias)
		if isMember {
			found++
		}
		if ts.err != nil {
			if isMember {
				return nil, vterrors.Wrapf(ts.err, "voter %s is unreachable", aliasString)
			}
			// A tablet that is not a voter does not take part in the group.
			logger.Warn("ignoring unreachable tablet that is not a voter", slog.String("tablet", aliasString), slog.Any("error", ts.err))
			continue
		}
		if mysql.IsGroupMemberActive(ts.status.GetGroupReplicationStatus()) {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the MySQL of %s is already an active group member", aliasString)
		}
		if !isMember {
			continue
		}
		gtidSet, err := memberGTIDSet(ts.status)
		if err != nil {
			return nil, vterrors.Wrapf(err, "failed to read the GTID set of %s", aliasString)
		}
		candidates = append(candidates, &groupBootstrapCandidate{tablet: ts.tablet, gtidSet: gtidSet})
	}
	if found < len(voters) {
		// A voter whose tablet no longer exists may still hold transactions that the others lack.
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "only %d of the %d voters of the shard have a tablet", found, len(voters))
	}

	// Prefer the shard primary among members with equal GTID sets, then the lowest alias, so
	// that concurrent VTOrcs make the same choice.
	slices.SortStableFunc(candidates, func(a, b *groupBootstrapCandidate) int {
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
		if containsAll(c, candidates) {
			return c, nil
		}
	}
	var sets []string
	for _, c := range candidates {
		sets = append(sets, fmt.Sprintf("%s: %s", topoproto.TabletAliasString(c.tablet.Alias), c.gtidSet))
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no voter has all the transactions of the others, bootstrapping any of them would lose transactions: %s", strings.Join(sets, "; "))
}

func containsAll(c *groupBootstrapCandidate, candidates []*groupBootstrapCandidate) bool {
	for _, other := range candidates {
		if !c.gtidSet.Contains(other.gtidSet) {
			return false
		}
	}
	return true
}

// memberGTIDSet returns the transactions that a member executed or received from its group.
func memberGTIDSet(status *replicationdatapb.FullStatus) (replication.GTIDSet, error) {
	if status.GetPrimaryStatus() == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the tablet did not report its executed GTID set")
	}
	executed, err := replication.DecodePosition(status.GetPrimaryStatus().GetPosition())
	if err != nil {
		return nil, err
	}
	gtidSet := executed.GTIDSet
	if gtidSet == nil {
		gtidSet = replication.Mysql56GTIDSet{}
	}
	if received := status.GetGroupReplicationStatus().GetReceivedTransactionSet(); received != "" {
		receivedSet, err := replication.ParseMysql56GTIDSet(received)
		if err != nil {
			return nil, err
		}
		gtidSet = gtidSet.Union(receivedSet)
	}
	return gtidSet, nil
}

// updateGroupReplicationVoters writes the voters that the durability policy selects for the shard's
// group into the shard record, and makes the tablets follow the new list: new voters join the
// group, and active members that are no longer voters leave it and replicate asynchronously from
// the group primary. It runs under the shard lock, and it re-reads the shard record and the status
// of every tablet of the shard before it decides, because VTOrc's view is up to
// --instance-poll-time old.
//
// A member that is no longer a voter only leaves the group when the group keeps a majority of its
// current members without it. The group primary always keeps its seat. A member whose vttablet
// VTOrc cannot reach cannot be made to leave; it keeps its seat as long as the group sees it as an
// active member, so its cell never gets a second voter meanwhile.
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
	durability, err := inst.GetDurabilityPolicy(keyspace)
	if err != nil {
		return false, topologyRecovery, err
	}
	grd, ok := policy.AsGroupReplication(durability)
	if !ok {
		return false, topologyRecovery, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the durability policy of keyspace %s does not use group replication", keyspace)
	}
	shardInfo, err := ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return false, topologyRecovery, vterrors.Wrapf(err, "failed to read the shard record of %s", keyspaceShard)
	}
	current := shardInfo.GroupReplicationVoters
	tabletInfos, err := getShardTablets(ctx, keyspace, shard)
	if err != nil {
		return false, topologyRecovery, err
	}

	statuses := readShardTabletStatuses(ctx, tabletInfos)
	// Only the shard's legitimate group counts: a member of a group of another incarnation has
	// no quorum and is no primary here, and the group primary holds a majority of the voters.
	legitimate := legitimateGroupOf(shardInfo, statuses)
	observations := make([]*inst.VoterObservation, len(statuses))
	groupUp := false
	for i, st := range statuses {
		if st.err != nil {
			lastServerUUID := ""
			if instance, _, err := inst.ReadInstance(st.tablet.Alias); err == nil && instance != nil {
				lastServerUUID = instance.ServerUUID
			}
			observations[i] = inst.NewVoterObservation(st.tablet, nil, lastServerUUID)
			continue
		}
		observations[i] = inst.NewVoterObservation(st.tablet, st.status, "")
		gs := st.status.GetGroupReplicationStatus()
		if legitimate.IsForeignIncarnation(gs) {
			observations[i].HasQuorum = false
			observations[i].PrimaryUUID = ""
		}
		observations[i].GroupPrimary = observations[i].GroupPrimary && legitimate.IsLegitimatePrimary(gs)
		if observations[i].Active && observations[i].HasQuorum {
			groupUp = true
		}
	}
	if !groupUp && len(current) > 0 {
		return true, topologyRecovery, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the replication group of %s is not active with quorum, its voters must keep their seats", keyspaceShard)
	}
	selection := inst.SelectGroupReplicationVoters(grd, current, observations, time.Now())
	if len(selection.Voters) == 0 {
		return true, topologyRecovery, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no tablet of %s can be a voter", keyspaceShard)
	}

	if !inst.SameGroupReplicationVoters(selection.Voters, current) {
		_, err = ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
			si.GroupReplicationVoters = selection.Voters
			return nil
		})
		if err != nil {
			return true, topologyRecovery, vterrors.Wrapf(err, "failed to write the voters of %s", keyspaceShard)
		}
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("changed the voters of %s from [%s] to [%s]", keyspaceShard, formatAliases(current), formatAliases(selection.Voters)))
	}
	if !groupUp {
		// The voters are selected; GroupNotBootstrapped bootstraps the group on one of them.
		return true, topologyRecovery, nil
	}

	var groupPrimary *inst.VoterObservation
	for _, o := range observations {
		if o.Reachable && o.GroupPrimary {
			groupPrimary = o
		}
	}
	var errs []error
	for i, o := range observations {
		aliasString := topoproto.TabletAliasString(o.Tablet.Alias)
		isVoter := policy.IsVoter(selection.Voters, o.Tablet.Alias)
		switch {
		case isVoter && !policy.IsVoter(current, o.Tablet.Alias) && o.Reachable && !o.Active:
			_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("starting group replication on the new voter %s", aliasString))
			if _, err := startGroupReplication(ctx, o.Tablet, false); err != nil {
				errs = append(errs, vterrors.Wrapf(err, "failed to start group replication on the new voter %s", aliasString))
			}
		case isVoter || topoproto.TabletAliasEqual(o.Tablet.Alias, selection.GroupPrimary) || !selection.IsActive(o.Tablet.Alias):
		case !o.Reachable:
			// Its MySQL still runs Group Replication, but only its vttablet could make it leave.
			message := fmt.Sprintf("%s is no longer a voter, but it is unreachable while its MySQL is still an active member of the group: it stays in the group", aliasString)
			logger.Warn(message)
			_ = AuditTopologyRecovery(topologyRecovery, message)
		case groupPrimary == nil:
			errs = append(errs, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s is no longer a voter, but the group primary is unreachable, so it cannot replicate from it", aliasString))
		default:
			heartbeatInterval := float64(statuses[i].status.GetReplicationConfiguration().GetReplicaNetTimeout()) / 2
			if err := leaveGroupReplication(ctx, o, groupPrimary.Tablet, heartbeatInterval, topologyRecovery); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if err := vterrors.Aggregate(errs); err != nil {
		return true, topologyRecovery, err
	}
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s: the voters of %s are [%s]", UpdateGroupReplicationVotersRecoveryName, keyspaceShard, formatAliases(selection.Voters)))
	return true, topologyRecovery, nil
}

// leaveGroupReplication makes an active member that is no longer a voter leave its group, and
// then replicate asynchronously from the group primary. It does nothing when the group would not
// keep a majority of its current members without the member.
func leaveGroupReplication(ctx context.Context, member *inst.VoterObservation, primary *topodatapb.Tablet, heartbeatInterval float64, topologyRecovery *TopologyRecovery) error {
	aliasString := topoproto.TabletAliasString(member.Tablet.Alias)
	primaryStatus, err := tabletFullStatus(ctx, primary)
	if err != nil {
		return vterrors.Wrapf(err, "failed to read the status of the group primary %s", topoproto.TabletAliasString(primary.Alias))
	}
	gr := primaryStatus.GetGroupReplicationStatus()
	if !mysql.IsGroupPrimary(gr) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s is no longer the group primary", topoproto.TabletAliasString(primary.Alias))
	}
	members := len(gr.GetMembers())
	onlineWithout := mysql.OnlineGroupMembers(gr)
	for _, m := range gr.GetMembers() {
		if m.GetMemberUuid() == member.ServerUUID && m.GetState() == mysql.GroupMemberStateOnline {
			onlineWithout--
		}
	}
	if 2*onlineWithout <= members {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s is no longer a voter, but the group would not keep a majority of its %d members without it (%d ONLINE)", aliasString, members, onlineWithout)
	}

	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s is no longer a voter: stopping group replication and replicating from %s", aliasString, topoproto.TabletAliasString(primary.Alias)))
	if err := stopGroupReplication(ctx, member.Tablet); err != nil {
		return vterrors.Wrapf(err, "failed to stop group replication on %s", aliasString)
	}
	// The group, not semi-sync, makes transactions durable.
	if err := setReplicationSource(ctx, member.Tablet, primary, false, heartbeatInterval); err != nil {
		return vterrors.Wrapf(err, "failed to make %s replicate from %s", aliasString, topoproto.TabletAliasString(primary.Alias))
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
func startGroupReplication(ctx context.Context, tablet *topodatapb.Tablet, bootstrap bool) (*replicationdatapb.GroupReplicationStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, max(topo.RemoteOperationTimeout, config.GetWaitReplicasTimeout()))
	defer cancel()
	return tmc.StartGroupReplication(ctx, tablet, bootstrap)
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
