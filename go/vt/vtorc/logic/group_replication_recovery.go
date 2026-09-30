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
	"slices"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
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
)

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
	// still the group's primary before making its tablet the shard primary.
	status, err := tabletFullStatus(ctx, tablet)
	if err != nil {
		return true, topologyRecovery, vterrors.Wrapf(err, "failed to read the status of %s", aliasString)
	}
	if !mysql.IsGroupPrimary(status.GetGroupReplicationStatus()) {
		return true, topologyRecovery, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the MySQL of %s is no longer the primary of a group with quorum", aliasString)
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
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("starting group replication on %s", aliasString))
	if _, err := startGroupReplication(ctx, tablet, false); err != nil {
		return true, topologyRecovery, vterrors.Wrapf(err, "failed to start group replication on %s", aliasString)
	}
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s: %s joined its group", StartGroupReplicationRecoveryName, aliasString))
	return true, topologyRecovery, nil
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
	tabletInfos, err := getShardTablets(ctx, analysisEntry.AnalyzedKeyspace, analysisEntry.AnalyzedShard)
	if err != nil {
		return false, topologyRecovery, err
	}
	candidate, err := chooseGroupBootstrapCandidate(ctx, durability, tabletInfos, logger)
	if err != nil {
		_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("not bootstrapping the group: %v", err))
		return true, topologyRecovery, err
	}

	aliasString := topoproto.TabletAliasString(candidate.tablet.Alias)
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("bootstrapping the replication group on %s, which has the most advanced GTID set %s", aliasString, candidate.gtidSet))
	if _, err := startGroupReplication(ctx, candidate.tablet, true); err != nil {
		return true, topologyRecovery, vterrors.Wrapf(err, "failed to bootstrap the replication group on %s", aliasString)
	}
	bootstrapped = &inst.Instance{InstanceAlias: candidate.tablet.Alias}
	_ = inst.AuditOperation(BootstrapGroupReplicationRecoveryName, candidate.tablet.Alias, "bootstrapped the replication group")
	_ = AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("%s: bootstrapped the replication group on %s", BootstrapGroupReplicationRecoveryName, aliasString))
	return true, topologyRecovery, nil
}

// chooseGroupBootstrapCandidate reads the status of every tablet of the shard and returns the
// voting member whose GTID set contains every other voting member's. It returns an error when the
// group must not be bootstrapped.
func chooseGroupBootstrapCandidate(ctx context.Context, durability policy.Durabler, tabletInfos []*topo.TabletInfo, logger *log.PrefixedLogger) (*groupBootstrapCandidate, error) {
	type tabletStatus struct {
		tablet *topodatapb.Tablet
		status *replicationdatapb.FullStatus
		err    error
	}
	statuses := make([]*tabletStatus, len(tabletInfos))
	var wg sync.WaitGroup
	for i, ti := range tabletInfos {
		statuses[i] = &tabletStatus{tablet: ti.Tablet}
		wg.Go(func() {
			statuses[i].status, statuses[i].err = tabletFullStatus(ctx, ti.Tablet)
		})
	}
	wg.Wait()

	var candidates []*groupBootstrapCandidate
	for _, ts := range statuses {
		aliasString := topoproto.TabletAliasString(ts.tablet.Alias)
		isMember := policy.IsGroupMember(durability, ts.tablet)
		if ts.err != nil {
			if isMember {
				return nil, vterrors.Wrapf(ts.err, "voting member %s is unreachable", aliasString)
			}
			// A tablet that the policy does not make a member does not take part in the group.
			logger.Warn("ignoring unreachable non-member tablet", slog.String("tablet", aliasString), slog.Any("error", ts.err))
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
	if len(candidates) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the shard has no voting member")
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
	return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no voting member has all the transactions of the others, bootstrapping any of them would lose transactions: %s", strings.Join(sets, "; "))
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
