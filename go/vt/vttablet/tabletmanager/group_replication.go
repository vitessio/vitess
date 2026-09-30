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

package tabletmanager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// groupReplicationPortName is the name under which a tablet publishes the port of its MySQL's
// group communication engine in its tablet record.
const groupReplicationPortName = "gr"

// defaultGroupMemberWeight is MySQL's default group_replication_member_weight. It is used when
// the keyspace durability policy does not use Group Replication, for example while a shard is
// migrated to it.
const defaultGroupMemberWeight = 50

var (
	groupReplicationPort                       int
	groupReplicationSyncInterval               = 1 * time.Second
	groupReplicationConsistency                = "BEFORE_ON_PRIMARY_FAILOVER"
	groupReplicationExitStateAction            = "READ_ONLY"
	groupReplicationUnreachableMajorityTimeout = 1 * time.Second
	groupReplicationAutorejoinTries            = -1

	// groupReplicationPollInterval is how often waits on the group replication state poll
	// MySQL. It can be changed to speed up tests.
	groupReplicationPollInterval = 100 * time.Millisecond
	// groupReplicationMaxRejoinBackoff caps the delay between two attempts of the sync loop to
	// rejoin the group.
	groupReplicationMaxRejoinBackoff = 1 * time.Minute
	// groupReplicationDurabilityCacheTTL is how long the sync loop caches the keyspace
	// durability policy.
	groupReplicationDurabilityCacheTTL = 10 * time.Second
)

func registerGroupReplicationFlags(fs *pflag.FlagSet) {
	utils.SetFlagIntVar(fs, &groupReplicationPort, "group-replication-port", groupReplicationPort,
		"Port on which the group communication engine of this tablet's MySQL listens for MySQL Group Replication (group_replication_local_address). It is published in the tablet record. 0 disables Group Replication support on this tablet.")
	utils.SetFlagDurationVar(fs, &groupReplicationSyncInterval, "group-replication-sync-interval", groupReplicationSyncInterval,
		"How often a tablet with --group-replication-port makes its tablet type follow its MySQL's role in its replication group, and rejoins the group if needed.")
	utils.SetFlagStringVar(fs, &groupReplicationConsistency, "group-replication-consistency", groupReplicationConsistency,
		"group_replication_consistency that the tablet applies before its MySQL starts Group Replication. Empty keeps the server's setting.")
	utils.SetFlagStringVar(fs, &groupReplicationExitStateAction, "group-replication-exit-state-action", groupReplicationExitStateAction,
		"group_replication_exit_state_action that the tablet applies before its MySQL starts Group Replication. Empty keeps the server's setting.")
	utils.SetFlagDurationVar(fs, &groupReplicationUnreachableMajorityTimeout, "group-replication-unreachable-majority-timeout", groupReplicationUnreachableMajorityTimeout,
		"group_replication_unreachable_majority_timeout that the tablet applies before its MySQL starts Group Replication: how long a member that lost contact with the majority of its group waits before it leaves the group. 0 waits forever, a negative value keeps the server's setting.")
	utils.SetFlagIntVar(fs, &groupReplicationAutorejoinTries, "group-replication-autorejoin-tries", groupReplicationAutorejoinTries,
		"group_replication_autorejoin_tries that the tablet applies before its MySQL starts Group Replication. A negative value keeps the server's setting.")
}

func init() {
	servenv.OnParseFor("vttablet", registerGroupReplicationFlags)
}

// groupReplicationEnabled returns whether the tablet supports MySQL Group Replication.
func groupReplicationEnabled() bool {
	return groupReplicationPort > 0
}

// validateGroupReplicationFlags returns an error when a group replication flag has an invalid value.
func validateGroupReplicationFlags() error {
	if groupReplicationPort < 0 || groupReplicationPort > 65535 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "--group-replication-port must be between 0 and 65535, got %d", groupReplicationPort)
	}
	if groupReplicationEnabled() && groupReplicationSyncInterval <= 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "--group-replication-sync-interval must be positive, got %v", groupReplicationSyncInterval)
	}
	return nil
}

// checkGroupReplicationEnabled returns a FAILED_PRECONDITION error if the tablet does not
// support Group Replication.
func checkGroupReplicationEnabled() error {
	if !groupReplicationEnabled() {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "group replication is not enabled on this tablet: --group-replication-port is not set")
	}
	return nil
}

// groupReplicationAddress returns the host:port on which the group communication engine of the
// tablet's MySQL listens, or "" if the tablet does not publish one. The host is the one other
// tablets use to replicate from this tablet's MySQL.
func groupReplicationAddress(tablet *topodatapb.Tablet) string {
	port := tablet.PortMap[groupReplicationPortName]
	if port <= 0 {
		return ""
	}
	host := tablet.MysqlHostname
	if host == "" {
		host = tablet.Hostname
	}
	if host == "" {
		return ""
	}
	return netutil.JoinHostPort(host, port)
}

// groupReplicationSeeds returns the group communication addresses of the tablets of the shard
// other than self, sorted so that every tablet computes the same list.
func groupReplicationSeeds(self *topodatapb.TabletAlias, tablets map[string]*topo.TabletInfo) []string {
	var seeds []string
	for _, ti := range tablets {
		if ti == nil || ti.Tablet == nil || topoproto.TabletAliasEqual(ti.Alias, self) {
			continue
		}
		if addr := groupReplicationAddress(ti.Tablet); addr != "" {
			seeds = append(seeds, addr)
		}
	}
	sort.Strings(seeds)
	return seeds
}

// keyspaceDurability returns the durability policy of the tablet's keyspace.
func (tm *TabletManager) keyspaceDurability(ctx context.Context) (policy.Durabler, error) {
	keyspace := tm.Tablet().Keyspace
	durabilityName, err := tm.TopoServer.GetKeyspaceDurability(ctx, keyspace)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot read durability policy of keyspace %v", keyspace)
	}
	durability, err := policy.GetDurabilityPolicy(durabilityName)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot get durability policy %v", durabilityName)
	}
	return durability, nil
}

// groupReplicationConfig derives the Group Replication configuration of the tablet's MySQL from
// the topology: the group name from the keyspace and shard, the seeds from the other tablets of
// the shard, and the member weight from the durability policy.
func (tm *TabletManager) groupReplicationConfig(ctx context.Context, durability policy.Durabler) (mysql.GroupReplicationConfig, error) {
	tablet := tm.Tablet()
	host := tablet.MysqlHostname
	if host == "" {
		host = tablet.Hostname
	}
	if host == "" {
		return mysql.GroupReplicationConfig{}, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot derive the group replication address: the tablet has no hostname")
	}

	tablets, err := tm.TopoServer.GetTabletMapForShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil && !topo.IsErrType(err, topo.PartialResult) {
		return mysql.GroupReplicationConfig{}, vterrors.Wrapf(err, "cannot read the tablets of shard %v/%v", tablet.Keyspace, tablet.Shard)
	}

	weight := defaultGroupMemberWeight
	if grd, ok := policy.AsGroupReplication(durability); ok {
		weighted := tablet.CloneVT()
		if isTransitionalTabletType(weighted.Type) {
			// A tablet that joins at the end of a restore returns to its base type.
			weighted.Type = tm.baseTabletType
		}
		weight = grd.MemberWeight(weighted)
	}

	unreachableMajorityTimeout := -1
	if groupReplicationUnreachableMajorityTimeout >= 0 {
		unreachableMajorityTimeout = int(groupReplicationUnreachableMajorityTimeout.Round(time.Second) / time.Second)
	}

	return mysql.GroupReplicationConfig{
		GroupName:                         policy.GroupName(tablet.Keyspace, tablet.Shard),
		LocalAddress:                      netutil.JoinHostPort(host, int32(groupReplicationPort)),
		Seeds:                             groupReplicationSeeds(tablet.Alias, tablets),
		MemberWeight:                      weight,
		Consistency:                       groupReplicationConsistency,
		ExitStateAction:                   groupReplicationExitStateAction,
		UnreachableMajorityTimeoutSeconds: unreachableMajorityTimeout,
		AutorejoinTries:                   groupReplicationAutorejoinTries,
	}, nil
}

// groupReplicationStatus returns the Group Replication state of the tablet's MySQL.
func (tm *TabletManager) groupReplicationStatus(ctx context.Context) (*replicationdatapb.GroupReplicationStatus, error) {
	status, err := tm.MysqlDaemon.GroupReplicationStatus(ctx)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read the group replication status")
	}
	return status, nil
}

// checkGroupAllowsReadWrite returns a FAILED_PRECONDITION error if the tablet's MySQL is an
// active member of a group but not its primary. Such a member must never be made writable: a
// Group Replication secondary without super_read_only accepts writes and replicates them to the
// whole group.
func (tm *TabletManager) checkGroupAllowsReadWrite(ctx context.Context) error {
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		return err
	}
	if mysql.IsGroupMemberActive(status) && !mysql.IsGroupPrimary(status) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"refusing to make MySQL writable: it is a %s %s member of group %s but not the primary of a group with quorum",
			status.MemberState, status.MemberRole, status.GroupName)
	}
	return nil
}

// twoPCDurable returns whether a primary with this group replication state and primary-side
// semi-sync setting makes prepared transactions durable enough for atomic transactions.
func twoPCDurable(status *replicationdatapb.GroupReplicationStatus, semiSync bool) bool {
	return semiSync || mysql.GroupSupersedesSemiSync(status)
}

// startGroupReplicationLocked makes the tablet's MySQL join its shard's group, or bootstrap a
// new group, unless it is already an active member. It then finishes the transition to Group
// Replication, which is idempotent. It does not wait for the member to become ONLINE.
//
// The caller must hold the action lock, or run during startup.
func (tm *TabletManager) startGroupReplicationLocked(ctx context.Context, bootstrap bool) (*replicationdatapb.GroupReplicationStatus, error) {
	tablet := tm.Tablet()
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		return nil, err
	}
	if mysql.IsGroupMemberActive(status) {
		if bootstrap {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot bootstrap a group: MySQL is already %s in group %s", status.MemberState, status.GroupName)
		}
		if groupName := policy.GroupName(tablet.Keyspace, tablet.Shard); status.GroupName != groupName {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "MySQL is %s in group %s, but the group of shard %s/%s is %s", status.MemberState, status.GroupName, tablet.Keyspace, tablet.Shard, groupName)
		}
	} else if err := tm.joinGroupLocked(ctx, status, bootstrap); err != nil {
		return nil, err
	}
	return tm.finishGroupJoinLocked(ctx)
}

// joinGroupLocked configures Group Replication and starts it on a MySQL that is not an active
// member.
func (tm *TabletManager) joinGroupLocked(ctx context.Context, status *replicationdatapb.GroupReplicationStatus, bootstrap bool) error {
	durability, err := tm.keyspaceDurability(ctx)
	if err != nil {
		return err
	}
	cfg, err := tm.groupReplicationConfig(ctx, durability)
	if err != nil {
		return err
	}

	// A member that failed or was expelled keeps Group Replication running in the ERROR state,
	// in which MySQL refuses to change its configuration.
	if status.PluginActive && status.MemberState == mysql.GroupMemberStateError {
		if err := tm.MysqlDaemon.StopGroupReplication(ctx); err != nil {
			return vterrors.Wrapf(err, "failed to stop group replication of a member in the ERROR state")
		}
	}

	// MySQL refuses to start Group Replication while the default channel runs (ERROR 3092).
	stoppedReplication, err := tm.stopReplicationForGroupJoin(ctx)
	if err != nil {
		return err
	}
	restartReplication := func() {
		if !stoppedReplication {
			return
		}
		// Keep the tablet replicating asynchronously, and acknowledging semi-sync
		// transactions, if it could not join the group.
		restartCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), topo.RemoteOperationTimeout)
		defer cancel()
		if err := tm.MysqlDaemon.StartReplication(restartCtx, tm.hookExtraEnv()); err != nil {
			log.Warn("Failed to restart replication after a failed group join", slog.Any("error", err))
		}
	}

	if err := tm.MysqlDaemon.ConfigureGroupReplication(ctx, cfg); err != nil {
		restartReplication()
		return vterrors.Wrapf(err, "failed to configure group replication")
	}
	log.Info("Starting group replication",
		slog.String("group", cfg.GroupName),
		slog.String("local_address", cfg.LocalAddress),
		slog.Any("seeds", cfg.Seeds),
		slog.Bool("bootstrap", bootstrap))
	if err := tm.MysqlDaemon.StartGroupReplication(ctx, bootstrap); err != nil {
		restartReplication()
		return vterrors.Wrapf(err, "failed to start group replication")
	}
	return nil
}

// stopReplicationForGroupJoin stops the default replication channel if it runs, and returns
// whether it did.
func (tm *TabletManager) stopReplicationForGroupJoin(ctx context.Context) (bool, error) {
	status, err := tm.MysqlDaemon.ReplicationStatus(ctx)
	if errors.Is(err, mysql.ErrNotReplica) {
		return false, nil
	}
	if err != nil {
		return false, vterrors.Wrapf(err, "failed to read the replication status")
	}
	if status.IOState == replication.ReplicationStateStopped && status.SQLState == replication.ReplicationStateStopped {
		return false, nil
	}
	if err := tm.MysqlDaemon.StopReplication(ctx, tm.hookExtraEnv()); err != nil {
		return false, vterrors.Wrapf(err, "failed to stop replication before joining the group")
	}
	return true, nil
}

// finishGroupJoinLocked completes the transition of an active member to Group Replication:
//   - It removes the default replication channel, so that it cannot be restarted by accident
//     (on a member, START REPLICA fails in the SQL thread).
//   - On a secondary, it disables semi-sync, which the member no longer uses.
//   - On the primary of a PRIMARY tablet, it makes MySQL writable and applies the rule that a
//     group with two ONLINE members supersedes semi-sync. Semi-sync otherwise stays as it is: a
//     primary that bootstraps a group during a migration still has asynchronous replicas
//     acknowledging its transactions.
func (tm *TabletManager) finishGroupJoinLocked(ctx context.Context) (*replicationdatapb.GroupReplicationStatus, error) {
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		return nil, err
	}
	if !mysql.IsGroupMemberActive(status) {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "MySQL is %s after starting group replication", status.MemberState)
	}
	if err := tm.MysqlDaemon.ExecuteSuperQueryList(ctx, []string{mysql.ResetDefaultReplicationChannelCommand()}); err != nil {
		return nil, vterrors.Wrapf(err, "failed to reset the default replication channel")
	}

	tablet := tm.Tablet()
	switch {
	case mysql.IsGroupPrimary(status):
		if tablet.Type == topodatapb.TabletType_PRIMARY {
			// The primary kept serving while it bootstrapped the group, and MySQL did not
			// restart, so prepared transactions are intact: only clear the read-only flags if
			// Group Replication left them set. Redoing prepared transactions would restart the
			// transaction engine and fail in-flight queries.
			if err := tm.setGroupPrimaryWritable(ctx); err != nil {
				return nil, vterrors.Wrapf(err, "failed to make the group primary writable")
			}
			if mysql.GroupSupersedesSemiSync(status) && tm.isPrimarySideSemiSyncEnabled(ctx) {
				if err := tm.disableSemiSync(ctx, tablet.Type); err != nil {
					return nil, err
				}
			}
		}
	case tablet.Type != topodatapb.TabletType_PRIMARY:
		if err := tm.disableSemiSync(ctx, tablet.Type); err != nil {
			return nil, err
		}
	}
	return status, nil
}

// disableSemiSync disables both sides of semi-sync, if the semi-sync plugin is loaded.
func (tm *TabletManager) disableSemiSync(ctx context.Context, tabletType topodatapb.TabletType) error {
	semiSyncAction, err := tm.convertBoolToSemiSyncAction(ctx, false)
	if err != nil {
		return err
	}
	if err := tm.fixSemiSync(ctx, tabletType, semiSyncAction); err != nil {
		return vterrors.Wrapf(err, "failed to disable semi-sync")
	}
	return nil
}

// waitForGroupMemberOnline waits until the tablet's MySQL is an ONLINE member of its group. A
// RECOVERING member is still catching up with the group.
func (tm *TabletManager) waitForGroupMemberOnline(ctx context.Context) (*replicationdatapb.GroupReplicationStatus, error) {
	for {
		status, err := tm.groupReplicationStatus(ctx)
		if err != nil {
			return nil, err
		}
		switch status.MemberState {
		case mysql.GroupMemberStateOnline:
			return status, nil
		case mysql.GroupMemberStateRecovering:
		default:
			return status, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "group member is %s instead of ONLINE", status.MemberState)
		}
		select {
		case <-ctx.Done():
			return status, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "timed out waiting for the group member to become ONLINE, it is %s: %v", status.MemberState, ctx.Err())
		case <-time.After(groupReplicationPollInterval):
		}
	}
}

// waitForGroupPrimaryWritable waits until the tablet's MySQL is the primary of its group and
// Group Replication has lifted super_read_only.
func (tm *TabletManager) waitForGroupPrimaryWritable(ctx context.Context) error {
	for {
		status, err := tm.groupReplicationStatus(ctx)
		if err != nil {
			return err
		}
		if mysql.IsGroupPrimary(status) {
			superReadOnly, err := tm.MysqlDaemon.IsSuperReadOnly(ctx)
			if err != nil {
				return err
			}
			if !superReadOnly {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "timed out waiting for MySQL to become the writable primary of its group (member %s %s): %v", status.MemberState, status.MemberRole, ctx.Err())
		case <-time.After(groupReplicationPollInterval):
		}
	}
}

// stopGroupReplicationLocked makes the tablet's MySQL leave its group. MySQL stays
// super_read_only afterwards. If the member was the primary of the group and the tablet is
// PRIMARY, which is the last step of a migration back to asynchronous replication, the tablet
// makes MySQL writable again and applies the semi-sync setting of the durability policy.
func (tm *TabletManager) stopGroupReplicationLocked(ctx context.Context) (*replicationdatapb.GroupReplicationStatus, error) {
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		return nil, err
	}
	if !status.PluginActive || status.MemberState == mysql.GroupMemberStateOffline {
		return status, nil
	}
	tablet := tm.Tablet()
	leavingPrimary := mysql.IsGroupPrimary(status) && tablet.Type == topodatapb.TabletType_PRIMARY
	if leavingPrimary {
		// While the primary leaves its group, MySQL rejects commits (the before_commit hook
		// fails) and then turns read-only. Stop serving first, like DemotePrimary does, so that
		// vtgate buffers writes instead of failing them.
		log.Info("Group primary is leaving its group, disabling query service")
		termStart := protoutil.TimeFromProto(tablet.PrimaryTermStartTime).UTC()
		if err := tm.QueryServiceControl.SetServingType(tablet.Type, termStart, false, "leaving the replication group"); err != nil {
			return nil, vterrors.Wrap(err, "SetServingType(serving=false) failed")
		}
		defer func() {
			if err := tm.QueryServiceControl.SetServingType(tablet.Type, termStart, true, ""); err != nil {
				log.Warn(fmt.Sprintf("SetServingType(serving=true) failed after leaving the replication group: %v", err))
			}
		}()
	}
	log.Info("Stopping group replication", slog.String("group", status.GroupName), slog.String("state", status.MemberState), slog.String("role", status.MemberRole))
	if err := tm.MysqlDaemon.StopGroupReplication(ctx); err != nil {
		return nil, vterrors.Wrapf(err, "failed to stop group replication")
	}

	if leavingPrimary {
		if err := tm.redoPreparedTransactionsAndSetReadWrite(ctx); err != nil {
			return nil, vterrors.Wrapf(err, "failed to make the primary writable after it left its group")
		}
		if err := tm.fixPrimarySemiSyncFromPolicy(ctx); err != nil {
			return nil, err
		}
	}
	return tm.groupReplicationStatus(ctx)
}

// setGroupPrimaryWritable clears super_read_only and read_only on the primary of a group, if
// they are set, without touching the query service.
func (tm *TabletManager) setGroupPrimaryWritable(ctx context.Context) error {
	if err := tm.checkGroupAllowsReadWrite(ctx); err != nil {
		return err
	}
	superReadOnly, err := tm.MysqlDaemon.IsSuperReadOnly(ctx)
	if err != nil {
		return err
	}
	if superReadOnly {
		if _, err := tm.MysqlDaemon.SetSuperReadOnly(ctx, false); err != nil {
			return err
		}
	}
	readOnly, err := tm.MysqlDaemon.IsReadOnly(ctx)
	if err != nil {
		return err
	}
	if readOnly {
		return tm.MysqlDaemon.SetReadOnly(ctx, false)
	}
	return nil
}

// fixPrimarySemiSyncFromPolicy applies the primary semi-sync setting of the durability policy.
func (tm *TabletManager) fixPrimarySemiSyncFromPolicy(ctx context.Context) error {
	durability, err := tm.keyspaceDurability(ctx)
	if err != nil {
		return err
	}
	semiSync := policy.SemiSyncAckers(durability, tm.Tablet()) > 0
	semiSyncAction, err := tm.convertBoolToSemiSyncAction(ctx, semiSync)
	if err != nil {
		return err
	}
	tm.QueryServiceControl.SetTwoPCAllowed(tabletserver.TwoPCAllowed_SemiSync, semiSync)
	return tm.fixSemiSync(ctx, topodatapb.TabletType_PRIMARY, semiSyncAction)
}

// promoteGroupMemberLocked implements PromoteReplica on an active group member: it makes the
// member the group's primary, unless it already is, waits until Group Replication has made it
// writable, and changes the tablet type. It does not touch the default replication channel.
func (tm *TabletManager) promoteGroupMemberLocked(ctx context.Context, status *replicationdatapb.GroupReplicationStatus, semiSyncAction SemiSyncAction) (string, error) {
	if !mysql.IsGroupPrimary(status) {
		if status.MemberState != mysql.GroupMemberStateOnline {
			return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot promote a group member that is %s", status.MemberState)
		}
		serverUUID, err := tm.MysqlDaemon.GetServerUUID(ctx)
		if err != nil {
			return "", vterrors.Wrapf(err, "failed to read the server uuid")
		}
		log.Info("Making MySQL the primary of its group", slog.String("group", status.GroupName), slog.String("server_uuid", serverUUID), slog.String("current_primary", status.PrimaryUuid))
		if err := tm.MysqlDaemon.SetGroupReplicationPrimary(ctx, serverUUID); err != nil {
			return "", vterrors.Wrapf(err, "failed to make %s the group primary", serverUUID)
		}
	}
	if err := tm.waitForGroupPrimaryWritable(ctx); err != nil {
		return "", err
	}
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		return "", err
	}

	tm.QueryServiceControl.SetTwoPCAllowed(tabletserver.TwoPCAllowed_SemiSync, twoPCDurable(status, semiSyncAction == SemiSyncActionSet))
	if err := tm.fixSemiSync(ctx, topodatapb.TabletType_PRIMARY, semiSyncAction); err != nil {
		return "", err
	}
	pos, err := tm.MysqlDaemon.PrimaryPosition(ctx)
	if err != nil {
		return "", err
	}
	if err := tm.changeTypeLocked(ctx, topodatapb.TabletType_PRIMARY, DBActionSetReadWrite, SemiSyncActionNone); err != nil {
		return "", err
	}
	return replication.EncodePosition(pos), nil
}

// bootstrapGroupForInitPrimaryLocked bootstraps the shard's group on the tablet's MySQL, as the
// first step of InitPrimary in a keyspace whose durability policy uses Group Replication. It is
// idempotent: a MySQL that is already the only member of its group is left as it is. It refuses
// to run on a member of a group with other members.
func (tm *TabletManager) bootstrapGroupForInitPrimaryLocked(ctx context.Context) error {
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		return err
	}
	if mysql.IsGroupMemberActive(status) {
		if len(status.Members) > 1 {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot initialize the primary: MySQL is already %s in group %s with %d members", status.MemberState, status.GroupName, len(status.Members))
		}
		if _, err := tm.startGroupReplicationLocked(ctx, false); err != nil {
			return err
		}
	} else if _, err := tm.startGroupReplicationLocked(ctx, true); err != nil {
		return err
	}
	return tm.waitForGroupPrimaryWritable(ctx)
}

// isGroupReplicationManaged returns whether the tablet should be a voting member of its shard's
// group: the keyspace durability policy uses Group Replication, makes a tablet of the given type
// a member, and the tablet has a group replication port.
func (tm *TabletManager) isGroupReplicationManaged(ctx context.Context, tabletType topodatapb.TabletType) (bool, error) {
	if !groupReplicationEnabled() {
		return false, nil
	}
	durability, err := tm.keyspaceDurability(ctx)
	if err != nil {
		return false, err
	}
	tablet := tm.Tablet()
	tablet.Type = tabletType
	return policy.IsGroupMember(durability, tablet), nil
}

// setGroupMemberReplicationSourceLocked implements SetReplicationSource on an active group
// member. The member does not replicate from the new primary through the default channel: the
// group delivers its transactions. Like an asynchronous replica, it waits for the requested
// position and reparent journal entry. It disables semi-sync, which a member does not use,
// unless the caller asked to leave semi-sync alone.
func (tm *TabletManager) setGroupMemberReplicationSourceLocked(ctx context.Context, status *replicationdatapb.GroupReplicationStatus, parentAlias *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, semiSync SemiSyncAction) error {
	log.Info("MySQL is an active group replication member, not changing its replication source",
		slog.String("group", status.GroupName),
		slog.String("state", status.MemberState),
		slog.String("role", status.MemberRole),
		slog.String("parent", topoproto.TabletAliasString(parentAlias)))
	if semiSync != SemiSyncActionNone {
		if err := tm.disableSemiSync(ctx, tm.Tablet().Type); err != nil {
			return err
		}
	}
	if waitPosition != "" {
		pos, err := replication.DecodePosition(waitPosition)
		if err != nil {
			return err
		}
		if err := tm.MysqlDaemon.WaitSourcePos(ctx, pos); err != nil {
			return err
		}
	}
	if timeCreatedNS != 0 {
		if err := tm.MysqlDaemon.WaitForReparentJournal(ctx, timeCreatedNS); err != nil {
			return err
		}
	}
	return nil
}
