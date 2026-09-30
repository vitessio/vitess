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
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
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
	groupReplicationPort            int
	groupReplicationSyncInterval    = 1 * time.Second
	groupReplicationConsistency     = "BEFORE_ON_PRIMARY_FAILOVER"
	groupReplicationExitStateAction = "READ_ONLY"
	// groupReplicationAutorejoinTries is 0 by default: the sync loop rejoins an expelled member
	// once the shard's legitimate group is active elsewhere. MySQL's own auto-rejoin does not
	// check that: an attempt blocks the member for about a minute, refuses every change
	// meanwhile, and can end in a group of its own.
	groupReplicationAutorejoinTries = 0

	// groupReplicationPollInterval is how often waits on the group replication state poll
	// MySQL. It can be changed to speed up tests.
	groupReplicationPollInterval = 100 * time.Millisecond
	// groupReplicationMaxRejoinBackoff caps the delay between two attempts of the sync loop to
	// rejoin the group.
	groupReplicationMaxRejoinBackoff = 1 * time.Minute
	// groupReplicationDurabilityCacheTTL is how long the sync loop caches the keyspace
	// durability policy.
	groupReplicationDurabilityCacheTTL = 10 * time.Second
	// groupReplicationVotersCacheTTL is how long the sync loop caches the voters of the shard's
	// group, read from the shard record. It is only read when the loop considers a rejoin.
	groupReplicationVotersCacheTTL = 5 * time.Second
	// groupReplicationIllegitimateLogInterval limits how often the sync loop logs that it does not
	// follow a group primary that is not legitimate, or does not join a group.
	groupReplicationIllegitimateLogInterval = 10 * time.Second
	// groupReplicationJoinTimeout bounds a join that the sync loop starts. MySQL keeps running a
	// START GROUP_REPLICATION whose client gave up, so the loop waits for it rather than leave it
	// running in the background.
	groupReplicationJoinTimeout = 1 * time.Minute
	// groupReplicationVoterUUIDWarmInterval is how often the sync loop asks, in the background,
	// the voters whose server_uuid the tablet does not know yet for it.
	groupReplicationVoterUUIDWarmInterval = 30 * time.Second
	// groupReplicationStatusTimeout bounds every read of the group replication status.
	groupReplicationStatusTimeout = 10 * time.Second
	// groupReplicationRejoinGateInterval is how long the sync loop waits before it checks again
	// whether the shard's group is active on another tablet, when it was not.
	groupReplicationRejoinGateInterval = 2 * time.Second
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
	utils.SetFlagIntVar(fs, &groupReplicationAutorejoinTries, "group-replication-autorejoin-tries", groupReplicationAutorejoinTries,
		"group_replication_autorejoin_tries that the tablet applies before its MySQL starts Group Replication. The default 0 leaves rejoins to the tablet, which only rejoins while the shard's group is active on another tablet. A negative value keeps the server's setting.")
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

// preferSeeds moves the preferred seeds to the front of the seeds, keeping the order of both. A
// joining member contacts its seeds in order: the members that were just seen active in the
// shard's legitimate group come first, rather than members that may be leaving, or in a group of
// their own.
func preferSeeds(seeds, preferred []string) []string {
	ordered := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		if slices.Contains(preferred, seed) {
			ordered = append(ordered, seed)
		}
	}
	for _, seed := range seeds {
		if !slices.Contains(preferred, seed) {
			ordered = append(ordered, seed)
		}
	}
	return ordered
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

	return mysql.GroupReplicationConfig{
		GroupName:       policy.GroupName(tablet.Keyspace, tablet.Shard),
		LocalAddress:    netutil.JoinHostPort(host, int32(groupReplicationPort)),
		Seeds:           preferSeeds(groupReplicationSeeds(tablet.Alias, tablets), tm.groupReplicationPeers.activeSeeds()),
		MemberWeight:    weight,
		Consistency:     groupReplicationConsistency,
		ExitStateAction: groupReplicationExitStateAction,
		AutorejoinTries: groupReplicationAutorejoinTries,
	}, nil
}

// groupReplicationStatus returns the Group Replication state of the tablet's MySQL.
//
// The read is bounded by groupReplicationStatusTimeout, whatever the caller's context: the status
// queries must never hold the action lock for long. A member that was expelled after a freeze has
// been seen to never answer a query on the group communication engine (S2 in
// doc/failover-audit/GroupReplication.md); a DemotePrimary that waited for it held the action
// lock forever, and every later RPC timed out.
func (tm *TabletManager) groupReplicationStatus(ctx context.Context) (*replicationdatapb.GroupReplicationStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, groupReplicationStatusTimeout)
	defer cancel()
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
	if bootstrap && isJoinWithoutGroup(status) {
		// A join that found no member to recover from. The caller verified that no member of the
		// shard's group is active, so there is nothing to join: stop it and bootstrap.
		log.Warn("MySQL is RECOVERING without an ONLINE member, stopping the join before the bootstrap", slog.String("group", status.GroupName))
		if status, err = tm.stopOngoingGroupStartLocked(ctx); err != nil {
			return nil, err
		}
	}
	if mysql.IsGroupMemberActive(status) {
		if bootstrap {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot bootstrap a group: MySQL is already %s in group %s", status.MemberState, status.GroupName)
		}
		if groupName := policy.GroupName(tablet.Keyspace, tablet.Shard); status.GroupName != groupName {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "MySQL is %s in group %s, but the group of shard %s/%s is %s", status.MemberState, status.GroupName, tablet.Keyspace, tablet.Shard, groupName)
		}
	} else if err := tm.joinGroupLocked(ctx, status, bootstrap); err != nil {
		if !isGroupReplicationCommandOngoing(err) {
			return nil, err
		}
		// A START GROUP_REPLICATION is still in progress, for example the join of an RPC whose
		// context expired: MySQL keeps running it, and refuses any change until it ends, which can
		// take minutes when no group exists. Such a join has been seen to end in a group of its
		// own. Stop it, then start again: a bootstrap, or a join that the caller decided on now.
		log.Warn("A START GROUP_REPLICATION is in progress, stopping it before starting again", slog.Bool("bootstrap", bootstrap), slog.Any("error", err))
		if status, err = tm.stopOngoingGroupStartLocked(ctx); err != nil {
			return nil, err
		}
		if err := tm.joinGroupLocked(ctx, status, bootstrap); err != nil {
			return nil, err
		}
	}
	status, err = tm.finishGroupJoinLocked(ctx)
	if err != nil {
		return nil, err
	}
	if bootstrap {
		// The caller records the new group's incarnation in the shard record right after the
		// bootstrap. Until then, the sync loop must not take the group for a foreign one.
		tm.groupReplicationPeers.noteBootstrap(policy.GroupIncarnation(status.GetViewId()))
		// The other voters join the new group with this member as their only donor, and MySQL
		// refuses their recovery connections, as the replication user, while offline_mode is ON.
		// The bootstrap has happened: a failure is left to the sync loop, which lifts offline_mode
		// on the primary of the shard's group too.
		if err := tm.liftOfflineMode(ctx, "MySQL bootstrapped the shard's group"); err != nil {
			log.Warn("Failed to clear offline_mode after bootstrapping the group", slog.Any("error", err))
		}
	}
	return status, nil
}

// liftOfflineMode clears offline_mode on the tablet's MySQL, if it is set.
//
// With group_replication_exit_state_action=OFFLINE_MODE, MySQL sets offline_mode, along with
// super_read_only, when a member leaves its group involuntarily: unreachable_majority_timeout, an
// expulsion, an applier or recovery error. MySQL then disconnects and refuses every connection of
// a user without CONNECTION_ADMIN or SUPER, which are vttablet's app, allprivs and filtered users,
// and the replication user; the tablet stops serving until the flag is cleared. This fences reads
// on a member that is out of its group, without its tablet having to act, but MySQL never clears
// the flag itself: not when the member rejoins, nor when it becomes the primary. Vitess clears it
// once the member is back in the shard's legitimate group, when it bootstraps the group, and when
// it makes MySQL the writable primary or an asynchronous replica of the shard primary.
func (tm *TabletManager) liftOfflineMode(ctx context.Context, reason string) error {
	on, err := tm.MysqlDaemon.IsOfflineMode(ctx)
	if err != nil {
		return vterrors.Wrapf(err, "failed to read offline_mode")
	}
	if !on {
		return nil
	}
	log.Info("Clearing offline_mode", slog.String("reason", reason))
	if err := tm.MysqlDaemon.SetOfflineMode(ctx, false); err != nil {
		return vterrors.Wrapf(err, "failed to clear offline_mode")
	}
	return nil
}

// mysqlErrGroupReplicationCommandOngoing is the MySQL error that refuses a change of the Group
// Replication configuration while START or STOP GROUP_REPLICATION is in progress.
const mysqlErrGroupReplicationCommandOngoing = 3724

// isGroupReplicationCommandOngoing returns whether MySQL refused a statement because START or STOP
// GROUP_REPLICATION is in progress.
func isGroupReplicationCommandOngoing(err error) bool {
	if sqlErr, ok := errors.AsType[*sqlerror.SQLError](err); ok && sqlErr.Number() == mysqlErrGroupReplicationCommandOngoing {
		return true
	}
	return err != nil && strings.Contains(err.Error(), "START or STOP GROUP_REPLICATION is ongoing")
}

// isJoinWithoutGroup returns whether MySQL is RECOVERING while it sees no ONLINE member: a join
// that has found no group to recover from.
func isJoinWithoutGroup(status *replicationdatapb.GroupReplicationStatus) bool {
	return status.GetPluginActive() && status.GetMemberState() == mysql.GroupMemberStateRecovering && mysql.OnlineGroupMembers(status) == 0
}

// stopOngoingGroupStartLocked stops Group Replication on a member whose join is in progress, and
// returns its status afterwards.
func (tm *TabletManager) stopOngoingGroupStartLocked(ctx context.Context) (*replicationdatapb.GroupReplicationStatus, error) {
	if err := tm.MysqlDaemon.StopGroupReplication(ctx); err != nil {
		return nil, vterrors.Wrapf(err, "failed to stop the group replication start in progress before the bootstrap")
	}
	return tm.groupReplicationStatus(ctx)
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

// waitUntilNotGroupPrimary waits until the tablet's MySQL is no longer the primary of its group,
// and returns its status then. It fails with FAILED_PRECONDITION when ctx ends first.
func (tm *TabletManager) waitUntilNotGroupPrimary(ctx context.Context) (*replicationdatapb.GroupReplicationStatus, error) {
	for {
		status, err := tm.groupReplicationStatus(ctx)
		if err != nil {
			return nil, err
		}
		if !mysql.IsGroupPrimary(status) {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "MySQL is still the primary of replication group %s: %v", status.GroupName, ctx.Err())
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
		if err := tm.liftOfflineMode(ctx, "the primary left its group and serves on its own"); err != nil {
			return nil, err
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
	if err := tm.liftOfflineMode(ctx, "MySQL is promoted to the shard primary"); err != nil {
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

// stopServingBeforeBootstrap makes a PRIMARY tablet stop serving before its MySQL bootstraps a new
// group, when the durability policy uses Group Replication and the shard record lists more than
// one voter. The new group has this member only, and MySQL makes it writable right away, so a
// transaction it committed would exist on a single voter until the others joined. The tablet keeps
// its type, and the sync loop makes it serve again once a majority of the voters is ONLINE in its
// view (enforceVoterMajority). In the S7d chaos scenario VTOrc bootstrapped the group on the old
// primary while its tablet was still PRIMARY, its sync loop being blocked on the unreachable
// topology, and the tablet acknowledged writes for 2.3s with a single voter in its group.
//
// During a migration the policy does not use Group Replication yet: the primary keeps serving,
// with semi-sync, while it bootstraps the group.
func (tm *TabletManager) stopServingBeforeBootstrap(ctx context.Context) error {
	if tm.Tablet().Type != topodatapb.TabletType_PRIMARY {
		return nil
	}
	durability, err := tm.keyspaceDurability(ctx)
	if err != nil {
		return err
	}
	if !policy.IsGroupReplication(durability) {
		return nil
	}
	voters, err := tm.groupReplicationVoters(ctx)
	if err != nil {
		return err
	}
	if len(voters) < 2 {
		return nil
	}
	log.Warn("Bootstrapping a group on a PRIMARY tablet: it stops serving until a majority of the voters is ONLINE in the group", slog.Int("voters", len(voters)))
	return tm.tmState.SetGroupReplicationNotServing(ctx, groupReplicationVoterMajorityLost)
}

// checkLegitimatePrimaryToServe returns an error unless the tablet may serve as the primary as far
// as its shard's replication group is concerned: under a group replication policy that lists
// voters, MySQL must be the primary of the shard's legitimate group (see policy.LegitimateGroup).
// VTOrc's PrimaryIsReadOnly recovery undid the demotion of a PRIMARY tablet whose MySQL was in the
// ERROR state after its group lost its majority, which cleared super_read_only on a MySQL outside
// of any group (S7d chaos scenario); a group primary without a majority of the voters would commit
// transactions that exist on too few voters.
func (tm *TabletManager) checkLegitimatePrimaryToServe(ctx context.Context, status *replicationdatapb.GroupReplicationStatus) error {
	if !groupReplicationEnabled() {
		return nil
	}
	durability, err := tm.keyspaceDurability(ctx)
	if err != nil {
		return err
	}
	if !policy.IsGroupReplication(durability) {
		return nil
	}
	rec, err := tm.readShardGroupRecord(ctx)
	if err != nil {
		return err
	}
	if len(rec.voters) == 0 {
		return nil
	}
	if !tm.legitimateGroup(ctx, rec, status, true).IsLegitimatePrimary(status) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "MySQL is not the primary of the shard's replication group with a majority of its voters (member %s %s, view %s)",
			status.GetMemberState(), status.GetMemberRole(), status.GetViewId())
	}
	return nil
}

// groupReplicationVoters returns the voting members of the tablet's shard's group, as recorded
// in the shard record.
func (tm *TabletManager) groupReplicationVoters(ctx context.Context) ([]*topodatapb.TabletAlias, error) {
	tablet := tm.Tablet()
	si, err := tm.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot read shard %v/%v", tablet.Keyspace, tablet.Shard)
	}
	return si.GetGroupReplicationVoters(), nil
}

// isGroupReplicationVoter returns whether the tablet should be a voting member of its shard's
// group: the tablet has a group replication port, the keyspace durability policy uses Group
// Replication, and the shard record lists the tablet among the group's voters. The voters are
// selected by MigrateReplicationMode, PlannedReparentShard and VTOrc; an empty list means that
// they have not been selected yet, and makes no tablet a voter.
//
// This decides the automatic membership of the tablet: joining at startup, rejoining in the
// sync loop, and StartReplication. The explicit StartGroupReplication and StopGroupReplication
// RPCs do not depend on it.
func (tm *TabletManager) isGroupReplicationVoter(ctx context.Context) (bool, error) {
	if !groupReplicationEnabled() {
		return false, nil
	}
	durability, err := tm.keyspaceDurability(ctx)
	if err != nil {
		return false, err
	}
	if !policy.IsGroupReplication(durability) {
		return false, nil
	}
	voters, err := tm.groupReplicationVoters(ctx)
	if err != nil {
		return false, err
	}
	return policy.IsVoter(voters, tm.tabletAlias), nil
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
