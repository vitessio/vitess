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

// defaultGroupMemberWeight is MySQL's default group_replication_member_weight. It is used when
// the shard's durability policy does not use Group Replication, for example while a shard is
// migrated to it.
const defaultGroupMemberWeight = 50

var (
	enableGroupReplication          bool
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
	// groupReplicationDemotionPublishTimeout bounds how long a tablet that demotes itself, because
	// its MySQL lost the primary role in its group, waits under the action lock for the topology
	// server to store its tablet record. The record is published in the background afterwards.
	groupReplicationDemotionPublishTimeout = 1 * time.Second
	// groupReplicationTopoReadTimeout bounds the topology reads with which a tablet prepares a
	// join or a bootstrap (StartGroupReplication), when it has read the same data before: it then
	// uses what it read last. These RPCs recover the shard's group, and VTOrc may reach a tablet
	// whose own topology server does not answer, for example one whose cell's topology server is
	// down or cut off.
	groupReplicationTopoReadTimeout = 1 * time.Second
	// groupReplicationElectionWaitTimeout bounds how long a decision that the tablet may serve as
	// PRIMARY waits, under the action lock, for the primary election that made its MySQL the group's
	// primary to end, before it decides: Group Replication sets super_read_only when the election ends,
	// which would undo the decision's making MySQL writable. Under BEFORE_ON_PRIMARY_FAILOVER the
	// election lasts as long as the new primary takes to apply its backlog. A decision whose wait ends
	// first does not serve, and is taken again later.
	groupReplicationElectionWaitTimeout = 10 * time.Second
	// groupReplicationMemberActionsCheckInterval is how often the serving primary of a group checks
	// that the group's configuration has the member action mysql_disable_super_read_only_if_primary
	// disabled, besides once per group incarnation.
	groupReplicationMemberActionsCheckInterval = 1 * time.Minute
)

func registerGroupReplicationFlags(fs *pflag.FlagSet) {
	utils.SetFlagBoolVar(fs, &enableGroupReplication, "enable-group-replication", enableGroupReplication,
		"Enable MySQL Group Replication support on this tablet: it can make its MySQL a member of its shard's group, whose members connect to each other through the MySQL port in their tablet records.")
	utils.SetFlagDurationVar(fs, &groupReplicationSyncInterval, "group-replication-sync-interval", groupReplicationSyncInterval,
		"How often a tablet with --enable-group-replication makes its tablet type follow its MySQL's role in its replication group, and rejoins the group if needed.")
	utils.SetFlagStringVar(fs, &groupReplicationConsistency, "group-replication-consistency", groupReplicationConsistency,
		"group_replication_consistency that the tablet applies before its MySQL starts Group Replication. Empty keeps the server's setting.")
	utils.SetFlagStringVar(fs, &groupReplicationExitStateAction, "group-replication-exit-state-action", groupReplicationExitStateAction,
		"group_replication_exit_state_action that the tablet applies before its MySQL starts Group Replication. READ_ONLY keeps a member that left its group readable; OFFLINE_MODE also refuses the tablet's app connections, which fences reads on it, and the tablet clears offline_mode once the member is back in the shard's group. Empty keeps the server's setting.")
	utils.SetFlagDurationVar(fs, &groupReplicationPauseNotice, "group-replication-pause-notice", groupReplicationPauseNotice,
		"How long a serving PRIMARY tablet reports that it does not serve, while it still serves, before it stops serving for a change of its MySQL's replication that makes MySQL refuse commits for a moment (a migration's bootstrap of the group, the primary leaving its group). vtgate buffers the writes that it receives meanwhile, instead of sending them to the tablet, which would then refuse them.")
	utils.SetFlagIntVar(fs, &groupReplicationAutorejoinTries, "group-replication-autorejoin-tries", groupReplicationAutorejoinTries,
		"group_replication_autorejoin_tries that the tablet applies before its MySQL starts Group Replication. The default 0 leaves rejoins to the tablet, which only rejoins while the shard's group is active on another tablet. A negative value keeps the server's setting.")
}

func init() {
	servenv.OnParseFor("vttablet", registerGroupReplicationFlags)
}

// groupReplicationEnabled returns whether the tablet supports MySQL Group Replication.
func groupReplicationEnabled() bool {
	return enableGroupReplication
}

// validateGroupReplicationFlags returns an error when a group replication flag has an invalid value.
func validateGroupReplicationFlags() error {
	if groupReplicationEnabled() && groupReplicationSyncInterval <= 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "--group-replication-sync-interval must be positive, got %v", groupReplicationSyncInterval)
	}
	if groupReplicationEnabled() && groupReplicationPauseNotice < 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "--group-replication-pause-notice must not be negative, got %v", groupReplicationPauseNotice)
	}
	return nil
}

// checkGroupReplicationEnabled returns a FAILED_PRECONDITION error if the tablet does not
// support Group Replication.
func checkGroupReplicationEnabled() error {
	if !groupReplicationEnabled() {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "group replication is not enabled on this tablet: --enable-group-replication is not set")
	}
	return nil
}

// groupReplicationAddress returns the address through which the other members of the shard's
// group reach the tablet's MySQL, or "" if the tablet record does not have it yet. The group uses
// the MySQL communication stack, so it is the address other tablets replicate from.
func groupReplicationAddress(tablet *topodatapb.Tablet) string {
	if tablet.MysqlPort <= 0 {
		return ""
	}
	host := tablet.MysqlHostname
	if host == "" {
		host = tablet.Hostname
	}
	if host == "" {
		return ""
	}
	return netutil.JoinHostPort(host, tablet.MysqlPort)
}

// groupReplicationSeeds returns the MySQL addresses of the tablets of the shard other than self,
// sorted so that every tablet computes the same list.
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

// shardDurability returns the durability policy that applies to the tablet's shard: the shard's own
// policy if its shard record sets one, else its keyspace's (topo.ShardDurabilityPolicy). Both
// records are read together, so the policy is never resolved from a shard record and a keyspace
// record of different moments of a migration.
func (tm *TabletManager) shardDurability(ctx context.Context) (policy.Durabler, error) {
	durability, _, err := tm.resolveShardDurability(ctx)
	return durability, err
}

// resolveShardDurability is shardDurability, and also returns the shard's own policy as its shard
// record set it ("" if it does not), so that a caller that caches the policy can tell when a newer
// shard record sets another one.
func (tm *TabletManager) resolveShardDurability(ctx context.Context) (policy.Durabler, string, error) {
	tablet := tm.Tablet()
	si, err := tm.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return nil, "", vterrors.Wrapf(err, "cannot read the durability policy of shard %v/%v", tablet.Keyspace, tablet.Shard)
	}
	durabilityName, err := tm.TopoServer.GetShardInfoDurability(ctx, si)
	if err != nil {
		return nil, "", vterrors.Wrapf(err, "cannot read the durability policy of shard %v/%v", tablet.Keyspace, tablet.Shard)
	}
	durability, err := policy.GetDurabilityPolicy(durabilityName)
	if err != nil {
		return nil, "", vterrors.Wrapf(err, "cannot get durability policy %v", durabilityName)
	}
	tm.groupReplicationTopo.setDurability(durabilityName)
	tm.noteShardGroupFields(si.Shard)
	return durability, si.GetDurabilityPolicy(), nil
}

// groupReplicationConfig derives the Group Replication configuration of the tablet's MySQL from
// the topology: the group name from the keyspace and shard, the seeds from the other tablets of
// the shard, and the member weight from the durability policy.
func (tm *TabletManager) groupReplicationConfig(ctx context.Context, durability policy.Durabler) (mysql.GroupReplicationConfig, error) {
	return tm.groupReplicationConfigUntil(ctx, durability, time.Time{})
}

// groupReplicationConfigUntil is groupReplicationConfig, but if the tablet read the shard's tablet
// records before, it waits for the topology until deadline only and otherwise derives the seeds
// from the records it read last (tabletsForGroupChange). A zero deadline does not bound the read.
func (tm *TabletManager) groupReplicationConfigUntil(ctx context.Context, durability policy.Durabler, deadline time.Time) (mysql.GroupReplicationConfig, error) {
	tablet := tm.Tablet()
	if tablet.MysqlPort <= 0 {
		// The tablet record gets the port once MySQL answered; the join cannot wait for that.
		port, err := tm.MysqlDaemon.GetMysqlPort(ctx)
		if err != nil {
			return mysql.GroupReplicationConfig{}, vterrors.Wrapf(err, "cannot derive the group replication address: failed to read the MySQL port")
		}
		tablet.MysqlPort = port
	}
	localAddress := groupReplicationAddress(tablet)
	if localAddress == "" {
		return mysql.GroupReplicationConfig{}, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot derive the group replication address: the tablet has no hostname")
	}

	// The seeds of the cells that answer in time are enough to join.
	tablets, err := tm.tabletsForGroupChange(ctx, tablet.Keyspace, tablet.Shard, deadline)
	if err != nil {
		return mysql.GroupReplicationConfig{}, err
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
		LocalAddress:    localAddress,
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

// checkGroupAllowsPrimary returns a FAILED_PRECONDITION error if the tablet's MySQL is an active
// member of a group but not its primary: only the group decides which of its members is the primary,
// so such a tablet cannot become PRIMARY.
func (tm *TabletManager) checkGroupAllowsPrimary(ctx context.Context) error {
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		return err
	}
	if mysql.IsGroupMemberActive(status) && !mysql.IsGroupPrimary(status) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"MySQL is a %s %s member of group %s but not the primary of a group with quorum",
			status.MemberState, status.MemberRole, status.GroupName)
	}
	return nil
}

// mysqlReadOnly returns whether MySQL has super_read_only or read_only set.
func (tm *TabletManager) mysqlReadOnly(ctx context.Context) (bool, error) {
	superReadOnly, err := tm.MysqlDaemon.IsSuperReadOnly(ctx)
	if err != nil {
		return false, err
	}
	if superReadOnly {
		return true, nil
	}
	return tm.MysqlDaemon.IsReadOnly(ctx)
}

// checkGroupAllowsReadWrite returns a FAILED_PRECONDITION error if the tablet's MySQL is an
// active member of a group but not its primary. Such a member must never be made writable: a
// Group Replication secondary without super_read_only accepts writes and replicates them to the
// whole group. It returns UNAVAILABLE while the primary election that made MySQL the primary still
// runs: Group Replication sets super_read_only when it ends, which would undo the change.
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
	if groupElectionInProgress(status) {
		return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE,
			"refusing to make MySQL writable: group %s is still electing it its primary, and Group Replication sets super_read_only when the election ends",
			status.GroupName)
	}
	return nil
}

// groupElectionInProgress returns whether MySQL is the primary of its group, and the primary
// election that made it the primary still runs.
func groupElectionInProgress(status *replicationdatapb.GroupReplicationStatus) bool {
	return mysql.IsGroupPrimary(status) && status.GetPrimaryElectionInProgress()
}

// twoPCDurable returns whether a primary with this group replication state and primary-side
// semi-sync setting makes prepared transactions durable enough for atomic transactions.
func twoPCDurable(status *replicationdatapb.GroupReplicationStatus, semiSync bool) bool {
	return semiSync || mysql.GroupSupersedesSemiSync(status)
}

// startGroupReplicationLocked makes the tablet's MySQL join its shard's group, or bootstrap a
// new group, unless it is already an active member. It then finishes the transition to Group
// Replication, which is idempotent. It does not wait for the member to become ONLINE. A bootstrap
// first passes the caller's checks, if any (see groupBootstrapChecks), right before MySQL's START.
//
// The caller must hold the action lock, or run during startup.
//
// While it runs, and for a while after it ended, the fence check watches MySQL's view (see
// groupReplicationFence.armed): a join that does not find its group can end in a group of its own,
// of which MySQL is the writable primary.
func (tm *TabletManager) startGroupReplicationLocked(ctx context.Context, bootstrap bool, checks *groupBootstrapChecks) (_ *replicationdatapb.GroupReplicationStatus, err error) {
	end := tm.groupReplicationFence.beginStart(bootstrap)
	defer func() { end(err != nil) }()
	tablet := tm.Tablet()
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		return nil, err
	}
	if bootstrap && isJoinWithoutGroup(status) {
		// A join that found no member to recover from. The caller verified that no member of the
		// shard's group is active, so there is nothing to join: stop it and bootstrap.
		log.Warn("MySQL is RECOVERING without an ONLINE member, stopping the join before the bootstrap", slog.String("group", status.GroupName))
		if status, err = tm.stopOngoingGroupStartLocked(ctx, checks); err != nil {
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
	} else if err := tm.joinGroupLocked(ctx, status, bootstrap, checks); err != nil {
		if !isGroupReplicationCommandOngoing(err) {
			return nil, err
		}
		// A START GROUP_REPLICATION is still in progress, for example the join of an RPC whose
		// context expired: MySQL keeps running it, and refuses any change until it ends, which can
		// take minutes when no group exists. Such a join has been seen to end in a group of its
		// own. Stop it, then start again: a bootstrap, or a join that the caller decided on now.
		log.Warn("A START GROUP_REPLICATION is in progress, stopping it before starting again", slog.Bool("bootstrap", bootstrap), slog.Any("error", err))
		if status, err = tm.stopOngoingGroupStartLocked(ctx, checks); err != nil {
			return nil, err
		}
		if err := tm.joinGroupLocked(ctx, status, bootstrap, checks); err != nil {
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
		// The new group's primary must serve. The bootstrap has happened: a failure is left to the
		// sync loop, which lifts offline_mode on the primary of the shard's group too.
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
// a user without CONNECTION_ADMIN or SUPER, which are vttablet's app, allprivs and filtered users;
// the tablet stops serving until the flag is cleared. The replication user has CONNECTION_ADMIN,
// which the MySQL communication stack requires, so the group's connections are kept. This fences reads
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

// The MySQL errors that refuse a statement while START or STOP GROUP_REPLICATION is in progress:
//   - mysqlErrGroupReplicationCommandOngoing refuses a change of the Group Replication configuration
//     (SET GLOBAL group_replication_*).
//   - mysqlErrGroupReplicationCommandFailure is MySQL's generic failure of a START or STOP
//     GROUP_REPLICATION (ER_GROUP_REPLICATION_COMMAND_FAILURE); it refuses a STOP, and a second START,
//     while a START runs with mysql.GroupReplicationCommandRunningMessage. A START that finds no group
//     runs for about a minute, and a STOP fails that way for as long (verified on MySQL 8.4.11).
const (
	mysqlErrGroupReplicationCommandOngoing = 3724
	mysqlErrGroupReplicationCommandFailure = 3663
)

var (
	// groupReplicationStopOngoingStartTimeout bounds how long the tablet waits, under the action lock,
	// for MySQL to accept the STOP GROUP_REPLICATION of a START in progress. MySQL refuses it until the
	// START ends, which takes up to about a minute when the START finds no group: a caller that cannot
	// wait that long gets UNAVAILABLE, and tries again later.
	groupReplicationStopOngoingStartTimeout = 10 * time.Second
	// groupReplicationStopOngoingStartRetry is how often the tablet tries that STOP again.
	groupReplicationStopOngoingStartRetry = 1 * time.Second
)

// isGroupReplicationCommandOngoing returns whether MySQL refused a statement because START or STOP
// GROUP_REPLICATION is in progress: errno 3724, or errno 3663 with
// mysql.GroupReplicationCommandRunningMessage (3663 also reports other failures of the command).
// vterrors does not unwrap, so the error's text counts too.
func isGroupReplicationCommandOngoing(err error) bool {
	if err == nil {
		return false
	}
	if sqlErr, ok := errors.AsType[*sqlerror.SQLError](err); ok {
		switch sqlErr.Number() {
		case mysqlErrGroupReplicationCommandOngoing:
			return true
		case mysqlErrGroupReplicationCommandFailure:
			if strings.Contains(sqlErr.Message, mysql.GroupReplicationCommandRunningMessage) {
				return true
			}
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "START or STOP GROUP_REPLICATION is ongoing") ||
		strings.Contains(msg, mysql.GroupReplicationCommandRunningMessage)
}

// isJoinWithoutGroup returns whether MySQL is RECOVERING while it sees no ONLINE member: a join
// that has found no group to recover from.
func isJoinWithoutGroup(status *replicationdatapb.GroupReplicationStatus) bool {
	return status.GetPluginActive() && status.GetMemberState() == mysql.GroupMemberStateRecovering && mysql.OnlineGroupMembers(status) == 0
}

// stopOngoingGroupStartLocked stops Group Replication on a member whose START GROUP_REPLICATION is
// in progress, and returns its status afterwards.
//
// MySQL refuses the STOP (errno 3663) until the START ends, for up to about a minute when the START
// finds no group, and accepts it right after. The tablet tries again every
// groupReplicationStopOngoingStartRetry, for at most groupReplicationStopOngoingStartTimeout or until
// ctx ends, and then fails with UNAVAILABLE: the caller holds the action lock, which the tablet's
// other RPCs and its sync loop need meanwhile.
//
// Before each attempt, a bootstrap checks that its intent still holds (see
// checkGroupBootstrapIntentLocked): the START it stops may be the bootstrap of a newer intent, whose
// group the STOP would make MySQL leave once it formed.
func (tm *TabletManager) stopOngoingGroupStartLocked(ctx context.Context, checks *groupBootstrapChecks) (*replicationdatapb.GroupReplicationStatus, error) {
	deadline := time.Now().Add(groupReplicationStopOngoingStartTimeout)
	for {
		if err := tm.checkGroupBootstrapIntentLocked(ctx, checks); err != nil {
			return nil, err
		}
		err := tm.MysqlDaemon.StopGroupReplication(ctx)
		if err == nil {
			return tm.groupReplicationStatus(ctx)
		}
		if !isGroupReplicationCommandOngoing(err) {
			return nil, vterrors.Wrapf(err, "failed to stop the group replication start in progress")
		}
		wait := groupReplicationStopOngoingStartRetry
		if remaining := time.Until(deadline); remaining < wait {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE,
				"a START GROUP_REPLICATION is still in progress on MySQL, which refuses to stop it until it ends (up to about a minute when it finds no group); try again later: %v", err)
		}
		log.Info("MySQL refuses to stop the START GROUP_REPLICATION in progress until it ends, trying again", slog.Duration("retry", wait), slog.Any("error", err))
		select {
		case <-ctx.Done():
			return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE,
				"a START GROUP_REPLICATION is still in progress on MySQL, which refuses to stop it until it ends: %v (last error: %v)", ctx.Err(), err)
		case <-time.After(wait):
		}
	}
}

// joinGroupLocked configures Group Replication and starts it on a MySQL that is not an active
// member. A bootstrap passes checks first, right before MySQL's START (see groupBootstrapChecks).
//
// The durability policy and the tablet records only give the member weight and the seeds. A tablet
// that read them before waits at most groupReplicationTopoReadTimeout for the topology, and
// otherwise uses what it read last: a bootstrap or a join that VTOrc asks for to recover the
// shard's group must not wait for a topology server that does not answer the tablet.
func (tm *TabletManager) joinGroupLocked(ctx context.Context, status *replicationdatapb.GroupReplicationStatus, bootstrap bool, checks *groupBootstrapChecks) error {
	deadline := time.Now().Add(groupReplicationTopoReadTimeout)
	durability, err := tm.durabilityForGroupChange(ctx, deadline)
	if err != nil {
		return err
	}
	cfg, err := tm.groupReplicationConfigUntil(ctx, durability, deadline)
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
	if bootstrap {
		if err := tm.checkGroupBootstrapLocked(ctx, checks); err != nil {
			restartReplication()
			return err
		}
	}
	log.Info("Starting group replication",
		slog.String("group", cfg.GroupName),
		slog.String("local_address", cfg.LocalAddress),
		slog.Any("seeds", cfg.Seeds),
		slog.Bool("bootstrap", bootstrap))
	// MySQL is not in a group: Group Replication decides from here whether it is writable, and the
	// fence check fences a group it must not take writes in again.
	tm.groupReplicationFence.reset()
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
//   - On the primary of a PRIMARY tablet, it makes MySQL writable if the tablet may serve (Group
//     Replication leaves the primary it elects super_read_only), and applies the rule that a group
//     with two ONLINE members supersedes semi-sync. Semi-sync otherwise stays as it is: a
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
			// Group Replication leaves the primary it elected super_read_only (the member action
			// mysql_disable_super_read_only_if_primary is disabled): the tablet makes MySQL writable
			// if it may serve, as the primary of a migration's bootstrap, and leaves it read-only
			// otherwise, as the primary of a group that VTOrc bootstrapped and whose incarnation is
			// not recorded yet. The primary kept serving while it bootstrapped the group, and MySQL
			// did not restart, so prepared transactions are intact: redoing them would restart the
			// transaction engine and fail in-flight queries.
			if err := tm.makeGroupPrimaryWritableLocked(ctx); err != nil {
				return nil, vterrors.Wrapf(err, "failed to make the group primary writable")
			}
			if status, err = tm.groupReplicationStatus(ctx); err != nil {
				return nil, err
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

// waitForGroupPrimaryElected waits until the tablet's MySQL is the primary of its group and the
// primary election that made it the primary has ended. Group Replication does not make it writable
// (the member action mysql_disable_super_read_only_if_primary is disabled), and sets super_read_only
// when the election ends: the caller decides afterwards whether MySQL takes writes.
func (tm *TabletManager) waitForGroupPrimaryElected(ctx context.Context) (*replicationdatapb.GroupReplicationStatus, error) {
	for {
		status, err := tm.groupReplicationStatus(ctx)
		if err != nil {
			return nil, err
		}
		if mysql.IsGroupPrimary(status) && !status.GetPrimaryElectionInProgress() {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return nil, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED,
				"timed out waiting for MySQL to become the primary of its group, with its election ended (member %s %s, election in progress: %v): %v",
				status.MemberState, status.MemberRole, status.GetPrimaryElectionInProgress(), ctx.Err())
		case <-time.After(groupReplicationPollInterval):
		}
	}
}

// waitForGroupElectionEnd waits, for at most groupReplicationElectionWaitTimeout, while the tablet's
// MySQL is the primary of its group and the primary election that made it the primary still runs. A
// decision whether the tablet may serve as PRIMARY waits for it before it takes its fence snapshot and
// reads MySQL's status: the end of the election makes MySQL super_read_only, which would undo the
// decision. A decision that still finds the election running does not serve
// (groupReplicationElectionInProgress).
func (tm *TabletManager) waitForGroupElectionEnd(ctx context.Context) {
	waitCtx, cancel := context.WithTimeout(ctx, groupReplicationElectionWaitTimeout)
	defer cancel()
	for {
		status, err := tm.groupReplicationStatus(waitCtx)
		if err != nil || !groupElectionInProgress(status) {
			return
		}
		select {
		case <-waitCtx.Done():
			log.Warn("Group replication: the primary election of MySQL still runs, deciding whether the tablet serves without waiting longer",
				slog.String("group", status.GetGroupName()), slog.String("view_id", status.GetViewId()))
			return
		case <-time.After(groupReplicationPollInterval):
		}
	}
}

// makeGroupPrimaryWritableLocked makes the tablet's MySQL, the primary of its group, writable if the
// tablet may serve as PRIMARY: a decision under the action lock, on MySQL's status read under it
// after the primary election ended (groupReplicationServingDecision, which also records a reason not
// to serve). Group Replication leaves the primary it elects super_read_only, since the member action
// mysql_disable_super_read_only_if_primary is disabled; this is how the primary of a migration's
// bootstrap, or of a group that the tablet serves again after a planned pause, becomes writable. A
// fence decided since the decision started stands (see groupReplicationFence). MySQL is left as it is
// if it is not the primary of a group, or if the tablet must not serve.
func (tm *TabletManager) makeGroupPrimaryWritableLocked(ctx context.Context) error {
	tm.waitForGroupElectionEnd(ctx)
	fences := tm.groupReplicationFence.snapshot()
	reason, status, err := tm.applyGroupReplicationServingDecisionLocked(ctx, nil)
	if err != nil {
		return err
	}
	if !mysql.IsGroupPrimary(status) || reason != "" {
		return nil
	}
	if tm.groupReplicationFence.decidedSince(fences) {
		return nil
	}
	if err := tm.setGroupPrimaryWritable(ctx); err != nil {
		return err
	}
	tm.settleGroupReplicationFenceLocked(ctx, fences)
	return nil
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
		// fails) and then turns read-only. A serving primary pauses first, so that vtgate buffers
		// writes instead of failing them, and serves again once MySQL is writable (see
		// pauseServingLocked).
		log.Info("Group primary is leaving its group")
		pause, err := tm.pauseServingLocked(ctx, groupReplicationLeavePause)
		if err != nil {
			return nil, err
		}
		defer func() {
			if pause != nil {
				pause.resume(ctx)
				return
			}
			// A primary that did not serve before serves now, unless it must not serve for its
			// replication group.
			termStart := protoutil.TimeFromProto(tablet.PrimaryTermStartTime).UTC()
			if err := tm.tmState.SetServingUnlessGroupReplicationNotServing(tablet.Type, termStart); err != nil {
				log.Warn(fmt.Sprintf("SetServingType(serving=true) failed after leaving the replication group: %v", err))
			}
		}()
	}
	log.Info("Stopping group replication", slog.String("group", status.GroupName), slog.String("state", status.MemberState), slog.String("role", status.MemberRole))
	if err := tm.MysqlDaemon.StopGroupReplication(ctx); err != nil {
		return nil, vterrors.Wrapf(err, "failed to stop group replication")
	}
	// MySQL left its group, super_read_only: from here on, whether it takes writes is decided below.
	tm.groupReplicationFence.reset()

	if leavingPrimary {
		// The last step of a migration back to asynchronous replication runs under the
		// asynchronous policy. Under a group replication policy that lists voters, a primary
		// outside of any group must not serve: it stays read-only, and the sync loop demotes it.
		reason, _, err := tm.applyGroupReplicationServingDecisionLocked(ctx, nil)
		if err != nil {
			return nil, err
		}
		if reason != "" {
			return tm.groupReplicationStatus(ctx)
		}
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
	durability, err := tm.shardDurability(ctx)
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
	// Group Replication leaves the new primary super_read_only, and sets it again when its election
	// ends: the promotion below makes MySQL writable once the election ended, if the tablet may serve.
	if _, err := tm.waitForGroupPrimaryElected(ctx); err != nil {
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
		if _, err := tm.startGroupReplicationLocked(ctx, false, nil); err != nil {
			return err
		}
	} else if _, err := tm.startGroupReplicationLocked(ctx, true, nil); err != nil {
		return err
	}
	// Group Replication leaves the bootstrapped primary super_read_only: InitPrimary makes MySQL
	// writable once the election ended.
	_, err = tm.waitForGroupPrimaryElected(ctx)
	return err
}

// stopServingBeforeBootstrap makes a PRIMARY tablet stop serving before its MySQL bootstraps a new
// group, when the durability policy uses Group Replication and the shard record lists more than
// one voter. The new group has this member only, and MySQL makes it writable right away, so a
// transaction it committed would exist on a single voter until the others joined. The tablet keeps
// its type, and the sync loop makes it serve again once the new incarnation is recorded and a
// majority of the voters is ONLINE in its view, as MySQL reports it under the action lock, which
// this RPC holds until the bootstrap is over (enforceVoterMajority, serveAgain). Setting the reason
// also counts as a new not-serving decision: a run of the sync loop that read MySQL's status
// before cannot undo it (ClearGroupReplicationNotServing). In the S7d chaos scenario VTOrc bootstrapped the group on the old
// primary while its tablet was still PRIMARY, its sync loop being blocked on the unreachable
// topology, and the tablet acknowledged writes for 2.3s with a single voter in its group.
//
// During a migration the policy does not use Group Replication yet: the primary keeps serving,
// with semi-sync, while it bootstraps the group.
func (tm *TabletManager) stopServingBeforeBootstrap(ctx context.Context) error {
	if tm.Tablet().Type != topodatapb.TabletType_PRIMARY {
		return nil
	}
	// Like the reads of a join, these wait at most groupReplicationTopoReadTimeout for the topology
	// when the tablet read the same data before.
	deadline := time.Now().Add(groupReplicationTopoReadTimeout)
	durability, err := tm.durabilityForGroupChange(ctx, deadline)
	if err != nil {
		return err
	}
	if !policy.IsGroupReplication(durability) {
		return nil
	}
	voters, err := tm.votersForGroupChange(ctx, deadline)
	if err != nil {
		return err
	}
	if len(voters) < 2 {
		return nil
	}
	log.Warn("Bootstrapping a group on a PRIMARY tablet: it stops serving until a majority of the voters is ONLINE in the group", slog.Int("voters", len(voters)))
	return tm.tmState.SetGroupReplicationNotServing(ctx, groupReplicationVoterMajorityLost)
}

// The reasons for which a PRIMARY tablet does not serve for its replication group, besides
// groupReplicationVoterMajorityLost.
const (
	// groupReplicationNotGroupPrimary: MySQL is not the ONLINE primary of a group with quorum.
	groupReplicationNotGroupPrimary = "MySQL is not the primary of the shard's replication group"
	// groupReplicationUnrecordedIncarnation: MySQL is the primary of a group of another incarnation
	// than the one the shard record lists, for example one it bootstrapped whose incarnation was
	// not recorded yet.
	groupReplicationUnrecordedIncarnation = "MySQL's replication group is not the incarnation recorded in the shard record"
	// groupReplicationRecordUnknown: the shard record could not be read to decide.
	groupReplicationRecordUnknown = "cannot read the shard record to check the replication group"
	// groupReplicationElectionInProgress: MySQL is the group's primary, but the primary election that
	// made it the primary still runs. Group Replication sets super_read_only when it ends.
	groupReplicationElectionInProgress = "MySQL's replication group is still electing it its primary"
	// groupReplicationNotVoter: MySQL is the group's primary, but the tablet is not a listed voter.
	groupReplicationNotVoter = "MySQL is the primary of its replication group, but its tablet is not a voter of the shard's group"
	// groupReplicationDemotionRevertUndecided: DemotePrimary failed, and its revert could not decide
	// whether the tablet may serve (revertDemotionWithGroupDecisionLocked).
	groupReplicationDemotionRevertUndecided = "the revert of a failed demotion could not decide whether the tablet serves"
	// groupReplicationDemotionRevertFailed: DemotePrimary failed, and its revert could not make MySQL
	// writable again.
	groupReplicationDemotionRevertFailed = "the revert of a failed demotion could not make MySQL writable"
)

// groupReplicationServingReason returns why a PRIMARY tablet must not serve as the primary of its
// shard, as far as its replication group is concerned, or "" if it may. Under a group replication
// policy that lists voters, a tablet serves as PRIMARY only while MySQL is the ONLINE primary, with
// quorum, of a view of the shard's recorded incarnation (when one is recorded) that holds a
// majority of the listed voters ONLINE. Unlike the promotion of the tablet record, it does not
// trust the incarnation of a group that the tablet bootstrapped itself before it is recorded: a
// bootstrap whose reply was lost leaves it unrecorded, and VTOrc then adopts it first (see
// doc/design-docs/GroupReplication.md, "Bootstrap intent").
//
// status must have been read after the decision it serves was protected against the RPCs that
// change MySQL's group: under the action lock, after the caller captured the not-serving generation
// (see ClearGroupReplicationNotServing). With fetchMissing, when the voters the tablet can identify
// do not make a majority of the view, it first asks the voters whose server_uuid it does not know
// for it (see legitimateGroup), each for at most groupReplicationPeerTimeout.
func (tm *TabletManager) groupReplicationServingReason(ctx context.Context, durability policy.Durabler, rec *shardGroupRecord, status *replicationdatapb.GroupReplicationStatus, fetchMissing bool) string {
	if !groupReplicationEnabled() || !policy.IsGroupReplication(durability) {
		return ""
	}
	if rec == nil {
		return groupReplicationRecordUnknown
	}
	if groupElectionInProgress(status) {
		// MySQL cannot take writes yet: Group Replication makes it super_read_only when the election
		// ends.
		return groupReplicationElectionInProgress
	}
	if len(rec.voters) == 0 {
		// The voters are not selected yet: MySQL's own view quorum applies, as it does everywhere
		// else (see policy.LegitimateGroup).
		return ""
	}
	if !mysql.IsGroupPrimary(status) {
		return groupReplicationNotGroupPrimary
	}
	// Only a voter serves: a member that is not one counts in the certification majority of its view,
	// which then need not hold a majority of the voters, and a bootstrap from the voters would lose
	// what it acknowledged. VTOrc gives the group primary a seat (policy.SelectVoters), and the
	// tablet serves then.
	if !policy.IsVoter(rec.voters, tm.tabletAlias) {
		return groupReplicationNotVoter
	}
	if rec.incarnation != "" && policy.GroupIncarnation(status.GetViewId()) != rec.incarnation {
		return groupReplicationUnrecordedIncarnation
	}
	hasMajority := func() bool { return tm.recordedLegitimateGroup(ctx, rec).HasVoterMajority(status) }
	if !hasMajority() && fetchMissing {
		if missing := tm.votersWithoutServerUUID(rec); len(missing) > 0 {
			tm.fetchPeerServerUUIDs(ctx, missing, hasMajority)
		}
	}
	if !hasMajority() {
		return groupReplicationVoterMajorityLost
	}
	return ""
}

// groupReplicationServingDecision reads what groupReplicationServingReason needs, with MySQL's
// status read last, and returns the reason. The caller holds the action lock. The durability policy
// and the shard record wait at most groupReplicationTopoReadTimeout for the topology when the
// tablet read them before; the policy then falls back to what it read last, but the shard record
// does not: the tablet then does not serve, and the sync loop decides again once the topology
// answers. rec, if set, is a shard record the caller read a moment ago; status, if set, is MySQL's
// status that the caller read under the action lock, after it captured the not-serving generation.
func (tm *TabletManager) groupReplicationServingDecision(ctx context.Context, rec *shardGroupRecord, status *replicationdatapb.GroupReplicationStatus) (string, error) {
	reason, _, err := tm.groupReplicationServingDecisionWithStatus(ctx, rec, status)
	return reason, err
}

// groupReplicationServingDecisionWithStatus is groupReplicationServingDecision, and also returns
// MySQL's status: the caller's, or the one it read last, also when the policy alone decided.
func (tm *TabletManager) groupReplicationServingDecisionWithStatus(ctx context.Context, rec *shardGroupRecord, status *replicationdatapb.GroupReplicationStatus) (string, *replicationdatapb.GroupReplicationStatus, error) {
	readStatus := func() (*replicationdatapb.GroupReplicationStatus, error) {
		if status != nil {
			return status, nil
		}
		return tm.groupReplicationStatus(ctx)
	}
	if !groupReplicationEnabled() {
		status, err := readStatus()
		return "", status, err
	}
	deadline := time.Now().Add(groupReplicationTopoReadTimeout)
	durability, err := tm.durabilityForGroupChange(ctx, deadline)
	if err != nil {
		return "", nil, err
	}
	if !policy.IsGroupReplication(durability) {
		status, err := readStatus()
		return "", status, err
	}
	if rec == nil {
		last := tm.groupReplicationTopo.lastRecord()
		readCtx, cancel := withTopoReadDeadline(ctx, deadline, last != nil)
		rec, err = tm.readShardGroupRecord(readCtx, last)
		cancel()
		if err != nil {
			log.Warn("Group replication: cannot read the shard record, the primary does not serve until it can", slog.Any("error", err))
			status, err := readStatus()
			return groupReplicationRecordUnknown, status, err
		}
	}
	if status, err = readStatus(); err != nil {
		return "", nil, err
	}
	return tm.groupReplicationServingReason(ctx, durability, rec, status, true), status, nil
}

// applyGroupReplicationServingDecisionLocked decides, under the action lock, whether the tablet may
// serve as PRIMARY for its replication group, and records the decision before the caller makes the
// tablet a serving PRIMARY: a reason makes it a PRIMARY that does not serve, right away if it is
// PRIMARY already, and no reason clears the one set before, unless another one was set since the
// decision started. A cleared reason only takes effect with the caller's next change of the
// tablet's state (ChangeTabletType, SetServingUnlessGroupReplicationNotServing), so that the
// tablet does not serve before MySQL is ready. rec, if set, is a shard record the caller read a
// moment ago. It returns the reason that stands.
//
// It also returns MySQL's status on which it decided.
func (tm *TabletManager) applyGroupReplicationServingDecisionLocked(ctx context.Context, rec *shardGroupRecord) (string, *replicationdatapb.GroupReplicationStatus, error) {
	_, gen := tm.tmState.GroupReplicationNotServingState()
	reason, status, err := tm.groupReplicationServingDecisionWithStatus(ctx, rec, nil)
	if err != nil {
		return "", nil, err
	}
	if reason != "" {
		log.Warn("Group replication: the tablet does not serve as the primary", slog.String("reason", reason))
		return reason, status, tm.tmState.SetGroupReplicationNotServing(ctx, reason)
	}
	if !tm.tmState.ClearGroupReplicationNotServingBeforeChange(gen) {
		// A reason was set since the decision started: it was decided on a newer state.
		reason, _ = tm.tmState.GroupReplicationNotServingState()
		return reason, status, nil
	}
	return "", status, nil
}

// groupReplicationVoters returns the voting members of the tablet's shard's group, as recorded
// in the shard record.
func (tm *TabletManager) groupReplicationVoters(ctx context.Context) ([]*topodatapb.TabletAlias, error) {
	tablet := tm.Tablet()
	si, err := tm.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot read shard %v/%v", tablet.Keyspace, tablet.Shard)
	}
	tm.groupReplicationTopo.setVoters(si.GetGroupReplicationVoters())
	tm.noteShardGroupFields(si.Shard)
	return si.GetGroupReplicationVoters(), nil
}

// isGroupReplicationVoter returns whether the tablet should be a voting member of its shard's
// group: the tablet has a group replication port, the shard's durability policy uses Group
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
	durability, err := tm.shardDurability(ctx)
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
