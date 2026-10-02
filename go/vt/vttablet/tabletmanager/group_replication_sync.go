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
	"log/slog"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// groupReplicationSync makes the tablet follow the state of its MySQL's replication group. The
// group elects its primary on its own, for example after the primary failed; the sync loop is
// how such a change reaches the topology within about one sync interval, without VTOrc:
//
//   - If MySQL is the legitimate primary of the shard's group and the tablet is not PRIMARY, it
//     promotes the tablet record. The new primary term makes the shard sync loop update the
//     shard record, and vtgate follow. The primary is legitimate when it is the ONLINE primary of
//     a group with quorum, in the group incarnation that the shard record lists, and a majority
//     of the shard's listed voters are ONLINE in its view (see policy.LegitimateGroup).
//   - If MySQL is an active member of a group of another incarnation than the one the shard
//     record lists, it makes MySQL leave that group: such a group formed without Vitess and does
//     not hold the shard's acknowledged transactions. MySQL then rejoins the legitimate group
//     like any other voter that is not in its group (see below).
//   - If the tablet is PRIMARY but MySQL is no longer the primary of a group with quorum, it
//     demotes the tablet record to REPLICA. MySQL is left as it is: the group already made it
//     read-only. Under a group replication policy, a PRIMARY tablet that is a listed voter and
//     whose MySQL is not in any group (OFFLINE, for example after mysqld restarted) is demoted
//     too, so that vtgate stops routing to it and the tablet rejoins the group like any voter.
//     This does not apply while the policy is not a group replication policy or no voter is
//     listed: during a migration, the primary serves outside of a group.
//   - If the durability policy uses Group Replication, the shard record lists the tablet as a
//     voter of the group, and MySQL is not in the group, it rejoins the group with exponential
//     backoff, but only while another tablet of the shard reports an active member of the shard's
//     legitimate group with quorum. It never bootstraps a group. It never makes a member leave its group either:
//     removing a member that is no longer a voter is VTOrc's decision.
//   - On a PRIMARY that is the group primary, it applies the effective semi-sync setting: a group
//     with at least two ONLINE members supersedes semi-sync.
//   - A PRIMARY whose group view holds fewer than a majority of the shard's listed voters, or that
//     is not in the recorded incarnation, stops serving, and serves again once both hold, as MySQL
//     reports it under the action lock (serveAgain). It keeps its type, so vtgate buffers.
//     MySQL still commits in such a group (MySQL's view quorum counts only the members that are
//     still in the view, after the others left it), but a transaction would then only exist on a
//     minority of the voters. This fails closed, like a semi-sync primary without an acker.
//   - On every run, it tells the query service whether MySQL is ONLINE in the shard's legitimate
//     group, with quorum and a majority of the voters in its view. The replication lag poller
//     (--enable-replication-reporter) reports a member that is not with the lag accumulated since
//     it last was, so that a member cut off from the shard's group leaves replica reads.
//
// Mutations take the action lock without waiting, so that the loop never queues behind a long
// running RPC (a backup, a restore, a reparent); it retries on its next run instead.
type groupReplicationSync struct {
	tm *TabletManager

	// durability is the shard's durability policy, read at durabilityRead from a shard record
	// that set the shard's own policy durabilityShardPolicy ("" for none).
	durability            policy.Durabler
	durabilityRead        time.Time
	durabilityShardPolicy string

	voters     []*topodatapb.TabletAlias
	votersRead time.Time

	// record is the shard record's view of the shard's legitimate group, read at recordRead.
	record     *shardGroupRecord
	recordRead time.Time
	// lastIllegitimateLog is when the loop last logged that it does not promote a group primary
	// that is not legitimate.
	lastIllegitimateLog time.Time
	// peersFetched is when the loop last asked the voters for their server_uuids.
	peersFetched time.Time

	lastState string
	lastRole  string

	rejoinBackoff time.Duration
	nextRejoin    time.Time

	// twoPCAllowed is the last value the loop passed to SetTwoPCAllowed, if any.
	twoPCAllowed *bool
	// lastVerdict is the last value the loop passed to publishVerdict, if any.
	lastVerdict *bool

	// loopCtx is the context of the loop, which ends when the loop stops.
	loopCtx context.Context
	// uuidsWarmed is when the loop last asked the voters whose server_uuid the tablet does not
	// know for it in the background.
	uuidsWarmed time.Time
}

func newGroupReplicationSync(tm *TabletManager) *groupReplicationSync {
	return &groupReplicationSync{tm: tm}
}

// startGroupReplicationSync starts the group replication sync loop if the tablet supports
// Group Replication.
func (tm *TabletManager) startGroupReplicationSync() {
	if !groupReplicationEnabled() {
		return
	}
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	if tm._groupReplicationSyncCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	tm._groupReplicationSyncCancel = cancel
	tm._groupReplicationSyncDone = done

	s := newGroupReplicationSync(tm)
	go func() {
		defer close(done)
		s.run(ctx, groupReplicationSyncInterval)
	}()
}

// stopGroupReplicationSync stops the group replication sync loop and waits for it to exit.
func (tm *TabletManager) stopGroupReplicationSync() {
	tm.mutex.Lock()
	cancel := tm._groupReplicationSyncCancel
	done := tm._groupReplicationSyncDone
	tm._groupReplicationSyncCancel = nil
	tm._groupReplicationSyncDone = nil
	tm.mutex.Unlock()

	if cancel != nil {
		cancel()
		<-done
	}
}

func (s *groupReplicationSync) run(ctx context.Context, interval time.Duration) {
	log.Info("Starting the group replication sync loop", slog.Duration("interval", interval))
	s.loopCtx = ctx
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		stepCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
		s.reconcile(stepCtx)
		cancel()
	}
}

// reconcile runs one iteration of the sync loop.
func (s *groupReplicationSync) reconcile(ctx context.Context) {
	tm := s.tm
	// The not-serving decisions made from here on are based on the status read below: a reason set
	// after this point by another component (a bootstrap RPC) is never cleared by this run.
	_, gen := tm.tmState.GroupReplicationNotServingState()
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		log.Warn("Group replication sync: cannot read the group replication status", slog.Any("error", err))
		s.publishVerdict(false, "")
		return
	}
	s.logTransition(status)
	s.publishVerdict(s.inLegitimateGroup(ctx, status), status.GetViewId())

	if mysql.IsGroupMemberActive(status) && s.isForeignGroup(ctx, status) {
		s.leaveForeignGroup(ctx)
		return
	}
	if mysql.IsGroupMemberActive(status) {
		s.warmVoterServerUUIDs(ctx)
	}
	s.liftOfflineModeInGroup(ctx, status)

	tablet := tm.Tablet()
	switch {
	case mysql.IsGroupPrimary(status) && tablet.Type != topodatapb.TabletType_PRIMARY:
		s.promote(ctx, tablet.Type)
	case tablet.Type == topodatapb.TabletType_PRIMARY && groupPrimaryLost(status):
		s.demote(ctx)
	}

	durability, err := s.getDurability(ctx)
	if err != nil {
		log.Warn("Group replication sync: cannot read the durability policy", slog.Any("error", err))
		return
	}
	tablet = tm.Tablet()
	if tablet.Type == topodatapb.TabletType_PRIMARY && mysql.IsGroupPrimary(status) {
		s.enforceSemiSync(ctx, status, durability, tablet)
	}
	s.enforceVoterMajority(ctx, status, durability, tablet, gen)
	if s.isStalePrimary(ctx, status, durability, tablet) {
		s.demoteStalePrimary(ctx, durability)
		tablet = tm.Tablet()
	}
	if s.shouldRejoin(ctx, status, durability, tablet) {
		s.rejoin(ctx)
	}
}

// inLegitimateGroup returns whether MySQL is ONLINE in the shard's legitimate group: in the
// recorded incarnation, with quorum in its view, and with a majority of the listed voters ONLINE
// in its view (MySQL's view quorum alone when no voter is listed). Only such a member receives the
// shard's transactions, and so can measure its replication lag (see publishVerdict).
//
// It does not ask the voters whose server_uuid the tablet does not know: the loop asks them in the
// background (warmVoterServerUUIDs), and voters are also found by the MySQL address of their
// tablet record.
func (s *groupReplicationSync) inLegitimateGroup(ctx context.Context, status *replicationdatapb.GroupReplicationStatus) bool {
	if status.GetMemberState() != mysql.GroupMemberStateOnline || !status.GetHasQuorum() {
		return false
	}
	rec, err := s.getRecord(ctx, false)
	if err != nil {
		log.Warn("Group replication sync: cannot read the shard's group record", slog.Any("error", err))
		return false
	}
	legitimate := s.tm.legitimateGroup(ctx, rec, status, false)
	return legitimate.IsLegitimateMember(status) && legitimate.HasVoterMajority(status)
}

// publishVerdict tells the query service whether MySQL is ONLINE in the shard's legitimate group,
// in the given view, on every run of the loop. The replication lag poller
// (--enable-replication-reporter) needs it on a member of a group: an applier that receives
// nothing reports no lag, whether the member is up to date or cut off from the shard's group. A
// member that is not in the legitimate group is reported with the lag accumulated since it last
// was, and the poller distrusts a verdict that the loop did not renew for a while.
func (s *groupReplicationSync) publishVerdict(healthy bool, viewID string) {
	if s.tm.QueryServiceControl == nil {
		return
	}
	s.tm.QueryServiceControl.SetGroupReplicationVerdict(healthy, viewID)
	if s.lastVerdict == nil || *s.lastVerdict != healthy {
		log.Info("Group replication sync: MySQL's membership of the shard's legitimate group changed",
			slog.Bool("in_legitimate_group", healthy),
			slog.String("view_id", viewID))
		s.lastVerdict = &healthy
	}
}

// liftOfflineModeInGroup clears offline_mode, which Group Replication's OFFLINE_MODE exit state
// action sets when the member leaves its group involuntarily, once the member is ONLINE in the
// shard's legitimate group again: in the recorded incarnation, with a majority of the listed voters
// ONLINE in its view (MySQL's view quorum when no voter is listed). It is not lifted earlier: while
// it is set, the tablet cannot serve stale reads, for example on a primary that its group left
// behind, or on a member that a join left alone in a group of its own. A RECOVERING member is
// still catching up.
//
// The primary of the recorded incarnation is the exception: it may hold fewer than a majority of
// the voters right after a bootstrap, and the other voters recover from it, which MySQL refuses
// while offline_mode is ON. Its tablet does not serve as the primary until the majority is back
// (enforceVoterMajority), and as a replica it holds every transaction of the shard's group.
func (s *groupReplicationSync) liftOfflineModeInGroup(ctx context.Context, status *replicationdatapb.GroupReplicationStatus) {
	tm := s.tm
	if status.GetMemberState() != mysql.GroupMemberStateOnline {
		return
	}
	on, err := tm.MysqlDaemon.IsOfflineMode(ctx)
	if err != nil {
		log.Warn("Group replication sync: cannot read offline_mode", slog.Any("error", err))
		return
	}
	if !on {
		return
	}
	rec, err := s.getRecord(ctx, false)
	if err != nil {
		log.Warn("Group replication sync: cannot read the shard's group record, not clearing offline_mode", slog.Any("error", err))
		return
	}
	legitimate := tm.legitimateGroup(ctx, rec, status, true)
	if !canLiftOfflineMode(legitimate, status) {
		return
	}
	if !tm.actionSema.TryAcquire(1) {
		return
	}
	defer tm.unlock()
	// Check again under the lock: an RPC may have made MySQL leave its group in the meantime.
	status, err = tm.groupReplicationStatus(ctx)
	if err != nil || !canLiftOfflineMode(legitimate, status) {
		return
	}
	if err := tm.liftOfflineMode(ctx, "MySQL is ONLINE in the shard's group"); err != nil {
		log.Error("Group replication sync: failed to clear offline_mode", slog.Any("error", err))
	}
}

// canLiftOfflineMode returns whether the member is ONLINE in the shard's legitimate group, with a
// majority of the voters in its view, or is the primary of the recorded incarnation.
func canLiftOfflineMode(legitimate *policy.LegitimateGroup, status *replicationdatapb.GroupReplicationStatus) bool {
	if status.GetMemberState() != mysql.GroupMemberStateOnline || !legitimate.IsLegitimateMember(status) {
		return false
	}
	return legitimate.HasVoterMajority(status) || (legitimate.Incarnation != "" && mysql.IsGroupPrimary(status))
}

// groupPrimaryLost returns whether MySQL was part of a group but is no longer its primary: it is
// an active member that is not the primary of a group with quorum, or it failed or was expelled.
// A MySQL that left its group on purpose, or never joined one, is OFFLINE and has not lost
// anything.
func groupPrimaryLost(status *replicationdatapb.GroupReplicationStatus) bool {
	if status == nil || !status.PluginActive {
		return false
	}
	if status.MemberState == mysql.GroupMemberStateError {
		return true
	}
	return mysql.IsGroupMemberActive(status) && !mysql.IsGroupPrimary(status)
}

// isTransitionalTabletType returns whether the tablet type belongs to an operation that owns
// the tablet for now: the sync loop leaves such a tablet alone.
func isTransitionalTabletType(tabletType topodatapb.TabletType) bool {
	switch tabletType {
	case topodatapb.TabletType_BACKUP, topodatapb.TabletType_RESTORE, topodatapb.TabletType_DRAINED:
		return true
	}
	return false
}

func (s *groupReplicationSync) logTransition(status *replicationdatapb.GroupReplicationStatus) {
	if status.MemberState == s.lastState && status.MemberRole == s.lastRole {
		return
	}
	log.Info("Group replication sync: member state changed",
		slog.String("group", status.GroupName),
		slog.String("from_state", s.lastState),
		slog.String("from_role", s.lastRole),
		slog.String("state", status.MemberState),
		slog.String("role", status.MemberRole),
		slog.Bool("has_quorum", status.HasQuorum),
		slog.Int("online_members", mysql.OnlineGroupMembers(status)))
	s.lastState = status.MemberState
	s.lastRole = status.MemberRole
}

// getDurability returns the shard's durability policy (TabletManager.shardDurability), cached for
// groupReplicationDurabilityCacheTTL. The cache is dropped as soon as a shard record that the loop
// read since sets another own policy for the shard: MigrateReplicationMode changes it under the
// shard lock when it converts the shard, and the loop reads the shard record much more often than
// the keyspace's.
func (s *groupReplicationSync) getDurability(ctx context.Context) (policy.Durabler, error) {
	if s.durability != nil && time.Since(s.durabilityRead) < groupReplicationDurabilityCacheTTL && !s.shardPolicyChanged() {
		return s.durability, nil
	}
	return s.readDurability(ctx)
}

// shardPolicyChanged returns whether the loop read a shard record after the cached durability
// policy, and that record sets another own policy for the shard than the one the cached policy was
// resolved with.
func (s *groupReplicationSync) shardPolicyChanged() bool {
	return s.record != nil && s.recordRead.After(s.durabilityRead) && s.record.durabilityPolicy != s.durabilityShardPolicy
}

// readDurability reads the shard's durability policy from the topology and caches it.
func (s *groupReplicationSync) readDurability(ctx context.Context) (policy.Durabler, error) {
	durability, shardPolicy, err := s.tm.resolveShardDurability(ctx)
	if err != nil {
		return nil, err
	}
	s.durability, s.durabilityRead, s.durabilityShardPolicy = durability, time.Now(), shardPolicy
	return durability, nil
}

// promote changes the tablet type to PRIMARY after the group made MySQL its primary.
func (s *groupReplicationSync) promote(ctx context.Context, tabletType topodatapb.TabletType) {
	tm := s.tm
	if isTransitionalTabletType(tabletType) {
		log.Warn("Group replication sync: MySQL is the group primary, but the tablet type does not allow promoting it",
			slog.String("tablet_type", tabletType.String()))
		return
	}
	if !tm.actionSema.TryAcquire(1) {
		return
	}
	defer tm.unlock()

	// Check again under the lock: an RPC may have changed the state in the meantime.
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil || !mysql.IsGroupPrimary(status) || tm.Tablet().Type == topodatapb.TabletType_PRIMARY {
		return
	}
	// Only the primary of the shard's legitimate group is followed. The shard record is read
	// again: the group may have been bootstrapped, or its voters changed, a moment ago.
	rec, err := s.getRecord(ctx, true)
	if err != nil {
		log.Warn("Group replication sync: cannot read the shard's group record, not promoting the tablet", slog.Any("error", err))
		return
	}
	legitimate := tm.legitimateGroup(ctx, rec, status, true)
	if legitimate.IsForeignIncarnation(status) {
		tm.leaveForeignGroupLocked(ctx, status, rec.incarnation)
		return
	}
	if !legitimate.IsLegitimatePrimary(status) {
		if time.Since(s.lastIllegitimateLog) >= groupReplicationIllegitimateLogInterval {
			s.lastIllegitimateLog = time.Now()
			log.Warn("Group replication sync: MySQL is the primary of its group, but not of the shard's legitimate group: not promoting the tablet",
				slog.String("group", status.GroupName),
				slog.String("view_id", status.ViewId),
				slog.String("recorded_incarnation", legitimate.Incarnation),
				slog.Int("online_voters", legitimate.OnlineVoters(status)),
				slog.Int("voters", len(legitimate.Voters)),
				slog.Int("online_members", mysql.OnlineGroupMembers(status)))
		}
		return
	}
	log.Info("Group replication sync: MySQL is the primary of its group, promoting the tablet to PRIMARY", slog.String("group", status.GroupName))
	if err := tm.liftOfflineMode(ctx, "MySQL is the primary of the shard's group"); err != nil {
		log.Error("Group replication sync: not promoting the tablet to PRIMARY", slog.Any("error", err))
		return
	}
	s.setTwoPCAllowed(twoPCDurable(status, tm.isPrimarySideSemiSyncEnabled(ctx)))
	// DBActionSetReadWrite redoes prepared transactions. MySQL is already writable. Whether the
	// new PRIMARY serves is decided on MySQL's status once more, against the shard's recorded
	// group (see groupReplicationServingReason).
	if err := tm.changeTypeWithGroupRecordLocked(ctx, topodatapb.TabletType_PRIMARY, DBActionSetReadWrite, SemiSyncActionNone, rec); err != nil {
		log.Error("Group replication sync: failed to promote the tablet to PRIMARY", slog.Any("error", err))
	}
}

// demote changes the tablet type from PRIMARY to REPLICA after MySQL lost the primary role in
// its group. It waits at most groupReplicationDemotionPublishTimeout for the topology server to
// store the tablet record, and leaves the rest to the background publisher: it holds the action
// lock, which the RPCs that recover the group need, and a member that lost its group is often one
// whose cell is cut off, topology server included.
func (s *groupReplicationSync) demote(ctx context.Context) {
	tm := s.tm
	if !tm.actionSema.TryAcquire(1) {
		return
	}
	defer tm.unlock()

	status, err := tm.groupReplicationStatus(ctx)
	if err != nil || !groupPrimaryLost(status) || tm.Tablet().Type != topodatapb.TabletType_PRIMARY {
		return
	}
	log.Warn("Group replication sync: MySQL is no longer the primary of its group, demoting the tablet to REPLICA",
		slog.String("group", status.GroupName),
		slog.String("state", status.MemberState),
		slog.String("role", status.MemberRole),
		slog.Bool("has_quorum", status.HasQuorum))
	s.twoPCAllowed = nil
	if err := tm.tmState.ChangeTabletTypeWithPublishTimeout(ctx, topodatapb.TabletType_REPLICA, DBActionNone, groupReplicationDemotionPublishTimeout); err != nil {
		log.Error("Group replication sync: failed to demote the tablet to REPLICA", slog.Any("error", err))
	}
}

// isStalePrimary returns whether the tablet is PRIMARY, although its MySQL is not in any group while
// the shard's durability policy uses Group Replication and the shard record lists the tablet as a
// voter. Such a tablet is left over from before its MySQL failed or restarted: the group elected
// another primary, or will once a majority is back. A member in the ERROR state is handled by
// groupPrimaryLost.
func (s *groupReplicationSync) isStalePrimary(ctx context.Context, status *replicationdatapb.GroupReplicationStatus, durability policy.Durabler, tablet *topodatapb.Tablet) bool {
	if tablet.Type != topodatapb.TabletType_PRIMARY || !policy.IsGroupReplication(durability) || status == nil {
		return false
	}
	if status.PluginActive && status.MemberState != mysql.GroupMemberStateOffline {
		return false
	}
	voters, err := s.getVoters(ctx)
	if err != nil {
		log.Warn("Group replication sync: cannot read the voters of the group", slog.Any("error", err))
		return false
	}
	return policy.IsVoter(voters, tablet.Alias)
}

// demoteStalePrimary changes the type of a stale PRIMARY tablet to REPLICA, after checking again
// under the action lock. MySQL is left as it is. Like demote, it does not wait for an unresponsive
// topology server under the action lock.
func (s *groupReplicationSync) demoteStalePrimary(ctx context.Context, durability policy.Durabler) {
	tm := s.tm
	if !tm.actionSema.TryAcquire(1) {
		return
	}
	defer tm.unlock()
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil || !s.isStalePrimary(ctx, status, durability, tm.Tablet()) {
		return
	}
	log.Warn("Group replication sync: the tablet is PRIMARY, but its MySQL is a voter that is not in its group, demoting the tablet to REPLICA",
		slog.Bool("plugin_active", status.PluginActive),
		slog.String("state", status.MemberState))
	s.twoPCAllowed = nil
	if err := tm.tmState.ChangeTabletTypeWithPublishTimeout(ctx, topodatapb.TabletType_REPLICA, DBActionNone, groupReplicationDemotionPublishTimeout); err != nil {
		log.Error("Group replication sync: failed to demote the tablet to REPLICA", slog.Any("error", err))
	}
}

// enforceSemiSync applies the effective semi-sync setting on the primary: the durability policy
// asks for semi-sync, and no group with at least two ONLINE members supersedes it. Semi-sync is
// only enabled while a replica is connected to acknowledge transactions: with Vitess's infinite
// semi-sync timeout, enabling it without one would block every commit, for example while a
// member of a group of two restarts.
func (s *groupReplicationSync) enforceSemiSync(ctx context.Context, status *replicationdatapb.GroupReplicationStatus, durability policy.Durabler, tablet *topodatapb.Tablet) {
	tm := s.tm
	superseded := mysql.GroupSupersedesSemiSync(status)
	want := policy.SemiSyncAckers(durability, tablet) > 0 && !superseded
	enabled := tm.isPrimarySideSemiSyncEnabled(ctx)
	if want && !enabled && !s.hasSemiSyncReplicas(ctx) {
		want = false
	}
	if want == enabled && s.twoPCAllowed != nil && *s.twoPCAllowed == twoPCDurable(status, enabled) {
		return
	}

	if !tm.actionSema.TryAcquire(1) {
		return
	}
	defer tm.unlock()
	if tm.Tablet().Type != topodatapb.TabletType_PRIMARY {
		return
	}
	if want != enabled {
		semiSyncAction, err := tm.convertBoolToSemiSyncAction(ctx, want)
		if err != nil {
			log.Warn("Group replication sync: cannot change semi-sync", slog.Any("error", err))
			return
		}
		log.Info("Group replication sync: changing primary semi-sync",
			slog.Bool("enabled", want),
			slog.Bool("superseded_by_group", superseded),
			slog.Int("online_members", mysql.OnlineGroupMembers(status)))
		if err := tm.fixSemiSync(ctx, topodatapb.TabletType_PRIMARY, semiSyncAction); err != nil {
			log.Error("Group replication sync: failed to change semi-sync", slog.Any("error", err))
			return
		}
		enabled = want
	}
	s.setTwoPCAllowed(twoPCDurable(status, enabled))
}

// groupReplicationVoterMajorityLost is the reason for which a PRIMARY tablet does not serve while
// its group lacks the majority of its voters.
const groupReplicationVoterMajorityLost = "replication group lost the majority of its voters"

// enforceVoterMajority makes a PRIMARY tablet stop serving while its MySQL is the primary of a
// group view that holds fewer than a majority of the shard's listed voters, or of a group of
// another incarnation than the recorded one, and serve again once both hold. MySQL is left alone.
// The check only applies under a group replication policy with listed voters: during a
// migration, the group grows from a single member while the primary keeps serving with semi-sync.
//
// The status that the loop read at the start of its run can be old by now: the run may have waited
// for an unresponsive topology since. Stopping to serve on it is safe, and it is done right away.
// Serving again is not: in the S7d chaos scenario, a run read MySQL's status while MySQL was the
// primary of two voters, waited for the topology of its cut-off cell, and after the heal made the
// tablet serve again 16ms after a bootstrap RPC had stopped it; the bootstrap then made MySQL the
// writable primary of a group of one, which acknowledged writes on that single voter for 1.3s. The
// loop therefore only lets the tablet serve again from serveAgain: under the action lock that the
// bootstrap and the other RPCs that change MySQL's group hold, on a status read after acquiring it,
// and only if no not-serving reason was set since (see ClearGroupReplicationNotServing).
//
// While the tablet is PRIMARY under a group replication policy but its MySQL is not a group primary
// (for example while a bootstrap runs), the loop leaves the reason as it is: the demotion steps
// handle such a tablet.
func (s *groupReplicationSync) enforceVoterMajority(ctx context.Context, status *replicationdatapb.GroupReplicationStatus, durability policy.Durabler, tablet *topodatapb.Tablet, gen uint64) {
	tm := s.tm
	current, _ := tm.tmState.GroupReplicationNotServingState()
	if tablet.Type != topodatapb.TabletType_PRIMARY || !policy.IsGroupReplication(durability) {
		// The reason only applies to a PRIMARY under a group replication policy. It is cleared
		// unless one was set since the run started: a promotion decides again under the action
		// lock (changeTypeLocked).
		if current != "" {
			if _, err := tm.tmState.ClearGroupReplicationNotServing(ctx, gen); err != nil {
				log.Error("Group replication sync: failed to change the serving state of the primary", slog.Any("error", err))
			}
		}
		return
	}
	if !mysql.IsGroupPrimary(status) {
		return
	}
	reason, rec, durability, err := s.servingReason(ctx, status, durability)
	if err != nil {
		log.Warn("Group replication sync: cannot check the voter majority of the group", slog.Any("error", err))
		return
	}
	if !policy.IsGroupReplication(durability) {
		// The policy changed since the loop read it: the rule does not apply anymore.
		if current != "" {
			if _, err := tm.tmState.ClearGroupReplicationNotServing(ctx, gen); err != nil {
				log.Error("Group replication sync: failed to change the serving state of the primary", slog.Any("error", err))
			}
		}
		return
	}
	if reason != "" {
		if reason != current {
			log.Warn("Group replication sync: the primary stops serving",
				slog.String("reason", reason),
				slog.String("group", status.GetGroupName()),
				slog.String("view_id", status.GetViewId()),
				slog.Int("online_members", mysql.OnlineGroupMembers(status)))
		}
		if err := tm.tmState.SetGroupReplicationNotServing(ctx, reason); err != nil {
			log.Error("Group replication sync: failed to change the serving state of the primary", slog.Any("error", err))
		}
		return
	}
	if current != "" {
		s.serveAgain(ctx, durability, rec)
	}
}

// servingReason returns why the PRIMARY tablet must not serve for the given status of its MySQL
// (see groupReplicationServingReason), the shard record it decided on, and the durability policy it
// decided under. Before it reports a lost voter majority, it reads the shard record and the
// durability policy again and asks the voters whose server_uuid it does not know
// (voterMajorityLost); before it reports an unrecorded incarnation, it reads the shard record again:
// the group may have been bootstrapped and recorded a moment ago.
func (s *groupReplicationSync) servingReason(ctx context.Context, status *replicationdatapb.GroupReplicationStatus, durability policy.Durabler) (string, *shardGroupRecord, policy.Durabler, error) {
	lost, err := s.voterMajorityLost(ctx, status)
	if err != nil {
		return "", nil, durability, err
	}
	if s.durability != nil {
		// voterMajorityLost may have read the policy again.
		durability = s.durability
	}
	if lost {
		return groupReplicationVoterMajorityLost, s.record, durability, nil
	}
	rec, err := s.getRecord(ctx, false)
	if err != nil {
		return "", nil, durability, err
	}
	if rec.incarnation != "" && rec.incarnation != policy.GroupIncarnation(status.GetViewId()) {
		if rec, err = s.getRecord(ctx, true); err != nil {
			return "", nil, durability, err
		}
	}
	// voterMajorityLost asked the voters it could not identify a moment ago, at most every
	// groupReplicationVotersCacheTTL.
	return s.tm.groupReplicationServingReason(ctx, durability, rec, status, false), rec, durability, nil
}

// serveAgain lets the PRIMARY tablet serve again, after deciding once more under the action lock,
// on a status of MySQL read after acquiring it, against the shard record rec that the run read
// before (the topology is not read under the lock). The lock is taken without waiting: an RPC that
// holds it may be changing MySQL's group (a bootstrap), and the next run decides after it.
func (s *groupReplicationSync) serveAgain(ctx context.Context, durability policy.Durabler, rec *shardGroupRecord) {
	tm := s.tm
	if !tm.actionSema.TryAcquire(1) {
		return
	}
	defer tm.unlock()
	_, gen := tm.tmState.GroupReplicationNotServingState()
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		log.Warn("Group replication sync: cannot read the group replication status, the primary does not serve yet", slog.Any("error", err))
		return
	}
	if tm.Tablet().Type != topodatapb.TabletType_PRIMARY {
		return
	}
	// The voters are not asked for their server_uuids under the lock: servingReason did, before.
	if reason := tm.groupReplicationServingReason(ctx, durability, rec, status, false); reason != "" {
		if err := tm.tmState.SetGroupReplicationNotServing(ctx, reason); err != nil {
			log.Error("Group replication sync: failed to change the serving state of the primary", slog.Any("error", err))
		}
		return
	}
	cleared, err := tm.tmState.ClearGroupReplicationNotServing(ctx, gen)
	if err != nil {
		log.Error("Group replication sync: failed to change the serving state of the primary", slog.Any("error", err))
		return
	}
	if cleared {
		log.Info("Group replication sync: the primary serves again",
			slog.String("group", status.GetGroupName()),
			slog.String("view_id", status.GetViewId()),
			slog.Int("online_members", mysql.OnlineGroupMembers(status)))
	}
}

// voterMajorityLost returns whether the member's view holds fewer than a majority of the shard's
// listed voters. Before it reports a loss, it reads the shard record and the durability policy
// again, and asks the voters whose server_uuid it does not know, at most every
// groupReplicationVotersCacheTTL. It reports no loss under a policy that is no longer a group
// replication policy.
func (s *groupReplicationSync) voterMajorityLost(ctx context.Context, status *replicationdatapb.GroupReplicationStatus) (bool, error) {
	rec, err := s.getRecord(ctx, false)
	if err != nil {
		return false, err
	}
	if len(rec.voters) == 0 {
		return false, nil
	}
	if s.tm.legitimateGroup(ctx, rec, status, false).HasVoterMajority(status) {
		return false, nil
	}
	if time.Since(s.peersFetched) < groupReplicationVotersCacheTTL {
		// Checked recently: the majority is still missing.
		return true, nil
	}
	s.peersFetched = time.Now()
	if rec, err = s.getRecord(ctx, true); err != nil {
		return false, err
	}
	// A migration back to semi-sync sets the shard's policy first, and then shrinks the group to
	// the primary: under the policy cached a moment ago, the primary would stop serving, and serve
	// again once the cache expires, a second pause before the primary's own leave of the group.
	if durability, err := s.readDurability(ctx); err == nil && !policy.IsGroupReplication(durability) {
		return false, nil
	}
	return len(rec.voters) > 0 && !s.tm.legitimateGroup(ctx, rec, status, true).HasVoterMajority(status), nil
}

// hasSemiSyncReplicas returns whether at least one semi-sync replica is connected to the primary.
func (s *groupReplicationSync) hasSemiSyncReplicas(ctx context.Context) bool {
	vars, err := s.tm.MysqlDaemon.GetGlobalStatusVars(ctx, []string{"Rpl_semi_sync_source_clients", "Rpl_semi_sync_master_clients"})
	if err != nil {
		return false
	}
	for _, v := range vars {
		if v != "" && v != "0" {
			return true
		}
	}
	return false
}

func (s *groupReplicationSync) setTwoPCAllowed(allowed bool) {
	if s.twoPCAllowed != nil && *s.twoPCAllowed == allowed {
		return
	}
	s.tm.QueryServiceControl.SetTwoPCAllowed(tabletserver.TwoPCAllowed_SemiSync, allowed)
	s.twoPCAllowed = &allowed
}

// getRecord returns the shard record's view of the shard's legitimate group, cached for
// groupReplicationVotersCacheTTL unless fresh is set. The tablet records are reused for longer
// (readShardGroupRecord).
func (s *groupReplicationSync) getRecord(ctx context.Context, fresh bool) (*shardGroupRecord, error) {
	if !fresh && s.record != nil && time.Since(s.recordRead) < groupReplicationVotersCacheTTL {
		return s.record, nil
	}
	rec, err := s.tm.readShardGroupRecord(ctx, s.record)
	if err != nil {
		return nil, err
	}
	s.record = rec
	s.recordRead = time.Now()
	return rec, nil
}

// warmVoterServerUUIDs asks the voters whose server_uuid the tablet does not know for it, in the
// background and at most every groupReplicationVoterUUIDWarmInterval, so that a promotion after a
// failure finds the voters in its view without waiting for them.
func (s *groupReplicationSync) warmVoterServerUUIDs(ctx context.Context) {
	if time.Since(s.uuidsWarmed) < groupReplicationVoterUUIDWarmInterval {
		return
	}
	rec, err := s.getRecord(ctx, false)
	if err != nil {
		return
	}
	missing := s.tm.votersWithoutServerUUID(rec)
	if len(missing) == 0 {
		return
	}
	s.uuidsWarmed = time.Now()
	base := s.loopCtx
	if base == nil {
		base = ctx
	}
	go s.tm.fetchPeerServerUUIDs(base, missing, func() bool { return false })
}

// isForeignGroup returns whether MySQL is an active member of a group of another incarnation
// than the one the shard record lists, and that this tablet did not bootstrap a moment ago. A
// mismatch with the cached record is confirmed with a fresh read before it counts.
func (s *groupReplicationSync) isForeignGroup(ctx context.Context, status *replicationdatapb.GroupReplicationStatus) bool {
	incarnation := policy.GroupIncarnation(status.GetViewId())
	if incarnation == "" || incarnation == s.tm.groupReplicationPeers.recentlyBootstrapped() {
		return false
	}
	rec, err := s.getRecord(ctx, false)
	if err != nil || rec.incarnation == "" || rec.incarnation == incarnation {
		return false
	}
	rec, err = s.getRecord(ctx, true)
	if err != nil {
		return false
	}
	return rec.incarnation != "" && rec.incarnation != incarnation
}

// leaveForeignGroup makes MySQL leave a group that is not the shard's legitimate group, after
// checking again under the action lock.
func (s *groupReplicationSync) leaveForeignGroup(ctx context.Context) {
	tm := s.tm
	if !tm.actionSema.TryAcquire(1) {
		return
	}
	defer tm.unlock()
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil || !mysql.IsGroupMemberActive(status) || !s.isForeignGroup(ctx, status) {
		return
	}
	s.twoPCAllowed = nil
	tm.leaveForeignGroupLocked(ctx, status, s.record.incarnation)
}

// getVoters returns the voters of the shard's group from the shard record, cached for
// groupReplicationVotersCacheTTL.
func (s *groupReplicationSync) getVoters(ctx context.Context) ([]*topodatapb.TabletAlias, error) {
	if !s.votersRead.IsZero() && time.Since(s.votersRead) < groupReplicationVotersCacheTTL {
		return s.voters, nil
	}
	voters, err := s.tm.groupReplicationVoters(ctx)
	if err != nil {
		return nil, err
	}
	s.voters = voters
	s.votersRead = time.Now()
	return voters, nil
}

// shouldRejoin returns whether the sync loop should make MySQL rejoin its group now. Only a
// tablet that the shard record lists as a voter rejoins.
func (s *groupReplicationSync) shouldRejoin(ctx context.Context, status *replicationdatapb.GroupReplicationStatus, durability policy.Durabler, tablet *topodatapb.Tablet) bool {
	if mysql.IsGroupMemberActive(status) {
		s.rejoinBackoff = 0
		return false
	}
	if !policy.IsGroupReplication(durability) {
		return false
	}
	// A PRIMARY tablet whose MySQL is not in the group serves writes on its own, for example
	// during a migration back to asynchronous replication. Joining would make it read-only.
	if tablet.Type == topodatapb.TabletType_PRIMARY || isTransitionalTabletType(tablet.Type) {
		return false
	}
	if s.tm.groupReplicationRejoinSuspended.Load() || s.tm.IsBackupRunning() {
		return false
	}
	if time.Now().Before(s.nextRejoin) {
		return false
	}
	// The shard record is read last, and cached: most runs of the loop stop earlier.
	voters, err := s.getVoters(ctx)
	if err != nil {
		log.Warn("Group replication sync: cannot read the voters of the group", slog.Any("error", err))
		return false
	}
	return policy.IsVoter(voters, tablet.Alias)
}

// rejoin makes MySQL join its group, and backs off exponentially if it fails. It does not start
// a join while no other tablet reports an active member of the shard's legitimate group: such a
// START cannot join anything and blocks until MySQL's join timeout, during which a bootstrap on
// this member fails with "START or STOP GROUP_REPLICATION is ongoing".
func (s *groupReplicationSync) rejoin(ctx context.Context) {
	tm := s.tm
	if err := tm.checkLegitimateGroupToJoin(ctx); err != nil {
		s.nextRejoin = time.Now().Add(max(groupReplicationSyncInterval, groupReplicationRejoinGateInterval))
		if time.Since(s.lastIllegitimateLog) >= groupReplicationIllegitimateLogInterval {
			s.lastIllegitimateLog = time.Now()
			log.Info("Group replication sync: MySQL is not in its group, but not joining it", slog.Any("reason", err))
		}
		return
	}
	if !tm.actionSema.TryAcquire(1) {
		return
	}
	defer tm.unlock()

	log.Info("Group replication sync: MySQL is not in its group, joining it")
	// A join includes the distributed recovery, and MySQL keeps running a START whose client gave
	// up: wait for it longer than one step of the loop.
	joinBase := s.loopCtx
	if joinBase == nil {
		joinBase = ctx
	}
	joinCtx, cancel := context.WithTimeout(joinBase, groupReplicationJoinTimeout)
	defer cancel()
	if _, err := tm.startGroupReplicationLocked(joinCtx, false /* bootstrap */); err != nil {
		if s.rejoinBackoff == 0 {
			s.rejoinBackoff = groupReplicationSyncInterval
		} else {
			s.rejoinBackoff = min(2*s.rejoinBackoff, groupReplicationMaxRejoinBackoff)
		}
		s.nextRejoin = time.Now().Add(s.rejoinBackoff)
		log.Warn("Group replication sync: failed to join the group, backing off",
			slog.Duration("backoff", s.rejoinBackoff),
			slog.Any("error", err))
		return
	}
	s.rejoinBackoff = 0
	s.nextRejoin = time.Time{}
}
