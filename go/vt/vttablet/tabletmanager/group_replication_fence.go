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
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// groupReplicationFenceCheckInterval is how often the fence check reads MySQL's group view while
	// it is armed (see groupReplicationFence.armed).
	groupReplicationFenceCheckInterval = 150 * time.Millisecond
	// groupReplicationJoinWatchWindow is how long the fence check stays armed after a join or a
	// bootstrap of the tablet's MySQL ended. A join that does not find its group can end in a group
	// of its own when it completes.
	groupReplicationJoinWatchWindow = 30 * time.Second
	// groupReplicationFenceTimeout bounds the fence check's read of MySQL, and its SET GLOBAL
	// super_read_only.
	groupReplicationFenceTimeout = 2 * time.Second
	// groupReplicationFenceLockWaitTimeout bounds how long SET GLOBAL super_read_only waits for the
	// statements that hold metadata locks: a long write statement holds it back until it ends
	// (verified on MySQL 8.4.11), and the check retries on its next run instead.
	groupReplicationFenceLockWaitTimeout = 1 * time.Second
)

// groupReplicationFence is what the tablet knows about the changes of its MySQL's group membership
// that it starts itself, and about the fence it set. Its zero value is ready to use.
//
// MySQL makes the primary of every group writable, including a group that the tablet does not
// follow: a join that did not find its group, completed by MySQL as the only member of a new
// incarnation (doc/failover-audit/GroupReplication.md, "A join can form a group of one"), or the
// group of a serving primary whose other voters left cleanly, a view of one that keeps quorum. The
// tablet fences such a MySQL with super_read_only, without waiting for its sync loop, and the fence
// is only lifted by a decision that the tablet may serve as PRIMARY (the serving invariant).
//
// The fence check does not take the action lock, so mu orders its fences with what the holders of
// the action lock do:
//   - A join, a bootstrap or a leave of MySQL's group starts a new epoch. A fence decided on a
//     status read in an earlier epoch is dropped, and a join or a bootstrap does not start while a
//     fence is being set: Group Replication clears super_read_only when it elects the bootstrapped
//     primary, and a fence set after that would keep the new group read-only.
//   - Each fence is a new decision (decisions). A decision to make MySQL writable takes a snapshot
//     of decisions before it reads MySQL's status, and yields to a fence decided since: it does not
//     make MySQL writable, or, if it already did, fences MySQL again (settle). Since a fence is
//     decided and set under mu, and settle runs under mu after MySQL was made writable, MySQL ends
//     up fenced whichever comes first.
type groupReplicationFence struct {
	// starts counts the joins and bootstraps of the tablet's MySQL in progress, bootstraps the
	// bootstraps among them.
	starts     atomic.Int32
	bootstraps atomic.Int32
	// lastStartEnd is when the last join or bootstrap ended, in Unix nanoseconds, 0 if none did, and
	// lastStartFailed whether it failed.
	lastStartEnd    atomic.Int64
	lastStartFailed atomic.Bool

	// mu is held to change fenced, epoch and decisions, and while the fence check sets
	// super_read_only (bounded by groupReplicationFenceTimeout). It is never held across a read of
	// the topology, nor while waiting for the action lock.
	mu sync.Mutex
	// fenced is set while the fence holds MySQL super_read_only: a fence was decided, and since then
	// no decision that the tablet may serve lifted it, nor did MySQL join or leave a group.
	fenced atomic.Bool
	// epoch counts the joins, bootstraps and leaves of MySQL's group that started.
	epoch atomic.Uint64
	// decisions counts the decisions to fence MySQL, and reason is the last one's reason.
	decisions atomic.Uint64
	reason    atomic.Pointer[string]
}

// beginStart records that a join, or a bootstrap, of the tablet's MySQL starts, in a new epoch. The
// returned function records its end, and whether it failed.
func (f *groupReplicationFence) beginStart(bootstrap bool) func(failed bool) {
	f.mu.Lock()
	f.starts.Add(1)
	if bootstrap {
		f.bootstraps.Add(1)
	}
	f.epoch.Add(1)
	f.mu.Unlock()
	return func(failed bool) {
		f.lastStartFailed.Store(failed)
		f.lastStartEnd.Store(time.Now().UnixNano())
		if bootstrap {
			f.bootstraps.Add(-1)
		}
		f.starts.Add(-1)
	}
}

// reset records that MySQL is about to start Group Replication, or stopped it, in a new epoch:
// Group Replication decides from here whether MySQL is writable (it keeps a member out of a group
// super_read_only), and the caller, which holds the action lock, whether the tablet serves.
func (f *groupReplicationFence) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fenced.Store(false)
	f.epoch.Add(1)
}

// decide records a decision to fence MySQL for reason, on a status read in the given epoch, unless
// a new epoch started since or exempt returns true. It returns whether the decision stands, and
// whether MySQL was not fenced before. On success, the caller holds mu, and must call unlock once
// it set super_read_only.
func (f *groupReplicationFence) decide(epoch uint64, reason string, exempt func() bool) (first, ok bool) {
	f.mu.Lock()
	if f.epoch.Load() != epoch || exempt() {
		f.mu.Unlock()
		return false, false
	}
	first = !f.fenced.Swap(true)
	f.decisions.Add(1)
	f.reason.Store(&reason)
	return first, true
}

// unlock releases mu after a successful decide.
func (f *groupReplicationFence) unlock() {
	f.mu.Unlock()
}

// snapshot returns the number of decisions to fence MySQL so far. A decision to make MySQL writable
// takes it before it reads MySQL's status.
func (f *groupReplicationFence) snapshot() uint64 {
	return f.decisions.Load()
}

// decidedSince returns whether a fence was decided since snapshot returned snap.
func (f *groupReplicationFence) decidedSince(snap uint64) bool {
	return f.decisions.Load() != snap
}

// settle records, after the caller made MySQL writable on a decision that it took after snapshot
// returned snap, that MySQL is not fenced, unless a fence was decided since: it then returns false,
// and the caller fences MySQL again (TabletManager.refenceGroupReplicationMember).
func (f *groupReplicationFence) settle(snap uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.decisions.Load() != snap {
		return false
	}
	f.fenced.Store(false)
	return true
}

// lastReason returns the reason of the last decision to fence MySQL.
func (f *groupReplicationFence) lastReason() string {
	if reason := f.reason.Load(); reason != nil {
		return *reason
	}
	return groupReplicationVoterMajorityLost
}

// armed returns whether the fence check reads MySQL's view, given the tablet's type: on a PRIMARY
// tablet, while MySQL is fenced, while a join or a bootstrap of the tablet's MySQL runs, and for
// groupReplicationJoinWatchWindow after one ended. MySQL keeps running a START GROUP_REPLICATION
// whose client gave up, so a start that failed keeps it armed for groupReplicationJoinTimeout, the
// time the sync loop gives a join, if that is longer.
func (f *groupReplicationFence) armed(tabletType topodatapb.TabletType) bool {
	if tabletType == topodatapb.TabletType_PRIMARY || f.fenced.Load() || f.starts.Load() > 0 {
		return true
	}
	end := f.lastStartEnd.Load()
	if end == 0 {
		return false
	}
	window := groupReplicationJoinWatchWindow
	if f.lastStartFailed.Load() {
		window = max(window, groupReplicationJoinTimeout)
	}
	return time.Since(time.Unix(0, end)) < window
}

// runFenceCheck runs the fence check every interval until ctx ends. It only reads MySQL while the
// check is armed.
func (s *groupReplicationSync) runFenceCheck(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		s.checkFence(ctx)
	}
}

// checkFence reads MySQL's group view with a single query and fences MySQL if it must not take
// writes (see groupReplicationFenceReason): it sets super_read_only, then makes a PRIMARY tablet
// stop serving through the serving invariant's not-serving reason, and wakes up the sync loop,
// which confirms the decision against a shard record read again, and leaves a foreign group.
//
// It reads neither the topology nor takes the action lock: it decides on the shard record and the
// durability policy that the tablet read last, and it only ever makes MySQL refuse writes. Making
// MySQL writable again, and serving again, is decided under the action lock, on MySQL's status read
// under it, against a shard record read before (serveAgain, changeTypeLocked, UndoDemotePrimary),
// and yields to a fence decided since it started (see groupReplicationFence). A fence decided on a
// status read before MySQL joined, bootstrapped or left a group is dropped.
func (s *groupReplicationSync) checkFence(ctx context.Context) {
	tm := s.tm
	tablet := tm.Tablet()
	if !tm.groupReplicationFence.armed(tablet.Type) {
		s.fenceViewID = ""
		return
	}
	epoch := tm.groupReplicationFence.epoch.Load()
	// MySQL does not return the view id while one of the tablet's START GROUP_REPLICATION runs,
	// which is when a join forms a group of its own: the check then decides on the members alone.
	withView := tm.groupReplicationFence.starts.Load() == 0
	readCtx, cancel := context.WithTimeout(ctx, groupReplicationFenceTimeout)
	fs, err := tm.MysqlDaemon.GroupReplicationFenceStatus(readCtx, withView)
	cancel()
	if err != nil || fs == nil || fs.Status == nil {
		return
	}
	if fs.ServerUUID != "" {
		tm.groupReplicationPeers.setServerUUID(topoproto.TabletAliasString(tm.tabletAlias), fs.ServerUUID)
	}
	if viewID := fs.Status.GetViewId(); fs.ViewKnown {
		if tablet.Type == topodatapb.TabletType_PRIMARY && s.fenceViewID != "" && viewID != s.fenceViewID {
			// The primary's view changed: a voter joined or left. The sync loop reads the shard record
			// and the durability policy again, so that the check below decides on them. A migration
			// back to semi-sync sets the shard's own policy before the first voter leaves.
			s.requestRefresh()
		}
		s.fenceViewID = viewID
	}

	reason := tm.groupReplicationFenceReason(fs.Status, fs.ViewKnown, tablet.Type, &s.majorityIncarnation, time.Now())
	if reason == "" {
		return
	}
	if tm.fenceGroupReplicationMember(ctx, fs, reason, epoch) {
		s.requestRefresh()
	}
}

// groupReplicationFenceReason returns why MySQL, as status reports it, must not take writes, or ""
// if it may. It decides on the shard record and the durability policy that the tablet read last,
// and on nothing that needs the topology or another tablet. Only the ONLINE primary of a group with
// quorum takes writes, and only under a group replication policy that lists voters:
//
//   - A stray group: the incarnation is not the recorded one, and the view holds fewer than a
//     majority of the voters. A join that did not find the shard's group can end that way, as the
//     only member of a new incarnation (any tablet type).
//   - A shrunk or superseded group, on a PRIMARY tablet: its MySQL was the primary of a view with a
//     majority of the voters in this incarnation (majorityIncarnation, which the check maintains),
//     and the view now holds fewer than a majority of the voters (they left cleanly, and MySQL's
//     view quorum only counts the members still in the view), or the shard record lists another
//     incarnation.
//   - While the fence holds, the same conditions keep it: Group Replication clears super_read_only
//     on the member it elects primary.
//   - Without the view id (viewKnown unset; MySQL does not return it while one of the tablet's
//     joins runs): a view that holds fewer than a majority of the voters. A join into the shard's
//     group never makes the joiner the primary: it is a group of its own, or a group whose other
//     voters all left.
//
// It never fences a group in the recorded incarnation that never had the voter majority, such as
// one bootstrapped a moment ago, which other voters are about to join, and which InitPrimary and
// PlannedReparentShard write to before they do; nor a group that this tablet is bootstrapping, or
// bootstrapped within groupReplicationBootstrapGrace and whose incarnation is not recorded yet; nor
// the new group of which this tablet's MySQL is the primary while a live bootstrap intent names
// this tablet (VTOrc adopts that group; see shardGroupRecord.adoptableByIntent); nor anything
// during a planned pause of the primary. A view whose voters the tablet cannot all identify, and
// which holds enough members to be a majority, is left to the sync loop, which asks the voters for
// their server_uuid.
func (tm *TabletManager) groupReplicationFenceReason(status *replicationdatapb.GroupReplicationStatus, viewKnown bool, tabletType topodatapb.TabletType, majorityIncarnation *string, now time.Time) string {
	if !mysql.IsGroupPrimary(status) {
		return ""
	}
	rec := tm.groupReplicationTopo.lastRecord()
	if rec == nil || len(rec.voters) == 0 || !tm.cachedPolicyIsGroupReplication(rec) {
		return ""
	}
	if !viewKnown {
		if tm.groupReplicationFence.bootstraps.Load() > 0 || tm.tmState.servingPaused() {
			return ""
		}
		legitimate := policy.NewLegitimateGroup(rec.incarnation, rec.voters, rec.tablets, tm.knownServerUUIDs(rec))
		if !legitimate.HasVoterMajority(status) &&
			(mysql.OnlineGroupMembers(status) < legitimate.VoterMajority() || identifiesEveryVoter(legitimate)) {
			return groupReplicationVoterMajorityLost
		}
		return ""
	}
	incarnation := policy.GroupIncarnation(status.GetViewId())
	recorded := rec.incarnation == "" || incarnation == rec.incarnation
	// The tablet trusts a group it bootstrapped until its incarnation is recorded, not after: the
	// migration to Group Replication and VTOrc's recoveries bootstrap a group that its voters join
	// and leave within groupReplicationBootstrapGrace.
	if tm.groupReplicationFence.bootstraps.Load() > 0 || tm.tmState.servingPaused() ||
		(!recorded && incarnation != "" && incarnation == tm.groupReplicationPeers.recentlyBootstrapped()) ||
		rec.adoptableByIntent(tm.tabletAlias, incarnation, now) {
		return ""
	}
	legitimate := policy.NewLegitimateGroup(rec.incarnation, rec.voters, rec.tablets, tm.knownServerUUIDs(rec))
	majority := legitimate.HasVoterMajority(status)
	if majority && recorded {
		if *majorityIncarnation != incarnation {
			log.Info("Group replication: the fence check saw MySQL as the primary of its group with the voter majority, and fences a later shrink of the group",
				slog.String("view_id", status.GetViewId()), slog.Int("online_members", mysql.OnlineGroupMembers(status)), slog.String("tablet_type", tabletType.String()))
		}
		*majorityIncarnation = incarnation
		return ""
	}
	lacking := !majority && (mysql.OnlineGroupMembers(status) < legitimate.VoterMajority() || identifiesEveryVoter(legitimate))
	notServable := lacking || !recorded
	switch {
	case !recorded && lacking:
		return groupReplicationUnrecordedIncarnation
	case tabletType == topodatapb.TabletType_PRIMARY && *majorityIncarnation != "" && incarnation == *majorityIncarnation && notServable,
		tm.groupReplicationFence.fenced.Load() && notServable:
		if !recorded {
			return groupReplicationUnrecordedIncarnation
		}
		return groupReplicationVoterMajorityLost
	}
	return ""
}

// identifiesEveryVoter returns whether every voter can be found in a view: by its server_uuid, or
// by the MySQL address of its tablet record.
func identifiesEveryVoter(legitimate *policy.LegitimateGroup) bool {
	for _, voter := range legitimate.Voters {
		if voter.ServerUUID == "" && (voter.MysqlHost == "" || voter.MysqlPort == 0) {
			return false
		}
	}
	return true
}

// cachedPolicyIsGroupReplication returns whether the shard's durability policy, as the tablet read
// it last, is a group replication policy: the shard's own policy in rec if it sets one, else the
// policy the tablet resolved last.
func (tm *TabletManager) cachedPolicyIsGroupReplication(rec *shardGroupRecord) bool {
	name := rec.durabilityPolicy
	if name == "" {
		var known bool
		if name, known = tm.groupReplicationTopo.lastDurability(); !known {
			return false
		}
	}
	durability, err := policy.GetDurabilityPolicy(name)
	return err == nil && policy.IsGroupReplication(durability)
}

// fenceGroupReplicationMember makes MySQL refuse writes for the given reason, decided on fs, which
// the fence check read in the given epoch: super_read_only first, which also refuses the clients
// that write to MySQL directly, then the not-serving reason of a PRIMARY tablet, through which vtgate
// buffers. A tablet of another type gets no reason, which its sync loop would clear: its promotion
// decides whether it serves, and lifts the fence only if it may (changeTypeWithGroupRecordLocked).
// The fence is dropped if MySQL joined, bootstrapped or left a group since fs was read, or if a
// bootstrap or a planned pause of the primary started since. It returns whether it changed
// anything: a fence that holds is checked on every run without waking the sync loop.
func (tm *TabletManager) fenceGroupReplicationMember(ctx context.Context, fs *mysql.GroupReplicationFenceStatus, reason string, epoch uint64) bool {
	f := &tm.groupReplicationFence
	status := fs.Status
	first, ok := f.decide(epoch, reason, func() bool {
		return f.bootstraps.Load() > 0 || tm.tmState.servingPaused()
	})
	if !ok {
		return false
	}
	changed := first
	if !fs.SuperReadOnly {
		changed = true
		log.Error("Group replication: fencing MySQL with super_read_only, the writable primary of a group in which it must not take writes",
			slog.String("reason", reason),
			slog.String("group", status.GetGroupName()),
			slog.String("view_id", status.GetViewId()),
			slog.Int("online_members", mysql.OnlineGroupMembers(status)),
			slog.Bool("already_fenced", !first))
		if err := tm.setFenceSuperReadOnly(ctx); err != nil {
			log.Error("Group replication: failed to fence MySQL with super_read_only, retrying on the next check", slog.Any("error", err))
		} else {
			log.Info("Group replication: MySQL is fenced with super_read_only", slog.String("view_id", status.GetViewId()))
		}
	}
	f.unlock()
	if current, _ := tm.tmState.GroupReplicationNotServingState(); tm.Tablet().Type == topodatapb.TabletType_PRIMARY &&
		(current != reason || tm.QueryServiceControl.IsServing()) {
		changed = true
		if err := tm.tmState.SetGroupReplicationNotServing(ctx, reason); err != nil {
			log.Error("Group replication: failed to stop serving after fencing MySQL", slog.Any("error", err))
		}
	}
	return changed
}

// setFenceSuperReadOnly sets super_read_only, within groupReplicationFenceTimeout: a write statement
// that is running holds it back for up to groupReplicationFenceLockWaitTimeout.
func (tm *TabletManager) setFenceSuperReadOnly(ctx context.Context) error {
	fenceCtx, cancel := context.WithTimeout(ctx, groupReplicationFenceTimeout)
	defer cancel()
	_, err := tm.MysqlDaemon.SetSuperReadOnly(fenceCtx, true, mysqlctl.WithLockWaitTimeout(groupReplicationFenceLockWaitTimeout))
	return err
}

// liftGroupReplicationFenceLocked makes MySQL writable again if it is fenced, after the caller
// decided, under the action lock and on MySQL's status read under it, that the tablet may serve as
// PRIMARY. snap is what groupReplicationFence.snapshot returned before the caller read that status:
// a fence decided since stands, MySQL stays (or is again) fenced, and it returns false. It returns
// true if MySQL is not fenced anymore.
func (tm *TabletManager) liftGroupReplicationFenceLocked(ctx context.Context, snap uint64) bool {
	f := &tm.groupReplicationFence
	if f.decidedSince(snap) {
		return false
	}
	if !f.fenced.Load() {
		return true
	}
	if err := tm.setGroupPrimaryWritable(ctx); err != nil {
		log.Warn("Group replication: cannot lift the fence of MySQL, the primary does not serve yet", slog.Any("error", err))
		return false
	}
	if !tm.settleGroupReplicationFenceLocked(ctx, snap) {
		return false
	}
	log.Info("Group replication: lifted the fence of MySQL, the primary may serve")
	return true
}

// settleGroupReplicationFenceLocked records, after the caller made MySQL writable on a decision that
// the tablet may serve as PRIMARY, taken after groupReplicationFence.snapshot returned snap, that
// MySQL is not fenced. If a fence was decided since, on a status that may be newer than the
// caller's, it fences MySQL again instead, makes a PRIMARY tablet stop serving for the fence's
// reason, and returns false.
func (tm *TabletManager) settleGroupReplicationFenceLocked(ctx context.Context, snap uint64) bool {
	f := &tm.groupReplicationFence
	if f.settle(snap) {
		return true
	}
	log.Warn("Group replication: MySQL was fenced while the tablet decided to serve, fencing it again")
	if err := tm.setFenceSuperReadOnly(ctx); err != nil {
		log.Error("Group replication: failed to fence MySQL again", slog.Any("error", err))
	}
	if tm.Tablet().Type == topodatapb.TabletType_PRIMARY {
		if err := tm.tmState.SetGroupReplicationNotServing(ctx, f.lastReason()); err != nil {
			log.Error("Group replication: failed to stop serving after fencing MySQL", slog.Any("error", err))
		}
	}
	return false
}

// adoptableByIntent returns whether a group of the given incarnation, of which the tablet self's
// MySQL is the primary, is the group that the shard's live bootstrap intent asked self to
// bootstrap, as VTOrc checks before it adopts it (see reparentutil.AdoptGroupReplicationBootstrap):
// the intent applies to the recorded incarnation, names self, is younger than
// reparentutil.GroupReplicationBootstrapIntentFence, and the incarnation is new and, by the time
// MySQL encodes in it, not older than the intent. A bootstrap whose reply was lost, and a join of
// the target that ended in a group of its own, look the same: VTOrc adopts either.
func (rec *shardGroupRecord) adoptableByIntent(self *topodatapb.TabletAlias, incarnation string, now time.Time) bool {
	if rec == nil || rec.intent == nil || incarnation == "" {
		return false
	}
	intent := rec.intent
	if !topoproto.TabletAliasEqual(intent.GetTarget(), self) || intent.GetPreviousIncarnation() != rec.incarnation ||
		incarnation == intent.GetPreviousIncarnation() {
		return false
	}
	started := protoutil.TimeFromProto(intent.GetTime())
	if now.Sub(started) >= reparentutil.GroupReplicationBootstrapIntentFence {
		return false
	}
	if created, ok := policy.GroupIncarnationTime(incarnation); ok && created.Before(started.Add(-reparentutil.GroupReplicationBootstrapIntentClockSkew)) {
		return false
	}
	return true
}

// noteShardGroupFields updates the shard's group record that the tablet read last with the shard
// record si, read for another purpose (the durability policy, the voters): the fence check decides
// on the freshest record without reading the topology itself. A bootstrap reads the shard record
// right after VTOrc recorded its intent, for example. The tablet records are kept.
func (tm *TabletManager) noteShardGroupFields(si *topodatapb.Shard) {
	last := tm.groupReplicationTopo.lastRecord()
	if last == nil || si == nil {
		return
	}
	rec := *last
	rec.incarnation = si.GetGroupReplicationIncarnation()
	rec.voters = si.GetGroupReplicationVoters()
	rec.primaryAlias = si.GetPrimaryAlias()
	rec.durabilityPolicy = si.GetDurabilityPolicy()
	rec.intent = reparentutil.CurrentGroupReplicationBootstrapIntent(si)
	tm.groupReplicationTopo.setRecord(&rec)
}
