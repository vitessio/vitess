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
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// recordedIncarnation is the incarnation of the shard's group in the shard record of
// newLegitimacyTestTM.
const recordedIncarnation = "1780000001"

// newFenceTestTM is newLegitimacyTestTM, after the tablet read the shard record and the durability
// policy, as its sync loop does every few seconds, and learned the server_uuids of the other voters.
// MySQL takes writes until something makes it super_read_only.
func newFenceTestTM(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon, *topo.Server) {
	t.Helper()
	ctx := t.Context()
	tm, fmd, _, ts := newLegitimacyTestTM(t)
	_, err := tm.readShardGroupRecord(ctx, nil)
	require.NoError(t, err)
	_, err = tm.shardDurability(ctx)
	require.NoError(t, err)
	for _, uid := range []int{2, 3} {
		tm.groupReplicationPeers.setServerUUID(topoproto.TabletAliasString(&topodatapb.TabletAlias{Cell: "cell1", Uid: uint32(uid)}), testServerUUID(uid))
	}
	fmd.SuperReadOnly.Store(false)
	return tm, fmd, ts
}

// primaryView returns the status of cell1-1 as the ONLINE primary of a view of the given
// incarnation, whose other members are the ONLINE secondaries cell1-<uid>.
func primaryView(incarnation string, uids ...int) *replicationdatapb.GroupReplicationStatus {
	members := []*replicationdatapb.GroupReplicationMember{groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)}
	for _, uid := range uids {
		members = append(members, groupMember(testServerUUID(uid), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary))
	}
	return withViewID(groupStatus(testServerUUID(1), members...), fmt.Sprintf("%s:%d", incarnation, 10+len(uids)))
}

// strayIncarnation is an incarnation that MySQL created on its own, at the time of the S7d chaos
// run that formed a group of one (doc/failover-audit/GroupReplication.md, "A join can form a group
// of one"), long before any intent of the tests.
const strayIncarnation = "17909416894997945"

// incarnationAt returns the incarnation of a group whose first view MySQL installed at t: the view
// id's fixed part is that time in units of 100ns since the Unix epoch.
func incarnationAt(t time.Time) string {
	return strconv.FormatInt(t.UnixNano()/100, 10)
}

// endJoin records that a join of the tablet's MySQL ended a moment ago, as the sync loop's, an RPC's
// or the startup's join does.
func endJoin(tm *TabletManager) {
	tm.groupReplicationFence.beginStart(false)(false)
}

// TestGroupReplicationFenceCheckFencesStrayGroupDuringJoin reproduces the killed-VTOrc variant of the
// S7d chaos scenario (doc/failover-audit/GroupReplication.md, "A join can form a group of one"): a
// voter's join (START GROUP_REPLICATION, not a bootstrap) did not find its group, and MySQL
// completed it as the only member of a new incarnation, ONLINE PRIMARY and writable, while its
// tablet was REPLICA. The sync loop noticed on its next run, after the join, and MySQL's leave took
// 4.7s more: clients writing to MySQL directly could commit for 5.7s. The fence check watches MySQL
// while the join runs, holding the action lock that the sync loop needs, and fences MySQL with
// super_read_only on its own; the leave that follows keeps it fenced.
func TestGroupReplicationFenceCheckFencesStrayGroupDuringJoin(t *testing.T) {
	withGroupReplication(t)
	groupReplicationFenceCheckInterval = 10 * time.Millisecond
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	require.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)

	// VTOrc's GroupMemberNotOnline makes the voter join. MySQL's START does not find the group, and
	// completes as the only member of a new incarnation, before the statement returns.
	formed := make(chan struct{})
	release := make(chan struct{})
	fmd.StartGroupReplicationError = nil
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	fmd.StartGroupReplicationFunc = func(bootstrap bool) error {
		fmd.SetGroupReplicationStatus(primaryView(strayIncarnation))
		fmd.SuperReadOnly.Store(false)
		close(formed)
		<-release
		return nil
	}
	var stopped, readOnlyAtStop atomic.Bool
	fmd.StopGroupReplicationHook = func() {
		readOnlyAtStop.Store(fmd.SuperReadOnly.Load())
		stopped.Store(true)
	}
	joinDone := make(chan struct{})
	var joinErr error
	go func() {
		defer close(joinDone)
		rpcCtx, cancel := context.WithTimeout(ctx, groupReplicationTestTimeout)
		defer cancel()
		_, joinErr = tm.StartGroupReplication(rpcCtx, false)
	}()
	waitClosed(t, formed, "the join's START GROUP_REPLICATION")

	assert.Eventually(t, fmd.SuperReadOnly.Load, groupReplicationTestTimeout, 5*time.Millisecond,
		"the stray group's primary must be fenced while the join still runs")
	if tm.actionSema.TryAcquire(1) {
		tm.actionSema.Release(1)
		require.FailNow(t, "the join must still hold the action lock: the sync loop cannot have fenced MySQL")
	}
	assert.False(t, stopped.Load(), "MySQL is fenced before it leaves the group")
	assert.True(t, tm.groupReplicationFence.fenced.Load())

	close(release)
	waitClosed(t, joinDone, "the join")
	require.NoError(t, joinErr)

	// The sync loop leaves the stray group; MySQL stays fenced, and the tablet REPLICA. The fence
	// woke the tablet's own sync loop, which may get the action lock first.
	s := newGroupReplicationSync(tm)
	require.Eventually(t, func() bool {
		if !stopped.Load() {
			s.reconcile(ctx)
		}
		return tmStatus(t, fmd).MemberState == mysql.GroupMemberStateOffline && !tm.groupReplicationFence.fenced.Load()
	}, groupReplicationTestTimeout, 5*time.Millisecond, "MySQL must leave the stray group, which ends the fence: out of any group, MySQL is Group Replication's to keep read-only")
	assert.True(t, readOnlyAtStop.Load(), "MySQL must be super_read_only before it leaves")
	assert.True(t, fmd.SuperReadOnly.Load())
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
}

// TestGroupReplicationSyncFencesForeignGroupBeforeLeaving checks the sync loop alone: when it finds
// MySQL the primary of a group of another incarnation than the recorded one, it makes MySQL
// super_read_only before MySQL's STOP GROUP_REPLICATION, which took 4.7s in the chaos tests.
func TestGroupReplicationSyncFencesForeignGroupBeforeLeaving(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _ := newFenceTestTM(t)
	fmd.SetGroupReplicationStatus(primaryView(strayIncarnation))
	var stopped, readOnlyAtStop atomic.Bool
	fmd.StopGroupReplicationHook = func() {
		readOnlyAtStop.Store(fmd.SuperReadOnly.Load())
		stopped.Store(true)
	}

	newGroupReplicationSync(tm).reconcile(t.Context())

	require.True(t, stopped.Load(), "MySQL must leave the foreign group")
	assert.True(t, readOnlyAtStop.Load(), "MySQL must be super_read_only before it leaves")
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
}

// TestGroupReplicationFenceCheckSparesLegitimateGroups checks that the fence check only fences a
// group that the tablet must not take writes in: never a member of the shard's recorded group that
// may serve, nor a group in the recorded incarnation that never had a majority of the voters, as
// right after a bootstrap that other voters are about to join (InitPrimary and PlannedReparentShard
// write to it before they do), nor a group that the tablet is bootstrapping or bootstrapped. The
// last case is the stray group, which it fences.
func TestGroupReplicationFenceCheckSparesLegitimateGroups(t *testing.T) {
	newIncarnation := incarnationAt(time.Now())
	testCases := []struct {
		name    string
		primary bool
		status  *replicationdatapb.GroupReplicationStatus
		prepare func(tm *TabletManager)
		fenced  bool
	}{{
		name:   "secondary of the recorded group after a join",
		status: withViewID(groupStatus(testServerUUID(1), groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary), groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), recordedIncarnation+":5"),
	}, {
		name:   "primary of the recorded group with the voter majority, about to be promoted",
		status: primaryView(recordedIncarnation, 2),
	}, {
		name:    "serving primary of the recorded group",
		primary: true,
		status:  primaryView(recordedIncarnation, 2, 3),
	}, {
		name:    "primary of the recorded group that never had the voter majority",
		primary: true,
		status:  primaryView(recordedIncarnation),
	}, {
		name:    "group that the tablet bootstrapped, not recorded yet",
		status:  primaryView(newIncarnation),
		prepare: func(tm *TabletManager) { tm.groupReplicationPeers.noteBootstrap(newIncarnation) },
	}, {
		name:    "group that the tablet is bootstrapping",
		status:  primaryView(newIncarnation),
		prepare: func(tm *TabletManager) { tm.groupReplicationFence.beginStart(true) },
	}, {
		name:   "stray group of one after a join",
		status: primaryView(strayIncarnation),
		fenced: true,
	}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			withGroupReplication(t)
			ctx := t.Context()
			tm, fmd, _ := newFenceTestTM(t)
			if tc.primary {
				setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
			}
			endJoin(tm)
			if tc.prepare != nil {
				tc.prepare(tm)
			}
			fmd.SetGroupReplicationStatus(tc.status)
			s := newGroupReplicationSync(tm)

			s.checkFence(ctx)
			s.checkFence(ctx)

			assert.Equal(t, tc.fenced, fmd.SuperReadOnly.Load(), "super_read_only")
			assert.Equal(t, tc.fenced, tm.groupReplicationFence.fenced.Load(), "fence")
			assert.Positive(t, fmd.GroupReplicationFenceStatusReads())
		})
	}
}

// TestGroupReplicationFenceCheckReadsOnlyWhenArmed checks the fence check's load: on a tablet that
// is not PRIMARY, it reads MySQL only while a join or a bootstrap of its MySQL runs, and for
// groupReplicationJoinWatchWindow after it ended.
func TestGroupReplicationFenceCheckReadsOnlyWhenArmed(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	fmd.SetGroupReplicationStatus(primaryView(strayIncarnation))
	s := newGroupReplicationSync(tm)

	s.checkFence(ctx)
	assert.Zero(t, fmd.GroupReplicationFenceStatusReads(), "a replica without a join does not read MySQL")
	assert.False(t, fmd.SuperReadOnly.Load())

	end := tm.groupReplicationFence.beginStart(false)
	s.checkFence(ctx)
	assert.Equal(t, 1, fmd.GroupReplicationFenceStatusReads(), "the check reads MySQL while a join runs")
	assert.True(t, fmd.SuperReadOnly.Load())

	end(false)
	tm.groupReplicationFence.fenced.Store(false)
	tm.groupReplicationFence.lastStartEnd.Store(time.Now().Add(-groupReplicationJoinWatchWindow).UnixNano())
	s.checkFence(ctx)
	assert.Equal(t, 1, fmd.GroupReplicationFenceStatusReads(), "the check stops reading once the join is long over")
}

// setBootstrapIntent records a bootstrap intent for cell1-<uid> in the shard record, made at the
// given time for the recorded incarnation.
func setBootstrapIntent(t *testing.T, ts *topo.Server, uid uint32, at time.Time) {
	t.Helper()
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationBootstrapIntent = &topodatapb.GroupReplicationBootstrapIntent{
			Target:              &topodatapb.TabletAlias{Cell: "cell1", Uid: uid},
			Time:                protoutil.TimeToProto(at),
			PreviousIncarnation: recordedIncarnation,
			Token:               "test",
		}
		return nil
	})
	require.NoError(t, err)
}

// TestGroupReplicationFenceSparesBootstrapIntentTarget checks the group of the target of a live
// bootstrap intent. VTOrc recorded the intent and bootstrapped the group on this tablet, but the
// RPC failed before the tablet noted the bootstrap (VTOrc was killed, or cut off), and MySQL's
// START completed anyway: MySQL is the only member of a new incarnation that the shard record does
// not list. VTOrc adopts that group (GroupBootstrapNotRecorded). The tablet neither fences it nor
// leaves it while the intent is live: leaving it left VTOrc nothing to adopt. It does not serve it
// before its incarnation is recorded and a majority of the voters joined. A group of another
// tablet's intent, or of an intent that expired, is a stray group: fenced and left. A tablet that
// fenced the group before it read the intent keeps it fenced, but does not leave it, and its
// promotion after the adoption lifts the fence.
func TestGroupReplicationFenceSparesBootstrapIntentTarget(t *testing.T) {
	testCases := []struct {
		name   string
		target uint32
		age    time.Duration
		// unread: the tablet did not read the shard record since VTOrc recorded the intent.
		unread bool
		spared bool
		fenced bool
	}{
		{name: "live intent for this tablet", target: 1, spared: true},
		{name: "live intent for this tablet, read after the fence", target: 1, unread: true, spared: true, fenced: true},
		{name: "live intent for another tablet", target: 2, fenced: true},
		{name: "expired intent for this tablet", target: 1, age: 3 * time.Minute, fenced: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			withGroupReplication(t)
			ctx := t.Context()
			tm, fmd, ts := newFenceTestTM(t)
			intentTime := time.Now().Add(-tc.age)
			setBootstrapIntent(t, ts, tc.target, intentTime)
			if !tc.unread {
				// The bootstrap read the shard record, intent included, before MySQL's START.
				_, err := tm.shardDurability(ctx)
				require.NoError(t, err)
			}
			end := tm.groupReplicationFence.beginStart(true)
			incarnation := incarnationAt(intentTime.Add(time.Second))
			fmd.SetGroupReplicationStatus(primaryView(incarnation))
			end(true)
			s := newGroupReplicationSync(tm)

			s.checkFence(ctx)
			s.reconcile(ctx)

			_, stops, _ := fmd.GroupReplicationCalls()
			assert.Equal(t, tc.fenced, fmd.SuperReadOnly.Load(), "super_read_only")
			assert.Equal(t, !tc.spared, stops == 1, "left the group")
			assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type, "a group whose incarnation is not recorded is not served")
			if !tc.spared {
				return
			}

			// VTOrc adopts the group, and a second voter joins it: the tablet becomes the serving
			// primary, and MySQL takes writes.
			setGroupReplicationIncarnation(t, ts, incarnation)
			fmd.SetGroupReplicationStatus(primaryView(incarnation, 2))
			s.reconcile(ctx)
			assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
			assert.True(t, tm.QueryServiceControl.IsServing())
			assert.False(t, fmd.SuperReadOnly.Load())
			assert.False(t, tm.groupReplicationFence.fenced.Load(), "the promotion lifted the fence")
		})
	}
}

// TestGroupReplicationFenceCheckFencesShrunkPrimary reproduces the G11 chaos scenario
// (doc/failover-audit/GroupReplication.md, "Serving invariant, bootstrap intent and the migration's
// read-only moment"): the other voters of a serving primary's group left it cleanly, and MySQL's view
// of one keeps quorum and commits. The sync loop stopped serving on its next run: 72 writes routed
// by vtgate were acknowledged on a single voter within 0.68s. The fence check, which watches a
// PRIMARY every groupReplicationFenceCheckInterval, fences MySQL and stops serving on its own, with
// the sync loop never running in between. The primary serves again, writable, once a majority of
// TestGroupReplicationFenceCheckFencesShrunkPrimaryAfterItsBootstrap reproduces the G11 chaos
// scenario as the harness runs it: the migration to Group Replication bootstraps the group on the
// primary, records its incarnation, and the other voters join; a few seconds later, within
// groupReplicationBootstrapGrace, they leave cleanly. The tablet trusts a group it bootstrapped
// TestGroupReplicationFenceIsLiftedOnlyByCurrentDecision checks how the fence interacts with the
// serving invariant: a decision to serve that read MySQL's status before the fence check fenced it
// does not make MySQL writable, nor the tablet serve; and a promotion that the invariant refuses
// TestGroupReplicationFenceCheckFollowsMigrationBack checks the migration back to semi-sync, which
// stores the semi-sync policy as the shard's own policy before the secondaries leave the group one
// by one, and the primary keeps serving as its group shrinks below the voter majority. The fence
// check decides on the policy the tablet read last: the first leave, which keeps the majority,
// makes the sync loop read it again, so that the second leave does not fence the primary. A fence
// decided under a policy that was out of date is lifted, and the primary serves again, once the
// TestGroupReplicationFenceCheckLeavesPausedPrimaryAlone checks the planned pause of a primary (see
// pauseServingLocked): the pause makes the primary serve again once MySQL takes writes, so the
// shrinkAndFenceOnce returns a function that, the first time it is called, shrinks the view of the
// primary cell1-1 to itself and runs the fence check, as if the other voters left, and the check ran,
// TestGroupReplicationFenceDecidedDuringUndoDemotePrimaryStands checks VTOrc's PrimaryIsReadOnly
// recovery of a fenced primary, racing with the fence check. UndoDemotePrimary reads MySQL's status
// while a majority of the voters is back, and decides that the primary may serve; the voters leave
// again, and the fence check, which does not wait for the action lock, fences MySQL, before
// TestGroupReplicationFenceDecidedDuringPromotionStands checks a promotion that lifts the fence of
// its MySQL, racing with the fence check: the promotion decided that the tablet may serve, and while
// it makes MySQL writable, the voters leave again and the fence check fences MySQL. The fence was
// TestGroupReplicationFenceDropsDecisionOfEarlierEpoch checks a fence decided on a status of MySQL
// read before MySQL joined, bootstrapped or left a group. The fence check read MySQL as the primary
// of a stray group; before it set super_read_only, the tablet left that group and bootstrapped a new
// one, which Group Replication made writable. Setting super_read_only then would keep the new group
// read-only: the bootstrapping RPC (InitPrimary, VTOrc) waits for a writable primary. The check
// drops the decision.
func TestGroupReplicationFenceDropsDecisionOfEarlierEpoch(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	endJoin(tm)
	fmd.SetGroupReplicationStatus(primaryView(strayIncarnation))
	newIncarnation := incarnationAt(time.Now())
	fmd.GroupReplicationFenceStatusHook = func() {
		fmd.GroupReplicationFenceStatusHook = nil
		// The tablet leaves the stray group, and bootstraps a new one, all under the action lock.
		tm.groupReplicationFence.reset()
		end := tm.groupReplicationFence.beginStart(true)
		fmd.SetGroupReplicationStatus(primaryView(newIncarnation))
		tm.groupReplicationPeers.noteBootstrap(newIncarnation)
		end(false)
	}

	newGroupReplicationSync(tm).checkFence(ctx)

	assert.False(t, fmd.SuperReadOnly.Load(), "the bootstrapped group must stay writable")
	assert.False(t, tm.groupReplicationFence.fenced.Load())
}
