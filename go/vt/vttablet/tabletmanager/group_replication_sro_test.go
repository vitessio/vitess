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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// The tests below run with the member action mysql_disable_super_read_only_if_primary disabled, as
// Vitess configures every member before it starts Group Replication: MySQL leaves the primary it
// elects super_read_only, and sets super_read_only again when the election ends (sro-eval case B,
// MySQL 8.4.11). Only a decision of the tablet that it may serve makes MySQL writable.

// clearedDuringElection counts the times super_read_only was cleared on fmd while a primary election
// of its MySQL ran: Group Replication undoes such a clear when the election ends.
func clearedDuringElection(t *testing.T, fmd *mysqlctl.FakeMysqlDaemon) *atomic.Int32 {
	t.Helper()
	var cleared atomic.Int32
	fmd.SetSuperReadOnlyHook = func(on bool) {
		if !on && fmd.GroupPrimaryElectionInProgress() {
			cleared.Add(1)
		}
	}
	return &cleared
}

// electionWaited returns a function that reports whether fmd's MySQL status was read at least twice
// while the election ran, since electionWaited was called: the tablet polls it, waiting for the
// election to end.
func electionWaited(fmd *mysqlctl.FakeMysqlDaemon) func() bool {
	var reads atomic.Int32
	fmd.SetGroupReplicationStatusHook(func() {
		if fmd.GroupPrimaryElectionInProgress() {
			reads.Add(1)
		}
	})
	return func() bool { return reads.Load() >= 2 }
}

// heldElection makes fmd's MySQL the primary of view in an election that runs until the returned
// function is called.
func heldElection(t *testing.T, fmd *mysqlctl.FakeMysqlDaemon, view *replicationdatapb.GroupReplicationStatus) (end func()) {
	t.Helper()
	fmd.HoldGroupPrimaryElection = true
	fmd.SuperReadOnly.Store(true)
	fmd.ReadOnly = true
	fmd.ElectGroupPrimary(view)
	t.Cleanup(fmd.EndGroupPrimaryElection)
	return fmd.EndGroupPrimaryElection
}

// TestPromoteReplicaWaitsForElectionEnd checks that PromoteReplica on a group member (PRS, ERS, VTOrc)
// waits until the election that group_replication_set_as_primary started has ended, and only then
// makes MySQL writable: under BEFORE_ON_PRIMARY_FAILOVER the new primary reports PRIMARY before it
// applied its backlog, and Group Replication sets super_read_only when the election ends. Before, the
// tablet waited for Group Replication to clear super_read_only, which it never does once the member
// action is disabled.
func TestPromoteReplicaWaitsForElectionEnd(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.SuperReadOnlyActionDisabled = true
	fmd.HoldGroupPrimaryElection = true
	t.Cleanup(fmd.EndGroupPrimaryElection)
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	fmd.SuperReadOnly.Store(true)
	fmd.ReadOnly = true
	pos, err := replication.ParsePosition(gtidFlavor, gtidPosition)
	require.NoError(t, err)
	fmd.SetPrimaryPositionLocked(pos)
	cleared := clearedDuringElection(t, fmd)

	ctx, cancel := context.WithTimeout(t.Context(), groupReplicationTestTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := tm.PromoteReplica(ctx, false)
		done <- err
	}()
	// The group switched its primary; the election runs on.
	require.Eventually(t, func() bool {
		_, _, setPrimary := fmd.GroupReplicationCalls()
		status, err := fmd.GroupReplicationStatus(ctx)
		return setPrimary == 1 && err == nil && status.GetPrimaryElectionInProgress()
	}, groupReplicationTestTimeout, time.Millisecond)
	assert.True(t, fmd.SuperReadOnly.Load(), "MySQL stays read-only while its election runs")
	fmd.EndGroupPrimaryElection()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		require.FailNow(t, "PromoteReplica did not return after the election ended")
	}
	assert.Zero(t, cleared.Load(), "super_read_only was cleared while the election ran")
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.False(t, fmd.ReadOnly)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.True(t, tm.QueryServiceControl.IsServing())
}

// TestGroupReplicationSyncPromotionWaitsForElectionEnd checks the same for the sync loop, which
// promotes the tablet whose MySQL the group elected after a failover: MySQL is made writable, and the
// tablet serves, once the election has ended and the serving invariant holds.
func TestGroupReplicationSyncPromotionWaitsForElectionEnd(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	cleared := clearedDuringElection(t, fmd)
	endElection := heldElection(t, fmd, primaryView(recordedIncarnation, 2, 3))
	s := newGroupReplicationSync(tm)

	waited := electionWaited(fmd)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.reconcile(ctx)
	}()
	// The run waits for the election to end.
	require.Eventually(t, waited, groupReplicationTestTimeout, time.Millisecond)
	endElection()
	waitClosed(t, done, "the run of the sync loop")

	assert.Zero(t, cleared.Load(), "super_read_only was cleared while the election ran")
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.True(t, tm.QueryServiceControl.IsServing())
	assert.False(t, fmd.SuperReadOnly.Load(), "the promotion makes MySQL writable once the election ended")
}

// TestGroupReplicationSyncMakesMySQLWritableOnlyUnderServingInvariant checks that a PRIMARY tablet
// whose MySQL Group Replication left read-only becomes writable, and serves, only once the serving
// invariant holds: before, the sync loop's decision to serve again only lifted the fence check's
// fence, and relied on Group Replication for the rest.
func TestGroupReplicationSyncMakesMySQLWritableOnlyUnderServingInvariant(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	fmd.SuperReadOnlyActionDisabled = true
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	// The group elected MySQL in a view without the voter majority: it is read-only.
	fmd.ElectGroupPrimary(primaryView(recordedIncarnation))
	require.True(t, fmd.SuperReadOnly.Load())
	s := newGroupReplicationSync(tm)

	s.reconcile(ctx)
	assert.False(t, qsc.IsServing())
	assert.True(t, fmd.SuperReadOnly.Load(), "a primary without the voter majority stays read-only")

	// A second voter joins the view: the tablet decides to serve, and makes MySQL writable first.
	fmd.SetGroupReplicationStatus(primaryView(recordedIncarnation, 2))
	s.reconcile(ctx)
	assert.True(t, qsc.IsServing())
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.False(t, fmd.ReadOnly)
}

// TestGroupReplicationSyncMakesServingPrimaryWritableAgain checks that a serving PRIMARY whose MySQL
// became read-only again, as when the end of its election set super_read_only after the decision to
// serve, is made writable again by the sync loop, but not after DemotePrimary: only the caller of
// DemotePrimary decides whether that primary serves, and takes writes, again.
func TestGroupReplicationSyncMakesServingPrimaryWritableAgain(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	fmd.SuperReadOnlyActionDisabled = true
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SetGroupReplicationStatus(primaryView(recordedIncarnation, 2, 3))
	s := newGroupReplicationSync(tm)
	s.reconcile(ctx)
	require.True(t, qsc.IsServing())
	require.False(t, fmd.SuperReadOnly.Load())

	fmd.SuperReadOnly.Store(true)
	fmd.ReadOnly = true
	s.reconcile(ctx)
	assert.False(t, fmd.SuperReadOnly.Load(), "the serving primary's MySQL takes writes again")
	assert.True(t, qsc.IsServing())

	// A planned reparent demotes the primary, which had a not-serving reason a moment before.
	require.NoError(t, tm.tmState.SetGroupReplicationNotServing(ctx, groupReplicationVoterMajorityLost))
	_, err := tm.DemotePrimary(ctx, false)
	require.NoError(t, err)
	require.True(t, fmd.SuperReadOnly.Load())
	s.reconcile(ctx)
	assert.True(t, fmd.SuperReadOnly.Load(), "a demoted primary stays read-only")
	assert.False(t, qsc.IsServing(), "a demoted primary does not serve")

	require.NoError(t, tm.UndoDemotePrimary(ctx, false))
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.True(t, qsc.IsServing())
}

// TestUndoDemotePrimaryWaitsForElectionEnd checks that UndoDemotePrimary, VTOrc's repair of a
// read-only primary, decides only once the election of its MySQL has ended, and makes MySQL writable
// only if the serving invariant holds.
func TestUndoDemotePrimaryWaitsForElectionEnd(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	cleared := clearedDuringElection(t, fmd)

	// Without the voter majority, it is refused, and MySQL stays read-only.
	fmd.SuperReadOnlyActionDisabled = true
	fmd.ElectGroupPrimary(primaryView(recordedIncarnation))
	requireCode(t, tm.UndoDemotePrimary(ctx, false), vtrpcpb.Code_FAILED_PRECONDITION)
	assert.True(t, fmd.SuperReadOnly.Load())

	endElection := heldElection(t, fmd, primaryView(recordedIncarnation, 2, 3))
	waited := electionWaited(fmd)
	done := make(chan error, 1)
	go func() { done <- tm.UndoDemotePrimary(ctx, false) }()
	require.Eventually(t, waited, groupReplicationTestTimeout, time.Millisecond)
	endElection()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(groupReplicationTestTimeout):
		require.FailNow(t, "UndoDemotePrimary did not return after the election ended")
	}
	assert.Zero(t, cleared.Load(), "super_read_only was cleared while the election ran")
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.True(t, tm.QueryServiceControl.IsServing())
}

// TestStrayGroupStaysReadOnlyWithoutFenceCheck reproduces the stray group of sro-eval case B: a voter's
// join did not find its group, and MySQL completed it as the only member, and the primary, of a new
// incarnation. Group Replication made such a primary writable, and it took writes until the fence
// check fenced it. The join's configuration disabled the member action first, so MySQL stays
// super_read_only, without any fence check.
func TestStrayGroupStaysReadOnlyWithoutFenceCheck(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	fmd.SuperReadOnly.Store(true)
	fmd.ReadOnly = true
	fmd.StartGroupReplicationError = nil
	require.False(t, fmd.SuperReadOnlyActionDisabled, "MySQL's default")
	var writable atomic.Bool
	fmd.SetSuperReadOnlyHook = func(on bool) {
		if !on {
			writable.Store(true)
		}
	}
	fmd.StartGroupReplicationFunc = func(bootstrap bool) error {
		fmd.ElectGroupPrimary(withViewID(groupStatus(testServerUUID(1),
			groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), strayIncarnation+":1"))
		return nil
	}
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}

	_, err := tm.StartGroupReplication(ctx, false)
	require.NoError(t, err)
	status := tmStatus(t, fmd)
	require.True(t, mysql.IsGroupPrimary(status), "MySQL is the primary of a group of its own")
	assert.Equal(t, []bool{false}, fmd.SuperReadOnlyActionEnabledAtStart, "the action was disabled before the START")
	assert.True(t, fmd.SuperReadOnly.Load(), "the primary of a stray group stays read-only")

	// The sync loop leaves that group; MySQL never took writes.
	newGroupReplicationSync(tm).reconcile(ctx)
	assert.Equal(t, mysql.GroupMemberStateOffline, tmStatus(t, fmd).MemberState)
	assert.True(t, fmd.SuperReadOnly.Load())
	assert.False(t, writable.Load(), "nothing made MySQL writable")
}

// TestBootstrapOnPrimaryStaysReadOnlyUntilRecorded checks that a bootstrap of the shard's group on a
// PRIMARY tablet under a group replication policy that lists voters, VTOrc's GroupNotBootstrapped,
// leaves MySQL read-only: the new group has one of the three voters, and its incarnation is not
// recorded yet. Before, the tablet made the bootstrapped primary writable at once.
func TestBootstrapOnPrimaryStaysReadOnlyUntilRecorded(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	// The tablet knows the server_uuids of the other voters.
	tm, fmd, ts := newFenceTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SuperReadOnly.Store(true)
	fmd.ReadOnly = true
	fmd.StartGroupReplicationError = nil
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}

	status, err := tm.StartGroupReplication(ctx, true)
	require.NoError(t, err)
	require.True(t, mysql.IsGroupPrimary(status))
	assert.True(t, fmd.SuperReadOnly.Load(), "the primary of a group of one voter stays read-only")
	assert.False(t, tm.QueryServiceControl.IsServing())

	// VTOrc records the incarnation, and a second voter joins: the tablet serves, writable.
	incarnation := policy.GroupIncarnation(status.GetViewId())
	setGroupReplicationIncarnation(t, ts, incarnation)
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), incarnation+":2"))
	newGroupReplicationSync(tm).reconcile(ctx)
	assert.True(t, tm.QueryServiceControl.IsServing())
	assert.False(t, fmd.SuperReadOnly.Load())
}

// TestPauseMakesBootstrappedPrimaryWritable checks that a planned pause of the primary, around a
// migration's bootstrap, ends with MySQL writable: once the election ended, the decision that the
// tablet may serve makes it so. Before, the pause waited for Group Replication to clear
// super_read_only, and served a read-only primary after its timeout.
func TestPauseMakesBootstrappedPrimaryWritable(t *testing.T) {
	withGroupReplication(t)
	oldTimeout := groupReplicationPauseResumeTimeout
	t.Cleanup(func() { groupReplicationPauseResumeTimeout = oldTimeout })
	groupReplicationPauseResumeTimeout = 200 * time.Millisecond
	ctx := t.Context()
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	require.True(t, tm.QueryServiceControl.IsServing())

	require.NoError(t, tm.lock(ctx))
	defer tm.unlock()
	pause, err := tm.pauseServingLocked(ctx, groupReplicationBootstrapPause)
	require.NoError(t, err)
	require.NotNil(t, pause)
	// MySQL bootstrapped the group, and Group Replication left it read-only.
	fmd.SuperReadOnlyActionDisabled = true
	fmd.ElectGroupPrimary(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)))
	require.True(t, fmd.SuperReadOnly.Load())

	pause.resume(ctx)
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.False(t, fmd.ReadOnly)
	assert.True(t, tm.QueryServiceControl.IsServing())
}

// TestGroupReplicationSyncDisablesSuperReadOnlyActionOnline checks that the serving, writable primary
// of a group bootstrapped before Vitess disabled the member action disables it in the group's
// configuration, once: the members in the group take the change, and the others when they join. A
// primary that does not serve leaves it alone.
func TestGroupReplicationSyncDisablesSuperReadOnlyActionOnline(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	require.False(t, fmd.SuperReadOnlyActionDisabled, "a group bootstrapped before the change")
	s := newGroupReplicationSync(tm)

	// Without the voter majority, the primary does not serve, and does not change the group.
	fmd.SetGroupReplicationStatus(primaryView(recordedIncarnation))
	s.reconcile(ctx)
	require.False(t, tm.QueryServiceControl.IsServing())
	assert.Zero(t, fmd.SuperReadOnlyActionOnlineDisables)

	fmd.SetGroupReplicationStatus(primaryView(recordedIncarnation, 2, 3))
	s.reconcile(ctx)
	require.True(t, tm.QueryServiceControl.IsServing())
	assert.Equal(t, 1, fmd.SuperReadOnlyActionOnlineDisables)
	assert.True(t, fmd.SuperReadOnlyActionDisabled)

	s.reconcile(ctx)
	assert.Equal(t, 1, fmd.SuperReadOnlyActionOnlineDisables, "once per group incarnation")
}

// TestSetReadWriteRequiresServingInvariant checks that SetReadOnly(false), which PRS uses to recover
// a partial promotion, makes MySQL writable under a group replication policy that lists voters only
// while the serving invariant holds.
func TestSetReadWriteRequiresServingInvariant(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SuperReadOnlyActionDisabled = true
	fmd.ElectGroupPrimary(primaryView(recordedIncarnation))
	require.True(t, fmd.SuperReadOnly.Load())

	requireCode(t, tm.SetReadOnly(ctx, false), vtrpcpb.Code_FAILED_PRECONDITION)
	assert.True(t, fmd.SuperReadOnly.Load())

	fmd.SetGroupReplicationStatus(primaryView(recordedIncarnation, 2))
	require.NoError(t, tm.SetReadOnly(ctx, false))
	assert.False(t, fmd.SuperReadOnly.Load())
}

// TestPromotionDoesNotServeWhileElectionRuns checks that a promotion whose wait for the end of the
// election times out makes the tablet a PRIMARY that does not serve, with MySQL read-only: MySQL is
// made writable, and the tablet serves, once the election has ended.
func TestPromotionDoesNotServeWhileElectionRuns(t *testing.T) {
	withGroupReplication(t)
	oldWait := groupReplicationElectionWaitTimeout
	t.Cleanup(func() { groupReplicationElectionWaitTimeout = oldWait })
	groupReplicationElectionWaitTimeout = 20 * time.Millisecond
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	cleared := clearedDuringElection(t, fmd)
	endElection := heldElection(t, fmd, primaryView(recordedIncarnation, 2, 3))
	s := newGroupReplicationSync(tm)

	s.reconcile(ctx)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.False(t, tm.QueryServiceControl.IsServing(), "the primary does not serve while its election runs")
	assert.True(t, fmd.SuperReadOnly.Load())
	reason, _ := tm.tmState.GroupReplicationNotServingState()
	assert.Equal(t, groupReplicationElectionInProgress, reason)

	endElection()
	s.reconcile(ctx)
	assert.True(t, tm.QueryServiceControl.IsServing())
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.Zero(t, cleared.Load(), "super_read_only was cleared while the election ran")
}

// TestMakingMySQLWritableIsRefusedDuringElection checks that MySQL is never made writable while the
// election that made it the primary still runs, whatever the policy: Group Replication sets
// super_read_only when it ends.
func TestMakingMySQLWritableIsRefusedDuringElection(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	fmd.SuperReadOnlyActionDisabled = true
	heldElection(t, fmd, groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)))

	requireCode(t, tm.SetReadOnly(t.Context(), false), vtrpcpb.Code_UNAVAILABLE)
	assert.True(t, fmd.SuperReadOnly.Load())
}

// TestServingAgainRedoesPreparedTransactions checks that a tablet that became PRIMARY without serving,
// and so without making MySQL writable and redoing its prepared transactions, does both when it serves
// again.
func TestServingAgainRedoesPreparedTransactions(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	fmd.SuperReadOnlyActionDisabled = true
	fmd.ElectGroupPrimary(primaryView(recordedIncarnation))
	s := newGroupReplicationSync(tm)

	// A promotion, as PromoteReplica's, while the view lacks the voter majority.
	require.NoError(t, tm.lock(ctx))
	err := tm.changeTypeLocked(ctx, topodatapb.TabletType_PRIMARY, DBActionSetReadWrite, SemiSyncActionNone)
	tm.unlock()
	require.NoError(t, err)
	require.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	require.False(t, qsc.IsServing())
	assert.True(t, fmd.SuperReadOnly.Load())
	assert.False(t, qsc.MethodCalled["RedoPreparedTransactions"])

	fmd.SetGroupReplicationStatus(primaryView(recordedIncarnation, 2))
	s.reconcile(ctx)
	assert.True(t, qsc.IsServing())
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.True(t, qsc.MethodCalled["RedoPreparedTransactions"], "the prepared transactions are redone before the primary serves")
}

// TestFenceCheckWakesSyncLoopForReadOnlyServingPrimary checks that the fence check, which reads
// super_read_only every groupReplicationFenceCheckInterval on a PRIMARY tablet, wakes the sync loop
// when the tablet serves while MySQL is read-only, so that it is made writable within about that
// interval rather than the sync loop's.
func TestFenceCheckWakesSyncLoopForReadOnlyServingPrimary(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _ := newFenceTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SetGroupReplicationStatus(primaryView(recordedIncarnation, 2, 3))
	s := newGroupReplicationSync(tm)
	s.reconcile(ctx)
	require.True(t, tm.QueryServiceControl.IsServing())

	s.checkFence(ctx)
	require.Empty(t, s.wake, "nothing to do for a writable primary")

	fmd.SuperReadOnly.Store(true)
	s.checkFence(ctx)
	assert.Len(t, s.wake, 1, "the sync loop runs right away")
	assert.False(t, tm.groupReplicationFence.fenced.Load(), "this is not a fence")
}
