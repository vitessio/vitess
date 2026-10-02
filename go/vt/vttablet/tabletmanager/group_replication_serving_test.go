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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// groupReplicationTestTimeout bounds the waits of the tests below.
const groupReplicationTestTimeout = 30 * time.Second

// twoVotersView returns the status of cell1-1 as the primary of a view of the recorded incarnation
// 1780000001 that holds two of the three voters.
func twoVotersView() *replicationdatapb.GroupReplicationStatus {
	return withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:7")
}

// errorState returns the status of cell1-1 after its group lost its majority and MySQL left it.
func errorState() *replicationdatapb.GroupReplicationStatus {
	return &replicationdatapb.GroupReplicationStatus{
		PluginActive: true, GroupName: policy.GroupName("ks", "0"), MemberState: mysql.GroupMemberStateError,
	}
}

// startStuckSyncRun starts a run of the sync loop that reads MySQL's status, then waits for the
// topology, which is cut off: the run's caches have expired. It returns once the run has read the
// status, and the returned channel is closed when the run ends. The run cannot get past its first
// read of the topology before the partition heals.
func startStuckSyncRun(t *testing.T, s *groupReplicationSync, fmd *mysqlctl.FakeMysqlDaemon, f *cutOffTopoFactory) <-chan struct{} {
	t.Helper()
	s.recordRead = time.Time{}
	s.durabilityRead = time.Time{}
	f.cut()
	read := make(chan struct{})
	var once sync.Once
	fmd.SetGroupReplicationStatusHook(func() { once.Do(func() { close(read) }) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		stepCtx, cancel := context.WithTimeout(t.Context(), topo.RemoteOperationTimeout)
		defer cancel()
		s.reconcile(stepCtx)
	}()
	waitClosed(t, read, "the run's read of MySQL's status")
	fmd.SetGroupReplicationStatusHook(nil)
	return done
}

// waitClosed waits until done is closed.
func waitClosed(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(groupReplicationTestTimeout):
		require.FailNow(t, what+" did not end")
	}
}

// newServingTestTM starts tablet cell1-1, PRIMARY and serving, as the primary of a view of the
// recorded incarnation that holds two of the three voters, on a topology that the test can cut off.
// A run of the sync loop has read the topology, as it does every few seconds.
func newServingTestTM(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon, *cutOffTopoFactory, *topo.Server, *groupReplicationSync) {
	t.Helper()
	tm, fmd, f, ts := newCutOffTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SetGroupReplicationStatus(twoVotersView())
	s := newGroupReplicationSync(tm)
	s.reconcile(t.Context())
	require.True(t, tm.QueryServiceControl.IsServing())
	// VTOrc may bootstrap the group on this tablet.
	fmd.StartGroupReplicationError = nil
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	return tm, fmd, f, ts, s
}

// TestGroupReplicationSyncDoesNotServeOnStatusReadBeforeBootstrap reproduces run r2 of the S7d chaos
// scenario (doc/failover-audit/GroupReplication.md, "r2: the sync loop acted on a stale MySQL
// status"). A run of the old primary's sync loop read MySQL's status while MySQL was the ONLINE
// primary of two voters, then waited for the topology of its cut-off cell. Meanwhile the group lost
// its majority, and VTOrc bootstrapped it on this tablet: the bootstrap stopped serving and made
// MySQL the writable primary of a group of one. After the heal, the run made the tablet serve again
// on the status it had read before the bootstrap, and writes were acknowledged by a single voter.
func TestGroupReplicationSyncDoesNotServeOnStatusReadBeforeBootstrap(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, f, ts, s := newServingTestTM(t)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)

	done := startStuckSyncRun(t, s, fmd, f)

	// The group loses its majority, and VTOrc bootstraps it on this tablet, which reaches it
	// although the tablet's own topology does not answer.
	fmd.SetGroupReplicationStatus(errorState())
	rpcCtx, cancel := context.WithTimeout(ctx, groupReplicationTestTimeout)
	defer cancel()
	status, err := tm.StartGroupReplication(rpcCtx, true)
	require.NoError(t, err)
	require.True(t, mysql.IsGroupPrimary(status))
	require.False(t, qsc.IsServing(), "the bootstrap stops serving first")

	// The partition heals, and the run ends with the status it read before the bootstrap.
	f.heal()
	waitClosed(t, done, "the run of the sync loop")
	assert.False(t, qsc.IsServing(), "a primary that is the only voter of its group must not serve")
	s.reconcile(ctx)
	assert.False(t, qsc.IsServing(), "a primary that is the only voter of its group must not serve")

	// VTOrc records the new incarnation, and a second voter joins the group: the tablet serves.
	incarnation := policy.GroupIncarnation(status.GetViewId())
	setGroupReplicationIncarnation(t, ts, incarnation)
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), incarnation+":2"))
	s.reconcile(ctx)
	assert.True(t, qsc.IsServing(), "the primary serves once a majority of the voters is in the recorded group")
}

// TestGroupReplicationSyncDoesNotServeDuringBootstrap checks the same race while VTOrc's bootstrap is
// still running on the tablet, which is still PRIMARY: the stuck run of the sync loop must not make
// the tablet serve on the status it read before the bootstrap started, or the tablet serves as soon
// as the bootstrap makes MySQL the writable primary of a group of one.
func TestGroupReplicationSyncDoesNotServeDuringBootstrap(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, f, _, s := newServingTestTM(t)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)

	done := startStuckSyncRun(t, s, fmd, f)

	// VTOrc's bootstrap stops serving, and runs MySQL's START GROUP_REPLICATION, which takes a while.
	fmd.SetGroupReplicationStatus(errorState())
	inStart := make(chan struct{})
	release := make(chan struct{})
	fmd.StartGroupReplicationHook = func(bootstrap bool) {
		close(inStart)
		<-release
	}
	bootstrapDone := make(chan struct{})
	var bootstrapErr error
	go func() {
		defer close(bootstrapDone)
		rpcCtx, cancel := context.WithTimeout(ctx, groupReplicationTestTimeout)
		defer cancel()
		_, bootstrapErr = tm.StartGroupReplication(rpcCtx, true)
	}()
	waitClosed(t, inStart, "the bootstrap's way to START GROUP_REPLICATION")
	require.False(t, qsc.IsServing(), "the bootstrap stops serving first")

	// The partition heals while MySQL bootstraps the group: the run ends.
	f.heal()
	waitClosed(t, done, "the run of the sync loop")
	assert.False(t, qsc.IsServing(), "the tablet must not serve while its MySQL bootstraps a group")

	close(release)
	waitClosed(t, bootstrapDone, "the bootstrap")
	require.NoError(t, bootstrapErr)
	assert.False(t, qsc.IsServing(), "a primary that is the only voter of its group must not serve")
	s.reconcile(ctx)
	assert.False(t, qsc.IsServing(), "a primary that is the only voter of its group must not serve")
}

// TestGroupReplicationSyncDoesNotServeBeforeBootstrappedMemberIsPrimary checks the same race when
// the bootstrap RPC gave up while MySQL's START GROUP_REPLICATION still runs, as MySQL keeps
// running a START whose client left: the action lock is free, and MySQL is not a group primary yet.
// The stuck run must not lift the not-serving state that the bootstrap set on the status it read
// before, and the tablet must not serve once MySQL is the primary of a group of one.
func TestGroupReplicationSyncDoesNotServeBeforeBootstrappedMemberIsPrimary(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, f, _, s := newServingTestTM(t)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)

	done := startStuckSyncRun(t, s, fmd, f)

	// The bootstrap RPC stops serving, then gives up waiting for MySQL's START.
	fmd.SetGroupReplicationStatus(errorState())
	fmd.StartGroupReplicationError = errors.New("context deadline exceeded")
	rpcCtx, cancel := context.WithTimeout(ctx, groupReplicationTestTimeout)
	defer cancel()
	_, err := tm.StartGroupReplication(rpcCtx, true)
	require.Error(t, err)
	require.False(t, qsc.IsServing(), "the bootstrap stops serving first")
	// MySQL's START still runs: the member recovers alone.
	fmd.SetGroupReplicationStatus(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateRecovering, "")))

	f.heal()
	waitClosed(t, done, "the run of the sync loop")
	assert.False(t, qsc.IsServing(), "the tablet must not serve before its MySQL is the primary of the shard's group")

	// The START ends with MySQL the primary of a group of one, which the tablet did not see itself
	// bootstrap: it leaves that group.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1790000777:1"))
	s.reconcile(ctx)
	assert.False(t, tm.Tablet().Type == topodatapb.TabletType_PRIMARY && qsc.IsServing(), "a primary that is the only voter of its group must not serve")
}

// TestPromoteReplicaServesOnlyWithVoterMajority checks that a member promoted while its view holds
// fewer than a majority of the voters, for example after the other voters left the group cleanly
// (MySQL's view quorum counts only the members still in the view), becomes PRIMARY without
// serving: PromoteReplica is how PRS, ERS and VTOrc make a tablet the shard primary, on statuses
// they read before. A member whose view holds the majority serves right away, even if the tablet
// does not know the other voters' server_uuids yet, for example right after it started: it asks
// them, as the promotion of the sync loop does.
func TestPromoteReplicaServesOnlyWithVoterMajority(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	tm.groupReplicationPeers.mu.Lock()
	tm.groupReplicationPeers.serverUUIDs = nil
	tm.groupReplicationPeers.mu.Unlock()

	// Voter cell1-1 is the primary of a view with a member that is not a voter.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(9), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:12"))
	_, err := tm.PromoteReplica(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.False(t, qsc.IsServing(), "a primary without the majority of its voters must not serve")

	// A second voter joins: the sync loop makes it serve.
	twoVoters := withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:13")
	fmd.SetGroupReplicationStatus(twoVoters)
	newGroupReplicationSync(tm).reconcile(ctx)
	assert.True(t, qsc.IsServing())

	// Promoted again in a view with the majority while the tablet knows no server_uuid: it serves.
	setTabletType(t, tm, topodatapb.TabletType_REPLICA)
	tm.groupReplicationPeers.mu.Lock()
	tm.groupReplicationPeers.serverUUIDs = nil
	tm.groupReplicationPeers.mu.Unlock()
	_, err = tm.PromoteReplica(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.True(t, qsc.IsServing(), "the voters of the view are identified by asking them")
}

// TestUndoDemotePrimaryRequiresRecordedIncarnation checks that a demotion is not undone on the
// primary of a group that the tablet bootstrapped itself but whose incarnation is not recorded:
// such a group only becomes the shard's group once VTOrc records it.
func TestUndoDemotePrimaryRequiresRecordedIncarnation(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	fmd.SuperReadOnly.Store(true)
	fmd.ReadOnly = true
	tm.groupReplicationPeers.noteBootstrap("1790000999")
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1790000999:3"))

	requireCode(t, tm.UndoDemotePrimary(ctx, false), vtrpcpb.Code_FAILED_PRECONDITION)
	assert.True(t, fmd.SuperReadOnly.Load())
}

// TestStopGroupReplicationOnPrimaryDoesNotServeUnderGroupReplicationPolicy checks that a PRIMARY
// whose MySQL leaves its group under a group replication policy that lists voters does not serve
// afterwards: its MySQL is outside of any group. Only the last step of a migration back to
// asynchronous replication, under the asynchronous policy, serves on its own.
func TestStopGroupReplicationOnPrimaryDoesNotServeUnderGroupReplicationPolicy(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	fmd.SetGroupReplicationStatus(twoVotersView())
	newGroupReplicationSync(tm).reconcile(ctx)
	require.True(t, qsc.IsServing())

	_, err := tm.StopGroupReplication(ctx)
	require.NoError(t, err)
	assert.False(t, qsc.IsServing(), "a primary outside of any group must not serve under a group replication policy")
	assert.True(t, fmd.SuperReadOnly.Load(), "MySQL stays read-only")
}

// TestClearGroupReplicationNotServingRequiresGeneration checks that a decision to serve again made
// on a status read before another not-serving decision does not undo it.
func TestClearGroupReplicationNotServingRequiresGeneration(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, _, _, _ := newLegitimacyTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)

	require.NoError(t, tm.tmState.SetGroupReplicationNotServing(ctx, groupReplicationVoterMajorityLost))
	_, gen := tm.tmState.GroupReplicationNotServingState()
	// Another decision, made on a newer state.
	require.NoError(t, tm.tmState.SetGroupReplicationNotServing(ctx, groupReplicationVoterMajorityLost))
	cleared, err := tm.tmState.ClearGroupReplicationNotServing(ctx, gen)
	require.NoError(t, err)
	assert.False(t, cleared)
	assert.False(t, qsc.IsServing())

	_, gen = tm.tmState.GroupReplicationNotServingState()
	cleared, err = tm.tmState.ClearGroupReplicationNotServing(ctx, gen)
	require.NoError(t, err)
	assert.True(t, cleared)
	assert.True(t, qsc.IsServing())
}
