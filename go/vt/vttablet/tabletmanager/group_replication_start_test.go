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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// blockedStart starts, in the background, a START GROUP_REPLICATION on fmd that runs until the
// returned function is called, like a START that finds no group: MySQL keeps running it after its
// client gave up, for about a minute (sro-eval case A). It returns once the START runs.
func blockedStart(t *testing.T, fmd *mysqlctl.FakeMysqlDaemon) (release func()) {
	t.Helper()
	running := make(chan struct{})
	unblock := make(chan struct{})
	fmd.StartGroupReplicationFunc = func(bootstrap bool) error {
		close(running)
		<-unblock
		return errors.New("The server is not configured properly to be an active member of the group")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = fmd.StartGroupReplication(context.Background(), false)
	}()
	<-running
	var released bool
	release = func() {
		if released {
			return
		}
		released = true
		close(unblock)
		<-done
		fmd.StartGroupReplicationFunc = nil
	}
	t.Cleanup(release)
	return release
}

// TestIsGroupReplicationCommandOngoing checks which MySQL errors mean that a START or STOP
// GROUP_REPLICATION is in progress: errno 3724, and errno 3663 only when MySQL says that another
// START or STOP runs, since 3663 is MySQL's generic failure of these commands.
func TestIsGroupReplicationCommandOngoing(t *testing.T) {
	running := sqlerror.NewSQLErrorf(3663, "HY000", "The STOP GROUP_REPLICATION command encountered a failure. %s.", mysql.GroupReplicationCommandRunningMessage)
	assert.True(t, isGroupReplicationCommandOngoing(running))
	assert.True(t, isGroupReplicationCommandOngoing(vterrors.Wrapf(running, "failed to stop group replication")),
		"vterrors does not unwrap: the message counts")
	assert.True(t, isGroupReplicationCommandOngoing(sqlerror.NewSQLError(3724, "HY000", "This option cannot be set while START or STOP GROUP_REPLICATION is ongoing.")))
	assert.False(t, isGroupReplicationCommandOngoing(sqlerror.NewSQLError(3663, "HY000",
		"The START GROUP_REPLICATION command encountered a failure. The server is not configured properly to be an active member of the group.")))
	assert.False(t, isGroupReplicationCommandOngoing(errors.New("no seed reachable")))
	assert.False(t, isGroupReplicationCommandOngoing(nil))
}

// TestBootstrapWaitsForStartInProgress checks that a bootstrap on a member whose START
// GROUP_REPLICATION is still in progress waits for MySQL to accept the STOP of that START, which MySQL
// refuses with errno 3663 until the START ends, and then bootstraps. Before, the tablet did not
// recognize 3663 and failed at once, for as long as the START ran.
func TestBootstrapWaitsForStartInProgress(t *testing.T) {
	withGroupReplication(t)
	oldTimeout, oldRetry := groupReplicationStopOngoingStartTimeout, groupReplicationStopOngoingStartRetry
	t.Cleanup(func() {
		groupReplicationStopOngoingStartTimeout, groupReplicationStopOngoingStartRetry = oldTimeout, oldRetry
	})
	groupReplicationStopOngoingStartTimeout = 30 * time.Second
	groupReplicationStopOngoingStartRetry = 10 * time.Millisecond

	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	require.NoError(t, fmd.ConfigureGroupReplication(t.Context(), mysql.GroupReplicationConfig{GroupName: policy.GroupName("ks", "0")}))
	release := blockedStart(t, fmd)
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}

	// MySQL ends the START a moment after the tablet's first STOP was refused.
	stops := func() int {
		_, stop, _ := fmd.GroupReplicationCalls()
		return stop
	}
	go func() {
		for stops() < 2 {
			time.Sleep(time.Millisecond)
		}
		release()
	}()
	status, err := tm.StartGroupReplication(t.Context(), true)
	require.NoError(t, err)
	assert.True(t, mysql.IsGroupPrimary(status))
	assert.True(t, fmd.GroupReplicationBootstrapped)
	assert.GreaterOrEqual(t, stops(), 2, "MySQL refused the STOP while the START ran")
}

// TestBootstrapGivesUpOnStartInProgress checks that the wait for a START in progress is bounded: the
// tablet holds its action lock meanwhile, and the START can run for about a minute. It fails with
// UNAVAILABLE, without having set anything for a bootstrap.
func TestBootstrapGivesUpOnStartInProgress(t *testing.T) {
	withGroupReplication(t)
	oldTimeout, oldRetry := groupReplicationStopOngoingStartTimeout, groupReplicationStopOngoingStartRetry
	t.Cleanup(func() {
		groupReplicationStopOngoingStartTimeout, groupReplicationStopOngoingStartRetry = oldTimeout, oldRetry
	})
	groupReplicationStopOngoingStartTimeout = 200 * time.Millisecond
	groupReplicationStopOngoingStartRetry = 50 * time.Millisecond

	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	require.NoError(t, fmd.ConfigureGroupReplication(t.Context(), mysql.GroupReplicationConfig{GroupName: policy.GroupName("ks", "0")}))
	blockedStart(t, fmd)

	started := time.Now()
	_, err := tm.StartGroupReplication(t.Context(), true)
	requireCode(t, err, vtrpcpb.Code_UNAVAILABLE)
	assert.Less(t, time.Since(started), 10*time.Second)
	_, stop, _ := fmd.GroupReplicationCalls()
	assert.GreaterOrEqual(t, stop, 2, "the tablet tried the STOP again before it gave up")
	assert.LessOrEqual(t, stop, 6, "the tablet does not spin")
	assert.False(t, fmd.GroupReplicationBootstrapped)
}

// TestStartGroupReplicationRefusesJoinOnPrimary checks that a join of MySQL into its group is refused
// on a PRIMARY tablet under a group replication policy that lists voters: the tablet demotes itself
// first, and a join would hold its action lock for up to a minute meanwhile. A REPLICA joins.
func TestStartGroupReplicationRefusesJoinOnPrimary(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplication)
	setGroupReplicationVoters(t, ts, 1, 2, 3)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	require.NoError(t, fmd.ConfigureGroupReplication(t.Context(), mysql.GroupReplicationConfig{GroupName: policy.GroupName("ks", "0")}))
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	startBefore, _, _ := fmd.GroupReplicationCalls()

	_, err := tm.StartGroupReplication(t.Context(), false)
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, startBefore, start, "no START GROUP_REPLICATION on a PRIMARY tablet")

	fmd.StartGroupReplicationError = nil
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	setTabletType(t, tm, topodatapb.TabletType_REPLICA)
	status, err := tm.StartGroupReplication(t.Context(), false)
	require.NoError(t, err)
	assert.Equal(t, mysql.GroupMemberStateOnline, status.MemberState)
}

// TestFullStatusReportsStartInProgress checks that FullStatus reports a START GROUP_REPLICATION in
// progress, from MySQL's processlist (the fake reports a running StartGroupReplicationFunc) or from
// the tablet's own joins and bootstraps, which MySQL does not list before the START is sent.
func TestFullStatusReportsStartInProgress(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplication)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.FullStatusData = &replicationdatapb.FullStatus{}
	})

	status, err := tm.FullStatus(t.Context())
	require.NoError(t, err)
	assert.False(t, status.GetGroupReplicationStatus().GetStartInProgress())

	end := tm.groupReplicationFence.beginStart(false)
	status, err = tm.FullStatus(t.Context())
	require.NoError(t, err)
	assert.True(t, status.GetGroupReplicationStatus().GetStartInProgress(), "the tablet's own join is in progress")
	end(false)

	status, err = tm.FullStatus(t.Context())
	require.NoError(t, err)
	assert.False(t, status.GetGroupReplicationStatus().GetStartInProgress())

	require.NoError(t, fmd.ConfigureGroupReplication(t.Context(), mysql.GroupReplicationConfig{GroupName: policy.GroupName("ks", "0")}))
	blockedStart(t, fmd)
	gr, err := tm.groupReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.True(t, gr.GetStartInProgress(), "MySQL runs a START")
}

// TestGroupReplicationSyncDoesNotRejoinWhileStartInProgress checks that the sync loop does not start a
// join while MySQL still runs a START GROUP_REPLICATION, for example one of an earlier join whose
// client gave up: MySQL refuses the new one, and the configuration that precedes it, until it ends.
func TestGroupReplicationSyncDoesNotRejoinWhileStartInProgress(t *testing.T) {
	withGroupReplication(t)
	ts := newGroupReplicationTopo(t, policy.DurabilityGroupReplication)
	setGroupReplicationVoters(t, ts, 1, 2)
	addPeerTablets(t, ts, 2)
	tm, fmd := newGroupReplicationTestTMWithPeers(t, ts, 1, activeGroupPeers(2), func(fmd *mysqlctl.FakeMysqlDaemon) {
		fmd.StartGroupReplicationError = errors.New("no seed reachable")
	})
	status := groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOffline, ""))
	status.StartInProgress = true
	fmd.SetGroupReplicationStatus(status)
	s := newGroupReplicationSync(tm)
	startBefore, _, _ := fmd.GroupReplicationCalls()

	s.reconcile(t.Context())
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, startBefore, start, "no join while a START runs")

	status.StartInProgress = false
	fmd.SetGroupReplicationStatus(status)
	s.reconcile(t.Context())
	start, _, _ = fmd.GroupReplicationCalls()
	assert.Equal(t, startBefore+1, start, "the loop joins once the START ended")
}
