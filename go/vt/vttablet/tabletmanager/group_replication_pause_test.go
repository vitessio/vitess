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
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// mysqlStartEvent and mysqlStopEvent are the events that the tests below record when the fake
// MySQL starts and stops Group Replication.
const (
	mysqlStartEvent = "mysql: START GROUP_REPLICATION"
	mysqlStopEvent  = "mysql: STOP GROUP_REPLICATION"
)

// pauseEvents are the serving events of a planned pause, until the change of MySQL: the tablet
// reports that it does not serve while it still serves, and then stops serving.
var pauseEvents = []string{"lameduck", "broadcast serving=false", "serving=false"}

// resumeEvents are the serving events of the end of a planned pause.
var resumeEvents = []string{"serving=true", "broadcast serving=true"}

// newPausingPrimary starts tablet cell1-1, a serving PRIMARY under a semi-sync policy, as during a
// migration to Group Replication, with a short pause notice.
func newPausingPrimary(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon, *tabletservermock.Controller) {
	t.Helper()
	withGroupReplication(t)
	oldNotice, oldResume := groupReplicationPauseNotice, groupReplicationPauseResumeTimeout
	t.Cleanup(func() { groupReplicationPauseNotice, groupReplicationPauseResumeTimeout = oldNotice, oldResume })
	groupReplicationPauseNotice = 10 * time.Millisecond
	ts := newGroupReplicationTopo(t, policy.DurabilitySemiSync)
	tm, fmd := newGroupReplicationTestTM(t, ts, 1, nil)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	return tm, fmd, qsc
}

// eventsSince returns the serving events recorded after the first n, by a tablet that served then,
// without the calls of SetServingType that kept the serving state as it was: the tablet's state is
// refreshed in the background too, for example when its shard record changes.
func eventsSince(qsc *tabletservermock.Controller, n int) []string {
	var events []string
	state := "serving=true"
	for _, event := range qsc.ServingEvents()[n:] {
		if strings.HasPrefix(event, "serving=") {
			if event == state {
				continue
			}
			state = event
		}
		events = append(events, event)
	}
	return events
}

// concat returns the concatenation of the given event lists.
func concat(lists ...[]string) []string {
	var all []string
	for _, l := range lists {
		all = append(all, l...)
	}
	return all
}

// TestBootstrapOnServingPrimaryPausesServing checks that a migration's bootstrap of the group on the
// serving primary is a planned pause: the tablet reports that it does not serve, then stops serving,
// before MySQL starts Group Replication, during which MySQL refuses commits for a moment, and serves
// again, with its primary term, once MySQL is the writable primary of its group. vtgate buffers the
// writes meanwhile.
func TestBootstrapOnServingPrimaryPausesServing(t *testing.T) {
	tm, fmd, qsc := newPausingPrimary(t)
	termStart := tm.Tablet().PrimaryTermStartTime
	var servingDuringStart atomic.Bool
	fmd.StartGroupReplicationHook = func(bool) {
		servingDuringStart.Store(qsc.IsServing())
		qsc.RecordServingEvent(mysqlStartEvent)
	}
	n := len(qsc.ServingEvents())

	_, err := tm.StartGroupReplication(t.Context(), startRequest(true))
	require.NoError(t, err)
	assert.False(t, servingDuringStart.Load(), "the primary must not serve while MySQL starts Group Replication")
	assert.Equal(t, concat(pauseEvents, []string{mysqlStartEvent}, resumeEvents), eventsSince(qsc, n))
	assert.True(t, qsc.IsServing())
	assert.Equal(t, topodatapb.TabletType_PRIMARY, qsc.CurrentTarget().TabletType)
	assert.Equal(t, termStart, tm.Tablet().PrimaryTermStartTime, "the pause keeps the primary term")
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.Empty(t, tm.tmState.primaryNotServingReason())
}

// TestPausedPrimaryServesOnlyOnceMySQLIsWritable checks that a primary that paused for the bootstrap
// serves again only once MySQL is writable: here MySQL stays super_read_only for a while after
// Group Replication started.
func TestPausedPrimaryServesOnlyOnceMySQLIsWritable(t *testing.T) {
	tm, fmd, qsc := newPausingPrimary(t)
	var readOnlyPolls atomic.Int32
	var servedWhileReadOnly atomic.Bool
	fmd.SetGroupReplicationStatusHook(func() {
		if start, _, _ := fmd.GroupReplicationCalls(); start == 0 || readOnlyPolls.Load() >= 5 {
			return
		}
		fmd.SuperReadOnly.Store(true)
		if qsc.IsServing() {
			servedWhileReadOnly.Store(true)
		}
		if readOnlyPolls.Add(1) == 5 {
			fmd.SuperReadOnly.Store(false)
		}
	})

	_, err := tm.StartGroupReplication(t.Context(), startRequest(true))
	require.NoError(t, err)
	require.Equal(t, int32(5), readOnlyPolls.Load(), "the tablet must wait for MySQL to be writable")
	assert.False(t, servedWhileReadOnly.Load(), "the primary must not serve while MySQL is read-only")
	assert.True(t, qsc.IsServing())
	assert.False(t, fmd.SuperReadOnly.Load())
}

// TestPausedPrimaryResumesAfterFailedBootstrap checks that a primary whose bootstrap failed during
// its pause serves again, as before the pause: MySQL is still the writable semi-sync primary, and the
// RPC reports the failure, so that the migration can be run again.
func TestPausedPrimaryResumesAfterFailedBootstrap(t *testing.T) {
	tm, fmd, qsc := newPausingPrimary(t)
	fmd.ExpectedExecuteSuperQueryList = nil
	fmd.StartGroupReplicationError = errors.New("the group failed to start")
	fmd.StartGroupReplicationHook = func(bool) { qsc.RecordServingEvent(mysqlStartEvent) }
	n := len(qsc.ServingEvents())

	_, err := tm.StartGroupReplication(t.Context(), startRequest(true))
	require.ErrorContains(t, err, "the group failed to start")
	assert.Equal(t, concat(pauseEvents, []string{mysqlStartEvent}, resumeEvents), eventsSince(qsc, n))
	assert.True(t, qsc.IsServing(), "the primary serves again after a failed bootstrap")
	assert.Empty(t, tm.tmState.primaryNotServingReason())
}

// TestPausedPrimaryResumesWhenMySQLStaysReadOnly checks that the pause is bounded: a primary whose
// MySQL is still read-only groupReplicationPauseResumeTimeout after the change ended serves again,
// as a primary whose MySQL is read-only, which VTOrc repairs, rather than stay paused.
func TestPausedPrimaryResumesWhenMySQLStaysReadOnly(t *testing.T) {
	tm, fmd, qsc := newPausingPrimary(t)
	groupReplicationPauseResumeTimeout = 50 * time.Millisecond
	fmd.ExpectedExecuteSuperQueryList = nil
	fmd.StartGroupReplicationError = errors.New("the group failed to start")
	fmd.StartGroupReplicationHook = func(bool) {
		fmd.SuperReadOnly.Store(true)
		qsc.RecordServingEvent(mysqlStartEvent)
	}
	n := len(qsc.ServingEvents())

	_, err := tm.StartGroupReplication(t.Context(), startRequest(true))
	require.ErrorContains(t, err, "the group failed to start")
	assert.Equal(t, concat(pauseEvents, []string{mysqlStartEvent}, resumeEvents), eventsSince(qsc, n))
	assert.True(t, qsc.IsServing())
}

// TestPausedPrimaryDoesNotServeWhenItsStateIsRefreshed checks that nothing else makes a paused
// primary serve before the change of its MySQL ended, for example a refresh of the tablet's state
// after a change of the shard record.
func TestPausedPrimaryDoesNotServeWhenItsStateIsRefreshed(t *testing.T) {
	tm, fmd, qsc := newPausingPrimary(t)
	var servingAfterRefresh atomic.Bool
	var refreshErr error
	fmd.StartGroupReplicationHook = func(bool) {
		tm.tmState.mu.Lock()
		refreshErr = tm.tmState.updateLocked(t.Context())
		tm.tmState.mu.Unlock()
		servingAfterRefresh.Store(qsc.IsServing())
	}

	_, err := tm.StartGroupReplication(t.Context(), startRequest(true))
	require.NoError(t, err)
	require.NoError(t, refreshErr)
	assert.False(t, servingAfterRefresh.Load(), "a refresh must not make a paused primary serve")
	assert.True(t, qsc.IsServing())
}

// TestPrimaryLeavingItsGroupPausesServing checks that the primary's leave of its group, the last
// step of a migration back to semi-sync, is a planned pause: MySQL refuses commits while Group
// Replication stops, and is read-only after; the tablet serves again once MySQL is writable.
func TestPrimaryLeavingItsGroupPausesServing(t *testing.T) {
	tm, fmd, qsc := newPausingPrimary(t)
	_, err := tm.StartGroupReplication(t.Context(), startRequest(true))
	require.NoError(t, err)
	fmd.SetGroupReplicationStatusHook(func() {
		if _, stop, _ := fmd.GroupReplicationCalls(); stop == 1 {
			fmd.SetGroupReplicationStatusHook(nil)
			qsc.RecordServingEvent(mysqlStopEvent)
		}
	})
	n := len(qsc.ServingEvents())

	_, err = tm.StopGroupReplication(t.Context())
	require.NoError(t, err)
	assert.Equal(t, concat(pauseEvents, []string{mysqlStopEvent}, resumeEvents), eventsSince(qsc, n))
	assert.True(t, qsc.IsServing())
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.False(t, fmd.ReadOnly)
}

// TestPausedPrimaryKeepsTheServingInvariant checks that the end of a pause does not make a primary
// serve that must not serve for its replication group: a PRIMARY whose MySQL leaves its group under
// a group replication policy that lists voters is outside of any group afterwards. It does not wait
// for MySQL to be writable either, since it will not serve.
func TestPausedPrimaryKeepsTheServingInvariant(t *testing.T) {
	withGroupReplication(t)
	oldNotice, oldResume := groupReplicationPauseNotice, groupReplicationPauseResumeTimeout
	t.Cleanup(func() { groupReplicationPauseNotice, groupReplicationPauseResumeTimeout = oldNotice, oldResume })
	groupReplicationPauseNotice = 10 * time.Millisecond
	groupReplicationPauseResumeTimeout = time.Hour
	ctx := t.Context()
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	fmd.SetGroupReplicationStatus(twoVotersView())
	newGroupReplicationSync(tm).reconcile(ctx)
	require.True(t, qsc.IsServing())
	n := len(qsc.ServingEvents())

	done := make(chan error, 1)
	go func() {
		_, err := tm.StopGroupReplication(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(groupReplicationTestTimeout):
		require.FailNow(t, "the pause waited for MySQL to be writable although the primary must not serve")
	}
	events := eventsSince(qsc, n)
	assert.Equal(t, pauseEvents, events[:len(pauseEvents)])
	assert.NotContains(t, events, "serving=true")
	assert.NotContains(t, events, "broadcast serving=true")
	assert.False(t, qsc.IsServing(), "a primary outside of any group must not serve under a group replication policy")
	assert.True(t, fmd.SuperReadOnly.Load(), "MySQL stays read-only")
}

// TestGroupReplicationSyncDoesNotStopServingUnderStalePolicy reproduces a run of
// TestGroupReplicationLifecycle: a migration back to semi-sync sets the keyspace policy first, and
// then shrinks the group to the primary. The sync loop, which had cached the group replication
// policy a moment before, stopped serving for the lost voter majority, and served again once its
// cache expired: a pause of its own, without notice, two seconds before the primary's planned pause
// for its leave of the group. vtgate buffers a shard at most once per
// --buffer-min-time-between-failovers (1 minute by default), so the planned pause would then fail
// writes. The loop reads the policy again before it stops serving.
func TestGroupReplicationSyncDoesNotStopServingUnderStalePolicy(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, ts := newLegitimacyTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	fmd.SetGroupReplicationStatus(twoVotersView())
	s := newGroupReplicationSync(tm)
	s.reconcile(ctx)
	require.True(t, qsc.IsServing())
	// The voters' leaves come within groupReplicationVotersCacheTTL of the last check.
	s.peersFetched = time.Time{}

	lockCtx, unlock, err := ts.LockKeyspace(ctx, "ks", "migrate back to semi-sync")
	require.NoError(t, err)
	ki, err := ts.GetKeyspace(lockCtx, "ks")
	require.NoError(t, err)
	ki.DurabilityPolicy = policy.DurabilitySemiSync
	require.NoError(t, ts.UpdateKeyspace(lockCtx, ki))
	unlock(&err)
	require.NoError(t, err)
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:8"))
	n := len(qsc.ServingEvents())
	s.reconcile(ctx)
	assert.True(t, qsc.IsServing(), "the voter majority does not apply under the semi-sync policy")
	assert.NotContains(t, qsc.ServingEvents()[n:], "serving=false")
}
