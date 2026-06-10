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

package buffer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// The tests in this file pin down the behavior of waitForFailoverEnd while
// shardBuffer.mu is released around shardMarker.MarkShardNotServing (the
// "mark window"). The buffering cycle is reserved (IDLE -> BUFFERING) before
// the mark so that stop events arriving during the window are applied by the
// normal state machine instead of being lost against the IDLE state.

// fakeShardMarker implements shardMarker with controllable blocking and
// result, so tests can deterministically interleave other actors with the
// mark window.
type fakeShardMarker struct {
	result bool
	// entered, if non-nil, receives one message per MarkShardNotServing call
	// as soon as the call begins.
	entered chan struct{}
	// release, if non-nil, blocks every MarkShardNotServing call until it is
	// closed.
	release chan struct{}
	calls   atomic.Int32
}

func (m *fakeShardMarker) MarkShardNotServing(ctx context.Context, keyspace, shard string, isReparentErr bool) bool {
	m.calls.Add(1)
	if m.entered != nil {
		m.entered <- struct{}{}
	}
	if m.release != nil {
		<-m.release
	}
	return m.result
}

type failoverEndResult struct {
	retryDone RetryDoneFunc
	err       error
}

// issueMarkedRequest runs waitForFailoverEnd with the given marker in a
// goroutine and returns a channel carrying its result.
func issueMarkedRequest(ctx context.Context, sb *shardBuffer, ks, sh string, marker shardMarker) chan failoverEndResult {
	resultCh := make(chan failoverEndResult, 1)
	go func() {
		retryDone, err := sb.waitForFailoverEnd(ctx, ks, sh, marker, failoverErr)
		resultCh <- failoverEndResult{retryDone: retryDone, err: err}
	}()
	return resultCh
}

func waitResult(t *testing.T, resultCh chan failoverEndResult) failoverEndResult {
	t.Helper()
	select {
	case r := <-resultCh:
		return r
	case <-time.After(30 * time.Second):
		require.FailNow(t, "waitForFailoverEnd did not return")
		return failoverEndResult{}
	}
}

// servingKeyspaceEvent returns a resolution event which stops buffering for
// the given shard.
func servingKeyspaceEvent(ks, sh string) *discovery.KeyspaceEvent {
	return &discovery.KeyspaceEvent{
		Keyspace: ks,
		Shards: []discovery.ShardEvent{{
			Tablet:  &topodatapb.TabletAlias{Cell: "cell1", Uid: 100},
			Target:  &querypb.Target{Keyspace: ks, Shard: sh, TabletType: topodatapb.TabletType_PRIMARY},
			Serving: true,
		}},
	}
}

func waitForShardBufferState(t *testing.T, sb *shardBuffer, want bufferState) {
	t.Helper()
	require.Eventually(t, func() bool {
		return sb.testGetState() == want
	}, 30*time.Second, time.Millisecond, "shardBuffer did not reach state %v", want)
}

// TestMarkWindow_StopAndDrainCompleteDuringMark covers the review blocker of
// the fix design: while a request is parked in MarkShardNotServing, the
// cycle it reserved is stopped by a keyspace event AND fully drained, so the
// state is IDLE (with a nil timeoutThread) by the time the request re-locks.
// The request must pass through without buffering. A state re-check via
// shouldBufferLocked instead of an exact BUFFERING test would enqueue into
// the drained queue and panic on the nil timeoutThread.
func TestMarkWindow_StopAndDrainCompleteDuringMark(t *testing.T) {
	resetVariables()
	const ks, sh = "markstop_ks", "0"

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	buf := New(cfg)
	defer buf.Shutdown()
	sb := buf.getOrCreateBuffer(ks, sh)

	marker := &fakeShardMarker{result: true, entered: make(chan struct{}), release: make(chan struct{})}
	resultCh := issueMarkedRequest(t.Context(), sb, ks, sh, marker)

	// The request reserved the cycle and is parked in the marker.
	<-marker.entered
	require.Equal(t, stateBuffering, sb.testGetState())

	// Stop the cycle and wait for the (empty) drain to complete.
	buf.HandleKeyspaceEvent(servingKeyspaceEvent(ks, sh))
	waitForShardBufferState(t, sb, stateIdle)

	close(marker.release)
	result := waitResult(t, resultCh)
	require.NoError(t, result.err)
	require.Nil(t, result.retryDone, "request must not be buffered after its cycle ended during the mark window")
	require.Equal(t, stateIdle, sb.testGetState())
	require.Equal(t, int64(1), stops.Counts()[ks+"."+sh+"."+string(stopFailoverEndDetected)])
}

// TestMarkWindow_RelockDuringDraining covers the request re-locking while
// the stopped cycle is still draining (kept open by another buffered
// request): it must pass through, not enqueue.
func TestMarkWindow_RelockDuringDraining(t *testing.T) {
	resetVariables()
	const ks, sh = "markdrain_ks", "0"

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	buf := New(cfg)
	defer buf.Shutdown()
	sb := buf.getOrCreateBuffer(ks, sh)

	marker1 := &fakeShardMarker{result: true, entered: make(chan struct{}), release: make(chan struct{})}
	result1Ch := issueMarkedRequest(t.Context(), sb, ks, sh, marker1)
	<-marker1.entered

	// A second request arrives while the state is BUFFERING: it enqueues via
	// the regular path and must not consult its own marker (no marker herd).
	marker2 := &fakeShardMarker{result: false}
	result2Ch := issueMarkedRequest(t.Context(), sb, ks, sh, marker2)
	require.Eventually(t, func() bool { return sb.testGetSize() == 1 }, 30*time.Second, time.Millisecond)
	require.Equal(t, int32(0), marker2.calls.Load(), "request arriving during BUFFERING must not call MarkShardNotServing")

	// Stop the cycle. The drain unblocks request 2 but then waits for its
	// retryDone, keeping the state in DRAINING.
	buf.HandleKeyspaceEvent(servingKeyspaceEvent(ks, sh))
	result2 := waitResult(t, result2Ch)
	require.NoError(t, result2.err)
	require.NotNil(t, result2.retryDone)
	require.Equal(t, stateDraining, sb.testGetState())

	// Request 1 resumes from the marker while the state is DRAINING: it must
	// pass through.
	close(marker1.release)
	result1 := waitResult(t, result1Ch)
	require.NoError(t, result1.err)
	require.Nil(t, result1.retryDone)
	require.Equal(t, stateDraining, sb.testGetState())

	result2.retryDone()
	waitForShardBufferState(t, sb, stateIdle)
	require.Equal(t, int64(1), requestsBuffered.Counts()[ks+"."+sh])
	require.Equal(t, int64(1), requestsDrained.Counts()[ks+"."+sh])
}

// TestMarkWindow_MarkFailureRollsBack covers the mark returning false with
// the reserved cycle still live: the cycle is rolled back (stop reason
// MarkNotServingFailed) and lastEnd is restored so the rollback does not
// suppress the next buffering attempt via --buffer-min-time-between-failovers.
func TestMarkWindow_MarkFailureRollsBack(t *testing.T) {
	resetVariables()
	const ks, sh = "markfail_ks", "0"

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	buf := New(cfg)
	defer buf.Shutdown()
	sb := buf.getOrCreateBuffer(ks, sh)

	marker := &fakeShardMarker{result: false}
	result := waitResult(t, issueMarkedRequest(t.Context(), sb, ks, sh, marker))
	require.NoError(t, result.err)
	require.Nil(t, result.retryDone)

	waitForShardBufferState(t, sb, stateIdle)
	require.Equal(t, int64(1), starts.Counts()[ks+"."+sh])
	require.Equal(t, int64(1), stops.Counts()[ks+"."+sh+"."+string(stopMarkFailed)])

	// lastEnd was restored: a new failover error must start buffering
	// immediately instead of being skipped as "last failover too recent".
	marker2 := &fakeShardMarker{result: true}
	result2Ch := issueMarkedRequest(t.Context(), sb, ks, sh, marker2)
	require.Eventually(t, func() bool { return sb.testGetSize() == 1 }, 30*time.Second, time.Millisecond)
	require.Equal(t, stateBuffering, sb.testGetState())
	require.Equal(t, int64(0), requestsSkipped.Counts()[ks+"."+sh+"."+string(skippedLastFailoverTooRecent)])

	buf.HandleKeyspaceEvent(servingKeyspaceEvent(ks, sh))
	result2 := waitResult(t, result2Ch)
	require.NoError(t, result2.err)
	require.NotNil(t, result2.retryDone)
	result2.retryDone()
	waitForShardBufferState(t, sb, stateIdle)
}

// TestMarkWindow_MarkFailureAfterRealStop covers the mark returning false
// after the cycle was already stopped by a keyspace event: the rollback is
// skipped (no double stop) and the genuine stop's lastEnd stands, so the
// next buffering attempt IS suppressed as too recent.
func TestMarkWindow_MarkFailureAfterRealStop(t *testing.T) {
	resetVariables()
	const ks, sh = "markfailstop_ks", "0"

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	buf := New(cfg)
	defer buf.Shutdown()
	sb := buf.getOrCreateBuffer(ks, sh)

	marker := &fakeShardMarker{result: false, entered: make(chan struct{}), release: make(chan struct{})}
	resultCh := issueMarkedRequest(t.Context(), sb, ks, sh, marker)
	<-marker.entered

	buf.HandleKeyspaceEvent(servingKeyspaceEvent(ks, sh))
	waitForShardBufferState(t, sb, stateIdle)

	close(marker.release)
	result := waitResult(t, resultCh)
	require.NoError(t, result.err)
	require.Nil(t, result.retryDone)
	require.Equal(t, int64(1), stops.Counts()[ks+"."+sh+"."+string(stopFailoverEndDetected)])
	require.Equal(t, int64(0), stops.Counts()[ks+"."+sh+"."+string(stopMarkFailed)], "rollback must not fire after a genuine stop")

	// The event's lastEnd stands: the next attempt is skipped as too recent.
	marker2 := &fakeShardMarker{result: true}
	result2 := waitResult(t, issueMarkedRequest(t.Context(), sb, ks, sh, marker2))
	require.NoError(t, result2.err)
	require.Nil(t, result2.retryDone)
	require.Equal(t, int32(0), marker2.calls.Load())
	require.Equal(t, int64(1), requestsSkipped.Counts()[ks+"."+sh+"."+string(skippedLastFailoverTooRecent)])
}

// TestMarkWindow_GenerationGuardProtectsSuccessorCycle covers a mark failure
// resuming after the original cycle was stopped AND a successor cycle has
// already started: the rollback must not stop the successor cycle.
func TestMarkWindow_GenerationGuardProtectsSuccessorCycle(t *testing.T) {
	resetVariables()
	const ks, sh = "markgen_ks", "0"

	var nowMu sync.Mutex
	now := time.Now()
	cfg := NewDefaultConfig()
	cfg.Enabled = true
	cfg.now = func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		defer nowMu.Unlock()
		now = now.Add(d)
	}

	buf := New(cfg)
	defer buf.Shutdown()
	sb := buf.getOrCreateBuffer(ks, sh)

	// Cycle 1: parked in its marker, then stopped by a keyspace event.
	marker1 := &fakeShardMarker{result: false, entered: make(chan struct{}), release: make(chan struct{})}
	result1Ch := issueMarkedRequest(t.Context(), sb, ks, sh, marker1)
	<-marker1.entered
	buf.HandleKeyspaceEvent(servingKeyspaceEvent(ks, sh))
	waitForShardBufferState(t, sb, stateIdle)

	// Move past --buffer-min-time-between-failovers so a successor cycle can
	// start, and start it.
	advance(cfg.MinTimeBetweenFailovers + time.Second)
	marker2 := &fakeShardMarker{result: true}
	result2Ch := issueMarkedRequest(t.Context(), sb, ks, sh, marker2)
	require.Eventually(t, func() bool { return sb.testGetSize() == 1 }, 30*time.Second, time.Millisecond)
	require.Equal(t, stateBuffering, sb.testGetState())

	// Cycle 1's mark fails now: the generation guard must leave the
	// successor cycle untouched.
	close(marker1.release)
	result1 := waitResult(t, result1Ch)
	require.NoError(t, result1.err)
	require.Nil(t, result1.retryDone)
	require.Equal(t, stateBuffering, sb.testGetState(), "mark failure of a stale cycle must not stop the successor cycle")
	require.Equal(t, 1, sb.testGetSize())
	require.Equal(t, int64(0), stops.Counts()[ks+"."+sh+"."+string(stopMarkFailed)])

	buf.HandleKeyspaceEvent(servingKeyspaceEvent(ks, sh))
	result2 := waitResult(t, result2Ch)
	require.NoError(t, result2.err)
	require.NotNil(t, result2.retryDone)
	result2.retryDone()
	waitForShardBufferState(t, sb, stateIdle)
}

// TestMarkWindow_ConcurrentRequestsEnqueueDuringMark covers requests arriving
// during the mark window: they enqueue into the reserved cycle (without
// calling their own marker) and the marking request enqueues as well after a
// successful mark.
func TestMarkWindow_ConcurrentRequestsEnqueueDuringMark(t *testing.T) {
	resetVariables()
	const ks, sh = "markconc_ks", "0"

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	buf := New(cfg)
	defer buf.Shutdown()
	sb := buf.getOrCreateBuffer(ks, sh)

	marker1 := &fakeShardMarker{result: true, entered: make(chan struct{}), release: make(chan struct{})}
	result1Ch := issueMarkedRequest(t.Context(), sb, ks, sh, marker1)
	<-marker1.entered

	marker2 := &fakeShardMarker{result: false}
	result2Ch := issueMarkedRequest(t.Context(), sb, ks, sh, marker2)
	require.Eventually(t, func() bool { return sb.testGetSize() == 1 }, 30*time.Second, time.Millisecond)
	require.Equal(t, int32(0), marker2.calls.Load(), "request arriving during BUFFERING must not call MarkShardNotServing")

	close(marker1.release)
	require.Eventually(t, func() bool { return sb.testGetSize() == 2 }, 30*time.Second, time.Millisecond)
	require.Equal(t, int32(1), marker1.calls.Load())

	// End the failover: both requests are drained sequentially in queue
	// order — request 2 enqueued first (while request 1 was parked in the
	// marker), and the drain waits for each retryDone before proceeding.
	buf.HandleKeyspaceEvent(servingKeyspaceEvent(ks, sh))
	for _, ch := range []chan failoverEndResult{result2Ch, result1Ch} {
		result := waitResult(t, ch)
		require.NoError(t, result.err)
		require.NotNil(t, result.retryDone)
		result.retryDone()
	}
	waitForShardBufferState(t, sb, stateIdle)
	require.Equal(t, int64(2), requestsBuffered.Counts()[ks+"."+sh])
	require.Equal(t, int64(2), requestsDrained.Counts()[ks+"."+sh])
	require.Equal(t, int64(1), starts.Counts()[ks+"."+sh])
}

// TestMarkWindow_ShutdownCompletesDuringMark covers Buffer.Shutdown() running
// to completion while a request is parked in the marker: Shutdown does not
// wait for that request (it is not tracked in sb.wg), so the request resumes
// after Shutdown returned and must not buffer or restart a cycle.
func TestMarkWindow_ShutdownCompletesDuringMark(t *testing.T) {
	resetVariables()
	const ks, sh = "markshutdown_ks", "0"

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	buf := New(cfg)
	sb := buf.getOrCreateBuffer(ks, sh)

	marker := &fakeShardMarker{result: true, entered: make(chan struct{}), release: make(chan struct{})}
	resultCh := issueMarkedRequest(t.Context(), sb, ks, sh, marker)
	<-marker.entered
	require.Equal(t, stateBuffering, sb.testGetState())

	// Shutdown stops the reserved cycle and returns while the request is
	// still parked in the marker.
	buf.Shutdown()
	require.Equal(t, stateIdle, sb.testGetState(), "Shutdown must stop and drain the reserved cycle")

	close(marker.release)
	result := waitResult(t, resultCh)
	require.NoError(t, result.err)
	require.Nil(t, result.retryDone)
	require.Equal(t, stateIdle, sb.testGetState(), "no buffering may start after Shutdown returned")
	require.Equal(t, int64(1), stops.Counts()[ks+"."+sh+"."+string(stopShutdown)])
}

// TestMarkWindow_TimeoutDuringMark covers the mark blocking longer than
// --buffer-max-failover-duration: the timeout thread stops the cycle. When
// the mark then fails, the rollback is skipped and the timeout stop's
// lastEnd stands (the cycle genuinely ran and timed out), so the next
// buffering attempt is suppressed as too recent.
func TestMarkWindow_TimeoutDuringMark(t *testing.T) {
	resetVariables()
	const ks, sh = "marktimeout_ks", "0"

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	cfg.Window = 50 * time.Millisecond
	cfg.MaxFailoverDuration = 100 * time.Millisecond
	buf := New(cfg)
	defer buf.Shutdown()
	sb := buf.getOrCreateBuffer(ks, sh)

	marker := &fakeShardMarker{result: false, entered: make(chan struct{}), release: make(chan struct{})}
	resultCh := issueMarkedRequest(t.Context(), sb, ks, sh, marker)
	<-marker.entered

	// The timeout thread stops the cycle while the mark is still pending.
	waitForShardBufferState(t, sb, stateIdle)
	require.Equal(t, int64(1), stops.Counts()[ks+"."+sh+"."+string(stopMaxFailoverDurationExceeded)])

	close(marker.release)
	result := waitResult(t, resultCh)
	require.NoError(t, result.err)
	require.Nil(t, result.retryDone)
	require.Equal(t, int64(0), stops.Counts()[ks+"."+sh+"."+string(stopMarkFailed)], "rollback must not fire after the timeout stop")

	// The timeout stop was genuine: its lastEnd stands and suppresses the
	// next attempt.
	marker2 := &fakeShardMarker{result: true}
	result2 := waitResult(t, issueMarkedRequest(t.Context(), sb, ks, sh, marker2))
	require.NoError(t, result2.err)
	require.Nil(t, result2.retryDone)
	require.Equal(t, int32(0), marker2.calls.Load())
	require.Equal(t, int64(1), requestsSkipped.Counts()[ks+"."+sh+"."+string(skippedLastFailoverTooRecent)])
}
