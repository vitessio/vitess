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

package vstreamer

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// newThrottleTestVStreamer builds the minimal vstreamer that drives
// parseEvents without MySQL: a nil-throttler client throttles only
// throttlerapp.TestingAlwaysThrottledName, so the app name picks which branch
// of throttleEvents the worker lands in.
func newThrottleTestVStreamer(app throttlerapp.Name, counterPrefix string, send func([]*binlogdatapb.VEvent) error) (*vstreamer, *Engine) {
	throttlerClient := throttle.NewBackgroundClient(nil, app, base.UndefinedScope)
	vse := &Engine{
		throttlerClient: throttlerClient,
		throttledCounts: stats.NewCounter(counterPrefix+"ThrottledCounts", "Number of times vstreamer was throttled by the tablet throttler"),
		errorCounts:     stats.NewCountersWithSingleLabel(counterPrefix+"Errors", "Tracks errors in vstreamer", "type"),
	}
	vs := &vstreamer{
		filter:       &binlogdatapb.Filter{},
		throttlerApp: app,
		vevents:      make(chan *localVSchema, 1),
		plans:        make(map[uint64]*streamerPlan),
		vse:          vse,
		send:         send,
	}
	return vs, vse
}

// parseEventsGone reports whether no goroutine is running parseEvents or any
// of its closures anymore. The throttleEvents worker runs as a parseEvents
// closure, so once parseEvents has returned this is how the worker's exit is
// observed directly, regardless of unrelated background goroutines.
func parseEventsGone() bool {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return !strings.Contains(string(buf[:n]), ").parseEvents")
}

// TestParseEventsStopsThrottlerGoroutine is a regression test for
// https://github.com/vitessio/vitess/issues/22118.
//
// parseEvents launches throttleEvents on the caller's context but returns on
// its own (io.EOF at a stop position, or any error). Before the fix, the
// goroutine kept running: a goroutine sitting in the fully-throttled branch
// sent on throttlerErrs after fullyThrottledTimeout, but parseEvents had
// already closed the channel via defer, so the send panicked and crashed
// vttablet.
//
// This test drives parseEvents with throttlerapp.TestingAlwaysThrottledName
// to keep the goroutine in the fully-throttled branch deterministically. Once
// the goroutine has settled, the errs channel (which models the BinlogConnection
// dying) makes parseEvents return while the caller's context stays alive. The
// worker must then exit through the derived context's cancellation, well
// before fullyThrottledTimeout could fire: the throttled counter, which the
// worker increments every throttleCheckDuration while alive, must stop moving.
func TestParseEventsStopsThrottlerGoroutine(t *testing.T) {
	origTimeout := fullyThrottledTimeout
	origHeartbeatTime := HeartbeatTime
	t.Cleanup(func() {
		fullyThrottledTimeout = origTimeout
		HeartbeatTime = origHeartbeatTime
	})
	// The goroutine reports fully-throttled after this much continuous
	// throttling; long enough that parseEvents can return first.
	fullyThrottledTimeout = 2 * time.Second
	// Keep the heartbeat branch out of the test entirely.
	HeartbeatTime = time.Hour

	vs, vse := newThrottleTestVStreamer(throttlerapp.TestingAlwaysThrottledName, "TestParseEventsStopsThrottlerGoroutine", func([]*binlogdatapb.VEvent) error { return nil })

	events := make(chan mysql.BinlogEvent)
	errs := make(chan error, 1)
	returned := make(chan error, 1)
	// The caller's context intentionally outlives parseEvents, like vs.ctx does
	// for the fast-forward vstreamer and the schema tracker / messager callers,
	// but is still cancelled when the test ends so a failure here cannot leak
	// the worker into subsequent tests while cleanup restores the shared
	// timeout settings.
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go func() {
		returned <- vs.parseEvents(ctx, events, errs)
	}()

	// Startup synchronization: wait until throttleEvents has entered the
	// fully-throttled branch and armed its fullyThrottledTimeout deadline.
	require.Eventually(t, func() bool {
		return vse.throttledCounts.Get() > 0
	}, 10*time.Second, 10*time.Millisecond, "throttleEvents never entered the fully-throttled branch")

	// The binlog connection dies: parseEvents returns while the caller's
	// context stays alive.
	connErr := errors.New("binlog connection lost")
	errs <- connErr
	select {
	case err := <-returned:
		require.ErrorIs(t, err, connErr)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "parseEvents did not return after the events error channel fired")
	}

	// The derived context is cancelled as parseEvents returns, so the worker
	// must exit immediately, long before fullyThrottledTimeout could end it.
	// A leaked worker would keep incrementing throttledCounts every
	// throttleCheckDuration (250ms); the counter of an exited worker never
	// moves again. The window stays under fullyThrottledTimeout so a worker
	// that would only stop at its timeout is still caught mid-leak.
	frozen := vse.throttledCounts.Get()
	assert.Never(t, func() bool {
		return vse.throttledCounts.Get() != frozen
	}, fullyThrottledTimeout/2, 25*time.Millisecond, "throttleEvents goroutine is still running after parseEvents returned")
	assert.True(t, parseEventsGone(), "throttleEvents goroutine is still running after parseEvents returned")
}

// TestParseEventsStopsBlockedForwarder covers the second leak path of
// https://github.com/vitessio/vitess/issues/22118: a throttleEvents worker
// blocked forwarding an event on throttledEvents cannot exit through
// fullyThrottledTimeout, because that deadline only applies to the
// fully-throttled branch. It can only leave through the context. Before the
// fix it leaked until the caller's context ended.
//
// The test parks parseEvents inside vs.send on the heartbeat path, then hands
// the worker an event it cannot forward. Releasing the send with io.EOF makes
// parseEvents return, cancelling the derived context; the blocked worker must
// then exit while the caller's context is still alive.
func TestParseEventsStopsBlockedForwarder(t *testing.T) {
	origHeartbeatTime := HeartbeatTime
	t.Cleanup(func() {
		HeartbeatTime = origHeartbeatTime
	})
	// Fire the heartbeat quickly; it is how parseEvents gets parked in vs.send.
	HeartbeatTime = 50 * time.Millisecond

	sendStarted := make(chan struct{})
	sendGate := make(chan error, 1)
	var sendOnce sync.Once
	vs, _ := newThrottleTestVStreamer("never-throttled-app", "TestParseEventsStopsBlockedForwarder", func([]*binlogdatapb.VEvent) error {
		sendOnce.Do(func() { close(sendStarted) })
		return <-sendGate
	})

	events := make(chan mysql.BinlogEvent)
	errs := make(chan error, 1)
	returned := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go func() {
		returned <- vs.parseEvents(ctx, events, errs)
	}()

	// Wait until parseEvents is parked in vs.send on the heartbeat path.
	select {
	case <-sendStarted:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "parseEvents never reached the heartbeat send")
	}
	// Hand the worker an event. parseEvents is not reading throttledEvents
	// while parked in send, so the worker blocks in its forwarding select with
	// the event in hand.
	events <- mysql.NewInvalidEvent()

	// Releasing the send with io.EOF makes parseEvents return nil, which
	// cancels the derived context and unblocks the worker through ctx.Done().
	sendGate <- io.EOF
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "parseEvents did not return after the heartbeat send completed")
	}

	// The worker must be gone while the caller's context is still alive.
	assert.Eventually(t, parseEventsGone, 10*time.Second, 10*time.Millisecond, "throttleEvents goroutine blocked on forwarding is still running after parseEvents returned")
}
