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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// TestParseEventsStopsThrottlerGoroutine is a regression test for
// https://github.com/vitessio/vitess/issues/22118.
//
// parseEvents launches throttleEvents on the caller's context but returns on
// its own (io.EOF at a stop position, or any error). Before the fix, the
// goroutine kept running: a goroutine sitting in the fully-throttled branch
// sent on throttlerErrs after fullyThrottledTimeout, but parseEvents had
// already closed the channel via defer, so the send panicked and crashed
// vttablet. A goroutine blocked forwarding an event also leaked until the
// caller's context ended.
//
// This test drives parseEvents without MySQL: a nil-throttler client combined
// with throttlerapp.TestingAlwaysThrottledName keeps the goroutine in the
// fully-throttled branch deterministically. Once the goroutine has settled,
// the errs channel (which models the BinlogConnection dying) makes parseEvents
// return while the caller's context stays alive. Without the fix the test
// binary panics on the closed throttlerErrs shortly after; with the fix the
// goroutine observes the derived context's cancellation and exits quietly.
func TestParseEventsStopsThrottlerGoroutine(t *testing.T) {
	origTimeout := fullyThrottledTimeout
	origHeartbeatTime := HeartbeatTime
	t.Cleanup(func() {
		fullyThrottledTimeout = origTimeout
		HeartbeatTime = origHeartbeatTime
	})
	// The goroutine reports fully-throttled after this much continuous
	// throttling; long enough that parseEvents can return first, short enough
	// that a leaked goroutine fires (and, unfixed, panics) within the test.
	fullyThrottledTimeout = 2 * time.Second
	// Keep the heartbeat branch out of the test entirely.
	HeartbeatTime = time.Hour

	// A nil-throttler client only throttles throttlerapp.TestingAlwaysThrottledName,
	// which keeps throttleEvents in the fully-throttled branch for the whole test.
	throttlerClient := throttle.NewBackgroundClient(nil, throttlerapp.TestingAlwaysThrottledName, base.UndefinedScope)
	vse := &Engine{
		throttlerClient: throttlerClient,
		throttledCounts: stats.NewCounter("TestParseEventsVstreamerThrottledCounts", "Number of times vstreamer was throttled by the tablet throttler"),
		errorCounts:     stats.NewCountersWithSingleLabel("TestParseEventsVstreamerErrors", "Tracks errors in vstreamer", "type"),
	}
	vs := &vstreamer{
		filter:       &binlogdatapb.Filter{},
		throttlerApp: throttlerapp.TestingAlwaysThrottledName,
		vevents:      make(chan *localVSchema, 1),
		plans:        make(map[uint64]*streamerPlan),
		vse:          vse,
		send:         func([]*binlogdatapb.VEvent) error { return nil },
	}

	events := make(chan mysql.BinlogEvent)
	errs := make(chan error, 1)
	returned := make(chan error, 1)
	// The caller's context intentionally outlives parseEvents, like vs.ctx does
	// for the fast-forward vstreamer and the schema tracker / messager callers.
	go func() {
		returned <- vs.parseEvents(context.Background(), events, errs)
	}()

	// Let throttleEvents settle into the fully-throttled branch and arm its
	// fullyThrottledTimeout deadline.
	time.Sleep(500 * time.Millisecond)

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

	// Without the fix, the orphaned throttleEvents goroutine sends on the
	// closed throttlerErrs once fullyThrottledTimeout elapses and panics,
	// taking the test binary down. With the fix it has already exited on the
	// derived context's cancellation, so the rest of the test is uneventful.
	time.Sleep(2*fullyThrottledTimeout + 2*time.Second)
}
