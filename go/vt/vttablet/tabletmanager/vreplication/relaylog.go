/*
Copyright 2019 The Vitess Authors.

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

package vreplication

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

const relayLogIOStalledMsg = "relay log I/O stalled"

type relayLog struct {
	ctx      context.Context
	maxItems int
	maxSize  int
	// lastThrottledNano is the applier's report of when it was last
	// denied by the throttler, as monotonic nanoseconds since
	// vplayerThrottleEpoch. A throttle-denied applier does not drain the
	// relay log, so a full relay log is expected backpressure then: time
	// within vplayerProgressDeadline of the last denial does not count
	// toward the stall verdict.
	lastThrottledNano *atomic.Int64

	// mu controls all variables below and is shared by canAccept and hasItems.
	// Broadcasting must be done while holding mu. This is mainly necessary because both
	// conditions depend on ctx.Done(), which can change state asynchronously.
	mu       sync.Mutex
	curSize  int
	items    [][]*binlogdatapb.VEvent
	timedout bool
	// canAccept is true if: curSize<=maxSize, len(items)<maxItems, and ctx is not Done.
	canAccept sync.Cond
	// hasItems is true if len(items)>0, ctx is not Done, and interuptFetch is false.
	hasItems sync.Cond
}

func newRelayLog(ctx context.Context, maxItems, maxSize int, lastThrottledNano *atomic.Int64) *relayLog {
	rl := &relayLog{
		ctx:               ctx,
		maxItems:          maxItems,
		maxSize:           maxSize,
		lastThrottledNano: lastThrottledNano,
	}
	rl.canAccept.L = &rl.mu
	rl.hasItems.L = &rl.mu

	// Any time context is done, wake up all waiters to make them exit.
	go func() {
		<-ctx.Done()
		rl.mu.Lock()
		defer rl.mu.Unlock()
		rl.canAccept.Broadcast()
		rl.hasItems.Broadcast()
	}()
	return rl
}

// Send writes events to the relay log
func (rl *relayLog) Send(events []*binlogdatapb.VEvent) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if err := rl.checkDone(); err != nil {
		return err
	}
	cancelTimer := rl.startSendTimer()
	defer cancelTimer()
	for rl.curSize > rl.maxSize || len(rl.items) >= rl.maxItems {
		rl.canAccept.Wait()
		if rl.timedout {
			return vterrors.Wrap(errVPlayerStalled, relayLogIOStalledMsg)
		}
		if err := rl.checkDone(); err != nil {
			return err
		}
	}
	rl.timedout = false
	rl.items = append(rl.items, events)
	rl.curSize += eventsSize(events)
	rl.hasItems.Broadcast()
	return nil
}

// Fetch returns all existing items in the relay log, and empties the log
func (rl *relayLog) Fetch() ([][]*binlogdatapb.VEvent, error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if err := rl.checkDone(); err != nil {
		return nil, err
	}
	cancelTimer := rl.startFetchTimer()
	defer cancelTimer()
	for len(rl.items) == 0 && !rl.timedout {
		rl.hasItems.Wait()
		if err := rl.checkDone(); err != nil {
			return nil, err
		}
	}
	rl.timedout = false
	items := rl.items
	rl.items = nil
	rl.curSize = 0
	rl.canAccept.Broadcast()
	return items, nil
}

func (rl *relayLog) checkDone() error {
	select {
	case <-rl.ctx.Done():
		return io.EOF
	default:
	}
	return nil
}

// startSendTimer starts a timer that will wake up the sender if we hit
// the vplayerProgressDeadline timeout. This ensures that we don't
// block forever if the vplayer cannot process the previous relay log
// contents in a timely manner; allowing us to provide the user with a
// helpful error message.
// A throttle-denied applier deliberately does not drain the relay log,
// so time near a throttler denial does not count toward the deadline:
// the timer defers the stall verdict until a full vplayerProgressDeadline
// has elapsed since the applier was last denied.
func (rl *relayLog) startSendTimer() (cancel func()) {
	// Capture the deadline once, on the caller's goroutine: the timer
	// goroutine below must not read the package var again, as tests
	// mutate it.
	deadline := vplayerProgressDeadline
	timer := time.NewTimer(deadline)
	timerDone := make(chan struct{})
	go func() {
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if deferral := rl.stallDeferral(deadline); deferral > 0 {
					timer.Reset(deferral)
					continue
				}
				rl.mu.Lock()
				rl.timedout = true
				rl.canAccept.Broadcast()
				rl.mu.Unlock()
				return
			case <-timerDone:
				return
			}
		}
	}()
	return func() {
		close(timerDone)
	}
}

// stallDeferral returns how long the stall verdict must be deferred so
// that only un-throttled time counts toward it: the remainder of a full
// deadline since the applier was last denied by the throttler. Both
// sides of the comparison are monotonic readings against
// vplayerThrottleEpoch, so wall clock steps cannot skew the verdict in
// either direction. Zero means the stall verdict is due.
func (rl *relayLog) stallDeferral(deadline time.Duration) time.Duration {
	if rl.lastThrottledNano == nil {
		return 0
	}
	last := rl.lastThrottledNano.Load()
	if last == 0 {
		return 0
	}
	since := time.Since(vplayerThrottleEpoch) - time.Duration(last)
	if since >= deadline {
		return 0
	}
	return min(deadline-since, deadline)
}

// startFetchTimer starts a timer that will wake up the fetcher after
// idleTimeout to be sure that we're regularly checking for new events.
func (rl *relayLog) startFetchTimer() (cancel func()) {
	timer := time.NewTimer(idleTimeout)
	timerDone := make(chan struct{})
	go func() {
		select {
		case <-timer.C:
			rl.mu.Lock()
			defer rl.mu.Unlock()
			rl.timedout = true
			rl.hasItems.Broadcast()
		case <-timerDone:
		}
	}()
	return func() {
		timer.Stop()
		close(timerDone)
	}
}

func eventsSize(events []*binlogdatapb.VEvent) int {
	size := 0
	for _, event := range events {
		if event.Type != binlogdatapb.VEventType_ROW {
			continue
		}
		for _, rowChange := range event.RowEvent.RowChanges {
			if rowChange.Before != nil {
				size += len(rowChange.Before.Values)
			}
			if rowChange.After != nil {
				size += len(rowChange.After.Values)
			}
		}
	}
	return size
}
