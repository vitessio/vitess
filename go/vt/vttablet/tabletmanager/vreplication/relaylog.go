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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"context"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	// At what point should we consider IO to the relay log to be stalled and return
	// an error. This stall can happen e.g. if vplayer is stuck in a loop trying to
	// process the previous relay log contents. This can happen e.g. if the queries
	// it's executing are doing table scans and it thus cannot complete the transaction
	// wrapping the previous log contents -- which includes updating the pos field in
	// the vreplication record to mark the new GTIDs that we've replicated in this
	// latest batch -- thus eventually blocking further relay log writes once we've
	// hit the configured max relay log items / size until the binlog dump connection
	// gets terminated by mysqld due to hitting replica_net_timeout as once the relay
	// log is full we cannot perform any more reads from the mysqld binlog stream.
	relayLogProgressTimeout = 5 * time.Minute

	// The error to return when we haven't made progress for the timeout.
	ErrRelayLogTimeout = fmt.Errorf("relay log progress stalled; vplayer was likely unable to replicate the previous log content's transaction in a timely manner; examine the target mysqld instance health and the replicated queries' EXPLAIN output to see why queries are taking unusually long")
)

type relayLog struct {
	ctx      context.Context
	maxItems int
	maxSize  int

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

	// progressTimer is reset every time that we've made progress with the relay log. If
	// this hits the progressTimeout then we end the relay log work and return an error.
	progressTimer *time.Timer
	// lastError is set if the we encountered an error that should end the relay log work.
	lastError atomic.Pointer[error]
}

func newRelayLog(ctx context.Context, maxItems, maxSize int) *relayLog {
	rl := &relayLog{
		ctx:      ctx,
		maxItems: maxItems,
		maxSize:  maxSize,
	}
	rl.canAccept.L = &rl.mu
	rl.hasItems.L = &rl.mu

	rl.progressTimer = time.NewTimer(relayLogProgressTimeout)

	// Any time the context is done or progress has stalled, wake up all waiters to
	// make them exit.
	go func() {
		select {
		case <-ctx.Done():
		case <-rl.progressTimer.C:
			rl.lastError.Store(&ErrRelayLogTimeout)
		}
		rl.mu.Lock()
		defer rl.mu.Unlock()
		rl.canAccept.Broadcast()
		rl.hasItems.Broadcast()
	}()
	return rl
}

// Send writes events to the relay log.
func (rl *relayLog) Send(events []*binlogdatapb.VEvent) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if err := rl.checkDone(); err != nil {
		return err
	}
	for rl.curSize > rl.maxSize || len(rl.items) >= rl.maxItems {
		rl.canAccept.Wait()
		rl.progressTimer.Reset(relayLogProgressTimeout)
		// See if we should exit.
		if err := rl.checkDone(); err != nil {
			return err
		}
	}
	rl.items = append(rl.items, events)
	rl.curSize += eventsSize(events)
	rl.hasItems.Broadcast()
	return nil
}

// Fetch returns all existing items in the relay log, and empties the log.
func (rl *relayLog) Fetch() ([][]*binlogdatapb.VEvent, error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if err := rl.checkDone(); err != nil {
		return nil, err
	}
	cancelTimer := rl.startTimer()
	defer cancelTimer()
	for len(rl.items) == 0 && !rl.timedout {
		rl.hasItems.Wait()
		// See if we should exit.
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

// checkDone checks to see if we've encounterd a fatal error and should thus end our
// work and return the error back to the vplayer.
func (rl *relayLog) checkDone() error {
	select {
	case <-rl.ctx.Done():
		return io.EOF
	default:
		if rl.lastError.Load() != nil {
			return *rl.lastError.Load()
		}
	}
	return nil
}

func (rl *relayLog) startTimer() (cancel func()) {
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
