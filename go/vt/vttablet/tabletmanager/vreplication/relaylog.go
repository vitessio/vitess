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
	"io"
	"sync"
	"time"

	"golang.org/x/net/context"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
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
}

func newRelayLog(ctx context.Context, maxItems, maxSize int) *relayLog {
	rl := &relayLog{
		ctx:      ctx,
		maxItems: maxItems,
		maxSize:  maxSize,
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

func (rl *relayLog) Send(events []*binlogdatapb.VEvent) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if err := rl.checkDone(); err != nil {
		return err
	}
	for rl.curSize > rl.maxSize || len(rl.items) >= rl.maxItems {
		rl.canAccept.Wait()
		if err := rl.checkDone(); err != nil {
			return err
		}
	}
	rl.items = append(rl.items, events)
	rl.curSize += eventsSize(events)
	rl.hasItems.Broadcast()
	return nil
}

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
