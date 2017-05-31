/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package buffer

import (
	"sync"
	"time"
)

// timeoutThread captures the state of the timeout thread.
// The thread actively removes the head of the queue when that entry exceeds
// its buffering window.
// For each active failover there will be one thread (Go routine).
type timeoutThread struct {
	sb *shardBuffer
	// maxDuration enforces that a failover stops after
	// -buffer_max_failover_duration at most.
	maxDuration *time.Timer
	// stopChan will be closed when the thread should stop e.g. before the drain.
	stopChan chan struct{}
	wg       sync.WaitGroup

	// mu guards access to "queueNotEmpty" between this thread and callers of
	// notifyQueueNotEmpty().
	mu sync.Mutex
	// queueNotEmpty will be closed to notify the timeout thread when the queue
	// state changes from empty to non-empty. After it's closed, a new object will
	// be assigned to this field.
	queueNotEmpty chan struct{}
}

func newTimeoutThread(sb *shardBuffer) *timeoutThread {
	return &timeoutThread{
		sb:            sb,
		maxDuration:   time.NewTimer(*maxFailoverDuration),
		stopChan:      make(chan struct{}),
		queueNotEmpty: make(chan struct{}),
	}
}

func (tt *timeoutThread) start() {
	tt.wg.Add(1)
	go tt.run()
}

// stop stops the thread and blocks until it has exited.
func (tt *timeoutThread) stop() {
	close(tt.stopChan)
	tt.wg.Wait()
}

func (tt *timeoutThread) notifyQueueNotEmpty() {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	close(tt.queueNotEmpty)
	// Create a new channel which will be used by the next notify call.
	tt.queueNotEmpty = make(chan struct{})
}

func (tt *timeoutThread) run() {
	defer tt.wg.Done()
	defer tt.maxDuration.Stop()

	// While this thread is running, it can be in two states:
	for {
		if e := tt.sb.oldestEntry(); e != nil {
			// 1. queue not empty: Wait for the oldest entry to exceed the window.
			if stopped := tt.waitForEntry(e); stopped {
				return
			}
		} else {
			// 2. queue empty: Wait for an entry to show up.
			if stopped := tt.waitForNonEmptyQueue(); stopped {
				return
			}
		}
	}
}

// waitForEntry blocks until "e" exceeds its buffering window or buffering stops
// in general. It returns true if the timeout thread should stop.
func (tt *timeoutThread) waitForEntry(e *entry) bool {
	windowExceeded := time.NewTimer(e.deadline.Sub(time.Now()))
	defer windowExceeded.Stop()

	select {
	// a) Always check these channels, regardless of the state.
	case <-tt.maxDuration.C:
		// Max duration is up. Stop buffering. Do not error out entries explicitly.
		tt.sb.stopBufferingDueToMaxDuration()
		return true
	case <-tt.stopChan:
		// Failover ended before timeout. Do nothing.
		return true
	// b) Entry-specific checks.
	case <-e.done:
		// Entry was drained or evicted. Get the next entry.
		return false
	// NOTE: We're not waiting for e.bufferCtx here (which triggers when the
	// request was externally aborted e.g. due to context canceled) because then
	// this thread would race with the request thread which runs
	// shardBuffer.remove(). Instead, remove() will notify us here eventually by
	// closing "e.done".
	case <-windowExceeded.C:
		// Entry expired. Evict it and then get the next entry.
		tt.sb.evictOldestEntry(e)
		return false
	}
}

// waitForNonEmptyQueue blocks until the buffer queue gets a new element or
// the timeout thread should be stopped.
// It returns true if the timeout thread should stop.
func (tt *timeoutThread) waitForNonEmptyQueue() bool {
	tt.mu.Lock()
	queueNotEmpty := tt.queueNotEmpty
	tt.mu.Unlock()

	select {
	// a) Always check these channels, regardless of the state.
	case <-tt.maxDuration.C:
		// Max duration is up. Stop buffering. Do not error out entries explicitly.
		tt.sb.stopBufferingDueToMaxDuration()
		return true
	case <-tt.stopChan:
		// Failover ended before timeout. Do nothing.
		return true
	// b) State-specific check.
	case <-queueNotEmpty:
		// At least one entry present. Check its timeout in the next iteration.
		return false
	}
}
