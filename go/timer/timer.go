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

// Package timer provides various enhanced timer functions.
package timer

import (
	"sync"
	"sync/atomic"
	"time"
)

// Out-of-band messages
type typeAction int

const (
	timerStop typeAction = iota
	timerReset
	timerTrigger
)

/*
Timer provides timer functionality that can be controlled
by the user. You start the timer by providing it a callback function,
which it will call at the specified interval.

	var t = timer.NewTimer(1e9)
	t.Start(KeepHouse)

	func KeepHouse() {
		// do house keeping work
	}

You can stop the timer by calling t.Stop, which is guaranteed to
wait if KeepHouse is being executed.

You can create an untimely trigger by calling t.Trigger. You can also
schedule an untimely trigger by calling t.TriggerAfter.

The timer interval can be changed on the fly by calling t.SetInterval.
A zero value interval will cause the timer to wait indefinitely, and it
will react only to an explicit Trigger or Stop.
*/
type Timer struct {
	interval atomic.Int64

	// state management
	mu      sync.Mutex
	running atomic.Bool

	// msg is used for out-of-band messages
	msg chan typeAction
}

// NewTimer creates a new Timer object
func NewTimer(interval time.Duration) *Timer {
	tm := &Timer{
		msg: make(chan typeAction),
	}
	tm.interval.Store(interval.Nanoseconds())
	return tm
}

// Start starts the timer.
func (tm *Timer) Start(keephouse func()) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	isRunning := tm.running.Swap(true)
	if isRunning {
		return
	}
	go tm.run(keephouse)
}

func (tm *Timer) run(keephouse func()) {
	var timer *time.Timer
	for {
		var ch <-chan time.Time
		interval := tm.interval.Load()
		if interval > 0 {
			timer = time.NewTimer(time.Duration(interval))
			ch = timer.C
		}
		select {
		case action := <-tm.msg:
			if timer != nil {
				timer.Stop()
				timer = nil
			}
			switch action {
			case timerStop:
				return
			case timerReset:
				continue
			}
		case <-ch:
		}
		keephouse()
	}
}

// SetInterval changes the wait interval.
// It will cause the timer to restart the wait.
func (tm *Timer) SetInterval(ns time.Duration) {
	tm.interval.Store(ns.Nanoseconds())
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.running.Load() {
		tm.msg <- timerReset
	}
}

// Trigger will cause the timer to immediately execute the keephouse function.
// It will then cause the timer to restart the wait.
func (tm *Timer) Trigger() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.running.Load() {
		tm.msg <- timerTrigger
	}
}

// TriggerAfter waits for the specified duration and triggers the next event.
func (tm *Timer) TriggerAfter(duration time.Duration) {
	go func() {
		time.Sleep(duration)
		tm.Trigger()
	}()
}

// Stop will stop the timer. It guarantees that the timer will not execute
// any more calls to keephouse once it has returned.
func (tm *Timer) Stop() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	isRunning := tm.running.Swap(false)
	if isRunning {
		tm.msg <- timerStop
	}
}

// Interval returns the current interval.
func (tm *Timer) Interval() time.Duration {
	return time.Duration(tm.interval.Load())
}

func (tm *Timer) Running() bool {
	return tm.running.Load()
}
