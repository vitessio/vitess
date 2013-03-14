// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
	Package timer provides timer functionality that can be controlled
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
package timer

import (
	"sync"
	"time"

	"code.google.com/p/vitess/go/sync2"
)

// Out-of-band messages
type typeAction int

const (
	STOP typeAction = iota
	RESET
	TRIGGER
)

// Timer implements the timer functionality described above.
type Timer struct {
	interval sync2.AtomicDuration

	// state management
	mu      sync.Mutex
	running bool

	// msg is used for out-of-band messages
	msg chan typeAction
}

// Create a new Timer object
func NewTimer(interval time.Duration) *Timer {
	tm := &Timer{
		msg: make(chan typeAction),
	}
	tm.interval.Set(interval)
	return tm
}

// Start starts the timer.
func (tm *Timer) Start(keephouse func()) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.running {
		return
	}
	tm.running = true
	go tm.run(keephouse)
}

func (tm *Timer) run(keephouse func()) {
	for {
		var ch <-chan time.Time
		interval := tm.interval.Get()
		if interval <= 0 {
			ch = nil
		} else {
			ch = time.After(interval)
		}
		select {
		case action := <-tm.msg:
			switch action {
			case STOP:
				return
			case RESET:
				continue
			}
		case <-ch:
		}
		keephouse()
	}
	panic("unreachable")
}

// SetInterval changes the wait interval.
// It will cause the timer to restart the wait.
func (tm *Timer) SetInterval(ns time.Duration) {
	tm.interval.Set(ns)
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.running {
		tm.msg <- RESET
	}
}

// Trigger will cause the timer to immediately execute the keephouse function.
// It will then cause the timer to restart the wait.
func (tm *Timer) Trigger() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.running {
		tm.msg <- TRIGGER
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
	if tm.running {
		tm.msg <- STOP
		tm.running = false
	}
}
