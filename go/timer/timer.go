// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
  Package timer provides timer functionality that can be controlled
  by the user. It is typically meant to be used in a for loop of a goroutine
  that would wait on the response of the Next function:
  Here is an example:

    var t = timer.NewTimer(1e9)

    func KeepHouse() {
      t.Start()
      for t.Next() {
        // do house keeping work
      }
    }

  KeepHouse is expected to run as a goroutine. To cause KeepHouse to
  terminate, you can call t.Close(), which will make the next call
  to t.Next() return false. The Close() function will return only
  after successfully delivering the message to t.Next()

  The timer interval can be changed on the fly by calling t.SetInterval().
  A zero value interval will cause t.Next() to wait indefinitely, and will
  return only from an explicit trigger.

  You can create an untimely trigger by calling t.Trigger(). You can also
  schedule an untimely trigger by calling t.TriggerAfter().
*/
package timer

import (
	"time"
)

type typeAction int

const (
	CLOSE typeAction = iota
	RESET
	FORCE
)

// Timer implements the Next() function whose basic functionality
// is to return true after waiting for the specified number of nanoseconds.
type Timer struct {
	interval  time.Duration
	running   bool
	msg, resp chan typeAction
}

// Create a new Timer object. An intervalNs specifies the length of time
// the Next() function has to wait before returning. A value of 0 will cause
// it to wait indefinitely
func NewTimer(interval time.Duration) *Timer {
	return &Timer{
		interval: interval,
		msg:      make(chan typeAction, 1),
		resp:     make(chan typeAction, 1),
	}
}

// Start must be called before iterating on Next.
func (tm *Timer) Start() {
	tm.running = true
}

// Next starts the timer and waits for the next tick.
// It will return true upon the next tick or on an explicit trigger.
// It will return false if Close was called.
func (tm *Timer) Next() bool {
	if !tm.running {
		return false
	}
	// loop needed to handle RESET message
	for {
		var ch <-chan time.Time
		if tm.interval <= 0 {
			ch = nil
		} else {
			ch = time.After(tm.interval)
		}
		select {
		case action := <-tm.msg:
			switch action {
			case CLOSE:
				tm.resp <- CLOSE
				return false
			case FORCE:
				return true
			}
		case <-ch:
			return true
		}
	}
	panic("unreachable")
}

// SetInterval changes the wait interval for the Next() function.
// It will cause the function to restart the wait if it's already executing.
func (tm *Timer) SetInterval(ns time.Duration) {
	tm.interval = ns
	if tm.running {
		tm.msg <- RESET
	}
}

// Trigger will cause the currently executing, or a subsequent call to Next()
// to immediately return true.
func (tm *Timer) Trigger() {
	if tm.running {
		tm.msg <- FORCE
	}
}

// Trigger will wait ns nanoseconds before triggering Next().
func (tm *Timer) TriggerAfter(ns time.Duration) {
	go func() {
		<-time.After(ns)
		tm.Trigger()
	}()
}

// Close will cause the currently executing, or a subsequent call to Next
// to immediately return false. Close will not return until the message is
// successfully delivered. To resume timer activities, you must call Start again.
func (tm *Timer) Close() {
	if !tm.running {
		return
	}
	tm.msg <- CLOSE
	<-tm.resp
	tm.running = false
}
