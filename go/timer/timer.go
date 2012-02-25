/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

/*
  Package timer provides timer functionality that can be controlled
  by the user. It is typically meant to be used in a for loop of a goroutine
  that would wait on the response of the Next function:
  Here is an example:

    var t = timer.NewTimer(1e9)

    func KeepHouse() {
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

// Next starts the timer and waits for the next tick.
// It will return true upon the next tick or on an explicit trigger.
// It will return false if Close was called.
func (self *Timer) Next() bool {
	for {
		var ch <-chan time.Time
		if self.interval <= 0 {
			ch = nil
		} else {
			ch = time.After(self.interval)
		}
		select {
		case action := <-self.msg:
			switch action {
			case CLOSE:
				self.resp <- CLOSE
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
func (self *Timer) SetInterval(ns time.Duration) {
	self.interval = ns
	self.msg <- RESET
}

// Trigger will cause the currently executing, or a subsequent call to Next()
// to immediately return true.
func (self *Timer) Trigger() {
	self.msg <- FORCE
}

// Trigger will wait ns nanoseconds before triggering Next().
func (self *Timer) TriggerAfter(ns time.Duration) {
	go func() {
		<-time.After(ns)
		self.Trigger()
	}()
}

// Close will cause the the currently executing, or a subsequent call to Next()
// to immediately return false. Close will not return until the message is
// successfully delivered.
func (self *Timer) Close() {
	self.msg <- CLOSE
	<-self.resp
}
