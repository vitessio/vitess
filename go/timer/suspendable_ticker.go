/*
Copyright 2020 The Vitess Authors.

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

package timer

import (
	"sync/atomic"
	"time"
)

// SuspendableTicker is similar to time.Ticker, but also offers Suspend() and Resume() functions.
// While the ticker is suspended, nothing comes from the time channel C
type SuspendableTicker struct {
	ticker *time.Ticker
	// C is user facing
	C chan time.Time

	suspended atomic.Bool
}

// NewSuspendableTicker creates a new suspendable ticker, indicating whether the ticker should start
// suspendable or running
func NewSuspendableTicker(d time.Duration, initiallySuspended bool) *SuspendableTicker {
	s := &SuspendableTicker{
		ticker: time.NewTicker(d),
		C:      make(chan time.Time),
	}
	if initiallySuspended {
		s.suspended.Store(true)
	}
	go s.loop()
	return s
}

// Suspend stops sending time events on the channel C
// time events sent during suspended time are lost
func (s *SuspendableTicker) Suspend() {
	s.suspended.Store(true)
}

// Resume re-enables time events on channel C
func (s *SuspendableTicker) Resume() {
	s.suspended.Store(false)
}

// Stop completely stops the timer, like time.Timer
func (s *SuspendableTicker) Stop() {
	s.ticker.Stop()
}

// TickNow generates a tick at this point in time. It may block
// if nothing consumes the tick.
func (s *SuspendableTicker) TickNow() {
	if !s.suspended.Load() {
		// not suspended
		s.C <- time.Now()
	}
}

// TickAfter generates a tick after given duration has passed.
// It runs asynchronously and returns immediately.
func (s *SuspendableTicker) TickAfter(d time.Duration) {
	time.AfterFunc(d, func() {
		s.TickNow()
	})
}

func (s *SuspendableTicker) loop() {
	for t := range s.ticker.C {
		if !s.suspended.Load() {
			// not suspended
			s.C <- t
		}
	}
}
