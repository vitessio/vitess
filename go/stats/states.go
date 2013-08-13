// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

// The States structure keeps historical data about a state machine
// state, and exports them using expvar:
// - our current state
// - how long we have been in each state
// - how many times we transitioned into a state (not counting initial state)
type States struct {
	// set at construction time
	labels []string

	// the following variables can change, protected by mutex
	mu    sync.Mutex
	state int64
	since time.Time // when we switched to our state

	// historical data about the states
	durations   []time.Duration // how much time in each state
	transitions []int64         // how many times we got into a state
}

// NewStates creates a states tracker.
// If name is empty, the variable is not published.
func NewStates(name string, labels []string, startTime time.Time, initialState int64) *States {
	s := &States{labels: labels, state: initialState, since: startTime, durations: make([]time.Duration, len(labels)), transitions: make([]int64, len(labels))}
	if initialState < 0 || initialState >= int64(len(s.labels)) {
		panic(fmt.Errorf("initialState out of range 0-%v: %v", len(s.labels), initialState))
	}
	if name != "" {
		Publish(name, s)
	}
	return s
}

func (s *States) SetState(state int64) {
	s.setStateAt(state, time.Now())
}

// now has to be increasing, or we panic. Usually, only one execution
// thread can change a state, and therefore just using time.now()
// will be enough
func (s *States) setStateAt(state int64, now time.Time) {
	if state < 0 || state >= int64(len(s.labels)) {
		panic(fmt.Errorf("State out of range 0-%v: %v", len(s.labels), state))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// check we're going strictly forward in time
	dur := now.Sub(s.since)
	if dur < 0 {
		panic(fmt.Errorf("Time going backwards? %v < %v", now, s.since))
	}

	// record the previous state duration, reset our state
	s.durations[s.state] += dur
	s.transitions[state] += 1
	s.state = state
	s.since = now
}

// Get returns the current state.
func (s *States) Get() (state int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *States) String() string {
	return s.stringAt(time.Now())
}

func (s *States) stringAt(now time.Time) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")

	// report our current state
	fmt.Fprintf(b, "\"Current\": \"%v\"", s.labels[s.state])

	// report the total durations
	for i := 0; i < len(s.labels); i++ {

		d := s.durations[i]
		t := s.transitions[i]
		if int64(i) == s.state {
			dur := now.Sub(s.since)
			if dur > 0 {
				// we don't panic if now is not growing,
				// as it can happen in some corner cases
				// (SetState called right before the beginning
				// of StringAt by another execution thread)
				d += dur
			}
		}

		fmt.Fprintf(b, ", ")
		fmt.Fprintf(b, "\"Duration%v\": %v, ", s.labels[i], int64(d))
		fmt.Fprintf(b, "\"TransitionInto%v\": %v", s.labels[i], t)
	}

	fmt.Fprintf(b, "}")
	return b.String()
}
