// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"bytes"
	"expvar"
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
	StateCount int
	Labels     []string

	// the following variables can change, protected by mutex
	mu                    sync.Mutex
	CurrentState          int
	CurrentStateStartTime time.Time // when we switched to our state

	// historical data about the states
	Durations   []time.Duration // how much time in each state
	Transitions []int           // how many times we got into a state
}

func NewStates(name string, labels []string, startTime time.Time, initialState int) *States {
	s := &States{StateCount: len(labels), Labels: labels, CurrentState: initialState, CurrentStateStartTime: startTime, Durations: make([]time.Duration, len(labels)), Transitions: make([]int, len(labels))}
	if initialState < 0 || initialState >= s.StateCount {
		panic(fmt.Errorf("initialState out of range 0-%v: %v", s.StateCount, initialState))
	}
	if name != "" {
		expvar.Publish(name, s)
	}
	return s
}

func (s *States) SetState(state int) {
	s.SetStateAt(state, time.Now())
}

// now has to be increasing, or we panic. Usually, only one execution
// thread can change a state, and therefore just using time.now()
// will be enough
func (s *States) SetStateAt(state int, now time.Time) {
	if state < 0 || state >= s.StateCount {
		panic(fmt.Errorf("State out of range 0-%v: %v", s.StateCount, state))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// check we're going strictly forward in time
	dur := now.Sub(s.CurrentStateStartTime)
	if dur < 0 {
		panic(fmt.Errorf("Time going backwards? %v < %v", now, s.CurrentStateStartTime))
	}

	// record the previous state duration, reset our state
	s.Durations[s.CurrentState] += dur
	s.Transitions[state] += 1
	s.CurrentState = state
	s.CurrentStateStartTime = now
}

// expvar.Var interface
// just call StringAt(now)
func (s *States) String() string {
	return s.StringAt(time.Now())
}

func (s *States) StringAt(now time.Time) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")

	// report our current state
	fmt.Fprintf(b, "\"Current\": \"%v\"", s.Labels[s.CurrentState])

	// report the total durations
	for i := 0; i < s.StateCount; i++ {

		d := s.Durations[i]
		t := s.Transitions[i]
		if i == s.CurrentState {
			dur := now.Sub(s.CurrentStateStartTime)
			if dur > 0 {
				// we don't panic if now is not growing,
				// as it can happen in some corner cases
				// (SetState called right before the beginning
				// of StringAt by another execution thread)
				d += dur
			}
		}

		fmt.Fprintf(b, ", ")
		fmt.Fprintf(b, "\"Duration%v\": %v, ", s.Labels[i], int64(d))
		fmt.Fprintf(b, "\"TransitionInto%v\": %v", s.Labels[i], t)
	}

	fmt.Fprintf(b, "}")
	return b.String()
}
