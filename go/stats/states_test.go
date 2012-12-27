// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"testing"
	"time"
)

func TestOneState(t *testing.T) {

	location, _ := time.LoadLocation("") // UTC

	// 2012/12/12 12:00:00 UTC
	now := time.Date(2012, 12, 12, 12, 0, 0, 0, location)
	s := NewStates("", []string{"Init", "Open", "Closed"}, now, 0)

	// get the value a bit later
	now = time.Date(2012, 12, 12, 12, 0, 25, 0, location)
	result := s.StringAt(now)
	if result != "{\"Current\": \"Init\", \"DurationInit\": 25000000000, \"TransitionIntoInit\": 0, \"DurationOpen\": 0, \"TransitionIntoOpen\": 0, \"DurationClosed\": 0, \"TransitionIntoClosed\": 0}" {
		t.Errorf("unexpected result: %v", result)
	}

	// get the value a bit later again
	now = time.Date(2012, 12, 12, 12, 10, 0, 0, location)
	result = s.StringAt(now)
	if result != "{\"Current\": \"Init\", \"DurationInit\": 600000000000, \"TransitionIntoInit\": 0, \"DurationOpen\": 0, \"TransitionIntoOpen\": 0, \"DurationClosed\": 0, \"TransitionIntoClosed\": 0}" {
		t.Errorf("unexpected result: %v", result)
	}

	// get the value a bit later again
	now = time.Date(2012, 12, 12, 12, 11, 0, 0, location)
	result = s.StringAt(now)
	if result != "{\"Current\": \"Init\", \"DurationInit\": 660000000000, \"TransitionIntoInit\": 0, \"DurationOpen\": 0, \"TransitionIntoOpen\": 0, \"DurationClosed\": 0, \"TransitionIntoClosed\": 0}" {
		t.Errorf("unexpected result: %v", result)
	}
}

func TestTransitions(t *testing.T) {

	location, _ := time.LoadLocation("") // UTC

	// 2012/12/12 12:00:00 UTC
	now := time.Date(2012, 12, 12, 12, 0, 0, 0, location)
	s := NewStates("", []string{"Init", "Open", "Closed"}, now, 0)

	// get the value a bit later
	now = time.Date(2012, 12, 12, 12, 0, 25, 0, location)
	result := s.StringAt(now)
	if result != "{\"Current\": \"Init\", \"DurationInit\": 25000000000, \"TransitionIntoInit\": 0, \"DurationOpen\": 0, \"TransitionIntoOpen\": 0, \"DurationClosed\": 0, \"TransitionIntoClosed\": 0}" {
		t.Errorf("unexpected result: %v", result)
	}

	// now transition to a new state
	now = time.Date(2012, 12, 12, 12, 0, 30, 0, location)
	s.SetStateAt(1, now)

	// and ask for current status a bit later
	now = time.Date(2012, 12, 12, 12, 1, 0, 0, location)
	result = s.StringAt(now)
	if result != "{\"Current\": \"Open\", \"DurationInit\": 30000000000, \"TransitionIntoInit\": 0, \"DurationOpen\": 30000000000, \"TransitionIntoOpen\": 1, \"DurationClosed\": 0, \"TransitionIntoClosed\": 0}" {
		t.Errorf("unexpected result: %v", result)
	}

	// and ask again for current status a bit later
	now = time.Date(2012, 12, 12, 12, 2, 0, 0, location)
	result = s.StringAt(now)
	if result != "{\"Current\": \"Open\", \"DurationInit\": 30000000000, \"TransitionIntoInit\": 0, \"DurationOpen\": 90000000000, \"TransitionIntoOpen\": 1, \"DurationClosed\": 0, \"TransitionIntoClosed\": 0}" {
		t.Errorf("unexpected result: %v", result)
	}
}
