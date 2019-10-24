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

package vttime

import (
	"sync"
	"time"
)

var (
	// testClock is the global test instance of the clock for tests.
	testClock *TestClock
)

// TestClock is an implementation of Clock for tests, where it is
// possible to set the current time and the uncertainty at any time.
//
// To use it:
// vttime.UseTestClock()
// vttime.SetTestClockTime(now)
// vttime.SetTestClockUncertainty(dur)
type TestClock struct {
	// mu protects the following fields
	mu          sync.Mutex
	now         time.Time
	uncertainty time.Duration
}

// Now is part of the Clock interface.
func (t *TestClock) Now() (Interval, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return NewInterval(t.now.Add(-(t.uncertainty)), t.now.Add(t.uncertainty))
}

// Set let the user set the time
func (t *TestClock) Set(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.now = now
}

// SetUncertainty lets the user set the uncertainty
func (t *TestClock) SetUncertainty(uncertainty time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.uncertainty = uncertainty
}

// SetTestClockTime sets the 'test' implementation time to the provided value.
func SetTestClockTime(now time.Time) {
	testClock.Set(now)
}

// SetTestClockUncertainty sets the 'test' implementation uncertainty
// to the provided value.
func SetTestClockUncertainty(uncertainty time.Duration) {
	testClock.SetUncertainty(uncertainty)
}

// UseTestClock is meant to be used in tests to start using the test clock.
func UseTestClock() {
	*defaultClockType = "test"
}

func init() {
	testClock = &TestClock{
		now:         time.Now(),
		uncertainty: 10 * time.Millisecond,
	}
	clockTypes["test"] = testClock
}
