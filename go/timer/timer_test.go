// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package timer

import (
	"testing"
	"time"
)

const (
	one     = time.Duration(1e9)
	half    = time.Duration(500e6)
	quarter = time.Duration(250e6)
	tenth   = time.Duration(100e6)
)

func TestWait(t *testing.T) {
	start := time.Now()
	timer := NewTimer(quarter)
	timer.Start()
	result := timer.Next()
	if !result {
		t.Errorf("Want true, got false")
	}
	if start.Add(quarter).After(time.Now()) {
		t.Error("Next returned too soon")
	}
}

func TestReset(t *testing.T) {
	start := time.Now()
	timer := NewTimer(quarter)
	timer.Start()
	ch := next(timer)
	timer.SetInterval(tenth)
	result := <-ch
	if !result {
		t.Errorf("Want true, got false")
	}
	if start.Add(tenth).After(time.Now()) {
		t.Error("Next returned too soon")
	}
	if start.Add(quarter).Before(time.Now()) {
		t.Error("Next returned too late")
	}
}

func TestIndefinite(t *testing.T) {
	start := time.Now()
	timer := NewTimer(0)
	timer.Start()
	ch := next(timer)
	timer.TriggerAfter(quarter)
	result := <-ch
	if !result {
		t.Errorf("Want true, got false")
	}
	if start.Add(quarter).After(time.Now()) {
		t.Error("Next returned too soon")
	}
}

func TestClose(t *testing.T) {
	start := time.Now()
	timer := NewTimer(0)

	// Should return false if Start was not called
	ch := next(timer)
	result := <-ch
	if result {
		t.Errorf("Want false, got true")
	}

	timer.Start()
	ch = next(timer)
	timer.Close()
	result = <-ch
	if result {
		t.Errorf("Want false, got true")
	}
	if start.Add(tenth).Before(time.Now()) {
		t.Error("Next returned too late")
	}

	// Should return false after Close
	ch = next(timer)
	result = <-ch
	if result {
		t.Errorf("Want false, got true")
	}
}

func next(timer *Timer) chan bool {
	ch := make(chan bool)
	go func() {
		ch <- timer.Next()
	}()
	return ch
}
