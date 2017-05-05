/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vttime

import (
	"testing"
	"time"
)

func TestNewInterval(t *testing.T) {
	e := time.Now()
	l := e.Add(10 * time.Millisecond)

	// earliest < latest
	i, err := NewInterval(e, l)
	if err != nil {
		t.Errorf("unexpected error in NewInterval: %v", err)
	}
	if got := i.Earliest(); got != e {
		t.Errorf("invalid Earliest, got %v expected %v", got, e)
	}
	if got := i.Latest(); got != l {
		t.Errorf("invalid Latest, got %v expected %v", got, l)
	}

	// earliest == latest
	l = e
	if _, err := NewInterval(e, l); err != nil {
		t.Errorf("unexpected error in NewInterval(l=e): %v", err)
	}

	// earliest > latest -> error
	l = e.Add(-10 * time.Millisecond)
	if _, err := NewInterval(e, l); err == nil {
		t.Errorf("unexpected nil error in NewInterval(l<e)")
	}
}

func TestIntervalLess(t *testing.T) {
	now := time.Now()

	// i1 earlier than i2
	i1, err := NewInterval(now, now.Add(10*time.Millisecond))
	if err != nil {
		t.Fatalf("NewInterval failed: %v", err)
	}
	i2, err := NewInterval(now.Add(20*time.Millisecond), now.Add(30*time.Millisecond))
	if err != nil {
		t.Fatalf("NewInterval failed: %v", err)
	}
	if !i1.Less(i2) {
		t.Errorf("unexpected Less result for i1 earlier than i2")
	}

	// i2.earliest = i1.latest
	i2.earliest = i1.latest
	if i1.Less(i2) {
		t.Errorf("unexpected Less result for i2.earliest == i1.latest")
	}

	// overlapping
	i2.earliest = now.Add(5 * time.Millisecond)
	if i1.Less(i2) {
		t.Errorf("unexpected Less result for overlapping")
	}

	// not less, not overlapping
	i2.earliest = now.Add(-20 * time.Millisecond)
	i2.latest = now.Add(-10 * time.Millisecond)
	if i1.Less(i2) {
		t.Errorf("unexpected Less result for not less")
	}
}

func TestIntervalIsValid(t *testing.T) {
	now := time.Now()

	// valid one
	i, err := NewInterval(now, now.Add(10*time.Millisecond))
	if err != nil {
		t.Fatalf("NewInterval failed: %v", err)
	}
	if !i.IsValid() {
		t.Errorf("IsValid() should be true for good interval")
	}

	// corner case
	i.latest = i.earliest
	if !i.IsValid() {
		t.Errorf("IsValid() should be true for latest=earliest")
	}

	// invalid one
	i.latest = now.Add(-10 * time.Millisecond)
	if i.IsValid() {
		t.Errorf("IsValid() should be false for latest < earliest")
	}
}
