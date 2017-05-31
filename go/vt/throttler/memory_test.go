/*
Copyright 2017 Google Inc.

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

package throttler

import (
	"testing"
	"time"
)

func TestMemory(t *testing.T) {
	m := newMemory(5, 1*time.Second, 0.10)
	// Add several good rates.
	m.markGood(201)
	want200 := int64(200)
	if got := m.highestGood(); got != want200 {
		t.Fatalf("memory with one good entry: got = %v, want = %v", got, want200)
	}
	m.markGood(101)
	if got := m.highestGood(); got != want200 {
		t.Fatalf("wrong order within memory: got = %v, want = %v", got, want200)
	}
	m.markGood(301)
	want300 := int64(300)
	if got := m.highestGood(); got != want300 {
		t.Fatalf("wrong order within memory: got = %v, want = %v", got, want300)
	}
	m.markGood(306)
	want305 := int64(305)
	if got := m.highestGood(); got != want305 {
		t.Fatalf("wrong order within memory: got = %v, want = %v", got, want305)
	}

	// 300 and 305 will turn from good to bad.
	if got := m.lowestBad(); got != 0 {
		t.Fatalf("lowestBad should return zero value when no bad rate is recorded yet: got = %v", got)
	}
	m.markBad(300, sinceZero(0))
	if got, want := m.lowestBad(), want300; got != want {
		t.Fatalf("bad rate was not recorded: got = %v, want = %v", got, want)
	}
	if got := m.highestGood(); got != want200 {
		t.Fatalf("new lower bad rate did not invalidate previous good rates: got = %v, want = %v", got, want200)
	}

	m.markBad(311, sinceZero(0))
	if got := m.lowestBad(); got != want300 {
		t.Fatalf("bad rates higher than the current one should be ignored: got = %v, want = %v", got, want300)
	}

	// a good 601 will be ignored because the first bad is at 300.
	if err := m.markGood(601); err == nil {
		t.Fatal("good rates cannot go beyond the lowest bad rate: should have returned an error")
	}
	if got := m.lowestBad(); got != want300 {
		t.Fatalf("good rates cannot go beyond the lowest bad rate: got = %v, want = %v", got, want300)
	}
	if got := m.highestGood(); got != want200 {
		t.Fatalf("good rates beyond the lowest bad rate must be ignored: got = %v, want = %v", got, want200)
	}

	// 199 will be rounded up to 200.
	m.markBad(199, sinceZero(0))
	if got := m.lowestBad(); got != want200 {
		t.Fatalf("bad rate was not updated: got = %v, want = %v", got, want200)
	}
	want100 := int64(100)
	if got := m.highestGood(); got != want100 {
		t.Fatalf("previous highest good rate was not marked as bad: got = %v, want = %v", got, want100)
	}
}

func TestMemory_markDownIgnoresDrasticBadValues(t *testing.T) {
	m := newMemory(1, 1*time.Second, 0.10)
	good := int64(1000)
	bad := int64(1001)
	m.markGood(good)
	m.markBad(bad, sinceZero(0))
	if got := m.highestGood(); got != good {
		t.Fatalf("good rate was not correctly inserted: got = %v, want = %v", got, good)
	}
	if got := m.lowestBad(); got != bad {
		t.Fatalf("bad rate was not correctly inserted: got = %v, want = %v", got, bad)
	}

	if err := m.markBad(500, sinceZero(0)); err == nil {
		t.Fatal("bad rate should have been ignored and an error should have been returned")
	}
	if got := m.highestGood(); got != good {
		t.Fatalf("bad rate should have been ignored: got = %v, want = %v", got, good)
	}
	if got := m.lowestBad(); got != bad {
		t.Fatalf("bad rate should have been ignored: got = %v, want = %v", got, bad)
	}
}

func TestMemory_Aging(t *testing.T) {
	m := newMemory(1, 2*time.Second, 0.10)

	m.markBad(100, sinceZero(0))
	if got, want := m.lowestBad(), int64(100); got != want {
		t.Fatalf("bad rate was not correctly inserted: got = %v, want = %v", got, want)
	}

	// Bad rate successfully ages by 10%.
	m.ageBadRate(sinceZero(2 * time.Second))
	if got, want := m.lowestBad(), int64(110); got != want {
		t.Fatalf("bad rate should have been increased due to its age: got = %v, want = %v", got, want)
	}

	// A recent aging resets the age timer.
	m.ageBadRate(sinceZero(2 * time.Second))
	if got, want := m.lowestBad(), int64(110); got != want {
		t.Fatalf("a bad rate should not age again until the age is up again: got = %v, want = %v", got, want)
	}

	// The age timer will be reset if the bad rate changes.
	m.markBad(100, sinceZero(3*time.Second))
	m.ageBadRate(sinceZero(4 * time.Second))
	if got, want := m.lowestBad(), int64(100); got != want {
		t.Fatalf("bad rate must not age yet: got = %v, want = %v", got, want)
	}

	// The age timer won't be reset when the rate stays the same.
	m.markBad(100, sinceZero(4*time.Second))
	m.ageBadRate(sinceZero(5 * time.Second))
	if got, want := m.lowestBad(), int64(110); got != want {
		t.Fatalf("bad rate should have aged again: got = %v, want = %v", got, want)
	}

	// Update the aging config. It will be effective immediately.
	m.updateAgingConfiguration(1*time.Second, 0.05)
	m.ageBadRate(sinceZero(6 * time.Second))
	if got, want := m.lowestBad(), int64(115); got != want {
		t.Fatalf("bad rate should have aged after the configuration update: got = %v, want = %v", got, want)
	}

	// If the new bad rate is not higher, it should increase by the memory granularity at least.
	m.markBad(5, sinceZero(10*time.Second))
	m.ageBadRate(sinceZero(11 * time.Second))
	if got, want := m.lowestBad(), int64(5+memoryGranularity); got != want {
		t.Fatalf("bad rate should have aged after the configuration update: got = %v, want = %v", got, want)
	}
}
