// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import "testing"

func TestMemory(t *testing.T) {
	m := newMemory(5)
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
	m.markBad(301)
	if got := m.lowestBad(); got != want300 {
		t.Fatalf("bad rate was not recorded: got = %v, want = %v", got, want300)
	}
	if got := m.highestGood(); got != want200 {
		t.Fatalf("new lower bad rate did not invalidate previous good rates: got = %v, want = %v", got, want200)
	}

	m.markBad(311)
	if got := m.lowestBad(); got != want300 {
		t.Fatalf("bad rates higher than the current one should be ignored: got = %v, want = %v", got, want300)
	}

	// a good 601 will be ignored because the first bad is at 300.
	m.markGood(601)
	if got := m.lowestBad(); got != want300 {
		t.Fatalf("good rates cannot go beyond the lowest bad rate: got = %v, want = %v", got, want300)
	}
	if got := m.highestGood(); got != want200 {
		t.Fatalf("good rates beyond the lowest bad rate must be ignored: got = %v, want = %v", got, want200)
	}

	m.markBad(201)
	if got := m.lowestBad(); got != want200 {
		t.Fatalf("bad rate was not updated: got = %v, want = %v", got, want200)
	}
	want100 := int64(100)
	if got := m.highestGood(); got != want100 {
		t.Fatalf("previous highest good rate was not marked as bad: got = %v, want = %v", got, want100)
	}
}

func TestMemory_markDownIgnoresDrasticBadValues(t *testing.T) {
	m := newMemory(1)
	good := int64(1000)
	bad := int64(1001)
	m.markGood(good)
	m.markBad(bad)
	if got := m.highestGood(); got != good {
		t.Fatalf("good rate was not correctly inserted: got = %v, want = %v", got, good)
	}
	if got := m.lowestBad(); got != bad {
		t.Fatalf("bad rate was not correctly inserted: got = %v, want = %v", got, bad)
	}

	m.markBad(500)
	if got := m.highestGood(); got != good {
		t.Fatalf("bad rate should have been ignored: got = %v, want = %v", got, good)
	}
	if got := m.lowestBad(); got != bad {
		t.Fatalf("bad rate should have been ignored: got = %v, want = %v", got, bad)
	}
}
