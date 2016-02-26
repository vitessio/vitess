// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"expvar"
	"testing"
	"time"
)

// For tests, we want to control exactly the time used by Rates.
// The way Rates works is:
// - at creation, do a snapshot.
// - every interval, do a snapshot.
// So in these tests, we make sure to always call snapshot() every interval.
// We do other actions after epsilon, but then wait for intervalMinusEpsilon
// and call snapshot().
const (
	interval             = 1 * time.Second
	epsilon              = 50 * time.Millisecond
	intervalMinusEpsilon = interval - epsilon
)

func TestRates(t *testing.T) {
	now := time.Now()
	timeNow = func() time.Time {
		return now
	}

	clear()
	c := NewCounters("rcounter1")
	r := NewRates("rates1", c, 3, -1*time.Second)
	r.snapshot()
	now = now.Add(epsilon)
	c.Add("tag1", 0)
	c.Add("tag2", 0)
	now = now.Add(intervalMinusEpsilon)
	r.snapshot()
	now = now.Add(epsilon)
	checkRates(t, r, "after 1s", 0.0, `{"tag1":[0],"tag2":[0]}`)

	c.Add("tag1", 10)
	c.Add("tag2", 20)
	now = now.Add(intervalMinusEpsilon)
	r.snapshot()
	now = now.Add(epsilon)
	checkRates(t, r, "after 2s", 30.0, `{"tag1":[0,10],"tag2":[0,20]}`)

	now = now.Add(intervalMinusEpsilon)
	r.snapshot()
	now = now.Add(epsilon)
	checkRates(t, r, "after 3s", 0.0, `{"tag1":[0,10,0],"tag2":[0,20,0]}`)

	now = now.Add(intervalMinusEpsilon)
	r.snapshot()
	now = now.Add(epsilon)
	checkRates(t, r, "after 4s", 0.0, `{"tag1":[10,0,0],"tag2":[20,0,0]}`)
}

func checkRates(t *testing.T, r *Rates, desc string, wantRate float64, wantRateMap string) {
	if got := r.String(); got != wantRateMap {
		t.Errorf("%v: want %s, got %s", desc, wantRateMap, got)
	}
	if got := r.TotalRate(); got != wantRate {
		t.Errorf("%v: want rate %v, got rate %v", desc, wantRate, got)
	}
}

func TestRatesConsistency(t *testing.T) {
	now := time.Now()
	timeNow = func() time.Time {
		return now
	}

	// This tests the following invariant: in the time window
	// covered by rates, the sum of the rates reported must be
	// equal to the count reported by the counter.
	clear()
	c := NewCounters("rcounter4")
	r := NewRates("rates4", c, 100, -1*time.Second)
	r.snapshot()

	now = now.Add(epsilon)
	c.Add("a", 1000)
	now = now.Add(intervalMinusEpsilon)
	r.snapshot()
	now = now.Add(epsilon)
	c.Add("a", 1)
	now = now.Add(intervalMinusEpsilon)
	r.snapshot()
	now = now.Add(epsilon)

	result := r.Get()
	counts := c.Counts()
	t.Logf("r.Get(): %v", result)
	t.Logf("c.Counts(): %v", counts)

	rate, count := result["a"], counts["a"]

	var sum float64
	for _, v := range rate {
		sum += v
	}
	if sum != float64(counts["a"]) {
		t.Errorf("rate inconsistent with count: sum of %v != %v", rate, count)
	}

}

func TestRatesHook(t *testing.T) {
	clear()
	c := NewCounters("rcounter2")
	var gotname string
	var gotv *Rates
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Rates)
	})

	v := NewRates("rates2", c, 2, 10*time.Second)
	if gotname != "rates2" {
		t.Errorf("want rates2, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}
