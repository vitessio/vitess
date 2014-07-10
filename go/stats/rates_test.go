// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"expvar"
	"testing"
	"time"
)

func TestRates(t *testing.T) {
	clear()
	c := NewCounters("rcounter1")
	r := NewRates("rates1", c, 3, 1*time.Second)
	time.Sleep(50 * time.Millisecond)
	c.Add("tag1", 0)
	c.Add("tag2", 0)
	time.Sleep(1 * time.Second)
	want := `{"tag1":[0],"tag2":[0]}`
	if r.String() != want {
		t.Errorf("want %s, got %s", want, r.String())
	}
	c.Add("tag1", 10)
	c.Add("tag2", 20)
	time.Sleep(1 * time.Second)
	want = `{"tag1":[0,10],"tag2":[0,20]}`
	if r.String() != want {
		t.Errorf("want %s, got %s", want, r.String())
	}
	time.Sleep(1 * time.Second)
	want = `{"tag1":[0,10,0],"tag2":[0,20,0]}`
	if r.String() != want {
		t.Errorf("want %s, got %s", want, r.String())
	}
	time.Sleep(1 * time.Second)
	want = `{"tag1":[10,0,0],"tag2":[20,0,0]}`
	if r.String() != want {
		t.Errorf("want %s, got %s", want, r.String())
	}
}

func TestRatesConsistency(t *testing.T) {
	// This tests the following invariant: in the time window
	// covered by rates, the sum of the rates reported must be
	// equal to the count reported by the counter.
	const (
		interval = 1 * time.Second
		epsilon  = 50 * time.Millisecond
	)

	clear()
	c := NewCounters("rcounter4")
	r := NewRates("rates4", c, 100, interval)

	time.Sleep(epsilon)
	c.Add("a", 1000)
	time.Sleep(interval)
	c.Add("a", 1)
	time.Sleep(interval)

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
