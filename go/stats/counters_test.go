// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"expvar"
	"testing"
)

func TestCounters(t *testing.T) {
	clear()
	c := NewCounters("counter1")
	c.Add("c1", 1)
	c.Add("c2", 1)
	c.Add("c2", 1)
	want1 := `{"c1": 1, "c2": 2}`
	want2 := `{"c2": 2, "c1": 1}`
	if s := c.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
	counts := c.Counts()
	if counts["c1"] != 1 {
		t.Errorf("want 1, got %d", counts["c1"])
	}
	if counts["c2"] != 2 {
		t.Errorf("want 2, got %d", counts["c2"])
	}
	f := CountersFunc(func() map[string]int64 {
		return map[string]int64{
			"c1": 1,
			"c2": 2,
		}
	})
	if s := f.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
}

func TestMapCounters(t *testing.T) {
	clear()
	c := NewMapCounters("mapCounter1", []string{"aaa", "bbb"})
	c.Add([]string{"c1a", "c1b"}, 1)
	c.Add([]string{"c2a", "c2b"}, 1)
	c.Add([]string{"c2a", "c2b"}, 1)
	want1 := `{"c1a.c1b": 1, "c2a.c2b": 2}`
	want2 := `{"c2a.c2b": 2, "c1a.c1b": 1}`
	if s := c.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
	counts := c.Counts()
	if counts["c1a.c1b"] != 1 {
		t.Errorf("want 1, got %d", counts["c1a.c1b"])
	}
	if counts["c2a.c2b"] != 2 {
		t.Errorf("want 2, got %d", counts["c2a.c2b"])
	}
	f := NewMapCountersFunc("", []string{"aaa", "bbb"}, func() map[string]int64 {
		return map[string]int64{
			"c1a.c1b": 1,
			"c2a.c2b": 2,
		}
	})
	if s := f.String(); s != want1 && s != want2 {
		t.Errorf("want %s or %s, got %s", want1, want2, s)
	}
}

func TestCountersHook(t *testing.T) {
	var gotname string
	var gotv *Counters
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Counters)
	})

	v := NewCounters("counter2")
	if gotname != "counter2" {
		t.Errorf("want counter2, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}
