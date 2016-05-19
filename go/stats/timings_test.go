// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"expvar"
	"testing"
	"time"
)

func TestTimings(t *testing.T) {
	clear()
	tm := NewTimings("timings1")
	tm.Add("tag1", 500*time.Microsecond)
	tm.Add("tag1", 1*time.Millisecond)
	tm.Add("tag2", 1*time.Millisecond)
	want := `{"TotalCount":3,"TotalTime":2500000,"Histograms":{"tag1":{"500000":1,"1000000":2,"5000000":2,"10000000":2,"50000000":2,"100000000":2,"500000000":2,"1000000000":2,"5000000000":2,"10000000000":2,"inf":2,"Count":2,"Time":1500000},"tag2":{"500000":0,"1000000":1,"5000000":1,"10000000":1,"50000000":1,"100000000":1,"500000000":1,"1000000000":1,"5000000000":1,"10000000000":1,"inf":1,"Count":1,"Time":1000000}}}`
	if got := tm.String(); got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestMultiTimings(t *testing.T) {
	clear()
	mtm := NewMultiTimings("maptimings1", []string{"dim1", "dim2"})
	mtm.Add([]string{"tag1a", "tag1b"}, 500*time.Microsecond)
	mtm.Add([]string{"tag1a", "tag1b"}, 1*time.Millisecond)
	mtm.Add([]string{"tag2a", "tag2b"}, 1*time.Millisecond)
	want := `{"TotalCount":3,"TotalTime":2500000,"Histograms":{"tag1a.tag1b":{"500000":1,"1000000":2,"5000000":2,"10000000":2,"50000000":2,"100000000":2,"500000000":2,"1000000000":2,"5000000000":2,"10000000000":2,"inf":2,"Count":2,"Time":1500000},"tag2a.tag2b":{"500000":0,"1000000":1,"5000000":1,"10000000":1,"50000000":1,"100000000":1,"500000000":1,"1000000000":1,"5000000000":1,"10000000000":1,"inf":1,"Count":1,"Time":1000000}}}`
	if got := mtm.String(); got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestTimingsHook(t *testing.T) {
	var gotname string
	var gotv *Timings
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Timings)
	})

	name := "timings2"
	v := NewTimings(name)
	if gotname != name {
		t.Errorf("got %q, want %q", gotname, name)
	}
	if gotv != v {
		t.Errorf("got %#v, want %#v", gotv, v)
	}
}
