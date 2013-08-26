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
	Register(nil)
	tm := NewTimings("timings1")
	tm.Add("tag1", 500*time.Microsecond)
	tm.Add("tag1", 1*time.Millisecond)
	tm.Add("tag2", 1*time.Millisecond)
	want := `{"TotalCount":3,"TotalTime":2500000,"Histograms":{"tag1":{"0.0005":1,"0.0010":1,"0.0050":0,"0.0100":0,"0.0500":0,"0.1000":0,"0.5000":0,"1.0000":0,"5.0000":0,"10.0000":0,"Max":0,"Count":2,"Time":1500000},"tag2":{"0.0005":0,"0.0010":1,"0.0050":0,"0.0100":0,"0.0500":0,"0.1000":0,"0.5000":0,"1.0000":0,"5.0000":0,"10.0000":0,"Max":0,"Count":1,"Time":1000000}}}`
	if tm.String() != want {
		t.Errorf("want %s, got %s", want, tm.String())
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

	v := NewTimings("timings2")
	if gotname != "timings2" {
		t.Errorf("want timings2, got %s", gotname)
	}
	if gotv != v {
		t.Errorf("want %#v, got %#v", v, gotv)
	}
}
