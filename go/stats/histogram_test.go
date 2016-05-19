// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"expvar"
	"testing"
)

func TestHistogram(t *testing.T) {
	clear()
	h := NewHistogram("hist1", []int64{1, 5})
	for i := 0; i < 10; i++ {
		h.Add(int64(i))
	}
	want := `{"1": 2, "5": 6, "inf": 10, "Count": 10, "Total": 45}`
	if h.String() != want {
		t.Errorf("got %v, want %v", h.String(), want)
	}
	counts := h.Counts()
	counts["Count"] = h.Count()
	counts["Total"] = h.Total()
	for k, want := range map[string]int64{
		"1":     2,
		"5":     4,
		"inf":   4,
		"Count": 10,
		"Total": 45,
	} {
		if got := counts[k]; got != want {
			t.Errorf("histogram counts [%v]: got %d, want %d", k, got, want)
		}
	}
	if got, want := h.CountLabel(), "Count"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := h.TotalLabel(), "Total"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGenericHistogram(t *testing.T) {
	clear()
	h := NewGenericHistogram(
		"histgen",
		[]int64{1, 5},
		[]string{"one", "five", "max"},
		"count",
		"total",
	)
	want := `{"one": 0, "five": 0, "max": 0, "count": 0, "total": 0}`
	if got := h.String(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestHistogramHook(t *testing.T) {
	var gotname string
	var gotv *Histogram
	clear()
	Register(func(name string, v expvar.Var) {
		gotname = name
		gotv = v.(*Histogram)
	})

	name := "hist2"
	v := NewHistogram(name, []int64{1})
	if gotname != name {
		t.Errorf("got %v; want %v", gotname, name)
	}
	if gotv != v {
		t.Errorf("got %#v, want %#v", gotv, v)
	}
}
